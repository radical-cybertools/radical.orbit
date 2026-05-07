"""RADICAL Edge Bridge — class-based.

This module hosts the :class:`Bridge` class.  The thin
``bin/radical-edge-bridge.py`` script just argparse-parses CLI options
and instantiates ``Bridge(...).run()``.

Bridge config (URL/cert/key) is resolved via
:mod:`radical.edge.utils` — CLI > env > file.  See that module's
``resolve_bridge_*`` helpers for the precedence rules.
"""

# pylint: disable=protected-access

import asyncio
import base64
import itertools
import json
import logging
import os
import re
import signal
from contextlib import asynccontextmanager
from typing  import Any, Dict, Optional

import msgpack

from fastapi               import FastAPI, WebSocket, WebSocketDisconnect
from fastapi               import Request, Response, HTTPException
from fastapi.responses     import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses   import StreamingResponse
from starlette.websockets  import WebSocketState

from . import _prof as rprof
from . import utils

from .bridge_plugin_host import BridgePluginHost


log = logging.getLogger("radical.edge.bridge")

BRIDGE_EDGE_NAME   = 'bridge'
HEARTBEAT_INTERVAL = 20
# Per proxied request: the bridge forwards a HTTP request to the edge over
# WS and waits this many seconds for the edge's response before returning
# 504.  Bumped to 600 s so submit batches of 1000s of tasks (whose dragon-
# side ProcessGroup creation takes seconds per task) don't time out.
REQUEST_TIMEOUT    = 600


class Bridge:
    """RADICAL Edge Bridge — reverse proxy connecting clients to edge services.

    Builds (or accepts) a FastAPI app, registers all bridge routes
    (proxy, WS register, SSE, UI, edge management), and exposes a
    blocking :meth:`run` method that starts uvicorn with the resolved
    TLS config.

    The advertised URL is *derived* from ``(host, port)`` — the bridge
    never reads ``bridge.url``, it only writes it.  Wildcard binds
    advertise the local FQDN (with an outbound-IPv4 fallback shown on
    stdout); specific binds advertise that literal address.

    Args:
      app:     existing ``FastAPI`` instance to register routes on; if
               ``None``, the bridge constructs its own with the lifespan
               handler attached.
      cert:    CLI override for the TLS cert path.  Falls back to
               ``$RADICAL_BRIDGE_CERT`` → ``~/.radical/edge/bridge_cert.pem``.
      key:     CLI override for the TLS key path.  Falls back to
               ``$RADICAL_BRIDGE_KEY`` → ``~/.radical/edge/bridge_key.pem``.
               Refuses to start if the key file is more permissive than
               ``0o600``.
      host:    bind address (default ``0.0.0.0``).
      port:    bind port (default ``8000``).
      plugins: comma-separated plugin spec for bridge-hosted plugins
               (default: ``'default'`` — the bridge role default set).
    """

    def __init__(self,
                 app:     Optional[FastAPI] = None,
                 cert:    Optional[str]     = None,
                 key:     Optional[str]     = None,
                 host:    str = '0.0.0.0',
                 port:    int = 8000,
                 plugins: str = 'default'):
        # ── Resolve TLS config (cert/key) ────────────────────────────
        cert_path, _ = utils.resolve_bridge_cert(cli=cert)
        key_path,  _ = utils.resolve_bridge_key (cli=key, cert=cert_path)
        self._cert: str = str(cert_path)
        self._key : str = str(key_path)

        # ── Derive advertised URL from (host, port) ──────────────────
        # The bridge produces its URL — it never reads ``bridge.url``.
        # Wildcard binds (0.0.0.0 / :: / '') yield FQDN- and IPv4-derived
        # forms; specific binds (e.g. 127.0.0.1) advertise that literal.
        self._host = host
        self._port = port
        self._url_forms = utils.public_url_forms(self._host, self._port)
        self._url: str  = self._url_forms[0]

        # ── Instance state (was module-level in the old script) ──────
        self.edges:               Dict[str, WebSocket] = {}
        self.pending:             Dict[str, tuple]     = {}
        self.pending_lock:        asyncio.Lock         = asyncio.Lock()
        self.endpoints:           Dict[str, Any]       = {
            "bridge": {"url": self._url},
            "edges":  {},
        }
        self.clients_sse:         set                  = set()
        self.bridge_plugin_host:  Optional[BridgePluginHost] = None
        self._plugin_ui_module_js: Dict[str, str]      = {}
        self._shutdown_event:     asyncio.Event        = asyncio.Event()
        self._bridge_prof                              = rprof.Profiler(
            'bridge', ns='radical.edge')
        self._bridge_req_ctr                           = itertools.count()
        self._plugins_spec:       str                  = plugins or ''

        # ── Build or accept the FastAPI app ──────────────────────────
        if app is None:
            app = FastAPI(
                title="RADICAL Edge Bridge",
                lifespan=self._lifespan,
                description=(
                    "RADICAL Edge Bridge — reverse proxy connecting clients "
                    "to HPC edge services."),
                version="0.1.0",
                docs_url="/docs",
                redoc_url="/redoc",
            )
        self._app: FastAPI = app
        # Mark this app as the bridge so utils.host_role() reports correctly.
        self._app.state.is_bridge = True

        self._setup_middleware()
        self._setup_exception_handlers()
        self._register_routes()

    # ── public API ───────────────────────────────────────────────────

    @property
    def app(self) -> FastAPI:
        """The underlying ``FastAPI`` app — useful for tests."""
        return self._app

    @property
    def url(self) -> str:
        """The bridge's advertised URL (canonical FQDN form)."""
        return self._url

    def run(self) -> None:
        """Start uvicorn.  Blocks until shutdown.

        Prints both the FQDN and IPv4-derived URL forms on stdout (for
        the operator to copy whichever is reachable from clients) and
        writes the canonical FQDN URL to ``~/.radical/edge/bridge.url``
        so future clients/edges can find this bridge without needing
        the env var set.
        """
        import uvicorn

        # Print all URL forms (canonical first, alternates after) so
        # the operator can copy whichever is reachable from clients.
        for form in self._url_forms:
            print(f'[Bridge] URL: {form}', flush=True)

        # Write the URL file only when it does not already exist —
        # never clobber a file the operator may have placed there
        # (e.g. for a different bridge they want consumers to default
        # to).  An operator who wants this bridge's URL recorded just
        # deletes the file before starting.
        if not utils.URL_FILE.exists():
            try:
                utils.write_bridge_url_file(self._url)
                log.info('[Bridge] wrote URL file: %s', utils.URL_FILE)
            except Exception as e:
                log.warning('[Bridge] could not write URL file %s: %s',
                            utils.URL_FILE, e)
        else:
            log.info('[Bridge] URL file already present, leaving it: %s',
                     utils.URL_FILE)

        # Suppress CancelledError noise during graceful shutdown.
        class _ShutdownFilter(logging.Filter):
            def filter(self, record):
                msg = str(record.getMessage())
                if 'CancelledError' in msg:
                    return False
                if record.exc_info:
                    exc = record.exc_info[1]
                    if isinstance(exc, asyncio.CancelledError):
                        return False
                return True

        logging.getLogger("uvicorn.error").addFilter(_ShutdownFilter())

        uvicorn.run(self._app,
                    host=self._host,
                    port=self._port,
                    reload=False,
                    ssl_certfile=self._cert,
                    ssl_keyfile=self._key,
                    log_level="info",
                    ws_max_size=10 * 1024 * 1024,
                    ws_per_message_deflate=True,
                    # Generous pong deadline so the edge's blocking dragon
                    # init (Batch(num_nodes=…) on a multi-node alloc takes
                    # 30+ s and pauses the asyncio loop) doesn't trip the
                    # websockets-library keepalive.  Probe cadence stays at
                    # the default 20 s; the 600 s ceiling is the per-ping
                    # tolerance window.
                    ws_ping_interval=20.0,
                    ws_ping_timeout=600.0,
                    timeout_graceful_shutdown=3)

    # ── lifecycle ────────────────────────────────────────────────────

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        """Startup/shutdown lifecycle for bridge-hosted plugins + cleanup."""
        self._shutdown_event.clear()

        # Load bridge-hosted plugins if configured.
        if self._plugins_spec:
            names = [t.strip() for t in self._plugins_spec.split(',')
                     if t.strip()]
            if names:
                async def _on_topology_changed():
                    if self.bridge_plugin_host:
                        self.endpoints['edges'][BRIDGE_EDGE_NAME] = \
                            self.bridge_plugin_host.get_topology_info()
                        self._plugin_ui_module_js.update(
                            self.bridge_plugin_host.get_ui_modules())
                    await self._broadcast_event('topology', self.endpoints)
                    await self._broadcast_topology_to_edges()

                self.bridge_plugin_host = BridgePluginHost(
                    names, self._broadcast_event, BRIDGE_EDGE_NAME,
                    on_topology_changed=_on_topology_changed)
                self.endpoints['edges'][BRIDGE_EDGE_NAME] = \
                    self.bridge_plugin_host.get_topology_info()
                self._plugin_ui_module_js.update(
                    self.bridge_plugin_host.get_ui_modules())
                log.info('[Bridge] Loaded bridge plugins: %s', names)

        async def _print_url():
            await asyncio.sleep(0.2)
            print(f"[Bridge] URL: "
                  f"{self.endpoints['bridge'].get('url', 'unknown')}",
                  flush=True)
        asyncio.ensure_future(_print_url())

        yield

        # Shutdown
        log.info("[Bridge] Shutting down...")
        self._shutdown_event.set()
        for q in list(self.clients_sse):
            try:    await q.put(None)
            except Exception as e:
                log.debug("[Bridge] SSE queue put failed during shutdown: %s", e)
        for edge_name, ws in list(self.edges.items()):
            try:    await ws.close(code=1001, reason="Server shutting down")
            except Exception as e:
                log.debug("[Bridge] WebSocket close failed for %s: %s",
                          edge_name, e)
        self.edges.clear()
        log.info("[Bridge] Shutdown complete")

    # ── helpers (was module-level) ───────────────────────────────────

    async def _broadcast_event(self, topic: str, data: dict):
        msg       = json.dumps({"topic": topic, "data": data})
        formatted = f"data: {msg}\n\n"
        for q in list(self.clients_sse):
            await q.put(formatted)

    async def _broadcast_topology_to_edges(self):
        """Broadcast current topology to all connected edges."""
        edge_list = {name: {"plugins": list(info.get("plugins", {}).keys())}
                     for name, info in self.endpoints.get("edges", {}).items()}
        msg = json.dumps({"type": "topology", "edges": edge_list})
        for edge_name, ws in list(self.edges.items()):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_text(msg)
            except Exception as e:
                log.warning("[Bridge] Failed to send topology to %s: %s",
                            edge_name, e)
        if self.bridge_plugin_host is not None:
            try:
                await self.bridge_plugin_host.on_topology_change(edge_list)
            except Exception as e:
                log.warning(
                    "[Bridge] Failed topology notify to bridge plugins: %s", e)

    async def _send_to_edge(self, edge_name: str, message,
                            binary: bool = False):
        ws = self.edges.get(edge_name)
        if not ws or ws.client_state != WebSocketState.CONNECTED:
            raise HTTPException(
                status_code=503,
                detail=f"Edge '{edge_name}' not connected")
        if binary: await ws.send_bytes(message)
        else:      await ws.send_text(message)

    @staticmethod
    def _strip_headers(request: Request) -> dict:
        to_strip = {"connection", "keep-alive", "proxy-authenticate",
                    "proxy-authorization", "te", "trailers",
                    "transfer-encoding", "upgrade"}
        return {k: v for k, v in request.headers.items()
                if k.lower() not in to_strip}

    # ── middleware + exception handlers ──────────────────────────────

    def _setup_middleware(self):
        # LUCID needs credentials; browsers reject credentials + wildcard
        # origin, so we list allowed origins explicitly.
        self._app.add_middleware(
            CORSMiddleware,
            allow_credentials=True,
            allow_origins=["https://dev-1.bv-brc.org"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def _setup_exception_handlers(self):
        app = self._app

        @app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request,
                                          exc: HTTPException) -> JSONResponse:
            return JSONResponse(
                status_code=exc.status_code,
                content={"error": True,
                         "status_code": exc.status_code,
                         "detail": exc.detail})

        @app.exception_handler(ValueError)
        async def value_error_handler(request: Request,
                                       exc: ValueError) -> JSONResponse:
            log.warning("[Bridge] ValueError: %s", exc)
            return JSONResponse(
                status_code=400,
                content={"error": True,
                         "status_code": 400,
                         "detail": str(exc)})

        @app.exception_handler(Exception)
        async def general_exception_handler(request: Request,
                                             exc: Exception) -> JSONResponse:
            log.exception("[Bridge] Unhandled exception: %s", exc)
            return JSONResponse(
                status_code=500,
                content={"error": True,
                         "status_code": 500,
                         "detail": "Internal server error"})

    # ── routes ───────────────────────────────────────────────────────

    def _register_routes(self):
        """Attach all bridge routes to ``self._app``.

        Defined as nested closures so they capture ``self`` from the
        enclosing method scope — keeps state instance-local while
        preserving the FastAPI decorator idiom.
        """
        self_ = self
        app   = self._app

        # ── /register (WebSocket) ────────────────────────────────────

        @app.websocket("/register")
        async def register(ws: WebSocket):
            await ws.accept()
            edge_name: Optional[str] = None

            async def pinger():
                elapsed = 0
                while not self_._shutdown_event.is_set():
                    try:
                        await asyncio.sleep(1.0)
                        elapsed += 1
                    except asyncio.CancelledError:
                        return
                    if elapsed < HEARTBEAT_INTERVAL:
                        continue
                    elapsed = 0
                    if ws.client_state != WebSocketState.CONNECTED:
                        return
                    try:
                        await ws.send_text(json.dumps({"type": "ping"}))
                    except Exception as e:
                        log.exception("[Bridge] Ping failed for edge: %s", e)
                        return

            ping_task = None
            try:
                ping_task = asyncio.create_task(pinger())

                while not self_._shutdown_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.receive(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    if raw.get("type") == "websocket.disconnect":
                        raise WebSocketDisconnect(raw.get("code", 1000))

                    try:
                        if raw.get("bytes"):
                            self_._bridge_prof.prof(
                                'bridge_deser',
                                msg='msgpack:%d' % len(raw["bytes"]))
                            data = msgpack.unpackb(raw["bytes"], raw=False)
                        else:
                            self_._bridge_prof.prof(
                                'bridge_deser',
                                msg='json:%d' % len(raw.get("text", "")))
                            data = json.loads(raw.get("text", "{}"))
                        self_._bridge_prof.prof('bridge_deser_done',
                                                uid=data.get('req_id', ''))
                    except Exception:
                        log.warning("[Bridge] Malformed message from edge '%s'",
                                    edge_name or '(unregistered)')
                        continue

                    if data.get("type") == "pong":
                        pass

                    elif data.get("type") == "register":
                        frame_edge_name = data.get("edge_name")
                        if not frame_edge_name:
                            log.warning("[Bridge] Registration missing edge_name")
                            continue
                        if frame_edge_name == BRIDGE_EDGE_NAME:
                            log.warning("[Bridge] Edge name '%s' is reserved",
                                        frame_edge_name)
                            await ws.send_text(json.dumps({
                                "type": "error",
                                "message": f"Edge name '{frame_edge_name}' is reserved"}))
                            return
                        if frame_edge_name in self_.edges:
                            log.warning("[Bridge] Edge '%s' already connected.",
                                        frame_edge_name)
                            await ws.send_text(json.dumps({
                                "type": "error",
                                "message": f"Edge '{frame_edge_name}' already used"}))
                            return

                        edge_name = frame_edge_name
                        self_.edges[edge_name] = ws
                        log.info("[Bridge] Edge '%s' connected", edge_name)
                        self_.endpoints["edges"][edge_name] = {
                            "endpoint": data.get("endpoint", {}),
                            "plugins":  {},
                        }
                        for pname, pdata in data.get("plugins", {}).items():
                            js_content = pdata.pop("ui_module", None)
                            if js_content:
                                self_._plugin_ui_module_js[pname] = js_content
                                log.info(
                                    "[Bridge] Cached UI module for plugin "
                                    "'%s' from edge '%s'", pname, edge_name)
                            self_.endpoints["edges"][edge_name]["plugins"][pname] = pdata

                        plugin_names = list(
                            self_.endpoints["edges"][edge_name]["plugins"].keys())
                        log.info("[Bridge] Edge '%s' registered  plugins=%s",
                                 edge_name, plugin_names)

                        await self_._broadcast_event("topology", self_.endpoints)
                        await self_._broadcast_topology_to_edges()

                    elif data.get("type") == "notification":
                        await self_._broadcast_event("notification", {
                            "edge":   edge_name,
                            "plugin": data.get("plugin"),
                            "topic":  data.get("topic"),
                            "data":   data.get("data"),
                        })

                    elif data.get("type") == "response":
                        req_id = data.get("req_id")
                        if not req_id:
                            log.warning(
                                "[Bridge] Response from '%s' missing req_id: %s",
                                edge_name, str(data)[:200])
                            continue
                        async with self_.pending_lock:
                            entry = self_.pending.pop(req_id, None)
                        if entry:
                            fut = entry[0]
                            if not fut.done():
                                fut.set_result(data)

                    else:
                        log.debug("[Bridge] Unknown message type received: %s",
                                  data)

            except WebSocketDisconnect:
                pass
            except RuntimeError as e:
                if "not connected" in str(e).lower():
                    log.debug(
                        "[Bridge] recv interrupted on disconnected edge '%s'",
                        edge_name or '(unknown)')
                else:
                    log.exception("[Bridge] Edge connection error: %s", e)
            except Exception as e:
                log.exception("[Bridge] Edge connection error: %s", e)

            finally:
                log.info("[Bridge] Edge disconnected: %s", edge_name)
                if ping_task:
                    ping_task.cancel()

                if edge_name:
                    if self_.edges.get(edge_name) == ws:
                        del self_.edges[edge_name]
                        if edge_name in self_.endpoints["edges"]:
                            log.info("[Bridge] Unregistering edge: %s", edge_name)
                            del self_.endpoints["edges"][edge_name]
                            await self_._broadcast_event(
                                "topology", self_.endpoints)
                            await self_._broadcast_topology_to_edges()
                    else:
                        log.info(
                            "[Bridge] Disconnected duplicate/inactive session "
                            "for: %s", edge_name)

                if edge_name:
                    async with self_.pending_lock:
                        failed = [rid for rid, (fut, ename)
                                  in self_.pending.items()
                                  if ename == edge_name]
                        for rid in failed:
                            fut, _ = self_.pending.pop(rid)
                            if not fut.done():
                                fut.set_exception(
                                    HTTPException(503, "Edge disconnected"))

        # ── /events (SSE) ────────────────────────────────────────────

        @app.get("/events", tags=["Events"])
        async def sse_events(request: Request):
            q = asyncio.Queue()
            self_.clients_sse.add(q)
            await q.put(
                f"data: {json.dumps({'topic': 'topology', 'data': self_.endpoints})}\n\n")

            async def event_generator():
                try:
                    while not self_._shutdown_event.is_set():
                        if await request.is_disconnected():
                            break
                        try:
                            msg = await asyncio.wait_for(q.get(), timeout=1.0)
                            if msg is None:
                                break
                            yield msg
                        except asyncio.TimeoutError:
                            continue
                except asyncio.CancelledError:
                    log.debug("[Bridge] SSE client cancelled")
                except Exception as e:
                    log.exception("[Bridge] SSE client error: %s", e)
                finally:
                    self_.clients_sse.discard(q)

            return StreamingResponse(event_generator(),
                                     media_type="text/event-stream")

        # ── topology / edge management ───────────────────────────────

        @app.post("/edge/list", tags=["Discovery"])
        async def edge_list(request: Request):
            return JSONResponse({"data": self_.endpoints})

        @app.get("/edges", tags=["Discovery"])
        async def get_edges():
            edge_list_resp = []
            for edge_name, edge_data in self_.endpoints.get("edges", {}).items():
                plugins = list(edge_data.get("plugins", {}).keys())
                connected = (edge_name in self_.edges
                             or edge_name == BRIDGE_EDGE_NAME)
                edge_list_resp.append({
                    "name":          edge_name,
                    "plugins":       plugins,
                    "connected":     connected,
                    "plugin_count":  len(plugins),
                })
            return JSONResponse({"edges": edge_list_resp,
                                 "total": len(edge_list_resp)})

        @app.post("/edge/disconnect/{edge_name}", tags=["Management"])
        async def disconnect_edge(edge_name: str):
            if edge_name == BRIDGE_EDGE_NAME:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot disconnect bridge-hosted plugins")
            if edge_name not in self_.edges:
                raise HTTPException(
                    status_code=404,
                    detail=f"Edge '{edge_name}' not connected")
            ws = self_.edges[edge_name]
            try:
                await ws.send_text(json.dumps({"type": "shutdown",
                                               "reason": "Disconnected by user"}))
                await ws.close(code=1000)
            except Exception as e:
                log.warning("[Bridge] Error shutting down edge %s: %s",
                            edge_name, e)
            return JSONResponse({"status": "shutdown", "edge": edge_name})

        @app.post("/bridge/terminate", tags=["Management"])
        async def terminate_bridge():
            async def delayed_shutdown():
                await asyncio.sleep(0.5)
                log.info("[Bridge] Terminating via API request")
                os.kill(os.getpid(), signal.SIGTERM)
            asyncio.create_task(delayed_shutdown())
            return JSONResponse({
                "status":  "terminating",
                "message": "Bridge will terminate shortly. "
                           "Edges will not be shut down."})

        # ── /edge/submit & friends (501 stubs) ───────────────────────

        @app.post("/edge/submit", tags=["Edge Submission"])
        async def submit_tunneled(request: Request):
            raise HTTPException(
                status_code=501,
                detail="Edge submission not implemented — "
                       "PsiJ remote submission not yet available")

        @app.get("/edge/job/{job_id}", tags=["Edge Submission"])
        async def get_edge_job_status(job_id: str):
            raise HTTPException(
                status_code=501,
                detail="Edge job status not implemented — "
                       "PsiJ remote submission not yet available")

        @app.post("/edge/job/{job_id}/cancel", tags=["Edge Submission"])
        async def cancel_edge_job(job_id: str):
            raise HTTPException(
                status_code=501,
                detail="Edge job cancellation not implemented — "
                       "PsiJ remote submission not yet available")

        @app.get("/edge/jobs", tags=["Edge Submission"])
        async def list_edge_jobs():
            raise HTTPException(
                status_code=501,
                detail="Edge job listing not implemented — "
                       "PsiJ remote submission not yet available")

        # ── UI: index + plugin JS modules ────────────────────────────

        @app.get("/", tags=["UI"], include_in_schema=False)
        async def root():
            html_path = None
            try:
                from importlib.resources import files
                data_dir  = files('radical.edge').joinpath('data')
                candidate = data_dir.joinpath('edge_explorer.html')
                if hasattr(candidate, '__fspath__'):
                    html_path = os.fspath(candidate)        # type: ignore[arg-type]
                else:
                    html_path = str(candidate)
                if not os.path.exists(html_path):
                    html_path = None
            except Exception as e:
                log.debug("[Bridge] importlib.resources lookup failed: %s", e)

            if not html_path:
                try:
                    import pkg_resources                    # type: ignore[import]
                    html_path = pkg_resources.resource_filename(
                        'radical.edge', 'data/edge_explorer.html')
                    if not os.path.exists(html_path):
                        html_path = None
                except Exception as e:
                    log.debug("[Bridge] pkg_resources lookup failed: %s", e)

            if html_path and os.path.exists(html_path):
                return FileResponse(html_path, headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma":        "no-cache",
                    "Expires":       "0",
                })
            return Response(content="edge_explorer.html not found",
                            status_code=404)

        @app.get("/plugins/{filename}", tags=["UI"], include_in_schema=False)
        async def serve_plugin(filename: str):
            if not re.match(r'^[a-z_][a-z0-9_.]*\.js$', filename):
                raise HTTPException(status_code=404,
                                    detail="Invalid plugin filename")

            plugin_path = None

            try:
                from importlib.resources import files
                data_dir       = (files('radical.edge')
                                  .joinpath('data').joinpath('plugins'))
                candidate      = data_dir.joinpath(filename)
                candidate_path = (os.fspath(candidate)      # type: ignore[arg-type]
                                  if hasattr(candidate, '__fspath__')
                                  else str(candidate))
                if os.path.exists(candidate_path):
                    plugin_path = candidate_path
            except Exception as e:
                log.debug(
                    "[Bridge] importlib.resources plugin lookup failed: %s", e)

            if not plugin_path:
                try:
                    import pkg_resources                    # type: ignore[import]
                    candidate_path = pkg_resources.resource_filename(
                        'radical.edge', f'data/plugins/{filename}')
                    if os.path.exists(candidate_path):
                        plugin_path = candidate_path
                except Exception as e:
                    log.debug(
                        "[Bridge] pkg_resources plugin lookup failed: %s", e)

            if not plugin_path:
                plugin_name = filename[:-3]
                js_content  = self_._plugin_ui_module_js.get(plugin_name)
                if js_content:
                    return Response(
                        js_content,
                        media_type="application/javascript",
                        headers={
                            "Cache-Control": "no-cache, no-store, must-revalidate",
                            "Pragma":        "no-cache",
                            "Expires":       "0",
                        })
                log.warning("[Bridge] No UI module found for plugin '%s'",
                            plugin_name)

            if plugin_path and os.path.exists(plugin_path):
                return FileResponse(
                    plugin_path,
                    media_type="application/javascript",
                    headers={
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Pragma":        "no-cache",
                        "Expires":       "0",
                    })

            raise HTTPException(status_code=404,
                                detail=f"Plugin '{filename}' not found")

        # ── catch-all proxy (must register LAST) ─────────────────────

        @app.api_route(
            "/{full_path:path}",
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
            tags=["Proxy"],
            summary="Proxy requests to edge plugins")
        async def proxy(full_path: str, request: Request):
            parts = full_path.strip('/').split('/', 1)
            if not parts:
                raise HTTPException(status_code=404, detail="Invalid path")
            edge_name = parts[0]

            # Bridge-hosted plugins: dispatch locally.
            if (edge_name == BRIDGE_EDGE_NAME
                    and self_.bridge_plugin_host is not None):
                forward_path = '/' + parts[1] if len(parts) > 1 else '/'
                return await self_.bridge_plugin_host.handle_request(
                    method       = request.method,
                    path         = forward_path,
                    headers      = dict(request.headers),
                    body_bytes   = await request.body(),
                    query_string = (str(request.url.query)
                                    if request.url.query else ''),
                )

            if edge_name not in self_.edges:
                raise HTTPException(
                    status_code=404,
                    detail=f"Edge '{edge_name}' unknown")

            forward_path = '/' + parts[1] if len(parts) > 1 else '/'

            body_bytes = await request.body()
            body       = None
            is_binary  = False
            if body_bytes:
                try:
                    body = body_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    is_binary = True

            req_id = (request.headers.get("x-request-id")
                      or 'req.%06d' % next(self_._bridge_req_ctr))

            self_._bridge_prof.prof('bridge_recv', uid=req_id,
                                    msg='%s %s' % (request.method, forward_path))
            self_._bridge_prof.prof('bridge_body_prep', uid=req_id,
                                    msg=str(len(body_bytes)))

            if request.url.query:
                forward_path += f"?{request.url.query}"

            message = {
                "type":      "request",
                "req_id":    req_id,
                "method":    request.method,
                "path":      forward_path,
                "headers":   self_._strip_headers(request),
                "is_binary": is_binary,
                "body":      body_bytes if is_binary else body,
            }

            fut = asyncio.get_running_loop().create_future()
            async with self_.pending_lock:
                self_.pending[req_id] = (fut, edge_name)

            try:
                self_._bridge_prof.prof('bridge_ser', uid=req_id)
                if is_binary:
                    wire = msgpack.packb(message, use_bin_type=True)
                else:
                    wire = json.dumps(message)
                self_._bridge_prof.prof('bridge_ser_done', uid=req_id,
                                        msg=str(len(wire)))
                self_._bridge_prof.prof('bridge_ws_send', uid=req_id)
                await self_._send_to_edge(edge_name, wire, binary=is_binary)
                self_._bridge_prof.prof('bridge_ws_sent', uid=req_id)
            except HTTPException:
                async with self_.pending_lock:
                    self_.pending.pop(req_id, None)
                raise

            try:
                resp = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)
            except asyncio.TimeoutError as exc:
                async with self_.pending_lock:
                    self_.pending.pop(req_id, None)
                raise HTTPException(
                    status_code=504,
                    detail="Upstream (edge) timeout") from exc

            self_._bridge_prof.prof('bridge_ws_recv', uid=req_id)

            status    = int(resp.get("status", 502))
            headers   = resp.get("headers") or {}
            resp_body = resp.get("body")

            self_._bridge_prof.prof('bridge_resp_ser', uid=req_id)

            if resp.get("is_binary"):
                try:
                    raw = base64.b64decode(resp_body or b"")
                except Exception as e:
                    log.exception(
                        "[Bridge] Failed to decode binary response: %s", e)
                    raw = b""
                self_._bridge_prof.prof('bridge_reply', uid=req_id,
                                        state=str(status))
                return Response(content=raw, status_code=status, headers=headers)

            content = resp_body or ""
            ctype   = headers.get("content-type", "")
            if "application/json" in ctype:
                try:
                    headers = {k.lower(): v for k, v in headers.items()
                               if k.lower() != "content-type"}
                    parsed = (content if isinstance(content, (dict, list))
                              else json.loads(content))
                    self_._bridge_prof.prof('bridge_reply', uid=req_id,
                                            state=str(status))
                    return JSONResponse(content=parsed, status_code=status,
                                        headers=headers)
                except Exception as e:
                    log.exception(
                        "[Bridge] Failed to parse JSON response: %s", e)

            self_._bridge_prof.prof('bridge_reply', uid=req_id,
                                    state=str(status))
            return Response(content=content, status_code=status, headers=headers)
