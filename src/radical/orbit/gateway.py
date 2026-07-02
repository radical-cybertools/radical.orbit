"""ORBIT Gateway — the broker's compat-tier HTTP/SSE/UI ingress.

The gateway is a broker **module**, explicitly *not* a :class:`Plugin`: it is
the single non-participant ingress onto the participant star.  Where a
:class:`~radical.orbit.plugin_base.Plugin` is handed a narrow app+session
surface and speaks the participant protocol, the gateway needs an interface far
wider than ``Plugin``'s — the shared pending table, the raw event tap, the
topology snapshot, the uvicorn app, CORS, and the ingress token gate — and it
serves clients that never speak the wire protocol at all (browsers, ``curl``,
the legacy HTTP client).  Making it an honest module with a constructor-declared
seam is the point.

**The seam (why it is wider than a plugin's).**  :class:`Gateway` consumes the
following members of the :class:`~radical.orbit.broker.Broker` it is handed:

* :attr:`Broker.app`               — mount the HTTP routes + middleware here
                                     (single port, single uvicorn server).
* :attr:`Broker.caller`            — the HTTP catch-all proxies every request
                                     through this handle over the **shared**
                                     broker pending table.
* :meth:`Broker.tap`               — the ``/events`` SSE fan-out subscribes to
                                     the raw event stream (plugin notifications).
* :meth:`Broker.add_topology_listener` — the ``/events`` topology fan-out (the
                                     one minimal hook M5 adds to the broker, as
                                     topology changes do not flow through the
                                     tap).
* :meth:`Broker.topology_snapshot` — the source of the discovery response and
                                     the SSE topology payload.
* :attr:`Broker.token` / :attr:`Broker.auth_enabled` — the ingress token gate
                                     and the ``/auth`` cookie.
* :attr:`Broker.url`               — advertised in the discovery response.
* :attr:`Broker.registry`          — the unknown-endpoint / disconnect check.
* :meth:`Broker._disconnect` / :meth:`Broker._handle_control` — the admin
                                     control path (protected members, used by
                                     the disconnect/terminate routes).

**Loops.**  Gateway HTTP/SSE handlers run on the **routing loop** (uvicorn's
loop): they are ``async`` and ``await`` the caller — no blocking work in a
handler.  The topology listener also fires on the routing loop, so it reads
``topology_snapshot()`` (routing-loop-owned state) directly.  The raw event tap,
however, fires on the **plugin-host loop**; its callback hands the formatted SSE
frame to the routing loop via ``call_soon_threadsafe`` before touching any
per-client queue.

**Backpressure contract.**  Each SSE client owns a *bounded, drop-oldest* queue
with a dropped counter (mirroring the broker's per-subscriber delivery queues):
a stalled browser can never grow broker memory — the oldest frame is dropped and
the loss is countable, never propagated back into the routing loop.

**Sessions.**  Gateway-originated sessions carry **no participant identity** —
they are capability-style within the token trust domain.  Only TTL/persistent
sessions are meaningful for such callers; owner-checked reattach (403) is an M6
concern.

The gateway preserves the **exact** bridge-era HTTP surface (paths, response
shapes, SSE message envelopes) so the Explorer UI keeps working unchanged; it is
the compat tier the broker-native runtime does not need.
"""

# pylint: disable=protected-access

import asyncio
import json
import logging
import os
import posixpath
import re

from collections import deque
from typing      import Any, Dict, Optional, Set

from fastapi                    import Request, Response, HTTPException
from fastapi.responses          import JSONResponse, FileResponse
from fastapi.middleware.cors    import CORSMiddleware
from starlette.middleware.base  import BaseHTTPMiddleware
from starlette.responses        import StreamingResponse

from . import utils


log = logging.getLogger("radical.orbit.gateway")

# Per proxied request: the gateway forwards an HTTP request to the endpoint over
# the shared broker pending table and waits this many seconds for the response
# before returning 504.  Carried over verbatim from the bridge (``bridge.py``
# ``REQUEST_TIMEOUT``): a submit batch of thousands of tasks (whose dragon-side
# ProcessGroup creation takes seconds per task) genuinely takes this long.
REQUEST_TIMEOUT = 600

# The name the broker reserves for its own hosted-plugin participant.  Kept in
# sync with :data:`radical.orbit.broker.BROKER_NAME`; duplicated here to avoid a
# module import cycle (broker imports gateway).
BROKER_NAME = 'broker'

# Allowed CORS origins — carried over from ``Bridge._setup_middleware``.  LUCID
# needs credentials; browsers reject credentials + wildcard origin, so the
# allow-list is explicit.
CORS_ORIGINS = [
    "http://localhost",
    "http://localhost:8080",
    "https://localhost",
    "https://localhost:8080",
    "https://dev-1.bv-brc.org",
]

# Hop-by-hop headers plus the bridge credential, stripped before a request is
# forwarded to an endpoint (so the shared token never rides on to plugins).
_HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate",
               "proxy-authorization", "te", "trailers",
               "transfer-encoding", "upgrade", "authorization"}


# ---------------------------------------------------------------------------
# Per-SSE-client bounded delivery queue
# ---------------------------------------------------------------------------

class _SSEQueue:
    """A per-SSE-client bounded, drop-oldest delivery queue.

    Overflow drops the *oldest* frame and bumps :attr:`dropped` — a stalled
    browser is disciplined at its own queue and can never backpressure the
    routing loop or grow broker memory.  The ``deque(maxlen=...)`` does the
    oldest-eviction; ``dropped`` makes the loss observable.
    """

    __slots__ = ('buf', 'dropped', 'wake')

    def __init__(self, maxlen: int):
        self.buf     : deque         = deque(maxlen=maxlen)
        self.dropped : int           = 0
        self.wake    : asyncio.Event = asyncio.Event()

    def push(self, item: str) -> None:
        if len(self.buf) == self.buf.maxlen:
            self.dropped += 1                  # deque evicts the oldest here
        self.buf.append(item)
        self.wake.set()


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

class Gateway:
    """The compat-tier HTTP/SSE/UI ingress attached onto :attr:`Broker.app`.

    Args:
      broker:          the :class:`~radical.orbit.broker.Broker` whose seam this
                       gateway consumes (see the module docstring).
      request_timeout: the proxy 504 deadline in seconds (default
                       :data:`REQUEST_TIMEOUT`).
      sse_queue:       per-SSE-client delivery queue depth (drop-oldest).
    """

    def __init__(self, broker,
                 request_timeout: int = REQUEST_TIMEOUT,
                 sse_queue:       int = 1024):
        self._broker          = broker
        self._app             = broker.app
        self._request_timeout = request_timeout
        self._sse_depth       = sse_queue

        # SSE fan-out state.  Clients live on the routing loop; the tap callback
        # (plugin-host loop) hands frames over via ``_routing_loop``.
        self._sse_clients:  Set[_SSEQueue]                        = set()
        self._routing_loop: Optional[asyncio.AbstractEventLoop]   = None
        self._untap         = None
        self._untopo        = None

        self._setup_middleware()
        self._register_routes()
        self._subscribe_events()

    # ── token config passthrough ──────────────────────────────────────

    @property
    def _token(self) -> Optional[str]:
        return self._broker.token

    @property
    def _auth_enabled(self) -> bool:
        return self._broker.auth_enabled

    # ── middleware (auth gate + CORS) ─────────────────────────────────

    def _setup_middleware(self) -> None:
        """Attach the ingress token gate + the CORS allow-list.

        Ported from ``Bridge._setup_middleware``: auth is added *before* CORS so
        CORS ends up outermost (Starlette applies the most-recently-added
        middleware first), which means a 401 still carries CORS headers and the
        browser can read it.  The broker suppresses its own HTTP auth middleware
        while the gateway is attached (the ``/register`` WS gate is separate).
        """
        if self._auth_enabled:
            self._app.add_middleware(BaseHTTPMiddleware,
                                     dispatch=self._auth_dispatch)
        self._app.add_middleware(
            CORSMiddleware,
            allow_credentials=True,
            allow_origins=CORS_ORIGINS,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @staticmethod
    def _request_token(request: Request) -> Optional[str]:
        """Bearer header first, else the ``orbit_bridge_token`` cookie (the
        browser/SSE path minted by ``POST /auth``)."""
        auth = request.headers.get('authorization', '')
        if auth.lower().startswith('bearer '):
            return auth[7:].strip()
        return request.cookies.get(utils.AUTH_COOKIE)

    async def _auth_dispatch(self, request: Request, call_next):
        """Require the shared token on every capability-bearing HTTP route.

        Exempt (ported verbatim from the bridge): CORS preflight (``OPTIONS``),
        the UI shell (``/``), and the static plugin JS (``/plugins/...``) —
        these carry no capability and must load so the Explorer can prompt for
        the token.  ``POST /auth`` is *not* exempt: it is reached only with a
        valid bearer header and then mints the cookie.  The path is normalized
        first so a traversal-style path cannot slip a gated route past the gate
        (normalizing can only ever *under*-exempt).
        """
        path = posixpath.normpath(request.url.path)
        if request.method == 'OPTIONS' \
                or path == '/' \
                or path.startswith('/plugins/'):
            return await call_next(request)
        if not utils.tokens_match(self._request_token(request), self._token):
            return JSONResponse(
                status_code=401,
                content={"error": True, "status_code": 401,
                         "detail": "missing or invalid bridge token"})
        return await call_next(request)

    # ── event subscriptions (SSE fan-out) ─────────────────────────────

    def _subscribe_events(self) -> None:
        self._untap  = self._broker.tap(self._on_event)
        self._untopo = self._broker.add_topology_listener(self._on_topology)

    def detach(self) -> None:
        """Unsubscribe the tap + topology listener (mainly for tests)."""
        if self._untap:
            self._untap()
            self._untap = None
        if self._untopo:
            self._untopo()
            self._untopo = None

    @staticmethod
    def _sse_frame(topic: str, data: Any) -> str:
        """Format one SSE frame exactly as the bridge wrote it on the wire:
        ``data: {"topic": <topic>, "data": <data>}\\n\\n`` (no keepalive
        comments — the bridge emitted none)."""
        return "data: %s\n\n" % json.dumps({"topic": topic, "data": data})

    def _on_event(self, event: Dict[str, Any]) -> None:
        """Raw event tap callback — runs on the **plugin-host loop**.

        Renders the legacy notification envelope
        ``{topic: 'notification', data: {endpoint, plugin, topic, data}}`` and
        hands it to the routing loop, which owns the per-client queues.
        """
        if not self._sse_clients:
            return
        loop = self._routing_loop
        if loop is None:
            return
        frame = self._sse_frame('notification', {
            "endpoint": event.get('src'),
            "plugin":   event.get('plugin'),
            "topic":    event.get('topic'),
            "data":     event.get('data') or {},
        })
        loop.call_soon_threadsafe(self._push_all, frame)

    def _on_topology(self) -> None:
        """Topology-change listener — runs on the **routing loop**.

        Emits the legacy topology envelope with the same ``{bridge, endpoints}``
        payload the discovery route returns (the exact shape the bridge sent).
        """
        if not self._sse_clients:
            return
        self._push_all(self._sse_frame('topology', self._discovery_snapshot()))

    def _push_all(self, frame: str) -> None:
        """Enqueue one frame onto every SSE client (routing loop only)."""
        for q in list(self._sse_clients):
            q.push(frame)

    # ── discovery snapshot (bridge-era shape) ─────────────────────────

    @staticmethod
    def _full_namespace(name: str, ns: str) -> str:
        """Present a star-model namespace the way the old API did.

        Endpoint plugins advertise **endpoint-relative** namespaces
        (``/{instance}``); the bridge-era clients + Explorer consume the *full*
        ``/{endpoint}/{instance}`` prefix.  Prefix the endpoint name unless the
        namespace is already rooted at it (the broker's own hosted-plugin
        participant advertises the full ``/broker/{instance}`` form).
        """
        prefix = '/' + name
        if ns == prefix or ns.startswith(prefix + '/'):
            return ns
        if not ns.startswith('/'):
            ns = '/' + ns
        return prefix + ns

    def _discovery_snapshot(self) -> Dict[str, Any]:
        """Build the bridge-era discovery structure from the topology snapshot.

        Shape (unchanged from the bridge)::

            {"bridge":    {"url": <broker url>},
             "endpoints": {<name>: {"endpoint": {...},
                                    "plugins":  {<pname>: {namespace, ...}}}}}

        Namespaces are normalized to the full ``/{endpoint}/{instance}`` form.
        """
        endpoints: Dict[str, Any] = {}
        for name, info in self._broker.topology_snapshot().items():
            plugins: Dict[str, Any] = {}
            for pname, pinfo in (info.get('plugins') or {}).items():
                pdata = dict(pinfo)
                pdata['namespace'] = self._full_namespace(
                    name, pdata.get('namespace') or '')
                plugins[pname] = pdata
            endpoints[name] = {
                "endpoint": {"role":     info.get('role'),
                             "liveness": info.get('liveness')},
                "plugins":  plugins,
            }
        return {"bridge":    {"url": self._broker.url},
                "endpoints": endpoints}

    # ── header hygiene (ported from Bridge._strip_headers) ────────────

    @staticmethod
    def _strip_headers(request: Request) -> Dict[str, str]:
        """Drop hop-by-hop headers + the bridge credential before forwarding.

        The ``authorization`` header and the ``orbit_bridge_token`` cookie are
        removed so the shared token is never forwarded to endpoint plugins; any
        other cookies are preserved.
        """
        headers: Dict[str, str] = {}
        for k, v in request.headers.items():
            k_lower = k.lower()
            if k_lower in _HOP_BY_HOP:
                continue
            if k_lower == "cookie":
                kept = [c.strip() for c in v.split(";")
                        if c.strip()
                        and not c.strip().startswith(f"{utils.AUTH_COOKIE}=")]
                if kept:
                    headers[k] = "; ".join(kept)
            else:
                headers[k] = v
        return headers

    # ── routes ─────────────────────────────────────────────────────────

    def _register_routes(self) -> None:
        """Attach every gateway route.  The catch-all proxy is registered LAST
        so the specific routes (auth/discovery/admin/UI) win the match."""
        self_ = self
        app   = self._app

        # ── /auth (mint the browser/SSE cookie) ──────────────────────
        # Reaching this handler means the auth middleware already validated a
        # bearer header (or an existing cookie); we then set the HttpOnly,
        # SameSite=Strict cookie the browser's EventSource rides.  No-op (still
        # 200) when auth is disabled.
        @app.post("/auth", tags=["Auth"])
        async def auth(request: Request):
            resp = JSONResponse({"ok": True})
            if self_._auth_enabled and self_._token:
                resp.set_cookie(key=utils.AUTH_COOKIE, value=self_._token,
                                httponly=True,
                                secure=request.url.scheme == "https",
                                samesite="strict", path="/")
            return resp

        # ── discovery ────────────────────────────────────────────────
        @app.post("/endpoint/list", tags=["Discovery"])
        async def endpoint_list(request: Request):
            return JSONResponse({"data": self_._discovery_snapshot()})

        @app.get("/endpoints", tags=["Discovery"])
        async def get_endpoints():
            out = []
            for name, info in self_._broker.topology_snapshot().items():
                plugins = list((info.get('plugins') or {}).keys())
                out.append({
                    "name":         name,
                    "plugins":      plugins,
                    "connected":    info.get('liveness') == 'present',
                    "plugin_count": len(plugins),
                })
            return JSONResponse({"endpoints": out, "total": len(out)})

        # ── /events (SSE) ────────────────────────────────────────────
        @app.get("/events", tags=["Events"])
        async def sse_events(request: Request):
            self_._routing_loop = asyncio.get_running_loop()
            q = _SSEQueue(self_._sse_depth)
            self_._sse_clients.add(q)
            # The bridge sends the current topology as the first SSE frame.
            q.push(self_._sse_frame('topology', self_._discovery_snapshot()))

            async def event_generator():
                try:
                    while True:
                        if await request.is_disconnected():
                            break
                        try:
                            await asyncio.wait_for(q.wake.wait(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        q.wake.clear()
                        while q.buf:
                            yield q.buf.popleft()
                except asyncio.CancelledError:
                    log.debug("[Gateway] SSE client cancelled")
                except Exception as e:
                    log.exception("[Gateway] SSE client error: %s", e)
                finally:
                    self_._sse_clients.discard(q)

            return StreamingResponse(event_generator(),
                                     media_type="text/event-stream")

        # ── admin ────────────────────────────────────────────────────
        @app.post("/endpoint/disconnect/{endpoint_name}", tags=["Management"])
        async def disconnect_endpoint(endpoint_name: str):
            if endpoint_name == BROKER_NAME:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot disconnect the broker host")
            if endpoint_name not in self_._broker.registry:
                raise HTTPException(
                    status_code=404,
                    detail=f"Endpoint '{endpoint_name}' not connected")
            await self_._broker._disconnect(endpoint_name)
            return JSONResponse({"status":   "shutdown",
                                 "endpoint": endpoint_name})

        async def _terminate():
            # Route both terminate paths through the broker's self-SIGTERM floor
            # (``_handle_control`` schedules the delayed ``os.kill``).
            await self_._broker._handle_control('gateway', {'op': 'terminate'})
            return JSONResponse({
                "status":  "terminating",
                "message": "Broker will terminate shortly. "
                           "Endpoints will not be shut down."})

        # Exact old path — the Explorer JS calls it; ``/broker/terminate`` is the
        # forward-looking alias the rename PR keeps once the old one retires.
        @app.post("/bridge/terminate", tags=["Management"])
        async def terminate_bridge():
            return await _terminate()

        @app.post("/broker/terminate", tags=["Management"])
        async def terminate_broker():
            return await _terminate()

        # ── UI: index + plugin JS modules ────────────────────────────
        @app.get("/", tags=["UI"], include_in_schema=False)
        async def root():
            html_path = self_._resource_path('orbit_explorer.html')
            if html_path:
                return FileResponse(html_path, headers=self_._no_cache())
            return Response(content="orbit_explorer.html not found",
                            status_code=404)

        @app.get("/plugins/{filename}", tags=["UI"], include_in_schema=False)
        async def serve_plugin(filename: str):
            if not re.match(r'^[a-z_][a-z0-9_.]*\.js$', filename):
                raise HTTPException(status_code=404,
                                    detail="Invalid plugin filename")
            plugin_path = self_._resource_path('plugins/%s' % filename)
            if plugin_path:
                return FileResponse(plugin_path,
                                    media_type="application/javascript",
                                    headers=self_._no_cache())
            raise HTTPException(status_code=404,
                                detail=f"Plugin '{filename}' not found")

        # ── catch-all proxy (must register LAST) ─────────────────────
        @app.api_route(
            "/{endpoint_name}/{path:path}",
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
            tags=["Proxy"],
            summary="Proxy requests to endpoint plugins")
        async def proxy(endpoint_name: str, path: str, request: Request):
            # Map the bridge-era URL ``/{endpoint}/{plugin_ns}/...`` onto the
            # star model: dst = endpoint, path = the endpoint-relative remainder.
            forward_path = '/' + path
            if request.url.query:
                forward_path += '?' + str(request.url.query)

            body    = await request.body()
            headers = self_._strip_headers(request)

            # dst == the broker's own hosted-plugin participant: route into the
            # plugin host (the same host-loop crossing ``_dispatch_to_host``
            # uses) instead of the routing-loop registry, which 404s for
            # ``dst=='broker'`` (pre-flip item 1: HTTP reach to hosted plugins).
            if endpoint_name == BROKER_NAME:
                resp = await self_._broker.call_host(
                    request.method, forward_path, headers=headers, body=body)
                status  = int(resp.get('status', 502))
                rheaders = resp.get('headers') or {}
                rbody   = resp.get('body') or b''
                if isinstance(rbody, str):
                    rbody = rbody.encode()
                return Response(content=rbody, status_code=status,
                                headers=dict(rheaders))

            try:
                resp = await self_._broker.caller.call(
                    endpoint_name, request.method, forward_path,
                    body=body, headers=headers,
                    timeout=self_._request_timeout)
            except asyncio.TimeoutError as exc:
                raise HTTPException(
                    status_code=504,
                    detail="Upstream (endpoint) timeout") from exc
            except RuntimeError as exc:
                # Unroutable dst (unknown endpoint) mirrors the bridge's 404;
                # a full pending table is a 503.
                if 'unknown' in str(exc):
                    raise HTTPException(
                        status_code=404,
                        detail=f"Endpoint '{endpoint_name}' unknown") from exc
                raise HTTPException(status_code=503, detail=str(exc)) from exc

            status  = int(resp.get('status', 502))
            rheaders = resp.get('headers') or {}
            rbody   = resp.get('body') or b''
            if isinstance(rbody, str):
                rbody = rbody.encode()
            return Response(content=rbody, status_code=status,
                            headers=dict(rheaders))

    # ── static-asset helpers (ported from the bridge) ─────────────────

    @staticmethod
    def _no_cache() -> Dict[str, str]:
        return {"Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma":        "no-cache",
                "Expires":       "0"}

    @staticmethod
    def _resource_path(rel: str) -> Optional[str]:
        """Resolve a packaged ``data/<rel>`` file path (importlib.resources with
        a pkg_resources fallback), or ``None`` if it does not exist."""
        try:
            from importlib.resources import files
            candidate = files('radical.orbit').joinpath('data').joinpath(rel)
            path = (os.fspath(candidate)                    # type: ignore[arg-type]
                    if hasattr(candidate, '__fspath__') else str(candidate))
            if os.path.exists(path):
                return path
        except Exception as e:
            log.debug("[Gateway] importlib.resources lookup failed: %s", e)

        try:
            import pkg_resources                            # type: ignore[import]
            path = pkg_resources.resource_filename(
                'radical.orbit', 'data/%s' % rel)
            if os.path.exists(path):
                return path
        except Exception as e:
            log.debug("[Gateway] pkg_resources lookup failed: %s", e)

        return None
