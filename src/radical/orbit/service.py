
import asyncio
import base64
import json
import logging
import os
import random
import ssl
import socket
import threading
from typing import Any, Dict, Optional

import urllib.parse

import msgpack
import websockets
from websockets import exceptions as ws_exc

from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse

from . import _prof as rprof
import radical.orbit.logging_config  # noqa: F401 # pylint: disable=unused-import

from radical.orbit.plugin_base      import Plugin
from radical.orbit.plugin_host_base import PluginHostBase
from radical.orbit.models import (
    RequestMessage, PingMessage, ErrorMessage, ShutdownMessage, TopologyMessage,
    ResponseMessage, NotificationMessage, RegisterMessage,
    parse_bridge_message
)
from radical.orbit.ui_schema import ui_config_to_dict

log = logging.getLogger("radical.orbit.endpoint")


# ---------------------------------------------------------------------------
# RequestShim — lightweight stand-in for starlette.requests.Request
# ---------------------------------------------------------------------------

class RequestShim:
    """Lightweight adapter for starlette ``Request``.

    Provides the three interfaces that every plugin handler uses:
    ``path_params``, ``query_params``, and ``await .json()`` / ``await .body()``.
    Encoding-agnostic: stores raw bytes, decodes lazily based on content_type.
    """

    def __init__(self, path_params : dict,
                       query_params: dict,
                       body_bytes  : bytes,
                       content_type: str = 'application/json'):
        self.path_params  = path_params
        self.query_params = query_params
        self.content_type = content_type
        self._body        = body_bytes
        self._decoded     = None

    async def body(self) -> bytes:
        """Raw body bytes (matches ``Request.body()``)."""
        return self._body

    async def json(self) -> dict:
        """Parse body into a Python dict (matches ``Request.json()``).

        Content-type-aware: JSON or msgpack based on Content-Type header.
        """
        if self._decoded is not None:
            return self._decoded

        ct = self.content_type or 'application/json'
        if 'msgpack' in ct:
            self._decoded = msgpack.unpackb(self._body, raw=False)
        else:
            self._decoded = json.loads(self._body) if self._body else {}
        return self._decoded


# Re-export for backward compatibility (bridge_plugin_host.py, tests, etc.)
from radical.orbit.plugin_host_base import _resolve_plugin_names  # noqa: F401


class EndpointService(PluginHostBase):
    """
    Embedded ORBIT Service.

    This class runs the Endpoint Service logic within an application, supporting both
    asyncio-based and synchronous applications. It manages the connection to the
    Bridge and hosts the local plugin execution environment.

    The service automatically loads the 'sysinfo' plugin to provide system
    metrics.

    Attributes:
        app (FastAPI): The internal FastAPI application hosting the plugins.
    """

    def __init__(self, bridge_url: Optional[str] = None,
                 cert:       Optional[str]      = None,
                 name:       Optional[str]      = None,
                 plugins:    Optional[list]     = None,
                 tunnel:     str                = 'none',
                 tunnel_via: Optional[str]      = None,
                 token:      Optional[str]      = None,
                 app:        Optional[FastAPI]  = None):
        """
        Initialize the Endpoint Service.

        Args:
            bridge_url: WebSocket URL for the Bridge.  CLI > env
                        (``RADICAL_ORBIT_BRIDGE_URL``) > file
                        (``~/.radical/orbit/bridge.url``).
            cert: Path to the bridge's TLS cert.  Same precedence using
                  ``RADICAL_ORBIT_BRIDGE_CERT`` and
                  ``~/.radical/orbit/bridge_cert.pem``.
            name: Endpoint service name for identification.  Defaults to
                  hostname.
            tunnel: SSH tunnel mode for the bridge connection.  One of:

                    * ``'none'``    — connect directly.
                    * ``'forward'`` — open an outbound ``ssh -L``
                                      to the login host (compute → login).
                                      Requires *tunnel_via* (or one of
                                      the env-var fallbacks).
                    * ``'reverse'`` — wait for the parent (login-side
                                      ``plugin_psij`` watcher) to
                                      open ``ssh -R`` and write the
                                      rendezvous file.  No SSH spawn
                                      from this side.

                    Boolean values are *not* accepted.
            tunnel_via: Explicit login host for ``forward`` mode.  If
                    unset, falls back to ``PBS_O_HOST`` (PBSPro) or
                    ``SLURM_SUBMIT_HOST`` (SLURM).  Ignored in
                    ``reverse`` / ``none`` modes.
            app: existing ``FastAPI`` instance to register plugin
                 routes on.  When ``None`` the endpoint constructs its own.
        """
        if tunnel not in ('none', 'forward', 'reverse'):
            raise ValueError(
                f"tunnel must be one of 'none' / 'forward' / 'reverse'; "
                f"got {tunnel!r}")
        from urllib.parse import urlparse
        from . import utils
        # Resolve bridge URL + cert via the shared helper.  No
        # side-effect on the local URL file: a one-off ``RADICAL_ORBIT_BRIDGE_URL``
        # to point at a different bridge must not clobber the file
        # the operator may rely on for the *default* bridge.
        resolved_url, _ = utils.resolve_bridge_url(cli=bridge_url)
        self._bridge_url: str = resolved_url

        # Cert is required for TLS schemes (https/wss) and ignored for
        # plain (http/ws) — mirrors BridgeClient.  Test setups that
        # don't exercise the TLS handshake use the plain forms.
        scheme = urlparse(self._bridge_url).scheme
        if scheme in ('https', 'wss'):
            resolved_cert, _    = utils.resolve_bridge_cert(cli=cert)
            self._cert: Optional[str] = str(resolved_cert)
        else:
            self._cert = None

        # Shared ingress auth token, sent in the register frame.  None is fine
        # when the bridge runs with auth disabled.
        self._token: Optional[str] = utils.resolve_bridge_token(cli=token)[0]

        self._app: FastAPI = app if app is not None \
                                 else FastAPI(title="Embedded Endpoint Service")
        self._app.state.bridge_url = self._bridge_url

        self._plugins: Dict[str, Plugin] = {}
        self._name: str = name or socket.gethostname()
        self._plugin_filter: list = plugins or ['all']
        self._app.state.endpoint_name = self._name
        self._app.state.endpoint_service = self
        self._app.state.is_bridge    = False
        self._tunnel: str = tunnel
        self._tunnel_via: Optional[str] = tunnel_via
        self._tunnel_proc = None     # subprocess.Popen of active SSH tunnel
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._send_lock: asyncio.Lock = asyncio.Lock()
        self._stop_event: asyncio.Event = asyncio.Event()
        self._running_task: Optional[asyncio.Task] = None
        self._thread: Optional[threading.Thread] = None
        self._direct_routes: list = []
        self._prof = rprof.Profiler('endpoint', ns='radical.orbit')

        self._load_plugins_from_filter(self._plugin_filter)

        # Reference the live list — not a copy — so dynamically registered
        # plugin routes are visible immediately.
        self._direct_routes = getattr(self._app.state, 'direct_routes', [])


    @property
    def bridge_url(self):
        """Get the current Bridge URL."""
        return self._bridge_url

    # -- direct dispatch ------------------------------------------------------

    def _match_route(self, method: str, path: str):
        """Match *method* + *path* against the direct-dispatch route table.

        Returns ``(handler, path_params)`` or ``(None, None)``.
        """
        for rt_method, pattern, param_names, handler in self._direct_routes:
            if rt_method == method:
                m = pattern.match(path)
                if m:
                    return handler, dict(zip(param_names, m.groups()))
        return None, None

    @staticmethod
    def _error_response(req_id: str, exc: Exception) -> ResponseMessage:
        """Build a ``ResponseMessage`` from an exception."""
        if isinstance(exc, HTTPException):
            body   = json.dumps({"detail": exc.detail})
            status = exc.status_code
        else:
            body   = json.dumps({"error": "endpoint-invoke-failed",
                                 "detail": str(exc)})
            status = 502
        return ResponseMessage(
            req_id=req_id, status=status,
            headers={"content-type": "application/json"},
            is_binary=False, body=body)

    async def _handle_request(self, msg: RequestMessage) -> None:
        """Dispatch a bridge-forwarded request directly to the plugin handler.

        Bypasses the ASGI/FastAPI stack entirely — route matching and request
        parsing are handled inline via ``_match_route`` and ``RequestShim``.
        """
        req_id = msg.req_id
        prof   = self._prof
        try:
            prof.prof('endpoint_recv', uid=req_id,
                      msg='%s %s' % (msg.method, msg.path))

            log.debug("[Endpoint] [req:%s] Handling %s %s", req_id, msg.method, msg.path)

            # Split query string from path
            if '?' in msg.path:
                path, qs = msg.path.split('?', 1)
                query_params = dict(urllib.parse.parse_qsl(qs))
            else:
                path         = msg.path
                query_params = {}

            # Match route
            prof.prof('endpoint_route', uid=req_id)
            handler, path_params = self._match_route(msg.method, path)
            if handler is None:
                log.error("[Endpoint] [req:%s] No route for %s %s",
                          req_id, msg.method, path)
                response = ResponseMessage(
                    req_id=req_id, status=404,
                    headers={"content-type": "application/json"},
                    is_binary=False,
                    body=json.dumps(
                        {"detail": f"No route: {msg.method} {path}"}))
                async with self._send_lock:
                    if self._ws:
                        await self._ws.send(response.model_dump_json())
                return

            # Build RequestShim
            prof.prof('endpoint_shim', uid=req_id)
            if isinstance(msg.body, bytes):
                body_bytes = msg.body                    # binary WS frame
            elif msg.is_binary and msg.body:
                body_bytes = base64.b64decode(msg.body)  # base64 fallback
            elif msg.body:
                body_bytes = msg.body.encode('utf-8')
            else:
                body_bytes = b''

            content_type = (msg.headers or {}).get(
                'content-type', 'application/json')
            shim = RequestShim(path_params, query_params,
                               body_bytes, content_type)

            # Dispatch to handler
            prof.prof('endpoint_handler', uid=req_id)
            try:
                result = await handler(shim)
            except HTTPException as e:
                result = JSONResponse({"detail": e.detail},
                                      status_code=e.status_code)
            except Exception as e:
                log.exception("[Endpoint] [req:%s] Handler error", req_id)
                result = JSONResponse(
                    {"error": "endpoint-invoke-failed", "detail": str(e)},
                    status_code=500)
            prof.prof('endpoint_handler_done', uid=req_id)

            # Build response — handlers return plain dicts/lists (fast
            # path) or JSONResponse (error path).
            #
            # Fast path: serialize body with json.dumps, then build the
            # WS frame manually so the body JSON is embedded verbatim
            # (avoids Pydantic model_dump_json double-encoding the body
            # string as an escaped JSON value).
            prof.prof('endpoint_body_ser', uid=req_id)
            if not hasattr(result, 'status_code'):
                resp_body = json.dumps(result)
                status    = 200
                headers   = {"content-type": "application/json"}
            else:
                resp_body = result.body.decode('utf-8')
                status    = result.status_code
                headers   = dict(result.headers)
            prof.prof('endpoint_body_ser_done', uid=req_id,
                      msg=str(len(resp_body)))

            log.debug("[Endpoint] [req:%s] Response status=%d",
                      req_id, status)

            # Manual JSON construction — body is already a JSON string,
            # embed it directly to avoid re-serialization.
            prof.prof('endpoint_resp_ser', uid=req_id)
            hdr_json  = json.dumps(headers)
            resp_text = (
                '{"type":"response"'
                ',"req_id":' + json.dumps(req_id) +
                ',"status":' + str(status) +
                ',"headers":' + hdr_json +
                ',"body":' + resp_body +
                ',"is_binary":false}')
            prof.prof('endpoint_resp_ser_done', uid=req_id,
                      msg=str(len(resp_text)))

            prof.prof('endpoint_ws_send', uid=req_id)
            async with self._send_lock:
                if self._ws:
                    await self._ws.send(resp_text)
            prof.prof('endpoint_ws_sent', uid=req_id, state=str(status))

        except Exception as e:
            log.exception("[Endpoint] [req:%s] Error handling request", req_id)
            response = self._error_response(req_id, e)
            async with self._send_lock:
                if self._ws:
                    await self._ws.send(response.model_dump_json())

    async def _handle_topology(self, msg: TopologyMessage) -> None:
        """
        Handle topology update from bridge (endpoint connect/disconnect).

        Args:
            msg: Validated topology message from bridge.
        """
        log.debug("[Endpoint] Topology update: %d endpoints", len(msg.endpoints))

        # Notify all plugins about the topology change
        for pname, plugin in self._plugins.items():
            try:
                if hasattr(plugin, 'on_topology_change'):
                    await plugin.on_topology_change(msg.endpoints)
            except Exception as e:
                log.warning("[Endpoint] Plugin %s topology handler failed: %s", pname, e)

    # -- topology announcement (PluginHostBase contract) -----------------------

    async def _announce_topology(self) -> None:
        """Send a topology message to the bridge over WebSocket.

        Called by ``register_dynamic_plugin`` / ``deregister_dynamic_plugin``
        after plugin set changes at runtime.
        """
        if not self._ws:
            log.warning("[Endpoint] Cannot announce topology, not connected")
            return

        plugins_data = {}
        for pname, plugin in self._plugins.items():
            plugins_data[pname] = {
                'type'     : pname,
                'namespace': f'/{self._name}{plugin.namespace}',
                'version'  : getattr(plugin, 'version', '0.0.1'),
                'enabled'  : True,
                'ui_config': ui_config_to_dict(
                    getattr(plugin, 'ui_config', None)),
            }

        msg = json.dumps({
            'type' : 'topology',
            'endpoints': {self._name: {'plugins': plugins_data}},
        })
        async with self._send_lock:
            try:
                await self._ws.send(msg)
                log.info("[Endpoint] Sent topology (%d plugins)",
                         len(plugins_data))
            except Exception as exc:
                log.warning("[Endpoint] Failed to send topology: %s", exc)

    # -- notifications --------------------------------------------------------

    async def send_notification(self, plugin_name: str, topic: str, data: Dict[str, Any]) -> None:
        """
        Send an unsolicited notification to the bridge to broadcast to UI clients.

        Args:
            plugin_name: Name of the plugin sending the notification.
            topic: Notification topic (e.g., "task_status", "job_status").
            data: Notification payload data.
        """
        if not self._ws:
            log.warning("[Endpoint] Cannot send notification, not connected")
            return

        notification = NotificationMessage(
            endpoint=self._name,
            plugin=plugin_name,
            topic=topic,
            data=data
        )

        async with self._send_lock:
            try:
                await self._ws.send(notification.model_dump_json())
                log.debug("[Endpoint] Sent notification: %s/%s", plugin_name, topic)
            except Exception as e:
                log.warning("[Endpoint] Failed to send notification: %s", e)

    @staticmethod
    def _classify_cert_error(verify_message: str, cert_pinned: bool,
                             check_hostname: bool) -> str:
        """Decide how to react to a TLS certificate verification failure.

        Returns ``'relax'`` (disable name validation and retry) or
        ``'abort'`` (fail hard).

        A hostname / IP-address mismatch is benign *only* when an explicit
        certificate has been pinned (``--cert``): ``CERT_REQUIRED`` then
        already guarantees the peer presents exactly that certificate, so the
        name check is redundant and relaxing it — with a loud warning — is a
        reasonable development convenience.  Without a pinned cert we trust the
        system store, where disabling the name check is a real downgrade; that
        case, and every other cert failure (expired, untrusted issuer,
        self-signed-not-pinned, …), aborts — reconnecting cannot recover from
        a bad certificate.
        """
        msg = (verify_message or '').lower()
        name_mismatch = 'hostname' in msg or 'ip address' in msg
        if check_hostname and cert_pinned and name_mismatch:
            return 'relax'
        return 'abort'

    async def run(self) -> None:
        """
        Main async entry point.
        Connects to Bridge and starts processing loop.
        """
        PING_INTERVAL  = 20
        PING_TIMEOUT   = 600    # 10 min: tolerate long blocking ops
                                # (e.g. dragon V3 Batch init across many
                                # nodes) without dropping the WS to bridge.
        MAX_BACKOFF    = 10
        JITTER_FACTOR  = 0.3  # Add up to 30% jitter to prevent thundering herd
        BACKOFF_FACTOR = 1.2
        backoff = 0.5

        self._stop_event.clear()
        self._running_task = asyncio.current_task()

        # ── Bridge connection: optional SSH tunnel ──────────────────────────
        # ``self._tunnel`` is one of:
        #   'none'    — connect directly.
        #   'forward' — open ssh -L from this (compute) node to the login
        #               host ourselves; rewrite bridge URL to localhost:<port>.
        #               Used where compute→login SSH works (Aurora,
        #               Perlmutter).
        #   'reverse' — wait for the parent (login-side) plugin_psij watcher
        #               to open ssh -R and write the rendezvous file with
        #               the remote port allocated by the compute-side sshd.
        #               Used where login→compute SSH works but the reverse
        #               direction is blocked (Odo).
        if self._tunnel == 'forward':
            await self._open_tunnel_forward()
        elif self._tunnel == 'reverse':
            await self._open_tunnel_reverse()
        # 'none' → fall through, use bridge URL as-is.
        # ── End tunnel setup ──────────────────────────────────────────────────

        ssl_check_hostname = True
        while not self._stop_event.is_set():
            try:
                # For the ws connect, we change http(s) to ws(s)
                if self._bridge_url.startswith("https://"):
                    ws_url = "wss://" + self._bridge_url[len("https://"):]
                elif self._bridge_url.startswith("http://"):
                    ws_url = "ws://" + self._bridge_url[len("http://"):]
                else:
                    ws_url = self._bridge_url

                # remove trailing slashes
                ws_url = ws_url.rstrip("/")
                if not ws_url.endswith("/register"):
                    ws_url += "/register"

                # Determine if we need SSL
                ssl_ctx = None
                if ws_url.startswith("wss://"):
                    ssl_ctx = ssl.create_default_context()
                    ssl_ctx.check_hostname = ssl_check_hostname
                    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
                    # Cert path was already resolved + validated in __init__.
                    if self._cert and os.path.exists(self._cert):
                        ssl_ctx.load_verify_locations(self._cert)

                async with websockets.connect(ws_url,
                                              ssl=ssl_ctx,
                                              ping_interval=PING_INTERVAL,
                                              ping_timeout=PING_TIMEOUT,
                                              close_timeout=2,
                                              max_size=10 * 1024 * 1024,
                                              compression='deflate',
                                              ) as ws:

                    self._ws = ws
                    log.info("[Endpoint] Connected to %s", self._bridge_url)
                    backoff = 0.5  # Reset backoff on success

                    # Register endpoint + all plugins in a single message
                    async with self._send_lock:
                        plugins_data = {}
                        for pname, plugin in self._plugins.items():
                            ui_module_content = None
                            ui_module_path = getattr(plugin.__class__, 'ui_module', None)
                            if ui_module_path and os.path.isfile(ui_module_path):
                                try:
                                    with open(ui_module_path, encoding='utf-8') as f:
                                        ui_module_content = f.read()
                                except Exception:
                                    log.warning("[Endpoint] Could not read ui_module for %s: %s",
                                                pname, ui_module_path)
                            plugins_data[pname] = {
                                "type": pname,
                                "namespace": f"/{self._name}{plugin.namespace}",
                                "version": getattr(plugin, 'version', '0.0.1'),
                                "enabled": True,
                                "ui_config": ui_config_to_dict(
                                    getattr(plugin, 'ui_config', None)
                                ),
                                "ui_module": ui_module_content,
                            }

                        reg = RegisterMessage(
                            endpoint_name=self._name,
                            endpoint={"type": "radical.orbit"},
                            plugins=plugins_data,
                            token=self._token,
                        )
                        await ws.send(reg.model_dump_json())

                    # Processing Loop — use asyncio.wait so the loop wakes
                    # immediately on either a new message or stop signal,
                    # eliminating the 1-second idle timeout overhead.
                    _recv_task = asyncio.ensure_future(ws.recv())
                    _stop_fut  = asyncio.ensure_future(self._stop_event.wait())
                    try:
                        while not self._stop_event.is_set():
                            done, _ = await asyncio.wait(
                                {_recv_task, _stop_fut},
                                return_when=asyncio.FIRST_COMPLETED)

                            if _stop_fut in done:
                                _recv_task.cancel()
                                break

                            # _recv_task completed — retrieve result
                            try:
                                raw_msg = _recv_task.result()
                            except websockets.exceptions.ConnectionClosed:
                                if self._stop_event.is_set():
                                    _stop_fut.cancel()
                                    break
                                log.info("[Endpoint] Connection closed")
                                _stop_fut.cancel()
                                raise  # Reconnect

                            # Arm next recv immediately
                            _recv_task = asyncio.ensure_future(ws.recv())

                            # Binary WS frame → msgpack; text → JSON
                            self._prof.prof('endpoint_deser',
                                msg='%s:%d' % (
                                    'msgpack' if isinstance(raw_msg, bytes)
                                              else 'json',
                                    len(raw_msg)))
                            if isinstance(raw_msg, bytes):
                                data = msgpack.unpackb(raw_msg, raw=False)
                            else:
                                data = json.loads(raw_msg)
                            self._prof.prof('endpoint_deser_done',
                                            uid=data.get('req_id', ''))

                            self._prof.prof('endpoint_parse',
                                            uid=data.get('req_id', ''))
                            try:
                                msg = parse_bridge_message(data)
                            except ValueError as ve:
                                log.warning("[Endpoint] Invalid message: %s", ve)
                                continue
                            self._prof.prof('endpoint_parse_done',
                                            uid=data.get('req_id', ''))

                            if isinstance(msg, ErrorMessage):
                                log.error("[Endpoint] Registration error: %s", msg.message)
                                self._stop_event.set()
                                _recv_task.cancel()
                                _stop_fut.cancel()
                                return  # Fatal error, stop

                            if isinstance(msg, PingMessage):
                                async with self._send_lock:
                                    await ws.send('{"type": "pong"}')
                                continue

                            if isinstance(msg, ShutdownMessage):
                                log.info("[Endpoint] Shutdown requested: %s", msg.reason)
                                self._stop_event.set()
                                _recv_task.cancel()
                                _stop_fut.cancel()
                                return

                            if isinstance(msg, RequestMessage):
                                asyncio.create_task(self._handle_request(msg))

                            if isinstance(msg, TopologyMessage):
                                asyncio.create_task(self._handle_topology(msg))
                    finally:
                        _recv_task.cancel()
                        _stop_fut.cancel()
                        await asyncio.gather(
                            _recv_task, _stop_fut,
                            return_exceptions=True)

            except ssl.SSLCertVerificationError as e:
                verify_msg  = getattr(e, 'verify_message', str(e))
                cert_pinned = bool(self._cert and os.path.exists(self._cert))
                if self._classify_cert_error(
                        verify_msg, cert_pinned, ssl_check_hostname) == 'relax':
                    log.warning("[Endpoint] TLS name/IP validation failed for "
                                "%s: %s. Pinned cert present — continuing with "
                                "name validation DISABLED (development mode).",
                                self._bridge_url, e)
                    ssl_check_hostname = False
                    continue
                if self._stop_event.is_set():
                    break

                # A bad / untrusted certificate is permanent — reconnecting
                # cannot recover from it, so abort with a clear error instead
                # of looping.  This propagates out of run() to the entrypoint,
                # which logs and exits non-zero.
                log.error("[Endpoint] TLS certificate verification failed for "
                          "%s: %s. Aborting.", self._bridge_url, e)
                raise

            except (ws_exc.ConnectionClosed, OSError) as e:
                if self._stop_event.is_set():
                    break  # no reconnect

                # Add jitter to backoff to prevent thundering herd
                jitter = backoff * JITTER_FACTOR * random.random()
                sleep_time = backoff + jitter
                log.warning("[Endpoint] Connection lost: %s. Reconnecting in %.1fs...",
                            e, sleep_time)
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)

            except Exception as e:
                # Fatal errors set the stop event, so check that first
                if self._stop_event.is_set():
                    break

                log.exception("[Endpoint] Unexpected error: %s", e)
                jitter = 2 * JITTER_FACTOR * random.random()
                await asyncio.sleep(2 + jitter)

    def stop(self):
        """Signal the service to stop."""
        self._prof.close()
        self._stop_event.set()
        if self._running_task:
            self._running_task.cancel()
        if self._tunnel_proc is not None:
            from . import tunnel as _tunnel
            _tunnel.cleanup_tunnel(self._tunnel_proc, self._name)
            self._tunnel_proc = None

    async def _open_tunnel_forward(self) -> None:
        """Forward mode: open an outbound SSH tunnel to the login host.

        Derives the login host from ``tunnel_via``, then ``PBS_O_HOST``,
        then ``SLURM_SUBMIT_HOST``.  Spawns the SSH process, parses the
        allocated port, and rewrites ``self._bridge_url`` to route through
        ``localhost:<port>``.
        """
        from urllib.parse import urlparse, urlunparse
        from . import tunnel as _tunnel

        login_host = (self._tunnel_via
                      or os.environ.get('PBS_O_HOST')
                      or os.environ.get('SLURM_SUBMIT_HOST'))
        if not login_host:
            raise RuntimeError(
                "--tunnel forward: no login host available. Pass "
                "--tunnel-via HOST or set PBS_O_HOST / SLURM_SUBMIT_HOST.")

        parsed      = urlparse(self._bridge_url)
        bridge_host = parsed.hostname or 'localhost'
        bridge_port = parsed.port or (443 if parsed.scheme == 'https' else 8000)

        log.info("[Endpoint] --tunnel forward: opening ssh -L via %s to %s:%d",
                 login_host, bridge_host, bridge_port)

        proc, port = await asyncio.to_thread(
            _tunnel.spawn_tunnel,
            login_host, bridge_host, bridge_port, self._name)
        self._tunnel_proc = proc

        self._bridge_url = urlunparse(
            parsed._replace(netloc=f'localhost:{port}'))
        log.info("[Endpoint] Tunnel active on localhost:%d; bridge URL now %s",
                 port, self._bridge_url)

    async def _open_tunnel_reverse(self,
                                    wait_timeout: float = 15.0) -> None:
        """Reverse mode: wait for a login-side spawner to open ``ssh -R``
        and write the rendezvous file.

        We always drop a ``<endpoint_name>.req`` JSON file first with our
        ``socket.gethostname()`` (the compute node SLURM placed us on)
        and the bridge target.  Two flows consume it:

        * **PsiJ-launched** — the parent endpoint's ``plugin_psij`` watcher
          reads ``.req`` to discover which compute node to ssh into,
          spawns ``ssh -R``, writes ``<endpoint_name>.port``.  Gating the
          spawn on ``.req`` (rather than on SLURM's RUNNING transition)
          avoids picking a wrong host on multi-node allocations where
          the script's node != ``scheduler.job_nodes()[0]``.

        * **IRI-launched** — no parent endpoint on the login node; a
          standalone helper (``bin/radical-orbit-iri-tunnel-helper.sh``)
          watches the relay dir for ``.req`` files and does the
          spawn+write.

        Either way we poll for ``<endpoint_name>.port`` and rewrite
        ``self._bridge_url`` to ``localhost:<port>`` once it appears.

        ``wait_timeout`` is the ssh-handshake budget after ``.req`` has
        been written — short on purpose, because Dragon startup is
        already past us by this point.  Tunnel setup is much more
        predictable than the work that got the child to this line.
        """
        from urllib.parse import urlparse, urlunparse
        from . import tunnel as _tunnel

        parsed      = urlparse(self._bridge_url)
        bridge_host = parsed.hostname or 'localhost'
        bridge_port = parsed.port or (443 if parsed.scheme in ('https', 'wss') else 8000)

        rdir       = _tunnel.relay_dir()
        relay_file = rdir / f'{self._name}.port'
        req_file   = rdir / f'{self._name}.req'

        # Drop the request file *before* polling — atomic via tmp + rename
        # so the helper script never reads a half-written payload.
        req_payload = json.dumps({
            'endpoint_name'  : self._name,
            'hostname'   : socket.gethostname(),
            'bridge_host': bridge_host,
            'bridge_port': bridge_port,
        })
        tmp = req_file.with_suffix('.req.tmp')
        tmp.write_text(req_payload)
        tmp.rename(req_file)
        log.info("[Endpoint] --tunnel reverse: wrote request file %s", req_file)

        log.info("[Endpoint] --tunnel reverse: waiting for parent-side "
                 "rendezvous file %s (timeout %.0fs)",
                 relay_file, wait_timeout)

        deadline = asyncio.get_running_loop().time() + wait_timeout
        while asyncio.get_running_loop().time() < deadline:
            # NFSv3 negative-lookup cache hides the parent's freshly-
            # written .port file from our `relay_file.exists()` checks
            # for tens of seconds.  An os.listdir on the parent dir
            # triggers a readdir RPC which forces fresh directory
            # attributes; on Linux NFS clients this invalidates the
            # cached ENOENT, so we see the parent's write within one
            # polling iteration (2s) instead of waiting for the
            # client's acregmin (30-60s) to expire.  Same trick is
            # applied parent-side for .req — see plugin_psij.py.
            try:
                dir_contents = set(os.listdir(str(relay_file.parent)))
            except OSError:
                dir_contents = set()
            if relay_file.name in dir_contents:
                try:
                    port = int(relay_file.read_text().strip())
                except (ValueError, OSError):
                    port = None
                if port:
                    self._bridge_url = urlunparse(
                        parsed._replace(netloc=f'localhost:{port}'))
                    log.info("[Endpoint] Reverse tunnel active on localhost:%d; "
                             "bridge URL now %s", port, self._bridge_url)
                    return
            await asyncio.sleep(2.0)

        raise RuntimeError(
            f"--tunnel reverse: rendezvous file {relay_file} did not "
            f"appear within {wait_timeout:.0f}s (parent-side ssh -R failed?)")



    def start_background(self):
        """Start the service in a separate daemon thread (for sync apps)."""
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Service already running in background")

        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()

    def _run_thread(self):
        """Entry point for background thread."""
        try:
            asyncio.run(self.run())
        except asyncio.CancelledError:
            log.info("[Endpoint] Background service cancelled")
        except Exception as e:
            log.exception("[Endpoint] Background thread failed: %s", e)
