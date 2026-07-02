"""ORBIT endpoint (participant) runtime — the single node abstraction.

One :class:`EndpointRuntime` dials the broker over a **single outbound
WebSocket** and may *serve* plugins, *consume* other participants' plugins, or
both — the workflow-manager-in-an-allocation case the star is built for.  Zero
served plugins ⇒ a pure consumer; a served plugin that only pushes events ⇒ a
callback plugin.

**Transport isolation (load-bearing — this is why liveness is fast).**

* A dedicated **transport thread** runs its own asyncio loop and owns the WS:
  connect / reconnect / backoff, the ``register`` handshake, WS keepalive, and
  raw frame ``send``/``recv`` — *nothing else*.  ``websockets`` client keepalive
  (``ping_interval``/``ping_timeout``) answers pings here regardless of what the
  work loop is doing, so a blocking plugin handler stalls request *handling*,
  never liveness (M0 lesson 3: the stock library keepalive is genuinely
  transport-independent).
* A **work loop** (its own runtime-owned thread) does envelope pack/parse and
  all plugin/handler + consumer-call work.
* User callbacks fire on a **dedicated callback-dispatcher thread** — never the
  transport or work loop (a slow user callback is user code and can never be
  "disciplined").

Frames cross the two loops through the M0 coalesced handoff
(:class:`radical.orbit.runtime_support.Handoff`): a bounded deque with a single
outstanding ``call_soon_threadsafe`` per burst, swap-drained.  The inbound path
is soft-bounded and **never blocks the transport loop** (M0 lesson 5); strict
request backpressure is the 503 fast-fail at the concurrency cap.

**Naming / session recovery.**  A zero-plugin consumer with no name given is
auto-named ``consumer.<uuid8>`` — fine for fire-and-forget consumers.  Session
reattach needs a *stable, client-supplied* name: reattach is owner-checked, so
an auto-named restart cannot recover its sessions by design.  A ``name-in-use``
register is retried with capped exponential backoff until the stale
registration passes ``lost`` — a crashed predecessor holds no resume key, and
under the fast keepalive the name frees in seconds — so "restart with the same
name → sessions come back" is automatic from the application's view.
"""

# pylint: disable=protected-access

import asyncio
import json as _json
import logging
import os
import random
import socket
import ssl
import threading
import uuid

from collections import deque
from typing      import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qsl, urlparse, urlunparse

import websockets
from websockets import exceptions as ws_exc

from fastapi             import FastAPI, HTTPException

from . import _prof as rprof
from . import protocol
from . import utils

from .client            import PluginClient
from .dispatch          import RequestShim, match_route
from .plugin_base       import Plugin
from .plugin_host_base  import PluginHostBase
from .runtime_client    import RuntimePluginClient, RuntimeResponse, _RuntimeHTTP
from .runtime_support   import CallbackDispatcher, Handoff
from .ui_schema         import ui_config_to_dict


log = logging.getLogger("radical.orbit.runtime")


# ---------------------------------------------------------------------------
# EndpointRuntime
# ---------------------------------------------------------------------------

class EndpointRuntime(PluginHostBase):
    """A participant: serve and/or consume over one outbound WS to the broker.

    Args:
        broker_url:  Broker URL.  CLI > env (``RADICAL_ORBIT_BRIDGE_URL``) >
                     file — resolved via :func:`utils.resolve_bridge_url`.
        cert:        Broker TLS cert (only for ``https``/``wss``).
        name:        Stable participant name.  ``None`` → ``consumer.<uuid8>``
                     (auto-named consumers cannot recover sessions on restart).
        plugins:     Served-plugin filter list (names / ``'all'`` / ``'default'``)
                     mounted pre-connect; ``None`` → pure consumer unless
                     :meth:`serve` is called.
        role:        Advertised role.  Defaults to ``'consumer'`` (no served
                     plugins) or ``'endpoint'``.
        tunnel:      ``'none'`` / ``'forward'`` / ``'reverse'`` (see
                     ``service.py``; boolean is not accepted).
        request_concurrency: max concurrently-served requests (work loop).
        accept_queue:        extra admitted-but-waiting requests before 503.
        callback_queue:      bounded depth of the user-callback dispatcher.
        ping_interval/ping_timeout: ``websockets`` client keepalive.
        frame_cap:   ``max_size`` for the WS + the pack/parse cap.
    """

    def __init__(self,
                 broker_url:  Optional[str]  = None,
                 cert:        Optional[str]  = None,
                 name:        Optional[str]  = None,
                 plugins:     Optional[list] = None,
                 role:        Optional[str]  = None,
                 tunnel:      str            = 'none',
                 tunnel_via:  Optional[str]  = None,
                 token:       Optional[str]  = None,
                 resume_key:  Optional[str]  = None,
                 app:         Optional[FastAPI] = None,
                 request_concurrency: int   = 64,
                 accept_queue:        int   = 64,
                 callback_queue:      int   = 1024,
                 call_timeout:        float = 600.0,
                 retry_after:         float = 1.0,
                 ping_interval:       float = 1.0,
                 ping_timeout:        float = 3.0,
                 frame_cap:           int   = protocol.FRAME_CAP,
                 backoff_start:       float = 0.5,
                 backoff_max:         float = 10.0,
                 backoff_factor:      float = 1.5):

        if tunnel not in ('none', 'forward', 'reverse'):
            raise ValueError(
                f"tunnel must be one of 'none' / 'forward' / 'reverse'; "
                f"got {tunnel!r}")

        # ── URL / TLS / token resolution (service.py shapes) ──────────
        resolved_url, _ = utils.resolve_bridge_url(cli=broker_url)
        self._broker_url: str = resolved_url
        scheme = urlparse(self._broker_url).scheme
        if scheme in ('https', 'wss'):
            resolved_cert, _ = utils.resolve_bridge_cert(cli=cert)
            self._cert: Optional[str] = str(resolved_cert)
        else:
            self._cert = None
        self._token: Optional[str] = utils.resolve_bridge_token(cli=token)[0]

        # ── Identity / role ───────────────────────────────────────────
        self._name: str = name or ('consumer.%s' % uuid.uuid4().hex[:8])
        self._role_override = role
        self._resume_key: Optional[str] = resume_key

        # ── Tunnel ─────────────────────────────────────────────────────
        self._tunnel     = tunnel
        self._tunnel_via = tunnel_via
        self._tunnel_proc = None

        # ── Served-plugin FastAPI app (service.py mounting shape) ──────
        self._app: FastAPI = app if app is not None \
                                 else FastAPI(title="ORBIT Endpoint Runtime")
        self._plugins: Dict[str, Plugin] = {}
        self._app.state.bridge_url       = self._broker_url
        self._app.state.endpoint_name    = self._name
        self._app.state.endpoint_service = self
        self._app.state.is_bridge        = False
        if not hasattr(self._app.state, 'direct_routes'):
            self._app.state.direct_routes = []
        self._direct_routes: list = self._app.state.direct_routes

        if plugins:
            self._load_plugins_from_filter(list(plugins))
        # Keep the live list reference (dynamic serve() appends to it).
        self._direct_routes = self._app.state.direct_routes

        # ── Tunables ───────────────────────────────────────────────────
        self._request_concurrency = request_concurrency
        self._accept_queue        = accept_queue
        self._call_timeout        = call_timeout
        self._retry_after         = retry_after
        self._ping_interval       = ping_interval
        self._ping_timeout        = ping_timeout
        self._frame_cap           = frame_cap
        self._backoff_start       = backoff_start
        self._backoff_max         = backoff_max
        self._backoff_factor      = backoff_factor
        self._prof = rprof.Profiler('endpoint', ns='radical.orbit')

        # ── Loops / threads (populated in start()) ────────────────────
        self._work_loop:      Optional[asyncio.AbstractEventLoop] = None
        self._transport_loop: Optional[asyncio.AbstractEventLoop] = None
        self._work_thread:      Optional[threading.Thread] = None
        self._transport_thread: Optional[threading.Thread] = None
        self._work_ident:      Optional[int] = None
        self._transport_ident: Optional[int] = None
        self._cb = CallbackDispatcher(maxlen=callback_queue)

        # inbound: transport thread -> work loop; outbound: work -> transport.
        self._inbound  = Handoff(self._drain_inbound)
        self._outbound = Handoff(self._drain_outbound)
        self._outbox: deque      = deque()
        self._outbox_evt: Optional[asyncio.Event] = None

        # ── State ──────────────────────────────────────────────────────
        self._ws = None
        self._stopping = False
        self._fatal    = False
        self._registered = threading.Event()
        self._transport_future = None

        # served-request backpressure (touched on the work loop)
        self._req_inflight = 0
        self._req_sem: Optional[asyncio.Semaphore] = None

        # consumer pending table (touched on the work loop)
        self._pending: Dict[str, asyncio.Future] = {}

        # callback registry (guarded — mutated by user threads, read on work)
        self._cb_lock = threading.Lock()
        self._callbacks: Dict[Any, List[Callable]] = {}
        self._topology_callbacks: List[Callable] = []

        # rich topology snapshot (name -> {role, plugins, liveness})
        self._topology: Dict[str, Dict[str, Any]] = {}

    # ── public identity ───────────────────────────────────────────────

    @property
    def name(self) -> str:
        return self._name

    @property
    def resume_key(self) -> Optional[str]:
        return self._resume_key

    @property
    def broker_url(self) -> str:
        return self._broker_url

    def _role(self) -> str:
        if self._role_override:
            return self._role_override
        return 'endpoint' if self._plugins else 'consumer'

    # ── serve a plugin (pre-connect; v1 push/event direction) ──────────

    def serve(self, plugin) -> Plugin:
        """Mount a served plugin (class or instance) before connecting.

        Requests to the plugin are dispatched through normal served-plugin
        dispatch; a pure-callback plugin simply also pushes events.
        """
        inst = plugin(app=self._app) if isinstance(plugin, type) else plugin
        self._plugins[inst.instance_name] = inst
        self._direct_routes = self._app.state.direct_routes
        return inst

    async def _announce_topology(self) -> None:
        """PluginHostBase hook.  v1 serves plugins pre-connect; a dynamic
        change is picked up on the next (re)register — the broker rebuilds this
        participant's plugin dict from the fresh ``register`` frame."""
        log.debug("[Runtime] topology announce (served plugins now: %s)",
                  list(self._plugins))

    # ── lifecycle ──────────────────────────────────────────────────────

    def start(self, wait: bool = True, timeout: float = 30.0) -> 'EndpointRuntime':
        """Bring up both loops + the callback thread and connect.

        With *wait* the call blocks until the first ``register_ack`` (or
        *timeout*); either way calls block on the work loop via futures.
        """
        self._cb.start()

        wk_ready = threading.Event()
        self._work_thread = threading.Thread(
            target=self._run_work_thread, args=(wk_ready,),
            name='orbit-work', daemon=True)
        self._work_thread.start()
        wk_ready.wait(timeout=5.0)

        rt_ready = threading.Event()
        self._transport_thread = threading.Thread(
            target=self._run_transport_thread, args=(rt_ready,),
            name='orbit-transport', daemon=True)
        self._transport_thread.start()
        rt_ready.wait(timeout=5.0)

        self._transport_future = asyncio.run_coroutine_threadsafe(
            self._transport_main(), self._transport_loop)

        if wait:
            self._registered.wait(timeout=timeout)
        return self

    def wait_registered(self, timeout: float = 30.0) -> bool:
        """Block until the first ``register_ack`` arrives; ``True`` on success."""
        return self._registered.wait(timeout=timeout)

    def stop(self) -> None:
        """Clean-close the WS (broker treats a clean close as immediate
        removal) and tear down loops + threads."""
        self._stopping = True
        if self._transport_loop is not None and self._transport_loop.is_running():
            coro = self._graceful_close()
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    coro, self._transport_loop)
                fut.result(timeout=3.0)
            except Exception as e:
                coro.close()
                log.debug("[Runtime] graceful close failed: %s", e)
        # Cancel served-plugin background tasks on the work loop before it
        # stops, so no task is destroyed while pending.
        if self._work_loop is not None and self._work_loop.is_running() \
                and self._plugins:
            coro = self._work_teardown()
            try:
                asyncio.run_coroutine_threadsafe(
                    coro, self._work_loop).result(timeout=3.0)
            except Exception:
                coro.close()
        for loop, thread in ((self._transport_loop, self._transport_thread),
                             (self._work_loop, self._work_thread)):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
            if thread is not None:
                thread.join(timeout=5.0)
        self._cb.stop()
        if self._tunnel_proc is not None:
            from . import tunnel as _tunnel
            _tunnel.cleanup_tunnel(self._tunnel_proc, self._name)
            self._tunnel_proc = None
        self._prof.close()

    async def _graceful_close(self) -> None:
        self._stopping = True
        if self._ws is not None:
            try:    await self._ws.close(code=1000)
            except Exception:
                pass

    async def _work_teardown(self) -> None:
        for plugin in list(self._plugins.values()):
            task = getattr(plugin, '_cleanup_task', None)
            if task is not None and not task.done():
                task.cancel()

    def __enter__(self) -> 'EndpointRuntime':
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ── loop threads ───────────────────────────────────────────────────

    def _run_work_thread(self, ready: threading.Event) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._work_loop  = loop
        self._work_ident = threading.get_ident()
        self._req_sem    = asyncio.Semaphore(self._request_concurrency)
        self._inbound.bind(loop)
        loop.call_soon(ready.set)
        try:
            loop.run_forever()
        finally:
            loop.close()

    def _run_transport_thread(self, ready: threading.Event) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._transport_loop  = loop
        self._transport_ident = threading.get_ident()
        self._outbox_evt      = asyncio.Event()
        self._outbound.bind(loop)
        loop.call_soon(ready.set)
        try:
            loop.run_forever()
        finally:
            loop.close()

    # ── transport loop: connect / register / recv / send ───────────────

    async def _transport_main(self) -> None:
        # One-time tunnel setup before the connect/reconnect loop.
        try:
            if self._tunnel == 'forward':
                await self._open_tunnel_forward()
            elif self._tunnel == 'reverse':
                await self._open_tunnel_reverse()
        except Exception as e:
            log.error("[Runtime] tunnel setup failed: %s", e)
            self._fatal = True
            return

        backoff = self._backoff_start
        ssl_check_hostname = True
        while not self._stopping and not self._fatal:
            try:
                ws_url  = self._ws_url()
                ssl_ctx = self._ssl_context(ws_url, ssl_check_hostname)
                async with websockets.connect(
                        ws_url, ssl=ssl_ctx,
                        ping_interval=self._ping_interval,
                        ping_timeout=self._ping_timeout,
                        close_timeout=2,
                        max_size=self._frame_cap,
                        compression='deflate') as ws:

                    ok = await self._do_register(ws)
                    if not ok:
                        if self._fatal:
                            break
                        # name-in-use: retry with capped backoff.  A crashed
                        # predecessor holds no resume key; under the fast
                        # keepalive the name frees within `lost` seconds.
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * self._backoff_factor,
                                      self._backoff_max)
                        continue

                    self._ws = ws
                    backoff  = self._backoff_start
                    self._registered.set()
                    self._resync_subscriptions()
                    await self._serve_ws(ws)

            except ssl.SSLCertVerificationError as e:
                verify_msg  = getattr(e, 'verify_message', str(e))
                cert_pinned = bool(self._cert and os.path.exists(self._cert))
                if self._classify_cert_error(
                        verify_msg, cert_pinned, ssl_check_hostname) == 'relax':
                    log.warning("[Runtime] TLS name/IP validation failed: %s; "
                                "pinned cert present — continuing with name "
                                "validation DISABLED (dev mode).", e)
                    ssl_check_hostname = False
                    continue
                log.error("[Runtime] TLS verification failed: %s. Aborting.", e)
                self._fatal = True
                break
            except (ws_exc.ConnectionClosed, OSError) as e:
                if self._stopping:
                    break
                jitter = backoff * 0.3 * random.random()
                log.warning("[Runtime] connection lost: %s; reconnecting in "
                            "%.1fs", e, backoff + jitter)
                await asyncio.sleep(backoff + jitter)
                backoff = min(backoff * self._backoff_factor, self._backoff_max)
            except Exception as e:
                if self._stopping:
                    break
                log.exception("[Runtime] unexpected transport error: %s", e)
                await asyncio.sleep(min(backoff, self._backoff_max))
            finally:
                self._ws = None

    def _ws_url(self) -> str:
        url = self._broker_url
        if   url.startswith('https://'): url = 'wss://' + url[len('https://'):]
        elif url.startswith('http://'):  url = 'ws://'  + url[len('http://'):]
        url = url.rstrip('/')
        if not url.endswith('/register'):
            url += '/register'
        return url

    def _ssl_context(self, ws_url: str, check_hostname: bool):
        if not ws_url.startswith('wss://'):
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = check_hostname
        ctx.verify_mode    = ssl.CERT_REQUIRED
        if self._cert and os.path.exists(self._cert):
            ctx.load_verify_locations(self._cert)
        return ctx

    @staticmethod
    def _classify_cert_error(verify_message: str, cert_pinned: bool,
                             check_hostname: bool) -> str:
        """A hostname/IP mismatch is benign only when a cert is pinned (then
        ``CERT_REQUIRED`` already guarantees the peer); every other failure
        aborts (mirrors ``EndpointService._classify_cert_error``)."""
        msg = (verify_message or '').lower()
        name_mismatch = 'hostname' in msg or 'ip address' in msg
        if check_hostname and cert_pinned and name_mismatch:
            return 'relax'
        return 'abort'

    async def _do_register(self, ws) -> bool:
        reg = protocol.Register(
            src=self._name, role=self._role(),
            plugins=self._plugin_dicts(),
            identity=protocol.Identity(name=self._name,
                                       credential=self._token,
                                       resume_key=self._resume_key))
        await ws.send(protocol.pack_message(reg, cap=self._frame_cap))
        first = await ws.recv()
        try:
            ack = protocol.parse_message(first, cap=self._frame_cap)
        except protocol.ProtocolError as e:
            log.error("[Runtime] bad register_ack frame: %s", e)
            self._fatal = True
            return False
        if ack.kind != 'register_ack':
            log.error("[Runtime] first frame is %r, not register_ack", ack.kind)
            self._fatal = True
            return False
        if not ack.ok:
            if ack.reason == 'name-in-use':
                log.warning("[Runtime] name %r in use; retrying with backoff",
                            self._name)
                return False
            log.error("[Runtime] register rejected: %s", ack.reason)
            self._fatal = True
            return False
        self._resume_key = ack.resume_key
        log.info("[Runtime] registered as %r (role=%s, plugins=%s)",
                 self._name, self._role(), list(self._plugins))
        return True

    def _plugin_dicts(self) -> List[Dict[str, Any]]:
        """Rich per-plugin register dicts.  Namespaces are **endpoint-relative**
        (``/{instance}``) — routing is by ``dst``, so the consumer builds paths
        relative to the target endpoint."""
        out: List[Dict[str, Any]] = []
        for pname, plugin in self._plugins.items():
            out.append({
                'name':      pname,
                'type':      pname,
                'namespace': plugin.namespace,
                'version':   getattr(plugin, 'version', '0.0.1'),
                'enabled':   True,
                'ui_config': ui_config_to_dict(
                    getattr(plugin, 'ui_config', None)),
            })
        return out

    async def _serve_ws(self, ws) -> None:
        sender = asyncio.ensure_future(self._sender(ws))
        try:
            while not self._stopping:
                data = await ws.recv()
                self._inbound.push(data)
        finally:
            sender.cancel()
            try:    await sender
            except Exception:
                pass

    async def _sender(self, ws) -> None:
        try:
            while True:
                await self._outbox_evt.wait()
                self._outbox_evt.clear()
                while self._outbox:
                    data = self._outbox.popleft()
                    await ws.send(data)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.debug("[Runtime] sender stopped: %s", e)

    # ── handoff drains ─────────────────────────────────────────────────

    def _drain_outbound(self, items: List[bytes]) -> None:
        """Runs on the transport loop: queue frames for the sender."""
        self._outbox.extend(items)
        if self._outbox_evt is not None:
            self._outbox_evt.set()

    def _drain_inbound(self, items: List[bytes]) -> None:
        """Runs on the work loop: parse + dispatch each inbound frame."""
        for data in items:
            self._on_frame(data)

    def _emit(self, msg) -> None:
        """Pack an envelope and hand it to the transport loop (thread-safe)."""
        try:
            data = protocol.pack_message(msg, cap=self._frame_cap)
        except protocol.ProtocolError as e:
            log.error("[Runtime] failed to pack %s: %s", msg.kind, e)
            return
        self._outbound.push(data)

    # ── inbound dispatch (work loop) ───────────────────────────────────

    def _on_frame(self, data: bytes) -> None:
        try:
            msg = protocol.parse_message(data, cap=self._frame_cap)
        except protocol.ProtocolError as e:
            log.warning("[Runtime] dropping unparseable frame: %s", e)
            return
        kind = msg.kind
        if   kind == 'request':   self._admit_request(msg)
        elif kind == 'response':  self._resolve_response(msg)
        elif kind == 'event':     self._on_event(msg)
        elif kind == 'topology':  self._on_topology(msg)
        elif kind == 'control':   self._on_control(msg)
        elif kind == 'register_ack':
            pass                                   # handled on transport loop
        else:
            log.debug("[Runtime] ignoring frame kind %r", kind)

    # ── served-plugin dispatch (work loop) ─────────────────────────────

    def _admit_request(self, req: protocol.Request) -> None:
        cap = self._request_concurrency + self._accept_queue
        if self._req_inflight >= cap:
            # Fast-fail at the cap: 503 + Retry-After, never silently dropped.
            self._emit(protocol.make_response(
                req, 503,
                headers={'content-type': 'application/json',
                         'retry-after': str(self._retry_after)},
                body=_json.dumps({"error": True, "status_code": 503,
                                  "detail": "endpoint at concurrency cap"
                                  }).encode()))
            return
        self._req_inflight += 1
        self._work_loop.create_task(self._serve_request(req))

    async def _serve_request(self, req: protocol.Request) -> None:
        try:
            async with self._req_sem:
                status, headers, body = await self._dispatch_served(req)
            self._emit(protocol.make_response(
                req, status, headers=headers, body=body))
        except Exception as e:                             # pragma: no cover
            log.exception("[Runtime] request handling error: %s", e)
            self._emit(protocol.make_response(
                req, 500,
                headers={'content-type': 'application/json'},
                body=_json.dumps({"error": True, "detail": str(e)}).encode()))
        finally:
            self._req_inflight -= 1

    async def _dispatch_served(self, req: protocol.Request):
        path = req.path
        if '?' in path:
            path, qs = path.split('?', 1)
            query = dict(parse_qsl(qs))
        else:
            query = {}
        self._prof.prof('endpoint_recv', uid=req.corr_id or '',
                        msg='%s %s' % (req.method, path))
        handler, path_params = match_route(self._direct_routes, req.method, path)
        if handler is None:
            return (404, {'content-type': 'application/json'},
                    _json.dumps({"detail": f"No route: {req.method} {path}"
                                 }).encode())
        content_type = (req.headers or {}).get('content-type',
                                               'application/json')
        # Trusted owner channel.  ``req.src`` is broker-stamped (the broker
        # overwrote the envelope src from the registered identity on every
        # forwarded request), so it — not any client-supplied copy — is the
        # authoritative participant identity.  Inject it as ``x-orbit-src``,
        # dropping any inbound header of that name (never trust the client's
        # copy); plugin_base reads it as the session owner.
        headers = {k: v for k, v in (req.headers or {}).items()
                   if k.lower() != 'x-orbit-src'}
        headers['x-orbit-src'] = req.src
        shim = RequestShim(path_params, query, req.body, content_type, headers)
        try:
            result = await handler(shim)
        except HTTPException as e:
            return (e.status_code, {'content-type': 'application/json'},
                    _json.dumps({"detail": e.detail}).encode())
        except Exception as e:
            log.exception("[Runtime] handler error: %s %s", req.method, path)
            return (500, {'content-type': 'application/json'},
                    _json.dumps({"error": "endpoint-invoke-failed",
                                 "detail": str(e)}).encode())
        if hasattr(result, 'status_code'):
            return (result.status_code, dict(result.headers), bytes(result.body))
        return (200, {'content-type': 'application/json'},
                _json.dumps(result).encode())

    # ── consumer: pending table + call ─────────────────────────────────

    def _resolve_response(self, resp: protocol.Response) -> None:
        fut = self._pending.pop(resp.corr_id, None)
        if fut is not None and not fut.done():
            fut.set_result(resp)

    async def _call(self, dst, method, path, body, headers, timeout):
        req = protocol.make_request(self._name, dst, method, path,
                                    headers=headers, body=body)
        fut = self._work_loop.create_future()
        self._pending[req.corr_id] = fut
        self._emit(req)
        try:
            resp = await asyncio.wait_for(fut, timeout=timeout)
        except Exception:
            self._pending.pop(req.corr_id, None)
            raise
        return RuntimeResponse(resp.status, dict(resp.headers), bytes(resp.body))

    def call(self, dst: str, method: str, path: str, *,
             body: bytes = b'', headers: Optional[Dict[str, str]] = None,
             timeout: Optional[float] = None) -> RuntimeResponse:
        """Send a ``request`` to *dst* and block for its ``response``.

        Returns a :class:`RuntimeResponse` (``.status``/``.headers``/``.body``
        plus the httpx-ish surface the plugin helpers use).
        """
        timeout = self._call_timeout if timeout is None else timeout
        fut = asyncio.run_coroutine_threadsafe(
            self._call(dst, method, path, body, headers, timeout),
            self._work_loop)
        return fut.result(timeout=None if timeout is None else timeout + 5.0)

    # ── consumer: plugin client discovery ──────────────────────────────

    def get_plugin(self, endpoint_name: str, plugin_name: str,
                   **session_kwargs) -> PluginClient:
        """Return a plugin ``client_class`` helper bound to *endpoint_name*.

        Keeps the ``get_plugin(target, …).method()`` + ``register_session(...)``
        call-site shape; the namespace is discovered from the rich topology.
        """
        info = self._topology.get(endpoint_name)
        if not info:
            raise RuntimeError(
                f"endpoint {endpoint_name!r} not in topology")
        pinfo = (info.get('plugins') or {}).get(plugin_name)
        if not pinfo:
            raise RuntimeError(
                f"plugin {plugin_name!r} not on {endpoint_name!r}")
        namespace = pinfo['namespace']

        helper_cls = Plugin.get_plugin_class(plugin_name)
        if helper_cls is None:
            raise RuntimeError(f"plugin class for {plugin_name!r} not found")
        client_cls = getattr(helper_cls, 'client_class', None)
        if client_cls is None:
            raise RuntimeError(f"plugin client for {plugin_name!r} not known")

        # Mix the concrete helper on top of the runtime transport seam.  The
        # standard PluginClient ctor signature is preserved so helpers that
        # override __init__ keep working.
        combined = type('Runtime%s' % client_cls.__name__,
                        (client_cls, RuntimePluginClient), {})
        client = combined(_RuntimeHTTP(self, endpoint_name), namespace,
                          bridge_client=None, endpoint_id=endpoint_name,
                          plugin_name=plugin_name)
        client._runtime = self
        client.register_session(**session_kwargs)
        return client

    # ── events + callbacks (consumer) ──────────────────────────────────

    def register_callback(self, endpoint_id: Optional[str] = None,
                          plugin_name: Optional[str] = None,
                          topic: Optional[str] = None,
                          callback: Callable = None) -> None:
        """Register an event callback and auto-``subscribe`` for its pattern.

        Same tuple semantics as ``BridgeClient.register_callback``: ``None`` is
        a wildcard; filtering happens at the edge.
        """
        if callback is None:
            raise ValueError("callback is required")
        key = (endpoint_id, plugin_name, topic)
        with self._cb_lock:
            self._callbacks.setdefault(key, []).append(callback)
        self._emit(protocol.Subscribe(
            src=self._name,
            patterns=[protocol.SubscribePattern(
                endpoint=endpoint_id, plugin=plugin_name, topic=topic)]))

    def unregister_callback(self, endpoint_id: Optional[str] = None,
                            plugin_name: Optional[str] = None,
                            topic: Optional[str] = None,
                            callback: Callable = None) -> None:
        key = (endpoint_id, plugin_name, topic)
        with self._cb_lock:
            cbs = self._callbacks.get(key)
            if cbs and callback in cbs:
                cbs.remove(callback)
        self._emit(protocol.Unsubscribe(
            src=self._name,
            patterns=[protocol.SubscribePattern(
                endpoint=endpoint_id, plugin=plugin_name, topic=topic)]))

    def register_topology_callback(self, callback: Callable) -> None:
        with self._cb_lock:
            self._topology_callbacks.append(callback)

    def unregister_topology_callback(self, callback: Callable) -> None:
        with self._cb_lock:
            if callback in self._topology_callbacks:
                self._topology_callbacks.remove(callback)

    def _resync_subscriptions(self) -> None:
        """Re-send the full interest set after a (re)connect (state re-sync)."""
        with self._cb_lock:
            keys = list(self._callbacks.keys())
        if not keys:
            return
        self._emit(protocol.Subscribe(
            src=self._name,
            patterns=[protocol.SubscribePattern(endpoint=e, plugin=p, topic=t)
                      for (e, p, t) in keys]))

    def _on_event(self, ev: protocol.Event) -> None:
        endpoint, plugin, topic = ev.src, ev.plugin, ev.topic
        data = ev.data
        with self._cb_lock:
            matched: List[Callable] = []
            for (e, p, t), cbs in self._callbacks.items():
                if (e is None or e == endpoint) and \
                   (p is None or p == plugin) and \
                   (t is None or t == topic):
                    matched.extend(cbs)
        for cb in matched:
            self._cb.submit(cb, endpoint, plugin, topic, data)

    def _on_topology(self, msg: protocol.Topology) -> None:
        new_topo = {name: info.model_dump()
                    for name, info in msg.participants.items()}
        prev = self._topology
        self._topology = new_topo
        with self._cb_lock:
            cbs = list(self._topology_callbacks)
        snapshot = dict(new_topo)
        for cb in cbs:
            self._cb.submit(cb, snapshot)
        # Served plugins react to the rich topology (owner-liveness → session
        # reclaim-drain in plugin_base).  Consumers have no served plugins.
        if self._plugins:
            self._dispatch_topology_to_plugins(prev, new_topo)

    def _dispatch_topology_to_plugins(self,
                                      prev: Dict[str, Dict[str, Any]],
                                      curr: Dict[str, Dict[str, Any]]) -> None:
        """Deliver the rich topology to served plugins, synthesizing a
        ``lost`` entry for a participant that has vanished after being seen.

        The wire carries no tombstone: the broker broadcasts ``suspect`` on a
        socket drop and, once the grace elapses, simply drops the participant
        from the topology.  A participant absent from *curr* that was
        ``present``/``suspect`` in *prev* is therefore the post-grace
        ``lost`` — synthesized here (as a copy carrying ``liveness='lost'``)
        so served plugins observe the loss exactly once."""
        participants = dict(curr)
        for name, info in prev.items():
            if name in curr:
                continue
            if (info or {}).get('liveness') in ('present', 'suspect'):
                lost = dict(info)
                lost['liveness'] = 'lost'
                participants[name] = lost
        for plugin in list(self._plugins.values()):
            self._work_loop.create_task(
                self._invoke_topology(plugin, participants))

    async def _invoke_topology(self, plugin, participants: Dict[str, Any]
                               ) -> None:
        try:
            await plugin.on_topology_change(participants)
        except Exception as e:                             # pragma: no cover
            log.error("[Runtime] %s.on_topology_change failed: %s",
                      plugin.instance_name, e)

    def _on_control(self, msg: protocol.Control) -> None:
        if msg.op in ('shutdown', 'terminate'):
            log.info("[Runtime] control %s received; closing connection",
                     msg.op)
            self._stopping = True
            if self._transport_loop is not None:
                asyncio.run_coroutine_threadsafe(
                    self._graceful_close(), self._transport_loop)
        elif msg.op == 'error':
            log.error("[Runtime] broker error: %s", msg.data)

    # ── notifications from served plugins ──────────────────────────────

    async def send_notification(self, plugin_name: str, topic: str,
                                data: Dict[str, Any]) -> None:
        """Turn a served plugin's notification into a broker ``event`` frame.

        ``seq`` is a placeholder (0) — the broker stamps the authoritative
        ``seq``/``ts`` on ingest.  Reuses the notification plumbing shape
        ``service.py`` exposes as ``send_notification``.
        """
        ev = protocol.Event(src=self._name, plugin=plugin_name, topic=topic,
                            session=None, ts=0.0, seq=0, data=data)
        self._emit(ev)

    # ── topology snapshot access ───────────────────────────────────────

    def topology(self) -> Dict[str, Dict[str, Any]]:
        """The current rich topology snapshot (``name -> {role, plugins,
        liveness}``)."""
        return dict(self._topology)

    # ── tunnels (service.py shapes) ────────────────────────────────────

    async def _open_tunnel_forward(self) -> None:
        """Forward mode: open ``ssh -L`` to the login host and rewrite the
        broker URL to ``localhost:<port>`` (compute → login)."""
        from . import tunnel as _tunnel

        login_host = (self._tunnel_via
                      or os.environ.get('PBS_O_HOST')
                      or os.environ.get('SLURM_SUBMIT_HOST'))
        if not login_host:
            raise RuntimeError(
                "--tunnel forward: no login host available. Pass --tunnel-via "
                "HOST or set PBS_O_HOST / SLURM_SUBMIT_HOST.")
        parsed      = urlparse(self._broker_url)
        broker_host = parsed.hostname or 'localhost'
        broker_port = parsed.port or (443 if parsed.scheme == 'https' else 8000)
        proc, port  = await asyncio.to_thread(
            _tunnel.spawn_tunnel, login_host, broker_host, broker_port,
            self._name)
        self._tunnel_proc = proc
        self._broker_url  = urlunparse(
            parsed._replace(netloc='localhost:%d' % port))
        log.info("[Runtime] forward tunnel active; broker URL now %s",
                 self._broker_url)

    async def _open_tunnel_reverse(self, wait_timeout: float = 15.0) -> None:
        """Reverse mode: wait for a login-side spawner to open ``ssh -R`` and
        write the rendezvous file, then rewrite the broker URL (login →
        compute)."""
        from . import tunnel as _tunnel

        parsed      = urlparse(self._broker_url)
        broker_host = parsed.hostname or 'localhost'
        broker_port = parsed.port or (443 if parsed.scheme in ('https', 'wss')
                                      else 8000)
        rdir       = _tunnel.relay_dir()
        relay_file = rdir / ('%s.port' % self._name)
        req_file   = rdir / ('%s.req' % self._name)
        req_payload = _json.dumps({
            'endpoint_name': self._name,
            'hostname':      socket.gethostname(),
            'bridge_host':   broker_host,
            'bridge_port':   broker_port,
        })
        tmp = req_file.with_suffix('.req.tmp')
        tmp.write_text(req_payload)
        tmp.rename(req_file)
        log.info("[Runtime] reverse tunnel: wrote request file %s", req_file)

        deadline = asyncio.get_running_loop().time() + wait_timeout
        while asyncio.get_running_loop().time() < deadline:
            try:    contents = set(os.listdir(str(relay_file.parent)))
            except OSError:
                contents = set()
            if relay_file.name in contents:
                try:    port = int(relay_file.read_text().strip())
                except (ValueError, OSError):
                    port = None
                if port:
                    self._broker_url = urlunparse(
                        parsed._replace(netloc='localhost:%d' % port))
                    log.info("[Runtime] reverse tunnel active; broker URL now "
                             "%s", self._broker_url)
                    return
            await asyncio.sleep(2.0)
        raise RuntimeError(
            "--tunnel reverse: rendezvous file %s did not appear within %.0fs"
            % (relay_file, wait_timeout))
