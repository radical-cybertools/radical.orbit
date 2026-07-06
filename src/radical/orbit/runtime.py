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
  never liveness (the stock library keepalive is genuinely
  transport-independent).
* A **work loop** (its own runtime-owned thread) does envelope pack/parse and
  all plugin/handler + consumer-call work.
* User callbacks fire on a **dedicated callback-dispatcher thread** — never the
  transport or work loop (a slow user callback is user code and can never be
  "disciplined").

Frames cross the two loops with the stdlib primitives, so the transport thread
never blocks the producer: an inbound frame is handed to the work loop with a
single ``loop.call_soon_threadsafe(self._on_frame, data)``; an outbound frame is
handed to the transport loop's :class:`asyncio.Queue` the same way.  Strict
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
import time
import uuid

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
from .runtime_client    import RuntimeResponse, _RuntimeHTTP
from .runtime_support   import CallbackDispatcher
from .ui_schema         import ui_config_to_dict


log = logging.getLogger("radical.orbit.runtime")

# ── Runtime tunables (defaults) ────────────────────────────────────────────
# Operator-facing knobs are explicit constructor arguments; these numeric
# defaults are speculative-generality seams a user never sets, so they live as
# named constants instead.  Tests that need to shrink a timeout patch the
# constant or set the corresponding private attribute (or pass the legacy
# keyword, still accepted via ``**tuning`` for the cross-module test harnesses).
_REQUEST_CAP    = 128     # served-request admission cap (503 fast-fail beyond)
_CALLBACK_QUEUE = 1024    # bounded depth of the user-callback dispatcher
_CALL_TIMEOUT   = 600.0   # default consumer-call timeout (seconds)
_RETRY_AFTER    = 1.0     # Retry-After on a 503 (seconds)
_PING_INTERVAL  = 1.0     # websockets client keepalive cadence
_PING_TIMEOUT   = 3.0     # websockets client keepalive answer deadline
_BACKOFF_START  = 0.5     # (re)connect backoff floor (seconds)
_BACKOFF_MAX    = 10.0    # (re)connect backoff ceiling (seconds)
_BACKOFF_FACTOR = 1.5     # (re)connect backoff growth factor

# Legacy per-instance tunables still accepted as keyword arguments (they feed
# private attributes) so existing harnesses keep constructing runtimes without
# change; they are deliberately absent from the visible signature.
_TUNING_KEYS = {'request_concurrency', 'accept_queue', 'callback_queue',
                'call_timeout', 'retry_after', 'frame_cap',
                'backoff_start', 'backoff_max', 'backoff_factor'}


# ---------------------------------------------------------------------------
# EndpointRuntime
# ---------------------------------------------------------------------------

class EndpointRuntime(PluginHostBase):
    """A participant: serve and/or consume over one outbound WS to the broker.

    Args:
        broker_url:  Broker URL.  CLI > env (``RADICAL_ORBIT_BROKER_URL``) >
                     file — resolved via :func:`utils.resolve_broker_url`.
        cert:        Broker TLS cert (only for ``https``/``wss``).
        name:        Stable participant name.  ``None`` → ``consumer.<uuid8>``
                     (auto-named consumers cannot recover sessions on restart).
        plugins:     Served-plugin filter list (names / ``'all'`` / ``'default'``)
                     mounted pre-connect; ``None`` → pure consumer unless
                     :meth:`serve` is called.
        role:        Advertised role.  Defaults to ``'consumer'`` (no served
                     plugins) or ``'endpoint'``.
        tunnel:      ``'none'`` / ``'forward'`` / ``'reverse'`` (boolean is not
                     accepted).
        ping_interval/ping_timeout: ``websockets`` client keepalive cadence.

    The numeric backpressure/backoff/frame-cap tunables are module-level
    constants (``_REQUEST_CAP``, ``_BACKOFF_*`` …), not per-instance knobs; they
    are still accepted as keyword arguments for existing test harnesses but are
    intentionally absent from the visible signature.
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
                 ping_interval:       float = _PING_INTERVAL,
                 ping_timeout:        float = _PING_TIMEOUT,
                 **tuning):

        if tunnel not in ('none', 'forward', 'reverse'):
            raise ValueError(
                f"tunnel must be one of 'none' / 'forward' / 'reverse'; "
                f"got {tunnel!r}")
        bad = set(tuning) - _TUNING_KEYS
        if bad:
            raise TypeError("unexpected keyword argument(s): %s"
                            % ', '.join(sorted(bad)))

        # ── URL / TLS / token resolution ──────────────────────────────
        resolved_url, _ = utils.resolve_broker_url(cli=broker_url)
        self._broker_url: str = resolved_url
        scheme = urlparse(self._broker_url).scheme
        if scheme in ('https', 'wss'):
            resolved_cert, _ = utils.resolve_broker_cert(cli=cert)
            self._cert: Optional[str] = str(resolved_cert)
        else:
            self._cert = None
        self._token: Optional[str] = utils.resolve_broker_token(cli=token)[0]

        # ── Identity / role ───────────────────────────────────────────
        self._name: str = name or ('consumer.%s' % uuid.uuid4().hex[:8])
        self._role_override = role
        self._resume_key: Optional[str] = resume_key

        # ── Tunnel ─────────────────────────────────────────────────────
        self._tunnel     = tunnel
        self._tunnel_via = tunnel_via
        self._tunnel_proc = None

        # ── Served-plugin registration substrate ──────────────────────
        # NOTE: this FastAPI app is **never served** — no server ever binds it.
        # It is only the plugins' route-registration substrate: served plugins
        # register their routes against it, and served-request dispatch reads
        # ``app.state.direct_routes`` (via :meth:`match_route`).  The gateway
        # alone owns a *served* FastAPI app.
        self._app: FastAPI = app if app is not None \
                                 else FastAPI(title="ORBIT Endpoint Runtime")
        self._plugins: Dict[str, Plugin] = {}
        self._app.state.broker_url       = self._broker_url
        self._app.state.endpoint_name    = self._name
        self._app.state.endpoint_service = self
        self._app.state.is_broker        = False
        if not hasattr(self._app.state, 'direct_routes'):
            self._app.state.direct_routes = []
        self._direct_routes: list = self._app.state.direct_routes

        if plugins:
            self._load_plugins_from_filter(list(plugins))

        # ── Tunables ───────────────────────────────────────────────────
        self._req_cap = _REQUEST_CAP
        if 'request_concurrency' in tuning or 'accept_queue' in tuning:
            self._req_cap = (int(tuning.get('request_concurrency', 0))
                             + int(tuning.get('accept_queue', 0)))
        self._call_timeout   = tuning.get('call_timeout',   _CALL_TIMEOUT)
        self._retry_after    = tuning.get('retry_after',    _RETRY_AFTER)
        self._ping_interval  = ping_interval
        self._ping_timeout   = ping_timeout
        self._frame_cap      = tuning.get('frame_cap',      protocol.FRAME_CAP)
        self._backoff_start  = tuning.get('backoff_start',  _BACKOFF_START)
        self._backoff_max    = tuning.get('backoff_max',    _BACKOFF_MAX)
        self._backoff_factor = tuning.get('backoff_factor', _BACKOFF_FACTOR)
        self._prof = rprof.Profiler('endpoint', ns='radical.orbit')

        # ── Loops / threads (populated in start()) ────────────────────
        self._work_loop:      Optional[asyncio.AbstractEventLoop] = None
        self._transport_loop: Optional[asyncio.AbstractEventLoop] = None
        self._work_thread:      Optional[threading.Thread] = None
        self._transport_thread: Optional[threading.Thread] = None
        self._work_ident:      Optional[int] = None
        self._transport_ident: Optional[int] = None
        self._cb = CallbackDispatcher(
            maxlen=tuning.get('callback_queue', _CALLBACK_QUEUE))

        # Outbound frames (any thread -> transport loop) ride one asyncio.Queue
        # owned by the transport loop; the producer hands over with a
        # non-blocking ``call_soon_threadsafe(put_nowait, ...)``.  Inbound frames
        # go the mirror way straight to ``_on_frame`` on the work loop.
        self._outbox: Optional[asyncio.Queue] = None

        # ── State ──────────────────────────────────────────────────────
        self._ws = None
        self._stopping = False
        self._fatal    = False
        self._registered = threading.Event()
        # Set once the first topology frame lands (the broker sends it right
        # after register_ack) so start(wait=True) can also wait for it.
        self._topology_ready = threading.Event()
        self._transport_future = None

        # served-request backpressure (touched on the work loop): one counter
        # against one cap — a 503 fast-fail beyond it, nothing else.
        self._req_inflight = 0

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

        With *wait* the call blocks until the first ``register_ack`` **and** the
        first topology frame (the broker sends it right after the ack) land, or
        until *timeout* — so after ``start(wait=True)`` returns, :meth:`topology`
        reflects the broker's snapshot at registration time and an immediate
        :meth:`get_plugin` will not race a not-yet-populated topology.  Either
        way calls block on the work loop via futures.
        """
        self._cb.start()

        wk_ready = threading.Event()
        self._work_thread = threading.Thread(
            target=self._run_work_thread, args=(wk_ready,),
            name='orbit-work', daemon=True)
        self._work_thread.start()
        if not wk_ready.wait(timeout=5.0):
            raise RuntimeError("work loop thread failed to start")

        rt_ready = threading.Event()
        self._transport_thread = threading.Thread(
            target=self._run_transport_thread, args=(rt_ready,),
            name='orbit-transport', daemon=True)
        self._transport_thread.start()
        if not rt_ready.wait(timeout=5.0):
            raise RuntimeError("transport loop thread failed to start")

        self._transport_future = asyncio.run_coroutine_threadsafe(
            self._transport_main(), self._transport_loop)

        if wait:
            deadline = time.monotonic() + timeout
            self._registered.wait(timeout=timeout)
            # Also wait (within the same budget) for the first topology frame,
            # so topology() is populated before start() returns.
            remaining = max(0.0, deadline - time.monotonic())
            self._topology_ready.wait(timeout=remaining)
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
        # Cancel the transport coroutine so a reconnect backoff-sleep is not
        # left pending when its loop stops ("Task was destroyed" warnings).
        if self._transport_future is not None:
            self._transport_future.cancel()
            self._transport_future = None
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
            if loop is not None and loop.is_running():
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
        tasks = []
        for plugin in list(self._plugins.values()):
            task = getattr(plugin, '_cleanup_task', None)
            if task is not None and not task.done():
                task.cancel()
                tasks.append(task)
        # Await the cancellations so they complete before the loop closes
        # (otherwise the loop tears down with the tasks still pending).
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
        self._outbox          = asyncio.Queue()
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
        while not self._stopping and not self._fatal:
            try:
                ws_url  = self._ws_url()
                ssl_ctx = self._ssl_context(ws_url)
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
                    # Consumer calls survive a reconnect by design (same name,
                    # broker still holds the route): pending futures are left in
                    # place to be resolved by the correlated response.  Surface
                    # that so a debugging reader sees why a call is still open.
                    if self._pending:
                        log.info("[Runtime] reconnected with %d call(s) still "
                                 "pending across the reconnect", len(self._pending))
                    self._resync_subscriptions()
                    await self._serve_ws(ws)

            except ssl.SSLCertVerificationError as e:
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

    def _ssl_context(self, ws_url: str):
        """Build the client TLS context.

        With a **pinned** cert, ``CERT_REQUIRED`` against that cert already
        authenticates the peer, so hostname matching (which a self-signed dev
        cert commonly fails) is redundant — it is disabled up front rather than
        reactively downgraded on an OpenSSL error string.  Without a pinned cert
        the system trust store is used with full hostname verification.
        """
        if not ws_url.startswith('wss://'):
            return None
        ctx = ssl.create_default_context()
        ctx.verify_mode = ssl.CERT_REQUIRED
        if self._cert and os.path.exists(self._cert):
            ctx.load_verify_locations(self._cert)
            ctx.check_hostname = False
        else:
            ctx.check_hostname = True
        return ctx

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
                # Hand the raw frame to the work loop without blocking the
                # transport thread (so keepalive is never starved).
                self._work_loop.call_soon_threadsafe(self._on_frame, data)
        finally:
            sender.cancel()
            try:    await sender
            except Exception:
                pass

    async def _sender(self, ws) -> None:
        """Drain the outbound queue onto the socket (transport loop)."""
        try:
            while True:
                await ws.send(await self._outbox.get())
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.debug("[Runtime] sender stopped: %s", e)

    def _emit(self, msg) -> None:
        """Pack an envelope and hand it to the transport loop (thread-safe)."""
        try:
            data = protocol.pack_message(msg, cap=self._frame_cap)
        except protocol.ProtocolError as e:
            log.error("[Runtime] failed to pack %s: %s", msg.kind, e)
            return
        loop = self._transport_loop
        if loop is None or self._outbox is None:
            log.warning("[Runtime] dropping %s: transport not up yet", msg.kind)
            return
        loop.call_soon_threadsafe(self._outbox.put_nowait, data)

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
        if self._req_inflight >= self._req_cap:
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
        # Case-insensitive content-type lookup: a client may send
        # ``Content-Type`` and a plain dict ``.get('content-type')`` would miss.
        lower_headers = {k.lower(): v for k, v in (req.headers or {}).items()}
        content_type = lower_headers.get('content-type', 'application/json')
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

        Returns a :class:`RuntimeResponse` — the httpx-ish surface
        (``.status_code``/``.headers``/``.content``/``.json()``) the plugin
        helpers use.
        """
        timeout = self._call_timeout if timeout is None else timeout
        fut = asyncio.run_coroutine_threadsafe(
            self._call(dst, method, path, body, headers, timeout),
            self._work_loop)
        # ``timeout`` is always a number here; the inner ``_call`` already
        # enforces it, so give the blocking wait a small grace over it.
        return fut.result(timeout=timeout + 5.0)

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

        # The concrete helper rides the runtime transport seam directly: its
        # ``self._http`` is a :class:`_RuntimeHTTP` over the WebSocket, and it is
        # handed ``broker_client=self`` so the base
        # ``register_notification_callback`` forwards straight to this runtime's
        # callback registry — no mixin, no synthesized class.
        client = client_cls(_RuntimeHTTP(self, endpoint_name), namespace,
                            broker_client=self, endpoint_id=endpoint_name,
                            plugin_name=plugin_name)
        client.register_session(**session_kwargs)
        return client

    # ── events + callbacks (consumer) ──────────────────────────────────

    def register_callback(self, endpoint_id: Optional[str] = None,
                          plugin_name: Optional[str] = None,
                          topic: Optional[str] = None,
                          callback: Callable = None,
                          with_meta: bool = False) -> None:
        """Register an event callback and auto-``subscribe`` for its pattern.

        Tuple semantics: ``None`` is a wildcard; filtering happens at the edge.

        By default the callback is invoked as
        ``callback(endpoint, plugin, topic, data)``.  With *with_meta* it is
        invoked as ``callback(endpoint, plugin, topic, data, meta)`` where
        ``meta = {'seq': int, 'ts': float, 'session': str | None}`` carries the
        broker-stamped envelope metadata (additive; the wire is unchanged).  The
        broker ``seq`` is the same authoritative value the replay plugin
        retains, so live delivery can dedup on it.
        """
        if callback is None:
            raise ValueError("callback is required")
        key = (endpoint_id, plugin_name, topic)
        with self._cb_lock:
            self._callbacks.setdefault(key, []).append((callback, with_meta))
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
            if cbs:
                self._callbacks[key] = [(cb, m) for (cb, m) in cbs
                                        if cb is not callback]
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
            matched: List[tuple] = []
            for (e, p, t), cbs in self._callbacks.items():
                if (e is None or e == endpoint) and \
                   (p is None or p == plugin) and \
                   (t is None or t == topic):
                    matched.extend(cbs)
        meta = None
        for cb, with_meta in matched:
            if with_meta:
                if meta is None:
                    meta = {'seq': ev.seq, 'ts': ev.ts, 'session': ev.session}
                self._cb.submit(cb, endpoint, plugin, topic, data, meta)
            else:
                self._cb.submit(cb, endpoint, plugin, topic, data)

    def _on_topology(self, msg: protocol.Topology) -> None:
        new_topo = {name: info.model_dump()
                    for name, info in msg.participants.items()}
        prev = self._topology
        self._topology = new_topo
        # One-shot ``lost`` synthesis — a single diff feeding two consumers
        # (consumer topology callbacks AND served plugins) so both observe the
        # loss exactly once, identically.  The stored snapshot
        # (``self._topology`` / :meth:`topology`) keeps the broker's
        # tombstone-free view and never retains the synthesized entry.
        participants = self._synthesize_lost(prev, new_topo)
        with self._cb_lock:
            cbs = list(self._topology_callbacks)
        for cb in cbs:
            self._cb.submit(cb, participants)
        # Served plugins react to the rich topology (owner-liveness → session
        # reclaim-drain in plugin_base).  Consumers have no served plugins.
        if self._plugins:
            for plugin in list(self._plugins.values()):
                self._work_loop.create_task(
                    self._invoke_topology(plugin, participants))
        # Unblock start(wait=True): the first topology frame has landed.
        self._topology_ready.set()

    @staticmethod
    def _synthesize_lost(prev: Dict[str, Dict[str, Any]],
                         curr: Dict[str, Dict[str, Any]]
                         ) -> Dict[str, Dict[str, Any]]:
        """Return *curr* plus a synthesized ``liveness='lost'`` entry for every
        participant that has vanished after being seen.

        The wire carries no tombstone: the broker broadcasts ``suspect`` on a
        socket drop and, once the grace elapses, simply drops the participant
        from the topology.  A participant absent from *curr* that was
        ``present``/``suspect`` in *prev* is therefore the post-grace ``lost`` —
        synthesized here (as a copy carrying ``liveness='lost'``) so both the
        consumer callbacks and the served plugins observe the loss exactly once.
        The result is delivery-only; the caller keeps *curr* as the stored
        snapshot so the synthesized entry never lingers."""
        participants = dict(curr)
        for name, info in prev.items():
            if name in curr:
                continue
            if (info or {}).get('liveness') in ('present', 'suspect'):
                lost = dict(info)
                lost['liveness'] = 'lost'
                participants[name] = lost
        return participants

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

    def send_notification(self, plugin_name: str, topic: str,
                          data: Dict[str, Any]) -> None:
        """Turn a served plugin's notification into a broker ``event`` frame.

        A plain sync method (the body only packs + hands off to the transport
        loop, never awaits) so consumer code can call it from any thread — the
        one obvious way, matching the rest of the consumer-facing API.

        ``seq`` is a placeholder (0) — the broker stamps the authoritative
        ``seq``/``ts`` on ingest.
        """
        ev = protocol.Event(src=self._name, plugin=plugin_name, topic=topic,
                            session=None, ts=0.0, seq=0, data=data)
        self._emit(ev)

    # ── topology snapshot access ───────────────────────────────────────

    def topology(self) -> Dict[str, Dict[str, Any]]:
        """The current rich topology snapshot (``name -> {role, plugins,
        liveness}``)."""
        return dict(self._topology)

    # ── tunnels ────────────────────────────────────────────────────────

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
            'broker_host':   broker_host,
            'broker_port':   broker_port,
        })
        tmp = req_file.with_suffix('.req.tmp')
        tmp.write_text(req_payload)
        tmp.rename(req_file)
        log.info("[Runtime] reverse tunnel: wrote request file %s", req_file)

        deadline = asyncio.get_running_loop().time() + wait_timeout
        while asyncio.get_running_loop().time() < deadline:
            # Poll via listdir (not relay_file.exists()) on purpose: a directory
            # read forces the shared-FS client (Lustre/NFS/DVS) to revalidate its
            # cache, so a file the login-side spawner just wrote is seen promptly.
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
