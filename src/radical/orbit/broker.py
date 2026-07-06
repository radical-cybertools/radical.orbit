"""ORBIT Broker — the active hub (routing loop + hosted-plugin host).

The broker is the sole hub of the participant star: endpoints dial in over a
single outbound WebSocket, the broker routes ``request``/``response`` frames
between them by ``dst``, fans out ``event`` frames to interested subscribers,
tracks topology + liveness, and is itself a participant (``src='broker'``)
that hosts plugins and can *call* endpoints.

**Transport isolation (load-bearing).**  The uvicorn/server loop is the *lean
routing loop*: every frame it touches is a handful of dict operations
(``msgpack`` unpack, a registry lookup, ``msgpack`` repack, a socket send).
The hosted-plugin host runs on its **own event loop in its own thread**, so a
blocking plugin handler stalls request *handling*, never liveness — the
routing loop keeps answering WS keepalive pings the whole time.  The two loops
exchange work with ``run_coroutine_threadsafe`` in either direction.

**Liveness is transport-level and two-stage.**  Server-wide WS keepalive
(``ws_ping_interval``/``ws_ping_timeout`` on uvicorn) closes a silent socket;
a socket drop makes the identity ``suspect`` immediately (topology signal); a
grace timer then declares it ``lost`` (actionable — inflight calls to it
fast-fail).  A valid re-register cancels the grace timer; a clean close skips
``suspect`` (immediate removal).  The grace timers live on the routing loop, so
a loop stall cannot declare anyone lost while it lasts, and every suspect keeps
its full grace window after the loop recovers.

**Minimal core.**  The broker owns a *minimal* app: only the token-gated WS
``/register`` route.  HTTP catch-all, SSE ``/events``, the Explorer UI and CORS
are the gateway module's job — it attaches onto this same app through the
constructor-declared **gateway seam**, the attributes/methods ``gateway.py`` is
handed:

* :attr:`Broker.app`               — the FastAPI app to mount HTTP routes on.
* :attr:`Broker.caller`            — the :class:`BrokerCaller` handle.
* :meth:`Broker.tap`               — subscribe to the raw event stream.
* :meth:`Broker.topology_snapshot` — the current rich topology dict.
* :attr:`Broker.token` / :attr:`Broker.auth_enabled` — the credential config.

Resume keys live in broker memory only — a broker restart clears them, so
re-registration is first-come after a restart.
"""

# pylint: disable=protected-access

import asyncio
import concurrent.futures
import itertools
import logging
import os
import signal
import threading

from contextlib  import asynccontextmanager
from dataclasses import dataclass
from typing      import Any, Callable, Dict, List, Optional, Tuple

import msgpack

from fastapi              import FastAPI, WebSocket, WebSocketDisconnect, \
                                 HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests   import Request
from starlette.responses  import JSONResponse

from . import _prof as rprof
from . import protocol
from . import utils

from .broker_plugin_host import BrokerPluginHost
from .broker_events      import EventRouter
from .errors             import error_body, error_dict


log = logging.getLogger("radical.orbit.broker")

BROKER_NAME = 'broker'


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

@dataclass
class BrokerTuning:
    """Liveness/backpressure knobs, grouped out of the constructor.

    Defaults are production values; unit tests pass a tuning object with tiny
    timeouts (or mutate attributes) so no test has to sleep for real.

    * ``ping_interval`` / ``ping_timeout`` — uvicorn WS keepalive; a silent
      socket is closed at ``ping_interval + ping_timeout``.
    * ``grace`` — the ``suspect`` → ``lost`` window.
    * ``call_cap`` — a cap on **broker-originated** in-flight calls (the only
      calls the broker itself waits on); overflow raises at the caller.
    * ``forwarded_call_cap`` — a cap on the **forwarded** endpoint↔endpoint
      call table; a flood beyond it fast-fails with a 503.
    * ``call_timeout`` — backstop deadline on every in-flight correlation; a
      responder that never answers is reaped and fast-failed with a 504.
    * ``reap_interval`` — how often the reaper sweeps for expired calls.
    * ``event_queue`` — per-subscriber delivery-queue depth.
    * ``frame_cap`` — uvicorn ``ws_max_size`` / the protocol frame cap.
    """

    ping_interval: float = 1.0
    ping_timeout:  float = 3.0
    grace:         float = 10.0
    call_cap:      int   = 1024
    forwarded_call_cap: int = 4096
    call_timeout:  float = 30.0
    reap_interval: float = 5.0
    event_queue:   int   = 1024
    frame_cap:     int   = protocol.FRAME_CAP


class _Call:
    """One in-flight correlated call the broker is tracking.

    ``future`` is set only for broker-originated calls (``src='broker'``, the
    :class:`BrokerCaller` path); a forwarded endpoint↔endpoint call leaves it
    ``None``.  ``deadline`` (loop time) bounds every entry so a responder that
    never answers cannot leak the table.
    """

    __slots__ = ('src', 'dst', 'future', 'deadline')

    def __init__(self, src: str, dst: str,
                 future: Optional[asyncio.Future], deadline: float):
        self.src      = src
        self.dst      = dst
        self.future   = future
        self.deadline = deadline


# ---------------------------------------------------------------------------
# Supervised task creation
# ---------------------------------------------------------------------------

def spawn(coro, label: str, loop: Optional[asyncio.AbstractEventLoop] = None
          ) -> asyncio.Task:
    """Create a background task whose exceptions are never swallowed.

    Handler *raises* are already isolated at the two plugin hosts; the real
    gap is ``create_task``'d background coroutines whose exceptions die
    silently.  Every background task in the broker goes through here so a
    crash is logged (and thus reportable) rather than lost.
    """
    loop = loop or asyncio.get_running_loop()
    task = loop.create_task(coro)

    def _done(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            log.error("[Broker] background task %r failed: %r", label, exc,
                      exc_info=exc)

    task.add_done_callback(_done)
    return task


# ---------------------------------------------------------------------------
# Broker-as-participant caller handle
# ---------------------------------------------------------------------------

class BrokerCaller:
    """In-process caller handle: the broker calling an endpoint (``src='broker'``).

    :meth:`call` is awaitable on the routing loop; :meth:`call_threadsafe`
    returns a ``concurrent.futures.Future`` for the plugin-host thread (the
    hosted task dispatcher uses it).  Both route through the broker's in-flight
    table with the broker-call cap enforced synchronously at the caller.
    """

    def __init__(self, broker: 'Broker'):
        self._broker = broker

    async def call(self, dst: str, method: str, path: str, *,
                   body:    bytes                    = b'',
                   headers: Optional[Dict[str, str]] = None,
                   timeout: Optional[float]          = None) -> Dict[str, Any]:
        """Send a ``request`` to *dst* and await its ``response`` (as a dict).

        Raises ``RuntimeError`` immediately when broker-originated in-flight
        calls are at the cap, or on an unroutable ``dst``; raises
        ``asyncio.TimeoutError`` if *timeout* elapses first.
        """
        return await self._broker._broker_call(
            dst, method, path, body=body, headers=headers, timeout=timeout)

    def call_threadsafe(self, dst: str, method: str, path: str, *,
                        body:    bytes                    = b'',
                        headers: Optional[Dict[str, str]] = None,
                        timeout: Optional[float]          = None
                        ) -> 'concurrent.futures.Future':
        """Schedule :meth:`call` on the routing loop from another thread."""
        return asyncio.run_coroutine_threadsafe(
            self.call(dst, method, path, body=body, headers=headers,
                      timeout=timeout),
            self._broker._loop)


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------

class Broker:
    """The active broker — lean routing loop + own-thread plugin host.

    The constructor takes the handful of things an operator sets — ``app``,
    ``cert``/``key``, ``host``/``port``, ``plugins``, ``token``/``no_auth`` and
    ``gateway``.  The liveness/backpressure knobs are grouped in an optional
    :class:`BrokerTuning` object (unit tests pass one with tiny timeouts, or
    mutate the resulting ``self._*`` attributes, so no test sleeps for real).
    """

    def __init__(self,
                 app:      Optional[FastAPI] = None,
                 cert:     Optional[str]     = None,
                 key:      Optional[str]     = None,
                 host:     str = '0.0.0.0',
                 port:     int = 8000,
                 plugins:  str = '',
                 token:    Optional[str]     = None,
                 no_auth:  bool              = False,
                 gateway:  bool              = True,
                 tuning:   Optional[BrokerTuning] = None):

        # ── TLS config ───────────────────────────────────────────────
        cert_path, _ = utils.resolve_broker_cert(cli=cert)
        key_path,  _ = utils.resolve_broker_key (cli=key, cert=cert_path)
        self._cert: str = str(cert_path)
        self._key : str = str(key_path)

        # ── Ingress auth token ────────────────────────────────────────
        self._auth_enabled: bool          = not utils.auth_disabled(no_auth)
        self._token:        Optional[str] = None
        self._token_source: str           = 'disabled'
        if self._auth_enabled:
            self._token, self._token_source = utils.ensure_broker_token(cli=token)

        # ── Advertised URL ───────────────────────────────────────────
        self._host = host
        self._port = port
        self._url_forms = utils.public_url_forms(self._host, self._port)
        self._url: str  = self._url_forms[0]

        # ── Tunables (unpacked to attributes; tests may set them) ─────
        tuning = tuning or BrokerTuning()
        self._ping_interval = tuning.ping_interval
        self._ping_timeout  = tuning.ping_timeout
        self._grace         = tuning.grace
        self._call_cap      = tuning.call_cap
        self._forwarded_call_cap = tuning.forwarded_call_cap
        self._call_timeout  = tuning.call_timeout
        self._reap_interval = tuning.reap_interval
        self._event_queue   = tuning.event_queue
        self._frame_cap     = tuning.frame_cap
        self._plugins_spec: str = plugins or ''
        self._gateway_enabled   = gateway

        # ── Routing state (all touched only on the routing loop) ──────
        self.registry:       Dict[str, Any]         = {}   # name -> transport ws
        self._participants:  Dict[str, Dict[str, Any]] = {}  # name->{role,plugins}
        self._liveness:      Dict[str, str]         = {}   # name -> present/...
        self._resume_keys:   Dict[str, str]         = {}   # name -> resume_key
        self._grace_timers:  Dict[str, Any]         = {}   # name -> TimerHandle

        # Topology-change listeners (the one seam the gateway needs beyond
        # tap/caller: topology changes do not flow through the raw event tap).
        # Fired synchronously on the routing loop from ``_broadcast_topology``.
        self._topology_listeners: List[Callable[[], None]] = []

        # Last rich topology delivered to the hosted-plugin host, so a
        # participant that vanishes (post-grace ``lost``) can be re-synthesized
        # with ``liveness='lost'`` for hosted plugins (the wire carries no
        # tombstone; the broker simply drops a lost participant).  Touched only
        # on the routing loop.
        self._host_topology_prev: Dict[str, Dict[str, Any]] = {}

        # Correlation: one in-flight table keyed by ``corr_id``.  A
        # broker-originated call carries an ``asyncio.Future``; a forwarded
        # endpoint↔endpoint call carries ``None``.  Every entry has a deadline
        # so an unanswered call is reaped (grace-bounded fast-fail on ``lost``,
        # deadline sweep otherwise).
        self._calls: Dict[str, _Call] = {}   # corr_id -> _Call

        # Event delivery + subscriptions live in a sibling module (the routing
        # loop stays lean); the router shares the live ``registry`` reference.
        self._prof     = rprof.Profiler('broker', ns='radical.orbit')
        self._events   = EventRouter(self.registry, spawn, self._prof,
                                     self._event_queue, lambda: self._host_loop)

        # Loops / host thread (populated in startup()).
        self._loop:      Optional[asyncio.AbstractEventLoop] = None
        self._host_loop: Optional[asyncio.AbstractEventLoop] = None
        self._host_thread: Optional[threading.Thread]        = None
        self._plugin_host: Optional[BrokerPluginHost]        = None
        self._reap_task: Optional[asyncio.Task]              = None
        self._started:   bool = False

        self.caller = BrokerCaller(self)

        # Profiling — broker_* labels
        # (``self._prof`` is created above with the event router).
        self._req_ctr  = itertools.count()

        # ── App ──────────────────────────────────────────────────────
        if app is None:
            app = FastAPI(title="ORBIT Broker",
                          lifespan=self._lifespan,
                          description="ORBIT active broker — participant star hub.",
                          version="0.1.0")
        self._app: FastAPI = app
        self._app.state.is_broker = True   # role detection: broker hosts plugins

        self._setup_middleware()
        self._register_routes()

        # ── Gateway module ──────────────────────────────────────────
        # On by default: build/attach the compat-tier HTTP/SSE/UI ingress onto
        # this same app (single port, single uvicorn server).  ``gateway=False``
        # is a headless broker (only the token-gated WS ``/register``).  Imported
        # lazily so a broker import never hard-depends on the gateway module.
        self._gateway = None
        if self._gateway_enabled:
            from .gateway import Gateway
            self._gateway = Gateway(self)

    # ── public API / gateway seam ─────────────────────────────────────

    @property
    def app(self) -> FastAPI:
        """The FastAPI app (gateway seam: mount HTTP routes here)."""
        return self._app

    @property
    def url(self) -> str:
        """The broker's advertised URL (canonical FQDN form)."""
        return self._url

    @property
    def token(self) -> Optional[str]:
        """The shared ingress token (gateway seam: credential config)."""
        return self._token

    @property
    def auth_enabled(self) -> bool:
        """Whether the ingress token gate is active (gateway seam)."""
        return self._auth_enabled

    def tap(self, callback: Callable[[Dict[str, Any]], Any]) -> Callable[[], None]:
        """Register an in-process subscriber for the raw (unfiltered) event
        stream — the replay plugin's hook and the gateway's SSE fan-out.

        The callback receives every event dict and is run supervised on the
        **plugin-host loop**, never the routing loop.  Returns an unsubscribe
        callable.
        """
        return self._events.tap(callback)

    def add_topology_listener(self, callback: Callable[[], None]
                              ) -> Callable[[], None]:
        """Register an in-process topology-change listener (gateway seam).

        Called (with no arguments) on the **routing loop** whenever the topology
        is (re)broadcast — a participant connecting, disconnecting, going
        ``suspect``/``lost``, or a hosted-plugin change.  The listener reads
        :meth:`topology_snapshot` itself.  Returns an unsubscribe callable.
        """
        self._topology_listeners.append(callback)

        def _remove() -> None:
            try:    self._topology_listeners.remove(callback)
            except ValueError:
                pass
        return _remove

    def topology_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """The current rich topology dict (gateway seam).

        ``{name: {role, plugins:{pname:{...}}, liveness}}`` including the
        broker's own hosted-plugin participant.
        """
        return {name: info.model_dump()
                for name, info in self._participants_for_topology().items()}

    async def get_ui_modules(self) -> Dict[str, str]:
        """Gateway seam: ``{plugin_name: js_content}`` for broker-hosted
        plugins that ship a dynamically-registered ``ui_module``.

        The static plugin JS the Explorer needs for the *packaged* plugins ships
        on disk (``data/plugins/*.js``); a hosted plugin registered at runtime —
        the ``iri.<endpoint>`` instances ``iri_connect`` mints — carries its JS
        as a file the plugin class points at, discoverable only through the
        host.  The read touches host-loop state (``BrokerPluginHost._plugins``),
        so it is fetched **across the host-loop boundary** — the routing loop
        never touches host state.  ``async`` so the gateway (on the routing loop)
        *awaits* the crossing instead of blocking it for up to 5s, which would
        stall WS keepalives.  Empty when the host is not up.
        """
        host = self._plugin_host
        loop = self._host_loop
        if host is None or loop is None:
            return {}

        async def _fetch() -> Dict[str, str]:
            return host.get_ui_modules()

        try:
            cfut = asyncio.run_coroutine_threadsafe(_fetch(), loop)
            return await asyncio.wrap_future(cfut)
        except Exception as e:
            log.debug("[Broker] get_ui_modules failed: %s", e)
            return {}

    # ── lifecycle ─────────────────────────────────────────────────────

    async def startup(self) -> None:
        """Capture the routing loop, start the host thread + the call reaper.

        Idempotent; called from the FastAPI lifespan and directly from unit
        tests that drive the broker without uvicorn.
        """
        if self._started:
            return
        self._loop = asyncio.get_running_loop()
        self._start_host_thread()
        self._reap_task = spawn(self._reap_calls(), 'broker_reap')
        self._started = True

    async def shutdown(self) -> None:
        """Tear down: close sockets, stop the reaper and the host thread."""
        if not self._started:
            return
        self._started = False
        if self._reap_task:
            self._reap_task.cancel()
        for name in list(self.registry):
            self._events.pause_sender(name)
        for name, ws in list(self.registry.items()):
            try:    await ws.close(code=1001)
            except Exception as e:
                log.debug("[Broker] close of %s failed: %s", name, e)
        self.registry.clear()
        self._shutdown_host_plugins()
        self._stop_host_thread()

    def _shutdown_host_plugins(self) -> None:
        """Ask the hosted plugins to stop their background work cleanly.

        Runs the host's plugin-shutdown path *on the host loop* (cancelling each
        plugin's cleanup / reclaim-drain tasks and closing sessions) and waits a
        bounded window, so a subsequent ``_stop_host_thread`` does not tear the
        loop down with plugin tasks still pending (the "Task was destroyed but
        it is pending" warnings)."""
        host = self._plugin_host
        loop = self._host_loop
        if host is None or loop is None:
            return
        try:
            cfut = asyncio.run_coroutine_threadsafe(host.shutdown(), loop)
            cfut.result(timeout=5.0)
        except Exception as e:
            log.debug("[Broker] host plugin shutdown failed: %s", e)

    def _start_host_thread(self) -> None:
        """Bring up the hosted-plugin host on its own loop/thread.

        Plugins are constructed *on* the host loop (some create cleanup tasks
        that need a running loop), so the build is scheduled onto the thread
        and awaited before startup returns.
        """
        ready = threading.Event()

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._host_loop = loop
            loop.call_soon(ready.set)
            loop.run_forever()

        self._host_thread = threading.Thread(
            target=_run, name='broker-host', daemon=True)
        self._host_thread.start()
        if not ready.wait(timeout=5.0):
            raise RuntimeError("broker host loop thread failed to start")

        # The host is always built (empty plugin list is valid) so that
        # ``dst=='broker'`` requests always have somewhere to route — the
        # broker is a participant in its own right.  Plugins are constructed
        # on the host loop (some create cleanup tasks needing a running loop).
        names = [t.strip() for t in self._plugins_spec.split(',') if t.strip()]
        fut: 'concurrent.futures.Future' = concurrent.futures.Future()

        def _build() -> None:
            try:
                host = BrokerPluginHost(
                    names, self._host_broadcast, BROKER_NAME,
                    broker_url=self._url,
                    broker_caller=self.caller, broker_tap=self.tap)
                fut.set_result(host)
            except Exception as e:                         # pragma: no cover
                fut.set_exception(e)

        self._host_loop.call_soon_threadsafe(_build)
        self._plugin_host = fut.result(timeout=30.0)
        if names:
            log.info("[Broker] hosted plugins: %s", names)

    def _stop_host_thread(self) -> None:
        if self._host_loop:
            self._host_loop.call_soon_threadsafe(self._host_loop.stop)
        if self._host_thread:
            self._host_thread.join(timeout=5.0)

    # ── run (uvicorn) ─────────────────────────────────────────────────

    def run(self) -> None:
        """Start uvicorn.  Blocks until shutdown."""
        import uvicorn

        for form in self._url_forms:
            print(f'[Broker] URL: {form}', flush=True)
        # Lifespan (startup/shutdown) is attached at FastAPI construction.
        # Never echo the token (CWE-532); report only its source/path.
        if not self._auth_enabled:
            log.warning("[Broker] ingress authentication DISABLED (--no-auth)")
            print('[Broker] WARNING: ingress authentication DISABLED '
                  '(--no-auth)', flush=True)
        elif self._token_source in ('generated', 'file'):
            verb = ('generated and written to' if self._token_source
                    == 'generated' else 'loaded from')
            print(f'[Broker] auth token {verb}: {utils.TOKEN_FILE}', flush=True)
        else:
            print(f'[Broker] auth token source: {self._token_source}', flush=True)

        uvicorn.run(self._app,
                    host=self._host,
                    port=self._port,
                    reload=False,
                    ssl_certfile=self._cert,
                    ssl_keyfile=self._key,
                    log_level="info",
                    # Transport enforces the frame cap on inbound frames.
                    ws_max_size=self._frame_cap,
                    ws_per_message_deflate=True,
                    # Server-wide WS keepalive disconnects a silent client at
                    # ping_interval + ping_timeout.
                    ws_ping_interval=self._ping_interval,
                    ws_ping_timeout=self._ping_timeout,
                    timeout_graceful_shutdown=3)

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()

    # ── middleware (token gate only — no CORS, that is the gateway's) ──

    def _setup_middleware(self) -> None:
        # When the gateway is attached it installs the fuller HTTP auth gate
        # (with the Explorer's ``/`` + ``/plugins/*`` exemptions); the core's
        # OPTIONS-only gate is only needed for a headless broker.  The WS
        # ``/register`` gate is separate (inside the handler) regardless.
        if self._auth_enabled and not self._gateway_enabled:
            self._app.add_middleware(BaseHTTPMiddleware,
                                     dispatch=self._auth_dispatch)

    async def _auth_dispatch(self, request: Request, call_next):
        """Gate every HTTP route on the shared token.

        The broker core exposes no capability-bearing HTTP surface of its own
        (the WS ``/register`` gate is separate, inside the handler), so the
        only exemption is the CORS preflight.
        """
        if request.method == 'OPTIONS':
            return await call_next(request)
        auth  = request.headers.get('authorization', '')
        token = auth[7:].strip() if auth.lower().startswith('bearer ') else \
                request.cookies.get(utils.AUTH_COOKIE)
        if not utils.tokens_match(token, self._token):
            return JSONResponse(
                status_code=401,
                content=error_dict(401, "missing or invalid broker token"))
        return await call_next(request)

    # ── /register ─────────────────────────────────────────────────────

    def _register_routes(self) -> None:
        self_ = self
        app   = self._app

        @app.websocket("/register")
        async def register(ws: WebSocket):
            await ws.accept()
            name: Optional[str] = None
            clean = False
            try:
                first = await ws.receive_bytes()
                try:
                    msg = protocol.parse_message(first, cap=self_._frame_cap)
                except protocol.ProtocolError as e:
                    log.warning("[Broker] bad first frame: %s", e)
                    await self_._close_quiet(ws)
                    return
                if msg.kind != 'register':
                    log.warning("[Broker] first frame is %r, not register",
                                msg.kind)
                    await self_._close_quiet(ws)
                    return

                ok, name = await self_._register(ws, msg)
                if not ok:
                    # _register already sent the rejecting ack; close cleanly.
                    await self_._close_quiet(ws)
                    return

                while True:
                    data = await ws.receive_bytes()
                    await self_._route_frame(name, data)

            except WebSocketDisconnect as e:
                clean = e.code in (1000, 1001)
            except protocol.ProtocolError as e:
                log.warning("[Broker] protocol error from %s: %s", name, e)
            except Exception as e:
                log.exception("[Broker] connection error (%s): %s", name, e)
            finally:
                if name:
                    await self_._on_socket_drop(name, ws, clean=clean)

    # ── registration + resume ─────────────────────────────────────────

    async def _register(self, ws, msg: protocol.Register) -> Tuple[bool, Optional[str]]:
        """Handle a ``register`` frame; returns ``(ok, name)``.

        Mints a ``resume_key`` on first registration; a reconnect presenting
        the key **replaces** the stale socket; a same-name register without it
        is rejected while the old registration is live or within grace.
        """
        identity = msg.identity
        name     = identity.name

        # Token gate.
        if self._auth_enabled and not utils.tokens_match(
                identity.credential, self._token):
            log.warning("[Broker] register with missing/invalid token: %s", name)
            await self._send(ws, self._ack(False, '', reason='invalid-credential'))
            return False, None

        # Reserved name.
        if name == BROKER_NAME:
            await self._send(ws, self._ack(False, '', reason='name-reserved'))
            return False, None

        self._prof.prof('broker_register', uid=name)

        if name in self.registry:
            # Name occupied (present or suspect-within-grace): require the key.
            if identity.resume_key and utils.tokens_match(
                    identity.resume_key, self._resume_keys.get(name)):
                self._prof.prof('broker_resume', uid=name)
                old = self.registry[name]
                self.registry[name] = ws                  # install new first
                self._cancel_grace(name)
                self._liveness[name] = 'present'
                self._participants[name]['role'] = msg.role
                self._participants[name]['plugins'] = \
                    self._plugins_list_to_dict(msg.plugins)
                self._events.restart_sender(name)
                await self._send(ws, self._ack(True, self._resume_keys[name]))
                # Close the old socket; its teardown no-ops via the guard.
                if old is not ws:
                    spawn(self._close_quiet(old), 'broker_close_old')
                # Broadcast reaches the resumed endpoint too (full topology).
                await self._broadcast_topology()
                return True, name

            log.warning("[Broker] name-in-use (no/invalid resume_key): %s", name)
            await self._send(ws, self._ack(False, '', reason='name-in-use'))
            return False, None

        # Fresh registration.
        key = protocol.mint_id()
        self.registry[name]       = ws
        self._resume_keys[name]   = key
        self._liveness[name]      = 'present'
        self._participants[name]  = {
            'role':    msg.role,
            'plugins': self._plugins_list_to_dict(msg.plugins),
        }
        self._events.add_endpoint(name)

        await self._send(ws, self._ack(True, key))
        # Broadcast reaches the newcomer too — its full-topology snapshot.
        await self._broadcast_topology()
        return True, name

    def _ack(self, ok: bool, resume_key: str,
             reason: Optional[str] = None) -> bytes:
        return protocol.pack_message(protocol.RegisterAck(
            src=BROKER_NAME, ok=ok, reason=reason, resume_key=resume_key),
            cap=self._frame_cap)

    # ── frame routing (the lean loop) ─────────────────────────────────

    async def _route_frame(self, name: str, data: bytes) -> None:
        """Dispatch one inbound frame.  Pure msgpack dict ops — the forwarding
        fast path never builds a pydantic model."""
        raw  = msgpack.unpackb(data, raw=False)
        kind = raw.get('kind')

        if   kind == 'request':      await self._route_request(name, raw)
        elif kind == 'response':     await self._route_response(name, raw)
        elif kind == 'event':        self._events.ingest(name, raw)
        elif kind == 'subscribe':    self._events.update_subscription(name, raw, True)
        elif kind == 'unsubscribe':  self._events.update_subscription(name, raw, False)
        elif kind == 'control':      await self._handle_control(name, raw)
        elif kind == 'register':
            log.warning("[Broker] unexpected mid-stream register from %s", name)
        else:
            log.debug("[Broker] unknown frame kind %r from %s", kind, name)

    async def _route_request(self, src_name: str, raw: dict) -> None:
        # Never trust a client-supplied src — overwrite with the sender's
        # registered name.
        raw['src'] = src_name
        dst        = raw.get('dst')
        corr_id    = raw.get('corr_id')
        self._prof.prof('broker_route_request', uid=corr_id or '',
                        msg='%s->%s' % (src_name, dst))

        if dst == BROKER_NAME:
            spawn(self._dispatch_to_host(src_name, raw), 'broker_host_dispatch')
            return

        target = self.registry.get(dst)
        if target is None:
            await self._send(self.registry.get(src_name),
                             self._error_response(raw, 502,
                                                  f"endpoint {dst!r} unknown"))
            return

        if len(self._calls) >= self._forwarded_call_cap:
            await self._send(self.registry.get(src_name),
                             self._error_response(raw, 503,
                                                  "broker forwarded-call table "
                                                  "at cap"))
            return

        self._calls[corr_id] = _Call(src_name, dst, None,
                                     self._loop.time() + self._call_timeout)
        await self._send(target, msgpack.packb(raw, use_bin_type=True))

    async def _route_response(self, src_name: str, raw: dict) -> None:
        corr_id = raw.get('corr_id')
        dst     = raw.get('dst')
        call    = self._calls.pop(corr_id, None)
        self._prof.prof('broker_route_response', uid=corr_id or '',
                        msg='%s->%s' % (src_name, dst))

        if dst == BROKER_NAME:
            self._settle_call(call, raw)
            return
        target = self.registry.get(dst)
        if target is not None:
            await self._send(target, msgpack.packb(raw, use_bin_type=True))

    # ── broker-as-caller (in-flight table) ────────────────────────────

    async def _broker_call(self, dst: str, method: str, path: str, *,
                           body:    bytes,
                           headers: Optional[Dict[str, str]],
                           timeout: Optional[float]) -> Dict[str, Any]:
        # Cap on broker-originated calls — overflow raises at the caller.
        n_broker = sum(1 for c in self._calls.values() if c.future is not None)
        if n_broker >= self._call_cap:
            raise RuntimeError(
                f"broker in-flight calls at cap ({self._call_cap}): too many "
                "broker-originated calls")

        req     = protocol.make_request(BROKER_NAME, dst, method, path,
                                        headers=headers, body=body)
        corr_id = req.corr_id
        target  = self.registry.get(dst)
        if target is None:
            raise RuntimeError(f"endpoint {dst!r} unknown")

        fut = self._loop.create_future()
        self._calls[corr_id] = _Call(BROKER_NAME, dst, fut,
                                     self._loop.time() + self._call_timeout)
        try:
            await self._send(target, protocol.pack_message(req, cap=self._frame_cap))
            return await asyncio.wait_for(fut, timeout=timeout)
        except Exception:
            self._calls.pop(corr_id, None)
            raise

    @staticmethod
    def _settle_call(call: Optional['_Call'], resp: Dict[str, Any]) -> None:
        """Resolve a broker-originated call's future (no-op for forwarded)."""
        if call is not None and call.future is not None and not call.future.done():
            call.future.set_result(resp)

    # ── hosted-plugin host dispatch (across the thread boundary) ──────

    async def _dispatch_to_host(self, src_name: str, raw: dict) -> None:
        """Route a ``dst='broker'`` request into the host and reply.

        A participant consumer discovers the broker's hosted plugins with the
        full ``/broker/{plugin}`` namespace (the topology display form), so it
        builds request paths rooted there; the host matches routes on the
        endpoint-relative ``/{plugin}`` form.  Strip the leading ``/broker``
        segment before dispatch — the same normalization the gateway proxy does
        by URL routing (``endpoint_name == 'broker'`` → forward the remainder).
        """
        resp = await self.call_host(
            raw.get('method', 'GET'),
            self._strip_broker_prefix(raw.get('path', '/')),
            headers=raw.get('headers', {}), body=raw.get('body', b''))
        out = protocol.Response(
            src=BROKER_NAME, dst=src_name, status=int(resp['status']),
            headers={k: v for k, v in (resp['headers'] or {}).items()},
            body=resp['body'], corr_id=raw.get('corr_id'))
        target = self.registry.get(src_name)
        if target is not None:
            await self._send(target, protocol.pack_message(out, cap=self._frame_cap))

    async def call_host(self, method: str, path: str, *,
                        headers: Optional[Dict[str, str]] = None,
                        body: Any = b'') -> Dict[str, Any]:
        """Invoke a broker-hosted plugin route across the thread boundary.

        Gateway seam: the HTTP catch-all proxies a ``dst=='broker'`` request
        here instead of 404-ing on the routing-loop registry.  Returns a
        ``{status, headers, body}`` dict (body is bytes), the same passthrough
        shape :meth:`_dispatch_to_host` wraps into a wire ``response``."""
        if self._plugin_host is None:
            return {'status': 503, 'headers': {},
                    'body': error_body(503, "no broker host")}
        query = ''
        if '?' in path:
            path, query = path.split('?', 1)
        try:
            cfut = asyncio.run_coroutine_threadsafe(
                self._plugin_host.handle_request(
                    method       = method,
                    path         = path,
                    headers      = headers or {},
                    body_bytes   = self._as_bytes(body),
                    query_string = query),
                self._host_loop)
            resp = await asyncio.wrap_future(cfut)
            return {'status':  resp.status_code,
                    'headers': dict(resp.headers),
                    'body':    bytes(resp.body)}
        except HTTPException as e:
            return {'status': e.status_code,
                    'headers': {'content-type': 'application/json'},
                    'body': error_body(e.status_code, e.detail)}
        except Exception as e:
            log.exception("[Broker] host dispatch failed: %s", e)
            return {'status': 500,
                    'headers': {'content-type': 'application/json'},
                    'body': error_body(500, str(e))}

    async def _host_broadcast(self, topic: str, data: dict) -> None:
        """``broadcast_fn`` handed to :class:`BrokerPluginHost`.

        Runs on the host loop and hands work to the routing loop: a hosted
        plugin's ``notification`` becomes a broker ``event`` fanned out on the
        routing loop; a ``topology`` change (a dynamically registered plugin)
        triggers a full topology rebroadcast there.  ``ts``/``seq`` are left
        unset — the event router assigns them on ingest.
        """
        if self._loop is None:
            return
        if topic == 'notification':
            raw = {
                'kind':    'event',
                'version': protocol.PROTOCOL_VERSION,
                'src':     BROKER_NAME,
                'plugin':  data.get('plugin'),
                'topic':   data.get('topic'),
                'session': None,
                'data':    data.get('data') or {},
            }
            self._loop.call_soon_threadsafe(
                self._events.ingest, BROKER_NAME, raw)
        elif topic == 'topology':
            self._loop.call_soon_threadsafe(self._spawn_topology_broadcast)

    def _spawn_topology_broadcast(self) -> None:
        """Schedule a full topology rebroadcast on the routing loop."""
        spawn(self._broadcast_topology(), 'broker_host_topology')

    # ── control ───────────────────────────────────────────────────────

    async def _handle_control(self, name: str, raw: dict) -> None:
        op = raw.get('op')
        if op == 'terminate':
            # Process floor: self-SIGTERM (uvicorn's own handler stops it),
            # mirroring broker.terminate_broker.
            log.info("[Broker] terminate requested by %s", name)
            spawn(self._delayed_terminate(), 'broker_terminate')
        elif op == 'disconnect':
            target = (raw.get('data') or {}).get('name')
            if target:
                await self._disconnect(target)

    async def _delayed_terminate(self) -> None:
        await asyncio.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)

    async def _disconnect(self, target: str) -> None:
        """Operator disconnect: clean close skips suspect (immediate removal)."""
        ws = self.registry.get(target)
        if ws is None:
            return
        self._remove_participant(target)
        await self._broadcast_topology()
        spawn(self._close_quiet(ws), 'broker_disconnect_close')

    # ── liveness ──────────────────────────────────────────────────────

    async def _on_socket_drop(self, name: str, ws, clean: bool = False) -> None:
        """Teardown guard: a replaced socket's handler fires its ``finally``
        after resume installed the new socket — proceed only if this socket is
        still the live one."""
        if self.registry.get(name) is not ws:
            log.debug("[Broker] stale socket drop for %s ignored (replaced)", name)
            return

        if clean:
            # A clean close skips suspect — immediate removal + broadcast.
            self._remove_participant(name)
            await self._broadcast_topology()
            return

        self._prof.prof('broker_suspect', uid=name)
        self._liveness[name] = 'suspect'
        # Pause event delivery on the dead socket; buffered events survive.
        self._events.pause_sender(name)
        await self._broadcast_topology()
        self._arm_grace(name, ws)

    def _arm_grace(self, name: str, ws) -> None:
        self._cancel_grace(name)
        self._grace_timers[name] = self._loop.call_later(
            self._grace, self._fire_lost, name, ws)

    def _cancel_grace(self, name: str) -> None:
        t = self._grace_timers.pop(name, None)
        if t is not None:
            t.cancel()

    def _fire_lost(self, name: str, ws) -> None:
        self._grace_timers.pop(name, None)
        spawn(self._on_lost(name, ws), 'broker_lost:%s' % name)

    async def _on_lost(self, name: str, ws) -> None:
        # Same guard on the lost path.
        if self.registry.get(name) is not ws:
            return

        self._prof.prof('broker_lost', uid=name)
        self._remove_participant(name)
        await self._fail_inflight_for(name)
        await self._broadcast_topology()

    async def _fail_inflight_for(self, name: str) -> None:
        """Fast-fail every in-flight call touching *name*.

        A ``dst==name`` entry is fast-failed with a synthesized 504 (the future
        resolves for a broker-originated call, the requester gets the 504 for a
        forwarded one).  A ``src==name`` entry (the requester itself is gone) is
        simply dropped — its response would go nowhere.
        """
        for corr_id, call in list(self._calls.items()):
            if call.dst == name:
                self._calls.pop(corr_id, None)
                await self._fail_call(corr_id, call, 'endpoint lost')
            elif call.src == name:
                # Requester gone — the response would go nowhere; drop it.
                self._calls.pop(corr_id, None)

    async def _fail_call(self, corr_id: str, call: '_Call', reason: str) -> None:
        """Synthesize a 504 for *call*: resolve its future, or send it upstream."""
        if call.future is not None:
            self._settle_call(call, {
                'kind': 'response', 'status': 504,
                'src': call.dst, 'dst': BROKER_NAME, 'corr_id': corr_id,
                'headers': {}, 'body': b'', 'reason': reason})
        else:
            target = self.registry.get(call.src)
            if target is not None:
                out = protocol.Response(
                    src=call.dst, dst=call.src, status=504,
                    body=reason.encode(), corr_id=corr_id)
                await self._send(target, protocol.pack_message(
                    out, cap=self._frame_cap))

    def _remove_participant(self, name: str) -> None:
        """Free name + resume_key + meta + delivery machinery (no broadcast)."""
        self.registry.pop(name, None)
        self._participants.pop(name, None)
        self._liveness.pop(name, None)
        self._resume_keys.pop(name, None)
        self._cancel_grace(name)
        self._events.remove_endpoint(name)

    # ── call reaper ───────────────────────────────────────────────────

    async def _reap_calls(self) -> None:
        """Evict in-flight calls past their deadline.

        Bounds the forwarded side of the in-flight table: a responder that
        simply never answers (a buggy plugin) would otherwise leak an entry
        forever.  Broker-originated calls usually clear via their own
        ``wait_for`` timeout; the deadline is the backstop for both.
        """
        while self._started:
            try:
                await asyncio.sleep(self._reap_interval)
            except asyncio.CancelledError:
                return
            now = self._loop.time()
            for corr_id, call in list(self._calls.items()):
                if call.deadline <= now:
                    self._calls.pop(corr_id, None)
                    await self._fail_call(corr_id, call, 'call timed out')

    # ── topology ──────────────────────────────────────────────────────

    def _participants_for_topology(self) -> Dict[str, protocol.ParticipantInfo]:
        parts: Dict[str, protocol.ParticipantInfo] = {}
        for name, info in self._participants.items():
            parts[name] = protocol.ParticipantInfo(
                role=info.get('role', 'endpoint'),
                plugins=info.get('plugins', {}),
                liveness=self._liveness.get(name, 'present'))
        # The broker is itself a participant (its hosted-plugin host).
        broker_plugins: Dict[str, Dict[str, Any]] = {}
        if self._plugin_host is not None:
            broker_plugins = \
                self._plugin_host.get_topology_info().get('plugins', {})
        parts[BROKER_NAME] = protocol.ParticipantInfo(
            role='broker', plugins=broker_plugins, liveness='present')
        return parts

    def _build_topology_msg(self) -> bytes:
        topo = protocol.Topology(src=BROKER_NAME,
                                 participants=self._participants_for_topology())
        return protocol.pack_message(topo, cap=self._frame_cap)

    async def _broadcast_topology(self) -> None:
        packed = self._build_topology_msg()
        for name, ws in list(self.registry.items()):
            await self._send(ws, packed)
        # Deliver the rich topology to the hosted-plugin host (task_dispatcher
        # consumes it for pilot-child liveness + owner-session reclaim-drain).
        self._deliver_topology_to_host()
        # Notify in-process topology listeners (the gateway's SSE fan-out).
        for cb in list(self._topology_listeners):
            try:    cb()
            except Exception as e:
                log.error("[Broker] topology listener failed: %r", e, exc_info=e)

    def _deliver_topology_to_host(self) -> None:
        """Hand the rich topology to hosted plugins on the host loop.

        Synthesizes a ``liveness='lost'`` entry for a participant that was
        ``present``/``suspect`` in the previous delivery but is now gone (the
        post-grace ``lost`` the wire never carries), mirroring the runtime's
        served-plugin delivery.  A fresh broker starts with an empty ``prev``,
        so a not-yet-reconnected child is never synthesized ``lost`` (it was
        never ``present`` in this broker's view)."""
        if self._plugin_host is None or self._host_loop is None:
            return
        curr = self.topology_snapshot()
        participants = dict(curr)
        for name, info in self._host_topology_prev.items():
            if name in curr:
                continue
            if (info or {}).get('liveness') in ('present', 'suspect'):
                lost = dict(info)
                lost['liveness'] = 'lost'
                participants[name] = lost
        self._host_topology_prev = curr
        try:
            cfut = asyncio.run_coroutine_threadsafe(
                self._plugin_host.on_topology_change(participants),
                self._host_loop)
        except Exception as e:
            log.debug("[Broker] host topology delivery failed: %s", e)
            return

        def _log_delivery(f: 'concurrent.futures.Future') -> None:
            if f.cancelled():
                return
            exc = f.exception()
            if exc is not None:
                log.error("[Broker] host topology delivery failed: %r", exc,
                          exc_info=exc)

        cfut.add_done_callback(_log_delivery)

    # ── low-level helpers ─────────────────────────────────────────────

    async def _send(self, ws, data: bytes) -> None:
        if ws is None:
            return
        try:
            await ws.send_bytes(data)
        except Exception as e:
            log.debug("[Broker] send failed: %s", e)

    @staticmethod
    async def _close_quiet(ws) -> None:
        try:    await ws.close(code=1000)
        except Exception:
            pass

    def _error_response(self, req_raw: dict, status: int, detail: str) -> bytes:
        out = protocol.Response(
            src=BROKER_NAME, dst=req_raw.get('src'), status=status,
            headers={'content-type': 'application/json'},
            body=error_body(status, detail),
            corr_id=req_raw.get('corr_id'))
        return protocol.pack_message(out, cap=self._frame_cap)

    @staticmethod
    def _strip_broker_prefix(path: str) -> str:
        """Drop a leading ``/broker`` segment from *path* (query preserved)."""
        prefix = '/' + BROKER_NAME
        if path == prefix:
            return '/'
        if path.startswith(prefix + '/'):
            return path[len(prefix):]
        if path.startswith(prefix + '?'):
            # '/broker?foo=bar' -> '/?foo=bar' (bare prefix + query string).
            return '/' + path[len(prefix):]
        return path

    @staticmethod
    def _as_bytes(body) -> bytes:
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode()
        return b''

    @staticmethod
    def _plugins_list_to_dict(plugins: List[Dict[str, Any]]
                              ) -> Dict[str, Dict[str, Any]]:
        """Normalise ``register.plugins`` (a list of per-plugin dicts) into the
        topology's name-keyed rich dict.  Each entry names itself via ``name``
        or ``type``; anonymous entries fall back to positional keys."""
        out: Dict[str, Dict[str, Any]] = {}
        for i, p in enumerate(plugins or []):
            key = p.get('name') or p.get('type') or 'plugin.%d' % i
            out[key] = p
        return out
