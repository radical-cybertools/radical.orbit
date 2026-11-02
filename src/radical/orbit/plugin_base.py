import re
import math
import uuid
import asyncio
import inspect
import functools
import logging
import time

from dataclasses import dataclass
from collections import OrderedDict
from typing import Type, Optional, Dict, Callable, Any, Union
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .ui_schema import UIConfig, ui_config_to_dict
from .utils import HOST_ROLES

log = logging.getLogger("radical.orbit")

# Reserved session ID.  The 'default' session is persistent and per plugin
# instance — a `rhapsody` default and a `psij` default are distinct sessions.
DEFAULT_SID = 'default'

ISSUES_URL  = 'https://github.com/radical-cybertools/radical.orbit/issues'
ENDED_KEEP  = 100_000     # ended sids remembered (for the 410), oldest dropped


@dataclass
class SessionRecord:
    """Lifetime bookkeeping for one session (see the policy table on `Plugin`).

    The session *object* lives in ``Plugin._sessions[sid]``; this record holds
    only the fields the expiry sweep needs.
    """
    lifetime:       str                       # 'ephemeral' | 'ttl' | 'persistent'
    ttl:            Optional[float]            # seconds, for lifetime == 'ttl'
    owner:          Optional[str]             # x-orbit-src (diagnostics only)
    last_access:    float
    client:         Optional[str]   = None    # X-Orbit-Client (e.g. explorer)
    inflight:       int             = 0       # {sid} requests still running


class _LastAccessView:
    """Backward-compat mapping shim over ``SessionRecord.last_access``.

    Lets plugins that predate the SessionRecord merge (globus / iri /
    rhapsody) keep bumping access time via
    ``self._session_last_access[sid] = ts``.  Writing an unknown ``sid``
    materialises an owner-less ephemeral record (the record-less default).
    Remove once those plugins record last_access on the record directly.
    """

    def __init__(self, records: Dict[str, "SessionRecord"],
                 ended: Optional[Dict[str, tuple]] = None):
        self._records = records
        self._ended   = ended if ended is not None else {}

    def __setitem__(self, sid: str, ts: float) -> None:
        rec = self._records.get(sid)
        if rec is None:
            if sid in self._ended:          # never revive an ended session
                return
            self._records[sid] = SessionRecord('ephemeral', None, None, ts)
        else:
            rec.last_access = ts

    def __getitem__(self, sid: str) -> float:
        return self._records[sid].last_access

    def get(self, sid: str, default: Any = None) -> Any:
        rec = self._records.get(sid)
        return rec.last_access if rec is not None else default

    def __contains__(self, sid: str) -> bool:
        return sid in self._records

    def pop(self, sid: str, default: Any = None) -> Any:
        rec = self._records.pop(sid, None)
        return rec.last_access if rec is not None else default

    def __delitem__(self, sid: str) -> None:
        self._records.pop(sid, None)


class Plugin(object):
    """
    Base class for Endpoint plugins.

    Each plugin gets its own URL namespace and built-in session management.
    Routes are added with `add_route_post` / `add_route_get`.

    **plugin_name vs instance_name**

    ``plugin_name`` is a *class-level* attribute that uniquely identifies the
    plugin type (e.g. ``"psij"``, ``"queue_info"``).  It is the key used in
    the global ``Plugin._registry`` and in client-side lookups
    (``endpoint.get_plugin("psij")``).

    ``instance_name`` is set at *construction time* (defaults to
    ``plugin_name`` when only one instance is needed) and drives the URL
    namespace: ``/{instance_name}/…``.  Multiple instances of the same plugin
    type on the same endpoint must be given distinct instance names.

    Subclasses that define a `plugin_name` class attribute will be
    automatically registered in the global plugin registry.

    Subclasses must define::

        session_class: The session class to instantiate (must inherit from PluginSession)

    Subclasses may define::

        client_class: The local helper class for the application-side client.
        version: The version string for the plugin.
        session_ttl: Default idle timeout in seconds of this plugin's
            sessions (default: 900 = 15 min, 0 = never expire); a session
            can set its own at creation (``register_session(ttl=...)``).
        ui_config: UI configuration dict for portal rendering (see ui_schema.py)

    Session lifetime policy
    -----------------------
    Every session has one **idle timeout**, fixed when it is created:
    ``register_session(ttl=<seconds>)``, ``ttl=0`` for never, else the
    plugin's ``session_ttl``.  (The legacy ``lifetime`` field maps onto it:
    ``persistent`` -> never, ``ephemeral`` -> the plugin default; an explicit
    ``ttl`` wins.)  A session expires once idle for longer than its timeout.
    Activity is every request on a ``{sid}`` route (for as long as it runs),
    every notification the session emits, and -- probed only at the
    deadline -- :meth:`PluginSession.is_busy` (work in flight that emits no
    events).  Known limit: work that never reaches a final state keeps its
    session alive.

    Any client holding the sid can attach to a session (reconnect, share it
    between e.g. a portal and a CLI); owners are recorded for diagnostics
    only and never govern lifetime or attachment.  The reserved ``default``
    session is persistent and per plugin instance.

    An expiry is logged with its reason (an idle one as likely API misuse,
    naming the remedy and the issue tracker; for an Explorer session, tagged
    by its ``X-Orbit-Client: explorer`` header, as the normal end of a
    browser tab).  Ended sids -- expired or unregistered, unless persistent
    -- are remembered for the process lifetime: any later request on one,
    including a reconnect, gets 410 with that explanation.

    Notifications
    -------------
    A session notifies via ``self.notify(topic, data)``; a plugin via
    ``await self.send_notification(topic, data)``.  Both flow Session ->
    Plugin -> runtime -> Broker -> subscribed consumers (and the gateway's SSE
    fan-out), reaching HTTP clients over ``/events``.

    Topology updates
    ----------------
    The serving runtime delivers the rich topology (``name -> {role, plugins,
    liveness}``) on every change.  The base ``on_topology_change`` is a no-op
    hook; override it to react to connect/disconnect/liveness::

        async def on_topology_change(self, participants: dict):
            for name, info in participants.items():
                print(f"{name}: {info.get('liveness')}")
            await super().on_topology_change(participants)
    """

    _registry: Dict[str, Type["Plugin"]] = {}
    session_class: Optional[Type[PluginSession]] = None
    client_class: Optional[Type] = None
    version: str = '0.0.1'
    session_ttl: int = 900  # default idle timeout (s) of a session; 0: never
    ui_config: Union[Dict, UIConfig, None] = None  # UI configuration for portal
    ui_module: Optional[str] = None  # Absolute path to JS plugin module, or None
    # Host roles (``utils.HOST_ROLES``) whose ``default`` plugin set
    # includes this plugin.  An own declaration wins; otherwise a class
    # inherits its parent's roles only under the same identity (the
    # parent's ``plugin_name``, or a parent without one), else gets ().
    default_roles: tuple[str, ...] = ()

    def __init_subclass__(cls, **kwargs):
        """Validate ``default_roles`` and auto-register subclasses that
        define plugin_name.

        Raises on an invalid ``default_roles``: the class is then never
        registered, and entry-point discovery isolates the failure.
        """
        super().__init_subclass__(**kwargs)
        if 'default_roles' in cls.__dict__:
            roles = cls.__dict__['default_roles']
        else:
            # Parents hold their effective roles (stored below); the
            # decision depends on plugin names only.
            parent_name = next((k.__dict__['plugin_name']
                                for k in cls.__mro__[1:]
                                if 'plugin_name' in k.__dict__), None)
            same = parent_name is None \
                or getattr(cls, 'plugin_name', None) == parent_name
            roles = super(cls, cls).default_roles if same else ()
        # Validate declared and inherited roles alike (a mixin may supply
        # them): a bare str would otherwise become a tuple of characters.
        if not isinstance(roles, (tuple, list, set, frozenset)) \
                or not all(isinstance(r, str) for r in roles):
            raise TypeError(
                f"{cls.__name__}.default_roles must be a tuple of role "
                f"names, not {roles!r}")
        unknown = set(roles) - set(HOST_ROLES)
        if unknown:
            raise ValueError(
                f"{cls.__name__}.default_roles: unknown role(s) "
                f"{sorted(unknown)}; valid: {list(HOST_ROLES)}")
        cls.default_roles = tuple(roles)
        if hasattr(cls, 'plugin_name'):
            name = getattr(cls, 'plugin_name')
            if name in Plugin._registry:
                log.warning("[Plugin] Duplicate plugin_name '%s' - overwriting "
                            "(the new class's default_roles apply)", name)
            Plugin._registry[name] = cls
            log.debug("[Plugin] Registered plugin: %s -> %s", name, cls.__name__)

    @classmethod
    def get_plugin_class(cls, name: str) -> Optional[Type]:
        """Look up a registered plugin class by name."""
        return cls._registry.get(name)

    @classmethod
    def get_plugin_names(cls) -> list[str]:
        """Get a list of registered plugin names."""
        return list(cls._registry.keys())

    def __init__(self, app: FastAPI, instance_name: Optional[str] = None):
        """
        Initialize the Plugin with a FastAPI app and instance name.
        Also sets up built-in session management.

        Args:
            app: The FastAPI application instance.
            instance_name: The name of the plugin instance, used in the
                namespace.  Defaults to the class ``plugin_name`` (the common
                single-instance case).
        """
        if instance_name is None:
            instance_name = getattr(type(self), 'plugin_name', None)
        if not instance_name:
            raise ValueError("instance_name is required "
                             "(no class plugin_name to default to)")

        self._app: FastAPI = app
        self._instance_name: str = instance_name
        self._uid: str = str(uuid.uuid4())
        self._namespace: str = f"/{self._instance_name}"
        self._start_time: float = time.time()
        self._role: Optional[str] = None  # cached host role (lazy)

        # Session store: the session objects, keyed by sid.
        self._sessions: Dict[str, PluginSession] = {}
        # Single source of truth for lifetime bookkeeping, keyed by sid.
        self._records: Dict[str, SessionRecord] = {}
        # Backward-compat mapping shim over record.last_access (see class).
        # sid -> (time, reason, timeout, client) of ended sessions, for the
        # 410 a later request on them gets (see _end_session)
        self._ended: 'OrderedDict[str, tuple]' = OrderedDict()
        self._session_last_access = _LastAccessView(self._records, self._ended)
        # Guards on-demand creation of the reserved 'default' session so two
        # concurrent first requests cannot double-create it.
        self._default_lock = asyncio.Lock()
        # Loop used to schedule thread-origin notifications.  Captured at
        # construction (the host builds plugins on its loop); refreshed on the
        # first async notify if construction happened outside a loop.
        try:
            self._main_loop: Optional[asyncio.AbstractEventLoop] = \
                asyncio.get_running_loop()
        except RuntimeError:
            self._main_loop = None
        self._cleanup_task: Optional[asyncio.Task] = None

        # Shared direct-dispatch route table (one list across all plugins).
        # We also track the entries this particular plugin instance owns,
        # so a dynamic-plugin host can strip them on deregister without
        # having to guess by handler identity or path prefix.
        if not hasattr(self._app.state, 'direct_routes'):
            self._app.state.direct_routes = []
        self._owned_routes: list = []

        # Built-in session management routes
        self.add_route_post('register_session', self.register_session)
        self.add_route_post('unregister_session/{sid}', self.unregister_session)
        self.add_route_post('touch/{sid}', self.touch_session)
        self.add_route_get('version', self.get_version)
        self.add_route_get('list_sessions', self.list_sessions)
        self.add_route_get('health', self.health_check)
        self.add_route_get('ui_config', self.get_ui_config)
        self._log_routes()

    # Role classification — one cached lookup (utils.host_role) backs the four
    # boolean views, so the role / scheduler / executor decision lives in
    # exactly one place.
    @property
    def role(self) -> str:
        """This host's role: 'broker' | 'login' | 'compute' | 'standalone'."""
        if self._role is None:
            from .utils import host_role
            self._role = host_role(self._app)['role']
        return self._role

    @property
    def is_broker(self) -> bool:
        """True when this plugin is hosted on the broker (not on an endpoint)."""
        return self.role == 'broker'

    @property
    def is_compute_node(self) -> bool:
        """True when running inside a batch job allocation (compute node)."""
        return self.role == 'compute'

    @property
    def is_login_node(self) -> bool:
        """True on an HPC login node — a real scheduler is installed but
        no allocation is active."""
        return self.role == 'login'

    @property
    def is_standalone(self) -> bool:
        """True for a non-HPC host (no batch scheduler installed)."""
        return self.role == 'standalone'

    @property
    def namespace(self) -> str:
        """Get the namespace of the plugin."""
        return self._namespace

    @property
    def instance_name(self) -> str:
        """Get the instance name of the plugin."""
        return self._instance_name

    @property
    def uid(self) -> str:
        """Get the unique ID of the plugin instance."""
        return self._uid

    def add_route_post(self, path: str, method: Callable):
        """Add a POST route to the plugin's namespace."""
        self._add_route(path, "POST", method)

    def add_route_get(self, path: str, method: Callable):
        """Add a GET route to the plugin's namespace."""
        self._add_route(path, "GET", method)

    def add_route_delete(self, path: str, method: Callable):
        """Add a DELETE route to the plugin's namespace.

        Direct dispatch (``_register_direct``) and the broker plugin host
        are method-generic, and the gateway catch-all already lists
        ``DELETE`` -- so a DELETE route is reachable over every transport
        without further plumbing.
        """
        self._add_route(path, "DELETE", method)

    def _add_route(self, path: str, verb: str, handler: Callable):
        """Register *handler* for *verb* on *path* in the plugin's namespace,
        for both direct dispatch and the ASGI app."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        if path == 'register_session':
            handler = self._tagging_client(handler)
        elif '{sid}' in path and path != 'unregister_session/{sid}':
            handler = self._accessing(handler)
        self._register_direct(full_path, verb, handler)
        self._app.add_route(full_path, self._wrap_handler(handler),
                            methods=[verb])

    def _accessing(self, handler: Callable) -> Callable:
        """Wrap a ``{sid}`` route: an ended or expired session answers 410
        with its reason; otherwise the request is an access of the session,
        for as long as it runs (also for plugins that look sessions up
        directly instead of through :meth:`_forward`)."""
        @functools.wraps(handler)
        async def _accessed(request):
            sid = (getattr(request, 'path_params', None) or {}).get('sid')
            rec = self._records.get(sid) if sid else None
            if sid and rec is None:
                text = self._ended_detail(sid)
                if text:
                    raise HTTPException(status_code=410, detail=text)
            if rec is None:
                return await handler(request)
            text = await self._expire_if_due(sid, rec, time.time())
            if text:
                raise HTTPException(status_code=410, detail=text)
            self._tag_client(rec, request)
            rec.inflight += 1
            try:
                return await handler(request)
            finally:
                rec.inflight   -= 1
                rec.last_access = time.time()
        return _accessed

    def _tagging_client(self, handler: Callable) -> Callable:
        """Wrap register_session (plugins override it): the session it
        returns records the caller's ``X-Orbit-Client`` tag."""
        @functools.wraps(handler)
        async def _tagged(request):
            result = await handler(request)
            sid = result.get('sid') if isinstance(result, dict) else None
            rec = self._records.get(sid) if sid else None
            if rec is not None:
                self._tag_client(rec, request)
            return result
        return _tagged

    @staticmethod
    def _tag_client(rec: SessionRecord, request) -> None:
        """Record the ``X-Orbit-Client`` header (client-supplied: it only
        selects how an idle expiry is logged, never expiry itself).  A
        session accessed by different clients (e.g. the Explorer and a
        script) is tagged ``mixed``, so it is never logged as the Explorer's
        alone."""
        client = Plugin._request_header(request, 'x-orbit-client') or 'other'
        if rec.client is None or rec.client == client:
            rec.client = client
        else:
            rec.client = 'mixed'

    @staticmethod
    def _wrap_handler(handler: Callable) -> Callable:
        """Wrap a dict-returning handler for ASGI compatibility.

        Handlers return plain dicts on the direct-dispatch path.
        The FastAPI/ASGI path (TestClient, Explorer UI) needs a
        ``JSONResponse`` wrapper.
        """
        async def _wrapped(request):
            result = await handler(request)
            if not hasattr(result, 'status_code'):
                return JSONResponse(result)
            return result
        return _wrapped

    def _register_direct(self, path: str, method: str, handler: Callable):
        """Compile '{param}' path pattern into regex, register for direct dispatch."""
        parts       = path.strip('/').split('/')
        regex_parts = []
        param_names = []
        for part in parts:
            if part.startswith('{') and part.endswith('}'):
                param_names.append(part[1:-1])
                regex_parts.append('([^/]+)')
            else:
                regex_parts.append(re.escape(part))
        pattern = re.compile('^/' + '/'.join(regex_parts) + '$')
        entry = (method, pattern, tuple(param_names), handler)
        self._app.state.direct_routes.append(entry)
        self._owned_routes.append(entry)

    def _create_session(self, sid: str, **kwargs) -> PluginSession:
        """
        Factory method to create a session instance.

        Injects a reference to this plugin so the session can call
        `_dispatch_notify` without a per-session closure.
        """
        if self.session_class is None:
            raise RuntimeError(f"[{self.instance_name}] session_class not defined")
        session = self.session_class(sid, **kwargs)
        session._plugin = self
        return session

    def _dispatch_notify(self, topic: str, data: dict) -> None:
        """
        Schedule a notification to be sent asynchronously.

        Called by sessions via ``self._plugin._dispatch_notify(topic, data)``.
        Works from both async contexts and background threads.

        Args:
            topic: Notification topic string.
            data:  Notification payload dict.
        """
        async def _send():
            try:
                await self.send_notification(topic, data)
            except Exception as e:
                log.error("[%s] Notification send failed for %s: %s",
                          self.instance_name, topic, e)

        try:
            loop = asyncio.get_running_loop()
            if self._main_loop is None:
                self._main_loop = loop
            loop.create_task(_send())
        except RuntimeError:
            # Called from a background thread — use the loop captured at
            # construction (or on the first async notify).
            if self._main_loop is not None:
                asyncio.run_coroutine_threadsafe(_send(), self._main_loop)
            else:
                log.warning("[%s] Dropping '%s' notification: no event loop "
                            "captured for this plugin", self.instance_name,
                            topic)

    async def register_session(self, request: Request) -> dict:
        """Register (or reconnect to) a session and return its session ID.

        Session policy
        --------------
        The request body (JSON, all fields optional) selects a lifetime
        policy and, for reconnect, a client-supplied session ID::

            {"sid": <str>, "lifetime": "ephemeral"|"ttl"|"persistent",
             "ttl": <seconds>}

        - ``sid`` omitted                    -> a fresh session ID is minted.
        - ``sid`` names an existing session  -> *reconnect* (by any client):
          an access; the same ``sid`` is returned.  The timeout was fixed at
          creation: ``ttl`` / ``lifetime`` of a reconnect are ignored.
        - ``sid`` names an ended session     -> 410 with the reason (an
          ended sid is not reused while the process runs; use a new one).
        - ``sid`` names no session           -> a session is created under
          exactly that ID.

        ``ttl`` is the session's idle timeout in seconds (``0``: never); see
        the lifetime policy on :class:`Plugin`.  The reserved ``default``
        session is per plugin instance and persistent.

        Raises:
            HTTPException 409: a negative/non-numeric ttl, ``lifetime='ttl'``
                without a ttl, or an unknown lifetime value.
            HTTPException 410: ``sid`` names a session that has ended.
        """
        self._ensure_cleanup_task()
        try:
            data = await request.json()
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        owner              = self._request_owner(request)
        sid, lifetime, ttl = await self._normalize_session_policy(data)
        sid = await self._open_session(sid, lifetime, ttl, owner)
        return {"sid": sid}

    @staticmethod
    def _request_owner(request) -> Optional[str]:
        """Extract the trusted owner identity from the request headers.

        The serving runtime injects the broker-stamped envelope src as the
        ``x-orbit-src`` header (overwriting any client-supplied copy).  Old-
        stack and gateway requests carry no such header — owner ``None``.
        Robust to header containers that are not plain mappings (e.g. test
        mocks): a non-string value resolves to ``None``.
        """
        return Plugin._request_header(request, 'x-orbit-src')

    @staticmethod
    def _request_header(request, name: str) -> Optional[str]:
        """A non-empty string header *name* (lower case) of *request*, else
        ``None``.  Case-insensitive also for plain-dict headers (runtime
        path); robust to header containers that are not mappings (e.g. test
        mocks)."""
        headers = getattr(request, 'headers', None)
        if headers is None:
            return None
        try:
            value = headers.get(name)
            if value is None and isinstance(headers, dict):
                value = next((v for k, v in headers.items()
                              if k.lower() == name), None)
        except Exception:
            return None
        return value if isinstance(value, str) and value else None

    async def _normalize_session_policy(self, data: Dict[str, Any]) -> tuple:
        """Validate the session-policy fields of a register_session body.

        Returns the normalized ``(sid, lifetime, ttl)`` triple; ``sid`` may be
        ``None`` (the caller mints one).  An explicit ``ttl`` wins: ``0`` ->
        ``persistent``, ``> 0`` -> ``ttl``; otherwise ``lifetime`` decides
        (``ephemeral``: the plugin default).  The reserved ``default`` session
        is special-cased *before* lifetime validation.  Every register path
        (base and plugin overrides) calls this, so an ended ``sid`` -- also one
        past its deadline that the sweep has not reached yet -- is refused
        here for all of them.

        Raises:
            HTTPException 409: invalid ttl, ``lifetime='ttl'`` without a ttl,
                or an unknown lifetime.
            HTTPException 410: ``sid`` names a session that has ended.
        """
        policy = self._validate_session_policy(data)     # 409 first
        sid    = policy[0]
        rec    = self._records.get(sid) if sid is not None else None
        if rec is not None:
            await self._expire_if_due(sid, rec, time.time())
        if sid is not None and sid not in self._records:
            text = self._ended_detail(sid)
            if text:
                raise HTTPException(status_code=410, detail=text)
        return policy

    @staticmethod
    def _validate_session_policy(data: Dict[str, Any]) -> tuple:
        """The validation half of :meth:`_normalize_session_policy`: no side
        effects, so a malformed request never ends a session."""
        sid      = data.get('sid')
        lifetime = data.get('lifetime', 'ephemeral')
        ttl      = data.get('ttl')

        if sid == DEFAULT_SID:
            if 'lifetime' in data and data['lifetime'] != 'persistent':
                raise HTTPException(
                    status_code=409,
                    detail="reserved session 'default' is always persistent")
            if ttl is not None:
                raise HTTPException(
                    status_code=409,
                    detail="reserved session 'default' takes no ttl")
            return sid, 'persistent', None

        if lifetime not in ('ephemeral', 'ttl', 'persistent'):
            raise HTTPException(
                status_code=409,
                detail=f"unknown session lifetime: {lifetime!r}")

        if ttl is not None:
            if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) \
                    or not math.isfinite(ttl) or ttl < 0:
                raise HTTPException(
                    status_code=409,
                    detail="ttl must be a number of seconds >= 0 (0: never)")
            if ttl == 0:
                return sid, 'persistent', None
            return sid, 'ttl', float(ttl)

        if lifetime == 'ttl':
            raise HTTPException(
                status_code=409,
                detail="lifetime='ttl' requires a positive ttl")

        return sid, lifetime, None

    async def _open_session(self, sid: Optional[str], lifetime: str,
                            ttl: Optional[float],
                            owner: Optional[str] = None) -> str:
        """Create-or-reconnect a session under the given policy.

        ``sid=None`` mints a fresh ID; an existing ``sid`` reconnects (any
        client; the timeout set at creation stays); the reserved ``default``
        is created on demand under a lock.  Returns the resolved session ID.
        """
        if sid == DEFAULT_SID:
            await self._ensure_default_session()
            self._touch(sid)
            return sid

        if sid is None:
            sid = f"session.{uuid.uuid4().hex[:8]}"
            while sid in self._sessions:
                sid = f"session.{uuid.uuid4().hex[:8]}"

        if sid in self._sessions:
            self._touch(sid)
            log.info("[%s] Reconnected session %s", self.instance_name, sid)
            return sid

        self._record_session(sid, self._create_session(sid), lifetime, ttl,
                             owner)
        log.info("[%s] Registered session %s (owner=%s)",
                 self.instance_name, sid, owner)
        return sid

    def _touch(self, sid: str) -> None:
        """Bump a session's last-access time (no-op if it has no record)."""
        rec = self._records.get(sid)
        if rec is not None:
            rec.last_access = time.time()

    def _record_session(self, sid: str, session: PluginSession,
                        lifetime: str, ttl: Optional[float],
                        owner: Optional[str] = None) -> None:
        """Store a session object and its :class:`SessionRecord`.

        ``owner`` is the requesting participant (diagnostics only).
        """
        self._sessions[sid] = session
        self._records[sid]  = SessionRecord(lifetime, ttl, owner, time.time())

    async def _ensure_default_session(self) -> PluginSession:
        """Return the reserved ``default`` session, creating it on demand.

        ``default`` is persistent and per plugin instance.  Creation is guarded
        by a lock so two concurrent first requests cannot double-create it.
        Persistent-default-owned resources are released only by explicit
        operator action (e.g. ``cancel_all``).
        """
        session = self._sessions.get(DEFAULT_SID)
        if session is not None:
            return session
        async with self._default_lock:
            session = self._sessions.get(DEFAULT_SID)
            if session is None:
                session = self._create_session(DEFAULT_SID)
                self._record_session(DEFAULT_SID, session, 'persistent', None)
                log.info("[%s] Created persistent 'default' session",
                         self.instance_name)
            return session

    async def unregister_session(self, request: Request) -> dict:
        """Unregister a session by its session ID and close it.

        A session that has already ended is a no-op; any later request on
        the sid gets 410 (closed).
        """
        sid = request.path_params['sid']
        if sid not in self._sessions and sid not in self._records:
            if self._ended_detail(sid):
                return {"ok": True}         # already ended
            raise HTTPException(status_code=404,
                                detail=f"unknown session id: {sid}")

        await self._end_session(sid, 'closed', time.time())
        log.info("[%s] Unregistered session %s", self.instance_name, sid)
        return {"ok": True}

    async def touch_session(self, request: Request) -> dict:
        """Count as an access of the session, so it does not idle-expire.

        No other effect: 404 for an unknown, 410 for an ended session.  The
        persistent ``default`` session never expires: touching it is a
        no-op (and never creates it).
        """
        sid = request.path_params['sid']
        if sid == DEFAULT_SID:
            return {}
        return await self._forward(sid, PluginSession.ping)

    async def get_version(self, request: Request) -> dict:
        """Return the plugin version."""
        return {"version": self.version}

    async def get_ui_config(self, request: Request) -> dict:
        """
        Return UI configuration for portal rendering.

        External plugins can define ui_config to describe their forms,
        monitors, and notification handlers, enabling seamless portal integration.
        """
        plugin_name = getattr(self.__class__, 'plugin_name', self._instance_name)
        return {
            "plugin_name": plugin_name,
            "instance_name": self._instance_name,
            "version": self.version,
            "ui": ui_config_to_dict(self.ui_config)
        }

    async def list_sessions(self, request: Request) -> dict:
        """Return a list of active session IDs."""
        return {"sessions": list(self._sessions.keys())}

    async def health_check(self, request: Request) -> dict:
        """
        Health check endpoint for monitoring.

        Returns plugin status including:
        - Plugin name and version
        - Uptime in seconds
        - Number of active sessions
        - Whether the plugin is healthy
        """
        uptime = time.time() - self._start_time
        active_sessions = len(self._sessions)

        return {
            "status": "healthy",
            "plugin": self._instance_name,
            "version": self.version,
            "uptime_seconds": round(uptime, 2),
            "active_sessions": active_sessions
        }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Return False to skip loading this plugin on this host.

        Checked *before* instantiation so no routes are registered when the
        plugin is not applicable.  Override in subclasses to gate on host type
        (broker vs endpoint) or runtime conditions (e.g. scheduler presence).
        Default: always load.
        """
        return True

    async def send_notification(self, topic: str, data: dict):
        """
        Broadcast a UI event over the broker SSE channels.
        Depends on `app.state.endpoint_service` having been injected by EndpointService.
        """
        endpoint_svc = getattr(self._app.state, "endpoint_service", None)
        if endpoint_svc is not None and hasattr(endpoint_svc, "send_notification"):
            # The endpoint runtime's send_notification is sync (it only hands the
            # event off to its transport loop); the broker plugin host's is
            # async.  Await only when the call returned an awaitable.
            result = endpoint_svc.send_notification(
                self.instance_name, topic, data)
            if inspect.isawaitable(result):
                await result
        else:
            log.warning("[%s] Cannot send notification: endpoint_service unlinked", self.instance_name)

    async def on_topology_change(self, participants: dict):
        """React to a topology / liveness change from the serving runtime.

        The serving runtime delivers the rich topology (``name -> {role,
        plugins, liveness}``) on every change, synthesizing a ``liveness ==
        'lost'`` entry for a participant that vanishes after the broker's
        grace (the wire carries no tombstone).

        The default implementation does nothing: sessions are not tied to a
        participant's liveness (a client may disconnect and reconnect later;
        see the lifetime policy on :class:`Plugin`).  Subclasses override it
        to react to topology changes.

        Args:
            participants: Dict mapping participant names to their rich info
                (``{role, plugins, liveness}``).
        """

    async def _forward(self, sid: str, func: Callable, *args: Any, **kwargs: Any) -> dict:
        """
        Forward a request to the specified session instance.

        Args:
            sid: Session ID to forward to.
            func: Session method to call.
            *args: Positional arguments for the method.
            **kwargs: Keyword arguments for the method.

        Returns:
            dict: The session method's return value (a plain dict).

        Raises:
            HTTPException 404: Session ID not found.
            HTTPException 410: Session has ended (expired or unregistered);
                the detail says why and what to change.
            HTTPException 500: Unexpected error inside the session method.
        """
        # On-demand creation of the reserved persistent 'default' session.
        if sid == DEFAULT_SID and sid not in self._sessions:
            await self._ensure_default_session()

        # Detect expiry of THIS session before the background cleanup removes it
        record = self._records.get(sid)
        text   = record and await self._expire_if_due(sid, record, time.time())
        if text:
            raise HTTPException(status_code=410, detail=text)

        session = self._sessions.get(sid)
        if not session:
            text = self._ended_detail(sid)
            if text:
                raise HTTPException(status_code=410, detail=text)
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        # Update last access time
        self._touch(sid)

        try:
            log.debug("[%s] Forwarding to session %s: %s", self.instance_name, sid, func.__name__)
            return await func(session, *args, **kwargs)
        except HTTPException:
            raise  # Re-raise HTTP exceptions as-is
        except Exception as e:
            log.exception("[%s] Error in session %s calling %s: %s",
                          self.instance_name, sid, func.__name__, e)
            raise HTTPException(
                status_code=500,
                detail=f"[{self.instance_name}/{sid}] {func.__name__}: {e}"
            ) from e

    def _expiry_deadline(self, record: SessionRecord,
                         now: float) -> Optional[float]:
        """Absolute time at which *record* expires, or ``None`` for never:
        ``last_access`` plus the session's idle timeout."""
        timeout = self._timeout(record)
        return record.last_access + timeout if timeout > 0 else None

    def _timeout(self, record: SessionRecord) -> float:
        """The idle timeout of a session (``0``: never), fixed at creation."""
        if record.lifetime == 'persistent':
            return 0
        if record.lifetime == 'ttl':
            return record.ttl
        return self.session_ttl

    def _is_expired(self, record: SessionRecord, now: float) -> bool:
        """True once *record* has passed its expiry deadline."""
        deadline = self._expiry_deadline(record, now)
        return deadline is not None and now > deadline

    def _is_busy(self, sid: str, rec: SessionRecord) -> bool:
        """Work in flight: a request still running, or the session's own
        :meth:`PluginSession.is_busy`.  A failing probe counts as busy --
        never reap work over a broken probe."""
        if rec.inflight > 0:
            return True
        session = self._sessions.get(sid)
        if session is None:
            return False
        try:
            return bool(session.is_busy())
        except Exception as e:
            log.warning("[%s] is_busy of session %s failed (kept as busy): "
                        "%s", self.instance_name, sid, e)
            return True

    async def _expire_if_due(self, sid: str, rec: SessionRecord,
                             now: float) -> Optional[str]:
        """The one expiry decision (sweep, ``_forward``, ``{sid}`` routes).

        A session past its deadline gets the benefit of work in flight
        (probed only here, at the deadline): busy counts as an access.  An
        idle one ends: it is dropped, its reason logged and remembered, and
        that reason returned (the 410 detail).  ``None`` if it lives on.
        """
        if not self._is_expired(rec, now):
            return None
        if self._is_busy(sid, rec):
            rec.last_access = now
            return None
        return await self._end_session(sid, 'expired', now)

    async def _end_session(self, sid: str, reason: str, now: float) -> str:
        """End a session (``reason``: ``'expired'`` or ``'closed'``): log,
        remember (unless persistent -- e.g. ``default`` is recreated on
        demand) and drop it.  Returns the explanation."""
        rec = self._records.get(sid)
        if rec is not None and self._timeout(rec) > 0:
            self._ended[sid] = (now, reason, self._timeout(rec),
                                now - rec.last_access, rec.client)
            self._ended.move_to_end(sid)
            while len(self._ended) > ENDED_KEEP:
                self._ended.popitem(last=False)
            text = self._ended_detail(sid)
            if reason == 'expired':
                log.log(logging.INFO if rec.client == 'explorer'
                        else logging.WARNING, text)
        else:
            text = f"[{self.instance_name}] session {sid} has ended"
        await self._drop_session(sid)
        return text

    def _ended_detail(self, sid: str) -> Optional[str]:
        """Why *sid* ended (the 410 detail), or ``None`` if it did not."""
        entry = self._ended.get(sid) if sid else None
        if entry is None:
            return None
        _, reason, timeout, idle, client = entry
        head = f"[{self.instance_name}] session {sid}"
        if reason == 'closed':
            return f"{head} was closed (unregister_session)."
        if client == 'explorer':
            return (f"{head} expired: Explorer session unused for "
                    f"{idle:.0f} s (idle timeout {timeout:.0f} s) -- the "
                    f"normal end of a closed or abandoned browser tab.")
        return (f"{head} expired after {idle:.0f} s without requests or work "
                f"in flight (idle timeout {timeout:.0f} s, set when the "
                f"session was created).  Likely API misuse: the client did "
                f"not close() the session.  To reconnect after longer idle "
                f"phases, create the session with register_session(ttl=<s>) "
                f"(ttl=0: never expires) or call touch() now and then.  If "
                f"this is a legitimate use case, please open an issue: "
                f"{ISSUES_URL}")

    async def _drop_session(self, sid: str) -> bool:
        """Close and forget one session (record + object).  Idempotent.

        The single teardown path shared by unregister / expiry sweep /
        shutdown.  Returns True if a session object was closed.
        """
        self._records.pop(sid, None)
        session = self._sessions.pop(sid, None)
        if session is None:
            return False
        try:
            await session.close()
        except Exception as e:
            log.warning("[%s] Error closing session %s: %s",
                        self.instance_name, sid, e)
        return True

    async def _cleanup_expired_sessions(self) -> int:
        """End every session idle past its timeout (see
        :meth:`_expire_if_due`).  Each candidate is decided afresh: an
        earlier close may have yielded long enough for it to be used.

        Returns:
            Number of sessions ended.
        """
        now = time.time()
        due = [(sid, rec) for sid, rec in list(self._records.items())
               if self._is_expired(rec, now)]
        ended = 0
        for sid, rec in due:
            if self._records.get(sid) is rec \
                    and await self._expire_if_due(sid, rec, time.time()):
                ended += 1
        return ended

    def _ensure_cleanup_task(self) -> None:
        """Start the background session-cleanup task if not already running."""
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
            self._cleanup_task = loop.create_task(self._cleanup_loop())
        except RuntimeError:
            pass  # No running loop yet; will retry on next call

    async def _cleanup_loop(self) -> None:
        """Background task: expire stale sessions every 5 seconds.

        The per-iteration sweep is guarded so a single failing sweep logs and
        the loop survives — a sweep that raised would otherwise kill the task
        permanently and leak every subsequent expired session.
        """
        while True:
            await asyncio.sleep(5)
            try:
                await self._cleanup_expired_sessions()
            except Exception:
                log.exception("[%s] session cleanup sweep failed",
                              self.instance_name)

    async def shutdown(self) -> None:
        """Orderly plugin teardown (host-shutdown path).

        Cancels the background session-cleanup task — awaiting it so the
        cancellation is processed before the loop is torn down (no "Task was
        destroyed but it is pending" warnings) — then closes every open
        session.  Subclasses that spawn additional background tasks override
        this and call ``super().shutdown()``.
        """
        if self._cleanup_task is not None and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:    await self._cleanup_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        for sid in set(self._sessions) | set(self._records):
            await self._drop_session(sid)

    def _log_routes(self) -> None:
        """Log all registered routes for debugging."""
        log.debug("[%s] %s routes:", self.instance_name, self.__class__.__name__)
        for method, pattern, _, handler in self._app.state.direct_routes:
            path = pattern.pattern  # compiled regex string
            if self.namespace in path:
                name = getattr(handler, '__name__', str(handler))
                log.debug("  %s %s -> %s", method, path, name)

