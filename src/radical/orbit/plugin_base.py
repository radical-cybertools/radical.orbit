import re
import math
import uuid
import asyncio
import inspect
import logging
import time

from dataclasses import dataclass
from typing import Type, Optional, Dict, Callable, Any, Union
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .ui_schema import UIConfig, ui_config_to_dict

log = logging.getLogger("radical.orbit")

# Reserved session ID.  The 'default' session is persistent and per plugin
# instance — a `rhapsody` default and a `psij` default are distinct sessions.
DEFAULT_SID = 'default'


@dataclass
class SessionRecord:
    """Lifetime bookkeeping for one session (see the policy table on `Plugin`).

    The session *object* lives in ``Plugin._sessions[sid]``; this record holds
    only the fields the expiry sweep needs.  ``drain_deadline`` is stamped when
    the owning participant is declared ``lost`` and cleared when it returns
    ``present`` (see :meth:`Plugin.on_topology_change`).
    """
    lifetime:       str                       # 'ephemeral' | 'ttl' | 'persistent'
    ttl:            Optional[float]            # seconds, for lifetime == 'ttl'
    owner:          Optional[str]             # trusted x-orbit-src, or None
    last_access:    float
    drain_deadline: Optional[float] = None    # set on owner 'lost'


class _LastAccessView:
    """Backward-compat mapping shim over ``SessionRecord.last_access``.

    Lets plugins that predate the SessionRecord merge (globus / iri /
    rhapsody) keep bumping access time via
    ``self._session_last_access[sid] = ts``.  Writing an unknown ``sid``
    materialises an owner-less ephemeral record (the record-less default).
    Remove once those plugins record last_access on the record directly.
    """

    def __init__(self, records: Dict[str, "SessionRecord"]):
        self._records = records

    def __setitem__(self, sid: str, ts: float) -> None:
        rec = self._records.get(sid)
        if rec is None:
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
        session_ttl: Idle timeout in seconds for owner-less ephemeral sessions
            (default: 3600 = 1 hour, 0 = no timeout).
        reclaim_drain: Grace, in seconds, between an owner being declared
            ``lost`` and its owner-bound ephemeral sessions being reclaimed
            (default: 300).  Injectable per instance for tests.
        ui_config: UI configuration dict for portal rendering (see ui_schema.py)

    Session lifetime policy
    -----------------------
    ``register_session`` records each session as a :class:`SessionRecord`
    (``lifetime``, ``ttl``, ``owner``, ``last_access``, ``drain_deadline``).
    The ``owner`` is the trusted ``x-orbit-src`` header the serving runtime
    injects from the broker-stamped src (``None`` for old-stack / gateway
    requests, which carry no participant identity).  A single 5-second sweep
    (:meth:`_cleanup_loop`) enforces every policy via :meth:`_expiry_deadline`:

    ==============  ===========  ========================================
    lifetime        owner        expires when
    ==============  ===========  ========================================
    persistent      any          never (explicit unregister only)
    ttl             any          ``now - last_access > ttl``
    ephemeral       bound        owner ``lost`` + ``reclaim_drain`` grace
    ephemeral       owner-less   ``now - last_access > session_ttl`` (idle)
    (record-less)   —            treated as owner-less ephemeral
    ==============  ===========  ========================================

    Reattach is owner-checked: reconnecting to a session whose recorded owner
    is non-``None`` and differs from the caller is rejected (403); recovery
    goes through re-registering the same participant name.  The reserved
    ``default`` session is always persistent, shared per plugin instance, and
    records owner ``None``.

    Notifications
    -------------
    A session notifies via ``self.notify(topic, data)``; a plugin via
    ``await self.send_notification(topic, data)``.  Both flow Session ->
    Plugin -> runtime -> Broker -> subscribed consumers (and the gateway's SSE
    fan-out), reaching HTTP clients over ``/events``.

    Topology updates
    ----------------
    The serving runtime delivers the rich topology (``name -> {role, plugins,
    liveness}``) on every change.  The base ``on_topology_change`` drives
    owner-bound ephemeral-session reclaim; override it to also react to
    connect/disconnect/liveness, calling ``super()`` to keep the drain::

        async def on_topology_change(self, participants: dict):
            for name, info in participants.items():
                print(f"{name}: {info.get('liveness')}")
            await super().on_topology_change(participants)
    """

    _registry: Dict[str, Type["Plugin"]] = {}
    session_class: Optional[Type[PluginSession]] = None
    client_class: Optional[Type] = None
    version: str = '0.0.1'
    session_ttl: int = 3600  # Default: 1 hour session timeout
    reclaim_drain: float = 300  # 'lost' owner -> ephemeral-session reclaim grace
    ui_config: Union[Dict, UIConfig, None] = None  # UI configuration for portal
    ui_module: Optional[str] = None  # Absolute path to JS plugin module, or None

    def __init_subclass__(cls, **kwargs):
        """Auto-register subclasses that define plugin_name."""
        super().__init_subclass__(**kwargs)
        if hasattr(cls, 'plugin_name'):
            name = getattr(cls, 'plugin_name')
            if name in Plugin._registry:
                log.warning("[Plugin] Duplicate plugin_name '%s' - overwriting", name)
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
        self._session_last_access = _LastAccessView(self._records)
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
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._register_direct(full_path, "POST", method)
        self._app.add_route(full_path, self._wrap_handler(method),
                            methods=["POST"])

    def add_route_get(self, path: str, method: Callable):
        """Add a GET route to the plugin's namespace."""
        full_path = self._namespace + '/' + path
        full_path = full_path.replace('//', '/')
        self._register_direct(full_path, "GET", method)
        self._app.add_route(full_path, self._wrap_handler(method),
                            methods=["GET"])

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
        - ``sid`` names an existing session  -> *reconnect*: its last-access
          time is bumped and the same ``sid`` is returned (409 if the supplied
          lifetime/ttl differs from the one it was created with).
        - ``sid`` names no session           -> a session is created under
          exactly that ID.

        See the lifetime-policy table on :class:`Plugin` for how ``lifetime``,
        ``ttl`` and ``owner`` govern expiry.  The reserved ``default`` session
        is per plugin instance, always persistent, and records owner ``None``.

        Raises:
            HTTPException 403: a reattach to an existing session from a
                different owner than the one recorded at create.
            HTTPException 409: an incoherent lifetime/ttl pair, an unknown
                lifetime value, or a reconnect whose policy conflicts with the
                existing session.
        """
        self._ensure_cleanup_task()
        try:
            data = await request.json()
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        owner              = self._request_owner(request)
        sid, lifetime, ttl = self._normalize_session_policy(data)
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
        headers = getattr(request, 'headers', None)
        if headers is None:
            return None
        try:
            owner = headers.get('x-orbit-src')
        except Exception:
            return None
        return owner if isinstance(owner, str) and owner else None

    def _normalize_session_policy(self, data: Dict[str, Any]) -> tuple:
        """Validate the session-policy fields of a register_session body.

        Returns the normalized ``(sid, lifetime, ttl)`` triple; ``sid`` may be
        ``None`` (the caller mints one).  The reserved ``default`` session is
        special-cased *before* lifetime validation, so a bare
        ``register_session(sid='default')`` — where ``lifetime`` still carries
        its ``'ephemeral'`` default — resolves to the persistent default rather
        than being rejected; only an *explicitly* non-persistent policy for
        ``default`` is incoherent.

        Raises:
            HTTPException 409: incoherent lifetime/ttl pair or unknown lifetime.
        """
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

        if lifetime == 'ttl':
            if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) \
                    or not math.isfinite(ttl) or ttl <= 0:
                raise HTTPException(
                    status_code=409,
                    detail="lifetime='ttl' requires a positive ttl")
            ttl = float(ttl)
        elif ttl is not None:
            raise HTTPException(
                status_code=409,
                detail=f"ttl is incoherent with lifetime={lifetime!r}")

        return sid, lifetime, ttl

    async def _open_session(self, sid: Optional[str], lifetime: str,
                            ttl: Optional[float],
                            owner: Optional[str] = None) -> str:
        """Create-or-reconnect a session under the given policy.

        ``sid=None`` mints a fresh ID; an existing ``sid`` reconnects (after an
        owner check then a policy-conflict check); the reserved ``default`` is
        created on demand under a lock and is shared (owner ``None``).  Returns
        the resolved session ID.
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
            self._check_owner(sid, owner)
            self._check_policy_conflict(sid, lifetime, ttl)
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

        ``owner`` is the trusted participant identity (``None`` for old-stack /
        gateway sessions and for the shared ``default`` session).
        """
        self._sessions[sid] = session
        self._records[sid]  = SessionRecord(lifetime, ttl, owner, time.time())

    def _check_owner(self, sid: str, owner: Optional[str]) -> None:
        """Reject a reattach from a different owner than recorded at create.

        A session created with a non-``None`` owner may only be reattached by
        that same identity — session recovery goes through re-registering the
        same participant name.  Owner-less sessions (``None`` recorded — old
        stack / gateway) accept any reattach; there is no identity to guard.
        """
        record = self._records.get(sid)
        if record is None:
            return
        if record.owner is not None and record.owner != owner:
            raise HTTPException(
                status_code=403,
                detail=f"session {sid} is owned by another participant")

    def _check_policy_conflict(self, sid: str, lifetime: str,
                               ttl: Optional[float]) -> None:
        """Reject a reconnect whose policy differs from the live session's.

        Sessions created outside the policy model (no record — e.g. a plugin
        that builds sessions directly) accept any reconnect; there is nothing
        to conflict with.
        """
        record = self._records.get(sid)
        if record is None:
            return
        if record.lifetime != lifetime or record.ttl != ttl:
            raise HTTPException(
                status_code=409,
                detail=f"session {sid} exists with a different lifetime policy")

    async def _ensure_default_session(self) -> PluginSession:
        """Return the reserved ``default`` session, creating it on demand.

        ``default`` is persistent and per plugin instance.  Creation is guarded
        by a lock so two concurrent first requests cannot double-create it.
        Persistent-default-owned resources have no liveness reclaim — they are
        released only by explicit operator action (e.g. ``cancel_all``).
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
        """Unregister a session by its session ID and close it."""
        sid = request.path_params['sid']
        if sid not in self._sessions and sid not in self._records:
            raise HTTPException(status_code=404,
                                detail=f"unknown session id: {sid}")

        await self._drop_session(sid)
        log.info("[%s] Unregistered session %s", self.instance_name, sid)
        return {"ok": True}

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

        The default implementation drives owner-bound ephemeral-session
        reclaim by stamping a ``drain_deadline`` on the affected records: an
        owner declared ``lost`` stamps ``now + reclaim_drain`` (the 5-second
        sweep drops them once it passes); an owner seen ``present`` again
        clears the stamp.  A ``suspect`` owner does **not** arm the drain — a
        transient blip reaches ``suspect`` at most and must never reclaim.

        Subclasses may override to react to topology changes; overriders that
        also want the reclaim behavior call ``super()``.

        Args:
            participants: Dict mapping participant names to their rich info
                (``{role, plugins, liveness}``).
        """
        now = time.time()
        for name, info in (participants or {}).items():
            liveness = (info or {}).get('liveness')
            if liveness not in ('lost', 'present'):
                continue  # 'suspect' (or unknown) never arms/clears the drain
            for rec in self._records.values():
                if rec.owner != name or rec.lifetime != 'ephemeral':
                    continue
                if liveness == 'lost':
                    if rec.drain_deadline is None:
                        rec.drain_deadline = now + self.reclaim_drain
                else:  # 'present'
                    rec.drain_deadline = None

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
            HTTPException 410: Session has expired (TTL exceeded); the session
                has already been cleaned up before this is raised.
            HTTPException 500: Unexpected error inside the session method.
        """
        # On-demand creation of the reserved persistent 'default' session.
        if sid == DEFAULT_SID and sid not in self._sessions:
            await self._ensure_default_session()

        # Detect expiry of THIS session before the background cleanup removes it
        now = time.time()
        record = self._records.get(sid)
        if record is not None and self._is_expired(record, now):
            await self._drop_session(sid)
            raise HTTPException(status_code=410, detail=f"session expired: {sid}")

        session = self._sessions.get(sid)
        if not session:
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
        """Absolute time at which *record* expires, or ``None`` for never.

        One function for every row of the lifetime-policy table on
        :class:`Plugin`:

        - ``persistent``               -> ``None``.
        - ``ttl``                      -> ``last_access + ttl``.
        - ``ephemeral`` owner-bound    -> ``drain_deadline`` (``None`` until
          the owner is declared ``lost``; liveness-governed, never idle).
        - ``ephemeral`` owner-less     -> ``last_access + session_ttl`` (idle),
          or ``None`` when ``session_ttl == 0``.
        """
        if record.lifetime == 'persistent':
            return None
        if record.lifetime == 'ttl':
            return record.last_access + record.ttl
        if record.owner is not None:                       # owner-bound
            return record.drain_deadline
        if self.session_ttl > 0:                           # owner-less idle
            return record.last_access + self.session_ttl
        return None

    def _is_expired(self, record: SessionRecord, now: float) -> bool:
        """True once *record* has passed its expiry deadline."""
        deadline = self._expiry_deadline(record, now)
        return deadline is not None and now > deadline

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
        """Close and drop every session past its lifetime policy.

        Enforces ttl expiry, ephemeral idle timeout, and owner-loss drain in
        one pass; ``persistent`` sessions are never touched.

        Returns:
            Number of sessions cleaned up.
        """
        now = time.time()
        expired_sids = [sid for sid, rec in list(self._records.items())
                        if self._is_expired(rec, now)]
        for sid in expired_sids:
            await self._drop_session(sid)
            log.info("[%s] Cleaned up expired session %s",
                     self.instance_name, sid)
        return len(expired_sids)

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

