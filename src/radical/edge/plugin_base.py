import os
import re
import uuid
import asyncio
import logging
import time

from typing import Type, Optional, Dict, Callable, Any, Union
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .plugin_session_base import PluginSession
from .ui_schema import UIConfig, ui_config_to_dict

log = logging.getLogger("radical.edge")


class Plugin(object):
    """
    Base class for Edge plugins.

    Each plugin gets its own URL namespace and built-in session management.
    Routes are added with `add_route_post` / `add_route_get`.

    **plugin_name vs instance_name**

    ``plugin_name`` is a *class-level* attribute that uniquely identifies the
    plugin type (e.g. ``"psij"``, ``"queue_info"``).  It is the key used in
    the global ``Plugin._registry`` and in client-side lookups
    (``edge.get_plugin("psij")``).

    ``instance_name`` is set at *construction time* (defaults to
    ``plugin_name`` when only one instance is needed) and drives the URL
    namespace: ``/{instance_name}/…``.  Multiple instances of the same plugin
    type on the same edge must be given distinct instance names.

    Subclasses that define a `plugin_name` class attribute will be
    automatically registered in the global plugin registry.

    Subclasses must define:
        session_class: The session class to instantiate (must inherit from PluginSession)

    Subclasses may define:
        client_class: The local helper class for the application-side client.
        version: The version string for the plugin.
        session_ttl: Session timeout in seconds (default: 3600 = 1 hour, 0 = no timeout)
        ui_config: UI configuration dict for portal rendering (see ui_schema.py)

    Notifications
    -------------
    Plugins can send real-time notifications to clients via Server-Sent Events (SSE).
    The notification flow is: Session -> Plugin -> EdgeService -> Bridge -> SSE clients.

    **Sending notifications from a session:**

        # In your PluginSession subclass method:
        if self._plugin:
            self._plugin._dispatch_notify("my_topic", {"key": "value", "status": "running"})

    The `_plugin` reference is automatically injected into sessions by the plugin.
    `_dispatch_notify` works from both sync and async contexts, including background threads.

    **Sending notifications from a plugin:**

        # In your Plugin subclass method:
        await self.send_notification("my_topic", {"key": "value"})

    **Subscribing to notifications (browser/JavaScript):**

        const eventSource = new EventSource('/events');
        eventSource.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            if (msg.topic === 'notification') {
                const {edge, plugin, topic, data} = msg.data;
                console.log(`${edge}/${plugin}: ${topic}`, data);
            }
        };

    **Subscribing to notifications (Python client):**

        import sseclient
        import requests

        response = requests.get('http://bridge:8000/events', stream=True)
        client = sseclient.SSEClient(response)
        for event in client.events():
            msg = json.loads(event.data)
            if msg['topic'] == 'notification':
                print(msg['data'])

    Topology Updates
    ----------------
    Plugins can receive notifications when edges connect or disconnect by
    overriding the `on_topology_change` method:

        async def on_topology_change(self, edges: dict):
            '''Called when edges connect/disconnect.

            Args:
                edges: Dict mapping edge names to their plugin info.
                       Example: {"edge1": {"plugins": ["sysinfo", "psij"]}}
            '''
            for edge_name, info in edges.items():
                print(f"Edge {edge_name} has plugins: {info.get('plugins', [])}")
    """

    _registry: Dict[str, Type["Plugin"]] = {}
    session_class: Optional[Type[PluginSession]] = None
    client_class: Optional[Type] = None
    version: str = '0.0.1'
    session_ttl: int = 3600  # Default: 1 hour session timeout
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

    def __init__(self, app: FastAPI, instance_name: str):
        """
        Initialize the Plugin with a FastAPI app and instance name.
        Also sets up built-in session management.

        Args:
            app: The FastAPI application instance.
            instance_name: The name of the plugin instance, used in the namespace.
        """
        self._app: FastAPI = app
        self._instance_name: str = instance_name
        self._uid: str = str(uuid.uuid4())
        self._namespace: str = f"/{self._instance_name}"
        self._start_time: float = time.time()

        self._sessions: Dict[str, PluginSession] = {}
        self._session_last_access: Dict[str, float] = {}  # Track last access time
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
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

    # Role classification — all four properties delegate to a single
    # helper so the role / scheduler / executor decision lives in
    # exactly one place (utils.host_role).
    @property
    def is_bridge(self) -> bool:
        """True when this plugin is hosted on the bridge (not on an edge)."""
        from .utils import host_role
        return host_role(self._app)['role'] == 'bridge'

    @property
    def is_compute_node(self) -> bool:
        """True when running inside a batch job allocation (compute node)."""
        from .utils import host_role
        return host_role(self._app)['role'] == 'compute'

    @property
    def is_login_node(self) -> bool:
        """True on an HPC login node — a real scheduler is installed but
        no allocation is active."""
        from .utils import host_role
        return host_role(self._app)['role'] == 'login'

    @property
    def is_standalone(self) -> bool:
        """True for a non-HPC host (no batch scheduler installed)."""
        from .utils import host_role
        return host_role(self._app)['role'] == 'standalone'

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
            # Called from a background thread — use the cached main loop
            if self._main_loop is not None:
                asyncio.run_coroutine_threadsafe(_send(), self._main_loop)
            else:
                log.debug("[%s] No event loop available for notification",
                          self.instance_name)

    async def register_session(self, request: Request) -> dict:
        """Register a new session and return its unique session ID."""
        self._ensure_cleanup_task()
        sid = f"session.{uuid.uuid4().hex[:8]}"
        self._sessions[sid] = self._create_session(sid)
        self._session_last_access[sid] = time.time()
        log.info("[%s] Registered session %s", self.instance_name, sid)
        return {"sid": sid}

    async def unregister_session(self, request: Request) -> dict:
        """Unregister a session by its session ID and close it."""
        sid = request.path_params['sid']
        inst = self._sessions.pop(sid, None)
        self._session_last_access.pop(sid, None)

        if not inst:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        await inst.close()
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
        (bridge vs edge) or runtime conditions (e.g. scheduler presence).
        Default: always load.
        """
        return True

    async def send_notification(self, topic: str, data: dict):
        """
        Broadcast a UI event over the bridge SSE channels.
        Depends on `app.state.edge_service` having been injected by EdgeService.
        """
        edge_svc = getattr(self._app.state, "edge_service", None)
        if edge_svc is not None and hasattr(edge_svc, "send_notification"):
            await edge_svc.send_notification(self.instance_name, topic, data)
        else:
            log.warning("[%s] Cannot send notification: edge_service unlinked", self.instance_name)

    async def on_topology_change(self, edges: dict):
        """
        Called when the bridge topology changes (edge connect/disconnect).

        Subclasses can override this to react to topology changes.
        Default implementation does nothing.

        Args:
            edges: Dict mapping edge names to their plugin info.
        """
        pass

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
        if self.session_ttl > 0:
            # Detect expiry of THIS session before the background cleanup removes it
            last        = self._session_last_access.get(sid)
            sid_expired = (last is not None and (time.time() - last) > self.session_ttl)
            if sid_expired:
                await self._cleanup_expired_sessions()
                raise HTTPException(status_code=410, detail=f"session expired: {sid}")

        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"unknown session id: {sid}")

        # Update last access time
        self._session_last_access[sid] = time.time()

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

    async def _cleanup_expired_sessions(self) -> int:
        """
        Clean up sessions that have exceeded their TTL.

        Returns:
            Number of sessions cleaned up.
        """
        if self.session_ttl <= 0:
            return 0

        now = time.time()
        expired_sids = [
            sid for sid, last_access in self._session_last_access.items()
            if (now - last_access) > self.session_ttl
        ]

        for sid in expired_sids:
            session = self._sessions.pop(sid, None)
            self._session_last_access.pop(sid, None)
            if session:
                try:
                    await session.close()
                except Exception as e:
                    log.warning("[%s] Error closing expired session %s: %s",
                                self.instance_name, sid, e)
            log.info("[%s] Cleaned up expired session %s", self.instance_name, sid)

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
        """Background task: expire stale sessions every 5 seconds."""
        while True:
            await asyncio.sleep(5)
            if self.session_ttl > 0:
                await self._cleanup_expired_sessions()

    def _log_routes(self) -> None:
        """Log all registered routes for debugging."""
        log.debug("[%s] %s routes:", self.instance_name, self.__class__.__name__)
        for method, pattern, _, handler in self._app.state.direct_routes:
            path = pattern.pattern  # compiled regex string
            if self.namespace in path:
                name = getattr(handler, '__name__', str(handler))
                log.debug("  %s %s -> %s", method, path, name)

