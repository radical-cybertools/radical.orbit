"""
Client API for RADICAL Edge.

This module provides Python client classes for interacting with the RADICAL Edge
bridge and edge services. It includes support for real-time notifications via
Server-Sent Events (SSE).

Classes
-------
BridgeClient
    Main client for connecting to the bridge. Supports notification callbacks.

EdgeClient
    Client for interacting with a specific edge service.

PluginClient
    Base class for plugin-specific client helpers.

Quick Start
-----------
::

    from radical.edge.client import BridgeClient

    # Connect to bridge
    client = BridgeClient(url="http://localhost:8000")

    # List connected edges
    edges = client.list_edges()
    print(f"Connected edges: {edges}")

    # Get a plugin client
    edge = client.get_edge_client("my_edge")
    psij = edge.get_plugin("psij")

    # Register for notifications
    def on_job_update(edge, plugin, topic, data):
        print(f"Job update: {data}")

    psij.register_notification_callback(on_job_update, topic="job_status")

    # ... use the plugin ...

    # Cleanup
    client.close()

Notification Callbacks
----------------------
Callbacks can be registered at multiple levels:

1. **Global** - all notifications from all plugins::

    client.register_callback(callback=my_handler)

2. **Edge-specific** - all notifications from plugins on an edge::

    client.register_callback(edge_id="hpc1", callback=my_handler)

3. **Plugin-specific** - notifications from a specific plugin::

    client.register_callback(edge_id="hpc1", plugin_name="psij", callback=my_handler)

4. **Topic-specific** - notifications for a specific topic::

    client.register_callback(edge_id="hpc1", plugin_name="psij",
                             topic="job_status", callback=my_handler)

5. **Via PluginClient** - convenience method::

    psij.register_notification_callback(my_handler, topic="job_status")

All callbacks receive four arguments: ``(edge, plugin, topic, data)``.

Topology Callbacks
------------------
Register for edge connect/disconnect events::

    def on_topology_change(edges):
        '''Called when edges connect or disconnect.

        Args:
            edges: Dict mapping edge names to plugin info.
        '''
        print(f"Connected: {list(edges.keys())}")

    client.register_topology_callback(on_topology_change)
"""

import os
import httpx
import logging
import urllib3
import json
import itertools
import threading

from typing import Any, Dict, List, Optional, Callable, Tuple

from . import _prof as rprof

from .plugin_base import Plugin


def _raise(resp, context: str = '') -> None:
    """Raise RuntimeError with HTTP status, optional context, and server detail."""
    if resp.is_error:
        try:   detail = str(resp.json().get('detail') or '')
        except Exception: detail = resp.text or ''
        parts = [f"HTTP {resp.status_code}"]
        if context: parts.append(context)
        if detail:  parts.append(detail)
        raise RuntimeError(' — '.join(parts))


# Disable SSL warnings for localhost/self-signed certs primarily used in dev
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("radical.edge.client")

# Silence per-request INFO logging from httpx/httpcore
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class BridgeClient:
    """
    Client for interacting with the Radical Edge Bridge.

    Notification Callbacks
    ----------------------
    The client supports receiving real-time notifications from plugins via SSE.
    Callbacks can be registered at three levels:

    1. **Global callbacks** - receive all notifications::

        def my_callback(edge, plugin, topic, data):
            print(f"{edge}/{plugin}: {topic} -> {data}")

        client.register_callback(callback=my_callback)

    2. **Plugin-specific callbacks** - receive notifications from a specific plugin::

        def job_callback(edge, plugin, topic, data):
            print(f"Job update: {topic} -> {data}")

        client.register_callback(edge_id="hpc1", plugin_name="psij", callback=job_callback)

    3. **Topic-specific callbacks** - receive notifications for a specific topic::

        def status_callback(edge, plugin, topic, data):
            print(f"Status: {data}")

        client.register_callback(edge_id="hpc1", plugin_name="psij",
                                 topic="job_status", callback=status_callback)

    Callbacks receive four arguments: edge (str), plugin (str), topic (str), data (dict).

    Topology Callbacks
    ------------------
    Register for edge connect/disconnect events::

        def on_topology(edges):
            print(f"Connected edges: {list(edges.keys())}")

        client.register_topology_callback(on_topology)
    """

    def __init__(self, url: Optional[str] = None, cert: Optional[str] = None):
        """
        Initialize the Bridge Client.

        Args:
            url: The bridge URL.  CLI > env (``RADICAL_BRIDGE_URL``) >
                 file (``~/.radical/edge/bridge.url``).
            cert: Path to CA cert.  Same precedence using
                  ``RADICAL_BRIDGE_CERT`` and
                  ``~/.radical/edge/bridge_cert.pem``.  Required when
                  the URL scheme is ``https``; ignored for ``http``.
        """
        from urllib.parse import urlparse
        from . import utils
        resolved_url, _      = utils.resolve_bridge_url(cli=url)
        self._url: str       = resolved_url

        # Cert is only meaningful for HTTPS.  HTTP URLs bypass cert
        # resolution entirely (no TLS in play).
        if urlparse(self._url).scheme == 'https':
            resolved_cert, _ = utils.resolve_bridge_cert(cli=cert)
            self._cert: Optional[str] = str(resolved_cert)
        else:
            self._cert = None

        self._prof = rprof.Profiler('client', ns='radical.edge')
        self._req_counter = itertools.count()

        def _inject_req_id(request):
            req_id = 'req.%06d' % next(self._req_counter)
            request.headers['X-Request-ID'] = req_id
            # stash for the response hook
            request.extensions['req_id'] = req_id
            self._prof.prof('client_send', uid=req_id, msg=str(request.url))

        def _on_response(response):
            req_id = response.request.extensions.get('req_id', '')
            self._prof.prof('client_recv', uid=req_id,
                            state=str(response.status_code))

        self._http: httpx.Client = httpx.Client(
            base_url=self._url,
            verify=self._cert if self._cert else False,
            # Match the bridge's REQUEST_TIMEOUT (600s).  Submit batches
            # of 1000s of tasks can take many seconds at the edge; a 60s
            # client cap would 504 long before the bridge would.
            timeout=600.0,
            event_hooks={'request' : [_inject_req_id],
                         'response': [_on_response]},
        )
        # Callbacks: key is (edge_id, plugin_name, topic) - None means wildcard
        self._callbacks: Dict[Tuple[Optional[str], Optional[str], Optional[str]], List[Callable]] = {}
        self._topology_callbacks: List[Callable] = []
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_stop: threading.Event = threading.Event()
        self._listener_connected: threading.Event = threading.Event()

    @property
    def url(self) -> str:
        """Resolved bridge URL (trailing slash stripped)."""
        return self._url

    def register_callback(self, edge_id: Optional[str] = None, plugin_name: Optional[str] = None,
                          topic: Optional[str] = None, callback: Callable = None) -> None:
        """
        Register a notification callback.

        Args:
            edge_id: Filter by edge name (None = all edges)
            plugin_name: Filter by plugin name (None = all plugins)
            topic: Filter by notification topic (None = all topics)
            callback: Function to call. Receives (edge, plugin, topic, data).

        Example::

            # All notifications
            client.register_callback(callback=my_handler)

            # Only job_status from psij on hpc1
            client.register_callback(edge_id="hpc1", plugin_name="psij",
                                     topic="job_status", callback=job_handler)
        """
        if callback is None:
            raise ValueError("callback is required")
        key = (edge_id, plugin_name, topic)
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)
        self._ensure_listener()

    def unregister_callback(self, edge_id: Optional[str] = None, plugin_name: Optional[str] = None,
                            topic: Optional[str] = None, callback: Callable = None) -> None:
        """Unregister a notification callback."""
        key = (edge_id, plugin_name, topic)
        if key in self._callbacks and callback in self._callbacks[key]:
            self._callbacks[key].remove(callback)

    def register_topology_callback(self, callback: Callable) -> None:
        """
        Register a callback for topology changes (edge connect/disconnect).

        Args:
            callback: Function to call. Receives edges dict mapping edge names
                      to their plugin info.

        Example::

            def on_topology(edges):
                for name, info in edges.items():
                    print(f"{name}: {info.get('plugins', [])}")

            client.register_topology_callback(on_topology)
        """
        self._topology_callbacks.append(callback)
        self._ensure_listener()

    def unregister_topology_callback(self, callback: Callable) -> None:
        """Unregister a topology callback."""
        if callback in self._topology_callbacks:
            self._topology_callbacks.remove(callback)

    def _ensure_listener(self) -> None:
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._listener_stop.clear()
            self._listener_connected.clear()
            self._listener_thread = threading.Thread(target=self._listen_sse, daemon=True)
            self._listener_thread.start()

    def wait_for_listener(self, timeout: float = 30) -> bool:
        """Block until the SSE listener is connected.

        Returns ``True`` if connected, ``False`` on timeout.
        """
        self._ensure_listener()
        return self._listener_connected.wait(timeout=timeout)

    def _dispatch_notification(self, edge: str, plugin: str, topic: str, data: dict) -> None:
        """Dispatch a notification to matching callbacks."""
        log.debug("[client] dispatch: edge=%s  plugin=%s  topic=%s  registered_keys=%s",
                  edge, plugin, topic, list(self._callbacks.keys()))
        matched = 0
        # Check all registered callback patterns
        for (e_filter, p_filter, t_filter), callbacks in self._callbacks.items():
            # Match if filter is None (wildcard) or matches exactly
            if (e_filter is None or e_filter == edge) and \
               (p_filter is None or p_filter == plugin) and \
               (t_filter is None or t_filter == topic):
                matched += len(callbacks)
                for cb in callbacks:
                    try:
                        cb(edge, plugin, topic, data)
                    except Exception as e:
                        log.error("Notification callback error: %s", e)
        if not matched:
            log.debug("[client] dispatch: no callbacks matched for edge=%s plugin=%s topic=%s",
                      edge, plugin, topic)

    def _listen_sse(self) -> None:
        log.debug("[client] SSE listener starting: url=%s/events", self._url)
        try:
            with httpx.stream("GET", f"{self._url}/events", verify=self._cert if self._cert else False, timeout=None) as response:
                log.debug("[client] SSE stream connected: status=%s", response.status_code)
                self._listener_connected.set()
                for line in response.iter_lines():
                    if self._listener_stop.is_set():
                        log.debug("[client] SSE listener stopping (stop flag set)")
                        break
                    if line.startswith("data: "):
                        data_str = line[6:]
                        if not data_str:
                            continue
                        log.debug("[client] SSE raw event: %s", data_str[:200])
                        try:
                            payload = json.loads(data_str)
                            msg_topic = payload.get("topic")

                            if msg_topic == "notification":
                                notif = payload.get("data", {})
                                log.debug("[client] SSE notification: edge=%s  plugin=%s  topic=%s",
                                          notif.get("edge"), notif.get("plugin"), notif.get("topic"))
                                self._dispatch_notification(
                                    edge=notif.get("edge"),
                                    plugin=notif.get("plugin"),
                                    topic=notif.get("topic"),
                                    data=notif.get("data", {})
                                )

                            elif msg_topic == "topology":
                                edges = payload.get("data", {}).get("edges", {})
                                log.debug("[client] SSE topology: edges=%s", list(edges.keys()))
                                for cb in self._topology_callbacks:
                                    try:
                                        cb(edges)
                                    except Exception as e:
                                        log.error("Topology callback error: %s", e)

                            else:
                                log.debug("[client] SSE unknown topic: %s", msg_topic)

                        except Exception as e:
                            log.error("Error parsing SSE event: %s", e)
        except Exception as e:
            if not self._listener_stop.is_set():
                log.debug("[client] SSE listener stopped: %s", e)

    def close(self) -> None:
        self._listener_stop.set()
        self._http.close()
        self._prof.close()

    def list_edges(self) -> List[str]:
        """
        List all connected edges and their plugins.
        """
        resp = self._http.post("/edge/list")
        _raise(resp)

        data = resp.json().get('data', {})
        edges = data.get('edges', {})
        return list(edges.keys())

    def get_edge_client(self, edge_id: str) -> "EdgeClient":
        """
        Get a client for a specific edge.
        """
        return EdgeClient(self, edge_id)


class EdgeClient:
    """
    Client for interacting with a specific Edge Service.
    """

    def __init__(self, bridge_client: BridgeClient, edge_id: str):
        self._bc = bridge_client
        self._edge_id = edge_id
        # Bridge forwards /{edge_id} -> Edge Service root
        self._edge_base = f"/{self._edge_id}"

    @property
    def http(self) -> httpx.Client:
        return self._bc._http

    def list_plugins(self) -> dict:
        """
        Return all plugins registered on this edge (enabled and disabled).

        Returns a dict mapping plugin name to its endpoint info, e.g.::

            {"sysinfo": {"enabled": True, "namespace": "/edge1/sysinfo", ...},
             "queue_info": {"enabled": False, ...}}
        """
        resp = self._bc._http.post("/edge/list")
        _raise(resp)
        data  = resp.json().get('data', {})
        edges = data.get('edges', {})
        edge_data = edges.get(self._edge_id, {})
        return edge_data.get('plugins', {})

    def get_plugin(self, plugin_name: str, **session_kwargs) -> "PluginClient":
        """
        Get a client helper for a plugin loaded on the edge.

        Any extra keyword arguments are forwarded to the client's
        ``register_session()`` call (e.g. ``backends=['local']``).
        """

        # 1. Discover plugin namespace from Bridge (reuse list_plugins)
        plugins = self.list_plugins()
        plugin_info = plugins.get(plugin_name)
        if not plugin_info:
            raise RuntimeError(f"Plugin '{plugin_name}' unknown on '{self._edge_id}'")

        namespace = plugin_info['namespace']

        # 2. Determine Client Helper Class
        plugin_cls = Plugin.get_plugin_class(plugin_name)
        if not plugin_cls:
            raise RuntimeError(f"Plugin class for '{plugin_name}' not found")

        client_cls = getattr(plugin_cls, 'client_class', None)
        if not client_cls:
            raise RuntimeError(f"Plugin client '{plugin_name}': not known")

        # 3. Instantiate Client Helper
        base_url = namespace
        client = client_cls(self.http, base_url, bridge_client=self._bc, edge_id=self._edge_id, plugin_name=plugin_name)

        # 4. Register Session
        client.register_session(**session_kwargs)

        return client


class PluginClient:
    """
    Base helper class for Edge Plugins (Application side).

    Notification Callbacks
    ----------------------
    Register callbacks to receive real-time notifications from this plugin::

        def on_job_status(edge, plugin, topic, data):
            print(f"Job {data['job_id']}: {data['status']}")

        psij = edge.get_plugin("psij")
        psij.register_notification_callback(on_job_status)

        # Or filter by topic:
        psij.register_notification_callback(on_job_status, topic="job_status")
    """

    def __init__(self, http_client: httpx.Client, base_url: str, bridge_client: "BridgeClient" = None, edge_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = bridge_client
        self._edge_id = edge_id
        self._plugin_name = plugin_name
        self._sid: Optional[str] = None

    def register_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """
        Register a callback to receive notifications from this plugin.

        Args:
            callback: Function to call. Receives (edge, plugin, topic, data).
            topic: Optional topic filter. If None, receives all topics.

        Example::

            def on_status(edge, plugin, topic, data):
                print(f"{topic}: {data}")

            # All notifications from this plugin
            client.register_notification_callback(on_status)

            # Only job_status notifications
            client.register_notification_callback(on_status, topic="job_status")
        """
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info; cannot register notifications.")
        self._bc.register_callback(edge_id=self._edge_id, plugin_name=self._plugin_name,
                                   topic=topic, callback=callback)

    def unregister_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """Unregister a previously registered callback."""
        if not self._bc or not self._edge_id or not self._plugin_name:
            raise RuntimeError("Missing edge tracking info.")
        self._bc.unregister_callback(edge_id=self._edge_id, plugin_name=self._plugin_name,
                                     topic=topic, callback=callback)

    @property
    def sid(self) -> Optional[str]:
        """Return the current session ID."""
        return self._sid

    def _require_session(self) -> None:
        """Raise RuntimeError if no session is active."""
        if not self._sid:
            raise RuntimeError("No active session")

    def _url(self, path: str) -> str:
        """Construct full URL for a path."""
        return f"{self._base_url}/{path.lstrip('/')}"

    def _raise(self, resp, context: str = '') -> None:
        """Raise RuntimeError with HTTP status, origin, optional context, and server detail."""
        origin = '/'.join(filter(None, [self._edge_id, self._plugin_name]))
        _raise(resp, f"[{origin}] {context}" if context else f"[{origin}]")

    def register_session(self, **kwargs: Any) -> None:
        """
        Register a session with the plugin.

        Subclasses may override to accept plugin-specific keyword
        arguments (e.g. ``backends``).
        """
        resp = self._http.post(self._url("register_session"))
        self._raise(resp)
        self._sid = resp.json()['sid']

    def unregister_session(self) -> None:
        """
        Unregister the current session.
        """
        if self._sid:
            resp = self._http.post(self._url(f"unregister_session/{self._sid}"))
            self._raise(resp)
            self._sid = None

    def close(self) -> None:
        """
        Close the client helper. Unregisters session if active.
        """
        if self._sid:
            try:
                self.unregister_session()
            except Exception as e:
                log.warning("Failed to unregister session on close: %s", e)

