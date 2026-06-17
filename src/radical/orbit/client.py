"""
Client API for ORBIT.

This module provides Python client classes for interacting with the ORBIT
bridge and endpoint services. It includes support for real-time notifications via
Server-Sent Events (SSE).

Classes
-------
BridgeClient
    Main client for connecting to the bridge. Supports notification callbacks.

EndpointClient
    Client for interacting with a specific endpoint service.

PluginClient
    Base class for plugin-specific client helpers.

Quick Start
-----------
::

    from radical.orbit.client import BridgeClient

    # Connect to bridge
    client = BridgeClient(url="http://localhost:8000")

    # List connected endpoints
    endpoints = client.list_endpoints()
    print(f"Connected endpoints: {endpoints}")

    # Get a plugin client
    endpoint = client.get_endpoint_client("my_endpoint")
    psij = endpoint.get_plugin("psij")

    # Register for notifications
    def on_job_update(endpoint, plugin, topic, data):
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

2. **Endpoint-specific** - all notifications from plugins on an endpoint::

    client.register_callback(endpoint_id="hpc1", callback=my_handler)

3. **Plugin-specific** - notifications from a specific plugin::

    client.register_callback(endpoint_id="hpc1", plugin_name="psij", callback=my_handler)

4. **Topic-specific** - notifications for a specific topic::

    client.register_callback(endpoint_id="hpc1", plugin_name="psij",
                             topic="job_status", callback=my_handler)

5. **Via PluginClient** - convenience method::

    psij.register_notification_callback(my_handler, topic="job_status")

All callbacks receive four arguments: ``(endpoint, plugin, topic, data)``.

Topology Callbacks
------------------
Register for endpoint connect/disconnect events::

    def on_topology_change(endpoints):
        '''Called when endpoints connect or disconnect.

        Args:
            endpoints: Dict mapping endpoint names to plugin info.
        '''
        print(f"Connected: {list(endpoints.keys())}")

    client.register_topology_callback(on_topology_change)
"""

import httpx
import logging
import urllib3
import json
import itertools

from .http_utils import make_http_client
import threading

import time as _time
from typing import Any, Dict, Iterable, List, Optional, Callable, Tuple, Union

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

log = logging.getLogger("radical.orbit.client")

# Silence per-request INFO logging from httpx/httpcore
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class BridgeClient:
    """
    Client for interacting with the ORBIT Bridge.

    Notification Callbacks
    ----------------------
    The client supports receiving real-time notifications from plugins via SSE.
    Callbacks can be registered at three levels:

    1. **Global callbacks** - receive all notifications::

        def my_callback(endpoint, plugin, topic, data):
            print(f"{endpoint}/{plugin}: {topic} -> {data}")

        client.register_callback(callback=my_callback)

    2. **Plugin-specific callbacks** - receive notifications from a specific plugin::

        def job_callback(endpoint, plugin, topic, data):
            print(f"Job update: {topic} -> {data}")

        client.register_callback(endpoint_id="hpc1", plugin_name="psij", callback=job_callback)

    3. **Topic-specific callbacks** - receive notifications for a specific topic::

        def status_callback(endpoint, plugin, topic, data):
            print(f"Status: {data}")

        client.register_callback(endpoint_id="hpc1", plugin_name="psij",
                                 topic="job_status", callback=status_callback)

    Callbacks receive four arguments: endpoint (str), plugin (str), topic (str), data (dict).

    Topology Callbacks
    ------------------
    Register for endpoint connect/disconnect events::

        def on_topology(endpoints):
            print(f"Connected endpoints: {list(endpoints.keys())}")

        client.register_topology_callback(on_topology)
    """

    def __init__(self, url: Optional[str] = None, cert: Optional[str] = None):
        """
        Initialize the Bridge Client.

        Args:
            url: The bridge URL.  CLI > env (``RADICAL_ORBIT_BRIDGE_URL``) >
                 file (``~/.radical/orbit/bridge.url``).
            cert: Path to CA cert.  Same precedence using
                  ``RADICAL_ORBIT_BRIDGE_CERT`` and
                  ``~/.radical/orbit/bridge_cert.pem``.  Required when
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

        self._prof = rprof.Profiler('client', ns='radical.orbit')
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

        self._http: httpx.Client = make_http_client(
            base_url=self._url,
            verify=self._cert if self._cert else False,
            # Match the bridge's REQUEST_TIMEOUT (600s).  Submit batches
            # of 1000s of tasks can take many seconds at the endpoint; a 60s
            # client cap would 504 long before the bridge would.
            timeout=600.0,
            event_hooks={'request' : [_inject_req_id],
                         'response': [_on_response]},
        )
        # Callbacks: key is (endpoint_id, plugin_name, topic) - None means wildcard
        self._callbacks: Dict[Tuple[Optional[str], Optional[str], Optional[str]], List[Callable]] = {}
        self._topology_callbacks: List[Callable] = []
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_stop: threading.Event = threading.Event()
        self._listener_connected: threading.Event = threading.Event()

    @property
    def url(self) -> str:
        """Resolved bridge URL (trailing slash stripped)."""
        return self._url

    def register_callback(self, endpoint_id: Optional[str] = None, plugin_name: Optional[str] = None,
                          topic: Optional[str] = None, callback: Callable = None) -> None:
        """
        Register a notification callback.

        Args:
            endpoint_id: Filter by endpoint name (None = all endpoints)
            plugin_name: Filter by plugin name (None = all plugins)
            topic: Filter by notification topic (None = all topics)
            callback: Function to call. Receives (endpoint, plugin, topic, data).

        Example::

            # All notifications
            client.register_callback(callback=my_handler)

            # Only job_status from psij on hpc1
            client.register_callback(endpoint_id="hpc1", plugin_name="psij",
                                     topic="job_status", callback=job_handler)
        """
        if callback is None:
            raise ValueError("callback is required")
        key = (endpoint_id, plugin_name, topic)
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)
        self._ensure_listener()

    def unregister_callback(self, endpoint_id: Optional[str] = None, plugin_name: Optional[str] = None,
                            topic: Optional[str] = None, callback: Callable = None) -> None:
        """Unregister a notification callback."""
        key = (endpoint_id, plugin_name, topic)
        if key in self._callbacks and callback in self._callbacks[key]:
            self._callbacks[key].remove(callback)

    def register_topology_callback(self, callback: Callable) -> None:
        """
        Register a callback for topology changes (endpoint connect/disconnect).

        Args:
            callback: Function to call. Receives endpoints dict mapping endpoint names
                      to their plugin info.

        Example::

            def on_topology(endpoints):
                for name, info in endpoints.items():
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

    def _dispatch_notification(self, endpoint: str, plugin: str, topic: str, data: dict) -> None:
        """Dispatch a notification to matching callbacks."""
        log.debug("[client] dispatch: endpoint=%s  plugin=%s  topic=%s  registered_keys=%s",
                  endpoint, plugin, topic, list(self._callbacks.keys()))
        matched = 0
        # Check all registered callback patterns
        for (e_filter, p_filter, t_filter), callbacks in self._callbacks.items():
            # Match if filter is None (wildcard) or matches exactly
            if (e_filter is None or e_filter == endpoint) and \
               (p_filter is None or p_filter == plugin) and \
               (t_filter is None or t_filter == topic):
                matched += len(callbacks)
                for cb in callbacks:
                    try:
                        cb(endpoint, plugin, topic, data)
                    except Exception as e:
                        log.error("Notification callback error: %s", e)
        if not matched:
            log.debug("[client] dispatch: no callbacks matched for endpoint=%s plugin=%s topic=%s",
                      endpoint, plugin, topic)

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
                                log.debug("[client] SSE notification: endpoint=%s  plugin=%s  topic=%s",
                                          notif.get("endpoint"), notif.get("plugin"), notif.get("topic"))
                                self._dispatch_notification(
                                    endpoint=notif.get("endpoint"),
                                    plugin=notif.get("plugin"),
                                    topic=notif.get("topic"),
                                    data=notif.get("data", {})
                                )

                            elif msg_topic == "topology":
                                endpoints = payload.get("data", {}).get("endpoints", {})
                                log.debug("[client] SSE topology: endpoints=%s", list(endpoints.keys()))
                                for cb in self._topology_callbacks:
                                    try:
                                        cb(endpoints)
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

    def list_endpoints(self) -> List[str]:
        """
        List all connected endpoints and their plugins.
        """
        resp = self._http.post("/endpoint/list")
        _raise(resp)

        data = resp.json().get('data', {})
        endpoints = data.get('endpoints', {})
        return list(endpoints.keys())

    def wait_for_endpoint(self,
                      names: Union[str, Iterable[str]],
                      timeout: float = 1800.0,
                      poll: float = 3.0,
                      on_heartbeat: Optional[Callable[[], None]] = None,
                      heartbeat_interval: float = 10.0,
                      ) -> str:
        """Block until any of *names* appears in :meth:`list_endpoints`.

        Args:
            names:              Endpoint name or iterable of names to wait for.
            timeout:            Maximum seconds to wait.
            poll:               Seconds between :meth:`list_endpoints` polls.
            on_heartbeat:       Optional callback fired at most every
                                ``heartbeat_interval`` seconds while we
                                are still waiting.  Useful for printing
                                progress dots during long queue waits;
                                kept outside this class so the API
                                doesn't touch stdout.
            heartbeat_interval: Minimum seconds between heartbeat
                                callbacks; ignored when on_heartbeat
                                is None.

        Returns:
            The first name from *names* observed live.

        Raises:
            TimeoutError: No expected name appeared within *timeout*.
            ValueError:   *names* is empty.
        """
        expected = [names] if isinstance(names, str) else list(names)
        if not expected:
            raise ValueError('no endpoint names — nothing to wait for')

        start_t = _time.time()
        last_hb = start_t
        while _time.time() - start_t < timeout:
            live = set(self.list_endpoints())
            for name in expected:
                if name in live:
                    return name
            _time.sleep(poll)
            if on_heartbeat is not None \
                    and _time.time() - last_hb >= heartbeat_interval:
                on_heartbeat()
                last_hb = _time.time()
        raise TimeoutError(f'no endpoint appeared within {timeout}s; '
                           f'expected one of {expected}')

    def get_endpoint_client(self, endpoint_id: str) -> "EndpointClient":
        """
        Get a client for a specific endpoint.
        """
        return EndpointClient(self, endpoint_id)


class EndpointClient:
    """
    Client for interacting with a specific Endpoint Service.
    """

    def __init__(self, bridge_client: BridgeClient, endpoint_id: str):
        self._bc = bridge_client
        self._endpoint_id = endpoint_id
        # Bridge forwards /{endpoint_id} -> Endpoint Service root
        self._endpoint_base = f"/{self._endpoint_id}"

    @property
    def http(self) -> httpx.Client:
        return self._bc._http

    def list_plugins(self) -> dict:
        """
        Return all plugins registered on this endpoint (enabled and disabled).

        Returns a dict mapping plugin name to its endpoint info, e.g.::

            {"sysinfo": {"enabled": True, "namespace": "/endpoint1/sysinfo", ...},
             "queue_info": {"enabled": False, ...}}
        """
        resp = self._bc._http.post("/endpoint/list")
        _raise(resp)
        data  = resp.json().get('data', {})
        endpoints = data.get('endpoints', {})
        endpoint_data = endpoints.get(self._endpoint_id, {})
        return endpoint_data.get('plugins', {})

    def get_plugin(self, plugin_name: str, **session_kwargs) -> "PluginClient":
        """
        Get a client helper for a plugin loaded on the endpoint.

        Any extra keyword arguments are forwarded to the client's
        ``register_session()`` call (e.g. ``backends=['local']``).
        """

        # 1. Discover plugin namespace from Bridge (reuse list_plugins)
        plugins = self.list_plugins()
        plugin_info = plugins.get(plugin_name)
        if not plugin_info:
            raise RuntimeError(f"Plugin '{plugin_name}' unknown on '{self._endpoint_id}'")

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
        client = client_cls(self.http, base_url, bridge_client=self._bc, endpoint_id=self._endpoint_id, plugin_name=plugin_name)

        # 4. Register Session
        client.register_session(**session_kwargs)

        return client


class PluginClient:
    """
    Base helper class for Endpoint Plugins (Application side).

    Notification Callbacks
    ----------------------
    Register callbacks to receive real-time notifications from this plugin::

        def on_job_status(endpoint, plugin, topic, data):
            print(f"Job {data['job_id']}: {data['status']}")

        psij = endpoint.get_plugin("psij")
        psij.register_notification_callback(on_job_status)

        # Or filter by topic:
        psij.register_notification_callback(on_job_status, topic="job_status")
    """

    def __init__(self, http_client: httpx.Client, base_url: str, bridge_client: "BridgeClient" = None, endpoint_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = bridge_client
        self._endpoint_id = endpoint_id
        self._plugin_name = plugin_name
        self._sid: Optional[str] = None

    def register_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """
        Register a callback to receive notifications from this plugin.

        Args:
            callback: Function to call. Receives (endpoint, plugin, topic, data).
            topic: Optional topic filter. If None, receives all topics.

        Example::

            def on_status(endpoint, plugin, topic, data):
                print(f"{topic}: {data}")

            # All notifications from this plugin
            client.register_notification_callback(on_status)

            # Only job_status notifications
            client.register_notification_callback(on_status, topic="job_status")
        """
        if not self._bc or not self._endpoint_id or not self._plugin_name:
            raise RuntimeError("Missing endpoint tracking info; cannot register notifications.")
        self._bc.register_callback(endpoint_id=self._endpoint_id, plugin_name=self._plugin_name,
                                   topic=topic, callback=callback)

    def unregister_notification_callback(self, callback: Callable, topic: Optional[str] = None) -> None:
        """Unregister a previously registered callback."""
        if not self._bc or not self._endpoint_id or not self._plugin_name:
            raise RuntimeError("Missing endpoint tracking info.")
        self._bc.unregister_callback(endpoint_id=self._endpoint_id, plugin_name=self._plugin_name,
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
        origin = '/'.join(filter(None, [self._endpoint_id, self._plugin_name]))
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

