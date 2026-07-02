"""
Client API for ORBIT.

This module provides the base :class:`PluginClient` helper that every plugin's
``client_class`` subclasses (``SysInfoClient``, ``PSIJClient``, …).  It speaks a
small ``httpx``-shaped transport surface (``self._http.get/post`` +
:meth:`PluginClient._request`) so a helper is transport-agnostic: the endpoint
runtime rides it over a WebSocket by swapping in a transport shim
(:class:`radical.orbit.runtime_client.RuntimePluginClient`), and plugin-level
``TestClient`` tests ride it over HTTP.

Notification Callbacks
----------------------
Register callbacks to receive real-time notifications from a plugin::

    def on_job_status(endpoint, plugin, topic, data):
        print(f"Job {data['job_id']}: {data['status']}")

    psij = runtime.get_plugin("hpc1", "psij")
    psij.register_notification_callback(on_job_status, topic="job_status")

All callbacks receive four arguments: ``(endpoint, plugin, topic, data)``.
"""

import logging

from typing import Any, Callable, Optional


log = logging.getLogger("radical.orbit.client")


def _raise(resp, context: str = '') -> None:
    """Raise RuntimeError with HTTP status, optional context, and server detail."""
    if resp.is_error:
        try:   detail = str(resp.json().get('detail') or '')
        except Exception: detail = resp.text or ''
        parts = [f"HTTP {resp.status_code}"]
        if context: parts.append(context)
        if detail:  parts.append(detail)
        raise RuntimeError(' — '.join(parts))


class PluginClient:
    """
    Base helper class for Endpoint Plugins (Application side).

    Notification Callbacks
    ----------------------
    Register callbacks to receive real-time notifications from this plugin::

        def on_job_status(endpoint, plugin, topic, data):
            print(f"Job {data['job_id']}: {data['status']}")

        psij = runtime.get_plugin("hpc1", "psij")
        psij.register_notification_callback(on_job_status)

        # Or filter by topic:
        psij.register_notification_callback(on_job_status, topic="job_status")

    The base implementation registers over an injected notification client
    (``self._bc``); :class:`~radical.orbit.runtime_client.RuntimePluginClient`
    overrides these to ride the runtime's callback registry over the WebSocket.
    """

    def __init__(self, http_client, base_url: str, broker_client=None,
                 endpoint_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = broker_client
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

    def _request(self, method: str, url: str, **kwargs):
        """Single transport seam for every helper-facing call.

        Dispatches to the matching verb method on ``self._http``
        (``self._http.post`` / ``.get`` / …) so behaviour is **bit-identical**
        to the direct ``self._http.post(...)`` calls the 11 plugin helpers
        make (and the old suite mocks).  The seam is overridable in two ways:
        swap ``self._http`` for a transport shim (what ``RuntimePluginClient``
        does), or override this method.
        """
        return getattr(self._http, method.lower())(url, **kwargs)

    def _raise(self, resp, context: str = '') -> None:
        """Raise RuntimeError with HTTP status, origin, optional context, and server detail."""
        origin = '/'.join(filter(None, [self._endpoint_id, self._plugin_name]))
        _raise(resp, f"[{origin}] {context}" if context else f"[{origin}]")

    def register_session(self, sid: Optional[str] = None,
                         lifetime: Optional[str] = None,
                         ttl: Optional[float] = None, **kwargs: Any) -> None:
        """
        Register (or reconnect to) a session with the plugin.

        Args:
            sid:      Client-supplied session ID for create-or-reconnect
                      (``None`` mints a fresh one).
            lifetime: Session lifetime policy — ``'ephemeral'`` (default),
                      ``'ttl'``, or ``'persistent'``.
            ttl:      Seconds until expiry; required (and only valid) when
                      ``lifetime='ttl'``.

        Subclasses may override to accept plugin-specific keyword
        arguments (e.g. ``backends``).
        """
        payload = {}
        if sid      is not None: payload['sid']      = sid
        if lifetime is not None: payload['lifetime'] = lifetime
        if ttl      is not None: payload['ttl']      = ttl
        resp = self._request("POST", self._url("register_session"),
                             json=payload or None)
        self._raise(resp)
        self._sid = resp.json()['sid']

    def unregister_session(self) -> None:
        """
        Unregister the current session.
        """
        if self._sid:
            resp = self._request(
                "POST", self._url(f"unregister_session/{self._sid}"))
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
