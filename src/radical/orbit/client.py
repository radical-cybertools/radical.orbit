"""
Client API for ORBIT.

This module provides the base :class:`PluginClient` helper that every plugin's
``client_class`` subclasses (``SysInfoClient``, ``PSIJClient``, …).  It speaks a
small ``httpx``-shaped transport surface (``self._http.get/post`` +
:meth:`PluginClient._request`) so a helper is transport-agnostic: the endpoint
runtime rides it over a WebSocket by injecting a
:class:`radical.orbit.runtime_client._RuntimeHTTP` transport (and passing the
runtime as ``broker_client`` so notifications register against the runtime's
callback registry), and plugin-level ``TestClient`` tests ride it over HTTP.

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


def _run_sync(coro):
    """Drive a client async-core coroutine to completion on the sync transport.

    A helper's public sync method (``PSIJClient.submit_tunneled`` …) is a thin
    wrapper around its ``async def`` core; the core awaits
    :meth:`PluginClient._arequest`, whose default implementation returns the
    sync :meth:`PluginClient._request` result **without suspending** (no event
    loop, no future).  The coroutine therefore completes in a single ``send``,
    so this driver reduces the sync path to the same ``self._http.<verb>(...)``
    call the helpers made before — bit-identical, and never touching an event
    loop.  A core that *does* suspend here (i.e. was handed an async transport)
    is a bug, and is reported as such.
    """
    try:
        coro.send(None)
    except StopIteration as e:
        return e.value
    coro.close()
    raise RuntimeError(
        'client async core suspended on the sync transport — a caller-backed '
        'async core must be awaited, not driven synchronously')


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
    (``self._bc``); the endpoint runtime passes itself as ``self._bc`` (via
    ``get_plugin(..., broker_client=runtime)``), so these calls ride the
    runtime's callback registry over the WebSocket with no subclass.
    """

    def __init__(self, http_client, base_url: str, broker_client=None,
                 endpoint_id: str = None, plugin_name: str = None):
        self._http = http_client
        self._base_url = base_url.rstrip('/')
        self._bc = broker_client
        self._endpoint_id = endpoint_id
        self._plugin_name = plugin_name
        self._sid: Optional[str] = None
        # Async transport for the ``a<method>`` cores.  ``None`` (the default,
        # incl. every user-thread client) routes async cores through the sync
        # ``_request`` — driven by ``_run_sync`` in one step, bit-identical.
        # The broker-hosted dispatcher installs a caller-backed transport here
        # (a ``CallerHTTP``) so the cores await the routing loop.  An explicit
        # attribute (not attribute-sniffing) so a ``MagicMock`` ``_http`` in a
        # test never looks like an async transport.
        self._async_http = None

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

    async def _arequest(self, method: str, url: str, **kwargs):
        """Async twin of :meth:`_request` — the transport seam for async cores.

        When an async transport is installed (``self._async_http`` — the
        broker-hosted :class:`~radical.orbit.runtime_client.CallerHTTP`), await
        it so the call rides the routing loop without ever blocking the host
        loop.  Otherwise fall back to the sync :meth:`_request`; driven by
        :func:`_run_sync` that fallback completes in one step without an event
        loop, keeping the user-thread sync path bit-identical.
        """
        if self._async_http is not None:
            return await self._async_http.arequest(method, url, **kwargs)
        return self._request(method, url, **kwargs)

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
        resp = self._request("POST", self._url("register_session"),
                             json=self._session_payload(sid, lifetime, ttl))
        self._raise(resp)
        self._sid = resp.json()['sid']

    @staticmethod
    def _session_payload(sid: Optional[str], lifetime: Optional[str],
                         ttl: Optional[float]):
        """Build the ``register_session`` request body (``None`` when empty).

        The one place that shapes the session payload — shared by the sync
        :meth:`register_session` and the async :meth:`aregister_session`."""
        payload = {}
        if sid      is not None: payload['sid']      = sid
        if lifetime is not None: payload['lifetime'] = lifetime
        if ttl      is not None: payload['ttl']      = ttl
        return payload or None

    async def aregister_session(self, sid: Optional[str] = None,
                                lifetime: Optional[str] = None,
                                ttl: Optional[float] = None,
                                **kwargs: Any) -> None:
        """Async core for :meth:`register_session` (see it for arguments).

        The broker-hosted dispatcher awaits this on the host loop (its
        ``self._async_http`` routes the call over the routing loop); the
        user-thread path uses the sync :meth:`register_session` instead.
        """
        resp = await self._arequest("POST", self._url("register_session"),
                                    json=self._session_payload(sid, lifetime,
                                                               ttl))
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
