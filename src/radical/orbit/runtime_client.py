"""Consumer-side plugin-client transport seam for the endpoint runtime.

The 11 plugin ``client_class`` helpers (``SysInfoClient``, ``PSIJClient``, …)
subclass :class:`radical.orbit.client.PluginClient` and speak an httpx-ish
surface (``self._http.get/post`` + ``PluginClient._request``).  This module
lets every one of them ride an :class:`~radical.orbit.runtime.EndpointRuntime`
WebSocket **unchanged**:

* :class:`RuntimeResponse` — an ``httpx.Response``-shaped result satisfying
  ``PluginClient._raise`` and the helpers (``status_code``/``json()``/
  ``content``/``text``/``is_error``) *and* the plain ``status``/``headers``/
  ``body`` consumer-API contract.
* :class:`_RuntimeHTTP` — a minimal ``httpx.Client``-shaped adapter bound to
  ``(runtime, dst)`` installed as the helper's ``self._http``.
* :class:`RuntimePluginClient` — the seam mixed under a concrete helper class
  by ``EndpointRuntime.get_plugin``: it overrides *only* the transport-touching
  surface (the ``_http`` object + notification registration over the runtime's
  callback registry instead of a ``BridgeClient`` + SSE).
"""

import json as _json

from typing       import Any, Callable, Dict, Optional
from urllib.parse import urlencode

from .client import PluginClient


class RuntimeResponse:
    """Response object returned by :meth:`EndpointRuntime.call` and handed to
    the plugin helpers.  Satisfies both the consumer-API surface
    (``status``/``headers``/``body``) and the httpx-ish surface the helpers +
    ``PluginClient._raise`` rely on."""

    def __init__(self, status: int, headers: Dict[str, str], body: bytes):
        self.status      = status
        self.status_code = status
        self.headers     = headers or {}
        self.body        = body or b''
        self.content     = self.body

    @property
    def text(self) -> str:
        return self.content.decode('utf-8', 'replace')

    @property
    def is_error(self) -> bool:
        return self.status_code >= 400

    def json(self) -> Any:
        return _json.loads(self.content) if self.content else {}


class _RuntimeHTTP:
    """Minimal ``httpx.Client``-shaped adapter bound to ``(runtime, dst)``.

    Installed as a plugin helper's ``self._http`` so every helper's
    ``self._http.get/post`` (and ``PluginClient._request``) rides
    ``EndpointRuntime.call`` instead of HTTP — no helper changes.
    """

    def __init__(self, runtime, dst: str):
        self._runtime = runtime
        self._dst     = dst

    def get(self, url: str, *, params=None, headers=None, **kw
            ) -> RuntimeResponse:
        return self._do('GET', url, params=params, headers=headers)

    def post(self, url: str, *, json=None, content=None, data=None,
             params=None, headers=None, **kw) -> RuntimeResponse:
        return self._do('POST', url, json=json, content=content, data=data,
                        params=params, headers=headers)

    def request(self, method: str, url: str, *, json=None, content=None,
                data=None, params=None, headers=None, **kw) -> RuntimeResponse:
        return self._do(method, url, json=json, content=content, data=data,
                        params=params, headers=headers)

    def _do(self, method, url, json=None, content=None, data=None,
            params=None, headers=None) -> RuntimeResponse:
        path = url if not params else f"{url}?{urlencode(params)}"
        hdrs: Dict[str, str] = dict(headers or {})
        body: bytes = b''
        if json is not None:
            body = _json.dumps(json).encode('utf-8')
            hdrs.setdefault('content-type', 'application/json')
        elif content is not None:
            body = content if isinstance(content, (bytes, bytearray)) \
                           else str(content).encode('utf-8')
        elif data is not None:
            body = urlencode(data).encode('utf-8')
            hdrs.setdefault('content-type', 'application/x-www-form-urlencoded')
        return self._runtime.call(self._dst, method, path,
                                  body=bytes(body), headers=hdrs)


class RuntimePluginClient(PluginClient):
    """Transport seam for the plugin ``client_class`` helpers over a runtime.

    A concrete helper class is mixed on top of this by
    ``EndpointRuntime.get_plugin``; this base overrides only the
    transport-touching surface: the ``self._http`` object is a
    :class:`_RuntimeHTTP` adapter (installed at construction), and notification
    registration rides the runtime's callback registry instead of a
    ``BridgeClient`` + SSE.
    """

    _runtime = None

    def register_notification_callback(self, callback: Callable,
                                       topic: Optional[str] = None) -> None:
        if self._runtime is None:
            raise RuntimeError("no runtime bound to this plugin client")
        self._runtime.register_callback(
            endpoint_id=self._endpoint_id, plugin_name=self._plugin_name,
            topic=topic, callback=callback)

    def unregister_notification_callback(self, callback: Callable,
                                         topic: Optional[str] = None) -> None:
        if self._runtime is None:
            raise RuntimeError("no runtime bound to this plugin client")
        self._runtime.unregister_callback(
            endpoint_id=self._endpoint_id, plugin_name=self._plugin_name,
            topic=topic, callback=callback)
