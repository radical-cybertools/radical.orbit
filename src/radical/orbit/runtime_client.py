"""Consumer-side plugin-client transport seam for the endpoint runtime.

The 11 plugin ``client_class`` helpers (``SysInfoClient``, ``PSIJClient``, …)
subclass :class:`radical.orbit.client.PluginClient` and speak an httpx-ish
surface (``self._http.get/post`` + ``PluginClient._request``).  This module
lets every one of them ride an :class:`~radical.orbit.runtime.EndpointRuntime`
WebSocket **unchanged**:

* :class:`RuntimeResponse` — an ``httpx.Response``-shaped result satisfying
  ``PluginClient._raise`` and the helpers (``status_code``/``json()``/
  ``content``/``text``/``is_error``).
* :class:`_RuntimeHTTP` — a minimal ``httpx.Client``-shaped adapter bound to
  ``(runtime, dst)`` installed as the helper's ``self._http``.  Its verbs are
  sync (user-thread callers); each rides ``EndpointRuntime.call``.

``EndpointRuntime.get_plugin`` constructs a helper directly with a
:class:`_RuntimeHTTP` transport and ``broker_client=`` the runtime itself, so the
base ``PluginClient.register_notification_callback`` forwards notification
(un)registration straight to the runtime's callback registry — no seam subclass.
"""

import asyncio
import json as _json

from typing       import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

from .client import PluginClient  # noqa: F401  (re-export convenience)


def _pack_request(method: str, url: str, *, json=None, content=None, data=None,
                  params=None, headers=None):
    """Turn httpx-shaped kwargs into ``(path, body_bytes, headers)``.

    The one place that maps a helper's ``self._http.get/post(...)`` call shape
    onto the ``(method, path, body, headers)`` a broker/runtime transport
    speaks.  Shared by :class:`_RuntimeHTTP` (WebSocket, user threads) and
    :class:`CallerHTTP` (broker caller, host loop) so both agree on encoding.
    """
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
        if isinstance(data, (bytes, bytearray)):
            body = bytes(data)                       # pre-encoded (e.g. msgpack)
        else:
            body = urlencode(data).encode('utf-8')
            hdrs.setdefault('content-type', 'application/x-www-form-urlencoded')
    return path, bytes(body), hdrs


def unpack_response_dict(resp: Dict[str, Any]) -> Tuple[int, Dict[str, str],
                                                        bytes]:
    """Normalize a broker/caller ``response`` dict to ``(status, headers, body)``.

    The body is coerced to ``bytes``.  One helper for the three places that
    unpacked this shape by hand (the gateway HTTP proxy and :class:`CallerHTTP`).
    """
    status = int(resp.get('status', 502))
    body   = resp.get('body') or b''
    if isinstance(body, str):
        body = body.encode()
    return status, resp.get('headers') or {}, body


class RuntimeResponse:
    """An ``httpx.Response``-shaped result returned by
    :meth:`EndpointRuntime.call` and handed to the plugin helpers — satisfying
    the httpx surface (``status_code``/``headers``/``content``/``text``/
    ``json()``/``is_error``) that the helpers and ``PluginClient._raise`` use."""

    def __init__(self, status: int, headers: Dict[str, str], body: bytes):
        self.status_code = status
        self.headers     = headers or {}
        self.content     = body or b''

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
        path, body, hdrs = _pack_request(
            method, url, json=json, content=content, data=data,
            params=params, headers=headers)
        return self._runtime.call(self._dst, method, path,
                                  body=body, headers=hdrs)


class CallerHTTP:
    """Async transport shim over a broker in-process caller (host-loop side).

    Installed as a plugin helper's ``self._http`` in the broker-hosted task
    dispatcher.  The helper's ``async`` cores reach the seam through
    :meth:`PluginClient._arequest`, which prefers this object's async
    :meth:`arequest`: it awaits ``asyncio.wrap_future(caller.call_threadsafe(
    ...))`` so the call rides the routing loop **without blocking the host
    loop**, and wraps the broker ``response`` dict in a :class:`RuntimeResponse`
    so the helpers' ``_raise`` / parsers are shared with every other transport.

    The sync ``get``/``post``/``request`` verbs deliberately raise: on the host
    loop only the async cores may run — a sync helper call here would block the
    loop, which this shim exists to prevent.
    """

    def __init__(self, caller, dst: str, timeout: Optional[float] = None):
        self._caller  = caller
        self._dst     = dst
        self._timeout = timeout

    async def arequest(self, method: str, url: str, *, json=None, content=None,
                       data=None, params=None, headers=None, **_kw
                       ) -> RuntimeResponse:
        path, body, hdrs = _pack_request(
            method, url, json=json, content=content, data=data,
            params=params, headers=headers)
        fut  = self._caller.call_threadsafe(
            self._dst, method, path, body=body, headers=hdrs,
            timeout=self._timeout)
        resp = await asyncio.wrap_future(fut)
        return RuntimeResponse(*unpack_response_dict(resp))

    def _blocked(self, *_a, **_k):
        raise RuntimeError(
            'CallerHTTP is async-only (broker host loop): drive the a<method> '
            'cores, never the sync helper verbs')

    get = post = request = _blocked
