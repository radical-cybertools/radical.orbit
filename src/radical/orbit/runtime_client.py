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
* :func:`max_body_of` / :func:`pack_batches` — the request-body limit of
  a transport, and the byte-bounded split of a list into msgpack bodies
  that fit it.

``EndpointRuntime.get_plugin`` constructs a helper directly with a
:class:`_RuntimeHTTP` transport and ``broker_client=`` the runtime itself, so the
base ``PluginClient.register_notification_callback`` forwards notification
(un)registration straight to the runtime's callback registry — no seam subclass.
"""

import json as _json

from typing       import Any, Dict, Iterator, List, Tuple
from urllib.parse import urlencode

import httpx
import msgpack

from . import protocol
from .client import PluginClient  # noqa: F401  (re-export convenience)


class PayloadTooLarge(RuntimeError):
    """Request items that no frame can carry (HTTP 413, before any send).

    ``indices`` are the offending items' positions, ``uids`` their ``uid``
    (for dict items carrying one), so a caller can drop just those.
    """

    status_code = 413

    def __init__(self, msg: str = '', indices=(), uids=()):
        super().__init__(msg)
        self.indices = list(indices)
        self.uids    = list(uids)

    def __reduce__(self):
        return (type(self), (str(self), self.indices, self.uids))


def max_body_of(holder) -> int:
    """Return the request-body limit of a transport or frame-cap *holder*.

    One lookup for every caller: a transport's own ``max_body`` (the
    runtime and broker-caller transports), else a holder's published
    ``frame_cap`` (``EndpointRuntime``, ``BrokerCaller``) through
    :func:`protocol.max_body`, else the protocol default — which is what a
    plain httpx client (gateway HTTP) gets, and what the gateway enforces
    on its ingress.
    """
    val = getattr(holder, 'max_body', None)
    if isinstance(val, int) and val > 0:
        return val
    cap = getattr(holder, 'frame_cap', None)
    if not (isinstance(cap, int) and cap > 0):
        cap = protocol.FRAME_CAP
    return protocol.max_body(cap)


def pack_batches(items: List[Any], budget: int, limit: int,
                 key: str = 'tasks') -> Iterator[Tuple[List[Any], bytes]]:
    """Split *items* into msgpack bodies ``{key: [...]}`` bounded in bytes.

    Each item is packed once; a body is assembled from those bytes (map
    header + *key* + array header + parts), so the size is exact and the
    bytes equal ``msgpack.packb({key: batch})``.  A body holds items up to
    *budget* bytes; an item larger than that travels alone.  Every item is
    checked here, before the first body exists, so the caller sends nothing
    when an item cannot fit *limit* (the transport's hard body limit):
    :class:`PayloadTooLarge` names all such items instead.

    Returns an iterator of ``(batch_items, body_bytes)``.  Memory: the
    packed parts of *all* items are kept until the last batch is yielded
    (the up-front size check needs them, and each body is joined from
    them), alongside the caller's *items*; the bodies themselves are built
    lazily, so at most one assembled body exists at a time on top of that.
    """
    head  = msgpack.packb({key: []}, use_bin_type=True)[:-1]   # minus [] hdr
    parts = [msgpack.packb(it, use_bin_type=True) for it in items]
    over  = 5 + len(head)                                      # + array hdr

    bad = [idx for idx, part in enumerate(parts) if over + len(part) > limit]
    if bad:
        uids = [str(items[i]['uid']) for i in bad
                if isinstance(items[i], dict) and 'uid' in items[i]]
        raise PayloadTooLarge(
            f"HTTP 413 — {len(bad)} item(s) (first: item {bad[0]}, "
            f"{len(parts[bad[0]])} bytes) over the {limit} byte request "
            f"body limit", bad, uids)

    return _iter_batches(items, parts, head, over, budget)


def _iter_batches(items, parts, head, over, budget
                  ) -> Iterator[Tuple[List[Any], bytes]]:
    start = 0
    size  = over
    for idx, part in enumerate(parts):
        if idx > start and size + len(part) > budget:
            yield _assemble(items, parts, head, start, idx)
            start = idx
            size  = over
        size += len(part)
    if start < len(parts):
        yield _assemble(items, parts, head, start, len(parts))


def _assemble(items, parts, head, start, end) -> Tuple[List[Any], bytes]:
    hdr = msgpack.Packer(use_bin_type=True).pack_array_header(end - start)
    return items[start:end], head + hdr + b''.join(parts[start:end])


def _pack_request(method: str, url: str, *, json=None, content=None, data=None,
                  params=None, headers=None):
    """Turn httpx-shaped kwargs into ``(path, body_bytes, headers)``.

    The one place that maps a helper's ``self._http.get/post(...)`` call shape
    onto the ``(method, path, body, headers)`` a broker/runtime transport
    speaks.  Shared by :class:`_RuntimeHTTP` (WebSocket, user threads) and the
    dispatcher's sync caller transport so both agree on encoding.
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

    The body is coerced to ``bytes``.  One helper for the places that
    unpacked this shape by hand (the gateway HTTP proxy and the dispatcher's
    caller transport).
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
        # Case-insensitive like ``httpx.Response.headers`` so helpers doing
        # ``resp.headers.get('Content-Type')`` match a lower-cased wire header.
        self.headers     = httpx.Headers(headers or {})
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

    @property
    def max_body(self) -> int:
        """Largest request body the runtime's frame cap carries."""
        return max_body_of(self._runtime)

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
