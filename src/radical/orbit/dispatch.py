"""Direct-dispatch machinery shared by the endpoint hosts.

Both the WebSocket endpoint (``service.py`` / ``runtime.py``) and the
broker-hosted plugin host (``broker_plugin_host.py``) bypass the ASGI/FastAPI
stack on the hot request path: they match a forwarded request against a plain
route table and feed the handler a lightweight request stand-in.  The two
pieces of that fast path live here so every host imports the *same* code:

* :class:`RequestShim`  — a lazy, encoding-agnostic ``starlette.Request``
  stand-in exposing exactly the surface plugin handlers use
  (``path_params``, ``query_params``, ``await .body()``, ``await .json()``).
* :func:`match_route`   — match ``method`` + ``path`` against a compiled
  direct-dispatch route table (the ``(method, pattern, param_names, handler)``
  tuples ``Plugin._register_direct`` appends to ``app.state.direct_routes``).

This is a pure extraction — the behaviour is exactly what ``service.py`` and
``broker_plugin_host.py`` carried inline before, and the old test suite proves
the move changed nothing.
"""

import json

from typing import Callable, List, Optional, Tuple

import msgpack


# ---------------------------------------------------------------------------
# RequestShim — lightweight stand-in for starlette.requests.Request
# ---------------------------------------------------------------------------

class RequestShim:
    """Lightweight adapter for starlette ``Request``.

    Provides the interfaces that every plugin handler uses: ``path_params``,
    ``query_params``, ``headers``, and ``await .json()`` / ``await .body()``.
    Encoding-agnostic: stores raw bytes, decodes lazily based on content_type.

    ``headers`` is a plain dict.  The serving runtime injects the trusted
    ``x-orbit-src`` owner header here (see ``runtime._dispatch_served``); the
    old-stack ``service.py`` and the broker gateway pass no such header, so
    ``headers.get('x-orbit-src')`` is ``None`` there (owner-less,
    capability-style within the token trust domain).
    """

    def __init__(self, path_params : dict,
                       query_params: dict,
                       body_bytes  : bytes,
                       content_type: str  = 'application/json',
                       headers     : dict = None):
        self.path_params  = path_params
        self.query_params = query_params
        self.content_type = content_type
        self.headers      = headers if headers is not None else {}
        self._body        = body_bytes
        self._decoded     = None

    async def body(self) -> bytes:
        """Raw body bytes (matches ``Request.body()``)."""
        return self._body

    async def json(self) -> dict:
        """Parse body into a Python dict (matches ``Request.json()``).

        Content-type-aware: JSON or msgpack based on Content-Type header.
        """
        if self._decoded is not None:
            return self._decoded

        ct = self.content_type or 'application/json'
        if 'msgpack' in ct:
            self._decoded = msgpack.unpackb(self._body, raw=False)
        else:
            self._decoded = json.loads(self._body) if self._body else {}
        return self._decoded


# ---------------------------------------------------------------------------
# route matching
# ---------------------------------------------------------------------------

def match_route(direct_routes: List[tuple], method: str, path: str
                ) -> Tuple[Optional[Callable], Optional[dict]]:
    """Match *method* + *path* against the direct-dispatch route table.

    *direct_routes* is the shared list of
    ``(method, pattern, param_names, handler)`` tuples that
    ``Plugin._register_direct`` populates on ``app.state.direct_routes``.

    Returns ``(handler, path_params)`` or ``(None, None)``.
    """
    for rt_method, pattern, param_names, handler in direct_routes:
        if rt_method == method:
            m = pattern.match(path)
            if m:
                return handler, dict(zip(param_names, m.groups()))
    return None, None
