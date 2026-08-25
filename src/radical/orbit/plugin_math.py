"""Tutorial plugin: a four-function calculator.

This module is the worked example for the Plugin Writer's Tutorial
(``docs/tutorial_plugin.md``).  It is intentionally small: the
arithmetic is trivial so that every line of ORBIT machinery — session,
routes, error mapping, notifications, client helper — stays in focus.

The plugin is not part of any default plugin set; load it explicitly::

    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,math
"""

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'


import logging

from fastapi import FastAPI
from starlette.requests import Request

from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient
from .errors import http_exception

log = logging.getLogger("radical.orbit")


# ------------------------------------------------------------------------
#
# --8<-- [start:MathSession]
class MathSession(PluginSession):
    """
    Math session (service side).

    Holds the per-session operation history — the tutorial's stand-in for
    real per-client state (jobs, tasks, backend connections).
    """

    def __init__(self, sid: str):
        super().__init__(sid)
        self._history = []      # [{"op", "a", "b", "result"}, ...]

    async def compute(self, op: str, a: float, b: float) -> dict:
        """
        Apply one arithmetic operation and record it.

        Args:
            op: One of ``'add'`` / ``'sub'`` / ``'mul'`` / ``'div'``.
            a:  Left operand.
            b:  Right operand.

        Returns:
            {"op": str, "a": float, "b": float, "result": float}
        """
        self._check_active()

        if   op == 'add': result = a + b
        elif op == 'sub': result = a - b
        elif op == 'mul': result = a * b
        elif op == 'div': result = a / b
        else:
            raise ValueError(f"unknown operation: {op}")

        entry = {"op": op, "a": a, "b": b, "result": result}
        self._history.append(entry)
        log.info("[math] %s(%s, %s) = %s", op, a, b, result)

        # Real-time notification to subscribed consumers (Python callbacks
        # over the WebSocket, browsers over the gateway's SSE `/events`).
        self.notify("result", dict(entry, count=len(self._history)))

        return entry

    async def history(self) -> dict:
        """
        Return the operations recorded in this session.

        Returns:
            {"count": int, "ops": [{"op", "a", "b", "result"}, ...]}
        """
        self._check_active()
        return {"count": len(self._history), "ops": list(self._history)}

    async def close(self) -> dict:
        """Release per-session state."""
        self._history = []
        return await super().close()
# --8<-- [end:MathSession]


# ------------------------------------------------------------------------
#
# --8<-- [start:MathClient]
class MathClient(PluginClient):
    """
    Client-side interface for the Math plugin.

    Obtained via ``runtime.get_plugin(endpoint_name, 'math')``, which also
    registers a session.
    """

    def add(self, a: float, b: float) -> float:
        """Return ``a + b``, computed on the serving participant."""
        return self._compute('add', a, b)

    def sub(self, a: float, b: float) -> float:
        """Return ``a - b``, computed on the serving participant."""
        return self._compute('sub', a, b)

    def mul(self, a: float, b: float) -> float:
        """Return ``a * b``, computed on the serving participant."""
        return self._compute('mul', a, b)

    def div(self, a: float, b: float) -> float:
        """Return ``a / b``, computed on the serving participant.

        Raises:
            RuntimeError: On division by zero (HTTP 400 from the plugin).
        """
        return self._compute('div', a, b)

    def history(self) -> dict:
        """Return ``{"count": int, "ops": [...]}`` for this session."""
        self._require_session()
        resp = self._request('GET', self._url(f'history/{self.sid}'))
        self._raise(resp, 'history')
        return resp.json()

    def _compute(self, op: str, a: float, b: float) -> float:
        """Single seam for the four operation calls."""
        self._require_session()
        resp = self._request('POST', self._url(f'{op}/{self.sid}'),
                             json={"a": a, "b": b})
        self._raise(resp, f'{op}({a}, {b})')
        return resp.json()['result']
# --8<-- [end:MathClient]


# ------------------------------------------------------------------------
#
# --8<-- [start:PluginMath]
class PluginMath(Plugin):
    """
    Math plugin for ORBIT — the Plugin Writer's Tutorial example.

    Serves the four basic arithmetic operations plus a per-session
    operation history.
    """

    plugin_name   = 'math'
    session_class = MathSession
    client_class  = MathClient
    version       = '0.1.0'

    ui_config = {
        "icon"       : "🧮",
        "title"      : "Math",
        "description": "Four-function calculator (tutorial plugin)."
    }

    def __init__(self, app: FastAPI, instance_name: str = 'math'):
        """
        Initialize the Math plugin.
        """
        super().__init__(app, instance_name)

        self.add_route_post('add/{sid}',     self.op_add)
        self.add_route_post('sub/{sid}',     self.op_sub)
        self.add_route_post('mul/{sid}',     self.op_mul)
        self.add_route_post('div/{sid}',     self.op_div)
        self.add_route_get ('history/{sid}', self.history)

    async def op_add(self, request: Request) -> dict:
        """Route handler for ``POST /math/add/{sid}``."""
        return await self._compute(request, 'add')

    async def op_sub(self, request: Request) -> dict:
        """Route handler for ``POST /math/sub/{sid}``."""
        return await self._compute(request, 'sub')

    async def op_mul(self, request: Request) -> dict:
        """Route handler for ``POST /math/mul/{sid}``."""
        return await self._compute(request, 'mul')

    async def op_div(self, request: Request) -> dict:
        """Route handler for ``POST /math/div/{sid}``."""
        return await self._compute(request, 'div')

    async def history(self, request: Request) -> dict:
        """Route handler for ``GET /math/history/{sid}``."""
        sid = request.path_params['sid']
        return await self._forward(sid, MathSession.history)

    async def _compute(self, request: Request, op: str) -> dict:
        """Validate the request body, then forward to the session.

        Request body:
            {"a": number, "b": number}

        Returns:
            {"op": str, "a": float, "b": float, "result": float}
        """
        sid  = request.path_params['sid']
        body = await request.json()

        try:
            a = float(body['a'])
            b = float(body['b'])
        except (KeyError, TypeError, ValueError) as e:
            raise http_exception(
                ValueError("body must carry numeric fields 'a' and 'b'")) \
                from e

        # Domain errors get their typed HTTP status at this boundary
        # (errors.EXC_STATUS maps ValueError -> 400); an exception escaping
        # the session method would surface as a generic 500 instead.
        if op == 'div' and b == 0.0:
            raise http_exception(ValueError("division by zero"))

        return await self._forward(sid, MathSession.compute, op=op, a=a, b=b)
# --8<-- [end:PluginMath]
