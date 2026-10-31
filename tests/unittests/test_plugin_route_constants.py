'''Drift guard: single-sourced route constants resolve against the route table.

``plugin_psij.py`` / ``plugin_rhapsody.py`` lift every route path a client
helper (or the broker-hosted task dispatcher) formats into a module-level
``ROUTE_*`` constant, used by BOTH the ``add_route_*`` registration and the
helper URL formatting.  This test pins that single-sourcing: for each constant,
the helper-formatted path (with real-looking sid/job/uid values) is resolved
against a locally constructed plugin's ``app.state.direct_routes`` table via
``radical.orbit.dispatch.match_route`` — the same matcher the broker/endpoint
runtime uses to serve a forwarded request.  A rename that touched only one side
would fail here.

Non-disposable: this replaces the M7 proxy-window contract test
(``test_dispatcher_route_contract.py``, deleted with the proxies).  The
constants are what make a route rename a one-line, drift-proof change.
'''

import pytest
from fastapi import FastAPI

from radical.orbit.dispatch        import match_route
from radical.orbit.plugin_psij     import PluginPSIJ
from radical.orbit.plugin_rhapsody import PluginRhapsody
from radical.orbit import plugin_psij     as P
from radical.orbit import plugin_rhapsody as R


# For each ROUTE_* constant: (method, formatted-path) as a helper sends it,
# namespace-relative (``/{instance_name}/...``) — the shape add_route_*
# registers and the caller-backed clients format.
PSIJ_ROUTES = [
    ('POST', '/psij/' + P.ROUTE_SUBMIT.format(sid='sid.abc')),
    ('POST', '/psij/' + P.ROUTE_SUBMIT_TUNNELED.format(sid='sid.abc')),
    ('GET',  '/psij/' + P.ROUTE_TUNNEL_STATUS.format(endpoint_name='ep0')),
    ('GET',  '/psij/' + P.ROUTE_STATUS.format(sid='sid.abc', job_id='job.42')),
    ('GET',  '/psij/' + P.ROUTE_LIST_JOBS.format(sid='sid.abc')),
    ('POST', '/psij/' + P.ROUTE_CANCEL.format(sid='sid.abc', job_id='job.42')),
]

RHAPSODY_ROUTES = [
    ('POST', '/rhapsody/' + R.ROUTE_SUBMIT.format(sid='sid.def')),
    ('POST', '/rhapsody/' + R.ROUTE_WAIT.format(sid='sid.def')),
    ('GET',  '/rhapsody/' + R.ROUTE_LIST_TASKS.format(sid='sid.def')),
    ('GET',  '/rhapsody/' + R.ROUTE_TASK.format(sid='sid.def', uid='t.001')),
    ('POST', '/rhapsody/' + R.ROUTE_CANCEL.format(sid='sid.def', uid='t.001')),
    ('POST', '/rhapsody/' + R.ROUTE_CANCEL_ALL.format(sid='sid.def')),
]


@pytest.fixture(scope='module')
def psij_direct_routes():
    app = FastAPI()
    PluginPSIJ(app)
    return app.state.direct_routes


@pytest.fixture(scope='module')
def rhapsody_direct_routes():
    app = FastAPI()
    PluginRhapsody(app)
    return app.state.direct_routes


@pytest.mark.parametrize('method, path', PSIJ_ROUTES)
def test_psij_route_constant_resolves(psij_direct_routes, method, path):
    handler, params = match_route(psij_direct_routes, method, path)
    assert handler is not None, (
        f'{method} {path!r} does not resolve against plugin_psij.py routes '
        f'— a ROUTE_* constant and its add_route_* registration have drifted')
    assert params is not None


@pytest.mark.parametrize('method, path', RHAPSODY_ROUTES)
def test_rhapsody_route_constant_resolves(rhapsody_direct_routes, method, path):
    handler, params = match_route(rhapsody_direct_routes, method, path)
    assert handler is not None, (
        f'{method} {path!r} does not resolve against plugin_rhapsody.py routes '
        f'— a ROUTE_* constant and its add_route_* registration have drifted')
    assert params is not None
