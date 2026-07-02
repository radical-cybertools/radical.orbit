'''Contract test: dispatcher-side proxy paths vs the real plugin route tables.

``plugin_task_dispatcher.py``'s ``_PsijProxy`` / ``_RhapsodyProxy`` — plus the
plugin's own ``_child_session`` / ``_await_rhapsody_ready`` helpers — hand-
format route paths that duplicate the route templates ``plugin_psij.py`` /
``plugin_rhapsody.py`` register via ``add_route_get`` / ``add_route_post``.  A
silent rename on either side 404s at runtime with no compile-time signal.

These tests pin that duplication: every path the dispatcher formats is
resolved against a locally constructed ``PluginPSIJ`` / ``PluginRhapsody``'s
``app.state.direct_routes`` table, through ``radical.orbit.dispatch.
match_route`` — the same matcher the broker/endpoint runtime uses to serve a
forwarded request.  Paths are matched exactly as the dispatcher sends them
over the broker caller: namespace-relative (``/{instance_name}/...``), which
is what ``add_route_get``/``add_route_post`` register (``self._namespace +
'/' + path``) since ``_PsijProxy``/``_RhapsodyProxy`` hard-code ``/psij/...``
and ``/rhapsody/...`` — the plugins' default ``instance_name``.

These tests guard the proxy window only and are deleted together with
``_PsijProxy``/``_RhapsodyProxy`` when the async-helper convergence lands
(the real ``PluginClient`` helpers grow a caller-backed async transport that
runs natively on the host loop, replacing the hand-formatted proxy paths).
'''

import pytest
from fastapi import FastAPI

from radical.orbit.dispatch         import match_route
from radical.orbit.plugin_psij      import PluginPSIJ
from radical.orbit.plugin_rhapsody  import PluginRhapsody


# ---------------------------------------------------------------------------
# Route table: (method, path) exactly as formatted by the dispatcher-side
# proxies / helpers in plugin_task_dispatcher.py, with real-looking sid/job/
# uid values.
# ---------------------------------------------------------------------------

PSIJ_ROUTES = [
    # PluginTaskDispatcher._child_session(dst, 'psij') — generic register path
    ('POST', '/psij/register_session'),
    # _PsijProxy.submit_tunneled
    ('POST', '/psij/submit_tunneled/sid.abc123'),
    # _PsijProxy.cancel_job
    ('POST', '/psij/cancel/sid.abc123/job.0042'),
    # _PsijProxy.get_job_status
    ('GET',  '/psij/status/sid.abc123/job.0042'),
]

RHAPSODY_ROUTES = [
    # PluginTaskDispatcher._child_session(dst, 'rhapsody') — generic register
    ('POST', '/rhapsody/register_session'),
    # _RhapsodyProxy.submit_tasks (msgpack body, same path template)
    ('POST', '/rhapsody/submit/sid.def456'),
    # _RhapsodyProxy.get_task
    ('GET',  '/rhapsody/task/sid.def456/task.uid.001'),
    # _RhapsodyProxy.cancel_task
    ('POST', '/rhapsody/cancel/sid.def456/task.uid.001'),
    # PluginTaskDispatcher._await_rhapsody_ready readiness poll
    ('GET',  '/rhapsody/list_tasks/sid.def456'),
]


# ---------------------------------------------------------------------------
# Fixtures: real plugins, constructed the way the existing unit tests do
# (test_plugin_psij.py / test_plugin_rhapsody.py) — no server, no broker.
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('method, path', PSIJ_ROUTES)
def test_psij_proxy_path_resolves(psij_direct_routes, method, path):
    handler, params = match_route(psij_direct_routes, method, path)
    assert handler is not None, (
        f'{method} {path!r} does not resolve against plugin_psij.py routes '
        f'— dispatcher proxy path and plugin route table have drifted')
    assert params is not None


@pytest.mark.parametrize('method, path', RHAPSODY_ROUTES)
def test_rhapsody_proxy_path_resolves(rhapsody_direct_routes, method, path):
    handler, params = match_route(rhapsody_direct_routes, method, path)
    assert handler is not None, (
        f'{method} {path!r} does not resolve against plugin_rhapsody.py '
        f'routes — dispatcher proxy path and plugin route table have drifted')
    assert params is not None
