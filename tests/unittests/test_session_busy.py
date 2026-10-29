"""``is_busy`` of the sessions that run long background work: a busy
session is not idle-expired (see ``Plugin._cleanup_expired_sessions``)."""

import asyncio
from types import SimpleNamespace

import pytest

from radical.orbit.plugin_psij          import PSIJSession
from radical.orbit.plugin_globus        import GlobusSession
from radical.orbit.plugin_xgfabric      import XGFabricSession


def test_psij_busy_while_a_job_is_not_terminal():
    s = SimpleNamespace(_jobs={'j1': object(), 'j2': object()},
                        _job_states={'j1': 'COMPLETED', 'j2': 'ACTIVE'})
    assert PSIJSession.is_busy(s)
    s._job_states['j2'] = 'FAILED'
    assert not PSIJSession.is_busy(s)
    s._jobs['j3'] = object()                 # submitted, no state yet
    assert PSIJSession.is_busy(s)


def test_globus_busy_while_a_transfer_is_not_reported_terminal():
    import time
    s = SimpleNamespace(_tasks={'t1': {'notified': 'SUCCEEDED'},
                                't2': {'status': 'ACTIVE'}},
                        _status_ok=time.time())
    assert GlobusSession.is_busy(s)
    s._status_ok = time.time() - 3600       # status unfetchable for long
    assert not GlobusSession.is_busy(s)
    s._status_ok = time.time()
    s._tasks['t2']['notified'] = 'INACTIVE'  # expired credential: no pin
    assert not GlobusSession.is_busy(s)


@pytest.mark.asyncio
async def test_xgfabric_busy_while_its_workflow_runs():
    s = SimpleNamespace(_workflow_task=None)
    assert not XGFabricSession.is_busy(s)
    gate = asyncio.Event()
    s._workflow_task = asyncio.ensure_future(gate.wait())
    assert XGFabricSession.is_busy(s)
    gate.set()
    await s._workflow_task
    assert not XGFabricSession.is_busy(s)


def test_rhapsody_session_busy_while_a_task_is_not_terminal():
    from radical.orbit.plugin_rhapsody import RhapsodySession
    s = SimpleNamespace(_tasks={'a': {'state': 'DONE'},
                                'b': {'state': 'running'}},
                        _get_attr=lambda t, k, d=None: t.get(k, d))
    assert RhapsodySession.is_busy(s)
    s._tasks['b']['state'] = 'completed'
    assert not RhapsodySession.is_busy(s)


def test_client_touch_uses_the_touch_route():
    from unittest.mock import MagicMock
    from radical.orbit.client import PluginClient
    http = MagicMock()
    http.post.return_value = MagicMock(status_code=200, is_error=False)
    c = PluginClient(http, '/psij')
    with pytest.raises(RuntimeError, match='No active session'):
        c.touch()
    c._sid = 'sid.1'
    c.touch()
    url = http.post.call_args.args[0]
    assert url.endswith('/psij/touch/sid.1')
    assert 'json' not in http.post.call_args.kwargs


def test_psij_terminal_state_is_final():
    from radical.orbit.plugin_psij import PSIJSession
    sent = []
    s = SimpleNamespace(_jobs={'j': SimpleNamespace(status=None)},
                        _job_states={'j': 'FAILED'},
                        notify=lambda *a: sent.append(a))
    PSIJSession._notify_state(s, 'j', SimpleNamespace(state='NEW'))
    assert sent == [] and s._job_states['j'] == 'FAILED'


def test_dispatcher_busy_while_a_task_is_queued_or_running():
    from radical.orbit.plugin_task_dispatcher import TaskDispatcherSession

    def task(terminal):
        return SimpleNamespace(is_terminal=lambda: terminal)
    pool   = SimpleNamespace(tasks={'a': task(True), 'b': task(False)},
                             pilots={})
    plugin = SimpleNamespace(_pool_states={'sid': {'cpu': pool}})
    s      = SimpleNamespace(_plugin=plugin, _sid='sid')
    assert TaskDispatcherSession.is_busy(s)          # queued or running
    pool.tasks['b'] = task(True)
    assert not TaskDispatcherSession.is_busy(s)      # idle pilots don't count
    assert not TaskDispatcherSession.is_busy(
        SimpleNamespace(_plugin=plugin, _sid='other'))
