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
                        _job_states={'j': 'FAILED'}, _cancelled_jobs=set(),
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


def test_rhapsody_idle_verdict_is_reused_until_a_task_is_added():
    from radical.orbit.plugin_rhapsody import RhapsodySession
    calls = []

    def get_attr(t, k, d=None):
        calls.append(1)
        return t.get(k, d)
    s = SimpleNamespace(_tasks={'a': {'state': 'DONE'}}, _get_attr=get_attr)
    assert not RhapsodySession.is_busy(s)
    n = len(calls)
    assert not RhapsodySession.is_busy(s)       # no rescan
    assert len(calls) == n
    s._tasks['b'] = {'state': 'RUNNING'}        # new task: rescan
    assert RhapsodySession.is_busy(s)


def test_psij_requested_cancel_still_remaps_a_terminal_state():
    from radical.orbit.plugin_psij import PSIJSession
    sent = []
    s = SimpleNamespace(_jobs={'j': SimpleNamespace(status=None)},
                        _job_states={'j': 'FAILED'}, _cancelled_jobs={'j'},
                        notify=lambda topic, data: sent.append(data))
    s._effective_state = lambda j, st: PSIJSession._effective_state(s, j, st)
    PSIJSession._notify_state(s, 'j', SimpleNamespace(state='FAILED',
                                                      exit_code=1))
    assert s._job_states['j'] == 'CANCELED'
    assert sent and sent[-1]['state'] == 'CANCELED'


def test_dispatcher_idle_verdict_is_reused_until_a_task_is_added():
    from radical.orbit.plugin_task_dispatcher import TaskDispatcherSession
    scans = []

    def task(terminal):
        def is_terminal():
            scans.append(1)
            return terminal
        return SimpleNamespace(is_terminal=is_terminal)
    pool = SimpleNamespace(tasks={'a': task(True)}, pilots={})
    s    = SimpleNamespace(_plugin=SimpleNamespace(
                               _pool_states={'sid': {'cpu': pool}}),
                           _sid='sid')
    assert not TaskDispatcherSession.is_busy(s)
    n = len(scans)
    assert not TaskDispatcherSession.is_busy(s)  # cached
    assert len(scans) == n
    s._idle_at = -1                              # _task_added on replace
    pool.tasks['a'] = task(False)
    assert TaskDispatcherSession.is_busy(s)


def test_rhapsody_old_live_task_is_found_without_a_rescan():
    from radical.orbit.plugin_rhapsody import RhapsodySession
    calls = []

    def get_attr(t, k, d=None):
        calls.append(1)
        return t.get(k, d)
    tasks = {'old': {'state': 'RUNNING'}}
    tasks.update({f't{i}': {'state': 'DONE'} for i in range(100)})
    s = SimpleNamespace(_tasks=tasks, _get_attr=get_attr)
    assert RhapsodySession.is_busy(s)          # one scan finds 'old'
    calls.clear()
    assert RhapsodySession.is_busy(s)          # hint: a single check
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_psij_cancel_of_a_finished_job_keeps_its_state():
    from radical.orbit.plugin_psij import PSIJSession
    job = SimpleNamespace(cancel=lambda: None)
    s = SimpleNamespace(_jobs={'j': job}, _job_states={'j': 'COMPLETED'},
                        _cancelled_jobs=set())
    await PSIJSession.cancel_job(s, 'j')
    assert 'j' not in s._cancelled_jobs
