"""``is_busy`` of the sessions that run long background work: a busy
session is not idle-expired (see ``Plugin._cleanup_expired_sessions``)."""

import asyncio
from types import SimpleNamespace

import pytest

from radical.orbit.plugin_psij          import PSIJSession
from radical.orbit.plugin_globus        import GlobusSession
from radical.orbit.plugin_xgfabric      import XGFabricSession
from radical.orbit.plugin_task_dispatcher import (TaskDispatcherSession,
                                                  _child_session_gone)


def test_psij_busy_while_a_job_is_not_terminal():
    s = SimpleNamespace(_jobs={'j1': object(), 'j2': object()},
                        _job_states={'j1': 'COMPLETED', 'j2': 'ACTIVE'})
    assert PSIJSession.is_busy(s)
    s._job_states['j2'] = 'FAILED'
    assert not PSIJSession.is_busy(s)
    s._jobs['j3'] = object()                 # submitted, no state yet
    assert PSIJSession.is_busy(s)


def test_globus_busy_while_a_transfer_is_not_reported_terminal():
    s = SimpleNamespace(_tasks={'t1': {'notified': 'SUCCEEDED'},
                                't2': {'status': 'ACTIVE'}})
    assert GlobusSession.is_busy(s)
    s._tasks['t2']['notified'] = 'FAILED'
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


def test_dispatcher_busy_while_a_pool_has_live_pilots_or_tasks():
    def rec(terminal):
        return SimpleNamespace(is_terminal=lambda: terminal)
    pool   = SimpleNamespace(pilots={'p': rec(True)}, tasks={'t': rec(False)})
    plugin = SimpleNamespace(_pool_states={'sid': {'cpu': pool}})
    s      = SimpleNamespace(_plugin=plugin, _sid='sid')
    assert TaskDispatcherSession.is_busy(s)
    pool.tasks['t'] = rec(True)
    assert not TaskDispatcherSession.is_busy(s)
    pool.pilots['p2'] = rec(False)
    assert TaskDispatcherSession.is_busy(s)
    assert not TaskDispatcherSession.is_busy(
        SimpleNamespace(_plugin=plugin, _sid='other'))


def _err(status, text):
    e = RuntimeError(f'HTTP {status} - {text}')
    e.status_code = status
    return e


def test_child_session_gone_is_narrow():
    assert _child_session_gone(_err(410, 'session expired: s1'))
    assert _child_session_gone(_err(404, 'unknown session id: s1'))
    assert _child_session_gone(_err(404, 'Unknown session s1'))
    assert not _child_session_gone(_err(404, 'task t1 not found'))
    assert not _child_session_gone(_err(404, 'Job j1 not found'))
    assert not _child_session_gone(_err(500, 'boom'))
    assert not _child_session_gone(RuntimeError('no status'))


@pytest.mark.asyncio
async def test_dispatcher_child_call_evicts_and_retries_a_gone_session():
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher

    class Client:
        def __init__(self, fail):
            self.fail, self.calls = fail, []

        def submit_tasks(self, tasks):
            self.calls.append(tasks)
            if self.fail:
                raise self.fail
            return ['ok']

    old   = Client(_err(410, 'session expired: s1'))
    fresh = Client(None)
    key   = ('child', 'rhapsody', 'dragon_v3')
    old._orbit_cache_key = fresh._orbit_cache_key = key
    seen  = {}

    async def _get(dst, backend):
        seen['args'] = (dst, backend)
        d._child_clients[key] = fresh
        return fresh

    d = SimpleNamespace(_child_clients={key: old}, instance_name='td',
                        _get_rhapsody_client=_get)
    res = await PluginTaskDispatcher._call_child(d, old, 'submit_tasks',
                                                 ['t1'], retry=True)
    assert res == ['ok']
    assert seen['args'] == ('child', 'dragon_v3')
    assert d._child_clients[key] is fresh and fresh.calls == [['t1']]

    # a task-level 404 is not a gone session: no eviction, error propagates
    task404 = Client(_err(404, 'task t9 not found'))
    task404._orbit_cache_key = key
    d._child_clients = {key: task404}
    with pytest.raises(RuntimeError, match='t9'):
        await PluginTaskDispatcher._call_child(d, task404, 'submit_tasks',
                                               ['t9'], retry=True)
    assert d._child_clients[key] is task404


def test_rhapsody_session_busy_while_a_task_is_not_terminal():
    from radical.orbit.plugin_rhapsody import RhapsodySession
    s = SimpleNamespace(_tasks={'a': {'state': 'DONE'},
                                'b': {'state': 'running'}},
                        _live={'a', 'b'},
                        _get_attr=lambda t, k, d=None: t.get(k, d))
    assert RhapsodySession.is_busy(s)
    assert s._live == {'b'}                  # terminal ones pruned
    s._tasks['b']['state'] = 'completed'
    assert not RhapsodySession.is_busy(s)
    assert s._live == set()


@pytest.mark.asyncio
async def test_dispatcher_child_call_does_not_retry_a_closing_session():
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher

    class Client:
        _orbit_cache_key = ('child', 'rhapsody', None)

        def submit_tasks(self, tasks):
            raise _err(410, 'session is closing')

    async def _get(dst, backend):            # must not be called
        raise AssertionError('retried')

    c = Client()
    d = SimpleNamespace(_child_clients={Client._orbit_cache_key: c},
                        instance_name='td', _get_rhapsody_client=_get)
    with pytest.raises(RuntimeError, match='closing'):
        await PluginTaskDispatcher._call_child(d, c, 'submit_tasks', ['t'],
                                               retry=True)
    assert d._child_clients == {}            # evicted, not retried


@pytest.mark.asyncio
async def test_dispatcher_second_stale_caller_retries_on_the_fresh_session():
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher

    key = ('child', 'rhapsody', None)

    class Client:
        def __init__(self, fail):
            self.fail, self._orbit_cache_key = fail, key

        def submit_tasks(self, tasks):
            if self.fail:
                raise _err(410, 'session expired: s1')
            return tasks

    stale, fresh = Client(True), Client(False)

    async def _get(dst, backend):
        return d._child_clients.setdefault(key, fresh)

    d = SimpleNamespace(_child_clients={key: fresh},   # A already replaced it
                        instance_name='td', _get_rhapsody_client=_get)
    assert await PluginTaskDispatcher._call_child(
        d, stale, 'submit_tasks', ['t'], retry=True) == ['t']
    assert d._child_clients[key] is fresh             # not evicted by B
