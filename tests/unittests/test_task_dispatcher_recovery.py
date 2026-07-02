"""Recovery / lifecycle tests for plugin_task_dispatcher (broker-hosted).

- C2  pilot child-endpoint liveness → DONE/FAILED on ``lost``, capacity
      reclaimed, tasks re-enqueued; suspect blip does not tear down; a child
      never seen ``present`` (post-restart) is never demoted.
- C4  restart correlation: pools + ``_uid_to_task`` rebuilt from the sid-scoped
      durable store; endpoint-mode ledger replayed (terminal entries filtered).
- C5  a late terminal event for a re-enqueued task's stale uid is ignored.
- C6  endpoint-mode ledger self-prunes via snapshot.
- H2  guard against the loop-state-fragile ``run_until_complete`` antipattern.
"""

import asyncio
import time
from pathlib import Path

from fastapi import FastAPI

from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher
from radical.orbit.task_dispatcher_config import PoolConfig, PilotSize
from radical.orbit.task_dispatcher_state import (
    PilotRecord, TaskRecord, EndpointModeRecord,
    PILOT_ACTIVE, PILOT_DONE, PILOT_FAILED,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE,
)


_SID = 'sessA'


def _pool_cfg(name='cpu'):
    return PoolConfig(
        name         = name,
        endpoint_name    = 'endpoint0',
        queue        = 'batch',
        account      = 'proj',
        pilot_sizes  = {'s': PilotSize(nodes=1, cpus_per_node=4,
                                       rhapsody_backend='concurrent')},
        default_size = 's',
        max_pilots   = 4,
        strategy     = 'conservative',
        strategy_config = {'min_dwell_sec': 0.0},
    )


def _make_plugin(tmp_path: Path, *, with_pool=True):
    app = FastAPI()
    app.state.endpoint_name  = 'endpoint0'
    app.state.bridge_url     = 'https://localhost:9999'
    app.state.broker_caller  = None
    app.state.broker_tap     = None
    plugin = PluginTaskDispatcher(
        app, state_root=tmp_path / 'state', scratch_root=tmp_path / 'scratch')
    if with_pool:
        plugin._materialise_pool(_SID, _pool_cfg())
    return plugin


def _pool(plugin, name='cpu'):
    return plugin._pool_states[_SID][name]


def _active_pilot(plugin, *, pid='p.1', child='endpoint0_p.1',
                  walltime_deadline=0.0):
    ps = _pool(plugin)
    pilot = PilotRecord(
        pid=pid, pool='cpu', owning_sid=_SID, size_key='s',
        rhapsody_backend='concurrent', state=PILOT_ACTIVE,
        submitted_at=100.0, active_at=110.0, capacity=4, in_flight=1,
        child_endpoint_name=child, walltime_deadline=walltime_deadline)
    ps.pilots[pid] = pilot
    return ps, pilot


def _topo(plugin, name, liveness):
    asyncio.run(plugin.on_topology_change(
        {name: {'role': 'endpoint',
                'plugins': {'rhapsody': {'namespace': '/rhapsody'}},
                'liveness': liveness}}))


# ---------------------------------------------------------------------------
# C2 — pilot child liveness
# ---------------------------------------------------------------------------

class TestPhantomPilotRecovery:

    def test_lost_after_walltime_marks_done(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps, pilot = _active_pilot(plugin, walltime_deadline=time.time() - 1)
        ps.tasks['t.1'] = TaskRecord(task_id='t.1', pool='cpu', owning_sid=_SID,
                                     cmd=['/bin/echo'], cwd=str(tmp_path),
                                     state=TASK_RUNNING, pilot_id='p.1')
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'lost')
        assert pilot.state == PILOT_DONE
        assert ps.tasks['t.1'].state == TASK_QUEUED
        assert ps.tasks['t.1'].pilot_id is None
        assert ps._pilots_snapshot() == []

    def test_lost_before_walltime_marks_failed(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps, pilot = _active_pilot(plugin, walltime_deadline=time.time() + 1000)
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'lost')
        assert pilot.state == PILOT_FAILED
        assert ps._pilots_snapshot() == []

    def test_suspect_blip_does_not_demote(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        _, pilot = _active_pilot(plugin, walltime_deadline=time.time() + 1000)
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'suspect')
        assert pilot.state == PILOT_ACTIVE
        assert pilot.accepting_new_tasks is False

    def test_unseen_child_never_demoted(self, tmp_path):
        """Restart race: an ACTIVE pilot whose child never appears 'present'
        (never synthesized 'lost') survives — no `_seen` heuristic needed."""
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        _, pilot = _active_pilot(plugin, walltime_deadline=time.time() + 1000)
        _topo(plugin, 'some_other_endpoint', 'present')
        assert pilot.state == PILOT_ACTIVE


# ---------------------------------------------------------------------------
# C4 — restart correlation
# ---------------------------------------------------------------------------

class TestRestartCorrelation:

    def test_pool_and_uid_map_rebuilt(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        ps = _pool(plugin)
        rec = TaskRecord(task_id='t.x', pool='cpu', owning_sid=_SID,
                         cmd=['/bin/echo'], cwd=str(tmp_path),
                         state=TASK_RUNNING, pilot_id='p.1', rhapsody_uid='rh.1')
        ps.tasks['t.x'] = rec
        ps.task_log.append(rec)

        plugin2 = _make_plugin(tmp_path, with_pool=False)   # fresh; replay only
        assert _SID in plugin2._pool_states
        ps2 = _pool(plugin2)
        assert ps2.tasks['t.x'].state == TASK_RUNNING
        assert plugin2._uid_to_task.get('rh.1') == (_SID, 'cpu', 't.x')

    def test_endpoint_mode_ledger_replayed(self, tmp_path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._endpoint_mode_log.append(
            EndpointModeRecord(task_id='t.e', endpoint='gpuendpoint',
                               state=TASK_RUNNING))
        plugin2 = _make_plugin(tmp_path, with_pool=False)
        assert plugin2._endpoint_mode_tasks.get('t.e') == 'gpuendpoint'

    def test_endpoint_mode_terminal_filtered(self, tmp_path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._endpoint_mode_log.append(
            EndpointModeRecord(task_id='t.e', endpoint='gpuendpoint',
                               state=TASK_RUNNING))
        plugin._endpoint_mode_log.append(
            EndpointModeRecord(task_id='t.e', endpoint='gpuendpoint',
                               state=TASK_DONE))
        plugin2 = _make_plugin(tmp_path, with_pool=False)
        assert 't.e' not in plugin2._endpoint_mode_tasks


# ---------------------------------------------------------------------------
# C5 — stale uid ignored after re-enqueue
# ---------------------------------------------------------------------------

class TestStaleUid:

    def test_late_terminal_for_reenqueued_task_ignored(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        ps = _pool(plugin)
        pilot = PilotRecord(pid='p.1', pool='cpu', owning_sid=_SID, size_key='s',
                            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
                            capacity=2, in_flight=1)
        ps.pilots['p.1'] = pilot
        task = TaskRecord(task_id='t.r', pool='cpu', owning_sid=_SID,
                          cmd=['/bin/echo'], cwd=str(tmp_path),
                          state=TASK_RUNNING, pilot_id='p.1', rhapsody_uid='rh.old')
        ps.tasks['t.r'] = task
        plugin._uid_to_task['rh.old'] = (_SID, 'cpu', 't.r')

        plugin._mark_pilot_failed(ps, pilot, 'lost')
        assert task.state == TASK_QUEUED
        assert 'rh.old' not in plugin._uid_to_task

        plugin._handle_task_terminal('rh.old', TASK_DONE, {})
        assert task.state == TASK_QUEUED    # not clobbered


# ---------------------------------------------------------------------------
# C6 — endpoint-mode ledger self-prunes via snapshot
# ---------------------------------------------------------------------------

class TestEndpointModeCompaction:

    def test_snapshot_drops_terminal_entries(self, tmp_path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        log = plugin._endpoint_mode_log
        plugin._endpoint_mode_tasks['t.live'] = 'e1'
        log.append(EndpointModeRecord(task_id='t.live', endpoint='e1',
                                      state=TASK_RUNNING))
        log.append(EndpointModeRecord(task_id='t.gone', endpoint='e2',
                                      state=TASK_DONE))
        live = {tid: EndpointModeRecord(task_id=tid, endpoint=ep,
                                        state=TASK_RUNNING)
                for tid, ep in plugin._endpoint_mode_tasks.items()}
        log.snapshot(live)
        assert set(log.replay().keys()) == {'t.live'}


# ---------------------------------------------------------------------------
# H2 — regression guard against the loop-state-fragile antipattern
# ---------------------------------------------------------------------------

def test_no_get_event_loop_run_until_complete_in_tests():
    here = Path(__file__).parent
    pattern = 'get_event_loop().run_' + 'until_complete'
    offenders = [p.name for p in here.glob('test_*.py')
                 if p.name != Path(__file__).name and pattern in p.read_text()]
    assert not offenders, f"use asyncio.run instead: {offenders}"
