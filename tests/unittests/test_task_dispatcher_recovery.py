"""Recovery / lifecycle tests for plugin_task_dispatcher.

Covers the paths that previously had no coverage (and were buggy):

- C2  pilot child-edge disconnect → DONE/FAILED, capacity reclaimed,
      tasks re-enqueued; plus the restart race where a replayed-ACTIVE
      pilot must NOT be torn down before its child reconnects.
- C4  restart correlation: ``_uid_to_task`` rebuilt from the replayed
      task log; edge-mode ledger replayed (terminal entries filtered).
- C5  a late terminal event for a re-enqueued task's stale uid is
      ignored rather than clobbering the task.
- H2  guard: no test reintroduces the loop-state-fragile
      ``get_event_loop().run_until_complete`` antipattern.
"""

import asyncio
import time
from pathlib import Path

from fastapi import FastAPI

from radical.edge.plugin_task_dispatcher import PluginTaskDispatcher
from radical.edge.task_dispatcher_config import PoolConfig, PilotSize
from radical.edge.task_dispatcher_state import (
    PilotRecord, TaskRecord, EdgeModeRecord,
    PILOT_ACTIVE, PILOT_DONE, PILOT_FAILED,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pool_cfg(name: str = 'cpu') -> PoolConfig:
    return PoolConfig(
        name         = name,
        edge_name    = 'edge0',
        queue        = 'batch',
        account      = 'proj',
        pilot_sizes  = {'s': PilotSize(nodes=1, cpus_per_node=4,
                                       rhapsody_backend='concurrent')},
        default_size = 's',
        max_pilots   = 4,
        strategy     = 'conservative',
        strategy_config = {'min_dwell_sec': 0.0},
    )


def _make_plugin(tmp_path: Path, *, with_pool: bool = True):
    app = FastAPI()
    app.state.edge_name  = 'edge0'
    app.state.bridge_url = 'https://localhost:9999'
    plugin = PluginTaskDispatcher(
        app, state_root=tmp_path / 'state', scratch_root=tmp_path / 'scratch')
    if with_pool:
        plugin._materialise_pool(_pool_cfg())
    return plugin


def _active_pilot(plugin, *, pid='p.1', child='edge0_p.1',
                  walltime_deadline=0.0):
    ps = plugin._pool_states['cpu']
    pilot = PilotRecord(
        pid=pid, pool='cpu', size_key='s', rhapsody_backend='concurrent',
        state=PILOT_ACTIVE, submitted_at=100.0, active_at=110.0,
        capacity=4, in_flight=1, child_edge_name=child,
        walltime_deadline=walltime_deadline)
    ps.pilots[pid] = pilot
    return ps, pilot


def _topology(plugin, edges_present):
    payload = {e: {'plugins': ['rhapsody']} for e in edges_present}
    asyncio.run(plugin.on_topology_change(payload))


# ---------------------------------------------------------------------------
# C2 — phantom-pilot recovery on disconnect
# ---------------------------------------------------------------------------

class TestPhantomPilotRecovery:

    def test_disconnect_after_walltime_marks_done(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps, pilot = _active_pilot(plugin, walltime_deadline=time.time() - 1)
        task = TaskRecord(task_id='t.1', pool='cpu', cmd=['/bin/echo'],
                          cwd=str(tmp_path), state=TASK_RUNNING,
                          pilot_id='p.1')
        ps.tasks['t.1'] = task

        _topology(plugin, ['edge0_p.1'])   # child seen
        _topology(plugin, [])              # child gone, walltime passed

        assert pilot.state == PILOT_DONE
        assert ps.tasks['t.1'].state == TASK_QUEUED   # re-enqueued
        assert ps.tasks['t.1'].pilot_id is None
        assert ps._pilots_snapshot() == []            # capacity reclaimed

    def test_disconnect_before_walltime_marks_failed(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps, pilot = _active_pilot(plugin,
                                  walltime_deadline=time.time() + 1000)
        _topology(plugin, ['edge0_p.1'])
        _topology(plugin, [])

        assert pilot.state == PILOT_FAILED
        assert ps._pilots_snapshot() == []

    def test_replayed_active_not_demoted_before_child_seen(self, tmp_path):
        """Restart race: an ACTIVE pilot whose child hasn't reconnected
        yet must survive a topology event that doesn't list it."""
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        _, pilot = _active_pilot(plugin,
                                 walltime_deadline=time.time() + 1000)
        # Never feed a topology event containing the child → never "seen".
        _topology(plugin, ['some_other_edge'])
        assert pilot.state == PILOT_ACTIVE   # untouched

    def test_disconnect_unseen_child_is_noop(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        _, pilot = _active_pilot(plugin,
                                 walltime_deadline=time.time() + 1000)
        _topology(plugin, [])   # child never seen → no demotion
        assert pilot.state == PILOT_ACTIVE


# ---------------------------------------------------------------------------
# C4 — restart correlation
# ---------------------------------------------------------------------------

class TestRestartCorrelation:

    def test_uid_to_task_rebuilt_on_materialise(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        rec = TaskRecord(task_id='t.x', pool='cpu', cmd=['/bin/echo'],
                         cwd=str(tmp_path), state=TASK_RUNNING,
                         pilot_id='p.1', rhapsody_uid='rh.1')
        ps.tasks['t.x'] = rec
        ps.task_log.append(rec)

        # Simulate a bridge restart: a fresh plugin over the same state.
        plugin2 = _make_plugin(tmp_path)
        ps2 = plugin2._pool_states['cpu']
        assert ps2.tasks['t.x'].state == TASK_RUNNING
        assert plugin2._uid_to_task.get('rh.1') == ('cpu', 't.x')

    def test_edge_mode_ledger_replayed(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._edge_mode_log.append(
            EdgeModeRecord(task_id='t.e', edge='gpuedge',
                           state=TASK_RUNNING))

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        assert plugin2._edge_mode_tasks.get('t.e') == 'gpuedge'

    def test_edge_mode_terminal_filtered_on_replay(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._edge_mode_log.append(
            EdgeModeRecord(task_id='t.e', edge='gpuedge',
                           state=TASK_RUNNING))
        plugin._edge_mode_log.append(
            EdgeModeRecord(task_id='t.e', edge='gpuedge',
                           state=TASK_DONE))

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        assert 't.e' not in plugin2._edge_mode_tasks


# ---------------------------------------------------------------------------
# C5 — stale uid ignored after re-enqueue
# ---------------------------------------------------------------------------

class TestStaleUid:

    def test_late_terminal_for_reenqueued_task_ignored(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        pilot = PilotRecord(pid='p.1', pool='cpu', size_key='s',
                            rhapsody_backend='concurrent',
                            state=PILOT_ACTIVE, capacity=2, in_flight=1)
        ps.pilots['p.1'] = pilot
        task = TaskRecord(task_id='t.r', pool='cpu', cmd=['/bin/echo'],
                          cwd=str(tmp_path), state=TASK_RUNNING,
                          pilot_id='p.1', rhapsody_uid='rh.old')
        ps.tasks['t.r'] = task
        plugin._uid_to_task['rh.old'] = ('cpu', 't.r')

        plugin._mark_pilot_failed(ps, pilot, 'lost')
        assert task.state == TASK_QUEUED
        assert 'rh.old' not in plugin._uid_to_task   # mapping cleared

        # A late terminal event for the dead pilot's old uid arrives.
        plugin._handle_task_terminal('rh.old', TASK_DONE, {})
        assert task.state == TASK_QUEUED   # not clobbered


# ---------------------------------------------------------------------------
# C6 — edge-mode ledger self-prunes via snapshot
# ---------------------------------------------------------------------------

class TestEdgeModeCompaction:

    def test_snapshot_drops_terminal_entries(self, tmp_path: Path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        log = plugin._edge_mode_log
        # one live, one already-terminal
        plugin._edge_mode_tasks['t.live'] = 'e1'
        log.append(EdgeModeRecord(task_id='t.live', edge='e1',
                                  state=TASK_RUNNING))
        log.append(EdgeModeRecord(task_id='t.gone', edge='e2',
                                  state=TASK_DONE))

        # Snapshot the live set only (what the compaction sweeper does).
        live = {tid: EdgeModeRecord(task_id=tid, edge=edge,
                                    state=TASK_RUNNING)
                for tid, edge in plugin._edge_mode_tasks.items()}
        log.snapshot(live)

        replayed = log.replay()
        assert set(replayed.keys()) == {'t.live'}


# ---------------------------------------------------------------------------
# H2 — regression guard against the loop-state-fragile antipattern
# ---------------------------------------------------------------------------

def test_no_get_event_loop_run_until_complete_in_tests():
    """``get_event_loop().run_until_complete`` breaks once any earlier
    test calls ``asyncio.run`` (which nulls the loop on 3.11+).  Keep the
    suite order-independent by forbidding the pattern outright."""
    here = Path(__file__).parent
    pattern = 'get_event_loop().run_' + 'until_complete'  # avoid self-match
    offenders = []
    for path in here.glob('test_*.py'):
        if path.name == Path(__file__).name:
            continue
        if pattern in path.read_text():
            offenders.append(path.name)
    assert not offenders, \
        f"use asyncio.run instead of get_event_loop(): {offenders}"
