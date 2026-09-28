"""Recovery / lifecycle tests for plugin_task_dispatcher (broker-hosted).

- C2  pilot child-endpoint liveness → DONE/FAILED on ``lost``, capacity
      reclaimed, tasks re-enqueued; suspect blip does not tear down; a child
      never seen ``present`` (post-restart) is never demoted.
- C4  restart correlation: pools + ``_uid_to_task`` rebuilt from the sid-scoped
      durable store (``_replay_state`` / ``PoolStore``); the endpoint-mode
      ledger (a plain ``task_id -> endpoint`` dict, atomically persisted) is
      replayed too.
- C5  a late terminal event for a re-enqueued task's stale uid is ignored.
- C6  a completed endpoint-mode task is dropped from the ledger immediately
      (no separate compaction step — the persisted dict only ever holds live
      entries).
- H2  guard against the loop-state-fragile ``run_until_complete`` antipattern.
"""

import asyncio
import time
from pathlib import Path

from fastapi import FastAPI

from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher
from radical.orbit.task_dispatcher_config import PoolConfig, PilotSize
from radical.orbit.task_dispatcher_state import (
    PilotRecord, TaskRecord,
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
    app.state.broker_url     = 'https://localhost:9999'
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
    """One ACTIVE, *submitted* pilot: a psij job plus its child endpoint.

    ``psij_job_id`` is what makes it submitted rather than an adopted
    endpoint (plan 122), and the two end differently when their child goes
    away: a batch job lost before its walltime is FAILED, an adopted
    endpoint is DONE.
    """
    ps = _pool(plugin)
    pilot = PilotRecord(
        pid=pid, pool='cpu', owning_sid=_SID, size_key='s',
        rhapsody_backend='concurrent', state=PILOT_ACTIVE, psij_job_id='j.1',
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
        ps, pilot = _active_pilot(plugin, walltime_deadline=time.time() - 1)
        ps.tasks['t.1'] = TaskRecord(task_id='t.1', pool='cpu', owning_sid=_SID,
                                     cmd=['/bin/echo'], cwd=str(tmp_path),
                                     state=TASK_RUNNING, pilot_id='p.1')
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'lost')
        assert pilot.state == PILOT_DONE
        assert ps.tasks['t.1'].state == TASK_QUEUED
        assert ps.tasks['t.1'].pilot_id is None
        assert ps.live_pilots() == []

    def test_lost_before_walltime_marks_failed(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        ps, pilot = _active_pilot(plugin, walltime_deadline=time.time() + 1000)
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'lost')
        assert pilot.state == PILOT_FAILED
        assert ps.live_pilots() == []

    def test_suspect_blip_does_not_demote(self, tmp_path):
        plugin = _make_plugin(tmp_path)
        _, pilot = _active_pilot(plugin, walltime_deadline=time.time() + 1000)
        _topo(plugin, 'endpoint0_p.1', 'present')
        _topo(plugin, 'endpoint0_p.1', 'suspect')
        assert pilot.state == PILOT_ACTIVE
        assert pilot.accepting_new_tasks is False

    def test_unseen_child_never_demoted(self, tmp_path):
        """Restart race: an ACTIVE pilot whose child never appears 'present'
        (never synthesized 'lost') survives — no `_seen` heuristic needed."""
        plugin = _make_plugin(tmp_path)
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
        ps.persist()

        plugin2 = _make_plugin(tmp_path, with_pool=False)   # fresh; replay only
        assert _SID in plugin2._pool_states
        ps2 = _pool(plugin2)
        assert ps2.tasks['t.x'].state == TASK_RUNNING
        assert plugin2._uid_to_task.get('rh.1') == (_SID, 'cpu', 't.x')

    def test_endpoint_mode_ledger_replayed(self, tmp_path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._endpoint_mode_tasks['t.e'] = 'gpuendpoint'
        plugin._persist_endpoint_mode()

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        assert plugin2._endpoint_mode_tasks.get('t.e') == 'gpuendpoint'

    def test_endpoint_mode_terminal_dropped_immediately(self, tmp_path):
        """A completed endpoint-mode task is popped (and re-persisted) the
        moment its terminal event is handled — the on-disk ledger is a plain
        dict of live entries only, so a restart never sees the terminal
        one."""
        plugin = _make_plugin(tmp_path, with_pool=False)
        plugin._endpoint_mode_tasks['t.e'] = 'gpuendpoint'
        plugin._persist_endpoint_mode()

        plugin._handle_task_terminal('t.e', TASK_DONE, {})
        assert 't.e' not in plugin._endpoint_mode_tasks

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
# C6 — endpoint-mode ledger only ever holds live entries (see
# TestRestartCorrelation.test_endpoint_mode_terminal_dropped_immediately: a
# terminal task is popped from ``_endpoint_mode_tasks`` and re-persisted the
# moment its terminal event is handled, so there is nothing left to compact).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# H2 — regression guard against the loop-state-fragile antipattern
# ---------------------------------------------------------------------------

def test_no_get_event_loop_run_until_complete_in_tests():
    here = Path(__file__).parent
    pattern = 'get_event_loop().run_' + 'until_complete'
    offenders = [p.name for p in here.glob('test_*.py')
                 if p.name != Path(__file__).name and pattern in p.read_text()]
    assert not offenders, f"use asyncio.run instead: {offenders}"


# ---------------------------------------------------------------------------
# Capability-class pools: persistence and replay (plan 121 §10)
# ---------------------------------------------------------------------------

import json                                                     # noqa: E402

from radical.orbit.task_dispatcher_config import PoolMember     # noqa: E402


def _member(mid='m_x', **overrides) -> PoolMember:
    defaults = dict(
        member_id=mid, endpoint_name=f'ep_{mid}', queue='regular',
        account='proj',
        pilot_sizes={'d': PilotSize(nodes=1, cpus_per_node=4,
                                    rhapsody_backend='concurrent')},
        default_size='d', attributes={'software': [mid[-1]]})
    defaults.update(overrides)
    return PoolMember(**defaults)


def _class_cfg(*members, name='fed') -> PoolConfig:
    members = members or (_member(),)
    primary = members[0]
    return PoolConfig(
        name=name, queue=primary.queue, account=primary.account,
        pilot_sizes=primary.pilot_sizes, default_size=primary.default_size,
        endpoint_name=primary.endpoint_name,
        pool_class='gpu', multi_member=True,
        members={m.member_id: m for m in members},
        strategy='conservative', strategy_config={'min_dwell_sec': 0.0})


class TestClassPoolReplay:

    def test_members_and_pilots_survive(self, tmp_path):
        plugin = _make_plugin(tmp_path, with_pool=False)
        ps = plugin._materialise_pool(_SID, _class_cfg(_member('m_x'),
                                                       _member('m_y')))
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed', owning_sid=_SID, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_y', endpoint_name='ep_m_y',
            attributes={'software': ['y']}, nodes=2, cpus_per_node=8,
            child_endpoint_name='fed_m_y_p.1', capacity=16)
        ps.persist()

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        ps2 = plugin2._pool_states[_SID]['fed']
        assert ps2.config.multi_member is True
        assert ps2.config.pool_class   == 'gpu'
        assert list(ps2.config.members) == ['m_x', 'm_y']
        p = ps2.pilots['p.1']
        assert p.member_id     == 'm_y'
        assert p.endpoint_name == 'ep_m_y'
        assert p.attributes    == {'software': ['y']}
        assert (p.nodes, p.cpus_per_node) == (2, 8)
        assert p.child_endpoint_name == 'fed_m_y_p.1'

    def test_replays_from_the_directory_it_was_found_in(self, tmp_path):
        """The dir must not be re-derived from mutable config: replay and
        declaration would then diverge and lose the pool's history."""
        plugin = _make_plugin(tmp_path, with_pool=False)
        ps = plugin._materialise_pool(_SID, _class_cfg(_member('m_x'),
                                                       _member('m_y')))
        found_dir = ps.state_dir
        ps.config.members.pop('m_x')          # primary member departs
        ps.config.reproject()
        ps.tasks['t.1'] = TaskRecord(task_id='t.1', pool='fed',
                                     owning_sid=_SID, cmd=[], cwd='/tmp')
        ps.persist()

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        ps2 = plugin2._pool_states[_SID]['fed']
        assert ps2.state_dir == found_dir
        assert list(ps2.config.members) == ['m_y']
        assert 't.1' in ps2.tasks

    def test_emptied_class_pool_replays(self, tmp_path):
        """DELETE .../members with force: true legitimately produces one."""
        plugin = _make_plugin(tmp_path, with_pool=False)
        ps = plugin._materialise_pool(_SID, _class_cfg())
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed', owning_sid=_SID, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_DONE,
            member_id='m_x', nodes=1, cpus_per_node=4)
        ps.config.members.clear()
        ps.config.reproject()
        ps.persist()

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        ps2 = plugin2._pool_states[_SID]['fed']
        assert ps2.config.members == {}
        assert ps2.config.multi_member is True
        assert 'p.1' in ps2.pilots     # its history is NOT lost


class TestLegacyReplayUnchanged:

    def test_persist_replay_persist_is_stable(self, tmp_path):
        """The explicit ``multi_member: false`` wins on replay, so the
        persisted implicit member never turns a legacy pool into a class
        pool."""
        plugin = _make_plugin(tmp_path)
        ps = _pool(plugin)
        ps.persist()
        first = json.loads((ps.state_dir / 'state.json').read_text())
        assert first['config']['multi_member'] is False

        plugin2 = _make_plugin(tmp_path, with_pool=False)
        ps2 = _pool(plugin2)
        assert ps2.config.multi_member is False
        assert list(ps2.config.members) == ['']
        ps2.persist()
        assert json.loads(
            (ps2.state_dir / 'state.json').read_text())['config'] == \
            first['config']

    def test_pre_121_state_file_replays_with_its_live_pilot(self, tmp_path):
        """An on-disk file written before 121: no members, no multi_member,
        and a pilot with no member/size snapshot."""
        state_dir = tmp_path / 'state' / _SID / 'cpu__endpoint0'
        state_dir.mkdir(parents=True)
        (state_dir / 'state.json').write_text(json.dumps({
            'owning_sid': _SID,
            'config': {
                'name': 'cpu', 'queue': 'batch', 'account': 'proj',
                'endpoint_name': 'endpoint0', 'default_size': 's',
                'pilot_sizes': {'s': {'nodes': 3, 'cpus_per_node': 4,
                                      'gpus_per_node': 0,
                                      'walltime_sec': 3600,
                                      'rhapsody_backend': 'concurrent'}},
                'min_pilots': 0, 'max_pilots': 4, 'scratch_base': None,
                'strategy': 'conservative', 'strategy_config': {}},
            'pilots': {'p.1': {
                'pid': 'p.1', 'pool': 'cpu', 'size_key': 's',
                'rhapsody_backend': 'concurrent', 'owning_sid': _SID,
                'state': 'STARTING', 'capacity': 0,
                'child_endpoint_name': 'cpu_p.1',
                'submitted_at': 100.0}},
            'tasks': {},
        }))

        plugin = _make_plugin(tmp_path, with_pool=False)
        ps = plugin._pool_states[_SID]['cpu']
        assert ps.config.multi_member is False
        assert list(ps.config.members) == ['']
        assert ps.config.members[''].endpoint_name == 'endpoint0'
        pilot = ps.pilots['p.1']
        assert pilot.member_id == ''          # -> the implicit member
        assert pilot.nodes     == 0           # no snapshot yet
        assert pilot.child_endpoint_name == 'cpu_p.1'   # untouched

        # the snapshot is repaired at the handshake, off the member menu
        plugin._dispatch_notify = lambda t, d: None
        plugin._activate_pilot(ps, pilot)
        assert (pilot.nodes, pilot.cpus_per_node) == (3, 4)
        assert pilot.capacity == 12
