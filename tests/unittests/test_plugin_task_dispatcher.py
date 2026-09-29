"""Unit tests for plugin_task_dispatcher (broker-hosted, strict per-session pools).

Focus: plugin-level behavior that does not require a live broker — strict
per-session pool isolation, restart-time replay, session-close teardown,
cached-state idempotency, staging, pilot binding via rich topology
(present/suspect/lost), and the transport port (broker caller +
``asyncio.to_thread``).

Endpoint calls are stubbed via :meth:`_get_psij_client` / :meth:`_get_rhapsody_client`
(async factories; in production they return real caller-backed ``PSIJClient`` /
``RhapsodyClient`` helpers).  The dispatcher drives those clients' plain SYNC
methods (``submit_tunneled`` / ``get_task`` / ``cancel_task`` / ``submit_tasks``
— there are no async ``a<method>`` twins) via ``asyncio.to_thread``, so a plain
``MagicMock`` returning a JSON-serializable dict is all a stub needs; no real
broker or WebSocket is required.
"""

import asyncio
import base64
import threading
import time
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_task_dispatcher import (
    PluginTaskDispatcher, PoolState, backend_kwargs,
)
from radical.orbit.task_dispatcher_config import PoolConfig, PilotSize
from radical.orbit.task_dispatcher_state   import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_ACTIVE, PILOT_FAILED, PILOT_DONE,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool_cfg(*, pool_name: str = 'cpu',
                   max_pilots: int = 4,
                   strategy: str = 'conservative') -> PoolConfig:
    return PoolConfig(
        name         = pool_name,
        endpoint_name    = 'endpoint0',
        queue        = 'batch',
        account      = 'proj',
        pilot_sizes  = {
            's': PilotSize(nodes=1, cpus_per_node=4,
                           rhapsody_backend='concurrent'),
        },
        default_size = 's',
        max_pilots   = max_pilots,
        strategy     = strategy,
        strategy_config = {'min_dwell_sec': 0.0},
    )


def _pool_dict(**overrides):
    d = {
        'name'        : 'cpu',
        'endpoint_name'   : 'endpoint0',
        'queue'       : 'batch',
        'account'     : 'proj',
        'default_size': 's',
        'pilot_sizes' : {
            's': {'nodes': 1, 'cpus_per_node': 4,
                  'rhapsody_backend': 'concurrent'}},
        'max_pilots'  : 4,
        'strategy'    : 'conservative',
        'strategy_config': {'min_dwell_sec': 0.0},
    }
    d.update(overrides)
    return d


def _make_plugin(tmp_path: Path, *, instance: str = 'task_dispatcher',
                 broker_caller=None) -> tuple:
    """Instantiate a plugin bound to tmp_path; return (app, plugin)."""
    app = FastAPI()
    app.state.endpoint_name   = 'endpoint0'
    app.state.broker_url      = 'https://localhost:9999'
    app.state.broker_caller   = broker_caller
    app.state.broker_tap      = None
    plugin = PluginTaskDispatcher(
        app, instance_name=instance,
        state_root=tmp_path / 'state',
        scratch_root=tmp_path / 'scratch')
    return app, plugin


def _register(client: TestClient, plugin, body=None, headers=None) -> str:
    r = client.post(f'{plugin.namespace}/register_session',
                    json=body if body is not None else {}, headers=headers)
    assert r.status_code == 200, r.text
    return r.json()['sid']


def _session_with_cpu(client, plugin, headers=None, sid=None,
                      lifetime=None) -> str:
    body = {'pools': [_pool_dict()]}
    if sid is not None:
        body['sid'] = sid
    if lifetime is not None:
        body['lifetime'] = lifetime
    return _register(client, plugin, body=body, headers=headers)


def _pool(plugin, sid, name='cpu') -> PoolState:
    return plugin._pool_states[sid][name]


# ---------------------------------------------------------------------------
# Init / is_enabled
# ---------------------------------------------------------------------------

class TestInit:

    def test_is_enabled_on_broker(self):
        with patch('radical.orbit.utils.host_role') as m:
            m.return_value = {'role': 'broker'}
            assert PluginTaskDispatcher.is_enabled(FastAPI()) is True

    def test_is_enabled_false_off_broker(self):
        with patch('radical.orbit.utils.host_role') as m:
            for role in ('login', 'compute', 'standalone'):
                m.return_value = {'role': role}
                assert PluginTaskDispatcher.is_enabled(FastAPI()) is False

    def test_init_starts_with_no_pools(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        assert plugin._pool_states == {}

    def test_routes_registered(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        pats = [pat.pattern for _, pat, _, _ in app.state.direct_routes]
        ns = plugin.namespace.lstrip('/')
        for frag in (f'{ns}/pools$', f'{ns}/fleet/', f'{ns}/submit/',
                     f'{ns}/cancel/', f'{ns}/cancel_all/',
                     f'{ns}/stage_in/', f'{ns}/stage_out/'):
            assert any(frag in p for p in pats), f'route {frag} missing'


# ---------------------------------------------------------------------------
# Strict per-session pool isolation (the M7 verification bullet)
# ---------------------------------------------------------------------------

class TestStrictIsolation:

    def test_same_named_pools_are_distinct_across_sessions(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid_a = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        sid_b = _session_with_cpu(client, plugin, sid='B', lifetime='persistent')
        assert sid_a != sid_b
        ps_a = _pool(plugin, sid_a, 'cpu')
        ps_b = _pool(plugin, sid_b, 'cpu')
        assert ps_a is not ps_b                     # distinct PoolStates
        assert ps_a.state_dir != ps_b.state_dir     # distinct on-disk state

    def test_cross_session_attach_impossible(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        # A second session that declares no pools gets its own default; it has
        # no 'cpu' pool → submit to 'cpu' 404s (no cross-session visibility).
        sid_b = _register(client, plugin, body={})
        r = client.post(f'{plugin.namespace}/submit/{sid_b}', json={
            'pool': 'cpu', 'task_id': 't.1',
            'cmd': ['/bin/echo'], 'cwd': '/tmp'})
        assert r.status_code == 404

    def test_reregister_same_pool_is_idempotent(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        first = _pool(plugin, sid, 'cpu')
        _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        assert _pool(plugin, sid, 'cpu') is first

    def test_no_pools_materialises_session_default(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={})
        assert 'default' in plugin._pool_states[sid]

    def test_invalid_pool_body_400(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        r = client.post(f'{plugin.namespace}/register_session',
                        json={'pools': 'not-a-list'})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Restart-time replay (built here, not lazy)
# ---------------------------------------------------------------------------

class TestRestartReplay:

    def test_replay_rebuilds_pools_for_multiple_sids(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        _session_with_cpu(client, plugin, sid='B', lifetime='persistent')
        # seed a pilot + a RUNNING task under A
        ps_a = _pool(plugin, 'A', 'cpu')
        ps_a.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE)
        rec = TaskRecord(task_id='t.x', pool='cpu', owning_sid='A',
                         cmd=['/bin/echo'], cwd=str(tmp_path),
                         state=TASK_RUNNING, pilot_id='p.1',
                         rhapsody_uid='rh.1')
        ps_a.tasks['t.x'] = rec
        ps_a.persist()

        # Simulate a broker restart: a fresh plugin over the same state root.
        _, plugin2 = _make_plugin(tmp_path)
        assert set(plugin2._pool_states.keys()) == {'A', 'B'}
        ps2 = _pool(plugin2, 'A', 'cpu')
        assert ps2.tasks['t.x'].state == TASK_RUNNING
        assert 'p.1' in ps2.pilots
        # uid→task correlation rebuilt (with the owning sid)
        assert plugin2._uid_to_task.get('rh.1') == ('A', 'cpu', 't.x')


# ---------------------------------------------------------------------------
# Session-close teardown + reclaim
# ---------------------------------------------------------------------------

class TestSessionTeardown:

    def test_unregister_tears_down_pools_and_cancels_pilots(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin,
                        body={'sid': 'A', 'lifetime': 'persistent',
                              'pools': [_pool_dict()]})
        ps = _pool(plugin, sid, 'cpu')
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', owning_sid=sid, size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE)
        r = client.post(f'{plugin.namespace}/unregister_session/{sid}')
        assert r.status_code == 200
        assert sid not in plugin._pool_states               # pools dropped
        assert ps.pilots['p.1'].state == PILOT_FAILED       # pilot cancelled

    def test_cancel_all_reclaims_persistent_pools(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin,
                        body={'sid': 'default'})   # reserved persistent
        ps = _pool(plugin, sid, 'default')
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='default', owning_sid=sid, size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE)
        r = client.post(f'{plugin.namespace}/cancel_all/{sid}')
        assert r.status_code == 200
        assert r.json()['pools_reclaimed'] == 1
        assert sid not in plugin._pool_states
        assert ps.pilots['p.1'].state == PILOT_FAILED

    def test_ephemeral_owner_lost_drains_and_cancels(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)

        async def scenario():
            plugin._maybe_start()
            sid = await plugin._open_session(None, 'ephemeral', None,
                                             owner='clientA')
            plugin._materialise_pool(sid, _make_pool_cfg())
            ps = _pool(plugin, sid, 'cpu')
            ps.pilots['p.1'] = PilotRecord(
                pid='p.1', pool='cpu', owning_sid=sid, size_key='s',
                rhapsody_backend='concurrent', state=PILOT_ACTIVE)
            # owner declared lost → stamps the reclaim-drain deadline; the
            # 5 s sweep reclaims once it passes.  Backdate + sweep now.
            await plugin.on_topology_change(
                {'clientA': {'role': 'endpoint', 'plugins': {},
                             'liveness': 'lost'}})
            plugin._records[sid].drain_deadline = time.time() - 1
            await plugin._cleanup_expired_sessions()
            return sid, ps

        sid, ps = asyncio.run(scenario())
        assert sid not in plugin._pool_states
        assert ps.pilots['p.1'].state == PILOT_FAILED

    def test_persistent_pool_survives_owner_loss(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin.reclaim_drain = 0.05

        async def scenario():
            plugin._maybe_start()
            sid = await plugin._open_session('P', 'persistent', None,
                                             owner='clientA')
            plugin._materialise_pool(sid, _make_pool_cfg())
            await plugin.on_topology_change(
                {'clientA': {'role': 'endpoint', 'plugins': {},
                             'liveness': 'lost'}})
            await asyncio.sleep(0.25)
            return sid

        sid = asyncio.run(scenario())
        assert sid in plugin._pool_states           # persistent → not reclaimed

    def test_pool_survives_suspect_owner_blip(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin.reclaim_drain = 0.05

        async def scenario():
            plugin._maybe_start()
            sid = await plugin._open_session('E', 'ephemeral', None,
                                             owner='clientA')
            plugin._materialise_pool(sid, _make_pool_cfg())
            # suspect must NOT arm the drain
            await plugin.on_topology_change(
                {'clientA': {'role': 'endpoint', 'plugins': {},
                             'liveness': 'suspect'}})
            await asyncio.sleep(0.25)
            return sid

        sid = asyncio.run(scenario())
        assert sid in plugin._pool_states           # blip → pool survives


# ---------------------------------------------------------------------------
# Routes: pools / fleet / submit
# ---------------------------------------------------------------------------

class TestRoutes:

    def test_fleet_scoped_to_session(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        r = client.get(f'{plugin.namespace}/fleet/{sid}')
        assert r.status_code == 200
        assert 'cpu' in r.json()['pools']

    def test_fleet_unknown_session_404(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        r = client.get(f'{plugin.namespace}/fleet/nope')
        assert r.status_code == 404

    def test_submit_rejects_unknown_pool(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'nope', 'task_id': 't.1',
            'cmd': ['/bin/echo'], 'cwd': '/tmp'})
        assert r.status_code == 404

    def test_submit_enqueues_task_and_stamps_owner(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps = _pool(plugin, sid, 'cpu')
        with patch.object(ps.policy, 'pick_dispatch',
                         return_value=None) as pick_dispatch:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo', 'hi'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            # submit drains any ready dispatches immediately (the policy
            # itself scales up on the housekeeping tick, not here).
            assert pick_dispatch.called
        assert ps.tasks['t.1'].state == TASK_QUEUED
        assert ps.tasks['t.1'].owning_sid == sid

    def test_cached_done_returns_without_reexec(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps = _pool(plugin, sid, 'cpu')
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu', owning_sid=sid,
            cmd=['/bin/echo'], cwd=str(tmp_path), state=TASK_DONE, exit_code=0)
        with patch.object(ps.policy, 'pick_dispatch') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.done',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.json()['state'] == TASK_DONE
            spy.assert_not_called()   # cached-DONE returns before draining

    def test_cancel_queued_is_immediate(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps = _pool(plugin, sid, 'cpu')
        ps.tasks['t.q'] = TaskRecord(
            task_id='t.q', pool='cpu', owning_sid=sid, cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_QUEUED)
        r = client.post(f'{plugin.namespace}/cancel/{sid}/t.q')
        assert r.status_code == 200
        assert ps.tasks['t.q'].state == TASK_CANCELED

    def test_get_task_404(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        r = client.get(f'{plugin.namespace}/task/{sid}/nope')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------

class TestStaging:

    def test_stage_in_out_roundtrip(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps = _pool(plugin, sid, 'cpu')
        content = b'hello world'
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'cpu', 'filename': 'in.txt',
                  'content_b64': base64.b64encode(content).decode('ascii')})
        assert r.status_code == 200
        assert (Path(r.json()['cwd']) / 'in.txt').read_bytes() == content

        # stage_out reads the record's own ``cwd`` (plan 121 §8 rule 4:
        # recomputing ``scratch_base / task_id`` was wrong for any task
        # with an explicit or dispatcher-assigned cwd), so the record has
        # to name the directory the output actually lands in.
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid=sid, cmd=['/bin/echo'],
            cwd=str(ps.scratch_base / 't.1'), state=TASK_DONE)
        (ps.scratch_base / 't.1' / 'out.txt').write_bytes(b'result')
        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/out.txt')
        assert r.status_code == 200
        assert base64.b64decode(r.json()['content_b64']) == b'result'

    def test_stage_in_bad_filename(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'cpu', 'filename': '../evil',
                  'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Pilot binding via rich topology (present / suspect / lost)
# ---------------------------------------------------------------------------

def _child_topo(name, liveness='present'):
    return {name: {'role': 'endpoint',
                   'plugins': {'rhapsody': {'namespace': '/rhapsody'}},
                   'liveness': liveness}}


class TestTopologyBinding:

    def _plugin_with_pilot(self, tmp_path, pid='p.1',
                           child='endpoint0_p.1', state=PILOT_PENDING,
                           walltime=1e12):
        """A pool holding one *submitted* pilot -- a psij job whose child
        endpoint is expected under *child*.  ``psij_job_id`` is what
        distinguishes it from an adopted endpoint (plan 122), which ends
        DONE rather than FAILED when its child goes away."""
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        ps.pilots[pid] = PilotRecord(
            pid=pid, pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=state, submitted_at=100.0,
            psij_job_id='j.1',
            child_endpoint_name=child, walltime_deadline=walltime,
            endpoint_name='endpoint0', nodes=1, cpus_per_node=4)
        return plugin, ps

    def test_present_binds_pending_pilot(self, tmp_path):
        plugin, ps = self._plugin_with_pilot(tmp_path)
        with patch.object(ps.policy, 'on_pilot_state') as spy:
            asyncio.run(plugin.on_topology_change(_child_topo('endpoint0_p.1')))
        assert ps.pilots['p.1'].state == PILOT_ACTIVE
        assert ps.pilots['p.1'].capacity == 4
        spy.assert_called_once()

    def test_suspect_child_pauses_not_demotes(self, tmp_path):
        plugin, ps = self._plugin_with_pilot(tmp_path, state=PILOT_ACTIVE)
        ps.pilots['p.1'].capacity = 4
        asyncio.run(plugin.on_topology_change(
            _child_topo('endpoint0_p.1', 'suspect')))
        assert ps.pilots['p.1'].state == PILOT_ACTIVE           # not demoted
        assert ps.pilots['p.1'].accepting_new_tasks is False    # paused
        # returning present un-pauses
        asyncio.run(plugin.on_topology_change(
            _child_topo('endpoint0_p.1', 'present')))
        assert ps.pilots['p.1'].accepting_new_tasks is True

    def test_lost_before_walltime_marks_failed(self, tmp_path):
        plugin, ps = self._plugin_with_pilot(tmp_path, state=PILOT_ACTIVE,
                                             walltime=1e12)
        ps.pilots['p.1'].capacity = 4
        asyncio.run(plugin.on_topology_change(
            _child_topo('endpoint0_p.1', 'lost')))
        assert ps.pilots['p.1'].state == PILOT_FAILED

    def test_lost_after_walltime_marks_done(self, tmp_path):
        plugin, ps = self._plugin_with_pilot(tmp_path, state=PILOT_ACTIVE,
                                             walltime=1.0)   # long past
        ps.pilots['p.1'].capacity = 4
        asyncio.run(plugin.on_topology_change(
            _child_topo('endpoint0_p.1', 'lost')))
        assert ps.pilots['p.1'].state == PILOT_DONE

    def test_absent_child_not_demoted(self, tmp_path):
        """A child never listed 'lost' (e.g. not-yet-reconnected after a
        restart) is left alone — replaces the old `_seen` heuristic."""
        plugin, ps = self._plugin_with_pilot(tmp_path, state=PILOT_ACTIVE)
        asyncio.run(plugin.on_topology_change(_child_topo('someone_else')))
        assert ps.pilots['p.1'].state == PILOT_ACTIVE


# ---------------------------------------------------------------------------
# Pilot failure re-enqueues tasks
# ---------------------------------------------------------------------------

class TestMarkPilotFailed:

    def test_reenqueues_running_tasks(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = PilotRecord(pid='p.1', pool='cpu', owning_sid='A', size_key='s',
                            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
                            capacity=2, in_flight=2)
        ps.pilots['p.1'] = pilot
        ps.tasks['t.r'] = TaskRecord(task_id='t.r', pool='cpu', owning_sid='A',
                                     cmd=['/bin/echo'], cwd=str(tmp_path),
                                     state=TASK_RUNNING, pilot_id='p.1')
        plugin._mark_pilot_failed(ps, pilot, 'test')
        assert pilot.state == PILOT_FAILED
        assert ps.tasks['t.r'].state == TASK_QUEUED
        assert ps.tasks['t.r'].pilot_id is None

    def test_finalize_stamps_finished_at(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = PilotRecord(pid='p.1', pool='cpu', owning_sid='A',
                            size_key='s', rhapsody_backend='concurrent',
                            state=PILOT_ACTIVE, submitted_at=time.time(),
                            active_at=time.time())
        ps.pilots['p.1'] = pilot
        assert pilot.finished_at is None
        plugin._mark_pilot_done(ps, pilot, 'walltime reached')
        assert pilot.state == PILOT_DONE
        assert pilot.finished_at is not None
        assert pilot.finished_at >= pilot.active_at


# ---------------------------------------------------------------------------
# Pilot history in the verbose pool summary (accounting surface)
# ---------------------------------------------------------------------------

class TestPilotHistory:

    def _seeded(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps  = _pool(plugin, sid, 'cpu')
        return plugin, client, sid, ps

    def test_history_keeps_a_pilot_the_live_list_drops(self, tmp_path):
        plugin, client, sid, ps = self._seeded(tmp_path)
        pilot = PilotRecord(pid='p.gone', pool='cpu', owning_sid=sid,
                            size_key='s', rhapsody_backend='concurrent',
                            state=PILOT_ACTIVE, submitted_at=100.0,
                            active_at=150.0, child_endpoint_name='cpu_p.gone')
        ps.pilots['p.gone'] = pilot
        plugin._mark_pilot_failed(ps, pilot, 'child endpoint lost')

        r = client.get(f'{plugin.namespace}/pool/{sid}/cpu')
        assert r.status_code == 200
        body = r.json()
        # gone from the live fleet ...
        assert body['pilots'] == []
        # ... but still in the history, with its end timestamp and the
        # size_key that resolves its node count through pilot_sizes.
        hist = {p['pid']: p for p in body['pilot_history']}
        assert set(hist) == {'p.gone'}
        assert hist['p.gone']['state']       == PILOT_FAILED
        assert hist['p.gone']['active_at']   == 150.0
        assert hist['p.gone']['finished_at'] is not None
        assert hist['p.gone']['size_key']    == 's'
        assert hist['p.gone']['child_endpoint_name'] == 'cpu_p.gone'
        assert body['pilot_sizes']['s']['nodes'] == 1

    def test_history_lists_live_and_terminal_pilots(self, tmp_path):
        plugin, client, sid, ps = self._seeded(tmp_path)
        ps.pilots['p.live'] = PilotRecord(
            pid='p.live', pool='cpu', owning_sid=sid, size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE)
        ps.pilots['p.dead'] = PilotRecord(
            pid='p.dead', pool='cpu', owning_sid=sid, size_key='s',
            rhapsody_backend='concurrent', state=PILOT_DONE,
            finished_at=42.0)
        body = client.get(f'{plugin.namespace}/pool/{sid}/cpu').json()
        assert {p['pid'] for p in body['pilots']}        == {'p.live'}
        assert {p['pid'] for p in body['pilot_history']} == {'p.live',
                                                             'p.dead'}

    def test_non_verbose_summary_has_no_history(self, tmp_path):
        plugin, client, sid, ps = self._seeded(tmp_path)
        body = client.get(f'{plugin.namespace}/pools').json()
        assert 'pilot_history' not in body['pools'][sid]['cpu']


# ---------------------------------------------------------------------------
# Housekeeping: an owner-less (replayed) pool is never ticked
# ---------------------------------------------------------------------------

class TestHousekeepingOrphanGuard:

    @pytest.mark.asyncio
    async def test_replayed_pool_without_session_is_not_ticked(self, tmp_path):
        """A pool replayed off disk has no session until its owner returns.

        ``_replay_state`` re-materialises every state dir at construction,
        before any client re-registers.  Such a pool must not scale up on
        its own — with a ``min_pilots`` floor it would otherwise submit a
        pilot per backoff window forever, to an endpoint that may be gone.
        """
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        _session_with_cpu(client, plugin, sid='A', lifetime='persistent')

        # restart: fresh plugin over the same state root, no session yet
        _, plugin2 = _make_plugin(tmp_path)
        assert 'cpu' in plugin2._pool_states['A']
        assert 'A' not in plugin2._sessions

        ps = _pool(plugin2, 'A', 'cpu')
        with patch.object(ps.policy, 'on_tick') as tick, \
                patch('radical.orbit.plugin_task_dispatcher'
                      '._TICK_INTERVAL_SEC', 0.01):
            task = asyncio.ensure_future(plugin2._housekeeping())
            await asyncio.sleep(0.1)
            task.cancel()
            try:    await task
            except asyncio.CancelledError:
                pass
        tick.assert_not_called()

    @pytest.mark.asyncio
    async def test_pool_with_a_live_session_is_ticked(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A', lifetime='persistent')
        ps  = _pool(plugin, sid, 'cpu')
        with patch.object(ps.policy, 'on_tick') as tick, \
                patch('radical.orbit.plugin_task_dispatcher'
                      '._TICK_INTERVAL_SEC', 0.01):
            task = asyncio.ensure_future(plugin._housekeeping())
            await asyncio.sleep(0.1)
            task.cancel()
            try:    await task
            except asyncio.CancelledError:
                pass
        assert tick.called


# ---------------------------------------------------------------------------
# Async transport port: proxies over the broker caller (mocked)
# ---------------------------------------------------------------------------

class TestPilotSubmitTransport:

    def test_submit_tunneled_passes_tunnel_none(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        member = ps.config.primary_member()
        size = member.pilot_sizes[member.default_size]
        record = PilotRecord(
            pid='p.a', pool='cpu', owning_sid='A',
            size_key=member.default_size,
            rhapsody_backend=size.rhapsody_backend, state=PILOT_PENDING,
            endpoint_name=member.endpoint_name)
        ps.pilots[record.pid] = record

        # The dispatcher drives child clients' SYNC methods via
        # asyncio.to_thread (there are no async `a<method>` twins anymore),
        # so a plain MagicMock returning a dict is all `submit_tunneled` needs.
        psij_mock = MagicMock()
        psij_mock.submit_tunneled = MagicMock(return_value={'job_id': 'jid'})
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size, member))

        psij_mock.submit_tunneled.assert_called_once()
        assert psij_mock.submit_tunneled.call_args.args[2] == 'none'
        assert record.psij_job_id == 'jid'

    def test_cancel_during_submit_cancels_the_new_job(self, tmp_path):
        """A pilot cancelled while submit_tunneled is in flight (no job id
        yet, so it is only marked FAILED) must not be resurrected when the
        submit returns: the just-created job is cancelled instead."""
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        member = ps.config.primary_member()
        size = member.pilot_sizes[member.default_size]
        record = PilotRecord(
            pid='p.a', pool='cpu', owning_sid='A',
            size_key=member.default_size,
            rhapsody_backend=size.rhapsody_backend, state=PILOT_PENDING,
            endpoint_name=member.endpoint_name)
        ps.pilots[record.pid] = record

        def _submit(*_args):
            # The member-removal cancel lands while psij is submitting.
            plugin._mark_pilot_failed(ps, record, 'cancel requested')
            return {'job_id': 'jid'}

        psij_mock = MagicMock()
        psij_mock.submit_tunneled = MagicMock(side_effect=_submit)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size, member))

        psij_mock.cancel_job.assert_called_once_with('jid')
        assert record.state       == PILOT_FAILED
        assert record.psij_job_id is None

    def test_child_registered_during_submit_stays_active(self, tmp_path):
        """The child may register (topology -> ACTIVE) before submit_tunneled
        returns; the late submit result must not demote the pilot back to
        STARTING, which would leave it unschedulable until the next
        topology change."""
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        member = ps.config.primary_member()
        size = member.pilot_sizes[member.default_size]
        record = PilotRecord(
            pid='p.a', pool='cpu', owning_sid='A',
            size_key=member.default_size,
            rhapsody_backend=size.rhapsody_backend, state=PILOT_PENDING,
            endpoint_name=member.endpoint_name,
            nodes=size.nodes, cpus_per_node=size.cpus_per_node)
        ps.pilots[record.pid] = record

        def _submit(*_args):
            # The child's registration reaches on_topology_change first.
            plugin._reconcile_pilots_for(
                ps, {record.child_endpoint_name: {'liveness': 'present'}})
            return {'job_id': 'jid'}

        psij_mock = MagicMock()
        psij_mock.submit_tunneled = MagicMock(side_effect=_submit)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size, member))

        assert record.state       == PILOT_ACTIVE
        assert record.psij_job_id == 'jid'
        assert record.capacity    >  0

    def test_refuses_without_broker_caller(self, tmp_path):
        """Old-stack construction (no caller) → the child-client factory
        refuses cleanly (None), so pilot/rhapsody paths mark work failed
        instead of touching a loop.  (The old `_call` refusal moved here when
        the dispatcher started driving the real caller-backed helpers.)"""
        _, plugin = _make_plugin(tmp_path, broker_caller=None)
        assert plugin._broker_caller is None
        assert asyncio.run(plugin._get_psij_client('someone')) is None
        assert asyncio.run(plugin._get_rhapsody_client('someone')) is None


# ---------------------------------------------------------------------------
# Endpoint-mode submit (transparent proxy to a target endpoint's rhapsody)
# ---------------------------------------------------------------------------

class TestEndpointMode:

    def _seed_topology(self, plugin, endpoint_plugins):
        plugin._connected_endpoints = {
            name: set(plugins) for name, plugins in endpoint_plugins.items()}

    def test_unknown_endpoint_404(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {})
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'endpoint': 'ghost', 'task_id': 't.1',
            'cmd': ['/bin/echo'], 'cwd': '/tmp'})
        assert r.status_code == 404

    def test_endpoint_without_rhapsody_503(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['sysinfo']})
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'endpoint': 'ep', 'task_id': 't.1',
            'cmd': ['/bin/echo'], 'cwd': '/tmp'})
        assert r.status_code == 503

    def test_proxy_submit_and_get_and_cancel(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['rhapsody']})
        # SYNC method names: the dispatcher drives them via asyncio.to_thread
        # (no async `a<method>` twins anymore); plain MagicMocks suffice.
        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[{'uid': 't.1',
                                                        'state': 'NEW'}])
        rh_mock.get_task     = MagicMock(return_value={'uid': 't.1',
                                                       'state': 'RUNNING'})
        rh_mock.cancel_task  = MagicMock(return_value={'uid': 't.1',
                                                       'state': 'CANCELED'})
        with patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/sleep', '0'], 'cwd': '/tmp',
                # accepted and shape-validated, but advisory only: with no
                # pool there is no known backend to map onto
                'requirements': {'cores': 4, 'gpus': 2}})
            assert r.status_code == 200, r.text
            assert plugin._endpoint_mode_tasks.get('t.1') == 'ep'
            rh_mock.submit_tasks.assert_called_once()
            # the forwarded dict is UNCHANGED from a submit without the
            # key, and the response body grows no 'requirements'
            fwd = rh_mock.submit_tasks.call_args.args[0][0]
            assert fwd['task_backend_specific_kwargs'] == {'cwd': '/tmp'}
            assert 'requirements' not in r.json()

            r = client.get(f'{plugin.namespace}/task/{sid}/t.1')
            assert r.json()['result']['state'] == 'RUNNING'

            r = client.post(f'{plugin.namespace}/cancel/{sid}/t.1')
            assert r.status_code == 200
            rh_mock.cancel_task.assert_called_once_with('t.1')

    def test_endpoint_mode_shape_400_still_fires(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['rhapsody']})
        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        with patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/true'], 'cwd': '/tmp',
                'requirements': {'gpu': 1}})
        assert r.status_code == 400
        assert r.json()['detail'] == "requirements: unknown key 'gpu'"
        rh_mock.submit_tasks.assert_not_called()

    def test_endpoint_mode_skips_the_pool_gates(self, tmp_path):
        # No pool ⇒ no fit check and no backend gate: 8 cores would be a
        # 400 on the 4-cpu 'cpu' pool, and mpi would be a 400 on a
        # dragon_v1 pool.  Endpoint mode accepts both.
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['rhapsody']})
        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        with patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/true'], 'cwd': '/tmp',
                'requirements': {'cores': 8, 'gpus': 4, 'mpi': True}})
        assert r.status_code == 200, r.text
        fwd = rh_mock.submit_tasks.call_args.args[0][0]
        assert fwd['task_backend_specific_kwargs'] == {'cwd': '/tmp'}

    def test_endpoint_mode_advisory_log(self, tmp_path, caplog):
        # exactly one advisory line for a submit that declared something
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['rhapsody']})
        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        body = {'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/true'], 'cwd': '/tmp',
                'requirements': {'cores': 4}}
        with caplog.at_level('INFO', logger='radical.orbit'), \
             patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json=body)
        assert r.status_code == 200, r.text
        lines = [rec.getMessage() for rec in caplog.records
                 if 'requirements are advisory' in rec.getMessage()]
        assert len(lines) == 1
        assert 't.1' in lines[0]

    def test_terminal_event_clears_endpoint_mode(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._endpoint_mode_tasks['t.1'] = 'ep'
        notified = []
        plugin._dispatch_notify = lambda t, d: notified.append((t, d))
        # Feed a rhapsody task_status event exactly as the broker tap delivers.
        plugin._on_event({'plugin': 'rhapsody', 'topic': 'task_status',
                          'data': {'uid': 't.1', 'state': 'DONE',
                                   'exit_code': 0}})
        assert 't.1' not in plugin._endpoint_mode_tasks
        assert notified and notified[0][1]['state'] == TASK_DONE

    def test_endpoint_mode_terminal_during_submit_is_not_lost(self,
                                                              tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        self._seed_topology(plugin, {'ep': ['rhapsody']})
        notified = []
        plugin._dispatch_notify = lambda t, d: notified.append((t, d))

        def submit(dicts):
            # the terminal event lands before the threaded call returns
            plugin._handle_task_terminal('t.1', TASK_DONE, {'exit_code': 0})
            return []

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(side_effect=submit)
        with patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/true'], 'cwd': '/tmp'})
        assert r.status_code == 200, r.text
        assert 't.1' not in plugin._endpoint_mode_tasks
        assert notified and notified[0][1]['state'] == TASK_DONE

    def test_terminal_batch_event_is_handled(self, tmp_path):
        '''Several completions in one frame arrive as task_status_batch.'''
        _, plugin = _make_plugin(tmp_path)
        seen = []
        plugin._handle_task_terminal = \
            lambda uid, state, data: seen.append((uid, state))
        plugin._on_event({'plugin': 'rhapsody',
                          'topic' : 'task_status_batch',
                          'data'  : {'tasks': [
                              {'uid': 't.1', 'state': 'DONE',
                               'exit_code': 0},
                              {'uid': 't.2', 'state': 'FAILED',
                               'exit_code': 1},
                              {'uid': 't.3', 'state': 'RUNNING'}]}})
        assert seen == [('t.1', TASK_DONE), ('t.2', TASK_FAILED)]

    def test_on_event_ignores_other_plugins(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        called = []
        plugin._handle_task_terminal = lambda *a: called.append(a)
        plugin._on_event({'plugin': 'psij', 'topic': 'task_status',
                          'data': {'uid': 'x', 'state': 'DONE'}})
        assert called == []


# ---------------------------------------------------------------------------
# Rhapsody-dialect bulk submit (pool mode for function tasks)
# ---------------------------------------------------------------------------

def _dialect_td(uid, pool='cpu', **extra):
    """A rhapsody-style task dict as the client ships it: cloudpickled
    fields ride as base64 strings the dispatcher never decodes."""
    td = {'uid': uid, 'pool': pool,
          'function': 'cloudpickle::AAAA',
          '_pickled_fields': ['function']}
    td.update(extra)
    return td


class TestRhapsodyDialect:

    def _two_pool_session(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        body = {'pools': [_pool_dict(), _pool_dict(name='gpu')],
                'sid': 'A', 'lifetime': 'persistent'}
        sid = _register(client, plugin, body=body)
        return plugin, client, sid

    def test_bulk_submit_groups_and_persists_once_per_pool(self, tmp_path):
        plugin, client, sid = self._two_pool_session(tmp_path)
        cpu, gpu = _pool(plugin, sid, 'cpu'), _pool(plugin, sid, 'gpu')
        with patch.object(cpu, 'persist') as p_cpu, \
             patch.object(gpu, 'persist') as p_gpu, \
             patch.object(cpu.policy, 'pick_dispatch', return_value=None), \
             patch.object(gpu.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
                'tasks': [_dialect_td('t.1'), _dialect_td('t.2'),
                          _dialect_td('t.3', pool='gpu')]})
            assert r.status_code == 200, r.text
            assert [a['state'] for a in r.json()] == [TASK_QUEUED] * 3
            # one ledger write per touched pool, not per task
            assert p_cpu.call_count == 1
            assert p_gpu.call_count == 1

        # the pool key came off; the rest of the dict is held verbatim
        rec = cpu.tasks['t.1']
        assert rec.task_dict['function'] == 'cloudpickle::AAAA'
        assert 'pool' not in rec.task_dict
        assert rec.owning_sid == sid

    def test_bulk_submit_unknown_pool_is_atomic(self, tmp_path):
        plugin, client, sid = self._two_pool_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [_dialect_td('t.1'), _dialect_td('t.2', pool='nope')]})
        assert r.status_code == 404
        # validation ran before any state was touched
        assert 't.1' not in _pool(plugin, sid, 'cpu').tasks

    def test_bulk_submit_requires_uid_and_pool(self, tmp_path):
        plugin, client, sid = self._two_pool_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'pool': 'cpu'}]})
        assert r.status_code == 400
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'uid': 't.1'}]})
        assert r.status_code == 400

    def test_resubmit_done_is_cached(self, tmp_path):
        plugin, client, sid = self._two_pool_session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu', owning_sid=sid, cmd=[], cwd='',
            task_dict={'function': 'cloudpickle::AAAA'},
            state=TASK_DONE, exit_code=0)
        with patch.object(ps.policy, 'pick_dispatch') as spy:
            r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
                'tasks': [_dialect_td('t.done')]})
            assert r.json() == [{'uid': 't.done', 'state': TASK_DONE}]
            spy.assert_not_called()

    def test_drain_forwards_bulk_with_namespaced_uids(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            child_endpoint_name='endpoint0_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        for uid in ('t.1', 't.2'):
            td = _dialect_td(uid)
            td.pop('pool')
            ps.tasks[uid] = TaskRecord(
                task_id=uid, pool='cpu', owning_sid='A', cmd=[], cwd='',
                task_dict=td, state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        picks = [(ps.tasks['t.1'], pilot), (ps.tasks['t.2'], pilot), None]

        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=picks), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=rh_mock)), \
                 patch.object(ps, 'persist') as persist:
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
                return persist.call_count

        persists = asyncio.run(drive())

        # both claims rode ONE ledger write and ONE bulk submit
        assert persists == 1
        rh_mock.submit_tasks.assert_called_once()
        sent = rh_mock.submit_tasks.call_args.args[0]
        # uids are namespaced by the owning session: the pilot's rhapsody
        # session is shared, client-side counters are not unique across
        # sessions
        assert [d['uid'] for d in sent] == ['t.1.A', 't.2.A']
        assert plugin._uid_to_task['t.1.A'] == ('A', 'cpu', 't.1')
        assert ps.tasks['t.1'].rhapsody_uid == 't.1.A'
        assert ps.tasks['t.1'].state == TASK_RUNNING

    def _submit_one(self, tmp_path, submit):
        '''Post one claimed task through ``_do_rhapsody_submit``; the pilot's
        ``submit_tasks`` runs *submit* (on the worker thread) with the loop.'''
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            child_endpoint_name='endpoint0_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        td = _dialect_td('t.1')
        td.pop('pool')
        task = ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A', cmd=[], cwd='',
            task_dict=td, state=TASK_RUNNING)

        async def drive():
            loop    = asyncio.get_running_loop()
            rh_mock = MagicMock()
            rh_mock.submit_tasks = MagicMock(
                side_effect=lambda dicts: submit(plugin, loop))
            with patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=rh_mock)):
                await plugin._do_rhapsody_submit(ps, [task], pilot)

        asyncio.run(drive())
        return plugin, task

    @staticmethod
    def _report_done(plugin, loop):
        # the pilot reports DONE, and the loop handles it, while the submit
        # call is still in flight
        handled = threading.Event()

        def deliver():
            plugin._on_event({
                'plugin': 'rhapsody', 'topic': 'task_status',
                'data'  : {'uid': 't.1.A', 'state': 'DONE', 'exit_code': 0}})
            handled.set()

        loop.call_soon_threadsafe(deliver)
        assert handled.wait(5)

    def test_terminal_during_submit_is_not_lost(self, tmp_path):
        def submit(plugin, loop):
            self._report_done(plugin, loop)
            return []
        _, task = self._submit_one(tmp_path, submit)
        assert task.state == TASK_DONE

    def test_failed_submit_fails_task_and_unmaps(self, tmp_path):
        def submit(plugin, loop):
            raise RuntimeError('boom')
        plugin, task = self._submit_one(tmp_path, submit)
        assert task.state == TASK_FAILED
        assert task.rhapsody_uid is None
        assert 't.1.A' not in plugin._uid_to_task

    def test_failed_submit_keeps_outcome_already_reported(self, tmp_path):
        def submit(plugin, loop):
            self._report_done(plugin, loop)
            raise RuntimeError('boom after accept')
        _, task = self._submit_one(tmp_path, submit)
        assert task.state == TASK_DONE

    def test_terminal_notifications_batch_and_forward_results(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        for uid in ('t.1', 't.2'):
            ps.tasks[uid] = TaskRecord(
                task_id=uid, pool='cpu', owning_sid='A', cmd=[], cwd='',
                task_dict={'function': 'cloudpickle::AAAA'},
                state=TASK_RUNNING, rhapsody_uid=f'{uid}.A')
            plugin._uid_to_task[f'{uid}.A'] = ('A', 'cpu', uid)

        notified = []
        plugin._dispatch_notify = lambda t, d: notified.append((t, d))

        async def drive():
            with patch.object(ps, 'persist') as persist:
                plugin._handle_task_terminal('t.1.A', TASK_DONE, {
                    'uid': 't.1.A', 'state': 'DONE', 'exit_code': 0,
                    'return_value': 'cloudpickle::QUJD',
                    '_return_value_encoding': 'cloudpickle'})
                plugin._handle_task_terminal('t.2.A', TASK_DONE, {
                    'uid': 't.2.A', 'state': 'DONE', 'exit_code': 0})
                plugin._flush_rh()
                await asyncio.sleep(0)
                return persist.call_count

        persists = asyncio.run(drive())

        # both completions coalesced: one frame, one ledger write
        assert persists == 1
        assert len(notified) == 1
        topic, frame = notified[0]
        assert topic == 'task_status_batch'
        payloads = frame['tasks']
        # the dispatcher's OWN uid, not the namespaced child uid
        assert [p['uid'] for p in payloads] == ['t.1', 't.2']
        assert payloads[0]['return_value'] == 'cloudpickle::QUJD'
        assert payloads[0]['state'] == TASK_DONE

    def test_client_requires_pool_key(self, tmp_path):
        from radical.orbit.plugin_task_dispatcher import TaskDispatcherClient
        c = TaskDispatcherClient.__new__(TaskDispatcherClient)
        c._sid = 'A'
        with pytest.raises(ValueError, match="pool"):
            c.submit_tasks([{'uid': 't.1'}])


# ---------------------------------------------------------------------------
# Per-task resource requirements (plan 120)
# ---------------------------------------------------------------------------

_ORACLE_REQ = {'cores': 4, 'gpus': 2, 'mem_gb': 8, 'ranks': 2,
               'mpi': True, 'software': ['gromacs'], 'labels': {'zone': 'a'}}


class TestBackendKwargs:
    """``backend_kwargs`` is the mapping oracle: the table in its docstring
    is the contract, and rhapsody reads NOTHING outside
    ``task_backend_specific_kwargs``, so a wrong key name is invisible at
    runtime.  These assertions are the only thing that catches that.
    """

    @pytest.mark.parametrize('backend,expected', [
        ('dragon_v2',     {'ranks': 2, 'gpus_per_rank': 1}),
        ('radical_pilot', {'ranks': 2, 'cores_per_rank': 2,
                           'gpus_per_rank': 1, 'mem_per_rank': 4096}),
        ('dragon_v3',     {'type': 'mpi', 'ranks': 2}),
        ('dragon_v1',     {'ranks': 2}),
        ('dask',          {'resources': {'GPU': 2}}),
        ('concurrent',    {}),
    ])
    def test_mapping_oracle(self, backend, expected):
        assert backend_kwargs(_ORACLE_REQ, backend) == expected

    @pytest.mark.parametrize('backend', [
        'dragon_v1', 'dragon_v2', 'dragon_v3', 'dask', 'concurrent',
        'radical_pilot', 'something_new'])
    def test_empty_requirements_emit_nothing(self, backend):
        # every key equal to the backend's own default is omitted, so an
        # existing task forwards byte-identically
        assert backend_kwargs({}, backend) == {}
        assert backend_kwargs({'cores': 1, 'gpus': 0, 'mem_gb': 0,
                               'ranks': 1, 'mpi': False}, backend) == {}

    def test_software_and_labels_never_reach_rhapsody(self):
        req = {'software': ['gromacs', 'cuda'],
               'labels': {'zone': 'a', 'tier': 2}}
        for backend in ('dragon_v1', 'dragon_v2', 'dragon_v3', 'dask',
                        'concurrent', 'radical_pilot'):
            out = backend_kwargs(req, backend)
            assert 'software' not in out
            assert 'labels' not in out
            assert out == {}

    def test_dragon_v3_ignores_ranks_without_mpi(self):
        # dragon_v3 reads `ranks` ONLY under type == 'mpi'
        assert backend_kwargs({'cores': 4, 'ranks': 4}, 'dragon_v3') == {}

    def test_dragon_v3_mpi_alone_emits_only_the_type(self):
        # ranks == 1 is the backend's own default and is omitted; 'type'
        # is not a default, so mpi alone still selects the MPI path
        assert backend_kwargs({'mpi': True}, 'dragon_v3') == {'type': 'mpi'}

    def test_radical_pilot_single_rank_still_carries_cores(self):
        # ranks == 1 is omitted, but cores_per_rank == 4 is not a default
        assert backend_kwargs({'cores': 4, 'ranks': 1}, 'radical_pilot') \
            == {'cores_per_rank': 4}


class TestRequirementsValidation:
    """Exact 400 detail strings — a typo must not vanish silently."""

    def _submit(self, client, plugin, sid, requirements, pool='cpu'):
        return client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': pool, 'task_id': 't.1',
            'cmd': ['/bin/echo'], 'cwd': '/tmp',
            'requirements': requirements})

    def _session(self, tmp_path, pools=None):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        body = {'sid': 'A', 'lifetime': 'persistent',
                'pools': pools if pools is not None else [_pool_dict()]}
        sid = _register(client, plugin, body=body)
        return plugin, client, sid

    # -- shape ---------------------------------------------------------

    @pytest.mark.parametrize('requirements,detail', [
        ({'gpu': 1},
         "requirements: unknown key 'gpu'"),
        ({'cores': 0},
         "requirements: 'cores' must be a positive integer, got 0"),
        ({'cores': True},
         "requirements: 'cores' must be a positive integer, got True"),
        ({'ranks': 0},
         "requirements: 'ranks' must be a positive integer, got 0"),
        ({'gpus': 'two'},
         "requirements: 'gpus' must be a non-negative integer, got 'two'"),
        ({'gpus': -1},
         "requirements: 'gpus' must be a non-negative integer, got -1"),
        ({'gpus': False},
         "requirements: 'gpus' must be a non-negative integer, got False"),
        ({'mem_gb': 'x'},
         "requirements: 'mem_gb' must be a non-negative number, got 'x'"),
        ({'mem_gb': -0.5},
         "requirements: 'mem_gb' must be a non-negative number, got -0.5"),
        ({'mem_gb': True},
         "requirements: 'mem_gb' must be a non-negative number, got True"),
        ({'mpi': 'yes'},
         "requirements: 'mpi' must be a boolean"),
        ({'mpi': 1},
         "requirements: 'mpi' must be a boolean"),
        ({'software': 'gromacs'},
         "requirements: 'software' must be a list of strings"),
        ({'software': ['ok', 3]},
         "requirements: 'software' must be a list of strings"),
        ({'labels': ['a']},
         "requirements: 'labels' must be a mapping of string to "
         "string|number"),
        ({'labels': {'a': ['b']}},
         "requirements: 'labels' must be a mapping of string to "
         "string|number"),
        ({'cores': 2, 'ranks': 4},
         "requirements: 'cores' (2) must be >= 'ranks' (4)"),
        ({'cores': 4, 'gpus': 3, 'ranks': 2},
         "requirements: 'gpus' (3) must be divisible by 'ranks' (2)"),
    ])
    def test_shape_400s(self, tmp_path, requirements, detail):
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, requirements)
        assert r.status_code == 400, r.text
        assert r.json()['detail'] == detail
        assert 't.1' not in _pool(plugin, sid, 'cpu').tasks

    def test_non_mapping_400(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, 'cores=4')
        assert r.status_code == 400
        assert r.json()['detail'] == 'requirements: must be a mapping'

    def test_labels_accept_numbers(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        with patch.object(_pool(plugin, sid, 'cpu').policy, 'pick_dispatch',
                          return_value=None):
            r = self._submit(client, plugin, sid,
                             {'labels': {'a': 'b', 'n': 2, 'f': 1.5}})
        assert r.status_code == 200, r.text

    @pytest.mark.parametrize('bad', [float('nan'), float('inf'),
                                     float('-inf')])
    def test_mem_gb_rejects_non_finite(self, tmp_path, bad):
        # NaN would sail past a bare `>= 0`; inf is meaningless as a size
        from radical.orbit.plugin_task_dispatcher import (
            parse_requirements, RequirementsError)
        with pytest.raises(RequirementsError) as e:
            parse_requirements({'mem_gb': bad})
        assert str(e.value).startswith(
            "requirements: 'mem_gb' must be a non-negative number")

    # -- cores derived from ranks --------------------------------------

    def test_ranks_alone_derives_cores(self, tmp_path):
        # {'ranks': 4} means "four processes" -- it must NOT 400 against
        # the cores default of 1
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = self._submit(client, plugin, sid, {'ranks': 4})
        assert r.status_code == 200, r.text
        # the derived value is what gets persisted and forwarded
        assert r.json()['requirements'] == {'ranks': 4, 'cores': 4}
        assert ps.tasks['t.1'].requirements == {'ranks': 4, 'cores': 4}

    def test_derived_cores_still_face_the_fit_check(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, {'ranks': 8})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 8 cores exceed every pilot_size "
            "(largest: 's', 4 cpus/node)")

    def test_explicit_cores_below_ranks_is_still_400(self, tmp_path):
        # an omission is derived; a contradiction is refused
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, {'cores': 2, 'ranks': 4})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 'cores' (2) must be >= 'ranks' (4)")

    def test_no_gratuitous_cores_key(self, tmp_path):
        # ranks == 1 derives cores == 1, which is the default: don't stamp
        # a key the caller never sent onto the record
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = self._submit(client, plugin, sid, {'gpus': 0, 'ranks': 1})
        assert r.status_code == 200, r.text
        assert r.json()['requirements'] == {'gpus': 0, 'ranks': 1}

    # -- fit -----------------------------------------------------------

    def test_cores_exceed_every_pilot_size(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, {'cores': 8})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 8 cores exceed every pilot_size "
            "(largest: 's', 4 cpus/node)")

    def test_gpus_exceed_every_pilot_size(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        r = self._submit(client, plugin, sid, {'cores': 2, 'gpus': 2})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 2 gpus exceed every pilot_size "
            "(largest: 's', 0 gpus/node)")

    def test_fit_passes_when_any_size_hosts_and_names_the_largest(
            self, tmp_path):
        # mixed pool: 'big' hosts 8 cores, so 8 fits; 16 does not, and the
        # message names the max over the FAILING dimension
        pools = [_pool_dict(pilot_sizes={
            's'  : {'nodes': 1, 'cpus_per_node': 4,
                    'rhapsody_backend': 'concurrent'},
            'big': {'nodes': 1, 'cpus_per_node': 8, 'gpus_per_node': 2,
                    'rhapsody_backend': 'dragon_v2'}})]
        plugin, client, sid = self._session(tmp_path, pools=pools)
        with patch.object(_pool(plugin, sid, 'cpu').policy, 'pick_dispatch',
                          return_value=None):
            r = self._submit(client, plugin, sid, {'cores': 8, 'gpus': 2})
        assert r.status_code == 200, r.text

        r = self._submit(client, plugin, sid, {'cores': 16})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 16 cores exceed every pilot_size "
            "(largest: 'big', 8 cpus/node)")

    def test_largest_is_per_dimension_not_the_biggest_size(self, tmp_path):
        # 'big' is the largest by cpus but has NO gpus; a gpu overflow must
        # name 's', the largest along the failing dimension
        pools = [_pool_dict(pilot_sizes={
            's'  : {'nodes': 1, 'cpus_per_node': 4, 'gpus_per_node': 4,
                    'rhapsody_backend': 'dragon_v2'},
            'big': {'nodes': 1, 'cpus_per_node': 8, 'gpus_per_node': 0,
                    'rhapsody_backend': 'concurrent'}})]
        plugin, client, sid = self._session(tmp_path, pools=pools)
        r = self._submit(client, plugin, sid, {'cores': 8, 'gpus': 8})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 8 gpus exceed every pilot_size "
            "(largest: 's', 4 gpus/node)")

    def test_mem_gb_has_no_fit_check(self, tmp_path):
        # PilotSize carries no memory field, so mem_gb is never a fit 400
        plugin, client, sid = self._session(tmp_path)
        with patch.object(_pool(plugin, sid, 'cpu').policy, 'pick_dispatch',
                          return_value=None):
            r = self._submit(client, plugin, sid, {'mem_gb': 1024})
        assert r.status_code == 200, r.text

    def test_default_pool_refuses_two_cores(self, tmp_path):
        # the built-in 'default' pool is one node of ONE cpu
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A',
                                              'lifetime': 'persistent'})
        r = self._submit(client, plugin, sid, {'cores': 2}, pool='default')
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 2 cores exceed every pilot_size "
            "(largest: 'node', 1 cpus/node)")

    # -- backend gate --------------------------------------------------

    def test_mpi_on_dragon_v1_pool_400(self, tmp_path):
        pools = [_pool_dict(name='x', pilot_sizes={
            's': {'nodes': 1, 'cpus_per_node': 4,
                  'rhapsody_backend': 'dragon_v1'}})]
        plugin, client, sid = self._session(tmp_path, pools=pools)
        r = self._submit(client, plugin, sid, {'mpi': True}, pool='x')
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 'mpi' is unsupported on dragon_v1 "
            "(pool 'x', size 's')")

    def test_mpi_passes_on_a_mixed_pool(self, tmp_path):
        # only ALL-dragon_v1 pools refuse mpi; one hostable size is enough
        pools = [_pool_dict(name='x', default_size='a', pilot_sizes={
            'a': {'nodes': 1, 'cpus_per_node': 4,
                  'rhapsody_backend': 'dragon_v1'},
            'b': {'nodes': 1, 'cpus_per_node': 4,
                  'rhapsody_backend': 'dragon_v3'}})]
        plugin, client, sid = self._session(tmp_path, pools=pools)
        with patch.object(_pool(plugin, sid, 'x').policy, 'pick_dispatch',
                          return_value=None):
            r = self._submit(client, plugin, sid, {'mpi': True}, pool='x')
        assert r.status_code == 200, r.text

    # -- validation precedes the resubmit cache ladder ------------------

    def test_bad_requirements_400_even_on_cached_done(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid=sid, cmd=['/bin/echo'],
            cwd='/tmp', state=TASK_DONE, exit_code=0)
        r = self._submit(client, plugin, sid, {'gpu': 1})
        assert r.status_code == 400
        assert ps.tasks['t.1'].state == TASK_DONE

    def test_changed_requirements_on_resubmit_are_ignored(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid=sid, cmd=['/bin/echo'],
            cwd='/tmp', state=TASK_DONE, exit_code=0,
            requirements={'cores': 1})
        r = self._submit(client, plugin, sid, {'cores': 4})
        assert r.status_code == 200
        assert r.json()['requirements'] == {'cores': 1}
        assert ps.tasks['t.1'].requirements == {'cores': 1}


class TestRequirementsRoundTrip:

    def _session(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={
            'sid': 'A', 'lifetime': 'persistent', 'pools': [_pool_dict()]})
        return plugin, client, sid

    def test_requirements_round_trip_through_get_task(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        req = {'cores': 4, 'gpus': 0, 'mem_gb': 1.5, 'ranks': 2,
               'mpi': False, 'software': ['gromacs'],
               'labels': {'zone': 'a'}}
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo'], 'cwd': '/tmp',
                'requirements': req})
        assert r.status_code == 200, r.text
        assert r.json()['requirements'] == req
        assert ps.tasks['t.1'].requirements == req

        got = client.get(f'{plugin.namespace}/task/{sid}/t.1')
        assert got.json()['requirements'] == req

    @pytest.mark.parametrize('body_extra', [{}, {'requirements': None}])
    def test_absent_or_null_yields_empty_dict(self, tmp_path, body_extra):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        body = {'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo'], 'cwd': '/tmp'}
        body.update(body_extra)
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json=body)
        assert r.status_code == 200, r.text
        assert r.json()['requirements'] == {}
        assert ps.tasks['t.1'].requirements == {}

    def test_dialect_submit_promotes_and_pops_requirements(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        td = _dialect_td('t.1')
        td['requirements'] = {'cores': 2, 'ranks': 2}
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit_rh/{sid}',
                            json={'tasks': [td]})
        assert r.status_code == 200, r.text
        rec = ps.tasks['t.1']
        assert rec.requirements == {'cores': 2, 'ranks': 2}
        # never reaches BaseTask.from_dict
        assert 'requirements' not in rec.task_dict

    def test_dialect_submit_persists_the_validated_block(self, tmp_path):
        # the record carries the derived 'cores', not the raw block
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        td = _dialect_td('t.1')
        td['requirements'] = {'ranks': 4}
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit_rh/{sid}',
                            json={'tasks': [td]})
        assert r.status_code == 200, r.text
        rec = ps.tasks['t.1']
        assert rec.requirements == {'ranks': 4, 'cores': 4}
        assert 'requirements' not in rec.task_dict

    def test_dialect_resubmit_ignores_changed_requirements(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        td = _dialect_td('t.1')
        td.pop('pool')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid=sid, cmd=[], cwd='',
            task_dict=td, state=TASK_DONE, exit_code=0,
            requirements={'cores': 1})
        td = _dialect_td('t.1')
        td['requirements'] = {'cores': 4}
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}',
                        json={'tasks': [td]})
        assert r.status_code == 200, r.text
        assert ps.tasks['t.1'].requirements == {'cores': 1}

    def test_dialect_submit_rejects_the_batch_on_a_bad_block(self, tmp_path):
        plugin, client, sid = self._session(tmp_path)
        ps = _pool(plugin, sid, 'cpu')
        good = _dialect_td('t.1')
        bad  = _dialect_td('t.2')
        bad['requirements'] = {'cores': 99}
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}',
                        json={'tasks': [good, bad]})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            "requirements: 99 cores exceed every pilot_size "
            "(largest: 's', 4 cpus/node)")
        # whole-batch validation ran before any state was touched
        assert ps.tasks == {}


class TestRequirementsForwarding:

    def _dragon_v2_pilot(self, ps):
        pilot = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='dragon_v2', state=PILOT_ACTIVE,
            child_endpoint_name='endpoint0_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        return pilot

    @staticmethod
    def _drain(plugin, ps, picks, rh_mock):
        async def drive():
            with patch.object(ps.policy, 'pick_dispatch', side_effect=picks), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=rh_mock)), \
                 patch.object(ps, 'persist'):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())

    def test_exec_style_merges_mapping_onto_cwd(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = self._dragon_v2_pilot(ps)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A',
            cmd=['/bin/echo', 'hi'], cwd='/scratch/t.1', task_dict=None,
            requirements={'cores': 2, 'gpus': 2, 'ranks': 2},
            state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        self._drain(plugin, ps, [(ps.tasks['t.1'], pilot), None], rh_mock)

        sent = rh_mock.submit_tasks.call_args.args[0]
        # merged ONTO cwd, not replacing it
        assert sent[0]['task_backend_specific_kwargs'] == {
            'cwd': '/scratch/t.1', 'ranks': 2, 'gpus_per_rank': 1}

    def test_exec_style_without_requirements_is_unchanged(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = self._dragon_v2_pilot(ps)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A',
            cmd=['/bin/echo'], cwd='/scratch/t.1', task_dict=None,
            state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        self._drain(plugin, ps, [(ps.tasks['t.1'], pilot), None], rh_mock)

        sent = rh_mock.submit_tasks.call_args.args[0]
        assert sent[0]['task_backend_specific_kwargs'] == {
            'cwd': '/scratch/t.1'}

    def test_dialect_caller_kwargs_win_per_key(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = self._dragon_v2_pilot(ps)
        td = _dialect_td('t.1')
        td.pop('pool')
        # the caller knows its backend: its own 'ranks' overrides the
        # derived one, while 'gpus_per_rank' (which it did not set) still
        # comes from the mapping
        td['task_backend_specific_kwargs'] = {'ranks': 8}
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A', cmd=[], cwd='',
            task_dict=td, requirements={'ranks': 2, 'gpus': 2, 'cores': 2},
            state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        self._drain(plugin, ps, [(ps.tasks['t.1'], pilot), None], rh_mock)

        sent = rh_mock.submit_tasks.call_args.args[0]
        assert sent[0]['task_backend_specific_kwargs'] == {
            'ranks': 8, 'gpus_per_rank': 1}

    def test_dialect_without_requirements_forwards_verbatim(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = self._dragon_v2_pilot(ps)
        td = _dialect_td('t.1')
        td.pop('pool')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A', cmd=[], cwd='',
            task_dict=td, state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        self._drain(plugin, ps, [(ps.tasks['t.1'], pilot), None], rh_mock)

        sent = rh_mock.submit_tasks.call_args.args[0]
        # no requirements ⇒ no derived keys ⇒ the key stays absent
        assert 'task_backend_specific_kwargs' not in sent[0]

    def test_concurrent_backend_forwards_only_cwd(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        pilot = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            child_endpoint_name='endpoint0_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid='A',
            cmd=['/bin/echo'], cwd='/scratch/t.1', task_dict=None,
            requirements={'cores': 4, 'gpus': 0}, state=TASK_QUEUED)

        rh_mock = MagicMock()
        rh_mock.submit_tasks = MagicMock(return_value=[])
        self._drain(plugin, ps, [(ps.tasks['t.1'], pilot), None], rh_mock)

        sent = rh_mock.submit_tasks.call_args.args[0]
        assert sent[0]['task_backend_specific_kwargs'] == {
            'cwd': '/scratch/t.1'}


class TestSubmitTaskClientPayload:

    def _client(self):
        from radical.orbit.plugin_task_dispatcher import TaskDispatcherClient
        c = TaskDispatcherClient.__new__(TaskDispatcherClient)
        c._sid  = 'A'
        c._http = MagicMock()
        c._http.post = MagicMock(return_value=MagicMock(
            status_code=200, json=MagicMock(return_value={})))
        c._url   = lambda p: f'/td/{p}'
        c._raise = lambda *a, **k: None
        return c

    def test_requirements_omitted_when_none(self):
        c = self._client()
        c.submit_task('t.1', ['/bin/echo'], '/tmp', pool='cpu')
        payload = c._http.post.call_args.kwargs['json']
        assert 'requirements' not in payload

    def test_requirements_present_when_given(self):
        c = self._client()
        c.submit_task('t.1', ['/bin/echo'], '/tmp', pool='cpu',
                      requirements={'cores': 4})
        payload = c._http.post.call_args.kwargs['json']
        assert payload['requirements'] == {'cores': 4}


# ---------------------------------------------------------------------------
# Capability-class pools (plan 121)
# ---------------------------------------------------------------------------

def _member(mid='m_x', **overrides):
    m = {
        'member_id'    : mid,
        'endpoint_name': f'ep_{mid}',
        'queue'        : 'regular',
        'account'      : 'proj',
        'default_size' : 'd',
        'pilot_sizes'  : {'d': {'nodes': 1, 'cpus_per_node': 4,
                                'gpus_per_node': 0,
                                'rhapsody_backend': 'concurrent'}},
        'attributes'   : {'software': ['x']},
    }
    m.update(overrides)
    return m


def _class_pool_dict(members=None, **overrides):
    d = {
        'name'      : 'fed',
        'pool_class': 'gpu',
        'members'   : members if members is not None else [_member()],
        'strategy'  : 'conservative',
        'strategy_config': {'min_dwell_sec': 0.0},
    }
    d.update(overrides)
    return d


def _class_session(tmp_path, members=None, **overrides):
    _, plugin = _make_plugin(tmp_path)
    client = TestClient(plugin._app)
    sid = _register(client, plugin, body={
        'sid': 'A', 'lifetime': 'persistent',
        'pools': [_class_pool_dict(members, **overrides)]})
    return plugin, client, sid


class TestMemberRoutes:

    def test_add_member_creates(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_y', attributes={'software': ['y']}))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['created'] is True
        assert body['members'] == ['m_x', 'm_y']
        assert body['member']['member_id'] == 'm_y'
        assert list(_pool(plugin, sid, 'fed').config.members) == \
            ['m_x', 'm_y']

    def test_identical_repost_is_an_idempotent_noop(self, tmp_path):
        """This is what makes the federation's restart replay idempotent."""
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x'))
        assert r.status_code == 200, r.text
        assert r.json()['created'] is False
        assert list(_pool(plugin, sid, 'fed').config.members) == ['m_x']

    def test_differing_redeclaration_is_409(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', queue='other'))
        assert r.status_code == 409
        assert r.json()['detail'] == \
            'member exists with a different declaration'

    def test_non_class_pool_is_409(self, tmp_path):
        """A single-member pool is never promoted in place."""
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A',
                                lifetime='persistent')
        r = client.post(f'{plugin.namespace}/pool/{sid}/cpu/members',
                        json=_member('m_y'))
        assert r.status_code == 409
        assert r.json()['detail'] == (
            "pool cpu is not a class pool; declare it with 'members'")

    def test_unknown_pool_is_404(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/nope/members',
                        json=_member('m_y'))
        assert r.status_code == 404

    def test_bad_declaration_is_400(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('BAD ID'))
        assert r.status_code == 400
        assert 'member_id' in r.json()['detail']

    def test_add_member_persists_and_notifies(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        seen = []
        plugin._dispatch_notify = lambda t, d: seen.append((t, d))
        client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                    json=_member('m_y'))
        assert ('pool_members', seen[0][1]) == seen[0]
        assert seen[0][1]['action'] == 'add'
        assert seen[0][1]['member_ids'] == ['m_x', 'm_y']
        # persisted: a fresh plugin replays both members
        _, plugin2 = _make_plugin(tmp_path)
        assert list(plugin2._pool_states[sid]['fed'].config.members) == \
            ['m_x', 'm_y']


class TestMemberRemoval:

    def _two_members(self, tmp_path):
        return _class_session(tmp_path, members=[
            _member('m_x', attributes={'software': ['x']}),
            _member('m_y', attributes={'software': ['y']})])

    def test_readded_member_starts_with_clean_policy_state(self, tmp_path):
        """A removed-then-re-added member id must not inherit backoff."""
        plugin, client, sid = self._two_members(tmp_path)
        pol = _pool(plugin, sid, 'fed').policy
        pol._last_submit_ts['m_x']       = 1e12
        pol._consecutive_failures['m_x'] = 5
        pol._backoff_until['m_x']        = 1e12
        pol._backoff_logged['m_x']       = True
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
            json={})
        assert r.status_code == 200, r.text
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', attributes={'software': ['x']}))
        assert r.status_code == 200, r.text
        for d in (pol._last_submit_ts, pol._consecutive_failures,
                  pol._backoff_until, pol._backoff_logged):
            assert 'm_x' not in d
        assert not pol._in_backoff('m_x', 0.0)

    def test_last_member_without_force_is_409(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
            json={})
        assert r.status_code == 409
        assert 'force' in r.json()['detail']

    def test_last_member_with_force_empties_the_pool(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
            json={'force': True})
        assert r.status_code == 200, r.text
        assert _pool(plugin, sid, 'fed').config.members == {}

    def test_unknown_member_is_404(self, tmp_path):
        plugin, client, sid = self._two_members(tmp_path)
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/nope',
            json={})
        assert r.status_code == 404

    def _pilot_with_task(self, plugin, sid, mid, req=None):
        """One ACTIVE *submitted* pilot of member *mid*, running one task.

        ``psij_job_id`` marks it a batch job rather than an adopted
        endpoint: cancelling one is a failure, releasing the other is not
        (plan 122)."""
        ps = _pool(plugin, sid, 'fed')
        pilot = PilotRecord(
            pid=f'p.{mid}', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id=mid, endpoint_name=f'ep_{mid}', psij_job_id=f'j.{mid}',
            attributes={'software': [mid[-1]]},
            nodes=1, cpus_per_node=4,
            child_endpoint_name=f'fed_{mid}_p.{mid}',
            capacity=4, in_flight=1)
        ps.pilots[pilot.pid] = pilot
        task = TaskRecord(
            task_id=f't.{mid}', pool='fed', owning_sid=sid,
            cmd=['/bin/echo'], cwd='/tmp', state=TASK_RUNNING,
            pilot_id=pilot.pid, member_id=mid,
            requirements=req or {})
        ps.tasks[task.task_id] = task
        return ps, pilot, task

    def test_removal_cancels_pilots_and_requeues_tasks(self, tmp_path):
        plugin, client, sid = self._two_members(tmp_path)
        ps, pilot, task = self._pilot_with_task(plugin, sid, 'm_x')
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        assert r.status_code == 200, r.text
        assert r.json()['pilots_cancelled'] == 1
        assert r.json()['tasks_requeued'] == 1
        assert pilot.state == PILOT_FAILED
        assert task.state == TASK_QUEUED
        assert task.requeues == 1
        assert task.pilot_id is None
        assert task.member_id is None       # cleared with the pilot_id
        assert 'm_x' not in ps.config.members

    def test_requeue_cap_failure_is_counted(self, tmp_path):
        """A task the pilot cancel fails (``_finalize_pilot``: re-queued
        too often) is in ``tasks_failed``, not ``tasks_requeued``."""
        plugin, client, sid = self._two_members(tmp_path)
        ps, _, task = self._pilot_with_task(plugin, sid, 'm_x')
        task.requeues = ps.policy.max_requeues
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        assert r.status_code == 200, r.text
        assert task.state == TASK_FAILED
        assert 'requeued too often' in task.error
        assert r.json()['tasks_failed']   == 1
        assert r.json()['tasks_requeued'] == 0

    def test_pilots_are_paused_before_the_member_is_dropped(self, tmp_path):
        """No other path may dispatch onto a pilot that is about to die."""
        plugin, client, sid = self._two_members(tmp_path)
        _, pilot, _ = self._pilot_with_task(plugin, sid, 'm_x')
        seen = {}

        async def _cancel(ps, rec):
            seen['accepting'] = rec.accepting_new_tasks
            seen['declared']  = 'm_x' in ps.config.members

        with patch.object(plugin, '_do_pilot_cancel', new=_cancel):
            client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        assert seen == {'accepting': False, 'declared': False}

    def test_unsatisfiable_sweep_fails_orphaned_tasks(self, tmp_path):
        plugin, client, sid = self._two_members(tmp_path)
        ps, _, task = self._pilot_with_task(
            plugin, sid, 'm_x', req={'software': ['x']})
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        assert task.state == TASK_FAILED
        assert task.error == \
            'no member satisfies the task requirements: software missing: x'
        assert r.json()['tasks_failed'] == 1

    def test_fail_unsatisfiable_false_keeps_them_queued(self, tmp_path):
        """The federation's liveness path: this member may come back."""
        plugin, client, sid = self._two_members(tmp_path)
        _, _, task = self._pilot_with_task(
            plugin, sid, 'm_x', req={'software': ['x']})
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={'fail_unsatisfiable': False})
        assert task.state == TASK_QUEUED

    def test_cancel_tasks_fails_everything_it_was_running(self, tmp_path):
        plugin, client, sid = self._two_members(tmp_path)
        _, _, task = self._pilot_with_task(plugin, sid, 'm_x')
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={'cancel_tasks': True})
        assert task.state == TASK_FAILED
        assert task.error == 'member removed with cancel_tasks'

    def test_removal_drains_without_waiting_for_a_tick(self, tmp_path):
        plugin, client, sid = self._two_members(tmp_path)
        ps, _, task = self._pilot_with_task(plugin, sid, 'm_x')
        # a sibling member's ACTIVE pilot that can serve the task
        sibling = PilotRecord(
            pid='p.sib', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_y', endpoint_name='ep_m_y',
            attributes={'software': ['y']}, nodes=1, cpus_per_node=4,
            child_endpoint_name='fed_m_y_p.sib', capacity=4)
        ps.pilots['p.sib'] = sibling
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)), \
             patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=MagicMock())):
            client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        assert task.state == TASK_RUNNING
        assert task.pilot_id == 'p.sib'
        assert task.member_id == 'm_y'


class TestMemberAwareSubmitValidation:

    def test_no_member_satisfies_software(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp', 'requirements': {'software': ['lammps']}})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'no member satisfies the task requirements: '
            'software missing: lammps')

    def test_gpus_exceed_every_member(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x'),
            _member('m_y', pilot_sizes={'d': {
                'nodes': 1, 'cpus_per_node': 4, 'gpus_per_node': 1,
                'rhapsody_backend': 'concurrent'}})])
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp', 'requirements': {'gpus': 2}})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'no member satisfies the task requirements: gpus 0 < 2')

    def test_only_a_non_default_size_fits_is_a_400(self, tmp_path):
        """The policy only grows a member's default size, so a task that
        fits nothing but a larger, non-default size would queue forever."""
        sizes = {'d'  : {'nodes': 1, 'cpus_per_node': 4,
                         'rhapsody_backend': 'concurrent'},
                 'big': {'nodes': 1, 'cpus_per_node': 64,
                         'rhapsody_backend': 'concurrent'}}
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', pilot_sizes=sizes)])
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'requirements': {'cores': 32}})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'no member satisfies the task requirements: cores 4 < 32')
        assert 't.1' not in _pool(plugin, sid, 'fed').tasks

    def test_gate_and_removal_sweep_agree(self, tmp_path):
        """Whatever the submit gate accepts, removing an unrelated member
        must leave QUEUED -- the sweep asks the gate's own question."""
        sizes = {'d'  : {'nodes': 1, 'cpus_per_node': 4,
                         'rhapsody_backend': 'concurrent'},
                 'big': {'nodes': 1, 'cpus_per_node': 64,
                         'rhapsody_backend': 'concurrent'}}
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', pilot_sizes=sizes),
            _member('m_y', pilot_sizes={'d': {
                'nodes': 1, 'cpus_per_node': 16,
                'rhapsody_backend': 'concurrent'}}),
            _member('m_z')])
        ps = _pool(plugin, sid, 'fed')
        accepted = []
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            for cores in (2, 8, 16, 32, 64):
                tid = f't.{cores}'
                r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                    'pool': 'fed', 'task_id': tid, 'cmd': ['/bin/echo'],
                    'requirements': {'cores': cores}})
                if r.status_code == 200:
                    accepted.append(tid)
        assert accepted == ['t.2', 't.8', 't.16']
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_z',
                json={})
        assert r.status_code == 200, r.text
        assert r.json()['tasks_failed'] == 0
        assert all(ps.tasks[t].state == TASK_QUEUED for t in accepted)

    def test_legacy_pool_keeps_the_unqualified_size_name(self, tmp_path):
        """120's exact strings survive for a single-site pool."""
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A',
                                lifetime='persistent')
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'cpu', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp', 'requirements': {'gpus': 2}})
        assert r.json()['detail'] == (
            "requirements: 2 gpus exceed every pilot_size "
            "(largest: 's', 0 gpus/node)")

    def test_rhapsody_dialect_is_validated_too(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'uid': 't.1', 'pool': 'fed', 'cwd': '/tmp',
                       'requirements': {'software': ['lammps']}}]})
        assert r.status_code == 400
        assert 'no member satisfies' in r.json()['detail']

    def test_matching_member_queues_the_task(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', attributes={'software': ['x']}),
            _member('m_y', attributes={'software': ['y']})])
        ps = _pool(plugin, sid, 'fed')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
                'requirements': {'software': ['y']}})
        assert r.status_code == 200, r.text
        assert ps.tasks['t.1'].state == TASK_QUEUED

    def test_mpi_400s_only_when_every_member_is_dragon_v1(self, tmp_path):
        v1 = {'d': {'nodes': 1, 'cpus_per_node': 4,
                    'rhapsody_backend': 'dragon_v1'}}
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', pilot_sizes=v1),
            _member('m_y', pilot_sizes=v1)])
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp', 'requirements': {'mpi': True}})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'no member satisfies the task requirements: '
            'backend dragon_v1 cannot run an mpi task')

    def test_mpi_accepted_when_one_member_can(self, tmp_path):
        v1 = {'d': {'nodes': 1, 'cpus_per_node': 4,
                    'rhapsody_backend': 'dragon_v1'}}
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', pilot_sizes=v1), _member('m_y')])
        ps = _pool(plugin, sid, 'fed')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
                'requirements': {'mpi': True}})
        assert r.status_code == 200, r.text


class TestCwdAtDispatch:

    def test_cwd_optional_for_a_class_pool(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        assert r.status_code == 200, r.text
        assert ps.tasks['t.1'].cwd == ''
        assert ps.tasks['t.1'].cwd_assigned is True

    def test_cwd_still_required_for_a_legacy_pool(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A',
                                lifetime='persistent')
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'cpu', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        assert r.status_code == 400
        assert r.json()['detail'] == "submit requires 'task_id', 'cmd', 'cwd'"

    def test_rhapsody_dialect_still_requires_a_cwd(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'uid': 't.1', 'pool': 'fed'}]})
        assert r.status_code == 400
        assert 'cwd' in r.json()['detail']

    def test_explicit_cwd_refused_with_a_non_shared_member(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False, scratch_base='/remote')])
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp'})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'explicit cwd is not valid for a pool with non-shared members')

    def _dispatch(self, plugin, ps, task, pilot):
        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=[(task, pilot), None]), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=MagicMock())), \
                 patch.object(plugin, '_get_staging_client',
                              new=AsyncMock(return_value=MagicMock())):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())

    def _active(self, ps, sid, mid='m_x', pid='p.1'):
        pilot = PilotRecord(
            pid=pid, pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id=mid, endpoint_name=f'ep_{mid}',
            attributes={'software': ['x']}, nodes=1, cpus_per_node=4,
            child_endpoint_name=f'fed_{mid}_{pid}', capacity=4)
        ps.pilots[pid] = pilot
        return pilot

    def test_cwd_assigned_under_the_members_scratch(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(tmp_path / 'member_x'))])
        ps    = _pool(plugin, sid, 'fed')
        pilot = self._active(ps, sid)
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        self._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        task = ps.tasks['t.1']
        assert task.cwd == str(tmp_path / 'member_x' / 't.1')
        assert Path(task.cwd).is_dir()      # shared_fs → broker mkdirs it
        assert task.member_id == 'm_x'

    def test_unwritable_scratch_fails_the_task(self, tmp_path):
        """A shared member whose scratch_base cannot be created (here: a
        path through a regular file, which fails even as root) fails the
        task at claim time instead of launching it into a missing cwd."""
        (tmp_path / 'blocker').write_text('')
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(tmp_path / 'blocker' / 's'))])
        ps    = _pool(plugin, sid, 'fed')
        pilot = self._active(ps, sid)
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        self._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        task = ps.tasks['t.1']
        assert task.state == TASK_FAILED
        assert task.error.startswith(
            f"could not create task cwd {tmp_path / 'blocker' / 's' / 't.1'}")
        assert pilot.in_flight == 0

    def test_nothing_created_locally_for_a_non_shared_member(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False,
                    scratch_base=str(tmp_path / 'remote'))])
        ps    = _pool(plugin, sid, 'fed')
        pilot = self._active(ps, sid)
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        self._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        assert ps.tasks['t.1'].cwd == str(tmp_path / 'remote' / 't.1')
        assert not (tmp_path / 'remote').exists()

    def test_redispatch_reassigns_only_an_assigned_cwd(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(tmp_path / 'x')),
            _member('m_y', scratch_base=str(tmp_path / 'y'),
                    attributes={'software': ['x']})])
        ps = _pool(plugin, sid, 'fed')
        px = self._active(ps, sid, 'm_x', 'p.x')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        self._dispatch(plugin, ps, ps.tasks['t.1'], px)
        assert ps.tasks['t.1'].cwd == str(tmp_path / 'x' / 't.1')

        # the TestClient's loop is closed by now; notifications are not
        # what this test is about
        plugin._dispatch_notify = lambda t, d: None
        plugin._mark_pilot_failed(ps, px, 'lost')
        py = self._active(ps, sid, 'm_y', 'p.y')
        self._dispatch(plugin, ps, ps.tasks['t.1'], py)
        assert ps.tasks['t.1'].cwd == str(tmp_path / 'y' / 't.1')

    def test_client_supplied_cwd_is_left_alone(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps    = _pool(plugin, sid, 'fed')
        pilot = self._active(ps, sid)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=['/bin/echo'],
            cwd='/client/path', cwd_assigned=False, state=TASK_QUEUED)
        self._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        assert ps.tasks['t.1'].cwd == '/client/path'


class TestInputsB64:

    def _b64(self, data: bytes) -> str:
        return base64.b64encode(data).decode('ascii')

    def _submit(self, client, plugin, sid, inputs_b64, **extra):
        ps = _pool(plugin, sid, 'fed')
        body = {'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
                'inputs_b64': inputs_b64}
        body.update(extra)
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            return client.post(f'{plugin.namespace}/submit/{sid}', json=body)

    def test_spools_into_the_state_dir_and_records_names(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        r = self._submit(client, plugin, sid,
                         {'md.json': self._b64(b'{"a":1}')})
        assert r.status_code == 200, r.text
        rec = ps.tasks['t.1']
        assert rec.spooled == ['md.json']
        assert rec.inputs  == []            # NOT the client-declared list
        assert (ps.spool_dir('t.1') / 'md.json').read_bytes() == b'{"a":1}'
        assert ps.spool_dir('t.1').is_relative_to(ps.state_dir)

    def test_bad_base64_is_400(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = self._submit(client, plugin, sid, {'md.json': 'not base64!!'})
        assert r.status_code == 400
        assert r.json()['detail'].startswith('invalid base64:')

    def test_bad_filename_is_400(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = self._submit(client, plugin, sid, {'../evil': self._b64(b'x')})
        assert r.status_code == 400

    def test_oversize_file_is_413(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        big = self._b64(b'x' * (2 * 1024 * 1024 + 1))
        r = self._submit(client, plugin, sid, {'big.bin': big})
        assert r.status_code == 413
        assert 'per file' in r.json()['detail']

    def test_oversize_block_is_413(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        chunk = self._b64(b'x' * (2 * 1024 * 1024))
        r = self._submit(client, plugin, sid,
                         {f'f{i}.bin': chunk for i in range(5)})
        assert r.status_code == 413
        assert 'in total' in r.json()['detail']

    def test_endpoint_mode_is_400(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _register(client, plugin, body={'sid': 'A'})
        plugin._connected_endpoints['gpu1'] = {'rhapsody'}
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'endpoint': 'gpu1', 'task_id': 't.1', 'cmd': ['/bin/echo'],
            'cwd': '/tmp', 'inputs_b64': {'a.txt': self._b64(b'x')}})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'inputs_b64 is only supported for pool-mode exec tasks')

    def test_rhapsody_dialect_is_400(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'uid': 't.1', 'pool': 'fed', 'cwd': '/tmp',
                       'inputs_b64': {'a.txt': self._b64(b'x')}}]})
        assert r.status_code == 400
        assert r.json()['detail'] == (
            'inputs_b64 is only supported for pool-mode exec tasks')

    def _active(self, ps, sid, mid='m_x'):
        pilot = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id=mid, endpoint_name=f'ep_{mid}',
            attributes={'software': ['x']}, nodes=1, cpus_per_node=4,
            child_endpoint_name=f'fed_{mid}_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        return pilot

    def test_copied_into_a_shared_fs_cwd(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(tmp_path / 'shared'))])
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        pilot = self._active(ps, sid)
        TestCwdAtDispatch()._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        assert (Path(ps.tasks['t.1'].cwd) / 'md.json').read_bytes() == b'hi'

    def test_put_to_a_non_shared_member_before_submit(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False,
                    scratch_base=str(tmp_path / 'remote'))])
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        pilot = self._active(ps, sid)

        order, stg, rh = [], MagicMock(), MagicMock()
        stg.put = MagicMock(side_effect=lambda *a, **k: order.append(('put',) + a))
        rh.submit_tasks = MagicMock(
            side_effect=lambda *a, **k: order.append(('submit',)) or [])

        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=[(ps.tasks['t.1'], pilot), None]), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=rh)), \
                 patch.object(plugin, '_get_staging_client',
                              new=AsyncMock(return_value=stg)):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())

        cwd = ps.tasks['t.1'].cwd
        assert order[0] == ('put', str(ps.spool_dir('t.1') / 'md.json'),
                            str(Path(cwd) / 'md.json'), True)
        assert order[1] == ('submit',)
        # nothing created under the member's scratch on the broker host
        assert not (tmp_path / 'remote').exists()

    def test_input_less_task_gets_a_cwd_marker(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False,
                    scratch_base=str(tmp_path / 'remote'))])
        ps = _pool(plugin, sid, 'fed')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        pilot = self._active(ps, sid)
        stg = MagicMock()
        stg.put = MagicMock(return_value={})

        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=[(ps.tasks['t.1'], pilot), None]), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=MagicMock())), \
                 patch.object(plugin, '_get_staging_client',
                              new=AsyncMock(return_value=stg)):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())
        assert stg.put.call_args.args[1].endswith('/.orbit-cwd')

    def test_placement_failure_fails_the_task(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False, scratch_base='/remote')])
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        pilot = self._active(ps, sid)
        stg = MagicMock()
        stg.put = MagicMock(side_effect=RuntimeError('boom'))
        rh = MagicMock()

        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=[(ps.tasks['t.1'], pilot), None]), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=rh)), \
                 patch.object(plugin, '_get_staging_client',
                              new=AsyncMock(return_value=stg)):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())
        assert ps.tasks['t.1'].state == TASK_FAILED
        assert ps.tasks['t.1'].error.startswith(
            'could not place inputs on the pilot:')
        rh.submit_tasks.assert_not_called()

    def test_spool_dropped_on_a_terminal_state(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        assert ps.spool_dir('t.1').exists()
        asyncio.run(plugin._cancel_task(ps, ps.tasks['t.1']))
        assert not ps.spool_dir('t.1').exists()

    def test_spool_survives_a_requeue(self, tmp_path):
        """The task is about to be dispatched somewhere else."""
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        pilot = self._active(ps, sid)
        ps.tasks['t.1'].state    = TASK_RUNNING
        ps.tasks['t.1'].pilot_id = 'p.1'
        plugin._dispatch_notify = lambda t, d: None
        plugin._mark_pilot_failed(ps, pilot, 'lost')
        assert ps.tasks['t.1'].state == TASK_QUEUED
        assert ps.spool_dir('t.1').exists()

    def test_close_drops_the_whole_spool(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        self._submit(client, plugin, sid, {'md.json': self._b64(b'hi')})
        ps.close()
        assert not (ps.state_dir / 'inputs').exists()


class TestRequeueCap:

    def test_second_pilot_loss_fails_the_task(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        task = TaskRecord(task_id='t.1', pool='fed', owning_sid=sid,
                          cmd=['/bin/echo'], cwd='/tmp',
                          state=TASK_RUNNING, pilot_id='p.1')
        ps.tasks['t.1'] = task
        plugin._dispatch_notify = lambda t, d: None
        for i in (1, 2):
            pilot = PilotRecord(pid='p.1', pool='fed', owning_sid=sid,
                                size_key='d', rhapsody_backend='concurrent',
                                state=PILOT_ACTIVE, member_id='m_x')
            ps.pilots['p.1'] = pilot
            task.state    = TASK_RUNNING
            task.pilot_id = 'p.1'
            plugin._mark_pilot_failed(ps, pilot, 'lost')
            assert task.requeues == i
        assert task.state == TASK_FAILED
        assert task.error == 'requeued too often (pilot lost)'


class TestStagingRefusals:

    def test_stage_in_without_a_record_is_unchanged(self, tmp_path):
        """The route has always staged into the pool scratch for any id."""
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/stage_in/{sid}/t.404', json={
            'pool': 'fed', 'filename': 'in.txt',
            'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 200, r.text

    def test_stage_in_for_an_unplaced_task_is_409(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        r = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json={
            'pool': 'fed', 'filename': 'in.txt',
            'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 409
        assert r.json()['detail'] == 'task not yet placed'

    def test_stage_out_for_a_non_shared_member_is_409(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False, scratch_base='/remote')])
        ps = _pool(plugin, sid, 'fed')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=[],
            cwd='/remote/t.1', member_id='m_x', state=TASK_DONE)
        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/out.txt')
        assert r.status_code == 409
        assert 'not broker-local' in r.json()['detail']

    def test_stage_out_reads_the_placed_cwd(self, tmp_path):
        """A placed shared-fs task's output lives in its assigned cwd, not
        in ``pool scratch / task_id``."""
        site = tmp_path / 'site'
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(site))])
        ps  = _pool(plugin, sid, 'fed')
        cwd = site / 't.1'
        cwd.mkdir(parents=True)
        (cwd / 'out.txt').write_bytes(b'placed')
        decoy = ps.scratch_base / 't.1'
        decoy.mkdir(parents=True)
        (decoy / 'out.txt').write_bytes(b'decoy')
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=[],
            cwd=str(cwd), member_id='m_x', state=TASK_DONE)
        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/out.txt')
        assert r.status_code == 200, r.text
        assert base64.b64decode(r.json()['content_b64']) == b'placed'


class TestClassPoolScratchAndDirs:

    def test_pool_scratch_is_broker_local(self, tmp_path):
        """The primary member's scratch_base is a path on someone else's
        filesystem; PoolState.__init__ mkdirs its own."""
        plugin, _, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base='/gpfs/nowhere')])
        ps = _pool(plugin, sid, 'fed')
        assert ps.scratch_base == tmp_path / 'scratch' / 'fed'
        assert ps.scratch_base.is_dir()

    def test_state_dir_does_not_move_with_the_primary_member(self, tmp_path):
        plugin, _, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        assert ps.state_dir.name == 'fed__members'

    def test_legacy_state_dir_is_unchanged(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A')
        assert _pool(plugin, sid, 'cpu').state_dir.name == 'cpu__endpoint0'


class TestClassPoolSummary:

    def test_non_verbose_gains_class_fields(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', max_pilots=2), _member('m_y', max_pilots=3)])
        s = plugin._summarize_pool(_pool(plugin, sid, 'fed'))
        assert s['pool_class']       == 'gpu'
        assert s['multi_member']     is True
        assert s['member_ids']       == ['m_x', 'm_y']
        assert s['max_pilots_total'] == 5
        assert 'members' not in s        # objects only in the verbose view

    def test_legacy_summary_reports_one_implicit_member(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A')
        s = plugin._summarize_pool(_pool(plugin, sid, 'cpu'))
        assert s['multi_member']     is False
        assert s['pool_class']       == ''
        # the implicit member is internal: it never reaches the wire
        assert s['member_ids']       == []
        assert s['max_pilots_total'] == 4
        assert s['queue'] == 'batch' and s['max_pilots'] == 4
        v = plugin._summarize_pool(_pool(plugin, sid, 'cpu'), verbose=True)
        assert v['members'] == []
        assert 'pilot_history' in v and 'node_hours_used' in v

    def test_verbose_member_block(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', budget={'node_hours': 10.0},
                    attributes={'site': 'NERSC', 'software': ['x']})])
        ps  = _pool(plugin, sid, 'fed')
        now = time.time()
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_x', endpoint_name='ep_m_x', nodes=2,
            cpus_per_node=4, submitted_at=now - 3600,
            active_at=now - 3600, finished_at=now)
        s = plugin._summarize_pool(ps, verbose=True)
        m = s['members'][0]
        assert m['member_id']     == 'm_x'
        assert m['endpoint_name'] == 'ep_m_x'
        assert m['queue']         == 'regular'
        assert m['attributes']    == {'site': 'NERSC', 'software': ['x']}
        assert m['budget']        == {'node_hours': 10.0}
        assert m['shared_fs']     is True
        assert m['live_pilots']   == 1
        assert m['pilots_active'] == 1
        assert m['default_size']  == 'd'
        assert set(m['pilot_sizes']) == {'d'}
        assert m['node_hours_used'] == pytest.approx(2.0, abs=0.01)
        assert m['node_hours_remaining'] == pytest.approx(8.0, abs=0.01)
        assert [p['pid'] for p in m['pilot_history']] == ['p.1']
        assert s['node_hours_used'] == pytest.approx(2.0, abs=0.01)

    def test_no_budget_reports_null_remaining(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        s = plugin._summarize_pool(_pool(plugin, sid, 'fed'), verbose=True)
        assert s['members'][0]['node_hours_remaining'] is None

    def test_pool_total_includes_a_departed_members_pilots(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x'), _member('m_y')])
        ps  = _pool(plugin, sid, 'fed')
        now = time.time()
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_DONE,
            member_id='m_x', nodes=1, cpus_per_node=4,
            submitted_at=now - 3600, active_at=now - 3600, finished_at=now)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={})
        s = plugin._summarize_pool(ps, verbose=True)
        assert [m['member_id'] for m in s['members']] == ['m_y']
        assert sum(m['node_hours_used'] for m in s['members']) == 0.0
        assert s['node_hours_used'] == pytest.approx(1.0, abs=0.01)


class TestPilotFailureIsVisible:
    """A member whose every submit fails must not read as a healthy idle one.

    The demo case: psij answered ``[Errno 122] Disk quota exceeded`` for half
    an hour, the pilots went FAILED, and every summary showed the member with
    0 pilots and nothing else — the reason lived only in the broker log.
    """

    QUOTA = 'OSError: [Errno 122] Disk quota exceeded'

    def _submit(self, plugin, ps, pid, exc):
        size   = ps.config.member('m_x').pilot_sizes['d']
        record = PilotRecord(
            pid=pid, pool='fed', owning_sid='A', size_key='d',
            rhapsody_backend='concurrent', state=PILOT_PENDING,
            member_id='m_x', submitted_at=time.time(),
            endpoint_name=ps.config.member('m_x').endpoint_name)
        ps.pilots[pid] = record
        psij_mock = MagicMock()
        psij_mock.submit_tunneled = MagicMock(side_effect=exc)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(
                ps, record, size, ps.config.member('m_x')))
        return record

    def test_a_failed_submit_is_kept_on_the_pilot_record(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        ps     = _pool(plugin, sid, 'fed')
        record = self._submit(plugin, ps, 'p.1', OSError(self.QUOTA))

        assert record.state == PILOT_FAILED
        assert 'psij error' in record.error
        assert 'Disk quota exceeded' in record.error
        # and it travels in the history, which is what the summaries carry
        entry = ps.pilot_history('m_x')[0]
        assert entry.error == record.error

    def test_the_error_is_truncated(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        ps     = _pool(plugin, sid, 'fed')
        record = self._submit(plugin, ps, 'p.1', OSError('x' * 5000))
        assert len(record.error) == 300

    def test_a_done_pilot_carries_no_error(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        rec = PilotRecord(pid='p.9', pool='fed', owning_sid=sid,
                          size_key='d', rhapsody_backend='concurrent',
                          state=PILOT_ACTIVE, member_id='m_x')
        ps.pilots['p.9'] = rec
        plugin._mark_pilot_done(ps, rec, 'walltime reached')
        assert rec.state == PILOT_DONE
        assert rec.error is None

    def test_the_member_block_reports_the_error_and_the_pause(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        for n in range(3):
            self._submit(plugin, ps, 'p.%d' % n, OSError(self.QUOTA))

        m = plugin._summarize_pool(ps, verbose=True)['members'][0]
        assert m['member_id'] == 'm_x'
        assert m['live_pilots'] == 0            # what used to be the whole
        assert 'Disk quota exceeded' in m['last_pilot_error']
        assert m['consecutive_pilot_failures'] == 3
        # the conservative policy's backoff, read off the policy itself
        assert m['paused_until'] > time.time()

    def test_a_healthy_member_reports_nothing_held_against_it(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        m = plugin._summarize_pool(_pool(plugin, sid, 'fed'),
                                   verbose=True)['members'][0]
        assert m['last_pilot_error']           is None
        assert m['consecutive_pilot_failures'] == 0
        assert m['paused_until']               is None

    def test_the_newest_failure_wins(self, tmp_path):
        plugin, _client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        self._submit(plugin, ps, 'p.0', OSError('older failure'))
        self._submit(plugin, ps, 'p.1', OSError(self.QUOTA))
        m = plugin._summarize_pool(ps, verbose=True)['members'][0]
        assert 'Disk quota exceeded' in m['last_pilot_error']

    def test_a_policy_without_member_health_reports_a_healthy_member(
            self, tmp_path):
        from radical.orbit.task_dispatcher_policy import DispatchPolicy
        plugin, _client, sid = _class_session(tmp_path)
        ps = _pool(plugin, sid, 'fed')
        with patch.object(type(ps.policy), 'member_health',
                          DispatchPolicy.member_health):
            m = plugin._summarize_pool(ps, verbose=True)['members'][0]
        assert m['consecutive_pilot_failures'] == 0
        assert m['paused_until'] is None


# ---------------------------------------------------------------------------
# Review round 2 (plan 121 Implementation notes)
# ---------------------------------------------------------------------------

class TestRhapsodyDialectOnNonSharedMember:
    """A dialect task's cwd is opaque: the dispatcher neither rewrites it
    nor writes markers into it, and its own staging routes fall back to
    legacy stage-by-id behaviour."""

    def _setup(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False, scratch_base='/remote')])
        ps    = _pool(plugin, sid, 'fed')
        pilot = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_x', endpoint_name='ep_m_x',
            attributes={'software': ['x']}, nodes=1, cpus_per_node=4,
            child_endpoint_name='fed_m_x_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        return plugin, client, sid, ps, pilot

    def test_no_cwd_marker_is_put_for_a_dialect_task(self, tmp_path):
        plugin, client, sid, ps, pilot = self._setup(tmp_path)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=[], cwd='',
            task_dict={'uid': 't.1', 'cwd': '/client/owned'},
            state=TASK_QUEUED)
        stg = MagicMock()
        stg.put = MagicMock(return_value={})

        async def drive():
            with patch.object(ps.policy, 'pick_dispatch',
                              side_effect=[(ps.tasks['t.1'], pilot), None]), \
                 patch.object(plugin, '_get_rhapsody_client',
                              new=AsyncMock(return_value=MagicMock())), \
                 patch.object(plugin, '_get_staging_client',
                              new=AsyncMock(return_value=stg)):
                plugin._drain_pending(ps)
                await asyncio.sleep(0.05)
        asyncio.run(drive())
        stg.put.assert_not_called()
        assert ps.tasks['t.1'].state == TASK_RUNNING

    def test_stage_in_for_a_dialect_task_is_legacy(self, tmp_path):
        """Its own ``cwd`` field is always '' -- applying "not yet placed"
        would 409 every dialect stage_in, a legacy behaviour change."""
        plugin, client, sid, ps, _ = self._setup(tmp_path)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=[], cwd='',
            member_id='m_x', task_dict={'uid': 't.1'}, state=TASK_QUEUED)
        r = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json={
            'pool': 'fed', 'filename': 'in.txt',
            'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 200, r.text

    def test_dialect_stage_in_lands_in_the_pool_scratch(self, tmp_path):
        """An empty record cwd must fall back to the pool scratch, never
        resolve to the broker's working directory."""
        plugin, client, sid, ps, _ = self._setup(tmp_path)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=[], cwd='',
            member_id='m_x', task_dict={'uid': 't.1'}, state=TASK_QUEUED)
        r = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json={
            'pool': 'fed', 'filename': 'in.txt',
            'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 200, r.text
        assert Path(r.json()['cwd']) == ps.scratch_base / 't.1'
        assert (ps.scratch_base / 't.1' / 'in.txt').read_bytes() == b'x'


class TestSharedFsCwdCreation:

    def test_explicit_cwd_is_created_before_the_copy(self, tmp_path):
        """A client-supplied cwd is only a promise; ``_claim`` did not
        create it, so ``_place_inputs`` must."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', scratch_base=str(tmp_path / 'shared'))])
        ps  = _pool(plugin, sid, 'fed')
        cwd = tmp_path / 'client' / 'owned'
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo'],
                'cwd': str(cwd),
                'inputs_b64': {'md.json':
                               base64.b64encode(b'hi').decode('ascii')}})
        assert r.status_code == 200, r.text
        assert not cwd.exists()

        pilot = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_x', endpoint_name='ep_m_x',
            attributes={'software': ['x']}, nodes=1, cpus_per_node=4,
            child_endpoint_name='fed_m_x_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        TestCwdAtDispatch()._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        assert (cwd / 'md.json').read_bytes() == b'hi'


class TestRemoteScratchIsNotExpandedLocally:

    def test_tilde_travels_untouched_to_a_non_shared_member(self, tmp_path):
        """'~' means the broker's home only when the broker shares the
        filesystem."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', shared_fs=False, scratch_base='~/site_scratch')])
        ps    = _pool(plugin, sid, 'fed')
        pilot = PilotRecord(
            pid='p.1', pool='fed', owning_sid=sid, size_key='d',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            member_id='m_x', endpoint_name='ep_m_x',
            attributes={'software': ['x']}, nodes=1, cpus_per_node=4,
            child_endpoint_name='fed_m_x_p.1', capacity=4)
        ps.pilots['p.1'] = pilot
        with patch.object(ps.policy, 'pick_dispatch', return_value=None):
            client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        TestCwdAtDispatch()._dispatch(plugin, ps, ps.tasks['t.1'], pilot)
        assert ps.tasks['t.1'].cwd == '~/site_scratch/t.1'


class TestCancelTasksOrdering:

    def test_cancelled_tasks_are_terminal_before_the_first_await(self,
                                                                tmp_path):
        """They must not be re-queued (and possibly re-dispatched to a
        sibling) between the cancel and the fail."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x'), _member('m_y')])
        removal = TestMemberRemoval()
        ps, _, task = removal._pilot_with_task(plugin, sid, 'm_x')
        seen = {}

        async def _cancel(pool_state, rec):
            seen['state'] = task.state

        with patch.object(plugin, '_do_pilot_cancel', new=_cancel):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
                json={'cancel_tasks': True})
        assert seen['state'] == TASK_FAILED
        assert r.json()['tasks_failed']   == 1
        assert r.json()['tasks_requeued'] == 0


class TestEmptiedPoolSubmit:

    def test_submit_to_a_member_less_pool_is_400(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
            json={'force': True})
        assert r.status_code == 200, r.text
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'fed', 'task_id': 't.1', 'cmd': ['/bin/echo']})
        assert r.status_code == 400
        assert r.json()['detail'] == "pool 'fed' has no members"

    def test_it_fires_without_a_requirements_block(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_x',
            json={'force': True})
        r = client.post(f'{plugin.namespace}/submit_rh/{sid}', json={
            'tasks': [{'uid': 't.1', 'pool': 'fed', 'cwd': '/tmp'}]})
        assert r.status_code == 400
        assert r.json()['detail'] == "pool 'fed' has no members"


class TestAddMemberFingerprint:

    def test_reordered_software_list_is_still_idempotent(self, tmp_path):
        """A federation rebuilding its declarations from a set must not get
        a 409 for a member that has not changed."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', attributes={'software': ['a', 'b'],
                                       'site': 'NERSC'})])
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x',
                                     attributes={'software': ['b', 'a'],
                                                 'site': 'NERSC'}))
        assert r.status_code == 200, r.text
        assert r.json()['created'] is False
        # the stored declaration is untouched (order preserved as declared)
        assert _pool(plugin, sid, 'fed').config.members['m_x'] \
            .attributes['software'] == ['a', 'b']

    def test_a_real_change_is_still_409(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', attributes={'software': ['a', 'b']})])
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x',
                                     attributes={'software': ['a', 'c']}))
        assert r.status_code == 409

    def test_an_identical_repost_says_it_updated_nothing(self, tmp_path):
        plugin, client, sid = _class_session(tmp_path)
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x'))
        assert r.status_code == 200, r.text
        assert (r.json()['created'], r.json()['updated']) == (False, False)

    def test_only_the_pilot_mode_differing_updates_in_place(self, tmp_path):
        """The upgrade path: a member replayed off a pre-122 state dir says
        `submit` while the federation re-POSTs it as `endpoint`.  A 409 there
        would leave the member detached until the state dir was wiped."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', endpoint_name='alloc_ep')])
        seen = []
        plugin._dispatch_notify = lambda t, d: seen.append((t, d))
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', endpoint_name='alloc_ep',
                                     pilot='endpoint'))
        assert r.status_code == 200, r.text
        assert (r.json()['created'], r.json()['updated']) == (False, True)
        # the Explorer learns of an update the way it learns of an add
        assert [(t, d['action']) for t, d in seen] == \
            [('pool_members', 'update')]

        member = _pool(plugin, sid, 'fed').config.members['m_x']
        assert member.pilot    == 'endpoint'
        # ... and the new mode brings its own pilot bounds with it
        assert (member.min_pilots, member.max_pilots) == (1, 1)
        # the update is durable: a reload sees the new declaration
        _, plugin2 = _make_plugin(tmp_path)
        assert _pool(plugin2, sid, 'fed').config.members['m_x'].pilot \
            == 'endpoint'

    def test_a_mode_switch_that_changes_a_bound_is_409(self, tmp_path):
        """endpoint -> submit keeps the forced 1/1 bounds: a declaration
        that also moves them is a redeclaration, not a mode switch."""
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x', pilot='endpoint')])
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', min_pilots=0, max_pilots=4))
        assert r.status_code == 409
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', min_pilots=1, max_pilots=1))
        assert r.status_code == 200, r.text
        assert r.json()['updated'] is True

    def test_a_pilot_mode_change_plus_a_real_change_is_still_409(self,
                                                                tmp_path):
        plugin, client, sid = _class_session(tmp_path, members=[
            _member('m_x')])
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_member('m_x', pilot='endpoint', queue='other'))
        assert r.status_code == 409
        assert _pool(plugin, sid, 'fed').config.members['m_x'].pilot \
            == 'submit'


# ---------------------------------------------------------------------------
# An endpoint inside an allocation IS the pilot (plan 122)
# ---------------------------------------------------------------------------

_ALLOC_EP = 'alloc_ep'


def _adopted_member(mid='m_a', **overrides):
    """A member whose endpoint runs inside its allocation."""
    return _member(mid, endpoint_name=_ALLOC_EP, pilot='endpoint',
                   **overrides)


class TestEndpointAdoption:

    def _session(self, tmp_path, connected=True, **mkw):
        plugin, client, sid = _class_session(
            tmp_path, members=[_adopted_member(**mkw)])
        plugin._dispatch_notify = lambda t, d: None
        if connected:
            asyncio.run(plugin.on_topology_change(_child_topo(_ALLOC_EP)))
        return plugin, client, sid, _pool(plugin, sid, 'fed')

    def _adopt(self, plugin, ps):
        """Adopt, with the psij path patched so a call to it would show."""
        submit = AsyncMock(return_value=None)
        with patch.object(plugin, '_do_pilot_submit', new=submit):
            pid = plugin._submit_pilot(ps, None, member_id='m_a')
        assert submit.await_count == 0, 'a psij job was submitted'
        return pid, ps.pilots[pid]

    def test_a_connected_endpoint_is_active_at_once(self, tmp_path):
        plugin, _, _, ps = self._session(tmp_path)
        pid, rec = self._adopt(plugin, ps)
        assert rec.state               == PILOT_ACTIVE
        assert rec.child_endpoint_name == _ALLOC_EP
        assert rec.endpoint_name       == _ALLOC_EP
        assert rec.psij_job_id         is None
        # capacity comes from the size snapshot, exactly as for a submitted
        # pilot -- adoption rides _activate_pilot, it does not bypass it
        assert rec.capacity  == 4
        assert rec.active_at is not None
        assert ps.live_pilots_for('m_a') == [rec]

    def test_the_declaration_forces_one_pilot(self, tmp_path):
        _, _, _, ps = self._session(tmp_path, min_pilots=0, max_pilots=4)
        member = ps.config.members['m_a']
        assert (member.min_pilots, member.max_pilots) == (1, 1)

    def test_an_absent_endpoint_waits_for_the_topology(self, tmp_path):
        plugin, _, _, ps = self._session(tmp_path, connected=False)
        _, rec = self._adopt(plugin, ps)
        assert rec.state == PILOT_PENDING

        asyncio.run(plugin.on_topology_change(_child_topo(_ALLOC_EP)))
        assert rec.state    == PILOT_ACTIVE
        assert rec.capacity == 4

    def test_a_pending_adoption_times_out_with_the_reason(self, tmp_path):
        """Left PENDING it would sit forever, counting against the
        strategy's in-flight guards -- there is no psij job to ask about."""
        from radical.orbit.plugin_task_dispatcher import _HANDSHAKE_TIMEOUT_SEC
        plugin, _, _, ps = self._session(tmp_path, connected=False)
        _, rec = self._adopt(plugin, ps)
        rec.submitted_at -= _HANDSHAKE_TIMEOUT_SEC + 1

        asyncio.run(plugin._reconcile_overdue_pilots(time.time()))
        assert rec.state == PILOT_FAILED
        assert rec.error == f'endpoint {_ALLOC_EP} not connected'

    def test_a_zero_capacity_adoption_fails_with_that_reason(self, tmp_path):
        """Nothing will ever bind it; the sweeper's "not connected" would
        be the wrong reason, and later."""
        plugin, _, _, ps = self._session(tmp_path)
        member = ps.config.members['m_a']
        member.pilot_sizes[member.default_size].cpus_per_node = 0
        _, rec = self._adopt(plugin, ps)
        assert rec.state == PILOT_FAILED
        assert 'zero capacity' in rec.error

    def test_a_pending_adoption_is_left_alone_before_the_timeout(self,
                                                                tmp_path):
        plugin, _, _, ps = self._session(tmp_path, connected=False)
        _, rec = self._adopt(plugin, ps)
        asyncio.run(plugin._reconcile_overdue_pilots(time.time()))
        assert rec.state == PILOT_PENDING

    def test_a_suspect_endpoint_is_not_activated_on_the_spot(self, tmp_path):
        """It is still in the topology but on its way out, so the record
        waits for the delivery that says `present` -- exactly as a submitted
        pilot's child does."""
        plugin, _, _, ps = self._session(tmp_path, connected=False)
        asyncio.run(plugin.on_topology_change(
            _child_topo(_ALLOC_EP, 'suspect')))
        _, rec = self._adopt(plugin, ps)
        assert rec.state == PILOT_PENDING

        asyncio.run(plugin.on_topology_change(_child_topo(_ALLOC_EP)))
        assert rec.state == PILOT_ACTIVE

    def test_an_endpoint_without_rhapsody_fails_at_once(self, tmp_path):
        """Every task dispatched to it would fail; say why, up front."""
        plugin, _, _, ps = self._session(tmp_path, connected=False)
        topo = _child_topo(_ALLOC_EP)
        topo[_ALLOC_EP]['plugins'] = {'sysinfo': {'namespace': '/sysinfo'}}
        asyncio.run(plugin.on_topology_change(topo))
        _, rec = self._adopt(plugin, ps)
        assert rec.state == PILOT_FAILED
        assert rec.error == f'endpoint {_ALLOC_EP} serves no rhapsody'

    def test_a_second_adoption_is_a_no_op(self, tmp_path):
        """There is one endpoint to adopt; a second record would bind a
        second pilot to the same child endpoint name."""
        plugin, _, _, ps = self._session(tmp_path)
        pid, _ = self._adopt(plugin, ps)
        again, _ = self._adopt(plugin, ps)
        assert again == pid
        assert list(ps.pilots) == [pid]

    def test_the_deadline_is_capped_by_the_allocation_end(self, tmp_path):
        """The member is re-declared with its join-time walltime on every
        re-attach, so only the absolute end keeps a re-adoption honest."""
        end = time.time() + 60
        plugin, _, _, ps = self._session(tmp_path, end_time=end)
        _, rec = self._adopt(plugin, ps)
        assert rec.walltime_deadline == pytest.approx(end, abs=1)

    def test_without_an_end_time_the_deadline_is_unknown(self, tmp_path):
        """Nothing ends an adopted endpoint at ``now + walltime_sec``, so
        that figure must not become its deadline."""
        plugin, _, _, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        assert rec.walltime_deadline == 0.0

    def test_without_an_end_time_tasks_flow_past_the_walltime(self,
                                                              tmp_path):
        """The size's walltime passing must not drop the endpoint from
        dispatch: it still holds the member's single pilot slot, so the
        pool would stall with every task queued."""
        plugin, _, sid, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=['/bin/echo'],
            cwd='/tmp', state=TASK_QUEUED)

        later = time.time() + 2 * 3600
        ps.policy._now = lambda: later
        pair = ps.policy.pick_dispatch(ps)
        assert pair is not None
        assert pair[1] is rec

    def test_a_suspect_endpoint_pauses_the_adopted_pilot(self, tmp_path):
        plugin, _, _, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        asyncio.run(plugin.on_topology_change(
            _child_topo(_ALLOC_EP, 'suspect')))
        assert rec.state               == PILOT_ACTIVE   # not demoted
        assert rec.accepting_new_tasks is False
        asyncio.run(plugin.on_topology_change(_child_topo(_ALLOC_EP)))
        assert rec.accepting_new_tasks is True

    def test_a_lost_endpoint_is_done_never_failed(self, tmp_path):
        """An allocation ending is not a pilot failure, whatever the
        deadline says -- and it must not feed the failure counter."""
        plugin, _, sid, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='fed', owning_sid=sid, cmd=['/bin/echo'],
            cwd='/tmp', state=TASK_RUNNING, pilot_id=rec.pid,
            member_id='m_a')

        asyncio.run(plugin.on_topology_change(
            _child_topo(_ALLOC_EP, 'lost')))
        assert rec.state         == PILOT_DONE
        assert rec.error         is None
        assert ps.tasks['t.1'].state    == TASK_QUEUED
        assert ps.tasks['t.1'].pilot_id is None
        assert ps.policy.member_health('m_a')[
            'consecutive_pilot_failures'] == 0

    def test_a_re_added_member_is_adopted_again(self, tmp_path):
        """Lost, dropped by the federation, re-added: a fresh adoption with
        a deadline capped by the allocation's own end."""
        plugin, client, sid, ps = self._session(tmp_path)
        first_pid, first = self._adopt(plugin, ps)
        asyncio.run(plugin.on_topology_change(
            _child_topo(_ALLOC_EP, 'lost')))
        assert first.state == PILOT_DONE

        # the endpoint comes back -- which is what makes the federation
        # re-add the member in the first place
        asyncio.run(plugin.on_topology_change(_child_topo(_ALLOC_EP)))
        end = time.time() + 90
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_a',
            json={'force': True})
        assert r.status_code == 200, r.text
        r = client.post(f'{plugin.namespace}/pool/{sid}/fed/members',
                        json=_adopted_member(end_time=end))
        assert r.status_code == 200, r.text

        pid, rec = self._adopt(plugin, ps)
        assert pid != first_pid
        assert rec.state == PILOT_ACTIVE
        assert rec.walltime_deadline == pytest.approx(end, abs=1)

    def test_removing_the_member_releases_the_endpoint(self, tmp_path):
        """The other exit: a leave, not a lost endpoint.  Same verdict, so
        the two hooks can fire in either order."""
        plugin, client, sid, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        r = client.request(
            'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_a',
            json={'force': True})
        assert r.status_code == 200, r.text
        assert r.json()['pilots_cancelled'] == 1
        assert rec.state == PILOT_DONE
        assert rec.error is None

    @pytest.mark.parametrize('cancel_tasks', [False, True])
    def test_releasing_the_endpoint_stops_its_tasks(self, tmp_path,
                                                    cancel_tasks):
        """The endpoint outlives the record and no psij cancel kills its
        tasks: they must be stopped on it, or a re-queued task runs twice
        (and a failed one keeps running)."""
        plugin, client, sid, ps = self._session(tmp_path)
        _, rec = self._adopt(plugin, ps)
        for i in (1, 2):
            ps.tasks[f't.{i}'] = TaskRecord(
                task_id=f't.{i}', pool='fed', owning_sid=sid,
                cmd=['/bin/echo'], cwd='/tmp', state=TASK_RUNNING,
                pilot_id=rec.pid, member_id='m_a', rhapsody_uid=f'rh.{i}')
            plugin._uid_to_task[f'rh.{i}'] = (sid, 'fed', f't.{i}')

        loop = None
        keys = []

        async def get_rh(name, backend=None):
            nonlocal loop
            loop = asyncio.get_running_loop()
            keys.append((name, backend))
            return rh_mock

        def cancel(uid):
            # rhapsody reports CANCELED over the tap, and the loop handles
            # it, before the next cancel call returns
            handled = threading.Event()

            def deliver():
                plugin._on_task_status({'uid': uid, 'state': 'CANCELED'})
                handled.set()

            loop.call_soon_threadsafe(deliver)
            assert handled.wait(5)

        rh_mock = MagicMock()
        rh_mock.cancel_task = MagicMock(side_effect=cancel)
        with patch.object(plugin, '_get_rhapsody_client', new=get_rh):
            r = client.request(
                'DELETE', f'{plugin.namespace}/pool/{sid}/fed/members/m_a',
                json={'force': True, 'fail_unsatisfiable': False,
                      'cancel_tasks': cancel_tasks})
        assert r.status_code == 200, r.text
        assert sorted(c.args[0] for c in rh_mock.cancel_task.call_args_list) \
            == ['rh.1', 'rh.2']
        rh_mock.cancel_all_tasks.assert_not_called()
        # the session the tasks were submitted on, not a backend-less one
        assert rec.rhapsody_backend
        assert keys == [(rec.child_endpoint_name, rec.rhapsody_backend)]
        assert rec.state == PILOT_DONE
        want = TASK_FAILED if cancel_tasks else TASK_QUEUED
        assert ps.tasks['t.1'].state == want
        assert ps.tasks['t.2'].state == want
        if not cancel_tasks:
            assert r.json()['tasks_requeued'] == 2

    def test_the_summary_reports_the_pilot_mode_and_the_runway(self,
                                                              tmp_path):
        end = time.time() + 600
        plugin, client, sid, ps = self._session(tmp_path, end_time=end)
        self._adopt(plugin, ps)
        r = client.get(f'{plugin.namespace}/pool/{sid}/fed')
        assert r.status_code == 200, r.text
        member = r.json()['members'][0]
        assert member['pilot']    == 'endpoint'
        assert member['end_time'] == pytest.approx(end, abs=1)
        assert member['remaining_sec'] == pytest.approx(600, abs=5)

    def test_the_summary_reports_no_runway_without_a_pilot(self, tmp_path):
        plugin, client, sid, _ = self._session(tmp_path)
        r = client.get(f'{plugin.namespace}/pool/{sid}/fed')
        assert r.json()['members'][0]['remaining_sec'] is None

    def test_a_submit_member_reports_the_longest_lived_pilot(self, tmp_path):
        """`remaining_sec` is the max over a member's live pilots -- the
        number the federation shows for a login-mode shape."""
        plugin, client, sid = _class_session(
            tmp_path, members=[_member('m_x')])
        ps  = _pool(plugin, sid, 'fed')
        now = time.time()
        for pid, left in (('p.1', 300), ('p.2', 900)):
            ps.pilots[pid] = PilotRecord(
                pid=pid, pool='fed', owning_sid=sid, size_key='d',
                rhapsody_backend='concurrent', state=PILOT_ACTIVE,
                psij_job_id='j.' + pid, member_id='m_x', nodes=1,
                cpus_per_node=4, submitted_at=now, active_at=now,
                child_endpoint_name=f'fed_m_x_{pid}',
                walltime_deadline=now + left)
        r = client.get(f'{plugin.namespace}/pool/{sid}/fed')
        member = r.json()['members'][0]
        assert member['pilot']    == 'submit'
        assert member['end_time'] is None
        assert member['remaining_sec'] == pytest.approx(900, abs=5)

        # a pilot past a mis-estimated deadline has nothing left rather than
        # owing time: the summary must never report a negative runway
        for pilot in ps.pilots.values():
            pilot.walltime_deadline = now - 300
        r = client.get(f'{plugin.namespace}/pool/{sid}/fed')
        assert r.json()['members'][0]['remaining_sec'] == 0.0

    def test_the_floor_adopts_a_declared_endpoint_member_on_tick(self,
                                                                tmp_path):
        """min_pilots is forced to 1, so the floor step does the adopting --
        no task has to arrive first."""
        plugin, _, _, ps = self._session(tmp_path)
        submit = AsyncMock(return_value=None)
        with patch.object(plugin, '_do_pilot_submit', new=submit):
            ps.policy.on_tick(ps, plugin._make_submit_pilot(ps))
            # a second tick must not adopt the same endpoint twice
            ps.policy.on_tick(ps, plugin._make_submit_pilot(ps))
        assert submit.await_count == 0
        assert len(ps.pilots) == 1
        rec = next(iter(ps.pilots.values()))
        assert (rec.state, rec.child_endpoint_name) == \
            (PILOT_ACTIVE, _ALLOC_EP)

    def test_an_adopted_active_pilot_survives_a_reload(self, tmp_path):
        """Replay keeps it ACTIVE with its absolute deadline, and
        `live_pilots_for` counts it -- so the floor does not double-adopt."""
        end = time.time() + 1800
        plugin, _, sid, ps = self._session(tmp_path, end_time=end)
        pid, rec = self._adopt(plugin, ps)
        active_at = rec.active_at
        ps.persist()

        _, plugin2 = _make_plugin(tmp_path)
        ps2 = _pool(plugin2, sid, 'fed')
        back = ps2.pilots[pid]
        assert back.state             == PILOT_ACTIVE
        assert back.active_at         == active_at
        assert back.psij_job_id       is None
        assert back.walltime_deadline == pytest.approx(end, abs=1)
        assert ps2.live_pilots_for('m_a') == [back]
        assert ps2.config.members['m_a'].pilot    == 'endpoint'
        assert ps2.config.members['m_a'].end_time == pytest.approx(end, abs=1)

        plugin2._dispatch_notify = lambda t, d: None
        plugin2._connected_endpoints = {_ALLOC_EP: {'rhapsody'}}
        submit = AsyncMock(return_value=None)
        with patch.object(plugin2, '_do_pilot_submit', new=submit):
            ps2.policy.on_tick(ps2, plugin2._make_submit_pilot(ps2))
        assert list(ps2.pilots) == [pid]
        assert submit.await_count == 0
        assert back.adopted is True     # the stamp survives the reload too


class TestInFlightSubmitIsNotAdoption:
    """``_do_pilot_submit`` pre-binds the child endpoint name *before*
    awaiting psij, so for the length of that submit a **submitted** record
    carries a child name and no ``psij_job_id`` — which is why adoption is an
    explicit stamp on the record and never inferred from those two.
    """

    def _pending(self, tmp_path, **kw):
        _, plugin = _make_plugin(tmp_path)
        plugin._dispatch_notify = lambda t, d: None
        plugin._materialise_pool('A', _make_pool_cfg())
        ps  = _pool(plugin, 'A', 'cpu')
        rec = PilotRecord(
            pid='p.1', pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_PENDING,
            submitted_at=time.time(), child_endpoint_name='cpu__p.1',
            walltime_deadline=time.time() + 3600,
            endpoint_name=ps.config.endpoint_name, **kw)
        ps.pilots['p.1'] = rec
        return plugin, ps, rec

    def test_a_pre_bound_record_without_a_job_id_is_not_adopted(self,
                                                               tmp_path):
        _, _, rec = self._pending(tmp_path)
        assert rec.adopted is False

    def test_the_handshake_sweep_does_not_fail_an_in_flight_submit(self,
                                                                  tmp_path):
        """It would read as "endpoint never connected" and kill a submission
        that is still perfectly alive."""
        from radical.orbit.plugin_task_dispatcher import _HANDSHAKE_TIMEOUT_SEC
        plugin, ps, rec = self._pending(tmp_path)
        rec.submitted_at -= _HANDSHAKE_TIMEOUT_SEC + 1
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            asyncio.run(plugin._reconcile_overdue_pilots(time.time()))
        assert rec.state == PILOT_PENDING
        assert rec.error is None

    def test_cancelling_an_in_flight_submit_fails_it(self, tmp_path):
        """DONE is the *adopted* verdict.  A submitted record may already
        have a batch job behind it, so it fails with a reason."""
        plugin, ps, rec = self._pending(tmp_path)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=None)):
            asyncio.run(plugin._do_pilot_cancel(ps, rec))
        assert rec.state == PILOT_FAILED
        assert rec.error == 'cancel requested'

    def test_a_late_submit_result_does_not_resurrect_a_terminal_record(
            self, tmp_path):
        """The record went terminal under the await (member removed, session
        closed): STARTING must not be written back, and the job that was
        started in the meantime has to be cancelled -- it would otherwise
        hold the allocation with nobody waiting for it."""
        plugin, ps, rec = self._pending(tmp_path)
        size = ps.config.pilot_sizes['s']

        psij_mock = MagicMock()
        psij_mock.cancel_job = MagicMock()

        def _submit(*args, **kw):
            # the state change that lands while psij is being called
            plugin._mark_pilot_failed(ps, rec, 'cancel requested')
            return {'job_id': 'jid'}

        psij_mock.submit_tunneled = MagicMock(side_effect=_submit)
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(
                ps, rec, size, ps.config.member(rec.member_id)))

        assert rec.state       == PILOT_FAILED
        assert rec.error       == 'cancel requested'
        assert rec.psij_job_id is None
        psij_mock.cancel_job.assert_called_once_with('jid')


class TestLegacyPilotMemberId:

    def test_a_legacy_pilot_carries_an_empty_member_id(self, tmp_path):
        """A legacy pilot carries '' -- the implicit member's own id."""
        _, plugin = _make_plugin(tmp_path)
        client = TestClient(plugin._app)
        sid = _session_with_cpu(client, plugin, sid='A',
                                lifetime='persistent')
        ps = _pool(plugin, sid, 'cpu')
        plugin._dispatch_notify = lambda t, d: None

        async def drive():
            with patch.object(plugin, '_do_pilot_submit',
                              new=AsyncMock(return_value=None)):
                return plugin._submit_pilot(ps, None)
        pid = asyncio.run(drive())
        assert ps.pilots[pid].member_id == ''
        # ...and it still resolves to the implicit member
        assert ps.member(ps.pilots[pid].member_id).member_id == ''
        assert ps.live_pilots_for('') == [ps.pilots[pid]]


def test_last_pilot_error_stops_at_the_newest_healthy_pilot():
    # a failure older than a pilot that reached ACTIVE is history, not the
    # member's current problem (a lost-and-re-added member would otherwise
    # show "child endpoint lost" under an ok row forever)
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher as P
    def _rec(pid, **kw):
        return PilotRecord(pid=pid, pool='p', size_key='s',
                           rhapsody_backend='concurrent', **kw)
    failed = _rec('p.f', state=PILOT_FAILED, error='quota exceeded')
    active = _rec('p.a', state=PILOT_ACTIVE, active_at=1.0)
    assert P._last_pilot_error([failed]) == 'quota exceeded'
    assert P._last_pilot_error([failed, active]) is None
    assert P._last_pilot_error([active, failed]) == 'quota exceeded'
    assert P._last_pilot_error([]) is None
