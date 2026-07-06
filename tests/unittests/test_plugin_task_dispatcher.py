"""Unit tests for plugin_task_dispatcher (broker-hosted, strict per-session pools).

Focus: plugin-level behavior that does not require a live broker — strict
per-session pool isolation, restart-time replay, session-close teardown,
cached-state idempotency, staging, pilot binding via rich topology
(present/suspect/lost), and the async transport port (broker caller, no
`asyncio.to_thread`).

Endpoint calls are stubbed via :meth:`_get_psij_client` / :meth:`_get_rhapsody_client`
(async; in production they return real caller-backed ``PSIJClient`` /
``RhapsodyClient`` helpers, driven here through their ``a<method>`` async cores);
no real broker or WebSocket is needed.
"""

import asyncio
import base64
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_task_dispatcher import (
    PluginTaskDispatcher, PoolState,
)
from radical.orbit.task_dispatcher_config import PoolConfig, PilotSize
from radical.orbit.task_dispatcher_state   import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_ACTIVE, PILOT_FAILED, PILOT_DONE,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_CANCELED,
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
        ps_a.pilot_log.append(ps_a.pilots['p.1'])
        rec = TaskRecord(task_id='t.x', pool='cpu', owning_sid='A',
                         cmd=['/bin/echo'], cwd=str(tmp_path),
                         state=TASK_RUNNING, pilot_id='p.1',
                         rhapsody_uid='rh.1')
        ps_a.tasks['t.x'] = rec
        ps_a.task_log.append(rec)

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
        plugin.reclaim_drain = 0.05        # tiny drain timer

        async def scenario():
            plugin._ensure_started()
            sid = await plugin._open_session(None, 'ephemeral', None,
                                             owner='clientA')
            plugin._materialise_pool(sid, _make_pool_cfg())
            ps = _pool(plugin, sid, 'cpu')
            ps.pilots['p.1'] = PilotRecord(
                pid='p.1', pool='cpu', owning_sid=sid, size_key='s',
                rhapsody_backend='concurrent', state=PILOT_ACTIVE)
            # owner declared lost → arms the reclaim-drain
            await plugin.on_topology_change(
                {'clientA': {'role': 'endpoint', 'plugins': {},
                             'liveness': 'lost'}})
            await asyncio.sleep(0.25)       # let the drain fire
            return sid, ps

        sid, ps = asyncio.run(scenario())
        assert sid not in plugin._pool_states
        assert ps.pilots['p.1'].state == PILOT_FAILED

    def test_persistent_pool_survives_owner_loss(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin.reclaim_drain = 0.05

        async def scenario():
            plugin._ensure_started()
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
            plugin._ensure_started()
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
        with patch.object(ps.strategy, 'on_task_arrived') as on_arrived, \
             patch.object(ps.strategy, 'pick_dispatch', return_value=None):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo', 'hi'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            assert on_arrived.called
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
        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.done',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.json()['state'] == TASK_DONE
            spy.assert_not_called()

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

        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', owning_sid=sid, cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE)
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
        _, plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        ps.pilots[pid] = PilotRecord(
            pid=pid, pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=state, submitted_at=100.0,
            child_endpoint_name=child, walltime_deadline=walltime)
        return plugin, ps

    def test_present_binds_pending_pilot(self, tmp_path):
        plugin, ps = self._plugin_with_pilot(tmp_path)
        with patch.object(ps.strategy, 'on_pilot_state') as spy:
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


# ---------------------------------------------------------------------------
# Async transport port: proxies over the broker caller (mocked)
# ---------------------------------------------------------------------------

class TestPilotSubmitTransport:

    def test_submit_tunneled_passes_tunnel_none(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        size = ps.config.pilot_sizes[ps.config.default_size]
        record = PilotRecord(
            pid='p.a', pool='cpu', owning_sid='A',
            size_key=ps.config.default_size,
            rhapsody_backend=size.rhapsody_backend, state=PILOT_PENDING)
        ps.pilots[record.pid] = record

        psij_mock = MagicMock()
        psij_mock.asubmit_tunneled = AsyncMock(return_value={'job_id': 'jid'})
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size))

        psij_mock.asubmit_tunneled.assert_awaited_once()
        assert psij_mock.asubmit_tunneled.await_args.args[2] == 'none'
        assert record.psij_job_id == 'jid'

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
        rh_mock = MagicMock()
        rh_mock.asubmit_tasks = AsyncMock(return_value=[{'uid': 't.1',
                                                         'state': 'NEW'}])
        rh_mock.aget_task    = AsyncMock(return_value={'uid': 't.1',
                                                       'state': 'RUNNING'})
        rh_mock.acancel_task = AsyncMock(return_value={'uid': 't.1',
                                                       'state': 'CANCELED'})
        with patch.object(plugin, '_get_rhapsody_client',
                          new=AsyncMock(return_value=rh_mock)):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'ep', 'task_id': 't.1',
                'cmd': ['/bin/sleep', '0'], 'cwd': '/tmp'})
            assert r.status_code == 200, r.text
            assert plugin._endpoint_mode_tasks.get('t.1') == 'ep'
            rh_mock.asubmit_tasks.assert_awaited_once()

            r = client.get(f'{plugin.namespace}/task/{sid}/t.1')
            assert r.json()['result']['state'] == 'RUNNING'

            r = client.post(f'{plugin.namespace}/cancel/{sid}/t.1')
            assert r.status_code == 200
            rh_mock.acancel_task.assert_awaited_once_with('t.1')

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

    def test_on_event_ignores_other_plugins(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        called = []
        plugin._handle_task_terminal = lambda *a: called.append(a)
        plugin._on_event({'plugin': 'psij', 'topic': 'task_status',
                          'data': {'uid': 'x', 'state': 'DONE'}})
        assert called == []
