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
        plugin._materialise_pool('A', _make_pool_cfg())
        ps = _pool(plugin, 'A', 'cpu')
        ps.pilots[pid] = PilotRecord(
            pid=pid, pool='cpu', owning_sid='A', size_key='s',
            rhapsody_backend='concurrent', state=state, submitted_at=100.0,
            child_endpoint_name=child, walltime_deadline=walltime)
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

        # The dispatcher drives child clients' SYNC methods via
        # asyncio.to_thread (there are no async `a<method>` twins anymore),
        # so a plain MagicMock returning a dict is all `submit_tunneled` needs.
        psij_mock = MagicMock()
        psij_mock.submit_tunneled = MagicMock(return_value={'job_id': 'jid'})
        with patch.object(plugin, '_get_psij_client',
                          new=AsyncMock(return_value=psij_mock)), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size))

        psij_mock.submit_tunneled.assert_called_once()
        assert psij_mock.submit_tunneled.call_args.args[2] == 'none'
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
