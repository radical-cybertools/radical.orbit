"""Unit tests for plugin_task_dispatcher.

Focus: plugin-level behavior that does not require a live bridge —
routing decisions, cached-state idempotency, stage_in/stage_out,
pilot_handshake binding, and strategy interaction.

All network paths (BridgeClient → psij / rhapsody on remote endpoints) are
stubbed; the plugin's in-process state is exercised directly.
"""

import asyncio
import base64
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

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
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool_cfg(*, pool_name: str = 'cpu',
                   max_pilots: int = 4,
                   strategy: str = 'conservative') -> PoolConfig:
    """Build a test-fixture PoolConfig (matches the old _write_pools_json shape)."""
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


def _make_plugin(tmp_path: Path, *, pool_name: str = 'cpu',
                 write_config: bool = True, strategy: str = 'conservative',
                 instance: str = 'task_dispatcher') -> tuple:
    """Instantiate a plugin bound to tmp_path; return (app, plugin).

    With ``write_config=True`` (default) the helper materialises one
    test pool directly via :meth:`_materialise_pool`, mirroring how a
    session-driven workflow would set things up.  Pass ``False`` for
    tests that need a dispatcher with zero pools.
    """
    app = FastAPI()
    app.state.endpoint_name  = 'endpoint0'
    app.state.bridge_url = 'https://localhost:9999'

    plugin = PluginTaskDispatcher(
        app, instance_name=instance,
        state_root=tmp_path / 'state',
        scratch_root=tmp_path / 'scratch')
    if write_config:
        plugin._materialise_pool(_make_pool_cfg(pool_name=pool_name,
                                                strategy=strategy))
    return app, plugin


# ---------------------------------------------------------------------------
# Plugin initialization
# ---------------------------------------------------------------------------

class TestInit:

    def test_is_enabled_on_bridge(self):
        """is_enabled returns True when host role is 'bridge'."""
        with patch('radical.orbit.utils.host_role') as m:
            m.return_value = {'role': 'bridge'}
            assert PluginTaskDispatcher.is_enabled(FastAPI()) is True

    def test_is_enabled_false_off_bridge(self):
        """is_enabled returns False on login / compute / standalone hosts."""
        with patch('radical.orbit.utils.host_role') as m:
            for role in ('login', 'compute', 'standalone'):
                m.return_value = {'role': role}
                assert PluginTaskDispatcher.is_enabled(FastAPI()) is False, \
                    f"is_enabled should be False for role={role!r}"

    def test_init_with_missing_config_is_non_fatal(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        assert plugin._pool_states == {}

    def test_init_loads_pools(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        assert 'cpu' in plugin._pool_states
        assert isinstance(plugin._pool_states['cpu'], PoolState)

    def test_routes_registered(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        pats = [pat.pattern for _, pat, _, _ in app.state.direct_routes]
        ns = plugin.namespace.lstrip('/')
        expected_fragments = [
            f'{ns}/pools$',
            f'{ns}/pool/',
            f'{ns}/fleet/',
            f'{ns}/submit/',
            f'{ns}/task/',
            f'{ns}/cancel/',
            f'{ns}/stage_in/',
            f'{ns}/stage_out/',
        ]
        for frag in expected_fragments:
            assert any(frag in p for p in pats), \
                f'route {frag} not registered; have: {pats}'


# ---------------------------------------------------------------------------
# Routes — pools / pool detail / fleet
# ---------------------------------------------------------------------------

class TestRoutes:

    def test_list_pools(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pools')
        assert r.status_code == 200
        body = r.json()
        assert 'cpu' in body['pools']
        assert body['pools']['cpu']['max_pilots'] == 4

    def test_pool_detail(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pool/cpu')
        assert r.status_code == 200
        assert 'pilots' in r.json()

    def test_pool_detail_unknown(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/pool/nope')
        assert r.status_code == 404

    def test_fleet_requires_session(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        r = client.get(f'{plugin.namespace}/fleet/nosuchsid')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Session registration + submit
# ---------------------------------------------------------------------------

def _register_session(client: TestClient, plugin, body=None) -> str:
    if body is None:
        r = client.post(f'{plugin.namespace}/register_session')
    else:
        r = client.post(f'{plugin.namespace}/register_session', json=body)
    assert r.status_code == 200, r.text
    return r.json()['sid']


# ---------------------------------------------------------------------------
# Per-session pool materialisation (Phase 4)
# ---------------------------------------------------------------------------

class TestSessionDrivenPools:

    def _pool_dict(self, **overrides):
        d = {
            'name'        : 'gpu',
            'endpoint_name'   : 'endpoint_remote',
            'queue'       : 'batch',
            'account'     : None,
            'default_size': 's',
            'pilot_sizes' : {
                's': {'nodes': 1, 'cpus_per_node': 4,
                      'gpus_per_node': 1,
                      'rhapsody_backend': 'concurrent'}},
            'max_pilots'  : 1,
            'strategy'    : 'conservative',
        }
        d.update(overrides)
        return d

    def test_session_can_declare_new_pool(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        sid = _register_session(client, plugin,
                                body={'pools': [self._pool_dict()]})
        assert sid
        assert 'gpu' in plugin._pool_states
        assert plugin._pool_states['gpu'].config.endpoint_name == 'endpoint_remote'

    def test_session_with_no_pools_materialises_default(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        _register_session(client, plugin)
        assert 'default' in plugin._pool_states

    def test_default_pool_materialised_only_once(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        _register_session(client, plugin)
        first_state = plugin._pool_states['default']
        _register_session(client, plugin)
        second_state = plugin._pool_states['default']
        assert first_state is second_state   # same PoolState instance

    def test_matching_pool_reattaches(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        body = {'pools': [self._pool_dict()]}
        _register_session(client, plugin, body=body)
        first = plugin._pool_states['gpu']
        _register_session(client, plugin, body=body)
        assert plugin._pool_states['gpu'] is first

    def test_pool_conflict_rejected(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        _register_session(client, plugin,
                          body={'pools': [self._pool_dict()]})
        # Same pool name, different max_pilots → conflict.
        r = client.post(f'{plugin.namespace}/register_session',
                        json={'pools': [self._pool_dict(max_pilots=99)]})
        assert r.status_code == 409
        assert 'gpu' in r.text

    def test_invalid_pool_body_400(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path, write_config=False)
        client = TestClient(plugin._app)
        r = client.post(f'{plugin.namespace}/register_session',
                        json={'pools': 'not-a-list'})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# State-dir pruning (Phase 5)
# ---------------------------------------------------------------------------

class TestStateSweeper:

    def test_prune_removes_stale_orphan_dir(self, tmp_path: Path):
        '''A dir not in active pools, last touched >30d ago, gets pruned.'''
        import os
        _, plugin = _make_plugin(tmp_path, write_config=False)
        state_root = tmp_path / 'state'
        state_root.mkdir(exist_ok=True)
        stale = state_root / 'oldpool__endpoint_gone'
        stale.mkdir()
        (stale / 'pilot.log').write_text('{}\n')
        # Age the only file in the dir to 40 days old.
        old = state_root.stat().st_mtime - 40 * 86400
        os.utime(stale / 'pilot.log', (old, old))

        plugin._prune_stale_state_dirs()
        assert not stale.exists()

    def test_prune_keeps_active_pool_dir(self, tmp_path: Path):
        '''A dir matching an active pool is NEVER pruned, even if old.'''
        import os
        _, plugin = _make_plugin(tmp_path)   # 'cpu' pool active
        # _make_plugin built state at state/cpu__endpoint0/ as part of PoolState init.
        active_dir = tmp_path / 'state' / 'cpu__endpoint0'
        if active_dir.exists():
            for p in active_dir.iterdir():
                old = (active_dir.stat().st_mtime - 365 * 86400)
                os.utime(p, (old, old))
        plugin._prune_stale_state_dirs()
        assert active_dir.exists()

    def test_prune_keeps_recent_orphan(self, tmp_path: Path):
        '''A dir not in active pools but recently touched is NOT pruned.'''
        _, plugin = _make_plugin(tmp_path, write_config=False)
        state_root = tmp_path / 'state'
        state_root.mkdir(exist_ok=True)
        fresh = state_root / 'recent__endpoint_x'
        fresh.mkdir()
        (fresh / 'pilot.log').write_text('{}\n')
        plugin._prune_stale_state_dirs()
        assert fresh.exists()


# ---------------------------------------------------------------------------
# Bridge plugin host integration (Phase 3 / Phase 5 wiring)
# ---------------------------------------------------------------------------

class TestBridgeHostLoadsDispatcher:
    '''Smoke test: BridgePluginHost can actually load + serve the dispatcher.

    The other tests in this file use a vanilla FastAPI app, which masks
    any mismatch between the dispatcher's expectations and what
    BridgePluginHost provides (is_bridge flag, send_notification,
    on_topology_change fan-out, etc.).  These tests exercise the real
    host.
    '''

    @pytest.fixture
    def host(self, tmp_path: Path, monkeypatch):
        from radical.orbit.bridge_plugin_host import BridgePluginHost
        # Steer the dispatcher's default state/scratch roots at tmp_path
        # so the host's auto-built plugin doesn't pollute $HOME.
        monkeypatch.setenv('RADICAL_ORBIT_BRIDGE_URL', 'https://localhost:9999')
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
            tmp_path / 'state')
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
            tmp_path / 'scratch')
        broadcasts: list = []

        async def broadcast(topic, data):
            broadcasts.append((topic, data))

        host = BridgePluginHost(
            plugin_names=['task_dispatcher'],
            broadcast_fn=broadcast,
            endpoint_name='bridge',
            bridge_url='https://localhost:9999')
        return host

    def test_dispatcher_loads(self, host):
        '''The dispatcher plugin instantiates and registers on the bridge host.'''
        assert 'task_dispatcher' in host._plugins
        td = host._plugins['task_dispatcher']
        assert td.plugin_name == 'task_dispatcher'
        # No pools loaded at startup (Phase 5: pools are session-driven).
        assert td._pool_states == {}

    @pytest.mark.asyncio
    async def test_dispatcher_routes_reachable(self, host):
        '''Dispatcher's GET /pools route is wired through the host.'''
        # Namespace prefix is whatever the plugin chose; for task_dispatcher
        # it's '/task_dispatcher' (inside the bridge host's view).
        td = host._plugins['task_dispatcher']
        path = f'{td.namespace}/pools'
        resp = await host.handle_request('GET', path, {}, b'')
        # JSONResponse → body has {'pools': {...}}; with no pools materialised,
        # the dict is empty but the route shouldn't 404.
        import json
        body = json.loads(resp.body)
        assert 'pools' in body
        assert body['pools'] == {}

    @pytest.mark.asyncio
    async def test_register_session_materialises_default_pool(self, host):
        '''POST /register_session with no body → default pool appears.'''
        td = host._plugins['task_dispatcher']
        path = f'{td.namespace}/register_session'
        resp = await host.handle_request('POST', path, {}, b'{}')
        import json
        body = json.loads(resp.body)
        assert 'sid' in body
        assert 'default' in td._pool_states

    def test_dispatcher_is_bridge_role(self, host):
        '''The dispatcher's is_enabled returns True under the bridge host.'''
        from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher
        assert PluginTaskDispatcher.is_enabled(host._app) is True


class TestSubmitTask:

    def test_rejects_unknown_pool(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'nope', 'task_id': 't.1',
            'cmd': ['/bin/echo', 'hi'], 'cwd': '/tmp'})
        assert r.status_code == 404

    def test_rejects_missing_fields(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'cpu'})
        assert r.status_code == 400

    def test_enqueues_task_and_triggers_strategy(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)

        spy = plugin._pool_states['cpu'].strategy
        with patch.object(spy, 'on_task_arrived') as on_arrived, \
             patch.object(spy, 'pick_dispatch', return_value=None) as pd:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.1',
                'cmd': ['/bin/echo', 'hi'],
                'cwd': str(tmp_path), 'priority': 7,
                'inputs': ['a'], 'outputs': ['b']})
            assert r.status_code == 200
            assert on_arrived.called
            assert pd.called

        # Record exists in memory and on disk
        ps = plugin._pool_states['cpu']
        assert 't.1' in ps.tasks
        assert ps.tasks['t.1'].priority == 7
        assert ps.tasks['t.1'].state == TASK_QUEUED
        # Log replays consistently
        replayed = ps.task_log.replay()
        assert 't.1' in replayed

    def test_cached_done_returns_without_reexec(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Seed a DONE record
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_DONE, exit_code=0)

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.done',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            assert r.json()['state'] == TASK_DONE
            assert r.json()['exit_code'] == 0
            spy.assert_not_called()   # no re-execution

    def test_cached_failed_reexecutes(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        ps.tasks['t.fail'] = TaskRecord(
            task_id='t.fail', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_FAILED, exit_code=1)

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.fail',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            # Re-executed → QUEUED again
            assert r.json()['state'] == TASK_QUEUED
            spy.assert_called_once()

    def test_cached_running_attaches(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        ps.tasks['t.run'] = TaskRecord(
            task_id='t.run', pool='cpu',
            cmd=['/bin/echo'], cwd=str(tmp_path),
            state=TASK_RUNNING, pilot_id='p.xyz')

        with patch.object(ps.strategy, 'on_task_arrived') as spy:
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'pool': 'cpu', 'task_id': 't.run',
                'cmd': ['/bin/echo'], 'cwd': str(tmp_path)})
            assert r.status_code == 200
            assert r.json()['state'] == TASK_RUNNING
            spy.assert_not_called()


class TestGetTaskAndCancel:

    def test_get_task_404(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.get(f'{plugin.namespace}/task/{sid}/nope')
        assert r.status_code == 404

    def test_cancel_queued_is_immediate(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Put a queued task on the books
        ps.tasks['t.q'] = TaskRecord(
            task_id='t.q', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_QUEUED)

        r = client.post(f'{plugin.namespace}/cancel/{sid}/t.q')
        assert r.status_code == 200
        assert r.json()['state'] == TASK_CANCELED
        assert ps.tasks['t.q'].state == TASK_CANCELED

    def test_cancel_terminal_is_noop(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.done'] = TaskRecord(
            task_id='t.done', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE, exit_code=0)
        r = client.post(f'{plugin.namespace}/cancel/{sid}/t.done')
        assert r.status_code == 200
        assert r.json()['state'] == TASK_DONE   # unchanged


# ---------------------------------------------------------------------------
# Staging routes
# ---------------------------------------------------------------------------

class TestStagingRoutes:

    def test_stage_in_writes_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        content = b'hello world'
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'cpu', 'filename': 'input.txt',
                  'content_b64': base64.b64encode(content).decode('ascii')})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['size'] == len(content)
        path = Path(body['cwd']) / 'input.txt'
        assert path.read_bytes() == content

    def test_stage_in_rejects_bad_filename(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        for bad in ('../evil', 'sub/dir', '', '.', '..'):
            r = client.post(
                f'{plugin.namespace}/stage_in/{sid}/t.1',
                json={'pool': 'cpu', 'filename': bad,
                      'content_b64': base64.b64encode(b'x').decode('ascii')})
            assert r.status_code == 400, \
                f'expected 400 for filename {bad!r}'

    def test_stage_in_rejects_unknown_pool(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'nope', 'filename': 'f.txt',
                  'content_b64': base64.b64encode(b'x').decode('ascii')})
        assert r.status_code == 404

    def test_stage_in_overwrite_flag(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        payload = {'pool': 'cpu', 'filename': 'f.txt',
                   'content_b64': base64.b64encode(b'v1').decode('ascii')}
        r1 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r1.status_code == 200

        # Re-upload w/o overwrite → 409
        r2 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r2.status_code == 409

        # With overwrite=True → 200 and contents updated
        payload['content_b64'] = base64.b64encode(b'v2').decode('ascii')
        payload['overwrite']   = True
        r3 = client.post(f'{plugin.namespace}/stage_in/{sid}/t.1', json=payload)
        assert r3.status_code == 200
        path = Path(r3.json()['cwd']) / 'f.txt'
        assert path.read_bytes() == b'v2'

    def test_stage_out_returns_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE)
        scratch = ps.scratch_base / 't.1'
        scratch.mkdir(parents=True, exist_ok=True)
        (scratch / 'out.txt').write_bytes(b'result payload')

        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/out.txt')
        assert r.status_code == 200
        body = r.json()
        assert body['size'] == len(b'result payload')
        assert base64.b64decode(body['content_b64']) == b'result payload'

    def test_stage_out_missing_file(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', cmd=['/bin/echo'],
            cwd=str(tmp_path), state=TASK_DONE)
        r = client.get(f'{plugin.namespace}/stage_out/{sid}/t.1/nope.txt')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pilot binding via topology hook
# ---------------------------------------------------------------------------

class TestTopologyBinding:

    def _new_pending(self, plugin, pid='p.1', child='endpoint0_p.1',
                    state=PILOT_PENDING):
        ps = plugin._pool_states['cpu']
        ps.pilots[pid] = PilotRecord(
            pid=pid, pool='cpu', size_key='s',
            rhapsody_backend='concurrent',
            state=state, submitted_at=100.0,
            child_endpoint_name=child)
        return ps

    def test_topology_binds_pending_pilot(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        plugin._loops_started = True   # bypass the not-yet-started gate
        ps = self._new_pending(plugin)
        with patch.object(ps.strategy, 'on_pilot_state') as spy:
            asyncio.run(plugin.on_topology_change(
                {'endpoint0_p.1': {'plugins': ['rhapsody']}}))
        assert ps.pilots['p.1'].state == PILOT_ACTIVE
        # capacity = nodes(1) * cpus_per_node(4) from _make_plugin's pool
        assert ps.pilots['p.1'].capacity == 4
        assert ps.pilots['p.1'].active_at is not None
        spy.assert_called_once()

    def test_topology_ignores_unknown_endpoints(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps = self._new_pending(plugin)
        asyncio.run(plugin.on_topology_change(
            {'someone_else': {'plugins': ['sysinfo']}}))
        assert ps.pilots['p.1'].state == PILOT_PENDING

    def test_topology_ignores_terminal_pilot(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        plugin._loops_started = True
        ps = self._new_pending(plugin, state=PILOT_FAILED)
        asyncio.run(plugin.on_topology_change(
            {'endpoint0_p.1': {'plugins': ['rhapsody']}}))
        assert ps.pilots['p.1'].state == PILOT_FAILED   # unchanged

    def test_topology_no_op_before_started(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        # Don't flip _loops_started — emulate startup race
        ps = self._new_pending(plugin)
        asyncio.run(plugin.on_topology_change(
            {'endpoint0_p.1': {'plugins': ['rhapsody']}}))
        assert ps.pilots['p.1'].state == PILOT_PENDING


# ---------------------------------------------------------------------------
# Internal helpers — pilot failure re-enqueues tasks
# ---------------------------------------------------------------------------

class TestMarkPilotFailed:

    def test_reenqueues_running_tasks(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        pilot = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            capacity=2, in_flight=2)
        ps.pilots['p.1'] = pilot
        t_running = TaskRecord(task_id='t.r', pool='cpu',
                                cmd=['/bin/echo'], cwd=str(tmp_path),
                                state=TASK_RUNNING, pilot_id='p.1')
        t_done    = TaskRecord(task_id='t.d', pool='cpu',
                                cmd=['/bin/echo'], cwd=str(tmp_path),
                                state=TASK_DONE, pilot_id='p.1')
        ps.tasks['t.r'] = t_running
        ps.tasks['t.d'] = t_done

        plugin._mark_pilot_failed(ps, pilot, 'test')

        assert pilot.state == PILOT_FAILED
        assert ps.tasks['t.r'].state == TASK_QUEUED
        assert ps.tasks['t.r'].pilot_id is None
        assert ps.tasks['t.d'].state == TASK_DONE    # terminal unchanged


# ---------------------------------------------------------------------------
# Strategy submit_pilot bookkeeping (no actual psij call)
# ---------------------------------------------------------------------------

class TestStrategyActions:

    def test_strategy_submit_records_pilot(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']

        # Prevent the async submit from running: no event loop here anyway
        with patch.object(plugin, '_schedule_pilot_submit') as sched:
            pid = ps.ctx.submit_pilot(None)

        assert pid.startswith('p.')
        assert pid in ps.pilots
        assert ps.pilots[pid].state == PILOT_PENDING
        assert ps.pilots[pid].rhapsody_backend == 'concurrent'
        sched.assert_called_once()

    def test_strategy_submit_unknown_size(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        with pytest.raises(KeyError):
            ps.ctx.submit_pilot('xxl')

    def test_drain_pilot_flips_flag(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        ps.pilots['p.x'] = PilotRecord(
            pid='p.x', pool='cpu', size_key='s',
            rhapsody_backend='concurrent', state=PILOT_ACTIVE,
            capacity=4, in_flight=1)
        ps.ctx.drain_pilot('p.x')
        assert ps.pilots['p.x'].accepting_new_tasks is False
        # Free capacity now zero despite slots
        assert ps.pilots['p.x'].free_capacity() == 0


# ---------------------------------------------------------------------------
# Handshake-arrival via handler → triggers drain (pick_dispatch loop)
# ---------------------------------------------------------------------------

class TestDispatchDrain:

    def test_drain_assigns_queued_task(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        ps = plugin._pool_states['cpu']

        # Two queued tasks + one active pilot
        for i in range(2):
            ps.tasks[f't.{i}'] = TaskRecord(
                task_id=f't.{i}', pool='cpu',
                cmd=['/bin/echo', str(i)], cwd=str(tmp_path),
                state=TASK_QUEUED, priority=0, arrival_ts=float(i))
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='cpu', size_key='s',
            rhapsody_backend='concurrent',
            state=PILOT_ACTIVE, capacity=4, in_flight=0,
            child_endpoint_name='endpoint0_p.1',
            walltime_deadline=10_000.0, submitted_at=0.0, active_at=10.0)

        # Prevent the async rhapsody submit from running
        with patch.object(plugin, '_do_rhapsody_submit') as spy, \
             patch.object(plugin, '_main_loop'):
            plugin._drain_pending(ps)

        # Both tasks advanced to RUNNING
        assert ps.tasks['t.0'].state == TASK_RUNNING
        assert ps.tasks['t.1'].state == TASK_RUNNING
        assert ps.pilots['p.1'].in_flight == 2


# ---------------------------------------------------------------------------
# Regression: psij submit_tunneled tunnel-arg shape
# ---------------------------------------------------------------------------
#
# Bug surfaced during the local e2e smoke (memory/project_bridge_dispatcher.md):
# the dispatcher used to call ``psij_c.submit_tunneled(spec, executor, False)``
# but psij now requires one of ``'none'`` / ``'forward'`` / ``'reverse'`` and
# rejects the boolean.  Fix was a literal ``False`` → ``'none'`` change in
# _do_pilot_submit.  This test pins the contract.

class TestPilotSubmitTunnelArg:

    def test_passes_tunnel_none_not_false(self, tmp_path: Path):
        _, plugin = _make_plugin(tmp_path)
        ps = plugin._pool_states['cpu']
        size = ps.config.pilot_sizes[ps.config.default_size]
        record = PilotRecord(
            pid='p.tunnel_arg', pool='cpu', size_key=ps.config.default_size,
            rhapsody_backend=size.rhapsody_backend,
            state=PILOT_PENDING, submitted_at=0.0)
        ps.pilots[record.pid] = record

        psij_mock = MagicMock()
        psij_mock.submit_tunneled.return_value = {'job_id': 'fake-jid'}

        with patch.object(plugin, '_get_psij_client', return_value=psij_mock), \
             patch('radical.orbit.batch_system.detect_batch_system') as bs:
            bs.return_value.psij_executor = 'local'
            asyncio.run(plugin._do_pilot_submit(ps, record, size))

        psij_mock.submit_tunneled.assert_called_once()
        # Third positional arg is the tunnel mode — must be the string
        # 'none', not False.
        call_args = psij_mock.submit_tunneled.call_args
        assert call_args.args[2] == 'none', (
            f'expected tunnel mode \'none\', got {call_args.args[2]!r}')
        assert call_args.args[2] is not False


# ---------------------------------------------------------------------------
# Endpoint-mode submit (transparent proxy to a target endpoint's rhapsody)
# ---------------------------------------------------------------------------

class TestEndpointModeSubmit:
    '''Submit/get/cancel paths that target an endpoint directly (no pool).

    These tests stub :meth:`_get_rhapsody_client` so no real bridge or
    HTTP traffic is needed.  ``_connected_endpoints`` is poked directly to
    simulate a topology update (the test client doesn't run the bridge
    WS subscription thread).
    '''

    def _seed_topology(self, plugin, endpoint_plugins: dict):
        '''Populate ``_connected_endpoints`` as on_topology_change would.'''
        plugin._connected_endpoints = {
            name: set(plugins) for name, plugins in endpoint_plugins.items()
        }

    def test_xor_neither_set_400(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'task_id': 't.1', 'cmd': ['/bin/echo', 'hi'],
            'cwd': '/tmp'})
        assert r.status_code == 400
        assert 'exactly one' in r.text

    def test_xor_both_set_400(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'pool': 'cpu', 'endpoint': 'endpoint_x',
            'task_id': 't.1', 'cmd': ['/bin/echo', 'hi'],
            'cwd': '/tmp'})
        assert r.status_code == 400
        assert 'exactly one' in r.text

    def test_unknown_endpoint_404(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        self._seed_topology(plugin, {})   # no endpoints connected
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'endpoint': 'ghost', 'task_id': 't.1',
            'cmd': ['/bin/echo', 'hi'], 'cwd': '/tmp'})
        assert r.status_code == 404
        assert 'unknown endpoint: ghost' in r.text

    def test_endpoint_without_rhapsody_503(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        self._seed_topology(plugin, {'endpoint_dumb': ['sysinfo']})
        r = client.post(f'{plugin.namespace}/submit/{sid}', json={
            'endpoint': 'endpoint_dumb', 'task_id': 't.1',
            'cmd': ['/bin/echo', 'hi'], 'cwd': '/tmp'})
        assert r.status_code == 503
        assert 'cannot run tasks' in r.text

    def test_rejects_inputs_in_endpoint_mode(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        self._seed_topology(plugin, {'endpoint_r': ['rhapsody']})
        rh_mock = MagicMock()
        rh_mock.submit_tasks.return_value = [{'uid': 't.1', 'state': 'NEW'}]
        with patch.object(plugin, '_get_rhapsody_client',
                          return_value=rh_mock):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'endpoint_r', 'task_id': 't.1',
                'cmd': ['/bin/echo', 'hi'], 'cwd': '/tmp',
                'inputs': ['a']})
        assert r.status_code == 400
        assert 'staging not supported' in r.text or \
               'not supported for endpoint-mode' in r.text

    def test_proxy_submit_invokes_rhapsody(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        self._seed_topology(plugin, {'endpoint_r': ['rhapsody', 'sysinfo']})
        rh_mock = MagicMock()
        rh_mock.submit_tasks.return_value = [{'uid': 't.1', 'state': 'NEW'}]
        with patch.object(plugin, '_get_rhapsody_client',
                          return_value=rh_mock):
            r = client.post(f'{plugin.namespace}/submit/{sid}', json={
                'endpoint': 'endpoint_r', 'task_id': 't.1',
                'cmd': ['/bin/sleep', '0'], 'cwd': '/tmp'})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['task_id'] == 't.1'
        assert body['endpoint'] == 'endpoint_r'
        # Rhapsody saw the task with the same uid + cwd carried via
        # backend_specific_kwargs (so rhapsody's concurrent backend
        # picks the right working dir).
        rh_mock.submit_tasks.assert_called_once()
        submitted = rh_mock.submit_tasks.call_args.args[0]
        assert submitted[0]['uid'] == 't.1'
        assert submitted[0]['executable'] == '/bin/sleep'
        assert submitted[0]['arguments'] == ['0']
        # Endpoint-mode mapping recorded so get/cancel can route back.
        assert plugin._endpoint_mode_tasks.get('t.1') == 'endpoint_r'

    def test_get_task_forwards_to_target_endpoint(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        plugin._endpoint_mode_tasks['t.1'] = 'endpoint_r'
        rh_mock = MagicMock()
        rh_mock.get_task.return_value = {'uid': 't.1', 'state': 'RUNNING'}
        with patch.object(plugin, '_get_rhapsody_client',
                          return_value=rh_mock):
            r = client.get(f'{plugin.namespace}/task/{sid}/t.1')
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['task_id'] == 't.1'
        assert body['endpoint'] == 'endpoint_r'
        assert body['result']['state'] == 'RUNNING'
        rh_mock.get_task.assert_called_once_with('t.1')

    def test_cancel_task_forwards_to_target_endpoint(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        plugin._endpoint_mode_tasks['t.1'] = 'endpoint_r'
        rh_mock = MagicMock()
        rh_mock.cancel_task.return_value = {'uid': 't.1', 'state': 'CANCELED'}
        with patch.object(plugin, '_get_rhapsody_client',
                          return_value=rh_mock):
            r = client.post(f'{plugin.namespace}/cancel/{sid}/t.1')
        assert r.status_code == 200, r.text
        rh_mock.cancel_task.assert_called_once_with('t.1')

    def test_stage_in_rejects_endpoint_mode_task(self, tmp_path: Path):
        app, plugin = _make_plugin(tmp_path)
        client = TestClient(app)
        sid = _register_session(client, plugin)
        plugin._endpoint_mode_tasks['t.1'] = 'endpoint_r'
        r = client.post(
            f'{plugin.namespace}/stage_in/{sid}/t.1',
            json={'pool': 'cpu', 'filename': 'x.txt',
                  'content_b64': base64.b64encode(b'hi').decode('ascii')})
        assert r.status_code == 400
        assert 'endpoint-mode' in r.text

    def test_terminal_clears_endpoint_mode_mapping(self, tmp_path: Path):
        '''Terminal notification removes the mapping and re-emits status.'''
        _, plugin = _make_plugin(tmp_path)
        plugin._endpoint_mode_tasks['t.1'] = 'endpoint_r'
        notified: list = []
        plugin._dispatch_notify = lambda topic, data: notified.append(
            (topic, data))   # type: ignore[method-assign]
        plugin._handle_task_terminal(
            't.1', TASK_DONE, {'exit_code': 0, 'error': None})
        assert 't.1' not in plugin._endpoint_mode_tasks
        assert len(notified) == 1
        topic, data = notified[0]
        assert topic == 'task_status'
        assert data['task_id'] == 't.1'
        assert data['endpoint']    == 'endpoint_r'
        assert data['state']   == TASK_DONE
        assert data['exit_code'] == 0
