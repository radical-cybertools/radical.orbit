"""Unit tests for plugin_federation (broker-hosted resource federation).

Two layers:

- **Unit** — the plugin driven through its HTTP routes with a fake
  ``_DispatcherAPI`` substituted in.  That object is the plugin's whole view
  of the task dispatcher, so faking it isolates join/leave/pick/submit/task
  bookkeeping from pool machinery.  Since capability-class pools the fake
  also models the member routes (``add_member`` / ``del_member``) and the
  per-member block of the verbose pool summary — the frozen contract this
  plugin is written against.
- **Co-hosted** — a real :class:`BrokerPluginHost` running the *real* task
  dispatcher next to the federation.  Those tests need the multi-member
  dispatcher and run against the real multi-member dispatcher via ``BrokerPluginHost`` (see :class:`TestCoHosted`).
"""

import asyncio
import concurrent.futures
import json
import logging
import os
import shutil
import time

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from fastapi import FastAPI, HTTPException
from starlette.testclient import TestClient

from radical.orbit.broker_plugin_host import BrokerPluginHost
from radical.orbit.plugin_federation import (
    PluginFederation, FederationSession, _DispatcherAPI, FED_SESSION_SID,
)
from radical.orbit.federation_state import (
    SubmitLedgerEntry, LIVENESS_OK, LIVENESS_LOST, LIVENESS_SUSPECT,
)
from radical.orbit.task_dispatcher_config import parse_member
from radical.orbit.task_dispatcher_state  import PilotRecord, PILOT_ACTIVE


# Scratch bases must lie under ``~`` or ``/tmp`` (the staging-plugin rule the
# federation enforces at join), so the join bodies below point at a fixed
# /tmp tree that this fixture keeps clean.
_SCRATCH_ROOT = Path('/tmp/orbit-fed-test')


@pytest.fixture(autouse=True)
def _clean_scratch():
    shutil.rmtree(_SCRATCH_ROOT, ignore_errors=True)
    yield
    shutil.rmtree(_SCRATCH_ROOT, ignore_errors=True)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------

class _FakeDispatcher:
    """Stands in for :class:`_DispatcherAPI` — records and answers.

    Mirrors only what the federation calls; every method is async, like the
    real one, and raises ``HTTPException`` the same way so error mapping is
    exercised.  ``del_member`` records its flags so a test can assert
    ``force`` / ``fail_unsatisfiable`` rather than guess.

    Two behaviours are modelled tightly because the plugin *relies* on them:

    - ``register_session`` **ignores a re-declaration of a pool that already
      exists** — the real ``_materialise_pool`` returns the existing
      ``PoolState`` before any config merge, which is precisely why the
      member routes exist.  A fake that quietly absorbed the members out of
      a re-declaration would hide a missing ``add_member``.
    - ``add_member`` is idempotent by member id but answers **409 on a
      *differing* declaration**, as the frozen contract says.  That is what
      makes the unconditional restart re-POST safe, so a fake that accepted
      anything would not test it.
    """

    def __init__(self):
        self.instance     = 'task_dispatcher'
        self.sessions     = {}                 # sid -> [pool declarations]
        self.pools        = {}                 # pool -> {member_id: decl}
        self.calls        = []                 # (verb, *args)
        self.submitted    = []                 # (sid, payload)
        self.canceled     = []                 # (sid, task_id)
        self.removed      = []                 # del_member call records
        self.tasks        = {}                 # task_id -> dispatcher dict
        self.details      = {}                 # pool -> verbose summary
        self.drain        = {}                 # member_id -> drain counters
        self.fail         = None               # verb name that raises
        self.fail_member  = None               # member_id whose add fails
        self.submit_error = None               # HTTPException for submit

    def _maybe_fail(self, verb):
        if self.fail == verb:
            raise HTTPException(status_code=503, detail=f'{verb} unavailable')

    def members_of(self, pool):
        return sorted(self.pools.get(pool) or {})

    async def register_session(self, sid, pools):
        self.calls.append(('register_session', sid))
        self._maybe_fail('register_session')
        self.sessions[sid] = pools
        for decl in pools:
            if decl['name'] in self.pools:
                continue          # an existing pool is not re-configured
            self.pools[decl['name']] = {
                member['member_id']: member
                for member in (decl.get('members') or [])}
        return {'sid': sid}

    async def unregister_session(self, sid):
        self.calls.append(('unregister_session', sid))
        self._maybe_fail('unregister_session')
        for decl in self.sessions.pop(sid, None) or []:
            self.pools.pop(decl['name'], None)
        return {'ok': True}

    async def add_member(self, sid, pool, member):
        mid = member['member_id']
        self.calls.append(('add_member', sid, pool, mid))
        self._maybe_fail('add_member')
        if self.fail_member == mid:
            raise HTTPException(status_code=400,
                                detail=f'member {mid} refused')
        held     = self.pools.setdefault(pool, {})
        existing = held.get(mid)
        if existing is not None and existing != member:
            raise HTTPException(
                status_code=409,
                detail=f'member {mid} is already declared differently')
        created = mid not in held
        held[mid] = member
        return {'pool': pool, 'member': member,
                'members': sorted(held), 'created': created}

    async def del_member(self, sid, pool, member_id, *, cancel_tasks=False,
                         force=False, fail_unsatisfiable=True):
        self.calls.append(('del_member', sid, pool, member_id))
        self.removed.append({'pool': pool, 'member_id': member_id,
                             'cancel_tasks': cancel_tasks, 'force': force,
                             'fail_unsatisfiable': fail_unsatisfiable})
        self._maybe_fail('del_member')
        (self.pools.get(pool) or {}).pop(member_id, None)
        counts = self.drain.get(member_id) or {}
        return {'pool': pool, 'member_id': member_id, 'pilots_cancelled': 0,
                'tasks_requeued': counts.get('tasks_requeued', 0),
                'tasks_failed'  : counts.get('tasks_failed', 0)}

    async def pool_detail(self, sid, name):
        self.calls.append(('pool_detail', sid, name))
        self._maybe_fail('pool_detail')
        if name in self.details:
            return self.details[name]
        return _pool_summary([_member_block(mid)
                              for mid in self.members_of(name)])

    async def submit(self, sid, payload):
        self.calls.append(('submit', sid, payload.get('task_id')))
        self._maybe_fail('submit')
        if self.submit_error is not None:
            raise self.submit_error
        self.submitted.append((sid, payload))
        rec = {'task_id': payload['task_id'], 'pool': payload['pool'],
               'cmd': payload['cmd'], 'cwd': '', 'state': 'QUEUED',
               'pilot_id': None, 'member_id': None}
        self.tasks[payload['task_id']] = rec
        return rec

    async def task(self, sid, task_id):
        self.calls.append(('task', sid, task_id))
        self._maybe_fail('task')
        rec = self.tasks.get(task_id)
        if rec is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown task: {task_id}')
        return rec

    async def cancel_task(self, sid, task_id):
        self.calls.append(('cancel_task', sid, task_id))
        self._maybe_fail('cancel_task')
        self.canceled.append((sid, task_id))
        return {'task_id': task_id, 'state': 'CANCELED'}


class _FakeCaller:
    """Broker-caller stand-in: answers endpoint routes from a canned map.

    The real caller hands back a ``concurrent.futures.Future`` resolved on
    the broker's *routing* loop, which the plugin bridges with
    ``asyncio.wrap_future``.  An already-completed future exercises that
    bridge exactly, without a broker.
    """

    def __init__(self, routes=None, raises=None):
        self.routes = routes or {}
        self.raises = raises
        self.calls  = []

    def call_threadsafe(self, dst, method, path, *, body=b'',
                        headers=None, timeout=None):
        self.calls.append((dst, method, path))
        fut = concurrent.futures.Future()
        if self.raises is not None:
            fut.set_exception(self.raises)
            return fut
        entry = self.routes.get((method, path))
        if entry is None:
            fut.set_result({'status': 404,
                            'body': b'{"detail": "no such route"}'})
        else:
            status, payload = entry
            fut.set_result({'status': status,
                            'body': json.dumps(payload).encode()})
        return fut


def _make_plugin(tmp_path: Path, *, dispatcher=None, host=None,
                 caller=None, instance='federation') -> tuple:
    """Instantiate a federation plugin bound to *tmp_path*."""
    app = FastAPI()
    app.state.endpoint_name    = 'broker'
    app.state.is_broker        = True
    app.state.broker_url       = 'https://localhost:9999'
    app.state.broker_caller    = caller
    app.state.broker_tap       = None
    app.state.endpoint_service = host
    plugin = PluginFederation(app, instance_name=instance,
                              state_root=tmp_path / 'fedroot')
    if dispatcher is not None:
        plugin._dispatcher = dispatcher
    return app, plugin


def _parts(**livenesses) -> dict:
    """Build the ``_participants`` map the plugin keeps off the topology."""
    return {name: {'liveness': live, 'role': 'endpoint'}
            for name, live in livenesses.items()}


def _joinable(tmp_path, **kw):
    """Return (client, plugin, fake dispatcher) with 'ep0'/'ep1' connected."""
    fake = _FakeDispatcher()
    _, plugin = _make_plugin(tmp_path, dispatcher=fake, **kw)
    plugin._participants = _parts(ep0='present', ep1='present')
    return TestClient(plugin._app), plugin, fake


def _alloc_body(name='alpha', endpoint='ep0', **overrides):
    body = {
        'name'        : name,
        'endpoint'    : endpoint,
        'mode'        : 'allocation',
        'site'        : 'HERE',
        'kind'        : 'workstation',
        'capabilities': {'cores': 8, 'gpus': 0, 'mem_gb': 16,
                         'software': ['lammps']},
        'scratch_base': str(_SCRATCH_ROOT / name),
    }
    body.update(overrides)
    return body


def _login_body(name='beta', endpoint='ep1', **overrides):
    body = {
        'name'        : name,
        'endpoint'    : endpoint,
        'mode'        : 'login',
        'capabilities': {'cores': 128, 'gpus': 4, 'software': ['pytorch']},
        'budget'      : {'node_hours': 40.0},
        'scratch_base': str(_SCRATCH_ROOT / name),
        'pool'        : {'queue': 'regular', 'account': 'm1234',
                         'nodes': 2, 'cpus_per_node': 128,
                         'gpus_per_node': 4, 'walltime_sec': 1800,
                         'max_pilots': 2},
    }
    body.update(overrides)
    return body


def _members_body(name='local_b', endpoint='ep1', **overrides):
    """A login join declaring two members — one per class."""
    body = {
        'name'        : name,
        'endpoint'    : endpoint,
        'mode'        : 'login',
        'site'        : 'NERSC',
        'kind'        : 'hpc',
        'scratch_base': str(_SCRATCH_ROOT / name),
        'members'     : [
            {'member': 'cpu', 'queue': 'RM', 'account': 'abc123',
             'nodes': 1, 'cpus_per_node': 128, 'walltime_sec': 3600,
             'max_pilots': 2, 'software': ['lammps', 'pytorch'],
             'attributes': {'site': 'NERSC', 'mem_gb_per_node': 256},
             'budget': {'node_hours': 20}},
            {'member': 'gpu', 'queue': 'GPU', 'account': 'abc123',
             'nodes': 1, 'cpus_per_node': 64, 'gpus_per_node': 8,
             'walltime_sec': 3600, 'max_pilots': 1, 'software': ['pytorch'],
             'class': 'gpu', 'budget': {'node_hours': 8}},
        ],
    }
    body.update(overrides)
    return body


def _join(client, plugin, body):
    return client.post(f'{plugin.namespace}/join/default', json=body)


def _member_decl(fake, pool, member_id):
    """The declaration the dispatcher holds for one member."""
    return (fake.pools.get(pool) or {})[member_id]


def _member_block(member_id, **usage):
    """One member's block of a 121 verbose pool summary: an idle member
    unless *usage* says otherwise.  The co-hosted dispatcher always reports
    one per member, so a canned summary should too."""
    return {'member_id': member_id, 'node_hours_used': 0.0,
            'node_hours_remaining': None, 'pilots_active': 0, **usage}


def _pool_summary(members, pilots=None, history=None):
    """A canned 121 verbose pool summary."""
    return {'multi_member' : True,
            'members'      : list(members),
            'pilots'       : list(pilots or []),
            'pilot_history': list(history or []),
            'pilot_sizes'  : {}}


def _run(coro):
    """Drive one coroutine to completion on a throwaway loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Init / gating
# ---------------------------------------------------------------------------

class TestInit:

    def test_is_enabled_on_broker_only(self):
        app = FastAPI()
        app.state.is_broker = True
        assert PluginFederation.is_enabled(app) is True
        app2 = FastAPI()
        assert PluginFederation.is_enabled(app2) is False

    def test_routes_registered(self, tmp_path):
        app, plugin = _make_plugin(tmp_path)
        pats = [pat.pattern for _, pat, _, _ in app.state.direct_routes]
        ns = plugin.namespace.lstrip('/')
        for frag in (f'{ns}/join/', f'{ns}/leave/', f'{ns}/resources/',
                     f'{ns}/resource/', f'{ns}/pick/', f'{ns}/submit/',
                     f'{ns}/task/'):
            assert any(frag in p for p in pats), f'route {frag} missing'

    def test_starts_empty(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)
        assert plugin._state.resources == {}

    def test_session_class_is_defined(self, tmp_path):
        # _ensure_default_session instantiates it; a plugin without one
        # would 500 on the first request.
        _, plugin = _make_plugin(tmp_path)
        assert plugin.session_class is FederationSession

    def test_ui_module_file_exists(self, tmp_path):
        assert Path(PluginFederation.ui_module).is_file()
        assert Path(PluginFederation.ui_module).name == 'federation.js'

    def test_env_overrides_the_state_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE',
                           str(tmp_path / 'viaenv'))
        app = FastAPI()
        app.state.is_broker = True
        plugin = PluginFederation(app)
        assert str(plugin._state.path).startswith(str(tmp_path / 'viaenv'))

    def test_routes_use_the_default_session(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resources/default')
        assert r.status_code == 200
        assert 'default' in plugin._sessions

    def test_unknown_session_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resources/nope')
        assert r.status_code == 404

    def test_an_empty_federation_registers_nothing(self, tmp_path):
        # parse_pools refuses an empty pool list, so a federation with no
        # resources must not register the fed session at all
        client, plugin, fake = _joinable(tmp_path)
        client.get(f'{plugin.namespace}/resources/default')
        assert fake.calls == []


# ---------------------------------------------------------------------------
# join
# ---------------------------------------------------------------------------

class TestJoin:

    def test_allocation_join_creates_one_implicit_member(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 200, r.text
        rec = r.json()
        assert rec['name']           == 'alpha'
        assert rec['dispatcher_sid'] == FED_SESSION_SID
        assert rec['pool_name']      == 'fed-cpu'
        assert rec['liveness']       == LIVENESS_OK
        assert rec['joined_at'] > 0

        assert [m['member'] for m in rec['members']] == ['default']
        member = rec['members'][0]
        assert member['member_id'] == 'alpha.default'
        assert member['class']     == 'cpu'      # no GPUs declared
        assert member['pool_name'] == 'fed-cpu'
        assert member['queue']     == 'allocation'
        assert member['min_pilots'] == 1         # the pilot IS the allocation
        assert member['max_pilots'] == 1
        assert member['software']  == ['lammps']
        assert member['shared_fs'] is True

    def test_the_session_declares_the_class_pool(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        decls = fake.sessions[FED_SESSION_SID]
        assert [d['name'] for d in decls] == ['fed-cpu']
        assert decls[0]['pool_class']   == 'cpu'
        assert decls[0]['multi_member'] is True
        assert decls[0]['strategy']     == 'conservative'
        assert decls[0]['strategy_config'] == {'min_dwell_sec': 5,
                                               'max_in_flight_submissions': 1}
        assert [m['member_id'] for m in decls[0]['members']] == \
            ['alpha.default']

    def test_the_member_is_posted_to_its_pool(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        assert ('add_member', FED_SESSION_SID, 'fed-cpu', 'alpha.default') \
            in fake.calls
        decl = _member_decl(fake, 'fed-cpu', 'alpha.default')
        assert decl['endpoint_name'] == 'ep0'
        assert decl['queue']         == 'allocation'
        assert decl['default_size']  == 'default'
        size = decl['pilot_sizes']['default']
        assert size['cpus_per_node']    == 8      # from declared cores
        assert size['nodes']            == 1      # no live allocation here
        assert size['walltime_sec']     == 3600
        assert size['rhapsody_backend'] == 'concurrent'
        # software rides in the attributes: that is the dispatcher's
        # vocabulary, it knows nothing about federations
        assert decl['attributes']['software'] == ['lammps']
        assert decl['attributes']['site']     == 'HERE'

    def test_allocation_budget_defaults_to_the_allocation(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, _alloc_body()).json()
        # 1 node x 3600 s, on the member and therefore in the aggregate
        assert rec['budget'] == {'node_hours': 1.0}
        assert rec['members'][0]['budget'] == {'node_hours': 1.0}
        assert rec['usage']['node_hours_remaining'] == 1.0

    def test_declared_allocation_budget_wins(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin,
                    _alloc_body(budget={'node_hours': 0.25})).json()
        assert rec['budget'] == {'node_hours': 0.25}

    def test_login_join_without_members_still_works(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _login_body())
        assert r.status_code == 200, r.text
        rec = r.json()
        assert [m['member'] for m in rec['members']] == ['default']
        member = rec['members'][0]
        assert member['class']     == 'gpu'     # gpus_per_node > 0
        assert member['pool_name'] == 'fed-gpu'
        decl = _member_decl(fake, 'fed-gpu', 'beta.default')
        assert decl['queue']      == 'regular'
        assert decl['account']    == 'm1234'
        assert decl['min_pilots'] == 0          # pilots on demand
        assert decl['max_pilots'] == 2
        size = decl['pilot_sizes']['default']
        assert (size['nodes'], size['cpus_per_node'],
                size['gpus_per_node'], size['walltime_sec']) == \
            (2, 128, 4, 1800)

    def test_login_join_requires_a_budget_without_members(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body.pop('budget')
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'node_hours' in r.text

    def test_login_join_requires_a_pool_block(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body.pop('pool')
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'pool' in r.text

    def test_login_join_rejects_the_queue_sentinel(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool']['queue'] = 'default'
        r = _join(client, plugin, body)
        assert r.status_code == 400

    def test_login_join_rejects_a_bad_pool_size(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool']['nodes'] = 0
        assert _join(client, plugin, body).status_code == 400

    def test_duplicate_name_409(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, _alloc_body()).status_code == 200
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 409

    def test_unknown_endpoint_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(endpoint='nope'))
        assert r.status_code == 404

    def test_lost_endpoint_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        plugin._participants['ep0']['liveness'] = 'lost'
        assert _join(client, plugin,
                     _alloc_body()).status_code == 404

    def test_bad_name_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        for bad in ('Upper', 'has space', 'a/b', ''):
            assert _join(client, plugin,
                         _alloc_body(name=bad)).status_code == 400

    def test_bad_mode_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin,
                     _alloc_body(mode='magic')).status_code == 400

    def test_bad_capabilities_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin,
                     _alloc_body(capabilities={'cores': 'many'})
                     ).status_code == 400

    def test_scratch_base_outside_home_or_tmp_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin,
                     _alloc_body(scratch_base='/etc/orbit')
                     ).status_code == 400

    def test_scratch_base_defaults_under_the_state_root(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _alloc_body()
        body.pop('scratch_base')
        rec = _join(client, plugin, body).json()
        assert rec['scratch_base'] == str(plugin._scratch_for('alpha'))
        assert rec['members'][0]['scratch_base'] == rec['scratch_base']

    def test_a_rejected_join_leaves_nothing_behind(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(name='BAD'))
        assert plugin._state.resources == {}
        assert fake.calls == []

    def test_join_persists(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        assert plugin._state.path.is_file()
        payload = json.loads(plugin._state.path.read_text())
        stored = payload['resources']['local_b']['members']
        assert sorted(stored) == ['cpu', 'gpu']
        assert stored['gpu']['cls'] == 'gpu'

    def test_wire_record_hides_the_pool_config(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, _alloc_body()).json()
        assert 'pool_config' not in rec

    def test_join_body_must_be_an_object(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.post(f'{plugin.namespace}/join/default', json=[1, 2])
        assert r.status_code == 400


class TestJoinWithMembers:

    def test_two_members_land_in_two_class_pools(self, tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        r = _join(client, plugin, _members_body())
        assert r.status_code == 200, r.text
        rec = r.json()
        assert [m['member'] for m in rec['members']] == ['cpu', 'gpu']
        assert [m['member_id'] for m in rec['members']] == \
            ['local_b.cpu', 'local_b.gpu']
        assert [m['class'] for m in rec['members']] == ['cpu', 'gpu']
        assert [m['pool_name'] for m in rec['members']] == \
            ['fed-cpu', 'fed-gpu']

    def test_one_register_session_and_one_add_member_per_member(self,
                                                                tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        registers = [c for c in fake.calls if c[0] == 'register_session']
        assert registers == [('register_session', FED_SESSION_SID)]
        adds = [(c[2], c[3]) for c in fake.calls if c[0] == 'add_member']
        assert adds == [('fed-cpu', 'local_b.cpu'),
                        ('fed-gpu', 'local_b.gpu')]

    def test_the_declaration_carries_every_class(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        decls = {d['name']: d for d in fake.sessions[FED_SESSION_SID]}
        assert set(decls) == {'fed-cpu', 'fed-gpu'}
        assert decls['fed-gpu']['pool_class'] == 'gpu'

    def test_a_second_resource_re_declares_the_full_pool_list(self,
                                                              tmp_path):
        # parse_pools rejects an empty list and _materialise_pool is
        # idempotent by name, so the FULL list is both required and free
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())               # fed-cpu
        _join(client, plugin, _members_body())             # fed-cpu + fed-gpu
        decls = {d['name']: d for d in fake.sessions[FED_SESSION_SID]}
        assert set(decls) == {'fed-cpu', 'fed-gpu'}
        assert sorted(m['member_id'] for m in decls['fed-cpu']['members']) \
            == ['alpha.default', 'local_b.cpu']

    def test_members_share_a_class_pool_across_resources(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _members_body())
        assert fake.members_of('fed-cpu') == ['alpha.default', 'local_b.cpu']
        assert fake.members_of('fed-gpu') == ['local_b.gpu']

    def test_a_member_without_scratch_inherits_the_resource_one(self,
                                                                tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        rec = _join(client, plugin, _members_body()).json()
        shared = str(_SCRATCH_ROOT / 'local_b')
        assert [m['scratch_base'] for m in rec['members']] == \
            [shared, shared]
        assert _member_decl(fake, 'fed-gpu',
                            'local_b.gpu')['scratch_base'] == shared

    def test_a_member_may_declare_its_own_scratch(self, tmp_path):
        body = _members_body()
        own  = str(_SCRATCH_ROOT / 'elsewhere')
        body['members'][1]['scratch_base'] = own
        body['members'][1]['shared_fs']    = False
        client, plugin, fake = _joinable(tmp_path)
        rec = _join(client, plugin, body).json()
        assert rec['members'][1]['scratch_base'] == own
        assert rec['members'][1]['shared_fs'] is False
        assert _member_decl(fake, 'fed-gpu',
                            'local_b.gpu')['shared_fs'] is False

    def test_capabilities_and_budget_become_the_aggregate(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, _members_body()).json()
        assert rec['capabilities']['cores']    == 128 + 64
        assert rec['capabilities']['gpus']     == 8
        assert rec['capabilities']['software'] == ['lammps', 'pytorch']
        assert rec['budget'] == {'node_hours': 28.0}

    def test_the_class_is_derived_when_not_declared(self, tmp_path):
        body = _members_body()
        body['members'][1].pop('class')
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, body).json()
        assert rec['members'][1]['class'] == 'gpu'   # from gpus_per_node

    def test_an_explicit_class_may_name_anything_valid(self, tmp_path):
        body = _members_body()
        body['members'][0]['class'] = 'bigmem'
        client, plugin, fake = _joinable(tmp_path)
        rec = _join(client, plugin, body).json()
        assert rec['members'][0]['pool_name'] == 'fed-bigmem'
        assert 'fed-bigmem' in fake.pools

    def test_a_miscased_class_is_refused_not_coerced(self, tmp_path):
        body = _members_body()
        body['members'][0]['class'] = 'GPU'
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'never coerced' in r.text
        assert fake.calls == []

    def test_a_duplicate_member_name_400(self, tmp_path):
        body = _members_body()
        body['members'][1]['member'] = 'cpu'
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'duplicate member' in r.text
        assert fake.calls == []

    def test_a_dotted_member_name_400(self, tmp_path):
        # the dot separates resource and member in a member id
        body = _members_body()
        body['members'][0]['member'] = 'a.b'
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, body).status_code == 400

    def test_an_empty_members_list_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, _members_body(members=[]))
        assert r.status_code == 400

    def test_members_in_allocation_mode_400(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        body = _alloc_body()
        body['members'] = _members_body()['members']
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'allocation' in r.text
        assert fake.calls == []

    def test_an_unknown_member_field_400(self, tmp_path):
        body = _members_body()
        body['members'][0]['max_piluts'] = 3
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'max_piluts' in r.text

    def test_a_member_needs_its_own_budget(self, tmp_path):
        body = _members_body()
        body['members'][1].pop('budget')
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'member gpu' in r.text

    def test_a_bad_member_size_names_the_member(self, tmp_path):
        body = _members_body()
        body['members'][1]['nodes'] = 0
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert "member gpu.nodes" in r.text

    def test_a_member_queue_sentinel_400(self, tmp_path):
        body = _members_body()
        body['members'][0]['queue'] = 'default'
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, body).status_code == 400

    def test_bad_member_attributes_400(self, tmp_path):
        body = _members_body()
        body['members'][0]['attributes'] = {'nested': {'a': 1}}
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, body).status_code == 400

    def test_declaring_both_a_pool_block_and_members_400(self, tmp_path):
        # the flat 'pool' block is the single-member shorthand; sending both
        # means one of the two is silently ignored, which is worse than a
        # refused join
        body = _members_body()
        body['pool'] = {'queue': 'regular', 'nodes': 1, 'cpus_per_node': 1,
                        'walltime_sec': 60}
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'mutually exclusive' in r.json()['detail']

    def test_an_over_long_pool_plus_member_id_400(self, tmp_path):
        # 121 enforces len(pool_name) + len(member_id) <= 64 in parse_member;
        # catching it here keeps the dispatcher untouched by a bad join
        long_name = 'r' * 60          # 'fed-cpu' + '<name>.cpu' = 71 > 64
        body = _members_body(name=long_name)
        body['scratch_base'] = str(_SCRATCH_ROOT / 'long')
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert '64 characters' in r.json()['detail']
        assert plugin._state.resources == {}
        assert [c for c in fake.calls if c[0] == 'add_member'] == []

    def test_a_member_id_at_the_limit_is_accepted(self, tmp_path):
        # 'fed-cpu' (7) + '<name>.cpu' — the longest name that still fits
        name = 'r' * (64 - len('fed-cpu') - len('.cpu'))
        body = _members_body(name=name)
        body['members'] = [body['members'][0]]
        body['scratch_base'] = str(_SCRATCH_ROOT / 'fits')
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, body).status_code == 200
        assert len('fed-cpu') + len(f'{name}.cpu') == 64

    def test_a_failed_add_member_rolls_the_others_back_with_force(self,
                                                                  tmp_path):
        # a join is all-or-nothing.  ``force`` matters: the first member of
        # a brand-new class pool is also its last, and the dispatcher
        # refuses to remove a last member without it.
        client, plugin, fake = _joinable(tmp_path)
        fake.fail_member = 'local_b.gpu'
        r = _join(client, plugin, _members_body())
        assert r.status_code == 400
        assert plugin._state.resources == {}
        assert fake.removed == [{'pool': 'fed-cpu',
                                 'member_id': 'local_b.cpu',
                                 'cancel_tasks': False, 'force': True,
                                 'fail_unsatisfiable': True}]
        assert fake.members_of('fed-cpu') == []


class TestJoinWithoutDispatcher:

    def test_join_503_when_no_dispatcher_is_hosted(self, tmp_path):
        _, plugin = _make_plugin(tmp_path)        # no host at all
        plugin._participants = _parts(ep0='present')
        client = TestClient(plugin._app)
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 503

    def test_join_503_when_the_host_has_no_dispatcher_plugin(self, tmp_path):
        class _Host:
            plugins = {'something_else': object()}
        _, plugin = _make_plugin(tmp_path, host=_Host())
        plugin._participants = _parts(ep0='present')
        client = TestClient(plugin._app)
        assert _join(client, plugin, _alloc_body()).status_code == 503

    def test_a_failing_dispatcher_register_propagates(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        fake.fail = 'register_session'
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 503
        assert plugin._state.resources == {}


# ---------------------------------------------------------------------------
# resources
# ---------------------------------------------------------------------------

class TestResources:

    def test_lists_both_with_zero_usage(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())
        body = client.get(f'{plugin.namespace}/resources/default').json()
        names = [r['name'] for r in body['resources']]
        assert names == ['alpha', 'beta']       # sorted, deterministic
        for r in body['resources']:
            assert r['usage']['node_hours_used'] == 0.0
            assert r['usage']['pilots_active']   == 0
            assert r['usage']['tasks_running']   == 0
            assert r['members']

    def test_resource_by_name(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        r = client.get(f'{plugin.namespace}/resource/default/alpha')
        assert r.status_code == 200
        assert r.json()['name'] == 'alpha'

    def test_unknown_resource_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.get(f'{plugin.namespace}/resource/default/nope')
        assert r.status_code == 404

    def test_per_member_usage_comes_off_the_dispatcher(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'node_hours_used': 3.5,
             'node_hours_remaining': 16.5, 'pilots_active': 2},
            {'member_id': 'someone.else', 'node_hours_used': 99.0,
             'node_hours_remaining': 1.0, 'pilots_active': 7}])
        fake.details['fed-gpu'] = _pool_summary([
            {'member_id': 'local_b.gpu', 'node_hours_used': 1.0,
             'node_hours_remaining': 7.0, 'pilots_active': 1}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        cpu, gpu = rec['members']
        assert cpu['usage']['node_hours_used']      == 3.5
        assert cpu['usage']['node_hours_remaining'] == 16.5
        assert cpu['usage']['pilots_active']        == 2
        assert gpu['usage']['node_hours_used']      == 1.0
        assert gpu['usage']['pilots_active']        == 1
        # a sibling resource's member in the same pool is not mine
        assert rec['usage']['node_hours_used']      == 4.5
        assert rec['usage']['node_hours_remaining'] == 23.5
        assert rec['usage']['pilots_active']        == 3
        assert rec['usage']['stale'] is False

    def test_node_hours_remaining_never_goes_negative(self, tmp_path):
        # an overspent member has nothing left, not a debt: a negative term
        # would drag the resource sum below its siblings and score the
        # member as worse than empty
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'node_hours_used': 25.0,
             'node_hours_remaining': -5.0, 'pilots_active': 1}])
        fake.details['fed-gpu'] = _pool_summary([
            {'member_id': 'local_b.gpu', 'node_hours_used': 2.0,
             'node_hours_remaining': 6.0, 'pilots_active': 0}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        assert rec['members'][0]['usage']['node_hours_remaining'] == 0.0
        assert rec['usage']['node_hours_remaining'] == 6.0

    def test_an_overspent_member_without_a_remaining_figure_clamps_too(
            self, tmp_path):
        # no dispatcher-side remaining: budget - used is computed here
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            _member_block('local_b.cpu', node_hours_used=25.0)])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        cpu = rec['members'][0]['usage']
        assert cpu['node_hours_used'] > 20.0          # budget is 20
        assert cpu['node_hours_remaining'] == 0.0

    def test_one_dispatcher_call_per_distinct_class_pool(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())        # fed-cpu
        _join(client, plugin, _members_body())      # fed-cpu + fed-gpu
        plugin._detail_cache.clear()
        for rec in plugin._state.resources.values():
            rec.usage.updated_at = 0.0
        client.get(f'{plugin.namespace}/resources/default')
        pools = [c[2] for c in fake.calls if c[0] == 'pool_detail']
        assert sorted(pools) == ['fed-cpu', 'fed-gpu']   # 2, not 3

    def test_a_member_missing_from_the_summary_reads_stale(self, tmp_path):
        # no block for this member: keep the previous numbers, flag stale
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(budget={'node_hours': 4.0}))
        member = plugin._state.resources['alpha'].members['default']
        member.usage.node_hours_used = 2.5
        plugin._apply_member_usage(
            member, _pool_summary([_member_block('other.default')]),
            time.time())
        assert member.usage.stale is True
        assert member.usage.node_hours_used == 2.5

    def test_usage_is_cached_for_two_seconds(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        for _ in range(4):
            client.get(f'{plugin.namespace}/resources/default')
        details = [c for c in fake.calls if c[0] == 'pool_detail']
        assert len(details) == 1

    def test_failed_refresh_keeps_values_and_flags_stale(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(budget={'node_hours': 4.0}))
        rec = plugin._state.resources['alpha']
        member = rec.members['default']
        member.usage.node_hours_used      = 2.5
        member.usage.node_hours_remaining = 1.5
        rec.usage.updated_at = 0.0
        plugin._detail_cache.clear()
        fake.fail = 'pool_detail'
        body = client.get(f'{plugin.namespace}/resource/default/alpha').json()
        assert body['usage']['stale'] is True
        assert body['usage']['node_hours_used'] == 2.5      # not zeroed
        assert body['usage']['node_hours_remaining'] == 1.5

    def test_pilot_failures_surface_and_flip_the_state_to_failing(
            self, tmp_path):
        # the demo case: the endpoint answers topology fine (liveness `ok`)
        # while every pilot it is asked to submit dies at submit time
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        paused = time.time() + 60
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'node_hours_used': 0.0,
             'node_hours_remaining': 20.0, 'pilots_active': 0,
             'last_pilot_error': 'psij error: [Errno 122] Disk quota '
                                 'exceeded',
             'consecutive_pilot_failures': 4, 'paused_until': paused}])
        fake.details['fed-gpu'] = _pool_summary([
            {'member_id': 'local_b.gpu', 'node_hours_used': 1.0,
             'node_hours_remaining': 7.0, 'pilots_active': 1}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        cpu, gpu = rec['members']
        assert 'Disk quota exceeded' in cpu['usage']['pilot_error']
        assert cpu['usage']['pilot_failures'] == 4
        assert cpu['usage']['paused_until']   == paused
        # liveness is untouched -- it still means "can we reach it", and the
        # routing policy is written against it
        assert cpu['liveness'] == 'ok'
        assert cpu['state']    == 'failing'
        # the healthy sibling stays healthy, and says nothing
        assert gpu['usage']['pilot_error'] is None
        assert gpu['state'] == 'ok'
        # one failing member is enough for the resource row to say so
        assert rec['liveness'] == 'ok'
        assert rec['state']    == 'failing'
        assert 'Disk quota exceeded' in rec['usage']['pilot_error']

    def test_a_member_that_still_holds_a_pilot_is_not_failing(self, tmp_path):
        # pilots are running: whatever failed, the site is producing work
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 1,
             'last_pilot_error': 'psij error: transient',
             'consecutive_pilot_failures': 5}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        assert rec['members'][0]['state'] == 'ok'
        # ... and neither is the resource row: its *other* shape (gpu, which
        # this summary says nothing about) is merely `idle`, and one shape
        # resting beside a working one does not make the machine idle
        assert rec['members'][1]['state'] == 'idle'
        assert rec['state'] == 'ok'

    def test_two_failures_are_not_yet_failing(self, tmp_path):
        # below the threshold, and not paused: a blip is not a verdict
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 0,
             'last_pilot_error': 'psij error: transient',
             'consecutive_pilot_failures': 2}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        cpu = rec['members'][0]
        assert cpu['usage']['pilot_failures'] == 2
        assert cpu['state'] == 'ok'          # but the error is still there
        assert 'transient' in cpu['usage']['pilot_error']

    def test_a_recovered_member_drops_the_error(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        member = plugin._state.resources['local_b'].members['cpu']
        member.usage.pilot_error    = 'psij error: old news'
        member.usage.pilot_failures = 3
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 1}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        cpu = rec['members'][0]
        assert cpu['usage']['pilot_error']    is None
        assert cpu['usage']['pilot_failures'] == 0
        assert cpu['state'] == 'ok'

    def test_a_lost_member_stays_lost(self, tmp_path):
        # an unreachable endpoint is lost; that its last pilot also failed
        # is not the headline
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        rec_state = plugin._state.resources['local_b']
        for member in rec_state.member_list():
            member.liveness = 'lost'
        rec_state.liveness = 'lost'
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 0,
             'last_pilot_error': 'psij error: gone',
             'consecutive_pilot_failures': 9}])
        rec_state.usage.updated_at = 0.0
        plugin._detail_cache.clear()

        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        assert rec['members'][0]['state'] == 'lost'
        assert rec['state'] == 'lost'

    def test_task_counts_come_from_the_ledger(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        for tid, state, mid in (('t.1', 'RUNNING', 'local_b.cpu'),
                                ('t.2', 'DONE',    'local_b.cpu'),
                                ('t.3', 'FAILED',  'local_b.gpu'),
                                ('t.4', 'QUEUED',  None)):
            plugin._state.ledger[tid] = SubmitLedgerEntry(
                task_id=tid, resource='local_b', member_id=mid, state=state)
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        rec = client.get(
            f'{plugin.namespace}/resource/default/local_b').json()
        # the undispatched task counts on the resource, on no member
        assert (rec['usage']['tasks_running'], rec['usage']['tasks_done'],
                rec['usage']['tasks_failed']) == (2, 1, 1)
        cpu, gpu = rec['members']
        assert (cpu['usage']['tasks_running'],
                cpu['usage']['tasks_done']) == (1, 1)
        assert gpu['usage']['tasks_failed'] == 1


# ---------------------------------------------------------------------------
# pick
# ---------------------------------------------------------------------------

class TestPick:

    def _two(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(
            name='cpu', capabilities={'cores': 8, 'gpus': 0,
                                      'software': ['lammps']}))
        _join(client, plugin, _login_body(
            name='gpu', capabilities={'cores': 128, 'gpus': 4,
                                      'software': ['pytorch']}))
        return client, plugin, fake

    def _pick(self, client, plugin, requirements):
        return client.post(f'{plugin.namespace}/pick/default',
                           json={'requirements': requirements})

    def test_returns_the_class_and_its_pool(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        r = self._pick(client, plugin, {'gpus': 2, 'software': ['pytorch']})
        assert r.status_code == 200
        body = r.json()
        assert body['class']          == 'gpu'
        assert body['pool']           == 'fed-gpu'
        assert body['dispatcher_sid'] == FED_SESSION_SID
        assert isinstance(body['score'], float)

    def test_the_eligible_members_are_listed_for_display(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        body = self._pick(client, plugin,
                          {'software': ['pytorch']}).json()
        assert body['members'] == [{'member_id': 'gpu.default',
                                    'resource': 'gpu',
                                    'score': body['members'][0]['score'],
                                    'reason': None}]

    def test_the_resource_is_advisory_but_populated(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        body = self._pick(client, plugin, {'gpus': 2}).json()
        assert body['resource'] == 'gpu'

    def test_a_cpu_task_that_also_fits_a_gpu_member_picks_fed_cpu(self,
                                                                  tmp_path):
        # the cheapest-class rule: never burn a GPU allocation on a CPU task
        # while a CPU member is available
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body())      # cpu has pytorch too
        body = self._pick(client, plugin,
                          {'cores': 4, 'software': ['pytorch']}).json()
        assert body['class'] == 'cpu'
        assert body['pool']  == 'fed-cpu'

    def test_no_fit_409_with_reasons_keyed_by_member(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        r = self._pick(client, plugin, {'gpus': 99})
        assert r.status_code == 409
        body = r.json()
        assert body['detail'] == 'no resource satisfies requirements'
        assert set(body['reasons']) == {'cpu.default', 'gpu.default'}
        assert 'gpus' in body['reasons']['cpu.default']

    def test_a_lost_resource_is_not_picked(self, tmp_path):
        client, plugin, _ = self._two(tmp_path)
        rec = plugin._state.resources['gpu']
        rec.liveness = LIVENESS_LOST
        rec.members['default'].liveness = LIVENESS_LOST
        r = self._pick(client, plugin, {'gpus': 2})
        assert r.status_code == 409
        assert 'liveness' in r.json()['reasons']['gpu.default']

    def test_empty_federation_409(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = self._pick(client, plugin, {})
        assert r.status_code == 409
        assert r.json()['reasons'] == {}

    def test_bad_requirements_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = client.post(f'{plugin.namespace}/pick/default',
                        json={'requirements': 'many'})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------

class TestSubmit:

    def _one(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        return client, plugin, fake

    def _submit(self, client, plugin, task, requirements=None):
        return client.post(f'{plugin.namespace}/submit/default',
                           json={'task': task,
                                 'requirements': requirements or {}})

    def test_lands_the_task_in_the_class_pool(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/echo', 'hi']})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body['pool']             == 'fed-cpu'
        assert body['class']            == 'cpu'
        assert body['dispatcher_sid']   == FED_SESSION_SID
        assert body['resource']         == 'alpha'      # advisory
        assert body['member']           is None         # until dispatch
        assert body['members_eligible'] == ['alpha.default']
        assert body['task']['state']    == 'QUEUED'

        sid, payload = fake.submitted[0]
        assert sid                == FED_SESSION_SID
        assert payload['pool']    == 'fed-cpu'
        assert payload['task_id'] == 't.1'
        assert payload['cmd']     == ['/bin/echo', 'hi']

    def test_no_cwd_is_ever_sent(self, tmp_path):
        # the dispatcher assigns it at dispatch, from the member that runs
        # the task — the only correct answer once a pool mixes members
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true']})
        assert 'cwd' not in fake.submitted[0][1]

    def test_a_client_supplied_cwd_is_refused(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/true'],
                          'cwd': str(_SCRATCH_ROOT / 'work')})
        assert r.status_code == 400
        assert 'cwd' in r.text
        assert fake.submitted == []

    def test_inputs_outputs_and_priority_pass_through(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {
            'task_id': 't.1', 'cmd': ['/bin/true'], 'priority': 3,
            'inputs': ['a.json'], 'outputs': ['b.json']})
        payload = fake.submitted[0][1]
        assert payload['priority'] == 3
        assert payload['inputs']   == ['a.json']
        assert payload['outputs']  == ['b.json']

    def test_a_non_numeric_priority_400(self, tmp_path):
        # a declaration error, not a 500 out of int('high')
        client, plugin, fake = self._one(tmp_path)
        for bad in ('high', ['1'], {'p': 1}, True):
            r = self._submit(client, plugin, {'task_id': 't.1',
                                              'cmd': ['/bin/true'],
                                              'priority': bad})
            assert r.status_code == 400, bad
            assert 'priority' in r.text
        assert fake.submitted == []

    def test_inputs_b64_rides_through_verbatim(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {
            'task_id': 't.1', 'cmd': ['/bin/true'],
            'inputs': ['md.json'], 'inputs_b64': {'md.json': 'e30='}})
        assert fake.submitted[0][1]['inputs_b64'] == {'md.json': 'e30='}

    def test_a_non_object_inputs_b64_is_refused(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        for bad in ('e30=', ['a'], {'md.json': 7}):
            r = self._submit(client, plugin, {'task_id': 't.1',
                                              'cmd': ['/bin/true'],
                                              'inputs_b64': bad})
            assert r.status_code == 400
        assert fake.submitted == []

    def test_requirements_are_forwarded_minus_node_hours(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin,
                     {'task_id': 't.1', 'cmd': ['/bin/true']},
                     {'cores': 2, 'gpus': 0, 'software': ['lammps'],
                      'labels': {'site': 'HERE'}, 'ranks': 1, 'mpi': False,
                      'node_hours': 0.1})
        forwarded = fake.submitted[0][1]['requirements']
        assert forwarded == {'cores': 2, 'gpus': 0, 'software': ['lammps'],
                             'labels': {'site': 'HERE'}, 'ranks': 1,
                             'mpi': False}
        assert 'node_hours' not in forwarded

    def test_an_empty_requirements_object_is_not_sent(self, tmp_path):
        # the wire body for a caller with no requirements stays as it was
        client, plugin, fake = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true']})
        assert 'requirements' not in fake.submitted[0][1]
        self._submit(client, plugin, {'task_id': 't.2', 'cmd': ['/bin/true']},
                     {'node_hours': 0.5})
        assert 'requirements' not in fake.submitted[1][1]

    def test_a_dispatcher_400_propagates_verbatim(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        fake.submit_error = HTTPException(
            status_code=400,
            detail='no member satisfies the task requirements: software '
                   'missing: pytorch')
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/true']})
        assert r.status_code == 400
        assert 'software missing: pytorch' in r.text
        assert 't.1' not in plugin._state.ledger

    def test_records_a_ledger_entry_with_a_late_placement(self, tmp_path):
        client, plugin, _ = self._one(tmp_path)
        self._submit(client, plugin, {'task_id': 't.1', 'cmd': ['/bin/true']})
        entry = plugin._state.ledger['t.1']
        assert entry.resource       == 'alpha'      # advisory
        assert entry.member_id      is None         # until dispatch
        assert entry.pool           == 'fed-cpu'
        assert entry.cls            == 'cpu'
        assert entry.dispatcher_sid == FED_SESSION_SID
        assert entry.state          == 'QUEUED'
        assert entry.submitted_at > 0

    def test_unsatisfiable_requirements_409(self, tmp_path):
        client, plugin, fake = self._one(tmp_path)
        r = self._submit(client, plugin,
                         {'task_id': 't.1', 'cmd': ['/bin/true']},
                         {'gpus': 8})
        assert r.status_code == 409
        assert 'reasons' in r.json()
        assert fake.submitted == []

    def test_missing_task_id_or_cmd_400(self, tmp_path):
        client, plugin, _ = self._one(tmp_path)
        assert self._submit(client, plugin,
                            {'cmd': ['/bin/true']}).status_code == 400
        assert self._submit(client, plugin,
                            {'task_id': 't.1'}).status_code == 400
        assert self._submit(client, plugin,
                            {'task_id': 't.1',
                             'cmd': []}).status_code == 400


# ---------------------------------------------------------------------------
# task
# ---------------------------------------------------------------------------

class TestTask:

    def _submitted(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        client.post(f'{plugin.namespace}/submit/default',
                    json={'task': {'task_id': 't.1',
                                   'cmd': ['/bin/true']}})
        return client, plugin, fake

    def test_before_dispatch_the_advisory_placement_stands(self, tmp_path):
        client, plugin, _ = self._submitted(tmp_path)
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['task_id']   == 't.1'
        assert body['resource']  == 'alpha'
        assert body['member']    is None
        assert body['member_id'] is None
        assert body['class']     == 'cpu'

    def test_the_dispatcher_member_id_becomes_the_truth(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1']['member_id'] = 'other_res.gpu'
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['member_id'] == 'other_res.gpu'
        assert body['member']    == 'gpu'
        assert body['resource']  == 'other_res'
        # ... and the ledger is corrected, not just the answer
        assert plugin._state.ledger['t.1'].resource  == 'other_res'
        assert plugin._state.ledger['t.1'].member_id == 'other_res.gpu'

    def test_a_re_queued_task_loses_its_member(self, tmp_path):
        # the dispatcher clears member_id beside pilot_id when it re-queues a
        # task off a lost pilot (121 §7.3); the ledger must follow, or
        # tasks_running keeps counting it on a member that is not running it
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1']['member_id'] = 'alpha.default'
        fake.tasks['t.1']['state']     = 'RUNNING'
        client.get(f'{plugin.namespace}/task/default/t.1')
        assert plugin._state.ledger['t.1'].member_id == 'alpha.default'
        assert plugin._state.resources['alpha'].members[
            'default'].usage.tasks_running == 0        # not refreshed yet

        fake.tasks['t.1']['member_id'] = None
        fake.tasks['t.1']['state']     = 'QUEUED'
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['member_id'] is None
        assert body['member']    is None
        entry = plugin._state.ledger['t.1']
        assert entry.member_id is None
        assert entry.resource  == 'alpha'      # advisory, and still true
        assert plugin._state.member_task_counts('alpha.default') == (0, 0, 0)

    def test_a_terminal_task_keeps_its_member(self, tmp_path):
        # a DONE task reports where it ran; only a re-queue clears the member
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1']['member_id'] = 'alpha.default'
        client.get(f'{plugin.namespace}/task/default/t.1')
        fake.tasks['t.1']['member_id'] = None
        fake.tasks['t.1']['state']     = 'DONE'
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['member_id'] == 'alpha.default'
        assert plugin._state.ledger['t.1'].member_id == 'alpha.default'

    def test_a_dotted_resource_name_splits_on_the_last_dot(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1']['member_id'] = 'site.cluster.a.gpu'
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['resource'] == 'site.cluster.a'
        assert body['member']   == 'gpu'

    def test_a_task_whose_resource_left_reports_a_null_resource(self,
                                                                tmp_path):
        client, plugin, _fake = self._submitted(tmp_path)
        client.post(f'{plugin.namespace}/leave/default/alpha', json={})
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['resource']  is None
        assert body['member_id'] is None
        assert body['state']     == 'QUEUED'      # still queued, not failed

    def test_a_poll_re_points_a_task_whose_resource_left(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        client.post(f'{plugin.namespace}/leave/default/alpha', json={})
        fake.tasks['t.1']['member_id'] = 'local_c.cpu'
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['resource'] == 'local_c'
        assert plugin._state.ledger['t.1'].resource == 'local_c'

    def test_ledger_state_follows_the_dispatcher(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'DONE', 'exit_code': 0,
                                  'finished_at': 1234.0})
        client.get(f'{plugin.namespace}/task/default/t.1')
        entry = plugin._state.ledger['t.1']
        assert entry.state       == 'DONE'
        assert entry.finished_at == 1234.0

    def test_child_endpoint_reported_while_the_pilot_lives(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'RUNNING', 'pilot_id': 'p.1'})
        fake.details['fed-cpu'] = _pool_summary(
            [], pilots=[{'pid': 'p.1',
                         'child_endpoint_name': 'fed-cpu_alpha.default_p.1'}])
        plugin._detail_cache.clear()      # submit already primed the 2 s cache
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert body['child_endpoint'] == 'fed-cpu_alpha.default_p.1'

    def test_no_child_endpoint_once_the_pilot_is_gone(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'DONE', 'pilot_id': 'p.1'})
        fake.details['fed-cpu'] = _pool_summary([])
        plugin._detail_cache.clear()
        body = client.get(f'{plugin.namespace}/task/default/t.1').json()
        assert 'child_endpoint' not in body

    def test_child_endpoint_lookup_reuses_the_cached_pool_detail(self,
                                                                 tmp_path):
        # A caller polling a task at 1 Hz must not multiply dispatcher calls:
        # usage refresh and the child lookup share one 2 s-cached summary.
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks['t.1'].update({'state': 'RUNNING', 'pilot_id': 'p.1'})
        before = len([c for c in fake.calls if c[0] == 'pool_detail'])
        for _ in range(3):
            client.get(f'{plugin.namespace}/task/default/t.1')
        after = len([c for c in fake.calls if c[0] == 'pool_detail'])
        assert after == before

    def test_unknown_task_404(self, tmp_path):
        client, plugin, _ = self._submitted(tmp_path)
        r = client.get(f'{plugin.namespace}/task/default/nope')
        assert r.status_code == 404

    def test_dispatcher_404_maps_through(self, tmp_path):
        client, plugin, fake = self._submitted(tmp_path)
        fake.tasks.pop('t.1')
        r = client.get(f'{plugin.namespace}/task/default/t.1')
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# leave
# ---------------------------------------------------------------------------

class TestLeave:

    def _with_tasks(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        for tid, state, mid in (('t.run', 'RUNNING', 'local_b.cpu'),
                                ('t.q',   'QUEUED',  None),
                                ('t.done', 'DONE',   'local_b.gpu')):
            plugin._state.ledger[tid] = SubmitLedgerEntry(
                task_id=tid, resource='local_b', member_id=mid,
                pool='fed-cpu', dispatcher_sid=FED_SESSION_SID, state=state,
                cls='cpu')
        return client, plugin, fake

    def _leave(self, client, plugin, name='local_b', **body):
        return client.post(f'{plugin.namespace}/leave/default/{name}',
                           json=body)

    def test_every_member_is_removed_from_its_pool(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        r = self._leave(client, plugin)
        assert r.status_code == 200
        assert [(c['pool'], c['member_id']) for c in fake.removed] == \
            [('fed-cpu', 'local_b.cpu'), ('fed-gpu', 'local_b.gpu')]
        assert plugin._state.resources == {}

    def test_the_fed_session_is_never_unregistered(self, tmp_path):
        # R4: the single fed session holds every other resource's class
        # pools; unregistering it would kill all of them
        client, plugin, fake = self._with_tasks(tmp_path)
        self._leave(client, plugin)
        verbs = [c[0] for c in fake.calls]
        assert 'unregister_session' not in verbs
        assert 'cancel_all'         not in verbs

    def test_an_explicit_leave_lets_the_dispatcher_fail_orphans(self,
                                                                tmp_path):
        # "gone", not "blinked": fail_unsatisfiable stays at its default
        client, plugin, fake = self._with_tasks(tmp_path)
        self._leave(client, plugin)
        assert all(c['force'] is True and c['fail_unsatisfiable'] is True
                   and c['cancel_tasks'] is False for c in fake.removed)

    def test_live_ledger_entries_survive_and_are_re_pointed(self, tmp_path):
        # a re-queued task keeps running on a sibling member, and
        # GET task/... 404s without its entry — which a campaign runner
        # reads as a hard failure
        client, plugin, _fake = self._with_tasks(tmp_path)
        self._leave(client, plugin)
        assert set(plugin._state.ledger) == {'t.run', 't.q'}
        for tid in ('t.run', 't.q'):
            entry = plugin._state.ledger[tid]
            assert entry.resource  is None
            assert entry.member_id is None
            assert entry.pool           == 'fed-cpu'
            assert entry.dispatcher_sid == FED_SESSION_SID

    def test_cancel_tasks_is_the_full_teardown(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        r = self._leave(client, plugin, cancel_tasks=True)
        assert r.status_code == 200
        assert all(c['cancel_tasks'] is True for c in fake.removed)
        assert plugin._state.ledger == {}

    def test_cancel_tasks_cancels_the_entries_the_drain_cannot_see(
            self, tmp_path):
        # del_member(cancel_tasks=True) only fails what was on the removed
        # member's *pilots* (121 §7.4).  't.q' is QUEUED with no pilot and no
        # member — merely attributed to this resource by the advisory submit
        # — so without an explicit cancel it would survive the drain and then
        # lose its ledger entry: a live task answering 404.
        client, plugin, fake = self._with_tasks(tmp_path)
        body = self._leave(client, plugin, cancel_tasks=True).json()
        assert sorted(t for _sid, t in fake.canceled) == ['t.q', 't.run']
        assert all(sid == FED_SESSION_SID for sid, _t in fake.canceled)
        assert body['tasks_cancelled'] == 2          # 't.done' was terminal
        assert plugin._state.ledger == {}

    def test_a_plain_leave_cancels_nothing(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        body = self._leave(client, plugin).json()
        assert fake.canceled == []
        assert body['tasks_cancelled'] == 0

    def test_a_failing_cancel_still_tears_the_resource_down(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        fake.fail = 'cancel_task'
        r = self._leave(client, plugin, cancel_tasks=True)
        assert r.status_code == 200
        assert r.json()['tasks_cancelled'] == 2
        assert plugin._state.ledger    == {}
        assert plugin._state.resources == {}

    def test_the_drain_counters_are_summed_over_the_members(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        fake.drain['local_b.cpu'] = {'tasks_requeued': 2, 'tasks_failed': 1}
        fake.drain['local_b.gpu'] = {'tasks_requeued': 1, 'tasks_failed': 0}
        body = self._leave(client, plugin).json()
        assert body == {'resource': 'local_b', 'ok': True,
                        'members_removed': 2, 'tasks_requeued': 3,
                        'tasks_failed': 1, 'tasks_cancelled': 0}

    def test_an_emptied_class_pool_is_left_in_place(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        self._leave(client, plugin)
        assert 'fed-gpu' in fake.pools           # inert, but reusable
        assert fake.members_of('fed-gpu') == []

    def test_unknown_resource_404(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = self._leave(client, plugin, name='nope')
        assert r.status_code == 404

    def test_leave_survives_a_failing_dispatcher(self, tmp_path):
        # A resource whose endpoint already died must still be removable.
        client, plugin, fake = self._with_tasks(tmp_path)
        fake.fail = 'del_member'
        r = self._leave(client, plugin)
        assert r.status_code == 200
        assert r.json()['members_removed'] == 0
        assert plugin._state.resources == {}

    def test_a_partial_removal_is_reported_not_swallowed(self, tmp_path):
        # 'ok: true, members_removed: 0' said nothing about the dispatcher
        # still believing in those members; a caller needs to know
        client, plugin, fake = self._with_tasks(tmp_path)
        fake.fail = 'del_member'
        body = self._leave(client, plugin).json()
        assert body['ok'] is False
        assert len(body['errors']) == 2
        assert 'local_b.cpu' in body['errors'][0]
        assert 'local_b.gpu' in body['errors'][1]

    def test_a_clean_leave_reports_no_errors_key(self, tmp_path):
        client, plugin, _fake = self._with_tasks(tmp_path)
        body = self._leave(client, plugin).json()
        assert body['ok'] is True
        assert 'errors' not in body

    def test_rejoin_after_leave_is_allowed(self, tmp_path):
        client, plugin, _ = self._with_tasks(tmp_path)
        self._leave(client, plugin)
        assert _join(client, plugin, _members_body()).status_code == 200

    def test_a_sibling_resource_keeps_its_members(self, tmp_path):
        client, plugin, fake = self._with_tasks(tmp_path)
        _join(client, plugin, _alloc_body())              # fed-cpu too
        self._leave(client, plugin)
        assert fake.members_of('fed-cpu') == ['alpha.default']
        assert set(plugin._state.resources) == {'alpha'}


# ---------------------------------------------------------------------------
# Topology: liveness, detach, restart re-attach
# ---------------------------------------------------------------------------

def _topo(**livenesses):
    return {name: {'role': 'endpoint', 'plugins': {}, 'liveness': live}
            for name, live in livenesses.items()}


class TestTopology:

    @pytest.mark.asyncio
    async def test_liveness_is_inherited_by_the_resource_and_its_members(
            self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        rec = plugin._state.resources['local_b']

        await plugin.on_topology_change(_topo(ep0='suspect'))
        assert rec.liveness == LIVENESS_SUSPECT
        assert all(m.liveness == LIVENESS_SUSPECT
                   for m in rec.member_list())
        await plugin.on_topology_change(_topo(ep0='present'))
        assert rec.liveness == LIVENESS_OK
        await plugin.on_topology_change(_topo(ep0='lost'))
        assert rec.liveness == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_a_lost_endpoint_removes_its_members_softly(self,
                                                              tmp_path):
        # fail_unsatisfiable=False: a lost endpoint is very often back in a
        # minute, so a task only this member could run must wait, not fail
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        fake.removed.clear()
        await plugin.on_topology_change(_topo(ep0='lost'))
        assert [(c['member_id'], c['force'], c['fail_unsatisfiable'],
                 c['cancel_tasks']) for c in fake.removed] == \
            [('local_b.cpu', True, False, False),
             ('local_b.gpu', True, False, False)]
        assert plugin._attached == set()
        # the fed session itself is untouched
        assert FED_SESSION_SID in fake.sessions

    @pytest.mark.asyncio
    async def test_a_suspect_endpoint_touches_no_dispatcher_route(self,
                                                                  tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        before = list(fake.calls)
        await plugin.on_topology_change(_topo(ep0='suspect'))
        assert fake.calls == before             # a blip tears down nothing
        assert plugin._attached == {'local_b.cpu', 'local_b.gpu'}

    @pytest.mark.asyncio
    async def test_a_returning_endpoint_re_adds_its_members(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        await plugin.on_topology_change(_topo(ep0='lost'))
        await plugin.on_topology_change(_topo(ep0='present'))
        assert fake.members_of('fed-cpu') == ['local_b.cpu']
        assert fake.members_of('fed-gpu') == ['local_b.gpu']
        assert plugin._state.resources['local_b'].liveness == LIVENESS_OK

    @pytest.mark.asyncio
    async def test_a_past_allocation_end_keeps_the_member_detached(
            self, tmp_path, caplog):
        """The same endpoint name back in a *new* allocation: re-declared
        with the old end_time, its adopted pilot would get a past deadline
        and never receive a task.  Only a new join knows the new end."""
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        rec = plugin._state.resources['local_b']
        await plugin.on_topology_change(_topo(ep0='lost'))
        for m in rec.member_list():
            m.end_time = time.time() - 10
        fake.calls.clear()

        with caplog.at_level(logging.WARNING):
            await plugin.on_topology_change(_topo(ep0='present'))
            await plugin.on_topology_change(_topo(ep0='present'))
        assert [c for c in fake.calls if c[0] == 'add_member'] == []
        assert plugin._attached == set()
        assert rec.liveness == LIVENESS_LOST
        assert all(m.liveness == LIVENESS_LOST for m in rec.member_list())
        # once per member, not on every topology delivery
        ended = [r for r in caplog.records
                 if 'allocation ended' in r.getMessage()]
        assert len(ended) == 2
        assert {mid for mid in ('local_b.cpu', 'local_b.gpu')
                for r in ended if mid in r.getMessage()} == \
            {'local_b.cpu', 'local_b.gpu'}

    @pytest.mark.asyncio
    async def test_a_vanished_class_pool_is_re_declared_first(self,
                                                              tmp_path):
        # every member of a class may have left while the endpoint was down
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body(endpoint='ep0'))
        await plugin.on_topology_change(_topo(ep0='lost'))
        fake.pools.pop('fed-gpu')
        plugin._pools.discard('fed-gpu')
        fake.calls.clear()
        await plugin.on_topology_change(_topo(ep0='present'))
        # the cpu member's pool still exists, so only the gpu one needs the
        # re-declaration — and it must come before that member is POSTed
        order = [c[0] if c[0] != 'add_member' else f'add:{c[3]}'
                 for c in fake.calls]
        assert order.index('register_session') < \
            order.index('add:local_b.gpu')
        assert fake.members_of('fed-gpu') == ['local_b.gpu']


class TestRestartReattach:

    def _restart(self, tmp_path, fake):
        """Build a second plugin over the same state root (= a restart)."""
        app = FastAPI()
        app.state.endpoint_name    = 'broker'
        app.state.is_broker        = True
        app.state.broker_caller    = None
        app.state.endpoint_service = None
        plugin = PluginFederation(app, instance_name='federation',
                                  state_root=tmp_path / 'fedroot')
        plugin._dispatcher = fake
        return plugin

    @pytest.mark.asyncio
    async def test_replay_registers_fed_once_with_the_full_pool_list(
            self, tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _members_body())

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        assert set(plugin2._state.resources) == {'alpha', 'local_b'}
        assert fake2.sessions == {}          # nothing until topology arrives

        await plugin2.on_topology_change(_topo(ep0='present', ep1='present'))
        assert [c for c in fake2.calls if c[0] == 'register_session'] == \
            [('register_session', FED_SESSION_SID)]
        decls = {d['name']: d for d in fake2.sessions[FED_SESSION_SID]}
        assert set(decls) == {'fed-cpu', 'fed-gpu'}
        assert sorted(m['member_id'] for m in decls['fed-cpu']['members']) \
            == ['alpha.default', 'local_b.cpu']
        for rec in plugin2._state.resources.values():
            assert rec.liveness == LIVENESS_OK

    @pytest.mark.asyncio
    async def test_replay_re_posts_every_member(self, tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _members_body())

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present', ep1='present'))
        adds = sorted(c[3] for c in fake2.calls if c[0] == 'add_member')
        assert adds == ['alpha.default', 'local_b.cpu', 'local_b.gpu']
        assert plugin2._attached == set(adds)

    @pytest.mark.asyncio
    async def test_replay_skips_a_member_whose_allocation_ended(
            self, tmp_path, caplog):
        """Re-POSTed with its stored end_time, the endpoint back in a new
        allocation would be adopted with a past deadline and never
        receive a task -- the same guard as the lost/present re-attach."""
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _members_body())
        for m in plugin._state.resources['alpha'].member_list():
            m.end_time = time.time() - 10
        plugin._state.save()

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        with caplog.at_level(logging.WARNING):
            await plugin2.on_topology_change(
                _topo(ep0='present', ep1='present'))
            await plugin2.on_topology_change(
                _topo(ep0='present', ep1='present'))
        adds = sorted(c[3] for c in fake2.calls if c[0] == 'add_member')
        assert adds == ['local_b.cpu', 'local_b.gpu']
        assert plugin2._attached == set(adds)
        rec = plugin2._state.resources['alpha']
        assert rec.liveness == LIVENESS_LOST
        assert all(m.liveness == LIVENESS_LOST for m in rec.member_list())
        ended = [r for r in caplog.records
                 if 'allocation ended' in r.getMessage()]
        assert len(ended) == 1
        assert 'alpha.default' in ended[0].getMessage()

    @pytest.mark.asyncio
    async def test_a_re_post_onto_a_live_dispatcher_is_a_no_op(self,
                                                               tmp_path):
        # the dispatcher already replayed its pools with their members; the
        # federation re-POSTs anyway, and nothing changes
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        before = {p: dict(m) for p, m in fake.pools.items()}
        plugin2 = self._restart(tmp_path, fake)
        await plugin2.on_topology_change(_topo(ep1='present'))
        assert fake.pools == before

    @pytest.mark.asyncio
    async def test_a_resource_whose_endpoint_is_gone_is_released(self,
                                                                 tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _members_body())

        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))

        # every stored member is re-attached first ...
        assert sorted(c[3] for c in fake2.calls if c[0] == 'add_member') == \
            ['alpha.default', 'local_b.cpu', 'local_b.gpu']
        # ... then the ones whose endpoint never came back are dropped
        assert sorted(c['member_id'] for c in fake2.removed) == \
            ['local_b.cpu', 'local_b.gpu']
        assert fake2.members_of('fed-cpu') == ['alpha.default']
        assert plugin2._state.resources['alpha'].liveness   == LIVENESS_OK
        assert plugin2._state.resources['local_b'].liveness == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_replay_runs_only_once(self, tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        fake2 = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        n = len([c for c in fake2.calls if c[0] == 'register_session'])
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert len([c for c in fake2.calls
                    if c[0] == 'register_session']) == n

    @pytest.mark.asyncio
    async def test_a_failing_add_member_marks_the_resource_lost(self,
                                                                tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        fake2 = _FakeDispatcher()
        fake2.fail = 'add_member'
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_a_failing_register_marks_everything_lost(self, tmp_path):
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        fake2 = _FakeDispatcher()
        fake2.fail = 'register_session'
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_LOST

    @pytest.mark.asyncio
    async def test_a_pre122_state_file_re_attaches_as_an_endpoint(self,
                                                                 tmp_path):
        """The upgrade case: a `state.json` from a 121 broker carries no
        `pilot` on its allocation member.  Re-POSTing that as `submit` would
        launch the psij child again, on the compute node the endpoint already
        owns."""
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())

        raw    = json.loads(plugin._state.path.read_text())
        member = raw['resources']['alpha']['members']['default']
        for key in ('pilot', 'end_time', 'endpoint'):
            member.pop(key, None)
        plugin._state.path.write_text(json.dumps(raw))

        fake2   = _FakeDispatcher()
        plugin2 = self._restart(tmp_path, fake2)
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert _member_decl(fake2, 'fed-cpu', 'alpha.default')['pilot'] \
            == 'endpoint'
        assert plugin2._state.resources['alpha'].members[
            'default'].endpoint == 'ep0'


class TestUpgradeFromPre08:
    """A state.json written before class pools, replayed once."""

    _PRE08 = {
        'resources': {
            'legacy': {
                'name': 'legacy', 'endpoint': 'ep0', 'mode': 'login',
                'site': 'NERSC', 'kind': 'hpc',
                'capabilities': {'cores': 128, 'gpus': 0, 'mem_gb': 256,
                                 'software': ['lammps']},
                'budget': {'node_hours': 40.0},
                'scratch_base': '/tmp/orbit-fed-test/legacy',
                'joined_at': 1.0,
                'dispatcher_sid': 'fed-legacy', 'pool_name': 'fed-legacy',
                'liveness': 'ok',
                'pool_config': {
                    'name': 'fed-legacy', 'endpoint_name': 'ep0',
                    'queue': 'regular', 'account': 'm1234',
                    'min_pilots': 0, 'max_pilots': 2,
                    'default_size': 'default',
                    'pilot_sizes': {'default': {
                        'nodes': 2, 'cpus_per_node': 64, 'gpus_per_node': 0,
                        'walltime_sec': 1800,
                        'rhapsody_backend': 'concurrent'}}},
            }},
        'ledger': {
            't.live': {'task_id': 't.live', 'resource': 'legacy',
                       'pool': 'fed-legacy', 'dispatcher_sid': 'fed-legacy',
                       'state': 'RUNNING'},
            't.old' : {'task_id': 't.old', 'resource': 'legacy',
                       'pool': 'fed-legacy', 'dispatcher_sid': 'fed-legacy',
                       'state': 'DONE'},
        },
    }

    def _plugin(self, tmp_path):
        state = tmp_path / 'fedroot' / 'federation' / 'state.json'
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(json.dumps(self._PRE08))
        fake = _FakeDispatcher()
        _, plugin = _make_plugin(tmp_path, dispatcher=fake)
        return plugin, fake

    def test_the_stored_record_derives_one_member(self, tmp_path):
        plugin, _ = self._plugin(tmp_path)
        rec = plugin._state.resources['legacy']
        assert list(rec.members) == ['default']
        assert rec.members['default'].cls       == 'cpu'
        assert rec.members['default'].pool_name == 'fed-cpu'

    @pytest.mark.asyncio
    async def test_the_legacy_session_is_re_owned_then_released(self,
                                                                tmp_path):
        # order matters: unregister_session on a sid the dispatcher does not
        # know is a 404 that tears down nothing, leaving orphan pilots
        plugin, fake = self._plugin(tmp_path)
        await plugin.on_topology_change(_topo(ep0='present'))
        verbs = [(c[0], c[1]) for c in fake.calls
                 if c[0] in ('register_session', 'unregister_session')]
        assert verbs[0] == ('register_session',   'fed-legacy')
        assert verbs[1] == ('unregister_session', 'fed-legacy')
        assert verbs[2] == ('register_session',   FED_SESSION_SID)

    @pytest.mark.asyncio
    async def test_the_legacy_pool_declaration_is_replayed_verbatim(
            self, tmp_path):
        plugin, fake = self._plugin(tmp_path)
        seen     = {}
        original = fake.register_session

        async def _spy(sid, pools):
            seen[sid] = pools
            return await original(sid, pools)

        fake.register_session = _spy
        await plugin.on_topology_change(_topo(ep0='present'))
        assert seen['fed-legacy'] == \
            [self._PRE08['resources']['legacy']['pool_config']]

    @pytest.mark.asyncio
    async def test_the_member_joins_its_class_pool(self, tmp_path):
        plugin, fake = self._plugin(tmp_path)
        await plugin.on_topology_change(_topo(ep0='present'))
        assert fake.members_of('fed-cpu') == ['legacy.default']
        assert plugin._state.resources['legacy'].dispatcher_sid == \
            FED_SESSION_SID
        assert plugin._state.resources['legacy'].pool_name == 'fed-cpu'

    @pytest.mark.asyncio
    async def test_live_tasks_of_the_old_pools_are_failed(self, tmp_path):
        # their pool has just been torn down; the task cannot be recovered
        plugin, _fake = self._plugin(tmp_path)
        await plugin.on_topology_change(_topo(ep0='present'))
        live = plugin._state.ledger['t.live']
        assert live.state  == 'FAILED'
        assert live.detail == 'the federation was upgraded'
        assert live.finished_at

    @pytest.mark.asyncio
    async def test_polling_an_upgraded_task_answers_from_the_ledger(
            self, tmp_path):
        # its 'fed-legacy' session was just released, so asking the
        # dispatcher would be a 404 on a sid it no longer holds — the
        # ledger's FAILED verdict is the only truthful answer
        plugin, fake = self._plugin(tmp_path)
        client = TestClient(plugin._app)
        await plugin.on_topology_change(_topo(ep0='present'))
        fake.calls.clear()

        r = client.get(f'{plugin.namespace}/task/default/t.live')
        assert r.status_code == 200
        body = r.json()
        assert body['state']   == 'FAILED'
        assert body['detail']  == 'the federation was upgraded'
        assert body['task_id'] == 't.live'
        assert body['finished_at']
        assert [c for c in fake.calls if c[0] == 'task'] == []

    @pytest.mark.asyncio
    async def test_a_terminal_legacy_task_is_answered_the_same_way(
            self, tmp_path):
        plugin, fake = self._plugin(tmp_path)
        client = TestClient(plugin._app)
        await plugin.on_topology_change(_topo(ep0='present'))
        fake.calls.clear()
        body = client.get(f'{plugin.namespace}/task/default/t.old').json()
        assert body['state'] == 'DONE'
        assert 'detail' not in body
        assert [c for c in fake.calls if c[0] == 'task'] == []

    @pytest.mark.asyncio
    async def test_terminal_history_is_kept(self, tmp_path):
        plugin, _fake = self._plugin(tmp_path)
        await plugin.on_topology_change(_topo(ep0='present'))
        assert plugin._state.ledger['t.old'].state  == 'DONE'
        assert plugin._state.ledger['t.old'].detail == ''

    @pytest.mark.asyncio
    async def test_the_upgrade_runs_only_once(self, tmp_path):
        plugin, fake = self._plugin(tmp_path)
        await plugin.on_topology_change(_topo(ep0='present'))
        n = len([c for c in fake.calls if c[0] == 'unregister_session'])
        await plugin.on_topology_change(_topo(ep0='present'))
        assert len([c for c in fake.calls
                    if c[0] == 'unregister_session']) == n


# ---------------------------------------------------------------------------
# The member declaration, through the parser that will actually receive it
# ---------------------------------------------------------------------------

class TestMemberDeclarationParses:
    """Every declaration this plugin sends must survive ``parse_member``.

    The fake dispatcher accepts any dict, so nothing else in this file
    notices a member the *real* parser would 400 — and because
    ``_class_pool_decls`` always re-sends the FULL member list, one bad
    member anywhere in the state fails every later join and every restart
    replay, not only its own record's.
    """

    def _decls(self, plugin):
        return [(m.pool_name, plugin._member_decl(rec, m))
                for rec in plugin._state.resources.values()
                for m in rec.member_list()]

    def _parse_all(self, plugin):
        parsed = {}
        for pool, decl in self._decls(plugin):
            member = parse_member(decl, f'test: {pool}', pool_name=pool)
            parsed[member.member_id] = member
        assert parsed
        return parsed

    def test_a_declared_member_join_parses(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        parsed = self._parse_all(plugin)
        assert sorted(parsed) == ['local_b.cpu', 'local_b.gpu']
        assert parsed['local_b.cpu'].attributes['site']     == 'NERSC'
        assert parsed['local_b.cpu'].attributes['software'] == ['lammps',
                                                                'pytorch']
        assert parsed['local_b.gpu'].pilot_sizes['default'].gpus_per_node == 8

    def test_an_allocation_join_parses(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        parsed = self._parse_all(plugin)
        assert parsed['alpha.default'].queue == 'allocation'
        assert parsed['alpha.default'].attributes == {
            'site': 'HERE', 'kind': 'workstation', 'mem_gb_per_node': 16,
            'software': ['lammps']}

    def test_a_login_join_without_members_parses(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _login_body())
        assert 'beta.default' in self._parse_all(plugin)

    def test_a_join_with_no_site_kind_or_mem_gb_parses(self, tmp_path):
        # the empty/None attribute values parse_member refuses
        client, plugin, caller = _joinable(tmp_path)
        _join(client, plugin, _alloc_body(name='bare', site='', kind='',
                                          capabilities={'cores': 4}))
        parsed = self._parse_all(plugin)
        assert parsed['bare.default'].attributes == {'software': []}

    def test_a_pre08_record_read_off_disk_parses(self, tmp_path):
        # BLOCKING regression: a record written before class pools derives
        # its single member at load time, and used to derive
        # {'site': '', 'kind': '', 'mem_gb_per_node': None} — which the real
        # parser refuses, 400ing every subsequent join
        pre08 = json.loads(json.dumps(TestUpgradeFromPre08._PRE08))
        rec   = pre08['resources']['legacy']
        rec['site'] = ''
        rec['kind'] = ''
        rec['capabilities'] = {'cores': 128, 'software': []}
        state = tmp_path / 'fedroot' / 'federation' / 'state.json'
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(json.dumps(pre08))

        _, plugin = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        parsed = self._parse_all(plugin)
        assert parsed['legacy.default'].attributes == {'software': []}
        assert parsed['legacy.default'].queue == 'regular'

    def test_the_length_rule_this_plugin_pre_checks_is_the_parsers(self,
                                                                   tmp_path):
        # the join-time 400 must line up with what parse_member enforces
        from radical.orbit.task_dispatcher_config import \
            MAX_POOL_MEMBER_NAME_LEN
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        pool, decl = self._decls(plugin)[0]
        decl = dict(decl)
        decl['member_id'] = 'a' * (MAX_POOL_MEMBER_NAME_LEN - len(pool) + 1)
        with pytest.raises(Exception) as ei:
            parse_member(decl, 'test', pool_name=pool)
        assert str(MAX_POOL_MEMBER_NAME_LEN) in str(ei.value)


# ---------------------------------------------------------------------------
# _DispatcherAPI decoding and the member verbs
# ---------------------------------------------------------------------------

class TestDispatcherAPI:

    def test_error_status_becomes_an_http_exception(self):
        class _Resp:
            status_code = 409
            body = b'{"detail": "boom"}'
        with pytest.raises(HTTPException) as ei:
            _DispatcherAPI._decode(_Resp())
        assert ei.value.status_code == 409
        assert ei.value.detail == 'boom'

    def test_error_without_a_body_still_maps(self):
        class _Resp:
            status_code = 500
            body = b''
        with pytest.raises(HTTPException) as ei:
            _DispatcherAPI._decode(_Resp())
        assert ei.value.status_code == 500

    def test_success_returns_the_parsed_body(self):
        class _Resp:
            status_code = 200
            body = b'{"sid": "fed"}'
        assert _DispatcherAPI._decode(_Resp()) == {'sid': 'fed'}

    def test_empty_success_body_is_none(self):
        class _Resp:
            status_code = 200
            body = b''
        assert _DispatcherAPI._decode(_Resp()) is None


class _RecordingHost:
    """A plugin host that records (method, path, body) and answers 200."""

    def __init__(self, answers=None):
        self.plugins = {'task_dispatcher': object()}
        self.calls   = []
        self.answers = answers or {}

    async def handle_request(self, method, path, headers, payload):
        self.calls.append((method, path,
                           json.loads(payload) if payload else None))
        status, body = self.answers.get((method, path), (200, {'ok': True}))

        class _Resp:
            pass
        _Resp.status_code = status
        _Resp.body        = json.dumps(body).encode()
        return _Resp


class TestDispatcherMemberVerbs:

    def _api(self, answers=None):
        app = FastAPI()
        host = _RecordingHost(answers)
        app.state.endpoint_service = host
        return _DispatcherAPI(app), host

    def test_add_member_posts_to_the_members_route(self):
        api, host = self._api()
        _run(api.add_member('fed', 'fed-gpu', {'member_id': 'a.gpu'}))
        assert host.calls[0][0] == 'POST'
        assert host.calls[0][1] == '/task_dispatcher/pool/fed/fed-gpu/members'
        assert host.calls[0][2] == {'member_id': 'a.gpu'}

    def test_del_member_sends_its_flags_in_the_body(self):
        # the plugin host takes no query string, so ?force=true would land
        # in the path and match no route
        api, host = self._api()
        _run(api.del_member('fed', 'fed-gpu', 'a.gpu', cancel_tasks=True,
                            force=True, fail_unsatisfiable=False))
        method, path, body = host.calls[0]
        assert method == 'DELETE'
        assert path == '/task_dispatcher/pool/fed/fed-gpu/members/a.gpu'
        assert body == {'cancel_tasks': True, 'force': True,
                        'fail_unsatisfiable': False}

    def test_del_member_defaults_match_the_dispatcher(self):
        api, host = self._api()
        _run(api.del_member('fed', 'fed-gpu', 'a.gpu'))
        assert host.calls[0][2] == {'cancel_tasks': False, 'force': False,
                                    'fail_unsatisfiable': True}

    def test_del_member_error_is_mapped_through(self):
        route = '/task_dispatcher/pool/fed/fed-gpu/members/a.gpu'
        api, host = self._api({('DELETE', route): (409, {'detail': 'last'})})
        with pytest.raises(HTTPException) as ei:
            _run(api.del_member('fed', 'fed-gpu', 'a.gpu'))
        assert ei.value.status_code == 409
        assert [c[0] for c in host.calls] == ['DELETE']


# ---------------------------------------------------------------------------
# Co-hosted with the real task dispatcher
#
# The real plan-121 multi-member dispatcher (class pools, the member routes,
# the per-member summary block) runs beside the federation here -- no fake
# on either side of the seam.
# ---------------------------------------------------------------------------

@pytest.fixture
def cohosted(tmp_path, monkeypatch):
    """A BrokerPluginHost running the real dispatcher + the federation."""
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
        tmp_path / 'td_state')
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
        tmp_path / 'td_scratch')
    monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE', str(tmp_path / 'fed'))

    async def _broadcast(*_a, **_kw):
        return None

    host = BrokerPluginHost(['task_dispatcher', 'federation'], _broadcast)
    host.plugins['federation']._participants = _parts(ep0='present',
                                                      ep1='present')
    return host


async def _call(host, method, path, body=None):
    payload = b'' if body is None else json.dumps(body).encode()
    return await host.handle_request(method, path, {}, payload)


def _fake_active_pilot(ps, pid, member_id, capacity=4):
    """Put one ACTIVE pilot for *member_id* into *ps* and return it.

    Stands in for a real pilot coming up: the size/attribute snapshots are
    the member's own (that is what ``_submit_pilot`` records), and the child
    endpoint carries the dispatcher's ``<pool>_<member_id>_<pid>`` name, so
    ``pick_dispatch`` matches on exactly the fields it would in production.
    """
    m   = ps.member(member_id)
    sz  = m.pilot_sizes[m.default_size]
    now = time.time()
    rec = PilotRecord(
        pid=pid, pool=ps.config.name, owning_sid=FED_SESSION_SID,
        size_key=m.default_size, rhapsody_backend=sz.rhapsody_backend,
        state=PILOT_ACTIVE, submitted_at=now - 10, active_at=now - 5,
        capacity=capacity, walltime_deadline=now + 3600,
        member_id=member_id, attributes=dict(m.attributes),
        endpoint_name=m.endpoint_name, nodes=sz.nodes,
        cpus_per_node=sz.cpus_per_node, gpus_per_node=sz.gpus_per_node,
        child_endpoint_name='%s_%s_%s' % (ps.config.name, member_id, pid))
    ps.pilots[pid] = rec
    return rec


class TestCoHostedLoads:

    def test_both_plugins_load_on_a_broker_host(self, cohosted):
        assert set(cohosted.plugins) == {'task_dispatcher', 'federation'}


class TestCoHosted:

    @pytest.mark.asyncio
    async def test_join_creates_a_real_class_pool(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        r  = await _call(cohosted, 'POST', '/federation/join/default',
                         _alloc_body(name='local'))
        assert r.status_code == 200, r.body
        rec = json.loads(r.body)
        assert rec['members'][0]['pool_name'] == 'fed-cpu'

        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert ps.config.multi_member is True
        assert ps.config.pool_class   == 'cpu'
        assert 'local.default' in ps.config.members
        assert td._records[FED_SESSION_SID].lifetime == 'persistent'

    @pytest.mark.asyncio
    async def test_two_resources_share_one_class_pool(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local_a'))
        await _call(cohosted, 'POST', '/federation/join/default',
                    _members_body(name='local_b'))
        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert sorted(ps.config.members) == ['local_a.default', 'local_b.cpu']
        assert 'fed-gpu' in td._pool_states[FED_SESSION_SID]

    @pytest.mark.asyncio
    async def test_the_expiry_sweep_does_not_kill_the_fed_session(
            self, cohosted, monkeypatch):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        real_time = time.time
        monkeypatch.setattr(time, 'time', lambda: real_time() + 7200)
        assert await td._cleanup_expired_sessions() == 0
        assert FED_SESSION_SID in td._sessions
        assert 'fed-cpu' in td._pool_states[FED_SESSION_SID]

    @pytest.mark.asyncio
    async def test_a_task_lands_on_the_member_with_the_software(self,
                                                                cohosted):
        """Both halves of the decision: the class, then the member.

        ``local_a`` and ``local_b.cpu`` share the ``fed-cpu`` class pool but
        only ``local_b.cpu`` declares ``lammps``.  The federation picks the
        *class* at submit; the **member** is bound at dispatch, so this
        brings a live pilot up on each member and drives one drain.
        """
        td = cohosted.plugins['task_dispatcher']
        # local_a joins the same class with a different software list
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local_a',
                                capabilities={'cores': 8, 'gpus': 0,
                                              'mem_gb': 16,
                                              'software': ['pytorch']}))
        await _call(cohosted, 'POST', '/federation/join/default',
                    _members_body(name='local_b'))

        r = await _call(cohosted, 'POST', '/federation/submit/default',
                        {'task': {'task_id': 't.1',
                                  'cmd': ['/bin/echo', 'x']},
                         'requirements': {'cores': 1,
                                          'software': ['lammps']}})
        assert r.status_code == 200, r.body
        assert json.loads(r.body)['pool'] == 'fed-cpu'

        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert sorted(ps.config.members) == ['local_a.default',
                                             'local_b.cpu']

        # one live pilot per member, named exactly as the dispatcher names
        # a child endpoint (``<pool>_<member_id>_<pid>``)
        for pid, mid in (('p.a', 'local_a.default'), ('p.b', 'local_b.cpu')):
            _fake_active_pilot(ps, pid, mid)

        # the rhapsody hop is not what is under test here: the placement is
        # made by ``_claim``, before the batch is handed to the pilot
        td._do_rhapsody_submit = AsyncMock()
        td._drain_pending(ps)

        assert ps.tasks['t.1'].member_id == 'local_b.cpu'
        assert ps.tasks['t.1'].pilot_id  == 'p.b'

    @pytest.mark.asyncio
    async def test_the_dispatcher_assigns_the_cwd(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        await _call(cohosted, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})
        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert ps.tasks['t.1'].cwd == ''
        assert ps.tasks['t.1'].cwd_assigned is True

    @pytest.mark.asyncio
    async def test_inputs_b64_is_spooled_by_the_dispatcher(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        await _call(cohosted, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true'],
                              'inputs': ['md.json'],
                              'inputs_b64': {'md.json': 'e30='}}})
        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert ps.tasks['t.1'].spooled == ['md.json']

    @pytest.mark.asyncio
    async def test_usage_reads_the_real_per_member_summary(self, cohosted):
        from radical.orbit.task_dispatcher_state import (
            PilotRecord, PILOT_ACTIVE)
        td  = cohosted.plugins['task_dispatcher']
        fed = cohosted.plugins['federation']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local', budget={'node_hours': 10.0}))
        ps  = td._pool_states[FED_SESSION_SID]['fed-cpu']
        now = time.time()
        ps.pilots['p.1'] = PilotRecord(
            pid='p.1', pool='fed-cpu', owning_sid=FED_SESSION_SID,
            size_key='default', rhapsody_backend='concurrent',
            state=PILOT_ACTIVE, submitted_at=now - 3700,
            active_at=now - 3600, member_id='local.default', nodes=1,
            cpus_per_node=8,
            child_endpoint_name='fed-cpu_local.default_p.1')
        fed._state.resources['local'].usage.updated_at = 0.0
        fed._detail_cache.clear()

        r = await _call(cohosted, 'GET',
                        '/federation/resource/default/local')
        rec = json.loads(r.body)
        assert rec['members'][0]['usage']['node_hours_used'] == \
            pytest.approx(1.0, abs=0.05)
        assert rec['usage']['pilots_active'] == 1

    @pytest.mark.asyncio
    async def test_leave_drains_a_member_and_the_task_re_queues(self,
                                                                cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local_a'))
        await _call(cohosted, 'POST', '/federation/join/default',
                    _members_body(name='local_b'))
        await _call(cohosted, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true']},
                     'requirements': {'software': ['lammps']}})
        r = await _call(cohosted, 'POST',
                        '/federation/leave/default/local_a')
        assert r.status_code == 200
        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        assert 'local_a.default' not in ps.config.members
        assert 'fed-cpu' in td._pool_states[FED_SESSION_SID]   # pool stays
        assert ps.tasks['t.1'].state == 'QUEUED'
        # and the ledger entry survives its resource
        body = json.loads((await _call(cohosted, 'GET',
                                       '/federation/task/default/t.1')).body)
        assert body['resource'] is None

    @pytest.mark.asyncio
    async def test_a_duplicate_join_does_not_disturb_the_pool(self, cohosted):
        td = cohosted.plugins['task_dispatcher']
        await _call(cohosted, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        ps = td._pool_states[FED_SESSION_SID]['fed-cpu']
        with pytest.raises(HTTPException) as ei:
            await _call(cohosted, 'POST', '/federation/join/default',
                        _alloc_body(name='local'))
        assert ei.value.status_code == 409
        assert td._pool_states[FED_SESSION_SID]['fed-cpu'] is ps


class TestCoHostedRestart:

    @pytest.mark.asyncio
    async def test_restart_re_attaches_to_the_replayed_class_pool(
            self, tmp_path, monkeypatch):
        """A second host over the same state dirs must find the same pool."""
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
            tmp_path / 'td_state')
        monkeypatch.setattr(
            'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
            tmp_path / 'td_scratch')
        monkeypatch.setenv('RADICAL_ORBIT_FEDERATION_STATE',
                           str(tmp_path / 'fed'))

        async def _broadcast(*_a, **_kw):
            return None

        host1 = BrokerPluginHost(['task_dispatcher', 'federation'],
                                 _broadcast)
        host1.plugins['federation']._participants = _parts(ep0='present')
        await _call(host1, 'POST', '/federation/join/default',
                    _alloc_body(name='local'))
        await _call(host1, 'POST', '/federation/submit/default',
                    {'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})

        host2 = BrokerPluginHost(['task_dispatcher', 'federation'],
                                 _broadcast)
        td2   = host2.plugins['task_dispatcher']
        fed2  = host2.plugins['federation']
        assert set(fed2._state.resources) == {'local'}
        ps_replayed = td2._pool_states[FED_SESSION_SID]['fed-cpu']
        assert 't.1' in ps_replayed.tasks

        await fed2.on_topology_change(_topo(ep0='present'))
        assert fed2._state.resources['local'].liveness == LIVENESS_OK
        assert td2._pool_states[FED_SESSION_SID]['fed-cpu'] is ps_replayed
        assert td2._records[FED_SESSION_SID].lifetime == 'persistent'

        r = await _call(host2, 'GET', '/federation/task/default/t.1')
        assert r.status_code == 200
        assert json.loads(r.body)['member_id'] is None


# ---------------------------------------------------------------------------
# FederationClient — route/verb construction
# ---------------------------------------------------------------------------

class _Resp:
    is_error    = False
    status_code = 200
    text        = ''

    def __init__(self, payload=None):
        self._payload = payload if payload is not None else {'ok': True}

    def json(self):
        return self._payload


class _RecordingHTTP:
    """Minimal transport: records (verb, url, json) and answers 200."""

    def __init__(self):
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(('GET', url, kw.get('json')))
        return _Resp()

    def post(self, url, **kw):
        self.calls.append(('POST', url, kw.get('json')))
        return _Resp()


class TestFederationClient:

    def _client(self):
        from radical.orbit.plugin_federation import FederationClient
        http = _RecordingHTTP()
        return FederationClient(http, '/broker/federation',
                                endpoint_id='broker',
                                plugin_name='federation'), http

    def test_every_verb_addresses_the_default_session(self):
        c, http = self._client()
        c.join({'name': 'a'})
        c.leave('a')
        c.resources()
        c.resource('a')
        c.pick({})
        c.submit({'task_id': 't'})
        c.task('t')
        for _verb, url, _body in http.calls:
            assert '/default' in url

    def test_leave_carries_the_cancel_flag(self):
        c, http = self._client()
        c.leave('a')
        assert http.calls[0][2] == {'cancel_tasks': False}
        c.leave('a', cancel_tasks=True)
        assert http.calls[1][2] == {'cancel_tasks': True}

    def test_pick_and_submit_wrap_their_bodies(self):
        c, http = self._client()
        c.pick({'cores': 4})
        c.submit({'task_id': 't'}, {'gpus': 1})
        assert http.calls[0][2] == {'requirements': {'cores': 4}}
        assert http.calls[1][2] == {'task': {'task_id': 't'},
                                    'requirements': {'gpus': 1}}


# ---------------------------------------------------------------------------
# Allocation-mode sizing
# ---------------------------------------------------------------------------

class TestAllocationSizing:

    def _join_with_alloc(self, tmp_path, alloc, **body_kw):
        routes = {('GET', '/queue_info/job_allocation'):
                  (200, {'allocation': alloc})} if alloc is not None else {}
        caller = _FakeCaller(routes)
        client, plugin, _fake = _joinable(tmp_path, caller=caller)
        _join(client, plugin, _alloc_body(**body_kw))
        return plugin._state.resources['alpha'].members['default']

    def test_nodes_and_walltime_come_from_the_allocation(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 4, 'runtime': 7200})
        assert (m.nodes, m.walltime_sec) == (4, 7200)

    def test_per_node_counts_prefer_the_allocation(self, tmp_path):
        m = self._join_with_alloc(tmp_path,
                                  {'n_nodes': 2, 'cpus_per_node': 64,
                                   'gpus_per_node': 4})
        assert (m.cpus_per_node, m.gpus_per_node) == (64, 4)
        assert m.cls == 'gpu'          # GPUs make it a gpu-class member

    def test_declared_total_is_divided_by_the_node_count(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 4},
                                  capabilities={'cores': 32, 'gpus': 8})
        assert (m.cpus_per_node, m.gpus_per_node) == (8, 2)

    def test_cpus_per_node_is_never_zero(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 16},
                                  capabilities={'cores': 8})
        assert m.cpus_per_node == 1

    def test_no_allocation_falls_back_to_one_node_one_hour(self, tmp_path):
        m = self._join_with_alloc(tmp_path, None)
        assert (m.nodes, m.walltime_sec) == (1, 3600)

    def test_the_allocations_end_time_beats_its_time_limit(self, tmp_path):
        """`runtime` is the limit; joining an hour in, what is left is what
        the pilot may have.  The endpoint computed it inside the
        allocation -- the broker's own environment describes another job."""
        end = time.time() + 900
        m   = self._join_with_alloc(tmp_path, {'n_nodes': 1, 'runtime': 7200,
                                               'end_time': end})
        assert 800 < m.walltime_sec <= 900
        # ... and the absolute instant is kept, because the member is
        # re-declared with this walltime on every re-attach
        assert m.end_time == end
        assert m.pilot    == 'endpoint'

    def test_an_expired_allocation_is_a_400(self, tmp_path):
        """No one-second pilots: an allocation that is over is not a
        resource."""
        routes = {('GET', '/queue_info/job_allocation'):
                  (200, {'allocation': {'n_nodes': 1, 'runtime': 7200,
                                        'end_time': time.time() - 10}})}
        client, plugin, _ = _joinable(tmp_path, caller=_FakeCaller(routes))
        r = _join(client, plugin, _alloc_body())
        assert r.status_code == 400
        assert 'no time left' in r.json()['detail']
        assert plugin._state.resources == {}

    def test_without_an_end_time_nothing_changes(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 1, 'runtime': 600})
        assert (m.walltime_sec, m.end_time) == (600, None)

    def test_the_endpoint_is_the_pilot_of_an_allocation(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 1, 'runtime': 600})
        assert (m.pilot, m.endpoint) == ('endpoint', 'ep0')

    def test_allocation_budget_follows_the_derived_size(self, tmp_path):
        m = self._join_with_alloc(tmp_path, {'n_nodes': 4, 'runtime': 1800})
        assert m.budget == {'node_hours': 2.0}


# ---------------------------------------------------------------------------
# The member payload: add, never rename (Orbit plan 122 §payload)
# ---------------------------------------------------------------------------

class TestPilotPayload:

    def _read(self, client, plugin, name='local_b'):
        return client.get(f'{plugin.namespace}/resource/default/{name}').json()

    def test_the_names_every_consumer_reads_are_untouched(self, tmp_path):
        """`federation.js`, ATOMIC's campaign UI and `smoke.py` read
        `member` and `pool_name`; the new fields ride beside them."""
        client, plugin, _ = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        m = self._read(client, plugin)['members'][0]
        for key in ('member', 'member_id', 'pool_name', 'class', 'queue',
                    'walltime_sec', 'attributes', 'usage', 'liveness',
                    'state'):
            assert key in m, key
        assert m['member']    == 'cpu'
        assert m['pool_name'] == 'fed-cpu'
        assert m['attributes']['mem_gb_per_node'] == 256
        # ... and the additions
        assert m['endpoint'] == 'ep1'
        assert m['pilot']    == 'submit'
        assert m['end_time'] is None
        assert m['remaining_sec'] is None

    def test_an_allocation_row_is_its_endpoint_with_a_countdown(self,
                                                               tmp_path):
        end    = time.time() + 900
        routes = {('GET', '/queue_info/job_allocation'):
                  (200, {'allocation': {'n_nodes': 1, 'runtime': 7200,
                                        'end_time': end}})}
        client, plugin, _ = _joinable(tmp_path, caller=_FakeCaller(routes))
        _join(client, plugin, _alloc_body())
        m = self._read(client, plugin, 'alpha')['members'][0]
        assert m['pilot']    == 'endpoint'
        assert m['endpoint'] == 'ep0'
        assert m['end_time'] == end
        assert m['remaining_sec'] == pytest.approx(900, abs=5)
        # the endpoint is the pilot, so there is exactly one of it
        assert (m['min_pilots'], m['max_pilots']) == (1, 1)

    def test_the_countdown_ticks_between_two_reads(self, tmp_path):
        routes = {('GET', '/queue_info/job_allocation'):
                  (200, {'allocation': {'n_nodes': 1,
                                        'end_time': time.time() + 900}})}
        client, plugin, _ = _joinable(tmp_path, caller=_FakeCaller(routes))
        _join(client, plugin, _alloc_body())
        first = self._read(client, plugin, 'alpha')['members'][0]
        time.sleep(0.01)
        again = self._read(client, plugin, 'alpha')['members'][0]
        assert again['remaining_sec'] < first['remaining_sec']

    def test_a_submit_shape_reports_the_dispatchers_number(self, tmp_path):
        """No allocation of its own: the runway is the most walltime any of
        its live pilots has, which only the dispatcher knows."""
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 1,
             'remaining_sec': 1234.0}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        m = self._read(client, plugin)['members'][0]
        assert m['remaining_sec'] == 1234.0
        assert m['state'] == 'ok'

    def test_a_shape_without_a_pilot_is_idle(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _members_body())
        fake.details['fed-cpu'] = _pool_summary([
            {'member_id': 'local_b.cpu', 'pilots_active': 0}])
        plugin._state.resources['local_b'].usage.updated_at = 0.0
        plugin._detail_cache.clear()

        m = self._read(client, plugin)['members'][0]
        assert m['state'] == 'idle'
        assert m['usage']['pilot_error'] is None
        assert m['remaining_sec'] is None

    def test_the_declaration_reaches_the_dispatcher(self, tmp_path):
        end    = time.time() + 900
        routes = {('GET', '/queue_info/job_allocation'):
                  (200, {'allocation': {'n_nodes': 1, 'end_time': end}})}
        client, plugin, fake = _joinable(tmp_path,
                                         caller=_FakeCaller(routes))
        _join(client, plugin, _alloc_body())
        decl = _member_decl(fake, 'fed-cpu', 'alpha.default')
        assert decl['pilot']    == 'endpoint'
        assert decl['end_time'] == end
        # and it survives the parser the real dispatcher runs it through
        member = parse_member(decl, 'test', pool_name='fed-cpu')
        assert member.pilot    == 'endpoint'
        assert member.end_time == end
        assert (member.min_pilots, member.max_pilots) == (1, 1)


class TestDeclaredPilotMode:
    """A login-mode member may say its endpoint is the pilot too."""

    def _body(self, **member_kw):
        member = {'member': 'cpu', 'queue': 'RM', 'account': 'abc123',
                  'nodes': 1, 'cpus_per_node': 128, 'walltime_sec': 3600,
                  'max_pilots': 2, 'budget': {'node_hours': 20}}
        member.update(member_kw)
        return _members_body(members=[member])

    def test_pilot_endpoint_is_accepted_and_forwarded(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, self._body(pilot='endpoint'))
        assert r.status_code == 200, r.text
        member = plugin._state.resources['local_b'].members['cpu']
        assert (member.pilot, member.endpoint) == ('endpoint', 'ep1')
        assert member.end_time is None       # only an allocation knows it
        # declared 0/2, but the federation reports what the dispatcher holds
        assert (member.min_pilots, member.max_pilots) == (1, 1)
        decl = _member_decl(fake, 'fed-cpu', 'local_b.cpu')
        assert decl['pilot'] == 'endpoint'
        assert (decl['min_pilots'], decl['max_pilots']) == (1, 1)
        # the dispatcher's parser forces the floor that drives adoption
        assert parse_member(decl, 'test', pool_name='fed-cpu').min_pilots == 1

    def test_an_unknown_pilot_mode_is_a_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, self._body(pilot='adopt'))
        assert r.status_code == 400
        assert 'pilot' in r.json()['detail']


# ---------------------------------------------------------------------------
# Capability discovery
# ---------------------------------------------------------------------------

class TestCapabilityDiscovery:

    _METRICS = {'cpu': {'cores_logical': 64},
                'memory': {'total': 68719476736},
                'gpus': [{'name': 'a'}, {'name': 'b'}]}

    def _caller(self, metrics=None):
        return _FakeCaller({
            ('POST', '/sysinfo/register_session'): (200, {'sid': 's.1'}),
            ('GET',  '/sysinfo/metrics/s.1')     : (200, metrics
                                                    or self._METRICS),
            ('POST', '/sysinfo/unregister_session/s.1'): (200, {'ok': True}),
        })

    def test_missing_capabilities_are_filled_from_sysinfo(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path, caller=self._caller())
        rec = _join(client, plugin,
                    _alloc_body(capabilities={'software': ['x']})).json()
        # cores/gpus become the member aggregate; mem_gb stays as discovered
        # and reaches the dispatcher as a per-node attribute
        assert rec['capabilities']['mem_gb'] == 64.0
        assert plugin._state.resources['alpha'].members[
            'default'].attributes['mem_gb_per_node'] == 64.0

    def test_declared_capabilities_win_over_discovery(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path, caller=self._caller())
        rec = _join(client, plugin,
                    _alloc_body(capabilities={'cores': 4, 'gpus': 0,
                                              'mem_gb': 8})).json()
        assert rec['capabilities']['mem_gb'] == 8
        assert rec['members'][0]['cpus_per_node'] == 4

    def test_no_sysinfo_session_leaves_capabilities_alone(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path, caller=_FakeCaller())
        rec = _join(client, plugin,
                    _alloc_body(capabilities={'software': ['x']})).json()
        assert 'mem_gb' not in rec['capabilities']

    def test_a_full_declaration_never_calls_sysinfo(self, tmp_path):
        caller = self._caller()
        client, plugin, _ = _joinable(tmp_path, caller=caller)
        _join(client, plugin, _alloc_body())
        assert not [c for c in caller.calls if 'sysinfo' in c[2]]

    def test_a_raising_caller_does_not_fail_the_join(self, tmp_path):
        caller = _FakeCaller(raises=RuntimeError('broker down'))
        client, plugin, _ = _joinable(tmp_path, caller=caller)
        r = _join(client, plugin, _alloc_body(capabilities={}))
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Login-pool validation (the flat, member-less form)
# ---------------------------------------------------------------------------

class TestLoginPoolValidation:

    def _bad_pool(self, tmp_path, **pool_overrides):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool'].update(pool_overrides)
        return _join(client, plugin, body)

    def test_non_integer_max_pilots_400(self, tmp_path):
        r = self._bad_pool(tmp_path, max_pilots='two')
        assert r.status_code == 400
        assert 'max_pilots' in r.text

    def test_boolean_is_not_an_integer(self, tmp_path):
        assert self._bad_pool(tmp_path, nodes=True).status_code == 400

    def test_out_of_range_values_400(self, tmp_path):
        for override in ({'nodes': 0}, {'cpus_per_node': 0},
                         {'walltime_sec': 0}, {'max_pilots': 0},
                         {'gpus_per_node': 999}):
            assert self._bad_pool(tmp_path, **override).status_code == 400

    def test_min_pilots_above_max_pilots_400(self, tmp_path):
        r = self._bad_pool(tmp_path, min_pilots=5, max_pilots=2)
        assert r.status_code == 400

    def test_min_pilots_is_accepted_and_forwarded(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        body = _login_body()
        body['pool']['min_pilots'] = 1
        _join(client, plugin, body)
        assert _member_decl(fake, 'fed-gpu',
                            'beta.default')['min_pilots'] == 1

    def test_unknown_pool_field_400(self, tmp_path):
        r = self._bad_pool(tmp_path, max_piltos=2)
        assert r.status_code == 400
        assert 'max_piltos' in r.text

    def test_missing_required_field_400(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _login_body()
        body['pool'].pop('nodes')
        assert _join(client, plugin, body).status_code == 400

    def test_non_string_backend_400(self, tmp_path):
        assert self._bad_pool(tmp_path,
                              rhapsody_backend=7).status_code == 400

    def test_non_string_account_400(self, tmp_path):
        assert self._bad_pool(tmp_path, account=7).status_code == 400


# ---------------------------------------------------------------------------
# The broker is a participant, but not a resource
# ---------------------------------------------------------------------------

class TestBrokerIsNotAResource:

    def test_joining_the_broker_400(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        plugin._participants['broker'] = {'liveness': 'present',
                                          'role': 'broker'}
        r = _join(client, plugin, _alloc_body(endpoint='broker'))
        assert r.status_code == 400
        assert 'broker' in r.text
        assert fake.calls == []

    @pytest.mark.asyncio
    async def test_role_is_kept_from_the_topology(self, tmp_path):
        _, plugin = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        await plugin.on_topology_change({
            'ep0'   : {'role': 'endpoint', 'liveness': 'present'},
            'broker': {'role': 'broker',   'liveness': 'present'}})
        assert plugin._participant('ep0')['role']    == 'endpoint'
        assert plugin._participant('broker')['role'] == 'broker'
        assert plugin._liveness_for('ep0')           == LIVENESS_OK


# ---------------------------------------------------------------------------
# The restart window: routes served before the first topology delivery
# ---------------------------------------------------------------------------

class TestRestartWindow:

    def _restarted(self, tmp_path):
        """A joined+submitted federation, restarted, no topology yet."""
        client, plugin, _fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        client.post(f'{plugin.namespace}/submit/default',
                    json={'task': {'task_id': 't.1', 'cmd': ['/bin/true']}})

        fake2 = _FakeDispatcher()
        fake2.tasks['t.1'] = {'task_id': 't.1', 'state': 'RUNNING',
                              'pilot_id': None, 'member_id': None}
        app = FastAPI()
        app.state.is_broker        = True
        app.state.broker_caller    = None
        app.state.endpoint_service = None
        plugin2 = PluginFederation(app, state_root=tmp_path / 'fedroot')
        plugin2._dispatcher = fake2
        return TestClient(plugin2._app), plugin2, fake2

    def test_records_load_as_lost_until_topology_says_otherwise(self,
                                                                tmp_path):
        _client, plugin2, _fake2 = self._restarted(tmp_path)
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_LOST

    def test_a_route_forces_the_re_attach(self, tmp_path):
        client, plugin2, fake2 = self._restarted(tmp_path)
        assert fake2.sessions == {}          # nothing yet
        r = client.get(f'{plugin2.namespace}/resources/default')
        assert r.status_code == 200
        assert set(fake2.sessions) == {FED_SESSION_SID}
        assert fake2.members_of('fed-cpu') == ['alpha.default']
        assert plugin2._replayed is True

    def test_task_polling_survives_the_window(self, tmp_path):
        # The case that motivates this: a client polling a task across a
        # broker restart, before any endpoint has (re)connected.
        client, plugin2, _fake2 = self._restarted(tmp_path)
        r = client.get(f'{plugin2.namespace}/task/default/t.1')
        assert r.status_code == 200
        assert r.json()['state']    == 'RUNNING'
        assert r.json()['resource'] == 'alpha'

    def test_submit_refuses_cleanly_in_the_window(self, tmp_path):
        # No endpoint has been seen yet, so no member may take new work —
        # a 409 with the reason, not a 404 from a dead dispatcher session.
        client, plugin2, fake2 = self._restarted(tmp_path)
        r = client.post(f'{plugin2.namespace}/submit/default',
                        json={'task': {'task_id': 't.2',
                                       'cmd': ['/bin/true']}})
        assert r.status_code == 409
        assert 'liveness' in r.json()['reasons']['alpha.default']
        assert fake2.submitted == []

    @pytest.mark.asyncio
    async def test_topology_after_a_route_does_not_replay_twice(self,
                                                                tmp_path):
        client, plugin2, fake2 = self._restarted(tmp_path)
        client.get(f'{plugin2.namespace}/resources/default')
        n = len([c for c in fake2.calls if c[0] == 'register_session'])
        await plugin2.on_topology_change(_topo(ep0='present'))
        assert len([c for c in fake2.calls
                    if c[0] == 'register_session']) == n
        assert plugin2._state.resources['alpha'].liveness == LIVENESS_OK


# ---------------------------------------------------------------------------
# Concurrent usage refresh
# ---------------------------------------------------------------------------

class TestRefreshAll:

    @pytest.mark.asyncio
    async def test_refreshes_every_class_pool_concurrently(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())        # fed-cpu
        _join(client, plugin, _login_body())        # fed-gpu

        started = []

        async def _slow(sid, name):
            started.append(name)
            await asyncio.sleep(0.05)
            return _pool_summary([])

        fake.pool_detail = _slow
        for rec in plugin._state.resources.values():
            rec.usage.updated_at = 0.0
        plugin._detail_cache.clear()

        t0 = time.monotonic()
        await plugin._refresh_all()
        elapsed = time.monotonic() - t0
        assert sorted(started) == ['fed-cpu', 'fed-gpu']
        assert elapsed < 0.09            # concurrent, not 2 x 0.05 s

    @pytest.mark.asyncio
    async def test_one_failing_refresh_does_not_break_the_others(self,
                                                                 tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        _join(client, plugin, _alloc_body())
        _join(client, plugin, _login_body())

        async def _half(sid, name):
            if name == 'fed-cpu':
                raise RuntimeError('boom')
            return _pool_summary([_member_block(mid)
                                  for mid in fake.members_of(name)])

        fake.pool_detail = _half
        for rec in plugin._state.resources.values():
            rec.usage.updated_at = 0.0
        plugin._detail_cache.clear()

        await plugin._refresh_all()
        assert plugin._state.resources['alpha'].usage.stale is True
        assert plugin._state.resources['beta'].usage.stale is False

    @pytest.mark.asyncio
    async def test_empty_federation_is_a_no_op(self, tmp_path):
        _, plugin = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        await plugin._refresh_all()          # must not raise


# ---------------------------------------------------------------------------
# Path containment on a member's scratch
# ---------------------------------------------------------------------------

class TestMemberScratchContainment:

    def test_a_member_scratch_outside_home_or_tmp_400(self, tmp_path):
        body = _members_body()
        body['members'][0]['scratch_base'] = '/etc/orbit-work'
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, body)
        assert r.status_code == 400
        assert 'member cpu.scratch_base' in r.text
        assert fake.calls == []

    def test_a_symlink_escape_is_caught_on_the_realpath(self, tmp_path):
        _SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        link = _SCRATCH_ROOT / 'escape'
        os.symlink('/etc', link)
        body = _members_body()
        body['members'][0]['scratch_base'] = str(link)
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin, body).status_code == 400

    def test_a_member_scratch_under_tmp_is_accepted(self, tmp_path):
        body = _members_body()
        own = str(_SCRATCH_ROOT / 'ok')
        body['members'][0]['scratch_base'] = own
        client, plugin, _ = _joinable(tmp_path)
        rec = _join(client, plugin, body).json()
        assert rec['members'][0]['scratch_base'] == own


# ---------------------------------------------------------------------------
# A resource that does NOT share the broker's filesystem
# ---------------------------------------------------------------------------

# a scratch tree on the resource's own host: absolute, and deliberately
# nowhere near the broker's ``~`` or ``/tmp``
_REMOTE_SCRATCH = '/pscratch/sd/m/x/atomic-demo'


class TestUnsharedScratch:
    """``shared_fs: false`` — ``scratch_base`` names a path on the resource.

    The containment rule exists because the *broker* writes a shared tree.
    A resource on another machine names a directory only its own pilots can
    reach, so the broker must neither judge that path against its own roots
    nor create it locally.
    """

    def test_an_unshared_allocation_join_keeps_a_remote_scratch(self,
                                                                tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(shared_fs=False,
                                              scratch_base=_REMOTE_SCRATCH))
        assert r.status_code == 200, r.text
        rec = r.json()
        assert rec['shared_fs']    is False
        assert rec['scratch_base'] == _REMOTE_SCRATCH

        member = rec['members'][0]
        assert member['shared_fs']    is False
        assert member['scratch_base'] == _REMOTE_SCRATCH

        # ... and that is exactly what the dispatcher was told
        decl = _member_decl(fake, 'fed-cpu', 'alpha.default')
        assert decl['shared_fs']    is False
        assert decl['scratch_base'] == _REMOTE_SCRATCH

        # ... and what a client reads back off the listing and the detail
        listed = client.get(f'{plugin.namespace}/resources/default').json()
        assert listed['resources'][0]['shared_fs'] is False
        detail = client.get(f'{plugin.namespace}/resource/default/alpha')
        assert detail.json()['shared_fs'] is False

    def test_an_unshared_join_creates_no_broker_local_scratch(
            self, tmp_path, monkeypatch):
        made = []
        real = Path.mkdir

        def _mkdir(self, *args, **kw):
            made.append(str(self))
            return real(self, *args, **kw)

        monkeypatch.setattr(Path, 'mkdir', _mkdir)
        client, plugin, _ = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(shared_fs=False,
                                              scratch_base=_REMOTE_SCRATCH))
        assert r.status_code == 200, r.text
        assert _REMOTE_SCRATCH not in made

    def test_an_unshared_scratch_must_still_be_absolute(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(shared_fs=False,
                                              scratch_base='demo/scratch'))
        assert r.status_code == 400
        assert 'absolute' in r.text
        assert fake.calls == []

    def test_a_shared_join_still_refuses_a_remote_path(self, tmp_path):
        # unchanged for every client that says nothing about shared_fs
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(scratch_base=_REMOTE_SCRATCH))
        assert r.status_code == 400
        assert 'must lie under' in r.text
        assert fake.calls == []

    def test_a_join_without_shared_fs_is_shared(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        rec = _join(client, plugin, _alloc_body()).json()
        assert rec['shared_fs']               is True
        assert rec['members'][0]['shared_fs'] is True
        assert _member_decl(fake, 'fed-cpu',
                            'alpha.default')['shared_fs'] is True

    def test_shared_fs_must_be_a_bool(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        r = _join(client, plugin, _alloc_body(shared_fs='yes'))
        assert r.status_code == 400
        assert 'shared_fs' in r.text
        assert fake.calls == []

    def test_declared_members_inherit_the_resource_shared_fs(self, tmp_path):
        client, plugin, fake = _joinable(tmp_path)
        body = _members_body(shared_fs=False, scratch_base=_REMOTE_SCRATCH)
        rec  = _join(client, plugin, body).json()
        assert [m['shared_fs'] for m in rec['members']] == [False, False]
        decl = _member_decl(fake, 'fed-cpu', 'local_b.cpu')
        assert decl['shared_fs']    is False
        assert decl['scratch_base'] == _REMOTE_SCRATCH

    def test_a_member_may_name_its_own_remote_scratch(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        body = _members_body(shared_fs=False, scratch_base=_REMOTE_SCRATCH)
        body['members'][1]['scratch_base'] = '~/other-scratch'
        rec = _join(client, plugin, body).json()
        # kept verbatim: '~' is expanded on the resource's host, not here
        assert rec['members'][1]['scratch_base'] == '~/other-scratch'

    def test_the_flag_survives_a_restart(self, tmp_path):
        client, plugin, _ = _joinable(tmp_path)
        assert _join(client, plugin,
                     _alloc_body(shared_fs=False,
                                 scratch_base=_REMOTE_SCRATCH)
                     ).status_code == 200
        _, restarted = _make_plugin(tmp_path, dispatcher=_FakeDispatcher())
        rec = restarted._state.resources['alpha']
        assert rec.shared_fs   is False
        assert rec.scratch_base == _REMOTE_SCRATCH
        assert rec.members['default'].shared_fs is False
