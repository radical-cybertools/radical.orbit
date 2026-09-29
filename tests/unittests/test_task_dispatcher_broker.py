"""Broker-integration tests for the task dispatcher (M7 transport port).

Drives a **real** :class:`~radical.orbit.broker.Broker` (hosting the dispatcher)
under uvicorn on an ephemeral port, plus a **real** ``EndpointRuntime`` child
that serves a fake plugin route.  Exercises:

- the in-process broker caller the dispatcher uses (``call_threadsafe`` → a real
  child endpoint → response), end-to-end;
- the dispatcher being wired with the broker caller + event tap on the host;
- gateway HTTP → broker-hosted dispatcher round-trip (pre-flip item 1).

No test sleeps for more than ~1 s; liveness/backoff knobs are injected tiny.
"""

import json
import subprocess
import threading
import time

import httpx
import pytest

from radical.orbit.plugin_base import Plugin
from radical.orbit.plugin_session_base import PluginSession


# ---------------------------------------------------------------------------
# TLS material
# ---------------------------------------------------------------------------

def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def self_signed(tmp_path):
    if not _have_openssl():
        pytest.skip("openssl not available")
    import os
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    os.chmod(key, 0o600)
    return cert, key


# ---------------------------------------------------------------------------
# A tiny served plugin standing in for a pilot's rhapsody/psij
# ---------------------------------------------------------------------------

class _FakePilot(Plugin):
    plugin_name = 'fake_pilot'
    version     = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'fake_pilot')
        self.add_route_get('ping', self._ping)

    async def _ping(self, request):
        return {'pong': True}


class _FakeRhapsody(Plugin):
    """A rhapsody-shaped served plugin on the fake pilot.

    Mounted at instance name ``rhapsody`` so its namespace is
    ``/rhapsody`` -- exactly what a real ``RhapsodyClient`` formats.  The
    submit body is msgpack (that is the rhapsody wire form), and every
    forwarded task dict is recorded on the class so a test can assert what
    actually crossed to which pilot.
    """
    plugin_name   = 'fake_rhapsody'
    session_class = PluginSession
    version       = '0.0.1'

    # (endpoint_name, task dict), appended in submit order
    received: list = []

    def __init__(self, app, instance_name: str = 'rhapsody'):
        super().__init__(app, instance_name)
        self.add_route_post('submit/{sid}', self._submit)

    async def _submit(self, request):
        import msgpack as _mp
        body = await request.body()
        data = _mp.unpackb(body, raw=False)
        me   = getattr(self._app.state, 'endpoint_name', '?')
        acks = []
        for td in data.get('tasks', []):
            _FakeRhapsody.received.append((me, td))
            acks.append({'uid': td.get('uid'), 'state': 'RUNNING'})
        return acks


class _FakeStaging(Plugin):
    """A staging-shaped served plugin on the fake pilot: ``put`` only.

    The real staging plugin is in the default plugin set, so it is faked
    here rather than hosted, to keep the assertion (what was put, where,
    and in what order relative to the rhapsody submit) local to the test.
    """
    plugin_name   = 'fake_staging'
    session_class = PluginSession
    version       = '0.0.1'

    # (endpoint_name, target path, byte count)
    puts: list = []

    def __init__(self, app, instance_name: str = 'staging'):
        super().__init__(app, instance_name)
        self.add_route_post('put/{sid}', self._put)

    async def _put(self, request):
        import base64 as _b64
        data = await request.json()
        me   = getattr(self._app.state, 'endpoint_name', '?')
        raw  = _b64.b64decode(data.get('content') or '')
        _FakeStaging.puts.append((me, data.get('filename'), len(raw)))
        return {'path': data.get('filename'), 'size': len(raw)}


class _FakePsij(Plugin):
    """A psij-shaped served plugin: base ``register_session`` + a canned
    ``submit_tunneled/{sid}`` that echoes what crossed the wire.

    Registered under a distinct ``plugin_name`` (so the real ``PluginPSIJ``
    stays in the class registry) but mounted at instance name ``psij`` so its
    namespace is ``/psij`` — exactly what a real ``PSIJClient`` formats.
    """
    plugin_name   = 'fake_psij'
    session_class = PluginSession
    version       = '0.0.1'

    def __init__(self, app, instance_name: str = 'psij'):
        super().__init__(app, instance_name)
        self.add_route_post('submit_tunneled/{sid}', self._submit_tunneled)

    submitted: list = []           # every job spec the dispatcher sent

    async def _submit_tunneled(self, request):
        data = await request.json()
        _FakePsij.submitted.append(data)
        return {'job_id':        'j.1',
                'native_id':     'n.1',
                'echo_tunnel':   data.get('tunnel'),
                'echo_executor': data.get('executor')}


# ---------------------------------------------------------------------------
# Broker-under-uvicorn harness + runtime factory (mirrors test_gateway.py)
# ---------------------------------------------------------------------------

class _RunningBroker:
    def __init__(self, broker):
        import uvicorn
        self.broker = broker
        config = uvicorn.Config(
            broker.app, host='127.0.0.1', port=0, log_level='error',
            ws_ping_interval=20.0, ws_ping_timeout=20.0)
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self):
        self._thread.start()
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if self._server.started and self._server.servers:
                socks = self._server.servers[0].sockets
                if socks:
                    self.port = socks[0].getsockname()[1]
                    return self
            time.sleep(0.02)
        raise RuntimeError("broker server did not start")

    @property
    def url(self):
        return 'http://127.0.0.1:%d' % self.port

    def stop(self):
        self._server.should_exit = True
        self._thread.join(timeout=10.0)


@pytest.fixture
def harness(self_signed, tmp_path, monkeypatch):
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_TOKEN', raising=False)
    # Keep the dispatcher's durable store off $HOME.
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
        tmp_path / 'td_state')
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
        tmp_path / 'td_scratch')
    cert, key = self_signed

    servers, runtimes = [], []

    def make_broker(**kw):
        from radical.orbit.broker import Broker, BrokerTuning
        tuning = BrokerTuning(grace=2.0)
        for _k in list(kw):
            if hasattr(tuning, _k):
                setattr(tuning, _k, kw.pop(_k))
        defaults = dict(cert=str(cert), key=str(key), auth=False, tuning=tuning)
        defaults.update(kw)
        srv = _RunningBroker(Broker(**defaults)).start()
        servers.append(srv)
        return srv

    def make_runtime(url, wait=True, serve=None, **kw):
        from radical.orbit.runtime import EndpointRuntime
        defaults = dict(broker_url=url, token=None, ping_interval=1.0,
                        ping_timeout=3.0, backoff_start=0.05, backoff_max=0.2)
        defaults.update(kw)
        rt = EndpointRuntime(**defaults)
        runtimes.append(rt)
        for p in (serve or []):
            rt.serve(p)                    # mount before connecting
        rt.start(wait=wait, timeout=10.0)
        return rt

    yield make_broker, make_runtime

    for rt in runtimes:
        try:    rt.stop()
        except Exception:
            pass
    for srv in servers:
        try:    srv.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _dispatcher(srv):
    return srv.broker._plugin_host._plugins['task_dispatcher']


def test_dispatcher_wired_with_caller_and_tap(harness):
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    td = _dispatcher(srv)
    # The dispatcher is broker-hosted and reaches endpoints via the broker
    # caller; the raw event tap is wired for child task events.
    assert td._broker_caller is srv.broker.caller
    assert td._broker_tap is not None


def test_dispatcher_calls_child_via_caller(harness):
    """End-to-end: the dispatcher's own caller reaches a real child endpoint."""
    make_broker, make_runtime = harness
    srv = make_broker(plugins='task_dispatcher')
    make_runtime(srv.url, name='ep', plugins=['fake_pilot'])

    td   = _dispatcher(srv)
    # Drive the exact seam the dispatcher's `_call` uses (call_threadsafe →
    # routing loop → child endpoint → response), from this thread.
    fut  = td._broker_caller.call_threadsafe('ep', 'GET', '/fake_pilot/ping',
                                             timeout=10.0)
    resp = fut.result(timeout=10.0)
    assert int(resp['status']) == 200
    body = resp['body']
    if isinstance(body, str):
        body = body.encode()
    assert json.loads(body)['pong'] is True


def test_dispatcher_drives_real_psij_helper_via_caller(harness):
    """End-to-end: the dispatcher builds a REAL ``PSIJClient`` wired to the
    broker caller (a sync ``_CallerSyncHTTP`` transport) and drives its plain
    sync helpers via ``asyncio.to_thread`` against a real child endpoint —
    register + ``submit_tunneled`` — with the payload and response crossing the
    routing loop intact.  One helper implementation, caller-backed, host loop
    never blocked.
    """
    import asyncio

    make_broker, make_runtime = harness
    srv = make_broker(plugins='task_dispatcher')
    make_runtime(srv.url, name='ep', serve=[_FakePsij])

    td = _dispatcher(srv)

    async def _drive():
        psij = await td._get_psij_client('ep')      # real PSIJClient over caller
        assert psij is not None
        assert psij.sid                              # session registered
        return await asyncio.to_thread(
            psij.submit_tunneled, {'executable': '/bin/true'}, 'local', 'none')

    result = asyncio.run(_drive())
    assert result['job_id']        == 'j.1'
    assert result['echo_tunnel']   == 'none'         # payload shape crossed intact
    assert result['echo_executor'] == 'local'
    # The client is cached for reuse on the next dispatcher call.
    assert ('ep', 'psij', None) in td._child_clients


def test_gateway_http_to_hosted_dispatcher(harness):
    """Gateway HTTP → broker-hosted dispatcher route (pre-flip item 1)."""
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    with httpx.Client(timeout=10.0) as c:
        r = c.get('%s/broker/task_dispatcher/pools' % srv.url)
    assert r.status_code == 200, r.text
    assert r.json()['pools'] == {}     # no sessions/pools declared yet


def test_gateway_unknown_hosted_route_404(harness):
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    with httpx.Client(timeout=10.0) as c:
        r = c.get('%s/broker/task_dispatcher/nope' % srv.url)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Capability-class pools, end to end (plan 121 §13, acceptance tests 1-5)
#
# These drive a real broker hosting the dispatcher, a real login-node
# endpoint serving the fake psij, and one real child endpoint per pilot
# registered under the **exact** ``child_endpoint_name`` the dispatcher
# recorded -- read out of the pilot record rather than hard-coded, which is
# precisely the naming rule 121 changed.
# ---------------------------------------------------------------------------

def _member(mid, software, **overrides):
    m = {
        'member_id'    : mid,
        'endpoint_name': 'login',
        'queue'        : 'regular',
        'account'      : 'proj',
        'default_size' : 'd',
        'pilot_sizes'  : {'d': {'nodes': 1, 'cpus_per_node': 2,
                                'rhapsody_backend': 'concurrent'}},
        'attributes'   : {'software': list(software)},
    }
    m.update(overrides)
    return m


def _class_pool(members, name='fed', **overrides):
    d = {'name': name, 'pool_class': 'gpu', 'members': members,
         'strategy': 'conservative',
         'strategy_config': {'min_dwell_sec': 0.0}}
    d.update(overrides)
    return d


class _Fed:
    """Broker + dispatcher + login endpoint + a class pool, session 'A'.

    *endpoints* are extra runtimes brought up **before** the pool is
    declared, each serving a pilot's plugin set (rhapsody + staging).  That
    is what an ``allocation``-mode resource looks like to the dispatcher: the
    endpoint is already inside its allocation when it hears about it, so it
    can be adopted on the spot instead of waiting for a handshake.
    """

    def __init__(self, harness, members, tmp_path, endpoints=(), **pool_kw):
        make_broker, make_runtime = harness
        _FakeRhapsody.received.clear()
        _FakeStaging.puts.clear()
        _FakePsij.submitted.clear()
        self.make_runtime = make_runtime
        self.srv = make_broker(plugins='task_dispatcher')
        make_runtime(self.srv.url, name='login', serve=[_FakePsij])
        for name in endpoints:
            make_runtime(self.srv.url, name=name,
                         serve=[_FakeRhapsody, _FakeStaging])
        self.td  = _dispatcher(self.srv)
        self.sid = 'A'
        with httpx.Client(timeout=10.0) as c:
            r = c.post('%s/broker/task_dispatcher/register_session'
                       % self.srv.url,
                       json={'sid': 'A', 'lifetime': 'persistent',
                             'pools': [_class_pool(members, **pool_kw)]})
            assert r.status_code == 200, r.text
        self.ps = self.td._pool_states['A']['fed']

    def post(self, path, body):
        with httpx.Client(timeout=10.0) as c:
            return c.post('%s/broker/task_dispatcher/%s'
                          % (self.srv.url, path), json=body)

    def delete(self, path, body):
        with httpx.Client(timeout=10.0) as c:
            return c.request(
                'DELETE',
                '%s/broker/task_dispatcher/%s' % (self.srv.url, path),
                json=body)

    def submit(self, task_id, **extra):
        body = {'pool': 'fed', 'task_id': task_id, 'cmd': ['/bin/echo', 'x']}
        body.update(extra)
        return self.post('submit/A', body)

    def on_loop(self, fn, timeout=10.0):
        """Run *fn* on the plugin-host loop the dispatcher actually lives
        on -- ``_submit_pilot`` schedules the psij submission with
        ``asyncio.create_task``, so it needs that loop running under it."""
        import concurrent.futures
        fut = concurrent.futures.Future()

        def _run():
            try:
                fut.set_result(fn())
            except Exception as e:              # pragma: no cover - test aid
                fut.set_exception(e)

        self.td._main_loop.call_soon_threadsafe(_run)
        return fut.result(timeout=timeout)

    def tick(self):
        """One housekeeping tick: policy scale-up, then drain."""
        self.on_loop(lambda: self.ps.policy.on_tick(
            self.ps, self.td._make_submit_pilot(self.ps)))

    def wait(self, pred, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(0.05)
        return False

    def bring_up(self, member_id):
        """Submit a pilot for *member_id* and register its child endpoint
        under the name the dispatcher actually recorded."""
        before = set(self.ps.pilots)
        pid    = self.on_loop(
            lambda: self.td._submit_pilot(self.ps, None,
                                          member_id=member_id))
        assert self.wait(lambda: self.ps.pilots[pid].child_endpoint_name)
        rec = self.ps.pilots[pid]
        self.make_runtime(self.srv.url, name=rec.child_endpoint_name,
                          serve=[_FakeRhapsody, _FakeStaging])
        assert self.wait(lambda: self.ps.pilots[pid].state == 'ACTIVE')
        assert set(self.ps.pilots) - before == {pid}
        return rec


def test_class_pool_routes_by_attribute(harness, tmp_path):
    """Test 1: a task lands only on a pilot whose member declares its
    software, and the pilot's child endpoint name carries the member."""
    fed = _Fed(harness, [_member('m_x', ['x']), _member('m_y', ['y'])],
               tmp_path)
    px = fed.bring_up('m_x')
    py = fed.bring_up('m_y')
    assert px.child_endpoint_name == 'fed_m_x_%s' % px.pid
    assert py.child_endpoint_name == 'fed_m_y_%s' % py.pid

    assert fed.submit('t.x', requirements={'software': ['x']}
                      ).status_code == 200
    assert fed.submit('t.y', requirements={'software': ['y']}
                      ).status_code == 200
    assert fed.wait(lambda: len(_FakeRhapsody.received) == 2)

    landed = {td['uid']: ep for ep, td in _FakeRhapsody.received}
    assert landed['t.x'] == px.child_endpoint_name
    assert landed['t.y'] == py.child_endpoint_name
    assert fed.ps.tasks['t.x'].member_id == 'm_x'
    assert fed.ps.tasks['t.y'].member_id == 'm_y'


def test_scale_up_picks_the_matching_member(harness, tmp_path):
    """Test 2: empty fleet, one task -- exactly one pilot, for m_y."""
    fed = _Fed(harness, [_member('m_x', ['x']), _member('m_y', ['y'])],
               tmp_path)
    assert fed.submit('t.1', requirements={'software': ['y']}
                      ).status_code == 200
    fed.tick()
    assert len(fed.ps.pilots) == 1
    assert next(iter(fed.ps.pilots.values())).member_id == 'm_y'


def test_member_removal_drains_to_a_sibling(harness, tmp_path):
    """Test 3: a RUNNING task on m_x is re-queued and picked up by m_y."""
    fed = _Fed(harness, [_member('m_x', ['x', 'shared']),
                         _member('m_y', ['y', 'shared'])], tmp_path)
    pilots = {m: fed.bring_up(m) for m in ('m_x', 'm_y')}
    assert fed.submit('t.1', requirements={'software': ['shared']}
                      ).status_code == 200

    task = fed.ps.tasks['t.1']
    assert fed.wait(lambda: task.pilot_id is not None)
    # either member can serve it; drain whichever one actually got it
    gone  = task.member_id
    other = 'm_y' if gone == 'm_x' else 'm_x'

    r = fed.delete('pool/A/fed/members/%s' % gone, {})
    assert r.status_code == 200, r.text
    assert r.json()['pilots_cancelled'] == 1

    assert pilots[gone].state == 'FAILED'
    assert gone not in fed.ps.config.members
    assert task.requeues == 1
    assert fed.wait(lambda: task.pilot_id == pilots[other].pid)
    assert task.member_id == other


def test_member_removal_fails_an_unsatisfiable_task(harness, tmp_path):
    """Test 3b: no remaining member can serve it -> FAILED with a reason."""
    fed = _Fed(harness, [_member('m_x', ['x']), _member('m_y', ['y'])],
               tmp_path)
    px = fed.bring_up('m_x')
    fed.bring_up('m_y')
    assert fed.submit('t.1', requirements={'software': ['x']}
                      ).status_code == 200
    assert fed.wait(lambda: fed.ps.tasks['t.1'].pilot_id == px.pid)

    r = fed.delete('pool/A/fed/members/m_x', {})
    assert r.status_code == 200, r.text
    assert r.json()['tasks_failed'] == 1
    task = fed.ps.tasks['t.1']
    assert task.state == 'FAILED'
    assert task.error == \
        'no member satisfies the task requirements: software missing: x'


def test_budget_per_member(harness, tmp_path):
    """Test 4: an exhausted member is skipped, and the verbose summary
    reports the two node_hours figures independently."""
    fed = _Fed(harness, [
        _member('m_x', ['shared'], budget={'node_hours': 0.001}),
        _member('m_y', ['shared'], budget={'node_hours': 100.0})],
        tmp_path)
    # burn m_x's budget with a long-running pilot of its own
    now = time.time()
    fed.ps.pilots['p.old'] = __import__(
        'radical.orbit.task_dispatcher_state', fromlist=['PilotRecord']
    ).PilotRecord(
        pid='p.old', pool='fed', owning_sid='A', size_key='d',
        rhapsody_backend='concurrent', state='DONE', member_id='m_x',
        nodes=1, cpus_per_node=2, submitted_at=now - 3600,
        active_at=now - 3600, finished_at=now)

    assert fed.submit('t.1', requirements={'software': ['shared']}
                      ).status_code == 200
    fed.tick()
    fresh = [p for p in fed.ps.pilots.values() if p.pid != 'p.old']
    assert [p.member_id for p in fresh] == ['m_y']

    with httpx.Client(timeout=10.0) as c:
        r = c.get('%s/broker/task_dispatcher/pool/A/fed' % fed.srv.url)
    assert r.status_code == 200, r.text
    members = {m['member_id']: m for m in r.json()['members']}
    assert members['m_x']['node_hours_used'] == pytest.approx(1.0, abs=0.05)
    assert members['m_y']['node_hours_used'] == pytest.approx(0.0, abs=0.05)
    assert members['m_x']['node_hours_remaining'] < 0
    assert r.json()['node_hours_used'] == pytest.approx(1.0, abs=0.05)


def test_inputs_reach_a_non_shared_member(harness, tmp_path):
    """Test 5: the file is ``put`` at <cwd>/<name> BEFORE submit_tasks, and
    the broker creates nothing under the member's scratch_base."""
    import base64
    remote = tmp_path / 'remote_scratch'
    fed = _Fed(harness, [_member('m_x', ['x'], shared_fs=False,
                                 scratch_base=str(remote))], tmp_path)
    px = fed.bring_up('m_x')

    r = fed.submit('t.1', requirements={'software': ['x']},
                   inputs_b64={'md.json':
                               base64.b64encode(b'{"a":1}').decode()})
    assert r.status_code == 200, r.text
    assert fed.wait(lambda: len(_FakeRhapsody.received) == 1)

    task = fed.ps.tasks['t.1']
    assert task.cwd     == str(remote / 't.1')
    assert task.spooled == ['md.json']
    assert _FakeStaging.puts == [
        (px.child_endpoint_name, str(remote / 't.1' / 'md.json'), 7)]
    # the put happened before the task was forwarded
    assert _FakeRhapsody.received[0][1]['uid'] == 't.1'
    # nothing created on the broker host under the member's scratch
    assert not remote.exists()


def test_inputs_reach_a_non_shared_member_through_the_real_staging_plugin(
        harness, tmp_path, monkeypatch):
    """The staging plugin's allow-list is `$HOME` + `/tmp` only, so a real
    member's `scratch_base` (`/pscratch/...`) would be refused with "Path
    escapes allowed directories" -- the dispatcher's own input placement
    blocked by its own staging plugin.  The pilot is started with
    `RADICAL_ORBIT_SCRATCH_BASE` set to that member's scratch_base
    (`_build_pilot_env`), and the staging session extends its allow-list
    from it.

    To *prove* the mechanism rather than ride on /tmp already being
    allowed, the static bases are narrowed to a nonexistent directory here:
    only the env var can let this put through.
    """
    import base64
    from radical.orbit.plugin_staging import PluginStaging, StagingSession

    scratch = tmp_path / 'site_scratch'
    scratch.mkdir()
    monkeypatch.setattr(StagingSession, '_ALLOWED_BASES',
                        ['/nonexistent/base'])
    monkeypatch.setenv('RADICAL_ORBIT_SCRATCH_BASE', str(scratch))

    fed = _Fed(harness, [_member('m_x', ['x'], shared_fs=False,
                                 scratch_base=str(scratch))], tmp_path)

    # the pilot serves the REAL staging plugin, not the fake one
    before = set(fed.ps.pilots)
    pid    = fed.on_loop(
        lambda: fed.td._submit_pilot(fed.ps, None, member_id='m_x'))
    assert fed.wait(lambda: fed.ps.pilots[pid].child_endpoint_name)
    rec = fed.ps.pilots[pid]
    fed.make_runtime(fed.srv.url, name=rec.child_endpoint_name,
                     serve=[_FakeRhapsody, PluginStaging])
    assert fed.wait(lambda: fed.ps.pilots[pid].state == 'ACTIVE')
    assert set(fed.ps.pilots) - before == {pid}

    r = fed.submit('t.1', requirements={'software': ['x']},
                   inputs_b64={'md.json':
                               base64.b64encode(b'{"a":1}').decode()})
    assert r.status_code == 200, r.text
    assert fed.wait(lambda: len(_FakeRhapsody.received) == 1), \
        'task never reached the pilot: %s' % fed.ps.tasks['t.1'].error

    task = fed.ps.tasks['t.1']
    assert task.state == 'RUNNING', task.error
    assert task.cwd   == str(scratch / 't.1')
    # the real plugin actually wrote the bytes where the task will run
    assert (scratch / 't.1' / 'md.json').read_bytes() == b'{"a":1}'


def test_real_staging_refuses_a_scratch_outside_the_allow_list(
        harness, tmp_path, monkeypatch):
    """The negative half: without the env var the same put is refused, and
    the task fails with a reason instead of running without its inputs."""
    import base64
    from radical.orbit.plugin_staging import PluginStaging, StagingSession

    scratch = tmp_path / 'site_scratch'
    scratch.mkdir()
    monkeypatch.setattr(StagingSession, '_ALLOWED_BASES',
                        ['/nonexistent/base'])
    monkeypatch.delenv('RADICAL_ORBIT_SCRATCH_BASE', raising=False)

    fed = _Fed(harness, [_member('m_x', ['x'], shared_fs=False,
                                 scratch_base=str(scratch))], tmp_path)
    pid = fed.on_loop(
        lambda: fed.td._submit_pilot(fed.ps, None, member_id='m_x'))
    assert fed.wait(lambda: fed.ps.pilots[pid].child_endpoint_name)
    fed.make_runtime(fed.srv.url,
                     name=fed.ps.pilots[pid].child_endpoint_name,
                     serve=[_FakeRhapsody, PluginStaging])
    assert fed.wait(lambda: fed.ps.pilots[pid].state == 'ACTIVE')

    assert fed.submit('t.1', requirements={'software': ['x']},
                      inputs_b64={'md.json':
                                  base64.b64encode(b'x').decode()}
                      ).status_code == 200
    task = fed.ps.tasks['t.1']
    assert fed.wait(lambda: task.state == 'FAILED')
    assert task.error.startswith('could not place inputs on the pilot:')
    assert _FakeRhapsody.received == []


def test_broker_cert_path_travels_only_to_shared_members(
        harness, tmp_path, monkeypatch):
    """The broker cert path is a path on the BROKER host.  A shared member
    can use it; a non-shared member runs on another machine where that
    path (the broker user's $HOME) need not exist -- shipping it made every
    remote pilot fail TLS silently.  Such a pilot keeps its endpoint's own
    setting, or the default ~/.radical/orbit/broker_cert.pem on its host."""

    monkeypatch.setenv('RADICAL_ORBIT_BROKER_CERT',
                       '/home/broker/.radical/orbit/broker_cert.pem')
    monkeypatch.setenv('RADICAL_ORBIT_SCRATCH_BASE', str(tmp_path))

    fed = _Fed(harness, [_member('m_shared', ['x']),
                         _member('m_remote', ['y'], shared_fs=False,
                                 scratch_base='/pscratch/u/atomic')],
               tmp_path)

    _FakePsij.submitted.clear()
    for mid in ('m_shared', 'm_remote'):
        pid = fed.on_loop(
            lambda mid=mid: fed.td._submit_pilot(fed.ps, None, member_id=mid))
        assert fed.wait(lambda pid=pid: fed.ps.pilots[pid].child_endpoint_name)

    assert fed.wait(lambda: len(_FakePsij.submitted) == 2)
    envs = {d['job_spec']['environment'].get('RADICAL_ORBIT_MEMBER'):
            d['job_spec']['environment'] for d in _FakePsij.submitted}

    assert envs['m_shared']['RADICAL_ORBIT_BROKER_CERT'] \
        == '/home/broker/.radical/orbit/broker_cert.pem'
    assert 'RADICAL_ORBIT_BROKER_CERT' not in envs['m_remote']
    assert envs['m_remote']['RADICAL_ORBIT_SCRATCH_BASE'] == '/pscratch/u/atomic'


# ---------------------------------------------------------------------------
# An endpoint inside an allocation IS the pilot (plan 122)
#
# Same broker, same class pool, but the member says ``pilot: endpoint``: the
# dispatcher adopts the endpoint it was told about instead of asking psij for
# a second process on it.  These tests assert the *whole* consequence -- no
# psij submit, tasks and staging reaching that endpoint's own plugins.
# ---------------------------------------------------------------------------

_ALLOC_EP = 'alloc_ep'


def test_an_allocation_endpoint_is_adopted_as_its_own_pilot(harness, tmp_path):
    remote = tmp_path / 'alloc_scratch'
    fed = _Fed(harness, [_member('m_a', ['x'], endpoint_name=_ALLOC_EP,
                                 pilot='endpoint', shared_fs=False,
                                 scratch_base=str(remote))],
               tmp_path, endpoints=[_ALLOC_EP])
    assert fed.wait(lambda: _ALLOC_EP in fed.td._connected_endpoints)

    # the floor adopts on the first tick -- min_pilots is forced to 1
    fed.tick()
    assert len(fed.ps.pilots) == 1
    rec = next(iter(fed.ps.pilots.values()))
    assert rec.child_endpoint_name == _ALLOC_EP
    assert rec.psij_job_id         is None
    assert fed.wait(lambda: rec.state == 'ACTIVE')
    # nothing was submitted anywhere: no second endpoint, no psij job
    assert _FakePsij.submitted == []

    # ... and the endpoint runs the work itself, through its own plugins
    import base64
    r = fed.submit('t.1', requirements={'software': ['x']},
                   inputs_b64={'md.json':
                               base64.b64encode(b'{"a":1}').decode()})
    assert r.status_code == 200, r.text
    assert fed.wait(lambda: len(_FakeRhapsody.received) == 1)
    assert _FakeRhapsody.received[0][0] == _ALLOC_EP
    assert _FakeStaging.puts == [
        (_ALLOC_EP, str(remote / 't.1' / 'md.json'), 7)]
    assert fed.ps.tasks['t.1'].member_id == 'm_a'
    # a second tick does not adopt the same endpoint twice
    fed.tick()
    assert len(fed.ps.pilots) == 1


def test_an_absent_endpoint_is_adopted_when_it_connects(harness, tmp_path):
    """PENDING until the topology carries it, then ACTIVE through the very
    same hook a submitted pilot's child goes through."""
    fed = _Fed(harness, [_member('m_a', ['x'], endpoint_name=_ALLOC_EP,
                                 pilot='endpoint')], tmp_path)
    fed.tick()
    rec = next(iter(fed.ps.pilots.values()))
    assert rec.state == 'PENDING'

    fed.make_runtime(fed.srv.url, name=_ALLOC_EP,
                     serve=[_FakeRhapsody, _FakeStaging])
    assert fed.wait(lambda: rec.state == 'ACTIVE')
    assert _FakePsij.submitted == []


def test_a_departing_adopted_endpoint_ends_done(harness, tmp_path):
    """Its allocation ended or it left: DONE, no failure counted, and the
    task it was running is re-queued for a sibling."""
    fed = _Fed(harness, [_member('m_a', ['x'], endpoint_name=_ALLOC_EP,
                                 pilot='endpoint'),
                         _member('m_b', ['x'])],
               tmp_path, endpoints=[_ALLOC_EP])
    assert fed.wait(lambda: _ALLOC_EP in fed.td._connected_endpoints)
    fed.tick()
    rec = next(iter(fed.ps.pilots.values()))
    assert fed.wait(lambda: rec.state == 'ACTIVE')

    assert fed.submit('t.1', requirements={'software': ['x']}
                      ).status_code == 200
    task = fed.ps.tasks['t.1']
    assert fed.wait(lambda: task.pilot_id == rec.pid)

    r = fed.delete('pool/A/fed/members/m_a', {})
    assert r.status_code == 200, r.text
    assert r.json()['pilots_cancelled'] == 1
    assert rec.state == 'DONE'
    assert rec.error is None
    assert task.requeues == 1
