
import asyncio
import pathlib
import pytest
import psij

from unittest.mock import AsyncMock, patch, MagicMock

# pylint: disable=protected-access,unused-import,redefined-outer-name,unused-variable
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_psij import PluginPSIJ


# Mock psij to avoid actual submission
@pytest.fixture
def mock_psij():
    with patch('radical.orbit.plugin_psij.psij') as mock:
        # Mock Job and JobSpec
        mock.Job = MagicMock()
        mock.JobSpec = MagicMock()
        
        # Mock Executor instance
        mock_executor = MagicMock()
        mock_executor.submit = MagicMock()
        
        # Mock Executor class method
        mock.JobExecutor.get_instance.return_value = mock_executor
        
        yield mock


def test_plugin_psij_enabled_on_broker():
    # psij must load on a broker too (embedded broker on a login node)
    app = FastAPI()
    app.state.is_broker = True
    assert PluginPSIJ.is_enabled(app)


def test_plugin_psij_init():
    app = FastAPI()
    plugin = PluginPSIJ(app)
    assert plugin.plugin_name == 'psij'
    assert plugin.instance_name == 'psij'
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    ns = plugin.namespace.lstrip('/')
    assert any(f'{ns}/submit/' in p for p in route_pats)


@pytest.mark.asyncio
async def test_submit_job(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    
    # Mock job instance
    mock_job = MagicMock()
    mock_job.id = 'job.123'
    mock_job.native_id = 'native.123'
    mock_psij.Job.return_value = mock_job

    client = TestClient(app)
    
    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Submit job
    payload = {
        "job_spec": {
            "executable": "/bin/sleep",
            "arguments": ["10"]
        },
        "executor": "local"
    }
    
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'job.123'
    
    # Verify PSIJ calls
    mock_psij.JobSpec.assert_called()
    assert mock_psij.JobExecutor.get_instance.call_args[0] == ('local',)
    assert 'config' in mock_psij.JobExecutor.get_instance.call_args[1]
    
    # Verify job is cached in session
    p_session = plugin._sessions[sid]
    assert 'job.123' in p_session._jobs


@pytest.mark.asyncio
async def test_get_job_status(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)
    
    # Register and manually insert a job into session cache
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']
    
    p_session = plugin._sessions[sid]
    
    mock_job = MagicMock()
    mock_job.id = 'job.123'
    mock_job.native_id = '12345'
    mock_job.status.state = psij.JobState.ACTIVE
    mock_job.status.message = "Running"
    mock_job.status.exit_code = None
    mock_job.status.time = None
    mock_job.spec = MagicMock()
    mock_job.spec.stdout_path = None
    mock_job.spec.stderr_path = None

    p_session._jobs['job.123'] = mock_job
    p_session._job_meta['job.123'] = {
        'executable': '/bin/test',
        'arguments':  [],
        'executor':   'local',
    }
    
    # Get status
    resp = client.get(f"{plugin.namespace}/status/{sid}/job.123")
    assert resp.status_code == 200
    data = resp.json()
    assert data['state'] == str(psij.JobState.ACTIVE)
    assert data['message'] == "Running"


@pytest.mark.asyncio
async def test_cancel_job(mock_psij):
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    p_session = plugin._sessions[sid]

    mock_job = MagicMock()
    p_session._jobs['job.123'] = mock_job

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/job.123")
    assert resp.status_code == 200
    assert resp.json()['status'] == 'canceled'

    mock_job.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_submit_tunneled_missing_name(mock_psij):
    """submit_tunneled returns 422 when -n/--name is absent from arguments."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-orbit-endpoint-wrapper.sh",
            "arguments": ["--url", "http://broker:8000"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_tunneled_no_tunnel(mock_psij):
    """submit_tunneled without tunnel submits the job and returns endpoint_name."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'endpoint-job.1'
    mock_job.native_id = '99999'
    mock_psij.Job.return_value = mock_job

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-orbit-endpoint-wrapper.sh",
            "arguments": ["--url", "http://broker:8000", "-n", "test-endpoint"]
        },
        "executor": "local",
        "tunnel": "none",
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'endpoint-job.1'
    assert data['endpoint_name'] == 'test-endpoint'
    # No watcher created when tunnel='none'
    assert 'test-endpoint' not in plugin._watchers


@pytest.mark.asyncio
async def test_submit_tunneled_with_tunnel(mock_psij):
    """submit_tunneled with tunnel='forward' injects --tunnel into args and spawns watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'endpoint-job.2'
    mock_job.native_id = '88888'
    mock_psij.Job.return_value = mock_job

    with patch('radical.orbit.plugin_psij.asyncio.create_task') as mock_create_task:
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mock_create_task.return_value = mock_task

        client = TestClient(app)
        resp = client.post(f"{plugin.namespace}/register_session")
        sid = resp.json()['sid']

        payload = {
            "job_spec": {
                "executable": "radical-orbit-endpoint-wrapper.sh",
                "arguments": ["--url", "http://broker:8000", "-n", "tunnel-endpoint"],
            },
            "executor": "slurm",
            "tunnel": "forward",
        }
        resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data['endpoint_name'] == 'tunnel-endpoint'

        # Watcher task was created
        assert mock_create_task.called

        # --tunnel forward must have been injected into the job arguments
        spec_instance = mock_psij.JobSpec.return_value
        assert '--tunnel' in spec_instance.arguments
        idx = spec_instance.arguments.index('--tunnel')
        assert spec_instance.arguments[idx + 1] == 'forward'
        assert '--tunnel-via' in spec_instance.arguments


@pytest.mark.asyncio
async def test_submit_tunneled_duplicate_watcher(mock_psij):
    """submit_tunneled returns 409 if a live watcher already exists for that endpoint."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Pre-install a running watcher
    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['dup-endpoint'] = mock_task

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-orbit-endpoint-wrapper.sh",
            "arguments": ["--url", "http://broker:8000", "-n", "dup-endpoint"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 409


def test_tunnel_status_no_tunnel():
    """tunnel_status returns 'no_tunnel' for an endpoint with no watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.get(f"{plugin.namespace}/tunnel_status/no-such-endpoint")
    assert resp.status_code == 200
    data = resp.json()
    assert data['status'] == 'no_tunnel'
    assert data['port'] is None


def test_tunnel_status_active(tmp_path):
    """tunnel_status returns 'active' + port when relay file is present."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Write a relay file
    relay_file = tmp_path / 'myendpoint.port'
    relay_file.write_text('12345')

    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['myendpoint'] = mock_task

    with patch('radical.orbit.tunnel.RELAY_BASE', tmp_path):
        client = TestClient(app)
        resp = client.get(f"{plugin.namespace}/tunnel_status/myendpoint")
        assert resp.status_code == 200
        data = resp.json()
        assert data['status'] == 'active'
        assert data['port'] == 12345


_VANISHED_MSG = 'vanished from queue'


async def _drive_watcher(plugin, state_seq, tmp_path, caplog):
    """Run the watcher with a scripted job_state sequence; UNKNOWN past the
    end.  Returns the number of polls actually consumed."""
    import logging as _logging
    idx = {'i': 0}
    def _next_state(_nid):
        i = idx['i']
        idx['i'] = i + 1
        return state_seq[i] if i < len(state_seq) else 'UNKNOWN'
    fake_batch = MagicMock()
    fake_batch.name = 'slurm'
    fake_batch.job_state = _next_state
    # The radical.orbit logger has propagate=False so external
    # ``basicConfig(force=True)`` calls during runtime can't wipe its
    # file handler.  pytest's ``caplog`` attaches its capture handler
    # only to the root logger, so we have to add it to radical.orbit
    # manually for these tests to see the warning.
    re_log = _logging.getLogger('radical.orbit')
    re_log.addHandler(caplog.handler)
    try:
        with patch('radical.orbit.batch_system.detect_batch_system',
                   return_value=fake_batch), \
             patch('radical.orbit.tunnel.RELAY_BASE', tmp_path), \
             patch('radical.orbit.plugin_psij.asyncio.sleep',
                   new=AsyncMock(return_value=None)):
            caplog.set_level(_logging.WARNING, logger='radical.orbit')
            await asyncio.wait_for(
                plugin._watch_forward('endpoint1', '12345'),
                timeout=5.0)
    finally:
        re_log.removeHandler(caplog.handler)
    return idx['i']


@pytest.mark.asyncio
async def test_tunnel_watcher_aborts_on_unknown_after_running(tmp_path, caplog):
    """Job runs, then disappears — watcher bails after the UNKNOWN streak."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    polls = await _drive_watcher(
        plugin, ['PENDING', 'RUNNING', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'],
        tmp_path, caplog)
    assert polls <= 6, f"watcher polled too many times: {polls}"
    assert any(_VANISHED_MSG in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_tunnel_watcher_aborts_on_unknown_after_pending(tmp_path, caplog):
    """Job cancelled while still pending — watcher bails after the UNKNOWN streak."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    polls = await _drive_watcher(
        plugin, ['PENDING', 'PENDING', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'],
        tmp_path, caplog)
    assert polls <= 6, f"watcher polled too many times: {polls}"
    assert any(_VANISHED_MSG in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_tunnel_watcher_tolerates_initial_unknown(tmp_path, caplog):
    """A short UNKNOWN streak BEFORE the scheduler ack must not bail out —
    that's a transient squeue glitch.  Once PENDING is observed, future
    UNKNOWNs reset the seen_known flag's protection."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    # 3 UNKNOWNs, then PENDING — must NOT bail; needs another 3 UNKNOWNs
    # AFTER PENDING to bail.  Sequence below has 3 leading UNKNOWN, then
    # PENDING, then 3 UNKNOWN — should bail after the second streak.
    polls = await _drive_watcher(
        plugin, ['UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'PENDING',
                 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'],
        tmp_path, caplog)
    # We tolerated the first UNKNOWN streak (3 polls), saw PENDING (1), then
    # bailed on the second UNKNOWN streak (3 more polls) → ~7 polls total.
    assert polls <= 8, f"watcher polled too many times: {polls}"
    assert any(_VANISHED_MSG in r.message for r in caplog.records)


# ─────────────────────────────────────────────────────────────────────────────
#  Reverse-tunnel watcher
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reverse_tunnel_watcher_happy_path(tmp_path, monkeypatch):
    """Reverse mode: job RUNNING → spawn ssh -R succeeds → port file written
    → watcher sees the rendezvous file and reports active."""
    import logging as _logging
    from radical.orbit import tunnel as _tunnel
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    app = FastAPI()
    plugin = PluginPSIJ(app)
    app.state.broker_url = 'https://broker:8000'

    fake_batch = MagicMock()
    fake_batch.name = 'slurm'
    # Normalised state vocabulary: PENDING / RUNNING / DONE / FAILED / etc.
    states = iter(['PENDING', 'RUNNING', 'RUNNING', 'DONE'])
    fake_batch.job_state = lambda _nid: next(states, 'DONE')
    fake_batch.job_nodes = lambda _nid: ['nid001']

    relay_file = tmp_path / 'endpoint-r.port'
    # New protocol: the child writes a .req file with its hostname; the
    # watcher reads it, then spawns ssh -R.  Simulate the child here.
    import json
    relay_file.with_suffix('.req').write_text(json.dumps({"hostname": "nid001"}))

    def fake_spawn(compute_host, bhost, bport, endpoint_name,
                   allocate_timeout=30.0):
        # Simulate sshd allocating remote port 54321 + writing rendezvous file
        relay_file.write_text('54321')
        proc = MagicMock()
        proc.poll.return_value = None
        return proc, 54321

    async def _to_thread_inline(fn, *args, **kw):
        return fn(*args, **kw)

    with patch('radical.orbit.batch_system.detect_batch_system',
               return_value=fake_batch), \
         patch('radical.orbit.tunnel.spawn_reverse_tunnel',
               side_effect=fake_spawn), \
         patch('radical.orbit.plugin_psij.asyncio.sleep',
               new=AsyncMock(return_value=None)), \
         patch('radical.orbit.plugin_psij.asyncio.to_thread',
               new=_to_thread_inline):
        await asyncio.wait_for(
            plugin._watch_reverse('sid-r', 'endpoint-r', '12345',
                                  'endpoint-job.r'),
            timeout=5.0)

    # No tunnel-failure recorded; the job_id is NOT in _failure_reasons
    assert 'endpoint-job.r' not in plugin._failure_reasons


@pytest.mark.asyncio
async def test_reverse_tunnel_watcher_spawn_failure(tmp_path, monkeypatch):
    """Reverse mode: ssh -R spawn raises → watcher records reason and
    fires a cancel; ``get_job_status`` would surface FAILED + the reason."""
    import logging as _logging
    from radical.orbit import tunnel as _tunnel
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    app = FastAPI()
    plugin = PluginPSIJ(app)
    app.state.broker_url = 'https://broker:8000'

    fake_batch = MagicMock()
    fake_batch.name = 'slurm'
    states = iter(['PENDING', 'RUNNING'])
    fake_batch.job_state = lambda _nid: next(states, 'RUNNING')
    fake_batch.job_nodes = lambda _nid: ['nid001']

    relay_file = tmp_path / 'endpoint-rfail.port'
    # New protocol: the child writes a .req file with its hostname; the
    # watcher reads it, then spawns ssh -R.  Simulate the child here.
    import json
    relay_file.with_suffix('.req').write_text(json.dumps({"hostname": "nid001"}))

    def fake_spawn_raises(*a, **kw):
        raise RuntimeError("ssh: connection refused (gatewayports denied)")

    cancelled: list = []

    async def fake_forward(sid, func, **kwargs):
        # _fail_tunnel cancels the useless allocation via the owning session.
        cancelled.append(kwargs.get('job_id'))
        return {}

    with patch('radical.orbit.batch_system.detect_batch_system',
               return_value=fake_batch), \
         patch('radical.orbit.tunnel.spawn_reverse_tunnel',
               side_effect=fake_spawn_raises), \
         patch.object(plugin, '_forward', new=fake_forward), \
         patch('radical.orbit.plugin_psij.asyncio.sleep',
               new=AsyncMock(return_value=None)):
        await asyncio.wait_for(
            plugin._watch_reverse('sid-rf', 'endpoint-rfail', '54321',
                                  'endpoint-job.rfail'),
            timeout=5.0)

    assert 'endpoint-job.rfail' in plugin._failure_reasons
    assert 'connection refused' in plugin._failure_reasons['endpoint-job.rfail']
    assert 'endpoint-job.rfail' in cancelled


def test_get_job_status_overrides_cancelled_to_failed(mock_psij):
    """When a job_id is in _failure_reasons, get_job_status synthesises
    state='FAILED' + error.  Operator-cancelled jobs (no entry) keep
    their natural CANCELLED state."""
    from unittest.mock import patch as _patch
    app = FastAPI()
    plugin = PluginPSIJ(app)
    plugin._failure_reasons['endpoint-job.99'] = 'reverse SSH tunnel: oops'

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    async def _fwd(self, *args, **kwargs):
        # Pretend PsiJ reports CANCELLED for the requested job_id.
        return {'job_id': kwargs.get('job_id'), 'state': 'CANCELLED'}

    with _patch.object(PluginPSIJ, '_forward', _fwd):
        # Failure-tagged job: state must be overridden to FAILED + error.
        resp = client.get(f"{plugin.namespace}/status/{sid}/endpoint-job.99")
        assert resp.status_code == 200
        data = resp.json()
        assert data['state'] == 'FAILED'
        assert 'oops' in data['error']

        # Untagged job: natural CANCELLED state preserved.
        resp = client.get(f"{plugin.namespace}/status/{sid}/endpoint-job.42")
        assert resp.status_code == 200
        data = resp.json()
        assert data['state'] == 'CANCELLED'
        assert 'error' not in data


def test_submit_tunneled_rejects_boolean_tunnel(mock_psij):
    """Boolean tunnel field is no longer accepted — must be a string."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {"job_spec": {"executable": "x", "arguments": ["-n", "e"]},
               "executor": "local", "tunnel": True}
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 400
    assert 'tunnel must be one of' in resp.json()['detail']


def test_psij_dir_follows_the_environment(monkeypatch, tmp_path):
    # HPC home directories are quota'd: the whole psij tree (output capture
    # and psij's own work directory) must be relocatable after import
    from radical.orbit import plugin_psij as m
    monkeypatch.delenv('RADICAL_ORBIT_PSIJ_DIR', raising=False)
    assert m._psij_dir() == m.pathlib.Path.home() / '.radical' / 'orbit' / 'psij'
    monkeypatch.setenv('RADICAL_ORBIT_PSIJ_DIR', str(tmp_path / 'psij'))
    assert m._psij_dir() == tmp_path / 'psij'
    assert m._output_base() == tmp_path / 'psij' / 'output'


# --- session lifetime: is_busy and final terminal states --------------------

def _psij_session(mock_psij):
    app    = FastAPI()
    plugin = PluginPSIJ(app)
    sid    = TestClient(app).post(
        f"{plugin.namespace}/register_session").json()['sid']
    sess   = plugin._sessions[sid]
    sent   = []
    sess.notify = lambda topic, data: sent.append(data)
    return sess, sent


def _status(state):
    st = MagicMock()
    st.state     = state
    st.exit_code = 0
    return st


def test_psij_failed_submit_is_recorded_failed(mock_psij):
    sess, sent = _psij_session(mock_psij)
    job        = MagicMock()
    job.id     = 'job.1'
    mock_psij.Job.return_value = job
    mock_psij.JobExecutor.get_instance.return_value.submit.side_effect = \
        RuntimeError('no scheduler')

    with pytest.raises(Exception):
        asyncio.run(sess.submit_job({'executable': '/bin/true'}, 'local'))

    assert sess._job_states['job.1'] == 'FAILED'
    assert sent[-1]['state'] == 'FAILED'
    assert 'no scheduler' in sent[-1]['stderr']
    assert sess.is_busy() is False


def test_psij_is_busy_while_a_job_is_not_terminal(mock_psij):
    sess, _ = _psij_session(mock_psij)
    assert sess.is_busy() is False
    sess._jobs['job.1'] = MagicMock()
    assert sess.is_busy() is True
    sess._notify_state('job.1', _status('COMPLETED'))
    assert sess.is_busy() is False


def test_psij_terminal_state_is_final(mock_psij):
    sess, sent = _psij_session(mock_psij)
    sess._jobs['job.1'] = MagicMock()
    sess._notify_state('job.1', _status('COMPLETED'))
    sess._notify_state('job.1', _status('ACTIVE'))
    assert sess._job_states['job.1'] == 'COMPLETED'
    assert [d['state'] for d in sent] == ['COMPLETED']


def test_psij_requested_cancel_remaps_a_racing_completion(mock_psij):
    sess, sent = _psij_session(mock_psij)
    sess._jobs['job.1'] = MagicMock()
    sess._notify_state('job.1', _status('ACTIVE'))
    asyncio.run(sess.cancel_job('job.1'))
    sess._notify_state('job.1', _status('COMPLETED'))
    assert sess._job_states['job.1'] == 'CANCELED'


def test_psij_cancel_of_a_finished_job_keeps_its_state(mock_psij):
    sess, sent = _psij_session(mock_psij)
    sess._jobs['job.1'] = MagicMock()
    sess._notify_state('job.1', _status('COMPLETED'))
    asyncio.run(sess.cancel_job('job.1'))
    sess._notify_state('job.1', _status('COMPLETED'))
    assert sess._job_states['job.1'] == 'COMPLETED'
    assert 'job.1' not in sess._cancelled_jobs


def test_psij_cancel_racing_a_recorded_completion_keeps_it(mock_psij):
    '''A job recorded COMPLETED stays COMPLETED even if a cancel lands in
    _cancelled_jobs afterwards (race with the status callback).'''
    sess, sent = _psij_session(mock_psij)
    sess._jobs['job.1'] = MagicMock()
    sess._notify_state('job.1', _status('COMPLETED'))
    sess._cancelled_jobs.add('job.1')
    sess._notify_state('job.1', _status('COMPLETED'))
    assert sess._job_states['job.1'] == 'COMPLETED'
    assert [d['state'] for d in sent] == ['COMPLETED']


# --- graceful cancel of endpoint jobs ------------------------------------------

class _FakeRuntime:
    '''The host runtime as plugin_psij sees it: name, topology, and the
    broker ``disconnect`` control.  ``on_disconnect`` runs when the control
    is "sent" (e.g. to let the fake job end).'''

    def __init__(self, connected=(), on_disconnect=None):
        self.name          = 'login'
        self.connected     = set(connected)
        self.disconnects   = []
        self.on_disconnect = on_disconnect

    def topology(self):
        return {n: {'role': 'endpoint', 'plugins': {}, 'liveness': 'alive'}
                for n in self.connected}

    def disconnect_endpoint(self, name):
        self.disconnects.append(name)
        if self.on_disconnect:
            self.on_disconnect(name)
        return True

    def send_notification(self, *a, **kw):
        pass


def _endpoint_plugin(mock_psij, rt, executor='local', name='child-ep',
                     caller=None):
    '''A psij plugin on a fake runtime, with one endpoint job submitted
    through ``submit_tunneled`` (tunnel 'none') and currently ACTIVE.'''
    app = FastAPI()
    app.state.endpoint_service = rt
    app.state.endpoint_name    = rt.name
    if caller is not None:
        app.state.broker_caller = caller
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    job = MagicMock()
    job.id        = 'endpoint-job.g'
    job.native_id = '4242'
    job.status.state = psij.JobState.ACTIVE
    mock_psij.Job.return_value = job

    sid  = client.post(f"{plugin.namespace}/register_session").json()['sid']
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json={
        "job_spec": {"executable": "radical-orbit-endpoint-wrapper.sh",
                     "arguments": ["-n", name, "-p", "default"]},
        "executor": executor, "tunnel": "none"})
    assert resp.status_code == 200
    return plugin, client, sid, job


def test_cancel_endpoint_job_stops_endpoint_gracefully(mock_psij, monkeypatch):
    '''Endpoint connected: cancel sends the broker disconnect, the job ends
    on its own within the grace period, and no batch cancel is issued.'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt)
    rt.connected.add('child-ep')            # the child registered after submit

    def _ends(name):
        job.status.state = psij.JobState.COMPLETED
    rt.on_disconnect = _ends

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.status_code == 200
    assert resp.json() == {'job_id': job.id, 'status': 'canceled',
                           'graceful': True}
    assert rt.disconnects == ['child-ep']
    job.cancel.assert_not_called()
    # still reported as the requested cancel
    assert plugin._sessions[sid]._effective_state(job.id, 'COMPLETED') \
        == 'CANCELED'


def test_cancel_endpoint_job_sees_batch_end_before_psij(mock_psij, monkeypatch):
    '''psij polls the queue only every 30 s: the batch system's own view of
    the job ending counts too (when the job used the detected backend).'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    backend = MagicMock()
    backend.psij_executor = 'slurm'
    backend.job_state.side_effect = ['RUNNING', 'DONE']
    monkeypatch.setattr(m.batch_system, 'detect_batch_system',
                        lambda: backend)
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt,
                                                executor='slurm')
    rt.connected.add('child-ep')

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.json()['graceful'] is True
    job.cancel.assert_not_called()
    assert backend.job_state.call_args[0] == ('4242',)


@pytest.mark.parametrize('cancel_fails', [False, True])
def test_cancel_endpoint_job_left_the_queue(mock_psij, monkeypatch,
                                            cancel_fails):
    '''UNKNOWN after RUNNING, UNKNOWN_TOLERANCE times in a row: the job left
    the queue -- or squeue failed (it answers UNKNOWN then, too).  Not a
    confirmed end: job.cancel() still runs (its error swallowed, the job is
    likely gone), and the reply says graceful False.  The trailing RUNNING
    is the squeue-outage case: the job was alive all along.'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    backend = MagicMock()
    backend.psij_executor = 'slurm'
    backend.job_state.side_effect = ['UNKNOWN', 'RUNNING'] + \
                                    ['UNKNOWN'] * 3 + ['RUNNING']
    monkeypatch.setattr(m.batch_system, 'detect_batch_system',
                        lambda: backend)
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt,
                                                executor='slurm')
    rt.connected.add('child-ep')
    if cancel_fails:
        job.cancel.side_effect = RuntimeError('scancel: invalid job id')

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.status_code == 200
    assert resp.json() == {'job_id': job.id, 'status': 'canceled',
                           'graceful': False, 'endpoint_stopped': True}
    assert rt.disconnects == ['child-ep']
    job.cancel.assert_called_once()
    assert backend.job_state.call_count == 5


@pytest.mark.parametrize('batch_state, stopped', [('PENDING', False),
                                                  ('UNKNOWN', False),
                                                  ('RUNNING', True)])
def test_cancel_endpoint_job_only_when_running(mock_psij, monkeypatch,
                                               batch_state, stopped):
    '''A queued job cannot have registered the endpoint: the endpoint that
    holds the name (an earlier job with the same -n, another user's) is not
    ours and must not be stopped.  psij lags behind the batch system, so
    batch RUNNING counts as active even while psij still says QUEUED.'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    backend = MagicMock()
    backend.psij_executor = 'slurm'
    backend.job_state.side_effect = [batch_state, 'DONE']
    monkeypatch.setattr(m.batch_system, 'detect_batch_system',
                        lambda: backend)
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt,
                                                executor='slurm')
    job.status.state = psij.JobState.QUEUED
    rt.connected.add('child-ep')          # someone holds the name

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    if stopped:
        assert rt.disconnects == ['child-ep']
        assert resp.json()['graceful'] is True
        job.cancel.assert_not_called()
    else:
        assert rt.disconnects == []
        assert resp.json() == {'job_id': job.id, 'status': 'canceled'}
        job.cancel.assert_called_once()


def test_cancel_queued_local_job_skips_graceful_step(mock_psij):
    '''No batch backend to ask: psij's own state decides.'''
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt)
    job.status.state = psij.JobState.QUEUED
    rt.connected.add('child-ep')
    client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert rt.disconnects == []
    job.cancel.assert_called_once()


def test_cancel_endpoint_job_falls_back_after_grace(mock_psij, monkeypatch):
    '''The job does not end within the grace period: job.cancel() follows.'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    monkeypatch.setenv('RADICAL_ORBIT_PSIJ_CANCEL_GRACE', '0.1')
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt)
    rt.connected.add('child-ep')

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.json() == {'job_id': job.id, 'status': 'canceled',
                           'graceful': False, 'endpoint_stopped': True}
    assert rt.disconnects == ['child-ep']
    job.cancel.assert_called_once()


@pytest.mark.parametrize('case', ['not_connected', 'not_owned', 'grace_0',
                                  'plain_job'])
def test_cancel_without_graceful_step(mock_psij, monkeypatch, case):
    '''No disconnect, immediate job.cancel(): a pending endpoint job (not
    connected yet), a name that was already connected at submit (not ours),
    a grace of 0, and an ordinary psij job.'''
    rt = _FakeRuntime(connected=['child-ep'] if case == 'not_owned' else [])
    if case == 'grace_0':
        monkeypatch.setenv('RADICAL_ORBIT_PSIJ_CANCEL_GRACE', '0')
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt)
    if case in ('grace_0',):
        rt.connected.add('child-ep')
    if case == 'plain_job':
        rt.connected.add('child-ep')
        plugin._sessions[sid]._job_meta[job.id].pop('endpoint_name')

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.json() == {'job_id': job.id, 'status': 'canceled'}
    assert rt.disconnects == []
    job.cancel.assert_called_once()


def test_cancel_never_stops_the_host_endpoint(mock_psij):
    rt = _FakeRuntime()
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt, name='login')
    rt.connected.add('login')
    client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert rt.disconnects == []
    job.cancel.assert_called_once()


def test_cancel_endpoint_job_from_broker_hosted_psij(mock_psij, monkeypatch):
    '''psij hosted on the (embedded) broker -- the HPPSS demo setup: the
    disconnect goes through the broker seam, not a runtime control frame.'''
    from radical.orbit import plugin_psij as m
    monkeypatch.setattr(m, 'CANCEL_POLL', 0.01)
    rt      = _FakeRuntime()
    rt.name = 'broker'
    caller  = MagicMock()
    caller.connected.return_value = False          # at submit: not connected
    plugin, client, sid, job = _endpoint_plugin(mock_psij, rt, caller=caller)
    caller.connected.return_value = True

    def _ends(name):
        job.status.state = psij.JobState.COMPLETED
    caller.disconnect_threadsafe.side_effect = _ends

    resp = client.post(f"{plugin.namespace}/cancel/{sid}/{job.id}")
    assert resp.json()['graceful'] is True
    caller.disconnect_threadsafe.assert_called_once_with('child-ep')
    assert rt.disconnects == []
    job.cancel.assert_not_called()


def test_cancel_grace_from_environment(monkeypatch):
    from radical.orbit import plugin_psij as m
    monkeypatch.delenv('RADICAL_ORBIT_PSIJ_CANCEL_GRACE', raising=False)
    assert m._cancel_grace() == 30.0
    monkeypatch.setenv('RADICAL_ORBIT_PSIJ_CANCEL_GRACE', '5')
    assert m._cancel_grace() == 5.0
    for bad in ('-1', 'nan', 'inf', 'soon'):
        monkeypatch.setenv('RADICAL_ORBIT_PSIJ_CANCEL_GRACE', bad)
        assert m._cancel_grace() == 30.0


# --- endpoint job output survives session close --------------------------------

def test_endpoint_job_output_survives_session_close(mock_psij, monkeypatch,
                                                    tmp_path):
    monkeypatch.setenv('RADICAL_ORBIT_PSIJ_DIR', str(tmp_path / 'psij'))
    plugin, client, sid, job = _endpoint_plugin(mock_psij, _FakeRuntime())
    sess = plugin._sessions[sid]
    sess._jobs['plain.1'] = MagicMock()
    out_dir = sess._output_dir
    for jid in (job.id, 'plain.1'):
        for ext in ('out', 'err'):
            (out_dir / f'{jid}.{ext}').write_text(f'{jid} {ext}\n')

    asyncio.run(sess.close())

    assert (out_dir / f'{job.id}.out').read_text() == f'{job.id} out\n'
    assert (out_dir / f'{job.id}.err').exists()
    assert not (out_dir / 'plain.1.out').exists()
    assert not (out_dir / 'plain.1.err').exists()


def test_plain_session_output_removed_on_close(mock_psij, monkeypatch,
                                               tmp_path):
    monkeypatch.setenv('RADICAL_ORBIT_PSIJ_DIR', str(tmp_path / 'psij'))
    sess, _ = _psij_session(mock_psij)
    sess._jobs['plain.1'] = MagicMock()
    (sess._output_dir / 'plain.1.out').write_text('x')
    asyncio.run(sess.close())
    assert not sess._output_dir.exists()
