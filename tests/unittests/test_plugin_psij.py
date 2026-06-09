
import asyncio
import pathlib
import pytest
import psij

from unittest.mock import AsyncMock, patch, MagicMock

# pylint: disable=protected-access,unused-import,redefined-outer-name,unused-variable
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_psij import PluginPSIJ


# Mock psij to avoid actual submission
@pytest.fixture
def mock_psij():
    with patch('radical.edge.plugin_psij.psij') as mock:
        # Mock Job and JobSpec
        mock.Job = MagicMock()
        mock.JobSpec = MagicMock()
        
        # Mock Executor instance
        mock_executor = MagicMock()
        mock_executor.submit = MagicMock()
        
        # Mock Executor class method
        mock.JobExecutor.get_instance.return_value = mock_executor
        
        yield mock


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
    mock_psij.JobExecutor.get_instance.assert_called_with('local')
    
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
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_tunneled_no_tunnel(mock_psij):
    """submit_tunneled without tunnel submits the job and returns edge_name."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'edge-job.1'
    mock_job.native_id = '99999'
    mock_psij.Job.return_value = mock_job

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000", "-n", "test-edge"]
        },
        "executor": "local",
        "tunnel": "none",
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data['job_id'] == 'edge-job.1'
    assert data['edge_name'] == 'test-edge'
    # No watcher created when tunnel='none'
    assert 'test-edge' not in plugin._watchers


@pytest.mark.asyncio
async def test_submit_tunneled_with_tunnel(mock_psij):
    """submit_tunneled with tunnel='forward' injects --tunnel into args and spawns watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    mock_job = MagicMock()
    mock_job.id = 'edge-job.2'
    mock_job.native_id = '88888'
    mock_psij.Job.return_value = mock_job

    with patch('radical.edge.plugin_psij.asyncio.create_task') as mock_create_task:
        mock_task = MagicMock()
        mock_task.done.return_value = False
        mock_create_task.return_value = mock_task

        client = TestClient(app)
        resp = client.post(f"{plugin.namespace}/register_session")
        sid = resp.json()['sid']

        payload = {
            "job_spec": {
                "executable": "radical-edge-wrapper.sh",
                "arguments": ["--url", "http://bridge:8000", "-n", "tunnel-edge"],
            },
            "executor": "slurm",
            "tunnel": "forward",
        }
        resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data['edge_name'] == 'tunnel-edge'

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
    """submit_tunneled returns 409 if a live watcher already exists for that edge."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Pre-install a running watcher
    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['dup-edge'] = mock_task

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    payload = {
        "job_spec": {
            "executable": "radical-edge-wrapper.sh",
            "arguments": ["--url", "http://bridge:8000", "-n", "dup-edge"]
        },
        "executor": "local"
    }
    resp = client.post(f"{plugin.namespace}/submit_tunneled/{sid}", json=payload)
    assert resp.status_code == 409


def test_tunnel_status_no_tunnel():
    """tunnel_status returns 'no_tunnel' for an edge with no watcher."""
    app = FastAPI()
    plugin = PluginPSIJ(app)
    client = TestClient(app)

    resp = client.get(f"{plugin.namespace}/tunnel_status/no-such-edge")
    assert resp.status_code == 200
    data = resp.json()
    assert data['status'] == 'no_tunnel'
    assert data['port'] is None


def test_tunnel_status_active(tmp_path):
    """tunnel_status returns 'active' + port when relay file is present."""
    app = FastAPI()
    plugin = PluginPSIJ(app)

    # Write a relay file
    relay_file = tmp_path / 'myedge.port'
    relay_file.write_text('12345')

    mock_task = MagicMock()
    mock_task.done.return_value = False
    plugin._watchers['myedge'] = mock_task

    with patch('radical.edge.plugin_psij._relay_dir', return_value=tmp_path):
        client = TestClient(app)
        resp = client.get(f"{plugin.namespace}/tunnel_status/myedge")
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
    relay_file = tmp_path / 'edge.port'
    # The radical.edge logger has propagate=False so external
    # ``basicConfig(force=True)`` calls during runtime can't wipe its
    # file handler.  pytest's ``caplog`` attaches its capture handler
    # only to the root logger, so we have to add it to radical.edge
    # manually for these tests to see the warning.
    re_log = _logging.getLogger('radical.edge')
    re_log.addHandler(caplog.handler)
    try:
        with patch('radical.edge.batch_system.detect_batch_system',
                   return_value=fake_batch), \
             patch('radical.edge.plugin_psij.asyncio.sleep',
                   new=AsyncMock(return_value=None)):
            caplog.set_level(_logging.WARNING, logger='radical.edge')
            await asyncio.wait_for(
                plugin._tunnel_watcher('edge1', '12345', 'edge-job.x',
                                        relay_file, 'forward'),
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
    from radical.edge import tunnel as _tunnel
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    app = FastAPI()
    plugin = PluginPSIJ(app)
    app.state.bridge_url = 'https://bridge:8000'

    fake_batch = MagicMock()
    fake_batch.name = 'slurm'
    # Normalised state vocabulary: PENDING / RUNNING / DONE / FAILED / etc.
    states = iter(['PENDING', 'RUNNING', 'RUNNING', 'DONE'])
    fake_batch.job_state = lambda _nid: next(states, 'DONE')
    fake_batch.job_nodes = lambda _nid: ['nid001']

    relay_file = tmp_path / 'edge-r.port'
    # New protocol: the child writes a .req file with its hostname; the
    # watcher reads it, then spawns ssh -R.  Simulate the child here.
    import json
    relay_file.with_suffix('.req').write_text(json.dumps({"hostname": "nid001"}))

    def fake_spawn(compute_host, bhost, bport, edge_name,
                   allocate_timeout=30.0):
        # Simulate sshd allocating remote port 54321 + writing rendezvous file
        relay_file.write_text('54321')
        proc = MagicMock()
        proc.poll.return_value = None
        return proc, 54321

    async def _to_thread_inline(fn, *args, **kw):
        return fn(*args, **kw)

    with patch('radical.edge.batch_system.detect_batch_system',
               return_value=fake_batch), \
         patch('radical.edge.tunnel.spawn_reverse_tunnel',
               side_effect=fake_spawn), \
         patch('radical.edge.plugin_psij.asyncio.sleep',
               new=AsyncMock(return_value=None)), \
         patch('radical.edge.plugin_psij.asyncio.to_thread',
               new=_to_thread_inline):
        await asyncio.wait_for(
            plugin._tunnel_watcher('edge-r', '12345', 'edge-job.r',
                                    relay_file, 'reverse'),
            timeout=5.0)

    # No tunnel-failure recorded; the job_id is NOT in _failure_reasons
    assert 'edge-job.r' not in plugin._failure_reasons


@pytest.mark.asyncio
async def test_reverse_tunnel_watcher_spawn_failure(tmp_path, monkeypatch):
    """Reverse mode: ssh -R spawn raises → watcher records reason and
    fires a cancel; ``get_job_status`` would surface FAILED + the reason."""
    import logging as _logging
    from radical.edge import tunnel as _tunnel
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)

    app = FastAPI()
    plugin = PluginPSIJ(app)
    app.state.bridge_url = 'https://bridge:8000'

    fake_batch = MagicMock()
    fake_batch.name = 'slurm'
    states = iter(['PENDING', 'RUNNING'])
    fake_batch.job_state = lambda _nid: next(states, 'RUNNING')
    fake_batch.job_nodes = lambda _nid: ['nid001']

    relay_file = tmp_path / 'edge-rfail.port'
    # New protocol: the child writes a .req file with its hostname; the
    # watcher reads it, then spawns ssh -R.  Simulate the child here.
    import json
    relay_file.with_suffix('.req').write_text(json.dumps({"hostname": "nid001"}))

    def fake_spawn_raises(*a, **kw):
        raise RuntimeError("ssh: connection refused (gatewayports denied)")

    cancelled: list = []

    async def fake_dispatch_cancel(job_id):
        cancelled.append(job_id)

    with patch('radical.edge.batch_system.detect_batch_system',
               return_value=fake_batch), \
         patch('radical.edge.tunnel.spawn_reverse_tunnel',
               side_effect=fake_spawn_raises), \
         patch.object(plugin, '_dispatch_cancel',
                       side_effect=fake_dispatch_cancel), \
         patch('radical.edge.plugin_psij.asyncio.sleep',
               new=AsyncMock(return_value=None)):
        await asyncio.wait_for(
            plugin._tunnel_watcher('edge-rfail', '54321',
                                    'edge-job.rfail', relay_file,
                                    'reverse'),
            timeout=5.0)

    assert 'edge-job.rfail' in plugin._failure_reasons
    assert 'connection refused' in plugin._failure_reasons['edge-job.rfail']
    assert 'edge-job.rfail' in cancelled


def test_get_job_status_overrides_cancelled_to_failed(mock_psij):
    """When a job_id is in _failure_reasons, get_job_status synthesises
    state='FAILED' + error.  Operator-cancelled jobs (no entry) keep
    their natural CANCELLED state."""
    from unittest.mock import patch as _patch
    app = FastAPI()
    plugin = PluginPSIJ(app)
    plugin._failure_reasons['edge-job.99'] = 'reverse SSH tunnel: oops'

    client = TestClient(app)
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    async def _fwd(self, *args, **kwargs):
        # Pretend PsiJ reports CANCELLED for the requested job_id.
        return {'job_id': kwargs.get('job_id'), 'state': 'CANCELLED'}

    with _patch.object(PluginPSIJ, '_forward', _fwd):
        # Failure-tagged job: state must be overridden to FAILED + error.
        resp = client.get(f"{plugin.namespace}/status/{sid}/edge-job.99")
        assert resp.status_code == 200
        data = resp.json()
        assert data['state'] == 'FAILED'
        assert 'oops' in data['error']

        # Untagged job: natural CANCELLED state preserved.
        resp = client.get(f"{plugin.namespace}/status/{sid}/edge-job.42")
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
