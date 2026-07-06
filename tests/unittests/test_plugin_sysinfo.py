
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
import psutil
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_sysinfo import PluginSysInfo, SysInfoProvider


def test_plugin_sysinfo_init():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    assert plugin.instance_name == 'sysinfo'
    assert plugin.namespace == '/sysinfo'

    # Check direct-dispatch routes
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    ns = plugin.namespace.lstrip('/')
    assert any(f'{ns}/metrics/' in p for p in route_pats)


def test_sysinfo_provider_basic():
    provider = SysInfoProvider()
    metrics = provider.get_metrics()

    # Check structure
    assert 'system' in metrics
    assert 'cpu' in metrics
    assert 'memory' in metrics
    assert 'disks' in metrics
    assert 'network' in metrics
    assert 'gpus' in metrics

    # Check content via keys
    sys = metrics['system']
    assert 'hostname' in sys
    assert 'user'     in sys
    assert 'uptime'   in sys
    assert isinstance(sys['user'], str)

    cpu = metrics['cpu']
    assert 'model' in cpu
    assert 'percent' in cpu

    if metrics['disks']:
        assert 'mount' in metrics['disks'][0]
        assert 'type' in metrics['disks'][0]

    # Type checks
    assert isinstance(metrics['cpu']['cores_physical'], int)
    assert isinstance(metrics['memory']['total'], int)


def test_sysinfo_gpus_structure():
    """Test that GPU metrics have expected structure."""
    provider = SysInfoProvider()
    metrics = provider.get_metrics()

    # GPUs might be empty or populated depending on system
    assert 'gpus' in metrics
    assert isinstance(metrics['gpus'], list)

    # If NVIDIA gpus are present (test system dependent), check structure
    for gpu in metrics['gpus']:
        assert 'vendor' in gpu
        assert 'name' in gpu
        # Dynamic metrics might not be present if nvidia-smi fails
        # So we only check static fields


def test_gpu_metric_subprocesses_pass_timeout(monkeypatch):
    """The NVIDIA and AMD metric collectors must bound their subprocess calls
    with a timeout (parity with the sibling static-detection calls)."""
    import radical.orbit.plugin_sysinfo as mod

    seen = []

    def _fake_check_output(cmd, **kwargs):
        seen.append((cmd[0], kwargs.get('timeout')))
        if cmd[0] == 'nvidia-smi':
            return '0, 10, 5, 8000, 2000, 6000\n'
        return '{"card0": {}}'                          # rocm-smi json

    monkeypatch.setattr(mod.subprocess, 'check_output', _fake_check_output)

    provider = SysInfoProvider()
    provider._get_gpu_metrics([
        {'vendor': 'NVIDIA', 'index': 0, 'id': '0'},
        {'vendor': 'AMD',    'index': 0, 'id': 'card0'},
    ])

    tools = dict(seen)
    assert tools.get('nvidia-smi') == 5
    assert tools.get('rocm-smi')   == 5


def test_sysinfo_disk_usage_unreadable_mountpoint_is_skipped(monkeypatch):
    """A mountpoint whose disk_usage() raises OSError -- unreadable
    (PermissionError) or vanished between disk_partitions() and disk_usage()
    (FileNotFoundError) -- is skipped, not fatal; good mounts still appear."""
    provider = SysInfoProvider()
    # Pre-populate the CPU/GPU caches so get_metrics() does not spawn
    # lscpu / nvidia-smi / rocm-smi subprocesses during the unit test.
    provider._cpu_model = 'TestCPU'
    provider._gpu_info  = []

    _P = lambda mp, dev: type('_Part', (), {
        'mountpoint': mp, 'device': dev, 'fstype': 'nfs', 'opts': 'rw'})()
    good, gone = _P('/data', 'srv:/data'), _P('/vanished', 'srv:/gone')
    monkeypatch.setattr(psutil, 'disk_partitions', lambda all=False: [good, gone])

    def _usage(mp):
        if mp == '/vanished':
            raise FileNotFoundError(2, 'No such file or directory', mp)
        return psutil._common.sdiskusage(total=100, used=40, free=60, percent=40.0)
    monkeypatch.setattr(psutil, 'disk_usage', _usage)

    metrics = provider.get_metrics()   # must not raise
    mounts  = [d['mount'] for d in metrics['disks']]
    assert '/data'     in mounts
    assert '/vanished' not in mounts


@pytest.mark.asyncio
async def test_endpoint():
    app = FastAPI()
    plugin = PluginSysInfo(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Get metrics
    resp = client.get(f"{plugin.namespace}/metrics/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert 'system' in data
    assert 'cpu' in data


# ---------------------------------------------------------------------------
# host_role endpoint — exercises broker / login / compute classification.
#
# Detection routes through batch_system.detect_batch_system(), which probes
# the local PATH for ``squeue`` (SLURM) and ``qstat`` (PBS).  We patch
# ``shutil.which`` to pin the backend per test, and reset the module-level
# cache via the autouse fixture below.
# ---------------------------------------------------------------------------

import re

from unittest.mock import patch
from radical.orbit.batch_system import reset_detection


_PY_VERSION_RE = re.compile(r'^\d+\.\d+\.\d+$')


@pytest.fixture(autouse=True)
def _reset_batch_system_cache():
    """Each host_role test sees a freshly-probed batch system."""
    reset_detection()
    yield
    reset_detection()


def _host_role(app: FastAPI) -> dict:
    """Hit the host_role route, validate the python_version shape, then
    pop that field so the remaining keys can be compared by exact equality
    in the test bodies."""
    plugin = PluginSysInfo(app)
    client = TestClient(app)
    resp   = client.get(f"{plugin.namespace}/host_role")
    assert resp.status_code == 200
    body = resp.json()
    assert _PY_VERSION_RE.match(body.pop('python_version')), \
        f"python_version not in major.minor.micro form: {body!r}"
    return body


def test_host_role_standalone(monkeypatch):
    """No scheduler installed and not a broker -> standalone (non-HPC host)."""
    for v in ('SLURM_JOB_ID', 'PBS_JOBID'):
        monkeypatch.delenv(v, raising=False)
    with patch('shutil.which', return_value=None):
        role = _host_role(FastAPI())
    assert role == {'role': 'standalone', 'scheduler': 'none',
                    'psij_executor': 'local', 'job_id': None}


def test_host_role_login_slurm_no_alloc(monkeypatch):
    """SLURM installed but no active job -> login role, scheduler reported."""
    monkeypatch.delenv('SLURM_JOB_ID', raising=False)
    def _which(cmd):
        return '/usr/bin/squeue' if cmd == 'squeue' else None
    with patch('shutil.which', side_effect=_which):
        role = _host_role(FastAPI())
    assert role == {'role': 'login', 'scheduler': 'slurm',
                    'psij_executor': 'slurm', 'job_id': None}


def test_host_role_broker():
    """When app.state.is_broker is True, role is 'broker'."""
    app = FastAPI()
    app.state.is_broker = True
    with patch('shutil.which', return_value=None):
        role = _host_role(app)
    assert role['role'] == 'broker'


def test_host_role_compute_slurm(monkeypatch):
    """SLURM installed + SLURM_JOB_ID set -> compute role."""
    monkeypatch.setenv('SLURM_JOB_ID', '12345')
    def _which(cmd):
        return '/usr/bin/squeue' if cmd == 'squeue' else None
    with patch('shutil.which', side_effect=_which):
        role = _host_role(FastAPI())
    assert role == {'role': 'compute', 'scheduler': 'slurm',
                    'psij_executor': 'slurm', 'job_id': '12345'}


def test_host_role_compute_pbs(monkeypatch):
    """PBS installed (no SLURM) + PBS_JOBID set -> compute role."""
    monkeypatch.delenv('SLURM_JOB_ID', raising=False)
    monkeypatch.setenv('PBS_JOBID', '7890.frontier')
    def _which(cmd):
        return '/usr/bin/qstat' if cmd == 'qstat' else None
    # Pin the Aurora marker absent so the generic PBS backend is selected.
    with patch('shutil.which', side_effect=_which), \
         patch('radical.orbit.batch_system_pbs.os.path.isdir',
               return_value=False):
        role = _host_role(FastAPI())
    assert role == {'role': 'compute', 'scheduler': 'pbs',
                    'psij_executor': 'pbs', 'job_id': '7890.frontier'}


def test_host_role_compute_pbs_aurora(monkeypatch):
    """Aurora's PBS subclass: scheduler='pbs-aurora' but psij_executor='pbs'."""
    monkeypatch.delenv('SLURM_JOB_ID', raising=False)
    monkeypatch.setenv('PBS_JOBID', '7890.aurora')
    def _which(cmd):
        return '/usr/bin/qstat' if cmd == 'qstat' else None
    # Aurora marker present -> AuroraPBSBatchSystem wins.
    with patch('shutil.which', side_effect=_which), \
         patch('radical.orbit.batch_system_pbs.os.path.isdir',
               return_value=True):
        role = _host_role(FastAPI())
    assert role == {'role': 'compute', 'scheduler': 'pbs-aurora',
                    'psij_executor': 'pbs', 'job_id': '7890.aurora'}


def test_host_role_client():
    """SysInfoClient.host_role() returns the same shape as the route."""
    from radical.orbit.plugin_sysinfo import SysInfoClient
    app = FastAPI()
    plugin = PluginSysInfo(app)
    http   = TestClient(app)
    client = SysInfoClient(http, plugin.namespace)
    with patch('shutil.which', return_value=None):
        role = client.host_role()
    assert role['role']          == 'standalone'
    assert role['scheduler']     == 'none'
    assert role['psij_executor'] == 'local'
    assert role['job_id']        is None
    assert _PY_VERSION_RE.match(role['python_version']), \
        f"python_version not in major.minor.micro form: {role!r}"
