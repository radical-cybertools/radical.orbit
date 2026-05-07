"""Unit tests for the SSH tunnel helper and the compute-side
EdgeService._open_tunnel flow."""

import asyncio
import io
from unittest.mock import MagicMock, patch

import pytest

from radical.edge import tunnel as _tunnel


# ---------------------------------------------------------------------------
# spawn_tunnel
# ---------------------------------------------------------------------------

class _FakeProc:
    """Mimics subprocess.Popen enough for spawn_tunnel()."""
    def __init__(self, stderr_lines=None, pid=4321, poll_result=None):
        body = b'\n'.join(stderr_lines or []) + b'\n'
        self.stderr = io.BytesIO(body)
        self.pid = pid
        self._poll = poll_result
        self.returncode = poll_result

    def poll(self):
        return self._poll


@pytest.fixture
def patch_port_and_listener(monkeypatch):
    """Patch port-pick + listener-probe so tests don't touch real sockets.

    Returns the picked port so tests can assert it shows up in argv /
    rendezvous files.
    """
    picked = 12345
    monkeypatch.setattr(_tunnel, '_pick_free_local_port', lambda: picked)
    monkeypatch.setattr(_tunnel, '_wait_for_listener',
                        lambda port, proc, timeout, lines: None)
    return picked


def test_spawn_tunnel_uses_picked_port(tmp_path, monkeypatch,
                                       patch_port_and_listener):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    port = patch_port_and_listener
    proc = _FakeProc()
    with patch('subprocess.Popen', return_value=proc) as popen:
        got_proc, got_port = _tunnel.spawn_tunnel(
            login_host='login01', bridge_host='bridge',
            bridge_port=8000, edge_name='myedge')

    assert got_port == port
    assert got_proc is proc

    argv = popen.call_args[0][0]
    assert argv[0] == 'ssh' and '-N' in argv
    assert '-L' in argv
    assert f'{port}:bridge:8000' in argv
    # No '-R' anywhere — module is compute -> login only.
    assert '-R' not in argv
    assert argv[-1] == 'login01'

    assert (tmp_path / 'myedge.port').read_text() == str(port)
    assert (tmp_path / 'myedge.pid').read_text()  == '4321'


def test_spawn_tunnel_raises_when_ssh_exits(tmp_path, monkeypatch):
    """If SSH dies before the listener comes up, the listener probe surfaces it."""
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.setattr(_tunnel, '_pick_free_local_port', lambda: 12345)
    monkeypatch.setattr(_tunnel, '_start_stderr_drain',
                        lambda proc, lines: lines.append(
                            'Bad local forwarding specification ...'))

    proc = _FakeProc(poll_result=255)
    proc.returncode = 255
    with patch('subprocess.Popen', return_value=proc):
        with pytest.raises(RuntimeError, match='exited.*before listener'):
            _tunnel.spawn_tunnel('login01', 'bridge', 8000, 'e1',
                                 listen_timeout=0.1)


def test_spawn_tunnel_raises_on_listener_timeout(tmp_path, monkeypatch):
    """Process stays alive but listener never accepts -> timeout error."""
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.setattr(_tunnel, '_pick_free_local_port', lambda: 12345)
    monkeypatch.setattr(_tunnel, '_start_stderr_drain',
                        lambda proc, lines: None)

    proc = _FakeProc(poll_result=None)   # alive throughout
    with patch('subprocess.Popen', return_value=proc):
        with pytest.raises(RuntimeError, match='did not come up within'):
            _tunnel.spawn_tunnel('login01', 'bridge', 8000, 'e2',
                                 listen_timeout=0.1)


def test_cleanup_tunnel_terminates():
    proc = MagicMock()
    proc.wait.return_value = 0
    _tunnel.cleanup_tunnel(proc, 'e1')
    proc.terminate.assert_called_once()


def test_cleanup_tunnel_handles_none():
    # No-op when proc is None (used when --tunnel never activated).
    _tunnel.cleanup_tunnel(None)


def test_cleanup_tunnel_falls_back_to_kill():
    proc = MagicMock()
    proc.terminate.side_effect = OSError
    _tunnel.cleanup_tunnel(proc, 'e2')
    proc.kill.assert_called_once()


# ---------------------------------------------------------------------------
# EdgeService._open_tunnel
# ---------------------------------------------------------------------------

def _make_edge_service(bridge_url='http://bridge:8000', tunnel_via=None):
    """Build an EdgeService without going through the plugin loader.

    Default URL uses ``http://`` (not ``https://``) so the cert
    resolution path is skipped — these tests don't exercise TLS.
    """
    from radical.edge.service import EdgeService
    with patch('radical.edge.service.EdgeService._load_plugins_from_filter'):
        svc = EdgeService(bridge_url=bridge_url, name='edge1',
                          tunnel='forward', tunnel_via=tunnel_via)
    return svc


def test_open_tunnel_uses_explicit_via(tmp_path, monkeypatch,
                                       patch_port_and_listener):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    for k in ('PBS_O_HOST', 'SLURM_SUBMIT_HOST'):
        monkeypatch.delenv(k, raising=False)
    port = patch_port_and_listener
    svc = _make_edge_service(tunnel_via='login42')

    proc = _FakeProc()
    with patch('subprocess.Popen', return_value=proc):
        asyncio.run(svc._open_tunnel_forward())

    assert f'localhost:{port}' in svc._bridge_url
    assert svc._tunnel_proc is proc


def test_open_tunnel_falls_back_to_pbs_o_host(tmp_path, monkeypatch,
                                              patch_port_and_listener):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.setenv('PBS_O_HOST', 'aurora-uan-0010')
    monkeypatch.delenv('SLURM_SUBMIT_HOST', raising=False)
    port = patch_port_and_listener
    svc = _make_edge_service()

    proc = _FakeProc()
    with patch('subprocess.Popen', return_value=proc) as popen:
        asyncio.run(svc._open_tunnel_forward())
    argv = popen.call_args[0][0]
    assert argv[-1] == 'aurora-uan-0010'
    assert f'localhost:{port}' in svc._bridge_url


def test_open_tunnel_falls_back_to_slurm_submit_host(tmp_path, monkeypatch,
                                                     patch_port_and_listener):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    monkeypatch.delenv('PBS_O_HOST', raising=False)
    monkeypatch.setenv('SLURM_SUBMIT_HOST', 'login3')
    svc = _make_edge_service()

    proc = _FakeProc()
    with patch('subprocess.Popen', return_value=proc) as popen:
        asyncio.run(svc._open_tunnel_forward())
    argv = popen.call_args[0][0]
    assert argv[-1] == 'login3'


def test_open_tunnel_raises_without_login_host(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    for k in ('PBS_O_HOST', 'SLURM_SUBMIT_HOST'):
        monkeypatch.delenv(k, raising=False)
    svc = _make_edge_service()

    with pytest.raises(RuntimeError, match='no login host'):
        asyncio.run(svc._open_tunnel_forward())


def test_stop_terminates_tunnel_process(tmp_path, monkeypatch):
    monkeypatch.setattr(_tunnel, 'RELAY_BASE', tmp_path)
    svc = _make_edge_service(tunnel_via='login42')
    fake_proc = MagicMock()
    fake_proc.wait.return_value = 0
    svc._tunnel_proc = fake_proc
    svc.stop()
    fake_proc.terminate.assert_called_once()
    assert svc._tunnel_proc is None
