"""The defaults under ~/.radical/orbit resolve per call from the current
HOME (issue #162 follow-up), with module attributes as override hooks."""

from fastapi import FastAPI

from radical.orbit import utils, tunnel


def test_orbit_dir_follows_home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    base = tmp_path / '.radical' / 'orbit'
    assert utils.orbit_dir()  == base
    assert utils.cert_file()  == base / 'broker_cert.pem'
    assert utils.key_file()   == base / 'broker_key.pem'
    assert utils.token_file() == base / 'broker.token'
    assert tunnel.relay_base() == base / 'tunnels'


def test_overrides_take_precedence(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    monkeypatch.setattr(utils, 'DEFAULT_DIR', tmp_path / 'conf')
    monkeypatch.setattr(utils, 'CERT_FILE',   tmp_path / 'my.pem')
    assert utils.cert_file()  == tmp_path / 'my.pem'          # per file
    assert utils.token_file() == tmp_path / 'conf' / 'broker.token'
    monkeypatch.setattr(tunnel, 'RELAY_BASE', tmp_path / 'relay')
    assert tunnel.relay_dir() == tmp_path / 'relay'
    assert (tmp_path / 'relay').is_dir()


def test_plugin_defaults_follow_home(tmp_path, monkeypatch):
    from radical.orbit.plugin_staging import StagingSession
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher
    import os
    monkeypatch.setenv('HOME', str(tmp_path))
    session = StagingSession('s1')
    assert os.path.realpath(tmp_path) in session._allowed_bases
    plugin = PluginTaskDispatcher(FastAPI())
    assert plugin._state_root == \
        tmp_path / '.radical' / 'orbit' / 'task_dispatcher' / 'state'
