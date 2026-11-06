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
    """Every home-derived plugin default resolves from the HOME at the time
    of use (not at import)."""
    import asyncio
    import os
    from radical.orbit.plugin_staging import StagingSession
    from radical.orbit.plugin_task_dispatcher import PluginTaskDispatcher
    from radical.orbit.plugin_federation import PluginFederation
    from radical.orbit import plugin_psij, plugin_globus
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.delenv('RADICAL_ORBIT_PSIJ_DIR', raising=False)
    base = tmp_path / '.radical' / 'orbit'

    assert os.path.realpath(tmp_path) in StagingSession('s1')._allowed_bases
    assert plugin_psij._psij_dir() == base / 'psij'

    td = PluginTaskDispatcher(FastAPI())
    assert td._state_root   == base / 'task_dispatcher' / 'state'
    assert td._scratch_root == base / 'task_dispatcher' / 'scratch'
    asyncio.run(td.shutdown())

    monkeypatch.delenv('RADICAL_ORBIT_FEDERATION_STATE', raising=False)
    fed = PluginFederation(FastAPI())
    assert fed._root == base / 'federation'

    (base / 'globus.json').parent.mkdir(parents=True, exist_ok=True)
    (base / 'globus.json').write_text('{"local_collection": "lc-uuid"}')
    monkeypatch.setattr(plugin_globus, 'globus_sdk', None)   # no GCP probe
    monkeypatch.delenv('RADICAL_ORBIT_GLOBUS_COLLECTION', raising=False)
    assert plugin_globus.detect_local_collection() == 'lc-uuid'
