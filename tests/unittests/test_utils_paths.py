"""Path resolution for bridge cert/key: ``~`` must be expanded.

The shell does not expand ``~`` after ``--cert=`` / ``--key=`` (nor inside env
vars), so the resolver has to — otherwise a literal ``~/...`` reaches the
filesystem and fails with FileNotFoundError.  Tested at ``_resolve_path_value``
(the expansion point) so we don't need real cert/key material.
"""

from pathlib import Path

from radical.orbit import utils


def test_cli_tilde_path_is_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))           # ~ -> tmp_path
    path, source = utils._resolve_path_value(
        '~/.radical/orbit/bridge_cert.pem', 'UNUSED_ENV', Path('/nope'))
    assert source == 'cli'
    assert path == tmp_path / '.radical' / 'orbit' / 'bridge_cert.pem'
    assert '~' not in str(path)


def test_env_tilde_path_is_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('ORBIT_TEST_PATH', '~/bridge_key.pem')
    path, source = utils._resolve_path_value(
        None, 'ORBIT_TEST_PATH', Path('/nope'))
    assert source == 'env'
    assert path == tmp_path / 'bridge_key.pem'
