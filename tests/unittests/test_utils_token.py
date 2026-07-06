"""Unit tests for the broker ingress auth-token helpers in radical.orbit.utils."""

import pytest

from radical.orbit import utils


@pytest.fixture(autouse=True)
def _isolate_token(tmp_path, monkeypatch):
    """Redirect the token file and clear the env so tests are hermetic."""
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    # Redirect the pre-rename predecessor file into the tmp dir too so a
    # developer's real ``~/.radical/orbit/bridge.token`` cannot leak in.
    monkeypatch.setattr(utils, 'TOKEN_FILE_LEGACY', tmp_path / 'bridge.token')
    monkeypatch.delenv(utils.ENV_TOKEN,          raising=False)
    monkeypatch.delenv(utils.ENV_NO_AUTH,        raising=False)
    monkeypatch.delenv(utils.ENV_TOKEN_LEGACY,   raising=False)
    monkeypatch.delenv(utils.ENV_NO_AUTH_LEGACY, raising=False)
    return tmp_path


def test_resolve_precedence(monkeypatch):
    # nothing configured
    assert utils.resolve_broker_token() == (None, '')

    # file
    utils.write_broker_token_file('filetok')
    assert utils.resolve_broker_token() == ('filetok', 'file')

    # env beats file
    monkeypatch.setenv(utils.ENV_TOKEN, 'envtok')
    assert utils.resolve_broker_token() == ('envtok', 'env')

    # cli beats env
    assert utils.resolve_broker_token(cli='clitok') == ('clitok', 'cli')


def test_write_token_file_is_0600(_isolate_token):
    utils.write_broker_token_file('x')
    mode = utils.TOKEN_FILE.stat().st_mode & 0o777
    assert mode == 0o600


def test_ensure_generates_then_reads(_isolate_token):
    tok, src = utils.ensure_broker_token()
    assert src == 'generated'
    assert tok
    assert utils.TOKEN_FILE.read_text().strip() == tok

    # second call picks the written file up
    tok2, src2 = utils.ensure_broker_token()
    assert tok2 == tok
    assert src2 == 'file'


def test_auth_disabled(monkeypatch):
    assert utils.auth_disabled() is False
    assert utils.auth_disabled(cli_no_auth=True) is True
    monkeypatch.setenv(utils.ENV_NO_AUTH, '1')
    assert utils.auth_disabled() is True
    monkeypatch.setenv(utils.ENV_NO_AUTH, 'no')
    assert utils.auth_disabled() is False


def test_tokens_match():
    assert utils.tokens_match('abc', 'abc') is True
    assert utils.tokens_match('abc', 'xyz') is False
    assert utils.tokens_match(None,  'abc') is False
    assert utils.tokens_match('abc', None)  is False
    assert utils.tokens_match('',    '')    is False
    # Non-string input (e.g. an int/bool token in a JSON /register payload)
    # must return False, not raise TypeError in hmac.compare_digest.
    assert utils.tokens_match(123,   'abc') is False
    assert utils.tokens_match('abc', 123)   is False
    assert utils.tokens_match(True,  'abc') is False


def test_legacy_env_fallback(_isolate_token, monkeypatch):
    # The broker-named var is unset; the pre-rename predecessor still resolves.
    monkeypatch.setenv(utils.ENV_TOKEN_LEGACY, 'legacytok')
    assert utils.resolve_broker_token() == ('legacytok', 'env')
    # The broker-named var takes precedence when both are set.
    monkeypatch.setenv(utils.ENV_TOKEN, 'newtok')
    assert utils.resolve_broker_token() == ('newtok', 'env')


def test_legacy_file_fallback(_isolate_token):
    # Only the predecessor file exists — it is read as a fallback.
    utils.write_broker_token_file('legacyfiletok', path=utils.TOKEN_FILE_LEGACY)
    assert utils.resolve_broker_token() == ('legacyfiletok', 'file')
    # ``ensure`` picks up the predecessor file rather than regenerating, and
    # never writes a bridge-named file.
    tok, src = utils.ensure_broker_token()
    assert (tok, src) == ('legacyfiletok', 'file')
    assert not utils.TOKEN_FILE.exists()


def test_legacy_no_auth_fallback(_isolate_token, monkeypatch):
    monkeypatch.setenv(utils.ENV_NO_AUTH_LEGACY, '1')
    assert utils.auth_disabled() is True
