"""Unit tests for the bridge ingress auth-token helpers in radical.orbit.utils."""

import pytest

from radical.orbit import utils


@pytest.fixture(autouse=True)
def _isolate_token(tmp_path, monkeypatch):
    """Redirect the token file and clear the env so tests are hermetic."""
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'bridge.token')
    monkeypatch.delenv(utils.ENV_TOKEN,   raising=False)
    monkeypatch.delenv(utils.ENV_NO_AUTH, raising=False)
    return tmp_path


def test_resolve_precedence(monkeypatch):
    # nothing configured
    assert utils.resolve_bridge_token() == (None, '')

    # file
    utils.write_bridge_token_file('filetok')
    assert utils.resolve_bridge_token() == ('filetok', 'file')

    # env beats file
    monkeypatch.setenv(utils.ENV_TOKEN, 'envtok')
    assert utils.resolve_bridge_token() == ('envtok', 'env')

    # cli beats env
    assert utils.resolve_bridge_token(cli='clitok') == ('clitok', 'cli')


def test_write_token_file_is_0600(_isolate_token):
    utils.write_bridge_token_file('x')
    mode = utils.TOKEN_FILE.stat().st_mode & 0o777
    assert mode == 0o600


def test_ensure_generates_then_reads(_isolate_token):
    tok, src = utils.ensure_bridge_token()
    assert src == 'generated'
    assert tok
    assert utils.TOKEN_FILE.read_text().strip() == tok

    # second call picks the written file up
    tok2, src2 = utils.ensure_bridge_token()
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
