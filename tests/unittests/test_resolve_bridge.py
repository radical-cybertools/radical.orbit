# pylint: disable=protected-access
"""Unit tests for the bridge config resolver in ``radical.edge.utils``.

The resolver is purely deterministic given (CLI arg, env, filesystem)
state, so each test sets exactly that triple and asserts the outcome.
``DEFAULT_DIR`` (and the file-path constants derived from it) are
monkey-patched to point at ``tmp_path`` in each test so the suite never
touches the developer's real ``~/.radical/edge/``.
"""

import os
import ssl
import subprocess

import pytest

from radical.edge import utils


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def isolated_dir(tmp_path, monkeypatch):
    """Redirect the resolver's file paths into a tmp dir + clear env vars.

    Yields the tmp directory so tests can drop files into it directly.
    """
    monkeypatch.setattr(utils, 'DEFAULT_DIR', tmp_path)
    monkeypatch.setattr(utils, 'URL_FILE',  tmp_path / 'bridge.url')
    monkeypatch.setattr(utils, 'CERT_FILE', tmp_path / 'bridge_cert.pem')
    monkeypatch.setattr(utils, 'KEY_FILE',  tmp_path / 'bridge_key.pem')
    for v in (utils.ENV_URL, utils.ENV_CERT, utils.ENV_KEY):
        monkeypatch.delenv(v, raising=False)
    yield tmp_path


@pytest.fixture
def self_signed(tmp_path):
    """Generate a throw-away self-signed cert+key in *tmp_path*.

    Returns ``(cert_path, key_path)``.  Skipped if openssl is not
    available (no other generation path is set up).
    """
    if not _have_openssl():
        pytest.skip("openssl not available")

    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True,
    )
    os.chmod(key, 0o600)
    return cert, key


def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------

def test_url_cli_wins_over_env_and_file(isolated_dir, monkeypatch):
    """CLI > env > file when all three are set."""
    monkeypatch.setenv(utils.ENV_URL, 'https://from-env:8000')
    (isolated_dir / 'bridge.url').write_text('https://from-file:8000\n')
    url, src = utils.resolve_bridge_url(cli='https://from-cli:8000')
    assert url == 'https://from-cli:8000'
    assert src == 'cli'


def test_url_env_wins_over_file(isolated_dir, monkeypatch):
    """Env > file when CLI is absent."""
    monkeypatch.setenv(utils.ENV_URL, 'https://from-env:8000')
    (isolated_dir / 'bridge.url').write_text('https://from-file:8000\n')
    url, src = utils.resolve_bridge_url()
    assert url == 'https://from-env:8000'
    assert src == 'env'


def test_url_file_used_when_no_cli_no_env(isolated_dir):
    """File is the lowest precedence successful source."""
    (isolated_dir / 'bridge.url').write_text('https://from-file:8000\n')
    url, src = utils.resolve_bridge_url()
    assert url == 'https://from-file:8000'
    assert src == 'file'


def test_url_trailing_slash_stripped(isolated_dir, monkeypatch):
    """Resolver strips a single trailing slash for consistency."""
    monkeypatch.setenv(utils.ENV_URL, 'https://x:8000/')
    url, _ = utils.resolve_bridge_url()
    assert url == 'https://x:8000'


def test_url_errors_when_unconfigured(isolated_dir):
    """Nothing set anywhere → ValueError."""
    with pytest.raises(ValueError, match='Bridge URL required'):
        utils.resolve_bridge_url()


# ---------------------------------------------------------------------------
# URL file write (atomic)
# ---------------------------------------------------------------------------

def test_write_bridge_url_file_creates_parents(tmp_path):
    """``write_bridge_url_file`` mkdirs parent dirs and writes the file."""
    target = tmp_path / 'fresh' / 'bridge.url'
    utils.write_bridge_url_file('https://here:8000', path=target)
    assert target.read_text().strip() == 'https://here:8000'
    assert target.parent.is_dir()


def test_write_bridge_url_file_overwrites(tmp_path):
    """Subsequent writes replace the file content (atomic via os.replace)."""
    target = tmp_path / 'bridge.url'
    utils.write_bridge_url_file('https://first:8000', path=target)
    utils.write_bridge_url_file('https://second:8000', path=target)
    assert target.read_text().strip() == 'https://second:8000'


# ---------------------------------------------------------------------------
# public_url_forms
# ---------------------------------------------------------------------------

def test_public_url_forms_wildcard_returns_at_least_one():
    """Wildcard bind: at least one form, fallback to hostname/localhost."""
    forms = utils.public_url_forms('0.0.0.0', 8000)
    assert forms
    for f in forms:
        assert f.startswith('https://')
        assert f.endswith(':8000')


def test_public_url_forms_scheme_arg():
    """Custom scheme is honoured in every returned URL."""
    forms = utils.public_url_forms('0.0.0.0', 80, scheme='http')
    assert all(f.startswith('http://') for f in forms)


def test_public_url_forms_specific_host_uses_literal():
    """Non-wildcard host is advertised literally — single form, no FQDN."""
    forms = utils.public_url_forms('127.0.0.1', 8000)
    assert forms == ['https://127.0.0.1:8000']


def test_public_url_forms_specific_hostname_uses_literal():
    """Hostname is also literal — no FQDN substitution."""
    forms = utils.public_url_forms('my-bridge', 8000)
    assert forms == ['https://my-bridge:8000']


def test_public_url_forms_ipv6_bracket_wrapped():
    """IPv6 literal hosts get bracket-wrapped per RFC 3986."""
    forms = utils.public_url_forms('::1', 8000)
    assert forms == ['https://[::1]:8000']


# ---------------------------------------------------------------------------
# Cert resolution
# ---------------------------------------------------------------------------

def test_cert_cli_wins(isolated_dir, self_signed, monkeypatch):
    """CLI cert path is taken even when env/file are also set."""
    cert, _ = self_signed
    other = isolated_dir / 'other_cert.pem'
    other.write_bytes(cert.read_bytes())
    monkeypatch.setenv(utils.ENV_CERT, str(other))
    (isolated_dir / 'bridge_cert.pem').write_bytes(cert.read_bytes())

    path, src = utils.resolve_bridge_cert(cli=str(cert))
    assert path == cert
    assert src == 'cli'


def test_cert_env_wins_over_file(isolated_dir, self_signed, monkeypatch):
    """Env > file when CLI is absent."""
    cert, _ = self_signed
    monkeypatch.setenv(utils.ENV_CERT, str(cert))
    other = isolated_dir / 'bridge_cert.pem'
    other.write_bytes(cert.read_bytes())   # different file, same content

    path, src = utils.resolve_bridge_cert()
    assert path == cert
    assert src == 'env'


def test_cert_file_fallback(isolated_dir, self_signed):
    """File at ``DEFAULT_DIR/bridge_cert.pem`` used when no env / no CLI."""
    cert, _ = self_signed
    target = isolated_dir / 'bridge_cert.pem'
    target.write_bytes(cert.read_bytes())

    path, src = utils.resolve_bridge_cert()
    assert path == target
    assert src == 'file'


def test_cert_missing_everywhere_raises(isolated_dir):
    """Nothing configured → ValueError."""
    with pytest.raises(ValueError, match='TLS cert required'):
        utils.resolve_bridge_cert()


def test_cert_path_set_but_file_missing(isolated_dir, monkeypatch):
    """Env-pointed cert that doesn't exist → FileNotFoundError."""
    monkeypatch.setenv(utils.ENV_CERT, '/nonexistent/cert.pem')
    with pytest.raises(FileNotFoundError):
        utils.resolve_bridge_cert()


def test_cert_invalid_content_raises(isolated_dir, monkeypatch):
    """A file that exists but isn't a valid cert → ssl.SSLError."""
    bad = isolated_dir / 'not_a_cert.pem'
    bad.write_text('this is not a TLS certificate')
    monkeypatch.setenv(utils.ENV_CERT, str(bad))
    with pytest.raises(ssl.SSLError):
        utils.resolve_bridge_cert()


# ---------------------------------------------------------------------------
# Key resolution (bridge-only, mode 0o600 enforced)
# ---------------------------------------------------------------------------

def test_key_resolves_when_strict_mode(isolated_dir, self_signed, monkeypatch):
    """A 0o600 key resolves cleanly."""
    cert, key = self_signed
    monkeypatch.setenv(utils.ENV_KEY, str(key))
    path, src = utils.resolve_bridge_key()
    assert path == key
    assert src == 'env'


def test_key_with_cert_validates_pair(isolated_dir, self_signed, monkeypatch):
    """Passing *cert* validates the cert/key pair (load_cert_chain)."""
    cert, key = self_signed
    monkeypatch.setenv(utils.ENV_KEY, str(key))
    path, _ = utils.resolve_bridge_key(cert=cert)
    assert path == key


def test_key_refuses_world_readable(isolated_dir, self_signed, monkeypatch):
    """Mode 0o644 must trigger the refusal (PermissionError)."""
    _, key = self_signed
    os.chmod(key, 0o644)
    monkeypatch.setenv(utils.ENV_KEY, str(key))
    with pytest.raises(PermissionError, match='too permissive'):
        utils.resolve_bridge_key()


def test_key_refuses_group_readable(isolated_dir, self_signed, monkeypatch):
    """Even mode 0o640 (group-readable) is refused."""
    _, key = self_signed
    os.chmod(key, 0o640)
    monkeypatch.setenv(utils.ENV_KEY, str(key))
    with pytest.raises(PermissionError, match='too permissive'):
        utils.resolve_bridge_key()


def test_key_missing_everywhere_raises(isolated_dir):
    """Nothing configured → ValueError."""
    with pytest.raises(ValueError, match='TLS key required'):
        utils.resolve_bridge_key()


def test_key_path_set_but_file_missing(isolated_dir, monkeypatch):
    """Env-pointed key that doesn't exist → FileNotFoundError."""
    monkeypatch.setenv(utils.ENV_KEY, '/nonexistent/key.pem')
    with pytest.raises(FileNotFoundError):
        utils.resolve_bridge_key()


def test_key_owner_readable_only_passes(isolated_dir, self_signed, monkeypatch):
    """Mode 0o400 (owner-read-only) is acceptable."""
    _, key = self_signed
    os.chmod(key, 0o400)
    monkeypatch.setenv(utils.ENV_KEY, str(key))
    path, _ = utils.resolve_bridge_key()
    assert path == key
    # Restore so later teardown can unlink.
    os.chmod(key, 0o600)
