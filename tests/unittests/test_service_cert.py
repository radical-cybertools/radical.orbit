"""TLS SSL-context construction for the endpoint runtime's transport.

The old string-classifier policy (``EndpointRuntime._classify_cert_error``,
which parsed OpenSSL error text at reconnect time to decide whether to relax
hostname checking) was removed. The runtime now decides hostname verification
**up front**, from :meth:`EndpointRuntime._ssl_context`: a **pinned** cert
(``--cert`` / resolved via ``utils.resolve_broker_cert``) already authenticates
the peer via ``CERT_REQUIRED``, so hostname matching (which a self-signed dev
cert commonly fails) is redundant and disabled. The pinned cert is the **sole**
trust root — the system store is not also loaded (#42) — and a
configured-but-missing cert **fails closed** rather than downgrading to system
trust. Without a pinned cert the system trust store is used with full hostname
verification. These tests exercise that decision directly. Cert *resolution*
(CLI > env > file) is already covered by ``test_resolve_broker.py``.
"""

import ssl
import subprocess

import pytest

from radical.orbit.runtime import EndpointRuntime


def _have_openssl():
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def self_signed(tmp_path):
    """Generate a throw-away self-signed cert+key in *tmp_path*."""
    if not _have_openssl():
        pytest.skip("openssl not available")

    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    return cert, key


def _rt(cert=None):
    """A bare EndpointRuntime with no network I/O (http scheme, explicit token
    so no on-disk resolution is touched)."""
    rt = EndpointRuntime(broker_url='http://localhost:1', token='x')
    rt._cert = cert
    return rt


def test_no_ssl_context_for_non_wss_url():
    rt = _rt()
    assert rt._ssl_context('ws://localhost:1/register')   is None
    assert rt._ssl_context('http://localhost:1/register') is None


def test_pinned_cert_disables_hostname_check(self_signed):
    cert, _key = self_signed
    rt  = _rt(cert=str(cert))
    ctx = rt._ssl_context('wss://localhost:1/register')
    assert ctx.verify_mode    == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False
    # Pin-only: the configured cert is the ONLY trust root, NOT the system CA
    # store (#42) -- else a broker presenting any other system-trusted cert
    # would be accepted in its place.
    assert len(ctx.get_ca_certs()) == 1


def test_no_pinned_cert_keeps_hostname_check():
    rt  = _rt(cert=None)
    ctx = rt._ssl_context('wss://localhost:1/register')
    assert ctx.verify_mode    == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    # No pinned cert -> fall back to the system CA store (many roots).
    assert len(ctx.get_ca_certs()) > 1


def test_missing_cert_file_fails_closed(tmp_path):
    # A cert path is set but the file doesn't exist on disk -> fail closed
    # (raise) rather than silently downgrading to the system CA store
    # (#42 / review feedback on PR #80).
    rt = _rt(cert=str(tmp_path / 'nope.pem'))
    with pytest.raises(FileNotFoundError):
        rt._ssl_context('wss://localhost:1/register')
