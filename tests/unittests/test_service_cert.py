"""TLS SSL-context construction for the endpoint runtime's transport.

The old string-classifier policy (``EndpointRuntime._classify_cert_error``,
which parsed OpenSSL error text at reconnect time to decide whether to relax
hostname checking) was removed. The runtime now decides hostname verification
**up front**, from :meth:`EndpointRuntime._ssl_context`: a **pinned** cert
(``--cert`` / resolved via ``utils.resolve_broker_cert``) already authenticates
the peer via ``CERT_REQUIRED``, so hostname matching (which a self-signed dev
cert commonly fails) is redundant and disabled; without a pinned cert the
system trust store is used with full hostname verification. These tests
exercise that decision directly. Cert *resolution* (CLI > env > file) is
already covered by ``test_resolve_broker.py``.
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


def test_no_pinned_cert_keeps_hostname_check():
    rt  = _rt(cert=None)
    ctx = rt._ssl_context('wss://localhost:1/register')
    assert ctx.verify_mode    == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_missing_cert_file_keeps_hostname_check(tmp_path):
    # A cert path is set but the file doesn't exist on disk -> treated the
    # same as "no pinned cert": full hostname verification stays on.
    rt  = _rt(cert=str(tmp_path / 'nope.pem'))
    ctx = rt._ssl_context('wss://localhost:1/register')
    assert ctx.check_hostname is True
