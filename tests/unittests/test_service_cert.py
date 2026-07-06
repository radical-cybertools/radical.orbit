"""TLS certificate-error policy for the endpoint service.

A hostname / IP-address mismatch is tolerated (relax name validation + warn)
*only* when an explicit certificate is pinned (``--cert``); with no pinned cert
that would be a real security downgrade, so it aborts.  Every other certificate
failure (expired, untrusted issuer, self-signed-not-pinned, …) aborts too —
reconnecting cannot recover from a bad certificate.  ``run()`` re-raises on
``'abort'``, which the entrypoint turns into a non-zero exit.
"""

from radical.orbit.service import EndpointService

_classify = EndpointService._classify_cert_error


def test_name_or_ip_mismatch_with_pinned_cert_relaxes():
    assert _classify("Hostname mismatch, certificate is not valid for 'x'",
                     cert_pinned=True, check_hostname=True) == 'relax'
    assert _classify("IP address mismatch, certificate is not valid for "
                     "'10.100.80.237'",
                     cert_pinned=True, check_hostname=True) == 'relax'


def test_name_mismatch_without_pinned_cert_aborts():
    # system trust store: disabling the name check would accept any valid
    # public cert -> abort instead of relaxing.
    assert _classify("IP address mismatch, certificate is not valid for "
                     "'10.100.80.237'",
                     cert_pinned=False, check_hostname=True) == 'abort'


def test_name_mismatch_when_check_already_disabled_aborts():
    # we already relaxed once and still failed -> don't loop, abort.
    assert _classify("IP address mismatch, certificate is not valid for 'x'",
                     cert_pinned=True, check_hostname=False) == 'abort'


def test_other_cert_failures_always_abort():
    for msg in ("certificate has expired",
                "self-signed certificate in certificate chain",
                "unable to get local issuer certificate",
                "certificate is not yet valid"):
        assert _classify(msg, cert_pinned=True, check_hostname=True) == 'abort'


def test_empty_message_aborts():
    assert _classify("",   cert_pinned=True, check_hostname=True) == 'abort'
    assert _classify(None, cert_pinned=True, check_hostname=True) == 'abort'


# ---------------------------------------------------------------------------
# _build_ssl_context — the pinned bridge cert must be the ONLY trust root
# (issue #42): the system CA store must not be loaded when a cert is pinned,
# or a bridge presenting any other system-trusted / same-host cert would be
# accepted in place of the configured one.
# ---------------------------------------------------------------------------

import ssl
import subprocess

import pytest


def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def _gen_cert(tmp_path, name, cn) -> str:
    cert = tmp_path / f'{name}.pem'
    key  = tmp_path / f'{name}.key'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', f'/CN={cn}'],
        check=True, capture_output=True,
    )
    return str(cert)


def test_build_ssl_context_pins_only_configured_cert(tmp_path):
    if not _have_openssl():
        pytest.skip("openssl not available")
    cert_a = _gen_cert(tmp_path, 'bridge_a', 'bridgeA')

    ctx = EndpointService._build_ssl_context(cert_a, check_hostname=False)

    cas = ctx.get_ca_certs()
    # Exactly one trusted CA — the pinned cert — NOT the system store.
    assert len(cas) == 1, \
        "pinned context must not also trust the system CA store (issue #42)"
    subject = dict(x[0] for x in cas[0]['subject'])
    assert subject.get('commonName') == 'bridgeA'
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False


def test_build_ssl_context_no_pinned_cert_uses_system_store():
    ctx = EndpointService._build_ssl_context(None, check_hostname=True)
    # With no pinned cert we fall back to the system CA store (many roots).
    assert len(ctx.get_ca_certs()) > 1
    assert ctx.verify_mode == ssl.CERT_REQUIRED
