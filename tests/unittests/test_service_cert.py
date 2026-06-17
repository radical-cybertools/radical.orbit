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
