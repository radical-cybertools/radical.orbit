
# pylint: disable=protected-access
"""Endpoint-wait / job-failure logic from the amsc demo (issue #82).

`examples/amsc.py` is a demo script, not a package module, so it is loaded by
path and the whole suite skips gracefully if its (optional) imports are absent.
"""

import os
import importlib.util

import pytest


_AMSC = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'examples', 'amsc.py'))


def _load_amsc():
    spec = importlib.util.spec_from_file_location('amsc_demo', _AMSC)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


try:
    amsc = _load_amsc()
    _SKIP = None
except Exception as e:                       # missing optional demo deps
    amsc  = None
    _SKIP = f'examples/amsc.py not importable: {e}'

pytestmark = pytest.mark.skipif(amsc is None, reason=_SKIP or '')


class _FakeBC:
    """Minimal stand-in for EndpointRuntime: a controllable topology and the
    register/unregister callback surface _JobFailureWatch touches."""

    def __init__(self, topo=None):
        self._topo = topo or {}
        self.subs  = []

    def topology(self):
        return dict(self._topo)

    def register_callback(self, topic=None, callback=None, **kw):
        self.subs.append((topic, callback))

    def unregister_callback(self, topic=None, callback=None, **kw):
        self.subs = [(t, c) for (t, c) in self.subs if c is not callback]


# ── _endpoint_argv (the --tunnel argv root-cause fix) ──────────────────────

def test_endpoint_argv_forward_includes_mode_and_via():
    assert amsc._endpoint_argv('ep', 'wss://b:8000', 'forward', 'login.host') \
        == ['--name', 'ep', '--url', 'wss://b:8000',
            '--tunnel', 'forward', '--tunnel-via', 'login.host']


def test_endpoint_argv_reverse_omits_via():
    assert amsc._endpoint_argv('ep', 'u', 'reverse', 'login') \
        == ['--name', 'ep', '--url', 'u', '--tunnel', 'reverse']


def test_endpoint_argv_none_has_no_tunnel_args():
    assert amsc._endpoint_argv('ep', 'u', 'none', None) \
        == ['--name', 'ep', '--url', 'u']


def test_endpoint_argv_forward_requires_login_host():
    # forward mode with no login_host would append None to argv -> raise instead.
    with pytest.raises(ValueError, match='login_host'):
        amsc._endpoint_argv('ep', 'u', 'forward', None)


# ── _wait_for_endpoint ─────────────────────────────────────────────────────

def test_wait_returns_when_endpoint_appears():
    bc = _FakeBC({'ep.0': {'plugins': {}}})
    assert amsc._wait_for_endpoint(bc, 'ep.0') == 'ep.0'


def test_wait_bails_immediately_on_job_failure():
    bc    = _FakeBC({})                       # endpoint never appears
    watch = amsc._JobFailureWatch(bc, 'job-1')
    watch._on_status('broker', 'iri.nersc', 'job_status',
                     {'job_id': 'job-1', 'state': 'failed', 'details': 'boom'})
    with pytest.raises(RuntimeError, match='job failed'):
        amsc._wait_for_endpoint(bc, 'ep.0', failure=watch)


def test_wait_times_out_when_nothing_happens(monkeypatch):
    monkeypatch.setattr(amsc, 'ENDPOINT_WAIT_SECONDS', 0)
    bc = _FakeBC({})
    with pytest.raises(TimeoutError):
        amsc._wait_for_endpoint(bc, 'ep.0')


# ── _JobFailureWatch ───────────────────────────────────────────────────────

def test_watch_subscribes_and_only_trips_on_own_terminal_failure():
    bc    = _FakeBC()
    watch = amsc._JobFailureWatch(bc, 'job-1')
    assert bc.subs and bc.subs[0][0] == 'job_status'

    watch._on_status('b', 'p', 'job_status',                # different job
                     {'job_id': 'other', 'state': 'failed'})
    watch._on_status('b', 'p', 'job_status',                # non-terminal
                     {'job_id': 'job-1', 'state': 'running'})
    assert not watch.failed

    watch._on_status('b', 'p', 'job_status',                # psij upper-case
                     {'job_id': 'job-1', 'state': 'FAILED'})
    assert watch.failed
    assert watch.reason

    watch.close()
    assert bc.subs == []


def test_watch_ignores_malformed_payload():
    # The callback runs on the dispatcher thread; a None / non-dict payload must
    # not raise (that would kill the thread).
    bc    = _FakeBC()
    watch = amsc._JobFailureWatch(bc, 'job-1')
    watch._on_status('b', 'p', 'job_status', None)
    watch._on_status('b', 'p', 'job_status', 'not-a-dict')
    watch._on_status('b', 'p', 'job_status', 42)
    assert not watch.failed
