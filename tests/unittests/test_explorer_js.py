"""Browser-JS unit tests, run under pytest via the quickjs engine.

The Explorer's pure session-heal helpers live in a shared, dependency-free ES
module (``src/radical/orbit/data/plugins/session_util.js``) that the browser
imports and these tests load into a quickjs context.  Only pure logic (no DOM /
fetch / SSE) is exercised here; end-to-end UI coverage would need a browser
harness (e.g. pytest-playwright) and is out of scope.

quickjs is a ``[test]`` / ``[dev]`` extra; the whole module skips where it is
not installed so the suite stays green.
"""

import json
import pathlib
import re

import pytest

quickjs = pytest.importorskip("quickjs")


_MODULE = (pathlib.Path(__file__).resolve().parents[2]
           / "src" / "radical" / "orbit" / "data" / "plugins"
           / "session_util.js")


def _context():
    """A quickjs context with session_util.js loaded.

    The file is an ES module (for the browser); quickjs ``eval`` runs a plain
    script, so the trailing ``export { ... }`` line is stripped — the function
    declarations then live in the context's global scope.
    """
    src = _MODULE.read_text()
    src = re.sub(r'^\s*export\s*\{[^}]*\};?\s*$', '', src, flags=re.M)
    ctx = quickjs.Context()
    ctx.eval(src)
    return ctx


def _run(ctx, expr):
    """Evaluate a JS expression and return its JSON-decoded value.

    Results cross the boundary as a JSON string (``JSON.stringify``) to avoid
    per-type marshalling; inputs are inlined as JSON literals by the caller.
    """
    return json.loads(ctx.eval("JSON.stringify((%s))" % expr))


@pytest.fixture(scope="module")
def ctx():
    return _context()


# ── isStaleSession ─────────────────────────────────────────────────────────

def test_is_stale_session_410_is_stale(ctx):
    assert _run(ctx, "isStaleSession(410, 'session expired: s1')") is True


def test_is_stale_session_404_unknown_sid_is_stale(ctx):
    assert _run(ctx, "isStaleSession(404, 'unknown session id: s1')") is True


def test_is_stale_session_404_job_not_found_is_not_stale(ctx):
    # A live session that simply doesn't have the job/task -> NOT a stale sid.
    assert _run(ctx, "isStaleSession(404, 'job not found: j1')") is False


def test_is_stale_session_other_status_is_not_stale(ctx):
    assert _run(ctx, "isStaleSession(500, 'boom')") is False
    assert _run(ctx, "isStaleSession(404, null)")   is False


# ── matchSessionForPath ────────────────────────────────────────────────────

_NSOF = "(en, pn) => '/' + en + '/' + pn"


def test_match_finds_session_scoped_by_namespace_and_sid(ctx):
    sessions = {"ep/psij": "sid-abc", "ep/rhapsody": "sid-xyz"}
    got = _run(ctx, "matchSessionForPath("
                    "'/ep/psij/status/sid-abc/job1', %s, %s)"
                    % (json.dumps(sessions), _NSOF))
    assert got == {"key": "ep/psij", "endpoint": "ep",
                   "plugin": "psij", "sid": "sid-abc"}


def test_match_requires_namespace_prefix(ctx):
    # sid present but the path is under a different namespace -> no match.
    sessions = {"ep/psij": "sid-abc"}
    got = _run(ctx, "matchSessionForPath("
                    "'/ep/rhapsody/x/sid-abc', %s, %s)"
                    % (json.dumps(sessions), _NSOF))
    assert got is None


def test_match_requires_sid_in_path(ctx):
    sessions = {"ep/psij": "sid-abc"}
    got = _run(ctx, "matchSessionForPath("
                    "'/ep/psij/status/sid-other/job1', %s, %s)"
                    % (json.dumps(sessions), _NSOF))
    assert got is None


def test_match_skips_in_flight_promise_entries(ctx):
    # A non-string value is an in-flight registration promise -> skipped.
    got = _run(ctx,
               "matchSessionForPath('/ep/psij/status/x', "
               "(function(){var s={}; s['ep/psij']={then:1}; return s;})(), %s)"
               % _NSOF)
    assert got is None


# ── swapSid ────────────────────────────────────────────────────────────────

def test_swap_sid_replaces_stale_segment(ctx):
    got = _run(ctx, "swapSid('/ep/psij/status/sid-old/job1', 'sid-old', 'sid-new')")
    assert got == "/ep/psij/status/sid-new/job1"


def test_swap_sid_no_op_when_absent(ctx):
    got = _run(ctx, "swapSid('/ep/psij/status/sid-x/j', 'sid-old', 'sid-new')")
    assert got == "/ep/psij/status/sid-x/j"
