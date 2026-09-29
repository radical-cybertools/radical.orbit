"""Browser-JS unit tests, run under pytest via the quickjs engine.

The Explorer's pure session-heal helpers live in a shared, dependency-free ES
module (``src/radical/orbit/data/plugins/session_util.js``) that the browser
imports and these tests load into a quickjs context.  Only pure logic (no DOM /
fetch / SSE) is exercised here; end-to-end UI coverage would need a browser
harness (e.g. pytest-playwright) and is out of scope.

quickjs is a ``[test]`` / ``[dev]`` extra; those tests skip where it is not
installed so the suite stays green.

The task-dispatcher plugin's pool rendering is pure string building, too, but
needs template literals and modern syntax, so it is exercised through ``node``
(also skipped where absent): the module's ``export`` keywords are stripped and
the declarations are evaluated in a ``vm`` context, exactly the trick the
quickjs helper below uses.
"""

import json
import pathlib
import re
import shutil
import subprocess

import pytest

try:
    import quickjs
except ImportError:
    quickjs = None


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
    if quickjs is None:
        pytest.skip("quickjs not installed")
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


# ── task_dispatcher.js: pool rendering (node) ──────────────────────────────

_NODE = shutil.which("node")

_TD_MODULE = (pathlib.Path(__file__).resolve().parents[2]
              / "src" / "radical" / "orbit" / "data" / "plugins"
              / "task_dispatcher.js")

# Driver: load the plugin as a script (export keywords stripped), then render
# the `GET /pools` payload handed in as JSON and print the resulting HTML.
_TD_DRIVER = r"""
const fs = require('fs');
const vm = require('vm');

let src = fs.readFileSync(process.argv[2], 'utf8').replace(/^export\s+/gm, '');

const api = {
  escHtml: s => String(s === null || s === undefined ? '' : s)
                .replace(/&/g, '&amp;').replace(/</g, '&lt;')
                .replace(/>/g, '&gt;').replace(/"/g, '&quot;'),
  flash: () => {},
};

const pools = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const ctx = vm.createContext({api: api, pools: pools, out: ''});
vm.runInContext(src + '\nout = renderEntries(flattenPools(pools), api);', ctx);
process.stdout.write(ctx.out);
"""


@pytest.fixture(scope="module")
def render_pools(tmp_path_factory):
    """Render a `GET /pools` payload's `pools` value to HTML via node."""
    if not _NODE:
        pytest.skip("node not installed")

    d      = tmp_path_factory.mktemp("task_dispatcher")
    driver = d / "render_pools.js"
    driver.write_text(_TD_DRIVER)

    def _render(pools):
        payload = d / "pools.json"
        payload.write_text(json.dumps(pools))
        r = subprocess.run([_NODE, str(driver), str(_TD_MODULE), str(payload)],
                           capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        return r.stdout

    return _render


def _card_names(html):
    """The pool name shown on each rendered card, in order."""
    return re.findall(r'class="td-pool-name">([^<]*)<', html)


def _size(nodes=1, gpus=0):
    return {"nodes": nodes, "cpus_per_node": 64, "gpus_per_node": gpus,
            "walltime_sec": 3600, "rhapsody_backend": "psij"}


def _class_pool(name="fed-gpu", members=("r3_default",)):
    return {"name": name, "endpoint_name": "fed", "queue": None,
            "account": None, "default_size": "small",
            "pilot_sizes": {"small": _size(gpus=4)},
            "live_pilots": 1, "pending_tasks": 2,
            "min_pilots": 0, "max_pilots": 1,
            "pool_class": "gpu", "multi_member": True,
            "member_ids": list(members), "max_pilots_total": 4}


def test_pools_grouped_by_session_render_one_card_per_pool(render_pools):
    # `GET /pools` answers `{sid: {pool name: summary}}` -- the session is a
    # grouping key, never a pool of its own.
    html = render_pools({"fed": {"fed-gpu": _class_pool()}})

    assert html.count('data-td-card=') == 1
    assert _card_names(html) == ["fed-gpu"]
    assert "session fed" in html
    # the class pool renders member rows, not the legacy queue/account meta
    assert "<strong>queue</strong>" not in html
    assert "No sizes defined." not in html
    assert "r3_default" in html


def test_pools_grouped_across_sessions_render_every_pool(render_pools):
    html = render_pools({
        "fed":  {"fed-gpu": _class_pool(),
                 "fed-cpu": _class_pool("fed-cpu", ["r3_cpu"])},
        "s2":   {"default": {"name": "default", "queue": "debug",
                             "account": "acct", "min_pilots": 1,
                             "max_pilots": 3, "live_pilots": 0,
                             "pending_tasks": 0, "default_size": "small",
                             "pilot_sizes": {"small": _size()},
                             "multi_member": False, "member_ids": []}},
    })

    assert html.count('data-td-card=') == 3
    assert sorted(_card_names(html)) == ["default", "fed-cpu", "fed-gpu"]
    assert "session fed" in html
    assert "session s2" in html
    # the single-site pool keeps its legacy meta line
    assert "<strong>queue</strong>" in html


def test_pools_empty_renders_placeholder(render_pools):
    assert "No pools configured." in render_pools({})
    # a session that owns no pool is not a pool either
    assert "No pools configured." in render_pools({"fed": {}})


def test_verbose_class_pool_shows_member_sizes_and_node_hours(render_pools):
    # The shape of the verbose `pool/{sid}/{name}` re-render: `members`
    # objects carry per-member sizes and node-hours.
    pool = _class_pool()
    pool["members"] = [{
        "member_id": "r3_default", "endpoint_name": "r3", "queue": "gpu",
        "account": "m1234", "attributes": {"site": "NERSC",
                                           "software": ["lammps", "pytorch"]},
        "min_pilots": 0, "max_pilots": 4, "live_pilots": 1,
        "pilot_sizes": {"gpu4": _size(nodes=2, gpus=4)},
        "default_size": "gpu4",
        "node_hours_used": 1.5, "node_hours_remaining": 98.5,
    }]
    html = render_pools({"fed": {"fed-gpu": pool}})

    assert _card_names(html) == ["fed-gpu"]
    assert "1.50 / 98.50" in html
    assert "site=NERSC" in html
    assert "software: lammps, pytorch" in html
    # the member's own size table is nested under its row
    assert "td-member-sizes" in html
    assert "gpu4" in html


# ── federation.js: resource / member rendering (node) ──────────────────────

_FED_MODULE = (pathlib.Path(__file__).resolve().parents[2]
               / "src" / "radical" / "orbit" / "data" / "plugins"
               / "federation.js")

_FED_DRIVER = r"""
const fs = require('fs');
const vm = require('vm');

let src = fs.readFileSync(process.argv[2], 'utf8').replace(/^export\s+/gm, '');

const api = {
  escHtml: s => String(s === null || s === undefined ? '' : s)
                .replace(/&/g, '&amp;').replace(/</g, '&lt;')
                .replace(/>/g, '&gt;').replace(/"/g, '&quot;'),
  flash: () => {},
};

const resources = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const ctx = vm.createContext({api: api, resources: resources, out: '',
                              Date: Date});
vm.runInContext(src + '\nout = renderTable(resources, api);', ctx);
process.stdout.write(ctx.out);
"""


@pytest.fixture(scope="module")
def render_resources(tmp_path_factory):
    """Render a `GET resources/default` resource list to HTML via node."""
    if not _NODE:
        pytest.skip("node not installed")

    d      = tmp_path_factory.mktemp("federation")
    driver = d / "render_resources.js"
    driver.write_text(_FED_DRIVER)

    def _render(resources):
        payload = d / "resources.json"
        payload.write_text(json.dumps(resources))
        r = subprocess.run([_NODE, str(driver), str(_FED_MODULE),
                            str(payload)], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        return r.stdout

    return _render


def _fed_member(name="default", state="ok", usage=None, **overrides):
    m = {"member": name, "member_id": "perlmutter." + name,
         "class": "cpu", "pool_name": "fed-cpu", "queue": "regular",
         "endpoint": "ep_perlmutter", "pilot": "submit",
         "nodes": 1, "cpus_per_node": 128, "gpus_per_node": 0,
         "walltime_sec": 3600, "end_time": None, "remaining_sec": None,
         "software": [], "attributes": {"site": "NERSC"},
         "budget": {}, "liveness": "ok", "state": state,
         "usage": usage or {}}
    m.update(overrides)
    return m


def _fed_resource(members, state="ok", **overrides):
    r = {"name": "perlmutter", "endpoint": "ep_perlmutter",
         "mode": "login", "site": "NERSC",
         "capabilities": {"cores": 128, "gpus": 0, "software": []},
         "budget": {}, "usage": {}, "liveness": "ok", "state": state,
         "members": members}
    r.update(overrides)
    return r


def _fed_rows(html):
    """The rendered `<tr>` blocks, in order: resource row, then its pilots."""
    return re.findall(r'<tr\b.*?</tr>', html, re.S)


def test_fed_healthy_member_renders_its_state_and_no_error_row(
        render_resources):
    html = render_resources([_fed_resource([_fed_member()])])

    assert "fed-live-ok" in html
    assert "fed-live-failing" not in html
    assert "fed-error" not in html
    assert "! pilot" not in html


def test_fed_failing_member_gets_the_badge_and_the_reason(render_resources):
    # the demo case: reachable, no pilots, every submit killed by a quota
    err    = "psij error: submit_tunneled failed: [Errno 122] Disk quota " \
             "exceeded"
    member = _fed_member(state="failing",
                         usage={"pilots_active": 0, "pilot_error": err,
                                "pilot_failures": 7})
    html   = render_resources([_fed_resource([member], state="failing")])

    # the state word, distinctly, on the member row AND the resource row
    assert html.count('class="fed-live-failing"') == 2
    assert ">failing<" in html
    # and the reason underneath, with the full text as the tooltip
    assert 'class="fed-error"' in html
    assert "Disk quota exceeded" in html
    assert 'title="%s"' % err in html
    assert "! pilot ×7:" in html


def test_fed_long_reason_is_truncated_but_kept_in_the_title(render_resources):
    err    = "psij error: " + "x" * 400
    member = _fed_member(state="failing",
                         usage={"pilot_error": err, "pilot_failures": 3})
    html   = render_resources([_fed_resource([member], state="failing")])

    assert "…" in html                           # the ellipsis
    assert 'title="%s"' % err in html            # nothing lost
    assert html.count("x" * 400) == 1            # only in the title


def test_fed_paused_member_says_until_when(render_resources):
    member = _fed_member(state="failing",
                         usage={"pilot_error": "psij error: boom",
                                "pilot_failures": 3,
                                "paused_until": 1757000000})
    html   = render_resources([_fed_resource([member], state="failing")])

    assert "paused until" in html


# ── federation.js: the pilot-row layout (Orbit 122 / ATOMIC 09) ────────────

def test_fed_table_has_the_two_column_sets(render_resources):
    html = render_resources([_fed_resource([_fed_member()])])
    for col in ("resource / pilot", "mode", "nodes", "cpn", "gpn", "mpn",
                "runtime", "left", "run", "done", "failed", "state"):
        assert "<th>%s</th>" % col in html
    # the word the layout replaced is gone from the header
    assert "<th>member</th>" not in html


def test_fed_a_login_pilot_row_is_endpoint_slash_shape(render_resources):
    """In login mode the endpoint submits one pilot per shape, so the row
    has to name both."""
    html = render_resources([_fed_resource([_fed_member(name="gpu")])])
    rows = _fed_rows(html)
    assert "ep_perlmutter/gpu" in rows[2]
    assert ">login<" in rows[2]


def test_fed_an_allocation_pilot_row_is_the_endpoint_itself(render_resources):
    """In allocation mode the endpoint *is* the pilot: no shape suffix."""
    member = _fed_member(name="default", pilot="endpoint", endpoint="ep_odo")
    html = render_resources([_fed_resource([member], mode="allocation",
                                           endpoint="ep_odo", site="OLCF")])
    rows = _fed_rows(html)
    assert "└ ep_odo" in rows[2]
    assert "ep_odo/default" not in html
    assert ">alloc<" in rows[2]


def test_fed_runtime_and_left_are_hours_with_two_decimals(render_resources):
    member = _fed_member(walltime_sec=5400, remaining_sec=4140,
                         attributes={"site": "OLCF",
                                     "mem_gb_per_node": 256})
    rows = _fed_rows(render_resources([_fed_resource([member])]))
    assert ">1.50<" in rows[2]          # 5400 s
    assert ">1.15<" in rows[2]          # 4140 s
    assert ">256<" in rows[2]           # mem per node, as declared


def test_fed_an_unknown_runway_is_a_dash(render_resources):
    """`remaining_sec: null` is 'nobody said', not 'nothing left'."""
    rows = _fed_rows(render_resources([_fed_resource([_fed_member()])]))
    assert ">-<" in rows[2]


def test_fed_mem_per_node_falls_back_to_the_record(render_resources):
    member = _fed_member(attributes={"site": "NERSC"})
    rec    = _fed_resource([member])
    rec["capabilities"] = dict(rec["capabilities"], mem_gb=512)
    rows   = _fed_rows(render_resources([rec]))
    assert ">512<" in rows[2]


def test_fed_an_idle_shape_gets_its_own_style(render_resources):
    html = render_resources([_fed_resource([_fed_member(state="idle")])])
    assert "fed-live-idle" in html
    assert ">idle<" in html
    assert "fed-error" not in html


def test_fed_resource_row_carries_the_classes_and_its_own_counts(
        render_resources):
    """The record's counts include tasks no shape holds yet, so the row must
    not be a sum over the pilot rows."""
    members = [_fed_member(name="cpu", pool_name="fed-cpu",
                           usage={"tasks_running": 1, "tasks_done": 1}),
               _fed_member(name="gpu", pool_name="fed-gpu",
                           usage={"tasks_running": 0, "tasks_done": 2})]
    rec = _fed_resource(members, usage={"tasks_running": 4, "tasks_done": 3,
                                        "tasks_failed": 1})
    rows = _fed_rows(render_resources([rec]))
    assert "fed-cpu" in rows[1] and "fed-gpu" in rows[1]
    assert ">4<" in rows[1] and ">3<" in rows[1] and ">1<" in rows[1]
    assert "OLCF" not in rows[1]


def test_fed_node_hours_moved_into_the_row_tooltip(render_resources):
    member = _fed_member(usage={"node_hours_used": 1.5,
                                "node_hours_remaining": 0.5})
    html = render_resources([_fed_resource([member])])
    assert "node-hours: 1.50 used · 0.50 left" in html
    # ... and out of the table body
    assert "fed-bar" not in html


def test_fed_a_stale_refresh_says_so_in_the_tooltip(render_resources):
    member = _fed_member(state="stale", usage={"stale": True})
    html = render_resources([_fed_resource([member], state="stale")])
    assert "(stale)" in html
    assert "fed-live-stale" in html
