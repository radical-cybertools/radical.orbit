'''
Unit tests for the Rhapsody Endpoint plugin.

All RHAPSODY imports are mocked so the tests do not require the rhapsody
package to be installed.
'''

import json
import asyncio

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from fastapi import FastAPI, HTTPException
from starlette.testclient import TestClient
from starlette.requests import Request

# ---------------------------------------------------------------------------
# Mock rhapsody *before* importing the plugin so the guarded import succeeds
# ---------------------------------------------------------------------------

_mock_rh = MagicMock()

# BaseTask.from_dict returns a task-like dict/mock
def _fake_from_dict(d):
    t = MagicMock()
    t.uid = d.get('uid', 'task.000001')
    t.state = d.get('state', 'SUBMITTED')
    t.get = lambda k, default=None: d.get(k, default)
    t.__getitem__ = lambda self_, k: d[k]
    t.__contains__ = lambda self_, k: k in d
    t.__iter__ = lambda self_: iter(d)
    t.items = lambda: d.items()
    t.keys = lambda: d.keys()

    # Allow dict(t) to work
    def _dict_conv():
        return dict(d, uid=t.uid, state=t.state)
    # Make dict(t) produce the expected mapping
    t.__iter__ = lambda self_: iter(d)
    t.__len__ = lambda self_: len(d)

    # Provide to_dict() that returns a JSON-serializable dict
    def _to_dict():
        ret = {
            'uid': t.uid,
            'state': str(t.state) if t.state else 'SUBMITTED',
            'executable': d.get('executable', ''),
            'arguments': d.get('arguments', []),
        }
        for k in ('task_backend_specific_kwargs', 'backend',
                   'function', 'return_value', 'stdout', 'stderr'):
            if k in d:
                ret[k] = d[k]
        return ret
    t.to_dict = _to_dict

    return t

_mock_rh.BaseTask.from_dict = _fake_from_dict
_mock_rh.Session = MagicMock
_mock_rh.get_backend = MagicMock(return_value=MagicMock())


@pytest.fixture(autouse=True)
def _patch_rhapsody():
    '''Patch `rhapsody` into sys.modules and into plugin_rhapsody.rh.'''
    import sys
    sys.modules['rhapsody'] = _mock_rh

    with patch('radical.orbit.plugin_rhapsody.rh', _mock_rh):
        yield

    # clean up
    sys.modules.pop('rhapsody', None)


# Now import after the mock is in place
from radical.orbit.plugin_base import DEFAULT_SID  # noqa: E402
from radical.orbit.plugin_rhapsody import (  # noqa: E402
    PluginRhapsody,
    RhapsodySession,
    RhapsodyClient,
    _DataEntry,
)

# Capture the unpatched initialize coroutine BEFORE the autouse stub
# fixture is applied, so the start-telemetry regression test below can
# exercise the real method.
_REAL_INITIALIZE = RhapsodySession.initialize


# Stub RhapsodySession.initialize for every test in this module.
# The background `_init_session` task runs concurrently inside the same
# TestClient.post that registers the session, so the test's own
# `_init_ready.set()` always loses the race with `await initialize()`
# — and initialize() now awaits start_telemetry / awaitable backends
# that the sync MagicMock cannot satisfy.  These tests assign their own
# mock `_rh_session` afterwards, so the real initialize is unwanted.
@pytest.fixture(autouse=True)
def _stub_session_initialize():
    with patch.object(RhapsodySession, 'initialize', new_callable=AsyncMock):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plugin():
    app = FastAPI()
    plugin = PluginRhapsody(app)
    client = TestClient(app)
    return app, plugin, client


def _register(client, plugin):
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']
    # Mark session as initialized — tests mock _rh_session directly
    plugin._sessions[sid]._init_ready.set()
    return sid


# ---------------------------------------------------------------------------
# Plugin initialisation
# ---------------------------------------------------------------------------

def test_plugin_rhapsody_init():
    app, plugin, client = _make_plugin()

    assert plugin.plugin_name == 'rhapsody'
    assert plugin.instance_name == 'rhapsody'

    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    ns = plugin.namespace.lstrip('/')
    assert any(f'{ns}/submit/' in p for p in route_pats)
    assert any(f'{ns}/wait/' in p for p in route_pats)
    assert any(f'{ns}/task/' in p for p in route_pats)
    assert any(f'{ns}/cancel/' in p for p in route_pats)
    assert any(f'{ns}/cancel_all/' in p for p in route_pats)


def test_plugin_rhapsody_class_attributes():
    assert PluginRhapsody.session_class is RhapsodySession
    assert PluginRhapsody.client_class is RhapsodyClient
    assert PluginRhapsody.version == '0.0.1'


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

def test_register_session():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    assert sid in plugin._sessions
    assert sid.startswith("session.")


def test_register_multiple_sessions():
    _, plugin, client = _make_plugin()
    sids = [_register(client, plugin) for _ in range(3)]

    assert len(set(sids)) == 3
    assert len(plugin._sessions) == 3


def test_register_session_policy_passthrough():
    '''The rhapsody override forwards the base sid/lifetime/ttl policy:
    explicit-sid create, reconnect, and 409 on an incoherent pair.'''
    _, plugin, client = _make_plugin()

    # Explicit sid + ttl lifetime create
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"sid": "s1", "lifetime": "ttl", "ttl": 30})
    assert resp.status_code == 200
    assert resp.json()['sid'] == "s1"
    assert (plugin._records["s1"].lifetime,
            plugin._records["s1"].ttl,
            plugin._records["s1"].owner) == ('ttl', 30.0, None)
    plugin._sessions["s1"]._init_ready.set()

    # Reconnect with the same policy — no rebuild, same sid
    obj  = plugin._sessions["s1"]
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"sid": "s1", "lifetime": "ttl", "ttl": 30})
    assert resp.status_code == 200
    assert resp.json()['sid'] == "s1"
    assert plugin._sessions["s1"] is obj

    # Incoherent pair maps to 409
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"lifetime": "ttl"})
    assert resp.status_code == 409


def test_register_session_reconnect_real_status():
    '''Reconnect reports the session's real init status — 'ready' once
    init completed, 'failed' once it errored — not 'initializing'.'''
    _, plugin, client = _make_plugin()

    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"sid": "s1"})
    assert resp.status_code == 200
    assert resp.json()['status'] == 'initializing'

    # Reconnect to a ready session reports 'ready'
    session = plugin._sessions["s1"]
    session._init_ready.set()
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"sid": "s1"})
    assert resp.status_code == 200
    assert resp.json() == {'sid': 's1', 'status': 'ready'}

    # ... and 'failed' once init errored
    session._init_error = 'boom'
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"sid": "s1"})
    assert resp.json()['status'] == 'failed'


def test_register_session_backends_must_be_list():
    '''A non-list `backends` (e.g. a bare string) is rejected synchronously
    with 400 — it would otherwise fail only in the background init.'''
    _, plugin, client = _make_plugin()
    resp = client.post(f"{plugin.namespace}/register_session",
                       json={"backends": "dragon_v3"})
    assert resp.status_code == 400


def test_register_session_sid_collision_mints_unique(monkeypatch):
    '''Minting a fresh sid loops until it does not collide with a live one.'''
    import radical.orbit.plugin_rhapsody as rmod
    _, plugin, client = _make_plugin()

    # Pre-occupy the id the first mint would produce so the loop must retry.
    plugin._sessions['session.aaaaaaaa'] = object()

    class _FakeUUID:
        def __init__(self, h):
            self.hex = h

    seq = iter([_FakeUUID('aaaaaaaa' + '0' * 24),
                _FakeUUID('bbbbbbbb' + '0' * 24)])
    monkeypatch.setattr(rmod.uuid, 'uuid4', lambda: next(seq))

    with patch.object(RhapsodySession, 'initialize', new_callable=AsyncMock):
        resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    assert resp.json()['sid'] == 'session.bbbbbbbb'   # skipped the collision


@pytest.mark.asyncio
async def test_register_default_session():
    '''register_session(sid='default') creates the persistent default
    session, kicks its background init, and reports the real status.'''
    _, plugin, _ = _make_plugin()

    with patch.object(PluginRhapsody, '_init_session',
                      new_callable=AsyncMock) as init_mock:
        request = MagicMock(spec=Request)
        request.json = AsyncMock(return_value={"sid": DEFAULT_SID})
        resp = await plugin.register_session(request)
        await asyncio.sleep(0)   # let the init task run

    assert resp['sid']    == DEFAULT_SID
    assert resp['status'] == 'initializing'
    assert plugin._records[DEFAULT_SID].lifetime == 'persistent'
    init_mock.assert_called_once_with(
        DEFAULT_SID, plugin._sessions[DEFAULT_SID])

    # Re-register once ready reports the real status; no rebuild, no re-init
    session = plugin._sessions[DEFAULT_SID]
    session._init_ready.set()
    with patch.object(PluginRhapsody, '_init_session',
                      new_callable=AsyncMock) as init_mock:
        resp = await plugin.register_session(request)
    assert resp == {'sid': DEFAULT_SID, 'status': 'ready'}
    assert plugin._sessions[DEFAULT_SID] is session
    init_mock.assert_not_called()


@pytest.mark.asyncio
async def test_default_session_via_forward():
    '''An unregistered `_forward` to 'default' auto-creates the rhapsody
    default session (persistent) and kicks its background init.'''
    _, plugin, _ = _make_plugin()

    assert DEFAULT_SID not in plugin._sessions
    with patch.object(PluginRhapsody, '_init_session',
                      new_callable=AsyncMock) as init_mock:
        # The call itself 409s — init has not completed yet — but the
        # session must exist afterwards with its init task started.
        with pytest.raises(HTTPException) as exc_info:
            await plugin._forward(DEFAULT_SID, RhapsodySession.list_tasks)
        await asyncio.sleep(0)   # let the init task run

    assert exc_info.value.status_code == 409
    assert DEFAULT_SID in plugin._sessions
    assert plugin._records[DEFAULT_SID].lifetime == 'persistent'
    init_mock.assert_called_once_with(
        DEFAULT_SID, plugin._sessions[DEFAULT_SID])


def test_unregister_session():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Ensure session.close can be awaited
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.close = AsyncMock()

    resp = client.post(f"{plugin.namespace}/unregister_session/{sid}")
    assert resp.status_code == 200
    assert sid not in plugin._sessions


def test_unregister_unknown_session():
    _, plugin, client = _make_plugin()

    with pytest.raises(HTTPException) as exc_info:
        # Use the internal handler directly for cleaner 404 detection
        asyncio.run(
            plugin.unregister_session(
                MagicMock(spec=Request, path_params={"sid": "bogus"})
            )
        )
    assert exc_info.value.status_code == 404


def test_list_sessions():
    _, plugin, client = _make_plugin()
    sid1 = _register(client, plugin)
    sid2 = _register(client, plugin)

    resp = client.get(f"{plugin.namespace}/list_sessions")
    assert resp.status_code == 200
    assert set(resp.json()['sessions']) == {sid1, sid2}


def test_version_endpoint():
    _, plugin, client = _make_plugin()

    resp = client.get(f"{plugin.namespace}/version")
    assert resp.status_code == 200
    assert resp.json()['version'] == '0.0.1'


# ---------------------------------------------------------------------------
# Submit tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_tasks():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Wire up a mock rhapsody.Session on the session object
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["hello"],
             "uid": "task.000001"}
        ]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data[0]['uid'] == 'task.000001'


# ---------------------------------------------------------------------------
# Wait tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_tasks():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    session._rh_session.wait_tasks = AsyncMock()

    # First submit
    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["hi"],
             "uid": "task.000002"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    # Then wait
    wait_payload = {"uids": ["task.000002"]}
    resp = client.post(f"{plugin.namespace}/wait/{sid}", json=wait_payload)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Get task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_task():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["yo"],
             "uid": "task.000003"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    resp = client.get(f"{plugin.namespace}/task/{sid}/task.000003")
    assert resp.status_code == 200


def test_get_task_unknown():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    # Mock session internals so it has a proper _rh_session
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()

    resp = client.get(f"{plugin.namespace}/task/{sid}/no_such_task")
    assert resp.status_code == 404  # HTTPException re-raised with original status


# ---------------------------------------------------------------------------
# Cancel task
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_task():
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    mock_backend = MagicMock()
    mock_backend.cancel_task = AsyncMock()
    session._rh_session.backends = {'dragon_v3': mock_backend}

    # submit first
    payload = {
        "tasks": [
            {"executable": "/bin/echo", "arguments": ["x"],
             "uid": "task.000004", "backend": "dragon_v3"}
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    # cancel
    resp = client.post(f"{plugin.namespace}/cancel/{sid}/task.000004")
    assert resp.status_code == 200
    assert resp.json()['status'] == 'canceled'


# ---------------------------------------------------------------------------
# Phase 0 — passthrough verification
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_with_backend_specific_kwargs():
    """task_backend_specific_kwargs must survive the plugin round-trip."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    # Intercept from_dict calls to capture the raw dicts
    captured = []
    orig_from_dict = _mock_rh.BaseTask.from_dict
    _mock_rh.BaseTask.from_dict = lambda d: (captured.append(d), orig_from_dict(d))[1]

    kwargs = {"timeout": 30, "ranks": 4, "type": "mpi"}
    payload = {
        "tasks": [{
            "executable": "/bin/echo", "arguments": ["hi"],
            "uid": "task.kw001",
            "task_backend_specific_kwargs": kwargs,
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200

    _mock_rh.BaseTask.from_dict = orig_from_dict
    assert len(captured) == 1
    assert captured[0]["task_backend_specific_kwargs"] == kwargs


@pytest.mark.asyncio
async def test_submit_with_per_task_backend():
    """Per-task 'backend' field must reach BaseTask.from_dict."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    captured = []
    orig_from_dict = _mock_rh.BaseTask.from_dict
    _mock_rh.BaseTask.from_dict = lambda d: (captured.append(d), orig_from_dict(d))[1]

    payload = {
        "tasks": [{
            "executable": "/bin/echo", "arguments": [],
            "uid": "task.be001",
            "backend": "dragon_v3",
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200

    _mock_rh.BaseTask.from_dict = orig_from_dict
    assert len(captured) == 1
    assert captured[0]["backend"] == "dragon_v3"


# ---------------------------------------------------------------------------
# Phase 1a — _sanitize_task hardening
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sanitize_callable_function():
    """Callable in 'function' field must be stringified."""
    session = RhapsodySession("test.san1")
    session._rh_session = MagicMock()

    def my_func(x):
        return x + 1

    task = MagicMock()
    task.uid = "task.fn001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.fn001", "state": "DONE",
                            "function": my_func}

    d = session._sanitize_task(task)
    assert isinstance(d["function"], str)
    assert "my_func" in d["function"]


@pytest.mark.asyncio
async def test_sanitize_non_serializable_return_value():
    """Non-JSON-serializable return_value must be cloudpickled, not str()'d."""
    session = RhapsodySession("test.san2")
    session._rh_session = MagicMock()

    class DragonRef:
        def __repr__(self):
            return "DataReference(0xdead)"

    task = MagicMock()
    task.uid = "task.rv001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.rv001", "state": "DONE",
                            "return_value": DragonRef()}

    d = session._sanitize_task(task)
    assert d["_return_value_encoding"] == "cloudpickle"
    assert d["return_value"].startswith("cloudpickle::")
    json.dumps(d)                                    # still wire-safe


@pytest.mark.asyncio
async def test_sanitize_bytes_stdout():
    """bytes stdout/stderr must be decoded to str."""
    session = RhapsodySession("test.san3")
    session._rh_session = MagicMock()

    task = MagicMock()
    task.uid = "task.bs001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.bs001", "state": "DONE",
                            "stdout": b"hello\n", "stderr": b"warn\n"}

    d = session._sanitize_task(task)
    assert d["stdout"] == "hello\n"
    assert d["stderr"] == "warn\n"


@pytest.mark.asyncio
async def test_sanitize_list_stdout():
    """list stdout/stderr (multi-rank) must be joined."""
    session = RhapsodySession("test.san4")
    session._rh_session = MagicMock()

    task = MagicMock()
    task.uid = "task.ls001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.ls001", "state": "DONE",
                            "stdout": ["rank0 out", "rank1 out"],
                            "stderr": ["rank0 err"]}

    d = session._sanitize_task(task)
    assert d["stdout"] == "rank0 out\nrank1 out"
    assert d["stderr"] == "rank0 err"


def test_sanitize_list_of_bytes_stdout():
    """multi-rank bytes output is decoded, not joined as b'...' reprs."""
    session = RhapsodySession("test.san4b")
    session._rh_session = MagicMock()

    task = MagicMock()
    task.uid = "task.lb001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.lb001", "state": "DONE",
                            "stdout": [b"rank0 out", b"rank1 \xff"]}

    d = session._sanitize_task(task)
    assert d["stdout"] == "rank0 out\nrank1 \ufffd"


@pytest.mark.asyncio
async def test_sanitize_preserves_normal_values():
    """Normal JSON-serializable values must pass through unchanged."""
    session = RhapsodySession("test.san5")
    session._rh_session = MagicMock()

    task = MagicMock()
    task.uid = "task.ok001"
    task.state = "DONE"
    task.to_dict = lambda: {"uid": "task.ok001", "state": "DONE",
                            "return_value": {"count": 42},
                            "stdout": "normal output",
                            "function": None}

    d = session._sanitize_task(task)
    assert d["return_value"] == {"count": 42}
    assert d["stdout"] == "normal output"
    assert d["function"] is None
    assert "_return_value_encoding" not in d       # unchanged wire form


# ---------------------------------------------------------------------------
# Return-value fidelity — encode (endpoint) / decode (client) round trip
# ---------------------------------------------------------------------------

def _done_task(uid, return_value):
    """Build a completed task whose to_dict() carries *return_value*."""
    task = MagicMock()
    task.uid = uid
    task.state = "DONE"
    task.to_dict = lambda: {"uid": uid, "state": "DONE",
                            "return_value": return_value}
    return task


@pytest.mark.asyncio
async def test_return_value_roundtrip_numpy():
    """A numpy result survives encode -> notification -> client decode."""
    np = pytest.importorskip("numpy")

    session = RhapsodySession("test.rv.np")
    session._rh_session = MagicMock()

    arr = np.arange(6, dtype=np.float64).reshape(2, 3)
    payload = session._notification_payload(_done_task("task.np001", arr))

    # what actually goes on the wire
    json.dumps(payload)
    assert payload["_return_value_encoding"] == "cloudpickle"

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status", payload)

    got = client._completed["task.np001"]["return_value"]
    assert isinstance(got, np.ndarray)
    assert np.array_equal(got, arr)
    assert "_return_value_encoding" not in client._completed["task.np001"]


@pytest.mark.asyncio
async def test_return_value_roundtrip_custom_class():
    """A rich Python object comes back as the object, not as its str()."""

    class Weights:
        def __init__(self, layers):
            self.layers = layers

        def __repr__(self):
            return "<Weights>"

    session = RhapsodySession("test.rv.obj")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _done_task("task.obj001", Weights([1, 2, 3])))

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status_batch",
                         {"tasks": [payload]})

    got = client._completed["task.obj001"]["return_value"]
    assert got.layers == [1, 2, 3]


@pytest.mark.asyncio
async def test_return_value_bytes_stay_base64():
    """bytes results keep the established plain-base64 convention."""
    session = RhapsodySession("test.rv.bytes")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _done_task("task.by001", b"\x00\xff"))
    assert payload["_return_value_encoding"] == "base64"
    assert not payload["return_value"].startswith("cloudpickle::")

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status", payload)
    assert client._completed["task.by001"]["return_value"] == b"\x00\xff"


@pytest.mark.asyncio
async def test_return_value_json_safe_not_encoded():
    """JSON-safe results are passed through verbatim (backward compatible)."""
    session = RhapsodySession("test.rv.json")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _done_task("task.js001", {"loss": 0.5, "epochs": [1, 2]}))
    assert payload["return_value"] == {"loss": 0.5, "epochs": [1, 2]}
    assert "_return_value_encoding" not in payload

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status", payload)
    assert client._completed["task.js001"]["return_value"] == {
        "loss": 0.5, "epochs": [1, 2]}


@pytest.mark.asyncio
async def test_return_value_oversized_reports_error(caplog):
    """An oversized result errors loudly instead of being truncated."""
    session = RhapsodySession("test.rv.big")
    session._rh_session = MagicMock()
    session._return_value_limit = 64

    with caplog.at_level('ERROR'):
        payload = session._notification_payload(
            _done_task("task.big001", set(range(1000))))

    assert payload["return_value"] is None
    assert "_return_value_encoding" not in payload
    assert "too large" in payload["error"]
    assert "too large" in caplog.text


@pytest.mark.asyncio
async def test_json_safe_return_value_oversized_reports_error(caplog):
    """A JSON-safe result is size-checked too — a huge plain list would
    otherwise overrun the frame exactly like a huge pickle."""
    session = RhapsodySession("test.rv.bigjson")
    session._rh_session = MagicMock()
    session._return_value_limit = 64

    with caplog.at_level('ERROR'):
        payload = session._notification_payload(
            _done_task("task.big002", list(range(1000))))

    assert payload["return_value"] is None
    assert "_return_value_encoding" not in payload
    assert "too large" in payload["error"]
    assert "too large" in caplog.text


def _output_task(uid, stdout, stderr):
    task = MagicMock()
    task.uid = uid
    task.state = "DONE"
    task.to_dict = lambda: {"uid": uid, "state": "DONE", "exit_code": 0,
                            "stdout": stdout, "stderr": stderr}
    return task


def test_notification_carries_output_to_sse_wait():
    """stdout/stderr reach the terminal notification, so the client's SSE
    wait path returns them like the poll path and ``get_task`` do."""
    session = RhapsodySession("test.out")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _output_task("task.out001", b"hello\n", ["r0 err", "r1 err"]))
    assert payload["stdout"] == "hello\n"
    assert payload["stderr"] == "r0 err\nr1 err"

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status", payload)
    assert client._completed["task.out001"]["stdout"] == "hello\n"
    assert client._completed["task.out001"]["stderr"] == "r0 err\nr1 err"


def test_notification_output_is_valid_utf8():
    """A lone surrogate (e.g. surrogateescape-decoded output) must not
    reach the frame: msgpack refuses it and the whole batch is lost."""
    import msgpack
    session = RhapsodySession("test.out.sur")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _output_task("task.sur", "ok \udcff", "\udcfe" * 2000))
    msgpack.packb(payload)
    assert payload["stdout"] == "ok ?"
    assert "stdout_truncated" not in payload

    # any string field, not only the output streams
    task = MagicMock()
    task.uid = "task.sur2"
    task.state = "FAILED"
    task.to_dict = lambda: {"uid": "task.sur2", "state": "FAILED",
                            "exception": "no such file b\udcff.dat",
                            "error": "\udcfe"}
    payload = session._notification_payload(task)
    msgpack.packb(payload)
    assert payload["exception"] == "no such file b?.dat"


def test_payload_size_counts_utf8_bytes():
    """Multi-byte output must count in bytes, else frame splitting packs a
    batch several times past the frame cap."""
    from radical.orbit.plugin_rhapsody import _payload_size
    assert _payload_size({"stdout": "\u4e2d" * 100}) \
        >= _payload_size({"stdout": "x" * 300})


def test_notification_output_keeps_tail_under_limit():
    """A stream over the limit keeps its tail behind a marker that names
    the dropped size; the result stays within the limit in UTF-8 bytes."""
    session = RhapsodySession("test.out.big")
    session._rh_session = MagicMock()
    session._output_notify_limit = 256

    out = "x" * 1000 + "THE END"
    err = "\u00e4" * 1000                     # 2 bytes each: cut mid-char
    payload = session._notification_payload(
        _output_task("task.out002", out, err))

    assert payload["stdout"].startswith("[... ")
    assert payload["stdout"].endswith("THE END")
    dropped = len(out) - (256 - 128)
    assert f"{dropped} characters truncated" in payload["stdout"]
    for key in ("stdout", "stderr"):
        assert len(payload[key].encode("utf-8")) <= 256
        assert payload[f"{key}_truncated"] is True

    # the cut lands on a character boundary and the count is exact
    err_tail = payload["stderr"].split("...]\n", 1)[1]
    assert set(err_tail) == {"\u00e4"}
    assert (f"{1000 - len(err_tail)} characters truncated"
            in payload["stderr"])

    # a stream that fits carries no flag
    small = session._notification_payload(_output_task("t.s", "ok", ""))
    assert small["stdout"] == "ok"
    assert "stdout_truncated" not in small

    # a limit too small for the marker yields the bare tail
    session._output_notify_limit = 16
    tiny = session._notification_payload(_output_task("t.t", out, ""))
    assert tiny["stdout"] == out[-16:]
    assert tiny["stdout_truncated"] is True

    # a zero limit drops the stream without encoding it
    session._output_notify_limit = 0
    zero = session._notification_payload(_output_task("t.z", out, ""))
    assert zero["stdout"] == ""
    assert zero["stdout_truncated"] is True
    assert "stderr_truncated" not in zero

    # get_task's view stays complete
    assert session._sanitize_task(
        _output_task("task.out002", out, err))["stdout"] == out


@pytest.mark.asyncio
async def test_literal_cloudpickle_string_return_untouched():
    """A result that *is* the string 'cloudpickle::x' is JSON-safe: it gets
    no marker, and the client must not mistake it for an encoded blob."""
    session = RhapsodySession("test.rv.lit")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _done_task("task.lit001", "cloudpickle::x"))
    assert payload["return_value"] == "cloudpickle::x"
    assert "_return_value_encoding" not in payload

    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status", payload)
    assert client._completed["task.lit001"]["return_value"] \
        == "cloudpickle::x"


def test_get_task_and_list_tasks_return_wire_form():
    """get_task()/list_tasks() must NOT decode.

    Their dicts are proxied verbatim as JSON (see
    ``plugin_task_dispatcher._route_get_task``), so a decoded object would
    break the proxy.  Pinned so the asymmetry with wait_tasks() is not
    'fixed' by accident.
    """
    wire = {"uid": "t.wire", "state": "DONE",
            "return_value": "cloudpickle::QUFB",
            "_return_value_encoding": "cloudpickle"}

    client = _make_rhapsody_client(dict(wire))
    assert client.get_task("t.wire") == wire

    client = _make_rhapsody_client({"tasks": [dict(wire)]})
    got = client.list_tasks()
    assert got["tasks"][0] == wire
    json.dumps(got)                                  # still proxyable


def test_notification_callbacks_fire_in_registration_order():
    """Cross-repo contract behind the client-side decode.

    rhapsody's ``OrbitExecutionBackend`` registers its own ``task_status``
    callback *after* ``RhapsodyClient._on_task_done`` and reads the very
    same dict object, relying on the decode having already happened.  That
    only holds because the runtime hands one payload to every callback and
    runs them in registration order on a single dispatcher thread.
    """
    import threading as _threading

    from radical.orbit import protocol
    from radical.orbit.runtime import EndpointRuntime

    # A bare registry — no transport, no dispatcher thread needed.
    rt = EndpointRuntime.__new__(EndpointRuntime)
    rt._callbacks = {}
    rt._cb_lock   = _threading.Lock()
    rt._name      = 'test-consumer'
    rt._emit      = MagicMock()

    queued: list = []
    rt._cb = MagicMock()
    rt._cb.submit = lambda fn, *args: queued.append((fn, args))

    order: list = []
    seen:  list = []

    def _client_cb(endpoint, plugin, topic, data):        # RhapsodyClient
        order.append('client')
        data['return_value'] = 'decoded'

    def _backend_cb(endpoint, plugin, topic, data):       # rhapsody backend
        order.append('backend')
        seen.append(data['return_value'])

    for cb in (_client_cb, _backend_cb):
        rt.register_callback(endpoint_id='ep', plugin_name='rhapsody',
                             topic='task_status', callback=cb)

    rt._on_event(protocol.Event(src='ep', plugin='rhapsody',
                                topic='task_status',
                                data={'uid': 't.1',
                                      'return_value': 'wire'}))

    # single-threaded FIFO dispatcher — run the queue in submission order
    for fn, args in queued:
        fn(*args)

    assert order == ['client', 'backend']
    assert seen  == ['decoded']      # same dict object reached the backend


@pytest.mark.asyncio
async def test_return_value_unpicklable_falls_back_to_str():
    """An unpicklable result degrades to str() rather than losing the task."""
    import threading as _threading

    session = RhapsodySession("test.rv.nopickle")
    session._rh_session = MagicMock()

    payload = session._notification_payload(
        _done_task("task.np002", _threading.Lock()))

    assert isinstance(payload["return_value"], str)
    assert "_return_value_encoding" not in payload


def test_decode_return_value_undecodable_keeps_wire_form(caplog):
    """A result the client cannot decode is kept, not raised on."""
    t = {"uid": "task.bad", "state": "DONE",
         "return_value": "cloudpickle::not-base64!!",
         "_return_value_encoding": "cloudpickle"}

    with caplog.at_level('WARNING'):
        RhapsodyClient._decode_return_value(t)

    assert t["return_value"] == "cloudpickle::not-base64!!"
    assert t["_return_value_encoding"] == "cloudpickle"
    assert "cannot decode" in caplog.text


def test_wait_tasks_poll_decodes_return_value():
    """The no-SSE poll fallback decodes exactly like the SSE path."""
    np = pytest.importorskip("numpy")

    session = RhapsodySession("test.rv.poll")
    session._rh_session = MagicMock()

    arr  = np.array([1, 2, 3])
    wire = session._sanitize_task(_done_task("t.poll", arr))

    client = _make_rhapsody_client([wire])
    client._submitted.add("t.poll")
    result = client.wait_tasks(["t.poll"])

    assert np.array_equal(result[0]["return_value"], arr)


# ---------------------------------------------------------------------------
# Phase 2 — function task serialization
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_function_task_cloudpickle_roundtrip():
    """cloudpickle-encoded function must be deserialized before from_dict."""
    import cloudpickle
    import base64

    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    # Simulate what the Endpoint backend (endpoint.py) would do
    def adder(a, b):
        return a + b

    fn_pickled = 'cloudpickle::' + base64.b64encode(
        cloudpickle.dumps(adder)).decode('ascii')
    args_pickled = 'cloudpickle::' + base64.b64encode(
        cloudpickle.dumps((3, 4))).decode('ascii')

    captured = []
    orig_from_dict = _mock_rh.BaseTask.from_dict
    _mock_rh.BaseTask.from_dict = lambda d: (captured.append(d),
                                              orig_from_dict(d))[1]

    payload = {
        "tasks": [{
            "uid": "task.cp001",
            "function": fn_pickled,
            "args": args_pickled,
            "_pickled_fields": ["function", "args"],
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200

    _mock_rh.BaseTask.from_dict = orig_from_dict
    assert len(captured) == 1
    td = captured[0]
    assert callable(td["function"])
    assert td["function"](3, 4) == 7
    assert td["args"] == (3, 4)
    assert "_pickled_fields" not in td


def test_serialize_task_pickles_unsafe_metadata():
    """A callable nested in ``metadata`` (e.g. asyncflow dependency
    descriptors) must be cloudpickled so the msgpack submit payload stays
    packable; JSON-safe metadata must pass through untouched."""
    import base64
    import cloudpickle
    import msgpack

    from radical.orbit.plugin_rhapsody import RhapsodyClient

    def dep_fn():
        return 42

    td = {
        "uid": "task.md001",
        "function": dep_fn,
        "metadata": {"dependencies": [{"args": (), "function": dep_fn}]},
    }
    RhapsodyClient._serialize_task(td)

    assert set(td["_pickled_fields"]) == {"function", "metadata"}
    assert td["metadata"].startswith("cloudpickle::")
    msgpack.packb({"tasks": [td]}, use_bin_type=True)   # must not raise

    # the endpoint-side decode restores the original structure
    meta = cloudpickle.loads(
        base64.b64decode(td["metadata"][len("cloudpickle::"):]))
    assert meta["dependencies"][0]["function"]() == 42

    # JSON-safe metadata is left alone
    td2 = {"uid": "task.md002", "executable": "/bin/true",
           "metadata": {"note": "plain", "n": 3}}
    RhapsodyClient._serialize_task(td2)
    assert td2["metadata"] == {"note": "plain", "n": 3}
    assert "_pickled_fields" not in td2


@pytest.mark.asyncio
async def test_function_task_import_path():
    """Import-path string 'module:func' must be resolved to a callable."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    captured = []
    orig_from_dict = _mock_rh.BaseTask.from_dict
    _mock_rh.BaseTask.from_dict = lambda d: (captured.append(d),
                                              orig_from_dict(d))[1]

    payload = {
        "tasks": [{
            "uid": "task.ip001",
            "function": "os.path:join",
            "args": ["/tmp", "test"],
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 200

    _mock_rh.BaseTask.from_dict = orig_from_dict
    assert len(captured) == 1
    td = captured[0]
    import os.path
    assert td["function"] is os.path.join


@pytest.mark.asyncio
async def test_function_task_pickled_disabled():
    """Pickled tasks must be rejected when allow_pickled_tasks=False."""
    import cloudpickle
    import base64

    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    session.allow_pickled_tasks = False

    fn_pickled = 'cloudpickle::' + base64.b64encode(
        cloudpickle.dumps(lambda x: x)).decode('ascii')

    payload = {
        "tasks": [{
            "uid": "task.dis001",
            "function": fn_pickled,
            "_pickled_fields": ["function"],
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_function_task_bad_import_path():
    """Invalid import path must return 400."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()

    payload = {
        "tasks": [{
            "uid": "task.bad001",
            "function": "no_such_module:no_func",
        }]
    }
    resp = client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# RhapsodySession direct tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_close():
    session = RhapsodySession("test.001")
    mock_rh_session = MagicMock()
    mock_rh_session.close = AsyncMock()
    session._rh_session = mock_rh_session
    session._telemetry = MagicMock()
    session._telemetry.summary.return_value = {"telemetry": "ok"}

    expected = json.dumps(session._telemetry.summary.return_value, indent=4)

    with patch('radical.orbit.plugin_rhapsody.log') as mock_log:
        result = await session.close()

    assert result == {}
    assert session._active is False
    assert session._telemetry is None
    mock_rh_session.close.assert_called_once()
    mock_log.info.assert_called_once_with(expected)


@pytest.mark.asyncio
async def test_session_close_cleans_up_when_rh_close_raises():
    '''If rhapsody's own close() raises, the session state is still
    cleared and the base-class close still runs; the error propagates.'''
    session = RhapsodySession("test.003")
    mock_rh_session = MagicMock()
    mock_rh_session.close = AsyncMock(side_effect=RuntimeError("stop"))
    session._rh_session = mock_rh_session
    session._tasks = {'t1': {}}

    with pytest.raises(RuntimeError, match="stop"):
        await session.close()

    assert session._rh_session is None
    assert session._tasks == {}
    assert session._active is False


@pytest.mark.asyncio
async def test_session_close_resets_tasks_without_rh_session():
    '''close() clears the task table even when no rhapsody session
    was ever built.'''
    session = RhapsodySession("test.close_no_rh")
    session._tasks = {'t1': {}}

    assert await session.close() == {}
    assert session._tasks == {}


@pytest.mark.parametrize('failed_start, warned', [(True, False),
                                                  (False, True)])
@pytest.mark.asyncio
async def test_unwind_telemetry_stops_and_detaches(orbit_caplog,
                                                   failed_start, warned):
    '''The manager is stopped best effort (a failing stop is tolerated)
    and detached.  The stop failure is a warning on a normal close and
    expected (debug only) after a failed start.'''
    session = RhapsodySession("test.unwind")
    tm = MagicMock()
    tm.stop = AsyncMock(side_effect=TypeError("stop broke"))
    rh_session = MagicMock()
    rh_session._telemetry = tm
    rh_session._state_manager._telemetry_observer = tm._on_task_state_change

    await session._unwind_telemetry(rh_session, failed_start=failed_start)

    tm.stop.assert_awaited_once()
    assert rh_session._telemetry is None
    assert rh_session._state_manager._telemetry_observer is None
    assert any('telemetry stop failed' in r.getMessage()
               for r in orbit_caplog.records) is warned


@pytest.mark.asyncio
async def test_session_close_survives_telemetry_failure():
    '''A failing telemetry summary must not block the rhapsody teardown.'''
    session = RhapsodySession("test.telem_summary_fails")
    mock_rh_session = MagicMock()
    mock_rh_session.close = AsyncMock()
    mock_rh_session._telemetry = None       # nothing for the unwind to stop
    session._rh_session = mock_rh_session
    telemetry = MagicMock()
    telemetry.summary.side_effect = RuntimeError("otel broke")
    session._telemetry = telemetry

    # patched logger: isEnabledFor(INFO) is truthy, so summary() runs
    with patch('radical.orbit.plugin_rhapsody.log') as mock_log:
        result = await session.close()

    telemetry.summary.assert_called_once()
    mock_log.warning.assert_called_once()
    assert result == {}
    assert session._telemetry is None
    assert session._rh_session is None
    mock_rh_session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# RhapsodyClient — HTTP wrapper tests (Tier 2)
# ---------------------------------------------------------------------------

def _make_rhapsody_client(json_resp=None, status_code=200):
    """Return a RhapsodyClient backed by a mock httpx.Client."""
    if json_resp is None:
        json_resp = {}
    mock_resp = MagicMock()
    mock_resp.is_error = (status_code >= 400)
    mock_resp.status_code = status_code
    mock_resp.json = MagicMock(return_value=json_resp)
    mock_http = MagicMock()
    mock_http.get = MagicMock(return_value=mock_resp)
    mock_http.post = MagicMock(return_value=mock_resp)
    client = RhapsodyClient(mock_http, "/rhapsody")
    client._sid = "sid-rh"
    return client


def test_rhapsody_client_submit_tasks():
    tasks = [{"executable": "/bin/echo", "arguments": ["hi"]}]
    client = _make_rhapsody_client([{"uid": "t.001", "state": "SUBMITTED"}])
    result = client.submit_tasks(tasks)
    assert isinstance(result, list)
    mock_call = client._http.post.call_args
    # Payload may be msgpack (data=) or JSON (json=) depending on availability
    if "json" in mock_call[1]:
        assert "tasks" in mock_call[1]["json"]
    else:
        import msgpack
        payload = msgpack.unpackb(mock_call[1]["data"], raw=False)
        assert "tasks" in payload


def test_rhapsody_client_wait_tasks():
    client = _make_rhapsody_client([{"uid": "t.001", "state": "DONE"}])
    client._submitted.add("t.001")
    result = client.wait_tasks(["t.001"])
    assert isinstance(result, list)
    mock_call = client._http.post.call_args
    assert "uids" in mock_call[1]["json"]
    assert "timeout" not in mock_call[1]["json"]


def test_rhapsody_client_wait_tasks_with_timeout():
    client = _make_rhapsody_client([])
    client._submitted.add("t.001")
    client.wait_tasks(["t.001"], timeout=30.0)
    mock_call = client._http.post.call_args
    assert mock_call[1]["json"].get("timeout") == 30.0


def test_rhapsody_client_wait_tasks_accepts_task_objects():
    """wait_tasks() accepts task objects/dicts, not just UID strings."""
    client = _make_rhapsody_client([{"uid": "t.001", "state": "DONE"}])
    client._submitted.add("t.001")
    result = client.wait_tasks([{"uid": "t.001"}])     # task dict, not str
    assert isinstance(result, list)
    assert client._http.post.call_args[1]["json"]["uids"] == ["t.001"]


def test_rhapsody_client_wait_tasks_unknown_uid_raises():
    """Waiting on a UID never submitted fails fast instead of hanging."""
    client = _make_rhapsody_client([])
    with pytest.raises(ValueError, match="unknown task UID"):
        client.wait_tasks(["t.never"])


def test_rhapsody_client_wait_tasks_bad_type_raises():
    """A non-UID, non-task argument raises TypeError instead of hanging."""
    client = _make_rhapsody_client([])
    with pytest.raises(TypeError, match="task UID"):
        client.wait_tasks([12345])


def test_rhapsody_client_cancel_task():
    client = _make_rhapsody_client({"uid": "t.001", "state": "CANCELED"})
    result = client.cancel_task("t.001")
    client._http.post.assert_called_once()
    assert "cancel" in client._http.post.call_args[0][0]


def test_rhapsody_client_list_tasks():
    client = _make_rhapsody_client({"tasks": []})
    result = client.list_tasks()
    assert "tasks" in result
    client._http.get.assert_called_once()


def test_rhapsody_client_get_task():
    task = {"uid": "t.001", "state": "DONE", "stdout": "hi\n"}
    client = _make_rhapsody_client(task)
    result = client.get_task("t.001")
    assert result["uid"] == "t.001"
    client._http.get.assert_called_once()


def test_rhapsody_client_cancel_all_tasks():
    client = _make_rhapsody_client({"canceled": 3})
    result = client.cancel_all_tasks()
    assert result["canceled"] == 3
    client._http.post.assert_called_once()


def test_rhapsody_client_no_session_raises():
    """All session-requiring methods must raise if no session is active."""
    mock_http = MagicMock()
    client = RhapsodyClient(mock_http, "/rhapsody")
    # sid is None by default

    with pytest.raises(RuntimeError, match="session"):
        client.submit_tasks([])

    with pytest.raises(RuntimeError, match="session"):
        client.wait_tasks(["t.001"])

    with pytest.raises(RuntimeError, match="session"):
        client.cancel_task("t.001")

    with pytest.raises(RuntimeError, match="session"):
        client.cancel_all_tasks()


# ---------------------------------------------------------------------------
# Phase 4 — cancel_all_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_all_tasks():
    """cancel_all_tasks must cancel non-terminal tasks and return count."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    mock_backend = MagicMock()
    mock_backend.cancel_task = AsyncMock()
    session._rh_session.backends = {'concurrent': mock_backend}

    # Submit 3 tasks
    payload = {
        "tasks": [
            {"executable": "/bin/echo", "uid": f"task.ca{i:03d}",
             "backend": "concurrent"}
            for i in range(3)
        ]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)

    # Mark one as DONE so it should be skipped
    session._tasks["task.ca001"].state = "DONE"

    resp = client.post(f"{plugin.namespace}/cancel_all/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["canceled"] == 2


@pytest.mark.asyncio
async def test_cancel_all_skips_terminal():
    """cancel_all_tasks with all tasks terminal should cancel 0."""
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.submit_tasks = AsyncMock()
    session._rh_session.backends = {}

    payload = {
        "tasks": [{"executable": "/bin/true", "uid": "task.term001"}]
    }
    client.post(f"{plugin.namespace}/submit/{sid}", json=payload)
    session._tasks["task.term001"].state = "DONE"

    resp = client.post(f"{plugin.namespace}/cancel_all/{sid}")
    assert resp.status_code == 200
    assert resp.json()["canceled"] == 0


# ---------------------------------------------------------------------------
# Regression: RhapsodySession.initialize survives older rhapsody libs that
# don't expose `Session.start_telemetry`, and installs without the
# rhapsody `[telemetry]` extra.
# ---------------------------------------------------------------------------
#
# Bug surfaced during the local e2e smoke (memory/project_broker_dispatcher.md):
# the rhapsody plugin called `self._rh_session.start_telemetry(...)`
# unconditionally; older rhapsody installs raise AttributeError.  Fix
# guards the call with `getattr(session, 'start_telemetry', None)`.

@pytest.mark.parametrize('has_method, otel', [
    (False, True),     # older rhapsody: no Session.start_telemetry
    (True,  False),    # [telemetry] extra (opentelemetry-sdk) missing
])
@pytest.mark.asyncio
async def test_initialize_without_telemetry(monkeypatch, has_method, otel):
    '''Session init must succeed without telemetry: when the rhapsody
    Session has no ``start_telemetry`` method, or when the ``[telemetry]``
    extra is missing (then ``start_telemetry`` must not even be called:
    rhapsody 0.4 wires a half-built manager in before it fails).'''
    from radical.orbit import plugin_rhapsody as prh

    fake_backend = MagicMock(spec=[])  # no __await__, no register_callback
    fake_session = MagicMock(spec=['start_telemetry'] if has_method else [])
    if has_method:
        fake_session.start_telemetry = AsyncMock()

    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=fake_backend))
    monkeypatch.setattr(prh.rh, 'Session',
                        MagicMock(return_value=fake_session))
    monkeypatch.setattr(prh, '_telemetry_available', lambda: otel)

    sess = RhapsodySession('sess.no_telem', backend_names=['fake'])
    # _REAL_INITIALIZE was captured at import time, before the autouse
    # stub fixture replaced RhapsodySession.initialize.  Bind it
    # manually to bypass the per-test stub.
    await _REAL_INITIALIZE(sess)

    if has_method:
        fake_session.start_telemetry.assert_not_awaited()
    assert sess._telemetry is None
    assert sess._rh_session is fake_session
    assert sess._init_error is None
    assert sess._init_ready.is_set()


@pytest.fixture
def orbit_caplog(caplog):
    '''``caplog`` capturing WARNING+ from the ``radical.orbit`` logger,
    which may have propagate=False (see test_plugin_psij.py).'''
    import logging as _logging
    re_log = _logging.getLogger('radical.orbit')
    re_log.addHandler(caplog.handler)
    caplog.set_level(_logging.WARNING, logger='radical.orbit')
    yield caplog
    re_log.removeHandler(caplog.handler)


@pytest.fixture
def _fresh_telemetry_switch():
    '''Clear the per-process telemetry switch cache around a test.'''
    from radical.orbit import plugin_rhapsody as prh
    prh._resolve_telemetry.cache_clear()
    yield prh
    prh._resolve_telemetry.cache_clear()


@pytest.mark.parametrize('env, on', [
    (None, True), ('1', True), ('on', True), ('YES', True),
    ('0', False), ('off', False), ('False', False), ('no', False),
])
def test_telemetry_switch(monkeypatch, _fresh_telemetry_switch, env, on):
    '''``$RADICAL_ORBIT_RHAPSODY_TELEMETRY`` switches telemetry off;
    unset keeps it on.'''
    prh = _fresh_telemetry_switch
    if env is None:
        monkeypatch.delenv(prh.ENV_TELEMETRY, raising=False)
    else:
        monkeypatch.setenv(prh.ENV_TELEMETRY, env)

    assert prh._resolve_telemetry() is on


def test_telemetry_switch_invalid_warns_once(monkeypatch, orbit_caplog,
                                             _fresh_telemetry_switch):
    '''An unrecognised switch value warns once and keeps telemetry on.'''
    prh = _fresh_telemetry_switch

    monkeypatch.setenv(prh.ENV_TELEMETRY, 'maybe')
    assert prh._resolve_telemetry() is True
    assert prh._resolve_telemetry() is True
    # count distinct records: with propagation on, caplog sees each twice
    assert len({id(r) for r in orbit_caplog.records
                if prh.ENV_TELEMETRY in r.getMessage()}) == 1


@pytest.mark.asyncio
async def test_initialize_telemetry_switched_off(monkeypatch):
    '''A session with telemetry switched off never starts it.'''
    from radical.orbit import plugin_rhapsody as prh

    fake_session = MagicMock(spec=['start_telemetry'])
    fake_session.start_telemetry = AsyncMock()
    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=MagicMock(spec=[])))
    monkeypatch.setattr(prh.rh, 'Session',
                        MagicMock(return_value=fake_session))
    monkeypatch.setattr(prh, '_telemetry_available', lambda: True)
    monkeypatch.setattr(prh, '_resolve_telemetry', lambda: False)

    sess = RhapsodySession('sess.telem_off', backend_names=['fake'])
    await _REAL_INITIALIZE(sess)

    fake_session.start_telemetry.assert_not_awaited()
    assert sess._telemetry is None
    assert sess._init_error is None


@pytest.mark.asyncio
async def test_initialize_telemetry_start_failure(monkeypatch, orbit_caplog):
    '''A telemetry start failure (e.g. an unwritable checkpoint dir)
    disables telemetry instead of failing the session.'''
    from radical.orbit import plugin_rhapsody as prh

    fake_session = MagicMock(spec=['start_telemetry'])
    fake_session.start_telemetry = AsyncMock(
        side_effect=PermissionError("telemetry-output"))
    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=MagicMock(spec=[])))
    monkeypatch.setattr(prh.rh, 'Session',
                        MagicMock(return_value=fake_session))
    monkeypatch.setattr(prh, '_telemetry_available', lambda: True)
    monkeypatch.setattr(prh, '_resolve_telemetry', lambda: True)

    sess = RhapsodySession('sess.telem_fail', backend_names=['fake'])
    await _REAL_INITIALIZE(sess)

    assert any('telemetry start failed' in r.getMessage()
               for r in orbit_caplog.records)
    fake_session.start_telemetry.assert_awaited_once()
    assert sess._telemetry is None
    assert sess._init_error is None
    assert sess._init_ready.is_set()


@pytest.mark.asyncio
async def test_initialize_cancelled_during_telemetry_start(monkeypatch):
    '''A cancel while start_telemetry() is pending unwinds the manager,
    unblocks init waiters and re-raises the cancellation.'''
    from radical.orbit import plugin_rhapsody as prh

    in_start = asyncio.Event()

    async def _hang(**kwargs):
        in_start.set()
        await asyncio.Event().wait()

    fake_session = MagicMock(spec=['start_telemetry'])
    fake_session.start_telemetry = AsyncMock(side_effect=_hang)
    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=MagicMock(spec=[])))
    monkeypatch.setattr(prh.rh, 'Session',
                        MagicMock(return_value=fake_session))
    monkeypatch.setattr(prh, '_telemetry_available', lambda: True)
    monkeypatch.setattr(prh, '_resolve_telemetry', lambda: True)

    sess = RhapsodySession('sess.telem_cancel', backend_names=['fake'])
    sess._unwind_telemetry = AsyncMock()
    init = asyncio.ensure_future(_REAL_INITIALIZE(sess))
    await in_start.wait()
    init.cancel()

    with pytest.raises(asyncio.CancelledError):
        await init

    sess._unwind_telemetry.assert_awaited_once_with(fake_session,
                                                    failed_start=True)
    assert sess._init_ready.is_set()
    assert sess._init_error is not None


@pytest.fixture
def real_rhapsody(monkeypatch, tmp_path):
    '''The real rhapsody package (this module mocks it) with telemetry
    forced on, cwd in *tmp_path*, and ``get_backend`` returning a fake
    backend.  Yields ``(rhapsody module, backend)``.'''
    import importlib
    import sys
    from radical.orbit import plugin_rhapsody as prh

    # re-import the real package (monkeypatch restores the mock afterwards)
    monkeypatch.delitem(sys.modules, 'rhapsody', raising=False)
    try:
        real = importlib.import_module('rhapsody')
    except ImportError:
        pytest.skip('needs rhapsody')
    if not hasattr(real.Session, 'start_telemetry'):
        pytest.skip('needs rhapsody with Session.start_telemetry')
    pytest.importorskip('opentelemetry.sdk')
    monkeypatch.setattr(prh, 'rh', real)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(prh, '_resolve_telemetry', lambda: True)
    monkeypatch.setattr(prh, '_telemetry_available', lambda: True)

    class _FakeBackend:
        name = 'fake'

        def __init__(self):
            self.attached_to = []
            self.shutdown    = AsyncMock()

        def register_callback(self, cb):
            pass

    backend = _FakeBackend()
    monkeypatch.setattr(real, 'get_backend', lambda name: backend)
    yield real, backend


@pytest.mark.asyncio
async def test_telemetry_stop_failure_does_not_leak_backends(
        real_rhapsody, monkeypatch):
    '''With a real rhapsody Session and running telemetry, a raising
    telemetry stop() must not keep rhapsody's close() from shutting the
    backends down.'''
    from rhapsody.telemetry.manager import TelemetryManager
    _, backend = real_rhapsody

    sess = RhapsodySession('sess.telem_stop', backend_names=['fake'])
    await _REAL_INITIALIZE(sess)
    assert sess._telemetry is not None

    monkeypatch.setattr(TelemetryManager, 'stop',
                        AsyncMock(side_effect=RuntimeError("stop broke")))
    await sess.close()

    backend.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_telemetry_start_does_not_leak_backends(
        real_rhapsody, tmp_path):
    '''With a real rhapsody Session, a telemetry start that fails half
    way (checkpoint dir cannot be created) must not keep rhapsody's
    close() from shutting the backends down.'''
    _, backend = real_rhapsody
    # 'telemetry-output' exists as a file, so the checkpoint mkdir fails
    (tmp_path / 'telemetry-output').write_text('')

    sess = RhapsodySession('sess.telem_real', backend_names=['fake'])
    await _REAL_INITIALIZE(sess)

    assert sess._init_error is None
    assert sess._telemetry is None
    rh_session = sess._rh_session
    assert rh_session._telemetry is None
    assert rh_session._state_manager._telemetry_observer is None

    await sess.close()

    backend.shutdown.assert_awaited_once()


def test_telemetry_available_without_otel(monkeypatch, orbit_caplog):
    '''A missing opentelemetry package reads as unavailable (find_spec
    raises for a dotted name whose parent is absent) and warns once.'''
    from radical.orbit import plugin_rhapsody as prh

    def _no_parent(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(prh.importlib.util, 'find_spec', _no_parent)
    prh._telemetry_available.cache_clear()
    try:
        assert prh._telemetry_available() is False
        assert prh._telemetry_available() is False
        assert len({id(r) for r in orbit_caplog.records
                    if 'telemetry disabled' in r.getMessage()}) == 1
    finally:
        prh._telemetry_available.cache_clear()


# ---------------------------------------------------------------------------
# Notification coalescing window ($RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW)
# ---------------------------------------------------------------------------

def test_notify_window_default(monkeypatch):
    '''Unset env var: plugin and its sessions keep the 0.25s default.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.delenv(prh.ENV_NOTIFY_WINDOW, raising=False)
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    assert plugin._notify_window == prh.NOTIFY_BATCH_WINDOW
    assert plugin._sessions[sid]._notify_window == prh.NOTIFY_BATCH_WINDOW


def test_notify_window_from_env(monkeypatch):
    '''The endpoint operator's window reaches every session of the plugin.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.setenv(prh.ENV_NOTIFY_WINDOW, '0.05')
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    assert plugin._notify_window == 0.05
    assert plugin._sessions[sid]._notify_window == 0.05


@pytest.mark.parametrize('raw', ['abc', '-1', '0.1s', 'nan', 'inf', '-inf'])
def test_notify_window_invalid_falls_back(monkeypatch, orbit_caplog, raw):
    '''A non-numeric, negative or non-finite window warns and keeps the
    default — ``nan`` in particular must not sneak past as a ~0 window.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.setenv(prh.ENV_NOTIFY_WINDOW, raw)
    _, plugin, _ = _make_plugin()

    assert plugin._notify_window == prh.NOTIFY_BATCH_WINDOW
    assert prh.ENV_NOTIFY_WINDOW in orbit_caplog.text


def test_notify_window_zero_flushes_immediately(monkeypatch):
    '''A window of 0 delivers each completion inline — no delayed flush.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.setenv(prh.ENV_NOTIFY_WINDOW, '0')
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    assert session._notify_window == 0

    plugin._dispatch_notify = MagicMock()
    payload = {'uid': 'task.nw001', 'state': 'DONE'}
    session._queue_notification(payload)

    plugin._dispatch_notify.assert_called_once_with('task_status', payload)
    assert session._notify_buf == []
    assert not session._flush_scheduled


def test_notify_window_defers_flush(monkeypatch):
    '''A non-zero window buffers the completion and schedules the delayed
    flush with exactly that window — not with the module default.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.setenv(prh.ENV_NOTIFY_WINDOW, '0.05')
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    plugin._dispatch_notify = MagicMock()
    # _schedule_flush is stubbed: it would post the delayed flush onto the
    # TestClient's event loop, which is already closed by now.
    with patch.object(session, '_schedule_flush') as sched:
        session._queue_notification({'uid': 'task.nw002', 'state': 'DONE'})

    sched.assert_called_once_with(delay=0.05)
    plugin._dispatch_notify.assert_not_called()
    assert len(session._notify_buf) == 1
    assert session._flush_scheduled


def _big_payload(uid, nbytes=200):
    """A completion carrying an encoded return value of roughly *nbytes*."""
    return {'uid': uid, 'state': 'DONE',
            'return_value': 'cloudpickle::' + 'x' * nbytes,
            '_return_value_encoding': 'cloudpickle'}


def test_notification_flush_splits_on_byte_budget():
    '''Completions carrying encoded return values must be split across
    frames — one frame past the transport cap is dropped and would strand
    every waiter on those tasks.'''
    from radical.orbit import plugin_rhapsody as prh

    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)
    session = plugin._sessions[sid]
    session._notify_batch_bytes = 512
    plugin._dispatch_notify = MagicMock()

    payloads = [_big_payload(f'task.split{i}') for i in range(5)]
    session._notify_buf   = list(payloads)
    session._notify_bytes = sum(prh._payload_size(p) for p in payloads)

    session._flush_notifications()

    calls = plugin._dispatch_notify.call_args_list
    assert len(calls) > 1                        # actually split

    delivered = []
    for topic, data in (c[0] for c in calls):
        tasks = data['tasks'] if topic == 'task_status_batch' else [data]
        assert sum(prh._payload_size(t) for t in tasks) <= 512
        delivered.extend(tasks)

    # every completion is delivered exactly once, in order
    assert [t['uid'] for t in delivered] == [p['uid'] for p in payloads]
    assert session._notify_buf   == []
    assert session._notify_bytes == 0


def test_queue_notification_flushes_on_byte_budget():
    '''The byte budget also bounds the *buffer*: a big completion flushes
    at once instead of waiting out the coalescing window.'''
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)
    session = plugin._sessions[sid]
    session._notify_window      = 10        # would otherwise defer for 10s
    session._notify_batch_bytes = 256
    plugin._dispatch_notify = MagicMock()

    session._queue_notification(_big_payload('task.budget'))

    plugin._dispatch_notify.assert_called_once()
    assert session._notify_buf   == []
    assert session._notify_bytes == 0
    assert not session._flush_scheduled


def test_frame_budgets_follow_host_runtime_cap():
    '''The budgets track the *serving runtime's* frame cap, not the protocol
    default: a runtime tuned below FRAME_CAP must shrink both, else the
    transport drops frames the plugin considers legal.'''
    from radical.orbit import plugin_rhapsody as prh

    app = FastAPI()
    app.state.endpoint_service = MagicMock(_frame_cap=64 * 1024)
    plugin = PluginRhapsody(app)

    assert plugin._frame_cap          == 64 * 1024
    assert plugin._return_value_limit == 32 * 1024
    assert plugin._notify_batch_bytes == 32 * 1024
    assert plugin._output_notify_limit == 8 * 1024

    # every session it creates inherits them
    session = plugin._create_session('sid.cap')
    assert session._return_value_limit == 32 * 1024
    assert session._notify_batch_bytes == 32 * 1024
    assert session._output_notify_limit == 8 * 1024

    # a directly built session keeps the module defaults
    assert RhapsodySession('sid.plain')._return_value_limit \
        == prh.RETURN_VALUE_LIMIT


def test_frame_budgets_fall_back_to_protocol_cap():
    '''Hosted without a runtime handle (broker plugin host, tests), the
    budgets fall back to the protocol default.'''
    from radical.orbit import plugin_rhapsody as prh

    _, plugin, _ = _make_plugin()             # no endpoint_service in state

    assert plugin._frame_cap          == prh.FRAME_CAP
    assert plugin._return_value_limit == prh.RETURN_VALUE_LIMIT
    assert plugin._notify_batch_bytes == prh.NOTIFY_BATCH_BYTES
    assert plugin._output_notify_limit == prh.OUTPUT_NOTIFY_LIMIT


# ---------------------------------------------------------------------------
# Data backends
# ---------------------------------------------------------------------------

class _FakeEndpoint:
    def __init__(self, desc):
        self._desc = desc

    def serialize(self):
        return self._desc


class _FakeDataBackend:
    def __init__(self, name, **kwargs):
        self.name       = name
        self.kwargs     = kwargs
        self.state      = MagicMock()
        self.state.name = 'CREATED'
        self.endpoints  = []
        self.shutdown   = AsyncMock()
        self.gate       = None      # an asyncio.Event to hold start() open

    async def start(self):
        if self.gate is not None:
            await self.gate.wait()
        self.state.name = 'READY'
        self.endpoints  = [_FakeEndpoint('node1:6379')]
        return self


def _data_session(plugin, client):
    sid = _register(client, plugin)
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    session._rh_session.backends = {'concurrent': MagicMock()}
    session._rh_session.add_backend = MagicMock(
        side_effect=lambda b: session._rh_session.backends.__setitem__(b.name, b))
    return sid, session


def _fake_factory(created):
    def _factory(kind, name=None, **kw):
        created[name] = _FakeDataBackend(name, **kw)
        return created[name]
    return _factory


def test_data_start_endpoints_shutdown():
    _, plugin, client = _make_plugin()
    sid, session = _data_session(plugin, client)
    created = {}

    with patch.object(_mock_rh, 'get_data_backend',
                      side_effect=_fake_factory(created)):
        resp = client.post(f"{plugin.namespace}/data_start/{sid}",
                           json={"kind": "redis", "kwargs": {"port": 6379}})
    assert resp.status_code == 200
    assert resp.json() == {"name": "redis_data", "kind": "redis",
                           "state": "READY", "endpoints": ["node1:6379"]}
    assert created['redis_data'].kwargs == {"port": 6379}
    assert session._rh_session.backends['redis_data'] is created['redis_data']

    resp = client.get(f"{plugin.namespace}/data_endpoints/{sid}")
    assert resp.status_code == 200
    assert resp.json()['backends']['redis_data']['endpoints'] == \
        ["node1:6379"]

    resp = client.post(f"{plugin.namespace}/data_shutdown/{sid}/redis_data")
    assert resp.status_code == 200
    created['redis_data'].shutdown.assert_awaited_once()
    assert 'redis_data' not in session._rh_session.backends
    resp = client.get(f"{plugin.namespace}/data_endpoints/{sid}")
    assert resp.json() == {"backends": {}}


def test_data_start_name_collisions():
    _, plugin, client = _make_plugin()
    sid, _ = _data_session(plugin, client)
    url = f"{plugin.namespace}/data_start/{sid}"

    with patch.object(_mock_rh, 'get_data_backend',
                      side_effect=_fake_factory({})):
        assert client.post(url, json={"kind": "redis"}).status_code == 200
        assert client.post(url, json={"kind": "redis"}).status_code == 409
        assert client.post(url, json={"kind": "redis",
                                      "name": "r2"}).status_code == 200
        # an execution backend of the session is never shadowed
        assert client.post(url, json={"kind": "redis",
                                      "name": "concurrent"}).status_code == 409


def test_data_start_bad_requests():
    _, plugin, client = _make_plugin()
    sid, session = _data_session(plugin, client)
    url = f"{plugin.namespace}/data_start/{sid}"

    with patch.object(_mock_rh, 'get_data_backend',
                      side_effect=ValueError("Data backend 'x' not found")):
        assert client.post(url, json={"kind": "x"}).status_code == 400
    assert client.post(url, json={}).status_code == 400
    assert client.post(url, json={"kind": "redis",
                                  "kwargs": [1]}).status_code == 400
    assert client.post(url, json={"kind": "redis",
                                  "name": "a/b"}).status_code == 400
    assert client.post(url, json={"kind": "redis",
                                  "name": 5}).status_code == 400
    assert session._data == {}


def test_data_start_failure_releases_name():
    _, plugin, client = _make_plugin()
    sid, session = _data_session(plugin, client)
    backend = _FakeDataBackend('redis_data')
    backend.start = AsyncMock(side_effect=RuntimeError("no redis-server"))

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        resp = client.post(f"{plugin.namespace}/data_start/{sid}",
                           json={"kind": "redis"})
    assert resp.status_code == 500
    assert session._data == {}
    backend.shutdown.assert_awaited_once()
    session._rh_session.add_backend.assert_not_called()


@pytest.mark.asyncio
async def test_data_start_cancelled_cleans_up():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    session._rh_session = MagicMock()
    session._rh_session.backends = {}
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        task = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        assert 'redis_data' in session._data
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert session._data == {}
    backend.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_data_shutdown_during_start():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    session._rh_session = MagicMock()
    session._rh_session.backends = {}
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        task = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        shut = asyncio.ensure_future(session.data_shutdown('redis_data'))
        await asyncio.sleep(0)
        backend.shutdown.assert_not_awaited()   # not while start runs
        backend.gate.set()
        with pytest.raises(HTTPException) as exc:
            await task
        await shut
    assert exc.value.status_code == 409
    session._rh_session.add_backend.assert_not_called()
    backend.shutdown.assert_awaited_once()      # once, after start
    assert session._data == {}


@pytest.mark.asyncio
async def test_close_stops_data_backend_still_starting():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    session._rh_session = MagicMock()
    session._rh_session.backends = {}
    session._rh_session.close = AsyncMock()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        task  = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        close = asyncio.ensure_future(session.close())
        await asyncio.sleep(0)
        assert not close.done()             # waits for the start in flight
        backend.gate.set()
        with pytest.raises(HTTPException) as exc:
            await task                      # gives up: session is closing
        assert exc.value.status_code == 409
        await close
    backend.shutdown.assert_awaited()
    assert session._rh_session is None
    assert not session.is_active


@pytest.mark.asyncio
async def test_close_drain_is_bounded():
    from radical.orbit import plugin_rhapsody as prh
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()          # start never finishes by itself

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend), \
         patch.object(prh, 'CLOSE_DRAIN_TIMEOUT', 0.05):
        task = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        await session.close()               # returns after the drain timeout
        assert not session.is_active
        backend.shutdown.assert_not_awaited()   # start still runs ...
        backend.gate.set()
        with pytest.raises(HTTPException):
            await task
    backend.shutdown.assert_awaited_once()      # ... data_start stops it


@pytest.mark.asyncio
async def test_close_detaches_before_closing_rhapsody_session():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    rh_session = MagicMock()
    rh_session.backends = {}
    seen = {}

    async def _close():
        # an in-flight data route resuming now must see a detached session
        seen['rh_session'] = session._rh_session
        seen['data'] = dict(session._data)

    rh_session.close = AsyncMock(side_effect=_close)
    session._rh_session = rh_session
    session._data = {'redis_data': _DataEntry(_FakeDataBackend('redis_data'), 'redis')}
    await session.close()
    assert seen == {'rh_session': None, 'data': {}}


@pytest.mark.asyncio
async def test_data_start_cleanup_failure_keeps_original_error():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    session._rh_session = MagicMock()
    session._rh_session.backends = {}
    backend = _FakeDataBackend('redis_data')
    backend.start = AsyncMock(side_effect=ValueError("bad port"))
    backend.shutdown = AsyncMock(side_effect=RuntimeError("cleanup broke"))

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        with pytest.raises(ValueError, match="bad port"):
            await session.data_start('redis')


def _ready_session():
    session = RhapsodySession('s1', backend_names=['concurrent'])
    session._init_ready.set()
    session._rh_session = MagicMock()
    session._rh_session.backends = {}
    session._rh_session.close = AsyncMock()
    return session


@pytest.mark.asyncio
async def test_requests_during_close_see_closed_session():
    session = _ready_session()
    session._rh_session = None              # close() detached it
    for call in (lambda: session.data_start('redis'),
                 lambda: session.cancel_task('t1'),
                 lambda: session.data_shutdown('redis_data'),
                 lambda: session.list_tasks(),
                 lambda: session.data_endpoints()):
        with pytest.raises(HTTPException) as exc:
            await call()
        assert exc.value.status_code == 410
        assert 'closing' in exc.value.detail


@pytest.mark.asyncio
async def test_closing_flag_turns_requests_away():
    session = _ready_session()
    session._closing = True                 # close() started, not detached
    with pytest.raises(HTTPException) as exc:
        await session.submit_tasks([{'uid': 't1'}])
    assert exc.value.status_code == 410


@pytest.mark.asyncio
async def test_cancelled_close_still_tears_down():
    session = _ready_session()
    stop    = asyncio.Event()
    slow, fast = _FakeDataBackend('slow'), _FakeDataBackend('fast')

    async def _slow_shutdown():
        await stop.wait()
    slow.shutdown = AsyncMock(side_effect=_slow_shutdown)
    session._data = {'slow': _DataEntry(slow, 'redis'), 'fast': _DataEntry(fast, 'redis')}

    close = asyncio.ensure_future(session.close())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    stop.set()
    await session._close_task               # teardown kept going
    slow.shutdown.assert_awaited_once()
    fast.shutdown.assert_awaited_once()
    assert not session.is_active
    await session.close()                   # idempotent


@pytest.mark.asyncio
async def test_close_waits_for_data_shutdown_in_flight():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    stop    = asyncio.Event()

    async def _slow_shutdown():
        await stop.wait()
    backend.shutdown = AsyncMock(side_effect=_slow_shutdown)
    session._data = {'redis_data': _DataEntry(backend, 'redis')}
    session._rh_session.backends = {'redis_data': backend}

    shut  = asyncio.ensure_future(session.data_shutdown('redis_data'))
    await asyncio.sleep(0)
    close = asyncio.ensure_future(session.close())
    await asyncio.sleep(0)
    assert not close.done()
    stop.set()
    await shut
    await close
    backend.shutdown.assert_awaited_once()  # no second, concurrent shutdown


@pytest.mark.asyncio
async def test_failed_shutdown_after_start_keeps_store_tracked():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()
    backend.shutdown = AsyncMock(side_effect=[RuntimeError('will not die'),
                                              None])

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        start = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        shut  = asyncio.ensure_future(session.data_shutdown('redis_data'))
        await asyncio.sleep(0)
        backend.gate.set()
        with pytest.raises(HTTPException) as exc:
            await start                     # never registered
        assert exc.value.status_code == 409
        with pytest.raises(RuntimeError, match='will not die'):
            await shut
    session._rh_session.add_backend.assert_not_called()
    assert session._data['redis_data'].doomed  # tracked for a retry
    await session.data_shutdown('redis_data')
    assert session._data == {}
    assert backend.shutdown.await_count == 2


@pytest.mark.asyncio
async def test_data_start_add_backend_failure_stops_store():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')

    def _add(b):
        session._rh_session.backends[b.name] = b     # registered, then fails
        raise AttributeError('old')
    session._rh_session.add_backend = MagicMock(side_effect=_add)

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        with pytest.raises(AttributeError):
            await session.data_start('redis')
    backend.shutdown.assert_awaited_once()
    assert 'redis_data' not in session._data
    assert 'redis_data' not in session._rh_session.backends


@pytest.mark.asyncio
async def test_data_shutdown_failure_keeps_backend_tracked():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.shutdown = AsyncMock(side_effect=RuntimeError('will not die'))
    session._data = {'redis_data': _DataEntry(backend, 'redis')}
    session._rh_session.backends = {'redis_data': backend}

    with pytest.raises(RuntimeError, match='will not die'):
        await session.data_shutdown('redis_data')
    # still tracked and its name reserved; rhapsody's close no longer sees it
    with pytest.raises(HTTPException):
        await session.data_start('redis')
    assert session._data['redis_data'].backend is backend
    assert 'redis_data' not in session._rh_session.backends


@pytest.mark.asyncio
async def test_data_start_gives_up_on_store_shut_down_meanwhile():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()
    stop     = asyncio.Event()

    async def _slow_shutdown():
        await stop.wait()
    backend.shutdown = AsyncMock(side_effect=_slow_shutdown)

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        start = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        shut  = asyncio.ensure_future(session.data_shutdown('redis_data'))
        await asyncio.sleep(0)
        backend.gate.set()                  # start finishes first ...
        await asyncio.sleep(0)
        stop.set()                          # ... then the shutdown
        with pytest.raises(HTTPException) as exc:
            await start
        assert exc.value.status_code == 409
        await shut
    session._rh_session.add_backend.assert_not_called()
    assert 'redis_data' not in session._data


@pytest.mark.asyncio
async def test_cancelled_data_shutdown_still_cleans_up():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    stop    = asyncio.Event()

    async def _slow_shutdown():
        await stop.wait()
    backend.shutdown = AsyncMock(side_effect=_slow_shutdown)
    session._data = {'redis_data': _DataEntry(backend, 'redis')}
    session._rh_session.backends = {'redis_data': backend}

    shut = asyncio.ensure_future(session.data_shutdown('redis_data'))
    await asyncio.sleep(0)
    shut.cancel()
    with pytest.raises(asyncio.CancelledError):
        await shut
    assert 'redis_data' in session._data          # still going down
    stop.set()
    for _ in range(5):                      # shutdown -> settle -> book-keeping
        await asyncio.sleep(0)
    assert 'redis_data' not in session._data


@pytest.mark.asyncio
async def test_close_stops_only_backends_rhapsody_did_not_close():
    session = _ready_session()
    owned, pending = _FakeDataBackend('owned'), _FakeDataBackend('pending')
    session._rh_session.backends = {'owned': owned}
    session._data = {'owned': _DataEntry(owned, 'redis'), 'pending': _DataEntry(pending, 'redis')}
    await session.close()
    owned.shutdown.assert_not_awaited()
    pending.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_stops_all_backends_if_rhapsody_close_fails():
    session = _ready_session()
    owned = _FakeDataBackend('owned')
    session._rh_session.backends = {'owned': owned}
    session._rh_session.close = AsyncMock(side_effect=RuntimeError('boom'))
    session._data = {'owned': _DataEntry(owned, 'redis')}
    with pytest.raises(RuntimeError, match='boom'):
        await session.close()
    owned.shutdown.assert_awaited_once()
    await session.close()                   # already torn down: no re-raise


@pytest.mark.asyncio
async def test_concurrent_stoppers_share_one_shutdown():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()
    stop    = asyncio.Event()

    async def _slow_shutdown():
        await stop.wait()
    backend.shutdown = AsyncMock(side_effect=_slow_shutdown)

    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        start = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        shut1 = asyncio.ensure_future(session.data_shutdown('redis_data'))
        shut2 = asyncio.ensure_future(session.data_shutdown('redis_data'))
        await asyncio.sleep(0)
        backend.gate.set()                  # start sees it doomed ...
        await asyncio.sleep(0)
        stop.set()                          # ... and joins the same stop
        await shut1
        await shut2
        with pytest.raises(HTTPException):
            await start
    backend.shutdown.assert_awaited_once()
    assert session._data == {}


@pytest.mark.asyncio
async def test_cancel_all_is_one_tracked_op():
    session = _ready_session()
    seen    = []

    async def _cancel(uid):
        seen.append(session._ops)
    backend = MagicMock()
    backend.cancel_task = AsyncMock(side_effect=_cancel)
    session._rh_session.backends = {'concurrent': backend}
    session._tasks = {f't{i}': {'uid': f't{i}', 'state': 'RUNNING',
                                'backend': 'concurrent'} for i in range(3)}
    res = await session.cancel_all_tasks()
    assert res == {'canceled': 3}
    assert seen == [1, 1, 1]
    assert session._ops == 0


@pytest.mark.asyncio
async def test_submit_after_teardown_is_410():
    session = _ready_session()

    def _prepare(chunk):
        session._rh_session = None          # teardown ran meanwhile
        return []
    with patch.object(session, '_prepare_batch', side_effect=_prepare):
        with pytest.raises(HTTPException) as exc:
            await session.submit_tasks([{'uid': 't1'}])
    assert exc.value.status_code == 410


def test_client_poll_fails_fast_on_gone_session():
    http = MagicMock()
    http.get.return_value = MagicMock(status_code=410)
    rc = RhapsodyClient(http, '/rhapsody')
    rc._sid = 'sid.1'
    with pytest.raises(RuntimeError, match='gone'):
        rc._poll_session_ready(timeout=5)
    assert http.get.call_count == 1


def test_client_poll_retries_transient_errors():
    http = MagicMock()
    http.get.side_effect = [RuntimeError('reconnecting'),
                            MagicMock(status_code=409),
                            MagicMock(status_code=200)]
    rc = RhapsodyClient(http, '/rhapsody')
    rc._sid = 'sid.1'
    with patch('time.sleep'):
        rc._poll_session_ready(timeout=5)
    assert http.get.call_count == 3


@pytest.mark.asyncio
async def test_failed_start_and_cleanup_still_release_name():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.start    = AsyncMock(side_effect=RuntimeError('port in use'))
    backend.shutdown = AsyncMock(side_effect=[RuntimeError('not running'),
                                              None])
    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        with pytest.raises(RuntimeError, match='port in use'):
            await session.data_start('redis')
    assert session._data == {}              # name released ...
    await session.close()                   # ... and close tries once more
    assert backend.shutdown.await_count == 2


@pytest.mark.asyncio
async def test_data_shutdown_waits_for_start_and_frees_name_after():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()
    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        start = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        shut  = asyncio.ensure_future(session.data_shutdown('redis_data'))
        await asyncio.sleep(0)
        assert not shut.done()              # no premature success ...
        with pytest.raises(HTTPException):  # ... and the name stays taken
            await session.data_start('redis')
        backend.gate.set()
        with pytest.raises(HTTPException):
            await start
        await shut
    assert session._data == {}
    backend.shutdown.assert_awaited_once()



@pytest.mark.asyncio
async def test_close_during_initialize_closes_new_rh_session(monkeypatch):
    from radical.orbit import plugin_rhapsody as prh
    fake_session = MagicMock(spec=['close'])
    fake_session.close = AsyncMock()
    sess = RhapsodySession('sess.closing', backend_names=['fake'])

    def _build(**kw):
        sess._closing = True                # close() ran while init built it
        return fake_session
    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=MagicMock(spec=[])))
    monkeypatch.setattr(prh.rh, 'Session', MagicMock(side_effect=_build))
    with pytest.raises(RuntimeError, match='closed during initialization'):
        await _REAL_INITIALIZE(sess)
    fake_session.close.assert_awaited_once()
    assert sess._rh_session is None
    assert 'closed during initialization' in sess._init_error


@pytest.mark.asyncio
async def test_failed_start_cleanup_during_drain_is_retried_by_close():
    session = _ready_session()
    backend = _FakeDataBackend('redis_data')
    backend.gate = asyncio.Event()

    async def _start():
        await backend.gate.wait()
        raise RuntimeError('port in use')
    backend.start    = AsyncMock(side_effect=_start)
    backend.shutdown = AsyncMock(side_effect=[RuntimeError('not running'),
                                              None])
    with patch.object(_mock_rh, 'get_data_backend', return_value=backend):
        start = asyncio.ensure_future(session.data_start('redis'))
        await asyncio.sleep(0)
        close = asyncio.ensure_future(session.close())   # drains the start
        await asyncio.sleep(0)
        backend.gate.set()
        with pytest.raises(RuntimeError, match='port in use'):
            await start
        await close
    assert backend.shutdown.await_count == 2    # close tried once more


@pytest.mark.asyncio
async def test_cancelled_stop_does_not_abort_teardown():
    session = _ready_session()
    a, b = _FakeDataBackend('a'), _FakeDataBackend('b')
    a.shutdown = AsyncMock(side_effect=asyncio.CancelledError())
    session._data = {'a': _DataEntry(a, 'redis'), 'b': _DataEntry(b, 'redis')}
    await session.close()
    b.shutdown.assert_awaited_once()
    assert not session.is_active


def test_data_shutdown_unknown():
    _, plugin, client = _make_plugin()
    sid, _ = _data_session(plugin, client)
    resp = client.post(f"{plugin.namespace}/data_shutdown/{sid}/nope")
    assert resp.status_code == 404


def test_data_start_old_rhapsody():
    _, plugin, client = _make_plugin()
    sid, _ = _data_session(plugin, client)
    with patch('radical.orbit.plugin_rhapsody.rh', spec=['Session']):
        resp = client.post(f"{plugin.namespace}/data_start/{sid}",
                           json={"kind": "redis"})
    assert resp.status_code == 501


def test_client_data_helpers():
    rc = RhapsodyClient.__new__(RhapsodyClient)
    rc._sid = 'sid.1'
    rc._require_session = MagicMock()
    rc._raise = MagicMock()
    rc._url = lambda path: f"/rhapsody/{path}"
    rc._http = MagicMock()
    rc._http.post.return_value.json.return_value = {"ok": 1}
    rc._http.get.return_value.json.return_value = {"backends": {}}

    assert rc.data_start('redis', name='r', port=1) == {"ok": 1}
    rc._http.post.assert_called_with(
        '/rhapsody/data_start/sid.1',
        json={"kind": "redis", "name": "r", "kwargs": {"port": 1}})
    assert rc.data_endpoints() == {"backends": {}}
    rc._http.get.assert_called_with('/rhapsody/data_endpoints/sid.1')
    rc.data_shutdown('r')
    rc._http.post.assert_called_with('/rhapsody/data_shutdown/sid.1/r')


# ---------------------------------------------------------------------------
# Submit batching: frame budget, fail-fast, partial submission
# ---------------------------------------------------------------------------


class _RecordingHTTP:
    """Fake transport: records each submit body; *fail_at* (0-based post
    index) answers with *fail_status*.  ``max_body`` is set only when given,
    so the default exercises the protocol-default fallback."""

    def __init__(self, max_body=None, fail_at=None, fail_status=503):
        if max_body is not None:
            self.max_body = max_body
        self.bodies      = []
        self.fail_at     = fail_at
        self.fail_status = fail_status

    def post(self, url, data=None, headers=None, **kw):
        import msgpack
        from radical.orbit.runtime_client import RuntimeResponse
        idx = len(self.bodies)
        self.bodies.append(data)
        if idx == self.fail_at:
            return RuntimeResponse(self.fail_status, {},
                                   b'{"detail": "nope"}')
        tasks = msgpack.unpackb(data, raw=False)['tasks']
        acks  = [{'uid': td['uid'], 'state': 'SUBMITTED'} for td in tasks]
        return RuntimeResponse(200, {}, json.dumps(acks).encode())


def _rh_client(http):
    client = RhapsodyClient(http, '/rhapsody')
    client._sid = 'sid-rh'
    return client


def _sent_tasks(body):
    import msgpack
    return msgpack.unpackb(body, raw=False)['tasks']


def _tasks(n, pad=0, prefix='t'):
    return [{'uid': f'{prefix}.{i:05d}', 'executable': '/bin/echo',
             'arguments': ['x' * pad]} for i in range(n)]


def test_pack_batches_bodies_are_exact_and_bounded():
    import msgpack
    from radical.orbit.runtime_client import pack_batches
    items   = _tasks(300, pad=40)
    batches = pack_batches(items, 4096, 8192)
    assert len(batches) > 1
    assert [td for b, _ in batches for td in b] == items      # order kept
    for batch, body in batches:
        # same bytes the old per-batch ``packb`` produced: no wire change
        assert body == msgpack.packb({'tasks': batch}, use_bin_type=True)
        assert len(body) <= 4096


def test_pack_batches_item_over_budget_travels_alone():
    from radical.orbit.runtime_client import pack_batches
    items   = _tasks(2) + _tasks(1, pad=6000, prefix='big') + _tasks(2)
    batches = pack_batches(items, 4096, 8192)
    assert [len(b) for b, _ in batches] == [2, 1, 2]


def test_pack_batches_item_over_limit_is_413_before_any_batch():
    from radical.orbit.runtime_client import PayloadTooLarge, pack_batches
    items = _tasks(2) + _tasks(1, pad=9000, prefix='big')
    with pytest.raises(PayloadTooLarge) as exc:
        pack_batches(items, 4096, 8192)
    assert exc.value.status_code == 413
    assert 'item 2' in str(exc.value)


def test_submit_8192_tasks_at_default_cap_splits_under_frame_cap():
    """Regression (overnight F2): 8192 tasks overran the 4 MiB frame in one
    8 MiB batch, the frame was dropped and the client hung."""
    from radical.orbit import protocol
    http   = _RecordingHTTP()                       # protocol-default budget
    client = _rh_client(http)
    tasks  = _tasks(8192, pad=600)
    result = client.submit_tasks(tasks)

    assert len(result) == 8192
    assert len(http.bodies) >= 2
    for body in http.bodies:
        req = protocol.make_request(
            'client', 'ep', 'POST', '/rhapsody/submit/sid-rh', body=body,
            headers={'Content-Type': 'application/msgpack'})
        assert len(protocol.pack_message(req)) <= protocol.FRAME_CAP
    assert client._submitted == {td['uid'] for td in tasks}


def test_submit_budget_is_half_the_transport_body_limit():
    http   = _RecordingHTTP(max_body=16 * 1024)
    client = _rh_client(http)
    client.submit_tasks(_tasks(200, pad=100))
    assert len(http.bodies) > 1
    assert all(len(b) <= 8 * 1024 for b in http.bodies)


def test_submit_ws_payload_limit_still_clamps(monkeypatch):
    from radical.orbit import plugin_rhapsody as prh
    monkeypatch.setattr(prh, 'WS_PAYLOAD_LIMIT', 2048)
    http = _RecordingHTTP(max_body=1024 * 1024)
    _rh_client(http).submit_tasks(_tasks(200, pad=100))
    assert all(len(b) <= 2048 for b in http.bodies)


def test_submit_oversized_task_413_sends_nothing():
    http   = _RecordingHTTP(max_body=8 * 1024)
    client = _rh_client(http)
    with pytest.raises(RuntimeError) as exc:
        client.submit_tasks(_tasks(3) + _tasks(1, pad=9000, prefix='big'))
    assert exc.value.status_code == 413
    assert http.bodies       == []
    assert client._submitted == set()


def test_submit_partial_failure_reports_accepted_uids():
    from radical.orbit import plugin_rhapsody as prh
    http   = _RecordingHTTP(max_body=8 * 1024, fail_at=1)
    client = _rh_client(http)
    tasks  = _tasks(200, pad=100)
    with pytest.raises(prh.PartialSubmitError) as exc:
        client.submit_tasks(tasks)
    err = exc.value
    assert err.status_code == 503
    assert len(http.bodies) == 2                    # stopped at the failure
    sent = _sent_tasks(http.bodies[0])
    assert err.accepted == [td['uid'] for td in sent]
    assert [r['uid'] for r in err.results] == err.accepted
    # only accepted uids are waitable; an unsent one is rejected, not hung on
    assert client._submitted == set(err.accepted)
    with pytest.raises(ValueError, match='unknown task UID'):
        client.wait_tasks([tasks[-1]['uid']], timeout=0.1)


def test_submit_first_batch_failure_raises_plain_error():
    from radical.orbit import plugin_rhapsody as prh
    http   = _RecordingHTTP(max_body=8 * 1024, fail_at=0, fail_status=413)
    client = _rh_client(http)
    with pytest.raises(RuntimeError) as exc:
        client.submit_tasks(_tasks(200, pad=100))
    assert not isinstance(exc.value, prh.PartialSubmitError)
    assert exc.value.status_code == 413
    assert client._submitted == set()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
