'''
Unit tests for the Rhapsody Endpoint plugin.

All RHAPSODY imports are mocked so the tests do not require the rhapsody
package to be installed.
'''

import threading
import time
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


def test_notify_window_zero_flushes_without_delay(monkeypatch):
    '''A window of 0 schedules one flush without delay (it runs on the next
    loop iteration) instead of flushing every completion inline.'''
    from radical.orbit import plugin_rhapsody as prh

    monkeypatch.setenv(prh.ENV_NOTIFY_WINDOW, '0')
    _, plugin, client = _make_plugin()
    sid = _register(client, plugin)

    session = plugin._sessions[sid]
    assert session._notify_window == 0

    plugin._dispatch_notify = MagicMock()
    with patch.object(session, '_schedule_flush') as sched:
        session._queue_notification({'uid': 'task.nw001', 'state': 'DONE'})
        session._queue_notification({'uid': 'task.nw003', 'state': 'DONE'})

    sched.assert_called_once_with(delay=0)
    plugin._dispatch_notify.assert_not_called()
    assert len(session._notify_buf) == 2
    assert session._flush_scheduled


def _watched_session(sid, window=0):
    """A plugin-attached session whose task futures the test resolves."""
    session = RhapsodySession(sid)
    session._plugin         = MagicMock()
    session._notify_window  = window
    session._rh_session     = MagicMock()
    futs: dict = {}
    session._rh_session._state_manager.get_wait_future = \
        lambda uid, t: futs.setdefault(
            uid, asyncio.get_running_loop().create_future())
    return session, futs


def _sent_frames(session):
    """The task lists of every notification frame the session sent."""
    frames = []
    for call in session._plugin._dispatch_notify.call_args_list:
        topic, data = call.args
        frames.append(data['tasks'] if topic == 'task_status_batch'
                      else [data])
    return frames


async def _ticks(n=3):
    for _ in range(n):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_window_zero_coalesces_completions_of_one_tick():
    '''With window 0, completions drained in one watcher pass go out in one
    batch frame; a lone completion still flushes without a timer.'''
    session, futs = _watched_session('test.nw0.tick')
    tasks   = [_done_task(f'task.c{i:03d}', i) for i in range(20)]
    watcher = asyncio.ensure_future(session._watch_batch(tasks))
    await _ticks(1)
    for fut in futs.values():
        fut.set_result(None)
    await asyncio.wait_for(watcher, 5)
    await _ticks()

    frames = _sent_frames(session)
    assert len(frames) == 1
    assert sorted(t['uid'] for t in frames[0]) == [t.uid for t in tasks]

    session, futs = _watched_session('test.nw0.lone')
    watcher = asyncio.ensure_future(
        session._watch_batch([_done_task('task.lone', 1)]))
    await _ticks(1)
    futs['task.lone'].set_result(None)
    await asyncio.wait_for(watcher, 5)
    await _ticks()                         # no sleep: no timer involved
    session._plugin._dispatch_notify.assert_called_once()
    assert session._plugin._dispatch_notify.call_args.args[0] \
        == 'task_status'


@pytest.mark.asyncio
async def test_window_zero_completions_across_ticks_all_delivered():
    '''Completions spread over loop iterations go out in several frames,
    each exactly once.'''
    session, futs = _watched_session('test.nw0.ticks')
    tasks   = [_done_task(f'task.x{i:03d}', i) for i in range(10)]
    watcher = asyncio.ensure_future(session._watch_batch(tasks))
    await _ticks(1)
    for i, fut in enumerate(futs.values()):
        fut.set_result(None)
        if i % 3 == 2:
            await _ticks()
    await asyncio.wait_for(watcher, 5)
    await _ticks()

    frames = _sent_frames(session)
    assert len(frames) > 1
    assert sorted(t['uid'] for f in frames for t in f) \
        == sorted(t.uid for t in tasks)


@pytest.mark.asyncio
async def test_running_state_changes_are_batched():
    '''RUNNING changes go through the batch buffer: a burst is one frame,
    not one frame per task — also when reported from a backend thread.'''
    session, _ = _watched_session('test.running')
    session._notified_states = {}
    session._notified_lock   = threading.Lock()

    for i in range(50):
        session._on_task_state_change({'uid': f'task.r{i}'}, 'RUNNING')
    session._plugin._dispatch_notify.assert_not_called()
    await _ticks()
    frames = _sent_frames(session)
    assert len(frames) == 1
    assert len(frames[0]) == 50
    assert {t['state'] for t in frames[0]} == {'RUNNING'}
    # sid is stamped on the frame by notify()
    assert session._plugin._dispatch_notify.call_args.args[1]['sid'] \
        == 'test.running'

    # from a backend thread: flushed on the plugin's loop
    session._plugin._dispatch_notify.reset_mock()
    session._plugin._main_loop = asyncio.get_running_loop()

    def _burst():
        for i in range(50, 100):
            session._on_task_state_change({'uid': f'task.r{i}'}, 'RUNNING')

    await asyncio.to_thread(_burst)
    deadline = time.monotonic() + 5
    while sum(map(len, _sent_frames(session))) < 50 \
            and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert sorted(t['uid'] for f in _sent_frames(session) for t in f) \
        == sorted(f'task.r{i}' for i in range(50, 100))


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
# task_states route — frame-bounded state fetch for lost notifications
# ---------------------------------------------------------------------------

def _states_session(plugin, client, tasks):
    sid = _register(client, plugin)
    session = plugin._sessions[sid]
    session._rh_session = MagicMock()
    for t in tasks:
        session._tasks[t.uid] = t
    return sid, session


def test_task_states_reports_unknown_uids():
    """Unknown uids are listed, not a 404; known ones carry the payload of
    their terminal notification."""
    _, plugin, client = _make_plugin()
    sid, _ = _states_session(plugin, client, [_done_task("task.k1", 7)])

    resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": ["task.k1", "task.nope"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["unknown"]   == ["task.nope"]
    assert body["remaining"] == []
    assert body["tasks"]     == [{"uid": "task.k1", "state": "DONE",
                                  "return_value": 7}]

    resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": "task.k1"})
    assert resp.status_code == 400


def test_task_states_truncates_output():
    """Large stdout is cut like in the terminal notification."""
    _, plugin, client = _make_plugin()
    sid, session = _states_session(
        plugin, client, [_output_task("task.big", "x" * 100_000, "")])
    session._output_notify_limit = 1024

    resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": ["task.big"]})
    task = resp.json()["tasks"][0]
    assert task["stdout_truncated"] is True
    assert len(task["stdout"]) <= 1024


def test_task_states_splits_large_uid_lists():
    """Payloads are packed within the frame budget; the rest comes back as
    ``remaining`` and a follow-up call picks it up."""
    from radical.orbit.plugin_rhapsody import _payload_size

    _, plugin, client = _make_plugin()
    tasks = [_output_task(f"task.s{i:02d}", "y" * 300, "") for i in range(20)]
    sid, session = _states_session(plugin, client, tasks)
    session._notify_batch_bytes = 2048

    todo, seen, calls = [t.uid for t in tasks], [], 0
    while todo:
        resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                           json={"uids": todo})
        body = resp.json()
        assert sum(_payload_size(t) for t in body["tasks"]) \
            <= session._frame_budget()
        seen.extend(t["uid"] for t in body["tasks"])
        todo   = body["remaining"]
        calls += 1
    assert calls > 1
    assert seen == [t.uid for t in tasks]


# ---------------------------------------------------------------------------
# Session scoping of task notifications
# ---------------------------------------------------------------------------

def test_session_notify_sets_event_session():
    """A session's notifications carry its sid as the event session."""
    session = RhapsodySession("test.scope.ev")
    session._plugin = MagicMock()
    session.notify("task_status", {"uid": "task.000001", "state": "DONE"})
    assert session._plugin._dispatch_notify.call_args.kwargs \
        == {"session": "test.scope.ev"}


@pytest.mark.asyncio
async def test_init_session_status_carries_the_session():
    """session_status is plugin-level but concerns one session: it passes
    that session's sid as the event session, ready and failed alike."""
    _, plugin, _ = _make_plugin()
    plugin._dispatch_notify = MagicMock()

    ok = MagicMock()
    ok._init_ready.is_set.return_value = False
    ok.initialize = AsyncMock()
    await plugin._init_session("sid.ok", ok)

    bad = MagicMock()
    bad._init_ready.is_set.return_value = False
    bad.initialize = AsyncMock(side_effect=RuntimeError("boom"))
    await plugin._init_session("sid.bad", bad)

    calls = plugin._dispatch_notify.call_args_list
    assert [(c.args[0], c.args[1]["status"], c.kwargs) for c in calls] == [
        ("session_status", "ready",  {"session": "sid.ok"}),
        ("session_status", "failed", {"session": "sid.bad"})]


def test_session_notify_stamps_sid_on_task_topics():
    """Task notifications carry the session id; the payload is a copy."""
    session = RhapsodySession("test.scope.1")
    session._plugin = MagicMock()

    single = {"uid": "task.000001", "state": "DONE"}
    batch  = {"tasks": [{"uid": "task.000002", "state": "DONE"}]}
    session.notify("task_status", single)
    session.notify("task_status_batch", batch)
    session.notify("session_status", {"state": "ready"})

    calls = session._plugin._dispatch_notify.call_args_list
    assert calls[0].args == ("task_status",
                             {"uid": "task.000001", "state": "DONE",
                              "sid": "test.scope.1"})
    assert calls[1].args[1]["sid"] == "test.scope.1"
    assert calls[1].args[1]["tasks"] == batch["tasks"]
    assert "sid" not in calls[2].args[1]          # other topics unchanged
    assert "sid" not in single and "sid" not in batch   # not mutated


def test_client_ignores_other_session_task_notifications():
    """Foreign-sid completions are dropped; own and sid-less are kept."""
    client = _make_rhapsody_client()                # sid-rh

    client._on_task_done(None, None, "task_status",
                         {"uid": "task.000001", "state": "DONE",
                          "return_value": "theirs", "sid": "sid-other"})
    client._on_task_done(None, None, "task_status_batch",
                         {"sid": "sid-other", "tasks": [
                             {"uid": "task.000002", "state": "DONE"}]})
    assert "task.000001" not in client._completed
    assert "task.000002" not in client._completed

    client._on_task_done(None, None, "task_status",
                         {"uid": "task.000001", "state": "DONE",
                          "return_value": "ours", "sid": "sid-rh"})
    client._on_task_done(None, None, "task_status",          # older endpoint
                         {"uid": "task.000003", "state": "DONE"})
    assert client._completed["task.000001"]["return_value"] == "ours"
    assert "task.000003" in client._completed
    assert "sid" not in client._completed["task.000001"]   # same shape as batch


def test_split_frames_reserves_room_for_sid():
    """Frames stay within the budget after notify() adds the session id."""
    from radical.orbit.plugin_rhapsody import _payload_size

    session = RhapsodySession("test.scope.budget")
    one = {"uid": "task.000001", "state": "DONE", "stdout": "x" * 60}
    session._notify_batch_bytes = 2 * _payload_size(one)   # two fit without sid
    frames = session._split_frames([dict(one), dict(one)])
    assert len(frames) == 2                     # but not once sid is counted


def test_client_wait_tasks_rejects_uid_only_seen_from_other_session():
    """A uid completed by another session is not known to this client."""
    client = _make_rhapsody_client()
    client._on_task_done(None, None, "task_status",
                         {"uid": "task.000009", "state": "DONE",
                          "sid": "sid-other"})
    with pytest.raises(ValueError, match="unknown task UID"):
        client.wait_tasks(["task.000009"])


def test_two_sessions_with_colliding_uids_get_their_own_results():
    """Broadcast notifications of two sessions reach both clients; each
    client keeps only its own session's result for the shared uid."""
    sessions, sent = [], []
    for sid in ("sid-a", "sid-b"):
        s = RhapsodySession(sid)
        s._rh_session = MagicMock()
        s._plugin = MagicMock()
        s._plugin._dispatch_notify.side_effect = \
            lambda topic, data, session=None: sent.append((topic, data))
        sessions.append(s)

    for s, value in zip(sessions, ("from-a", "from-b")):
        s.notify("task_status",
                 s._notification_payload(_done_task("task.000001", value)))

    clients = []
    for sid in ("sid-a", "sid-b"):
        c = _make_rhapsody_client()
        c._sid = sid
        for topic, data in sent:                  # the broadcast
            c._on_task_done(None, None, topic, data)
        clients.append(c)

    assert clients[0]._completed["task.000001"]["return_value"] == "from-a"
    assert clients[1]._completed["task.000001"]["return_value"] == "from-b"




# ---------------------------------------------------------------------------
# Client fan-out and reconcile of lost task notifications
# ---------------------------------------------------------------------------

class _FakeRuntime:
    """Runtime stand-in: its callback registry feeds a real
    CallbackDispatcher, the way ``EndpointRuntime._on_event`` does
    (including the session filter and the ``with_meta`` form)."""

    def __init__(self, maxlen=1024):
        from radical.orbit.runtime_support import CallbackDispatcher
        self._cb       = CallbackDispatcher(maxlen=maxlen)
        self.callbacks = []             # (topic, callback, with_meta, session)

    def register_callback(self, endpoint_id=None, plugin_name=None,
                          topic=None, callback=None, with_meta=False,
                          session=None):
        self.callbacks.append((topic, callback, with_meta, session))

    def unregister_callback(self, endpoint_id=None, plugin_name=None,
                            topic=None, callback=None, session=None):
        self.callbacks = [c for c in self.callbacks
                          if (c[0], c[1], c[3]) != (topic, callback, session)]

    def deliver(self, topic, data, session=None, seq=0):
        """An event as the broker sends it; ``session=None`` is an older
        endpoint's session-less event."""
        meta = {'seq': seq, 'ts': 0.0, 'session': session}
        for t, cb, with_meta, s in list(self.callbacks):
            if t not in (None, topic):
                continue
            if s is not None and session is not None and s != session:
                continue
            args = ('ep', 'rhapsody', topic, data)
            self._cb.submit(cb, *args, *((meta,) if with_meta else ()))


class _SessionHTTP:
    """httpx-shaped transport serving ``task_states`` from a session."""

    def __init__(self, session, gate=None):
        self.session = session
        self.gate    = gate
        self.threads = []                       # thread of each task_states

    def post(self, url, json=None, **kwargs):
        body = {}
        if 'task_states' in url:
            self.threads.append(threading.current_thread().name)
            if self.gate is not None:
                self.gate.wait(10)
            try:
                body = asyncio.run(self.session.task_states(
                    json['uids'], json.get('terminal_only', False)))
            except HTTPException as e:
                return self._resp(e.status_code, {'detail': e.detail})
        return self._resp(200, body)

    @staticmethod
    def _resp(status, body):
        resp = MagicMock()
        resp.is_error, resp.status_code = status >= 400, status
        resp.json = MagicMock(return_value=body)
        return resp


def _done_session(sid, uids, tag=None):
    session = RhapsodySession(sid)
    session._rh_session = MagicMock()
    session._init_ready.set()
    for uid in uids:
        session._tasks[uid] = _done_task(uid, f'{tag or sid}:{uid}')
    return session


def _reconciling_client(rt, session, idle=60.0, gate=None):
    http   = _SessionHTTP(session, gate)
    client = RhapsodyClient(http, '/rhapsody', broker_client=rt,
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid              = session.sid
    client._reconcile_idle   = idle
    client._reconcile_settle = 0.01
    client._start_task_listener()
    return client, http


def _track(client, uids):
    """Record *uids* as submitted and acknowledged by the endpoint."""
    with client._cond:
        client._submitted.update(uids)
        client._pending.update(uids)


def _backend_recorder(client):
    """Register a backend-style callback (as rhapsody's
    OrbitExecutionBackend does) and return what it receives."""
    got = {'uids': [], 'sids': set()}

    def _cb(endpoint, plugin, topic, data):
        tasks = data.get('tasks', []) if topic == 'task_status_batch' \
            else [data]
        got['sids'].add(data.get('sid'))
        got['uids'].extend(t['uid'] for t in tasks)

    client.register_notification_callback(_cb, topic='task_status')
    client.register_notification_callback(_cb, topic='task_status_batch')
    return got


def _until(cond, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not cond() and time.monotonic() < deadline:
        time.sleep(0.01)
    return cond()


def test_fan_out_one_runtime_callback_per_topic():
    """The client holds the only runtime registration per task topic; the
    backend callbacks it records run after ``_on_task_done``, in order, on
    the same (decoded) data object."""
    import base64
    import threading as _threading

    from radical.orbit import protocol
    from radical.orbit.runtime import EndpointRuntime

    rt = EndpointRuntime.__new__(EndpointRuntime)
    rt._callbacks = {}
    rt._cb_lock   = _threading.Lock()
    rt._name      = 'test-consumer'
    rt._emit      = MagicMock()
    queued: list  = []
    rt._cb        = MagicMock()
    rt._cb.submit = lambda fn, *args: queued.append((fn, args))

    client = RhapsodyClient(MagicMock(), '/rhapsody', broker_client=rt,
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid = 'sid-a'
    client._start_task_listener()
    try:
        seen = []

        def _backend_cb(endpoint, plugin, topic, data):
            seen.append(('backend', topic, data['return_value']))

        def _other_cb(endpoint, plugin, topic, data):
            seen.append(('other', topic, data['return_value']))

        for topic in ('task_status', 'task_status_batch'):
            client.register_notification_callback(_backend_cb, topic=topic)
        client.register_notification_callback(_other_cb, topic='task_status')

        # one registration: all topics, scoped to the session
        assert list(rt._callbacks) == [('ep', 'rhapsody', None)]
        assert [(m, s) for _, m, s in rt._callbacks[('ep', 'rhapsody', None)]] \
            == [(True, 'sid-a')]

        rt._on_event(protocol.Event(
            src='ep', plugin='rhapsody', topic='task_status',
            data={'uid': 't.1', 'state': 'DONE', 'sid': 'sid-a',
                  'return_value': base64.b64encode(b'raw').decode(),
                  '_return_value_encoding': 'base64'}))
        assert len(queued) == 1                 # one queue entry per event
        for fn, args in queued:
            fn(*args)
        assert seen == [('backend', 'task_status', b'raw'),
                        ('other',   'task_status', b'raw')]
        assert client._completed['t.1']['return_value'] == b'raw'

        client.unregister_notification_callback(_other_cb,
                                                topic='task_status')
        assert client._fanout['task_status'] == [_backend_cb]
    finally:
        client._stop_reconcile()


def test_reconcile_after_client_drop():
    """Callback-queue overflow: the drop hook wakes the reconcile, and both
    wait_tasks and the backend callbacks see every completion."""
    uids    = [f'task.d{i:03d}' for i in range(50)]
    session = _done_session('sid-drop', uids)
    rt      = _FakeRuntime(maxlen=4)            # dispatcher not started yet
    client, http = _reconciling_client(rt, session, idle=60.0)
    try:
        backend = _backend_recorder(client)
        _track(client, uids)

        for uid in uids:
            payload = session._notification_payload(session._tasks[uid])
            rt.deliver('task_status', {**payload, 'sid': session.sid})
        assert rt._cb.dropped == 46

        rt._cb.start()
        done = client.wait_tasks(uids, timeout=10)
        assert [t['state'] for t in done] == ['DONE'] * 50
        assert [t['return_value'] for t in done] \
            == [f'sid-drop:{u}' for u in uids]
        assert _until(lambda: set(backend['uids']) == set(uids))
        assert backend['sids'] == {'sid-drop'}
        assert http.threads and set(http.threads) == {'orbit-rh-reconcile'}
    finally:
        client.close()
        rt._cb.stop()


def test_reconcile_after_broker_drop():
    """A broker-side drop is invisible to the client: the idle path
    recovers it (no dispatcher drop involved)."""
    from radical.orbit.queues import BoundedDropOldestQueue

    uids    = [f'task.b{i:03d}' for i in range(50)]
    session = _done_session('sid-broker', uids)
    rt      = _FakeRuntime()
    rt._cb.start()
    client, _ = _reconciling_client(rt, session, idle=0.05)
    try:
        backend = _backend_recorder(client)
        _track(client, uids)

        broker_q = BoundedDropOldestQueue(4)    # the broker's event_queue
        for uid in uids:
            payload = session._notification_payload(session._tasks[uid])
            broker_q.push(('task_status', {**payload, 'sid': session.sid}))
        assert broker_q.dropped == 46
        while broker_q.buf:
            rt.deliver(*broker_q.buf.popleft())

        done = client.wait_tasks(uids, timeout=10)
        assert [t['state'] for t in done] == ['DONE'] * 50
        assert rt._cb.dropped == 0
        assert _until(lambda: set(backend['uids']) == set(uids))
    finally:
        client.close()
        rt._cb.stop()


def test_reconcile_two_clients_recover_their_own():
    """Two sessions with colliding uids on one runtime: each client
    recovers only its own uids, and its synthesized events carry its sid."""
    uids = [f'task.t{i:03d}' for i in range(20)]
    rt   = _FakeRuntime(maxlen=4)
    sessions = [_done_session(sid, uids) for sid in ('sid-a', 'sid-b')]
    clients, backends = [], []
    try:
        for session in sessions:
            client, _ = _reconciling_client(rt, session)
            _track(client, uids)
            clients.append(client)
            backends.append(_backend_recorder(client))

        for session in sessions:                # the broadcast
            for uid in uids:
                payload = session._notification_payload(session._tasks[uid])
                rt.deliver('task_status', {**payload, 'sid': session.sid})
        rt._cb.start()

        for client in clients:
            done = client.wait_tasks(uids, timeout=10)
            assert [t['return_value'] for t in done] \
                == [f'{client.sid}:{u}' for u in uids]
        for client, backend in zip(clients, backends):
            # the backend saw its own sid (recovered), plus foreign real
            # events that its own sid filter drops — never a sid-less one
            assert _until(lambda b=backend: set(b['uids']) == set(uids))
            assert client.sid in backend['sids'] and None not in \
                backend['sids']
    finally:
        for client in clients:
            client.close()
        rt._cb.stop()


def test_reconcile_unknown_uids_fail_and_inflight_skipped():
    """A uid the endpoint does not know is completed as FAILED; a uid whose
    submit is still in flight is not asked for."""
    session = _done_session('sid-unk', ['task.known'])
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session)
    try:
        _track(client, ['task.known', 'task.lost'])
        client._submitted.add('task.busy')          # submit not acked yet
        assert sorted(client._outstanding()) == ['task.known', 'task.lost']

        client._reconcile_wake.set()
        assert _until(lambda: 'task.lost' in client._completed)
        assert client._completed['task.lost']['state'] == 'FAILED'
        assert 'unknown' in client._completed['task.lost']['error']
        assert client._completed['task.known']['state'] == 'DONE'
        assert 'task.busy' not in client._completed
    finally:
        client.close()
        rt._cb.stop()


def test_drop_hook_does_not_block_the_work_loop():
    """The drop hook only wakes the reconcile thread: delivering into a
    full queue returns at once even while a reconcile fetch is stuck."""
    gate    = threading.Event()
    session = _done_session('sid-hook', ['task.h1'])
    rt      = _FakeRuntime(maxlen=1)
    client, http = _reconciling_client(rt, session, gate=gate)
    try:
        _track(client, ['task.h1'])
        rt.deliver('task_status', {'uid': 'task.x', 'state': 'RUNNING'})
        t0 = time.monotonic()
        for _ in range(20):                     # all dropped
            rt.deliver('task_status', {'uid': 'task.x', 'state': 'RUNNING'})
        assert _until(lambda: http.threads)     # reconcile is in its fetch
        for _ in range(20):
            rt.deliver('task_status', {'uid': 'task.x', 'state': 'RUNNING'})
        assert time.monotonic() - t0 < 1.0
        assert http.threads == ['orbit-rh-reconcile']
    finally:
        gate.set()
        rt._cb.start()
        client.close()
        rt._cb.stop()


def test_close_during_reconcile_stops_cleanly():
    """close() waits for the reconcile in flight, then the thread is gone
    and the drop hook is removed."""
    gate    = threading.Event()
    session = _done_session('sid-close', ['task.c1'])
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session, gate=gate)
    thread = client._reconcile_thread
    _track(client, ['task.c1'])
    client._reconcile_wake.set()
    assert _until(lambda: http.threads)         # reconcile is in its fetch

    closer = threading.Thread(target=client.close)
    closer.start()
    closer.join(0.2)
    assert closer.is_alive()                    # waits for the reconcile
    gate.set()
    closer.join(10)
    assert not closer.is_alive()
    assert not thread.is_alive()
    assert client._reconcile_thread is None
    assert rt._cb._hooks == []
    rt._cb.stop()



# ---------------------------------------------------------------------------
# Code-review round 1
# ---------------------------------------------------------------------------

def _ack_http(fail_at=None, gate=None, before_ack=None, status=None):
    """Transport acking every submit POST; the *fail_at*-th one (0-based)
    waits for *gate* and raises (with *status* as ``status_code`` if given:
    an HTTP error, else a transport error).  *before_ack(uids)* runs before
    an ack."""
    calls = []

    def post(url, data=None, json=None, headers=None):
        n = len(calls)
        calls.append(url)
        if 'submit' not in url:
            body = {"sid": "sid-new", "status": "ready"}
        else:
            if n == fail_at:
                if gate is not None:
                    gate.wait(10)
                exc = RuntimeError("submit POST failed")
                if status is not None:
                    exc.status_code = status
                raise exc
            import msgpack
            tasks = msgpack.unpackb(data)["tasks"]
            if before_ack:
                before_ack([t["uid"] for t in tasks])
            body = [{"uid": t["uid"], "state": "SUBMITTED"} for t in tasks]
        resp = MagicMock()
        resp.is_error, resp.status_code = False, 200
        resp.json = MagicMock(return_value=body)
        return resp

    http = MagicMock()
    http.post = post
    return http


def _sse_client(http, rt=None):
    client = RhapsodyClient(http, '/rhapsody', broker_client=rt or MagicMock(),
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid = 'sid-rh'
    return client


def test_session_switch_and_close_reset_tracking():
    """A new session (or close) forgets the old session's tasks: they are
    neither reconciled nor waited for."""
    client = _sse_client(_ack_http())
    _track(client, ['t.old1', 't.old2'])
    client._on_task_done(None, None, 'task_status',
                         {'uid': 't.old1', 'state': 'DONE'})

    client._register_and_wait(None, 5, False, {}, threading.Event())
    assert client.sid == 'sid-new'
    assert client._outstanding() == []
    assert not client._submitted and not client._completed
    with pytest.raises(ValueError, match='unknown task UID'):
        client.wait_tasks(['t.old2'])

    _track(client, ['t.new'])
    client.close()
    assert client._outstanding() == [] and not client._submitted


def test_fan_out_does_not_repeat_terminal_states():
    """A terminal state already recorded (live and recovered events
    crossing) is not passed on again; RUNNING and other sessions' events
    pass unchanged."""
    client = _sse_client(MagicMock(), rt=_FakeRuntime())
    got = []
    client.register_notification_callback(
        lambda e, p, t, d: got.append((t, d)), topic='task_status')
    client.register_notification_callback(
        lambda e, p, t, d: got.append((t, d)), topic='task_status_batch')

    run  = {'uid': 't.1', 'state': 'RUNNING', 'sid': 'sid-rh'}
    done = {'uid': 't.1', 'state': 'DONE',    'sid': 'sid-rh'}
    client._fan_out('ep', 'rhapsody', 'task_status', run)
    client._fan_out('ep', 'rhapsody', 'task_status', done)
    client._fan_out('ep', 'rhapsody', 'task_status', dict(done))   # dup
    client._fan_out('ep', 'rhapsody', 'task_status_batch',          # recovered
                    {'sid': 'sid-rh', 'tasks': [
                        {'uid': 't.1', 'state': 'DONE'},
                        {'uid': 't.2', 'state': 'DONE'}]})
    client._fan_out('ep', 'rhapsody', 'task_status_batch',
                    {'sid': 'sid-rh', 'tasks': [
                        {'uid': 't.2', 'state': 'FAILED'}]})       # all dups
    foreign = {'uid': 't.1', 'state': 'DONE', 'sid': 'sid-other'}
    client._fan_out('ep', 'rhapsody', 'task_status', foreign)

    assert [(t, d.get('uid') or [x['uid'] for x in d['tasks']])
            for t, d in got] == [
        ('task_status', 't.1'), ('task_status', 't.1'),
        ('task_status_batch', ['t.2']), ('task_status', 't.1')]
    assert got[1][1] is done and got[3][1] is foreign
    assert 't.2' in client._completed      # recorded, passed on once


def _failing_submit(status, pre=()):
    """Submit 4 one-task batches whose second POST fails; a waiter on the
    unsent t.2 blocks before the failure.  *pre* uids were submitted (and
    acked) earlier and are part of the failing batch's call."""
    from radical.orbit import plugin_rhapsody as prh

    gate   = threading.Event()
    client = _sse_client(_ack_http(fail_at=1, gate=gate, status=status))
    _track(client, pre)
    tasks  = [{'uid': f't.{i}', 'executable': '/bin/true'} for i in range(4)]
    tasks += [{'uid': u, 'executable': '/bin/true'} for u in pre]
    errors, waited = [], []

    def _submit():
        try:
            client.submit_tasks(tasks)
        except RuntimeError as e:
            errors.append(e)

    def _wait():
        try:
            client.wait_tasks(['t.0', 't.2'], timeout=10)
        except RuntimeError as e:
            waited.append(e)

    with patch.object(prh, 'WS_PAYLOAD_LIMIT', 1):  # one task per batch
        submitter = threading.Thread(target=_submit)
        submitter.start()
        assert _until(lambda: 't.0' in client._pending)
        waiter = threading.Thread(target=_wait)
        waiter.start()
        time.sleep(0.05)                         # waiter blocks
        gate.set()
        submitter.join(10)
        waiter.join(10)

    assert errors and 'submit POST failed' in str(errors[0])
    assert waited and 'withdrawn' in str(waited[0])
    return client


def test_failed_submit_keeps_uncertain_batch():
    """A transport error (or 5xx) leaves the failing batch uncertain: it
    stays submitted and pending, for the reconcile to resolve.  Batches
    never sent are withdrawn."""
    for status in (None, 504):
        client = _failing_submit(status)
        assert client._submitted == {'t.0', 't.1'}
        assert sorted(client._outstanding()) == ['t.0', 't.1']


def test_failed_submit_withdraws_rejected_batch_only_new_uids():
    """A 4xx withdraws the rejected batch too — but only uids this call
    added: an earlier submit's uid stays."""
    client = _failing_submit(400, pre=('t.early',))
    assert client._submitted == {'t.0', 't.early'}
    assert sorted(client._outstanding()) == ['t.0', 't.early']


def test_outstanding_is_kept_incrementally():
    """Acked uids become pending, a completion removes one, and a
    completion that beats its ack never becomes pending."""
    holder = {}

    def _early(uids):
        if 't.fast' in uids:
            holder['c']._on_task_done(None, None, 'task_status',
                                      {'uid': 't.fast', 'state': 'DONE'})

    client = _sse_client(_ack_http(before_ack=_early))
    holder['c'] = client
    client.submit_tasks([{'uid': u, 'executable': '/bin/true'}
                         for u in ('t.a', 't.b', 't.fast')])
    assert sorted(client._outstanding()) == ['t.a', 't.b']

    client._on_task_done(None, None, 'task_status_batch',
                         {'sid': 'sid-rh', 'tasks': [
                             {'uid': 't.a', 'state': 'RUNNING'},
                             {'uid': 't.b', 'state': 'CANCELED'}]})
    assert client._outstanding() == ['t.a']


def test_task_states_rejects_oversized_requests():
    """Too many uids, or uids too large for half the budget: 413."""
    _, plugin, client = _make_plugin()
    sid, session = _states_session(plugin, client, [])

    from radical.orbit.plugin_rhapsody import TASK_STATES_MAX_UIDS
    resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": [f"t.{i}" for i in
                                      range(TASK_STATES_MAX_UIDS + 1)]})
    assert resp.status_code == 413

    session._notify_batch_bytes = 1024
    resp = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": ["x" * 40 for _ in range(30)]})
    assert resp.status_code == 413


def test_task_states_response_fits_with_uid_lists():
    """The whole response — tasks, remaining and unknown — stays within
    the frame budget."""
    _, plugin, client = _make_plugin()
    tasks = [_output_task(f"task.f{i:02d}", "z" * 300, "") for i in range(20)]
    sid, session = _states_session(plugin, client, tasks)
    session._notify_batch_bytes = 4096

    uids = [f"task.unknown{i:02d}" for i in range(10)] \
        + [t.uid for t in tasks] + [f"task.gone{i:02d}" for i in range(10)]
    body = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": uids}).json()
    assert body["remaining"] and len(body["unknown"]) == 10
    assert len(json.dumps(body)) <= session._frame_budget()


def test_task_states_non_terminal_bare_and_payloads_computed_once():
    """Running tasks come back as {uid, state}; across pages each terminal
    payload is built at most once more than needed (the page boundary)."""
    _, plugin, client = _make_plugin()
    running = MagicMock()
    running.uid, running.state = "task.run", "RUNNING"
    running.to_dict = lambda: {"uid": "task.run", "state": "RUNNING",
                               "stdout": "partial"}
    tasks = [_output_task(f"task.p{i:02d}", "y" * 300, "") for i in range(12)]
    sid, session = _states_session(plugin, client, [running] + tasks)
    session._notify_batch_bytes = 2048

    with patch.object(session, '_notification_payload',
                      wraps=session._notification_payload) as built:
        todo, pages, first = ["task.run"] + [t.uid for t in tasks], 0, None
        while todo:
            body = client.post(f"{plugin.namespace}/task_states/{sid}",
                               json={"uids": todo}).json()
            first = first or body["tasks"][0]
            todo   = body["remaining"]
            pages += 1
    assert first == {"uid": "task.run", "state": "RUNNING"}
    assert pages > 1
    assert built.call_count <= len(tasks) + pages - 1


@pytest.mark.asyncio
async def test_thread_notifications_are_serialized_on_the_loop():
    """A RUNNING change from a backend thread is handed to the loop: it is
    never flushed on that thread (even with a full buffer) and cannot
    overtake a completion queued after it."""
    session, _ = _watched_session('test.handoff')
    session._notified_states = {}
    session._notified_lock   = threading.Lock()
    loop = asyncio.get_running_loop()
    session._plugin._main_loop = loop
    flushed_on = []
    session._plugin._dispatch_notify.side_effect = \
        lambda topic, data, session=None: flushed_on.append(threading.get_ident())

    await asyncio.to_thread(session._on_task_state_change,
                            {'uid': 't.1'}, 'RUNNING')
    session._queue_notification({'uid': 't.1', 'state': 'DONE'})
    await _ticks()
    frames = _sent_frames(session)
    assert [(t['uid'], t['state']) for f in frames for t in f] \
        == [('t.1', 'RUNNING'), ('t.1', 'DONE')]

    session._notify_batch_bytes = 1               # every item flushes now
    await asyncio.to_thread(session._on_task_state_change,
                            {'uid': 't.2'}, 'RUNNING')
    await _ticks()
    main = threading.get_ident()
    assert len(flushed_on) >= 2 and set(flushed_on) == {main}


class _NoDispatcherRuntime(_FakeRuntime):
    """A runtime without a CallbackDispatcher: callbacks run on the
    delivering thread."""

    def __init__(self):
        super().__init__()
        del self._cb

    def deliver(self, topic, data):
        for t, cb in list(self.callbacks):
            if t is None or t == topic:
                cb('ep', 'rhapsody', topic, data)


def test_fan_out_serialized_without_dispatcher():
    """Without a dispatcher the reconcile delivers on its own thread, but
    never concurrently with live delivery."""
    uids    = [f'task.n{i:02d}' for i in range(10)]
    session = _done_session('sid-nodisp', uids)
    rt      = _NoDispatcherRuntime()
    client, http = _reconciling_client(rt, session)
    active, peak, got = [0], [0], []
    lock = threading.Lock()

    def _slow(endpoint, plugin, topic, data):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.005)
        got.append(topic)
        with lock:
            active[0] -= 1

    try:
        assert client._dispatcher is None
        for topic in ('task_status', 'task_status_batch'):
            client.register_notification_callback(_slow, topic=topic)
        _track(client, uids)

        def _live():
            for i in range(40):
                rt.deliver('task_status', {'uid': f'task.live{i}',
                                           'state': 'RUNNING',
                                           'sid': 'sid-nodisp'})

        live = threading.Thread(target=_live)
        live.start()
        client._reconcile_wake.set()
        live.join(10)
        assert client.wait_tasks(uids, timeout=10)
        assert _until(lambda: 'task_status_batch' in got)
        assert peak[0] == 1
    finally:
        client.close()



# ---------------------------------------------------------------------------
# Code-review round 2
# ---------------------------------------------------------------------------

def test_all_topics_callbacks_go_through_the_fan_out():
    """A topic=None callback gets task events through the fan-out (once,
    deduplicated, including recovered ones) and other topics as is."""
    rt     = _FakeRuntime()
    client = _sse_client(MagicMock(), rt=rt)
    got    = []
    client.register_notification_callback(
        lambda e, p, t, d: got.append((t, d.get('uid') or d.get('status')
                                       or [x['uid'] for x in d['tasks']])))
    rt._cb.start()
    try:
        rt.deliver('task_status', {'uid': 't.1', 'state': 'DONE',
                                   'sid': 'sid-rh'})
        rt.deliver('session_status', {'sid': 'sid-rh', 'status': 'ready'})
        assert _until(lambda: len(got) == 2)    # live ones first
        client._redeliver('sid-rh', [{'uid': 't.1', 'state': 'DONE'},
                                     {'uid': 't.2', 'state': 'DONE'}])
        assert _until(lambda: len(got) == 3)
        time.sleep(0.05)
        assert sorted(got, key=str) == sorted([
            ('task_status', 't.1'), ('session_status', 'ready'),
            ('task_status_batch', ['t.2'])], key=str)
        # one runtime registration: all topics, scoped to the session
        assert [(t, m, s) for t, _, m, s in rt.callbacks] \
            == [(None, True, 'sid-rh')]
    finally:
        rt._cb.stop()


def test_example_rhapsody_runs_against_the_client(capsys):
    """examples/example_rhapsody.py (all-topics callback, submit, wait,
    get_task, close) runs end to end against a fake runtime."""
    import importlib.util
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[2] \
        / 'examples' / 'example_rhapsody.py'
    spec = importlib.util.spec_from_file_location('example_rhapsody', path)
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)

    rt      = _FakeRuntime()
    session = RhapsodySession('sid-example')
    session._rh_session = MagicMock()
    session._init_ready.set()

    class _ExampleHTTP:
        def post(self, url, data=None, json=None, headers=None, **kw):
            body = {}
            if 'submit' in url:
                import msgpack
                tasks = msgpack.unpackb(data)['tasks']
                for t in tasks:
                    session._tasks[t['uid']] = _done_task(t['uid'], 'ok')
                body = [{'uid': t['uid'], 'state': 'SUBMITTED'}
                        for t in tasks]
                for t in tasks:                    # completions follow
                    rt.deliver('task_status', {
                        **session._notification_payload(
                            session._tasks[t['uid']]),
                        'sid': session.sid})
            return self._resp(body)

        def get(self, url, **kw):
            return self._resp({'uid': url.rsplit('/', 1)[-1],
                               'state': 'DONE'})

        @staticmethod
        def _resp(body):
            resp = MagicMock()
            resp.is_error, resp.status_code = False, 200
            resp.json = MagicMock(return_value=body)
            return resp

    class _ExampleRuntime:
        name = 'consumer'

        def __init__(self, *a, **kw):
            pass

        def start(self, wait=True):
            rt._cb.start()

        def topology(self):
            return {'ep': {'plugins': {'rhapsody': {}}}}

        def get_plugin(self, eid, plugin):
            client = RhapsodyClient(_ExampleHTTP(), '/rhapsody',
                                    broker_client=rt, endpoint_id=eid,
                                    plugin_name=plugin)
            client._sid = session.sid
            client._start_task_listener()
            return client

        def stop(self):
            rt._cb.stop()

    with patch.object(example, 'EndpointRuntime', _ExampleRuntime):
        example.main()
    out = capsys.readouterr().out
    assert 'All tasks finished' in out and 'Done.' in out
    assert out.count('[Notification]') == 3        # each completion once


def test_reconcile_chunks_by_bytes_and_splits_on_413():
    """The reconcile sizes requests by uid bytes; an endpoint with a smaller
    budget answers 413 and the request is split until it fits."""
    from radical.orbit.plugin_rhapsody import _task_states_fits

    uids    = [f'task.{"l" * 40}.{i:03d}' for i in range(60)]
    session = _done_session('sid-bytes', uids)
    session._notify_batch_bytes = 2048         # endpoint: small budget
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session)
    try:
        assert all(_task_states_fits(c, client._states_budget)
                   for c in client._uid_chunks('sid-bytes', uids))
        client._states_budget = 64 * 1024      # client: assumes more
        assert len(client._uid_chunks('sid-bytes', uids)) == 1
        _track(client, uids)
        client._reconcile_wake.set()
        assert len(client.wait_tasks(uids, timeout=10)) == 60
        assert len(http.threads) > 2           # 413s, then split calls
    finally:
        client.close()
        rt._cb.stop()


def test_reconcile_aborts_when_the_session_changes():
    """The sid is taken once per pass; if the session changed while
    fetching, nothing is delivered."""
    session = _done_session('sid-old', ['task.s1', 'task.s2'])
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session)
    urls = []
    orig = http.post

    def _switching(url, json=None, **kwargs):
        urls.append(url)
        resp = orig(url, json=json, **kwargs)
        client._sid = 'sid-new'                # switched meanwhile
        return resp

    http.post = _switching
    try:
        _track(client, ['task.s1', 'task.s2'])
        got = []
        client.register_notification_callback(
            lambda *a: got.append(a), topic='task_status_batch')
        client._reconcile()
        assert urls == ['/rhapsody/task_states/sid-old']
        time.sleep(0.05)
        assert got == [] and not client._completed
    finally:
        client.close()
        rt._cb.stop()


def test_stale_running_after_terminal_is_not_passed_on():
    """State never goes backwards: a RUNNING for a task recorded terminal
    is dropped from the fan-out (alone, or from a batch)."""
    client = _sse_client(MagicMock(), rt=_FakeRuntime())
    got = []
    for topic in ('task_status', 'task_status_batch'):
        client.register_notification_callback(
            lambda e, p, t, d: got.append(d), topic=topic)
    client._fan_out('ep', 'rhapsody', 'task_status',
                    {'uid': 't.1', 'state': 'DONE', 'sid': 'sid-rh'})
    client._fan_out('ep', 'rhapsody', 'task_status',
                    {'uid': 't.1', 'state': 'RUNNING', 'sid': 'sid-rh'})
    client._fan_out('ep', 'rhapsody', 'task_status_batch',
                    {'sid': 'sid-rh', 'tasks': [
                        {'uid': 't.1', 'state': 'RUNNING'},
                        {'uid': 't.2', 'state': 'RUNNING'}]})
    assert [d.get('state') or [x['uid'] for x in d['tasks']]
            for d in got] == ['DONE', ['t.2']]


def test_ensure_fan_out_failed_registration_changes_nothing():
    """A failed runtime registration raises and leaves the previous state:
    nothing registered at first, the old scope on a session switch; a
    later attempt registers (or moves) the callback."""
    rt   = _FakeRuntime()
    orig = rt.register_callback
    fail = [True]

    def _flaky(**kw):
        if fail[0]:
            raise RuntimeError('register failed')
        orig(**kw)

    rt.register_callback = _flaky
    client = _sse_client(MagicMock(), rt=rt)
    with pytest.raises(RuntimeError, match='register failed'):
        client._ensure_fan_out()
    assert rt.callbacks == [] and not client._fanout_on

    fail[0] = False
    client._ensure_fan_out()
    assert [s for _, _, _, s in rt.callbacks] == ['sid-rh']

    fail[0] = True
    client._sid = 'sid-next'
    with pytest.raises(RuntimeError, match='register failed'):
        client._ensure_fan_out()
    assert [s for _, _, _, s in rt.callbacks] == ['sid-rh']
    assert client._fanout_sid == 'sid-rh'
    fail[0] = False
    client._ensure_fan_out()
    assert [s for _, _, _, s in rt.callbacks] == ['sid-next']


def test_close_does_not_stall_on_a_stuck_reconcile():
    """close() waits only briefly for a stuck task_states call (which is
    bounded by a timeout), returns at once from inside a callback, and the
    stuck reconcile delivers nothing afterwards."""
    from radical.orbit import plugin_rhapsody as prh

    gate    = threading.Event()
    session = _done_session('sid-stuck', ['task.k1'])
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session, gate=gate)
    timeouts = []
    orig = http.post

    def _record(url, json=None, **kwargs):
        timeouts.append(kwargs.get('timeout'))
        return orig(url, json=json, **kwargs)

    http.post = _record
    got = []
    client.register_notification_callback(
        lambda *a: got.append(a), topic='task_status_batch')
    _track(client, ['task.k1'])
    client._reconcile_wake.set()
    assert _until(lambda: http.threads)
    assert timeouts == [prh.RECONCILE_TIMEOUT]

    with patch.object(prh, 'RECONCILE_JOIN', 0.2):
        t0 = time.monotonic()
        client.close()
        assert time.monotonic() - t0 < 1.0

    # from inside a callback (holding the delivery lock): no wait at all
    other, other_http = _reconciling_client(
        rt, _done_session('sid-stuck2', ['task.k2']), gate=gate)
    _track(other, ['task.k2'])
    other._reconcile_wake.set()
    assert _until(lambda: other_http.threads)
    t0 = time.monotonic()
    with other._delivering():
        other.close()
    assert time.monotonic() - t0 < 0.1

    gate.set()
    time.sleep(0.1)
    assert got == []                            # stopped: nothing delivered
    rt._cb.stop()


def test_task_states_terminal_only():
    """With terminal_only, running tasks are left out of `tasks` (and are
    not reported unknown)."""
    _, plugin, client = _make_plugin()
    running = MagicMock()
    running.uid, running.state = "task.r", "RUNNING"
    running.to_dict = lambda: {"uid": "task.r", "state": "RUNNING"}
    sid, _ = _states_session(plugin, client,
                             [running, _done_task("task.d", 1)])
    body = client.post(f"{plugin.namespace}/task_states/{sid}",
                       json={"uids": ["task.r", "task.d", "task.x"],
                             "terminal_only": True}).json()
    assert [t["uid"] for t in body["tasks"]] == ["task.d"]
    assert body["unknown"] == ["task.x"] and body["remaining"] == []


# ---------------------------------------------------------------------------
# Session-scoped routing and seq gap detection
# ---------------------------------------------------------------------------

def _bare_runtime(maxlen=1024):
    """A real EndpointRuntime registry and _on_event, with a real (not yet
    started) callback dispatcher and no transport."""
    from radical.orbit.runtime import EndpointRuntime
    from radical.orbit.runtime_support import CallbackDispatcher

    rt = EndpointRuntime.__new__(EndpointRuntime)
    rt._name      = 'consumer'
    rt._callbacks = {}
    rt._cb_lock   = threading.Lock()
    rt._emit      = MagicMock()
    rt._cb        = CallbackDispatcher(maxlen=maxlen)
    return rt


def _event(sid, payload, seq, topic='task_status'):
    from radical.orbit import protocol
    return protocol.Event(src='ep', plugin='rhapsody', topic=topic,
                          session=sid, seq=seq, ts=0.0,
                          data={**payload, 'sid': sid})


def test_two_clients_bursts_no_foreign_queue_entries():
    """Two clients, each on its own runtime with a callback queue of 4, get
    both sessions' bursts (the broadcast): the foreign events never take
    queue room, so neither drops and each gets exactly its own results."""
    uids     = [f'task.q{i}' for i in range(4)]
    sessions = [_done_session(sid, uids) for sid in ('sid-a', 'sid-b')]
    pairs    = []
    try:
        for session in sessions:
            rt = _bare_runtime(maxlen=4)
            client, _ = _reconciling_client(rt, session)
            _track(client, uids)
            pairs.append((rt, client))

        for i, uid in enumerate(uids):            # interleaved bursts
            for session in sessions:
                ev = _event(session.sid, session._notification_payload(
                    session._tasks[uid]), seq=i)
                for rt, _ in pairs:
                    rt._on_event(ev)

        for rt, client in pairs:
            assert rt._cb.dropped == 0
            assert rt._cb._q.qsize() == len(uids)   # own events only
            rt._cb.start()
        for rt, client in pairs:
            done = client.wait_tasks(uids, timeout=10)
            assert [t['return_value'] for t in done] \
                == [f'{client.sid}:{u}' for u in uids]
            assert client._seq_gaps == 0
    finally:
        for rt, client in pairs:
            client.close()
            rt._cb.stop()


def _tracking_client():
    """A client whose fan-out is not registered: _on_plugin_event is
    called directly, and the reconcile wake is observed, not run."""
    client = _make_rhapsody_client()
    client._sid = 'sid-t'
    return client


def _feed(client, seq, topic='task_status', session='sid-t'):
    client._reconcile_wake.clear()
    client._on_plugin_event('ep', 'rhapsody', topic,
                            {'uid': 'x', 'state': 'RUNNING'},
                            {'seq': seq, 'ts': 0.0, 'session': session})
    return client._reconcile_wake.is_set()


def test_seq_tracker_starts_at_first_seen_and_gap_wakes_reconcile():
    client = _tracking_client()
    assert _feed(client, 7) is False             # mid-stream join: no gap
    assert _feed(client, 8) is False
    assert _feed(client, 11) is True             # 9, 10 lost
    assert client._seq_gaps == 2
    assert _feed(client, 11) is False            # duplicate
    assert _feed(client, 12) is False


def test_seq_tracker_backwards_restart_needs_confirmation():
    """A backwards seq restarts the tracker only once the next event
    continues from it (broker restart); a single stale event is ignored."""
    client = _tracking_client()
    for seq in (40, 41):
        _feed(client, seq)
    assert _feed(client, 39) is False            # stale: baseline kept
    assert client._seq_last == 41
    assert _feed(client, 42) is False            # no gap after the stale one
    assert _feed(client, 0) is False             # broker restart ...
    assert client._seq_last == 42
    assert _feed(client, 1) is False             # ... confirmed
    assert client._seq_last == 1
    assert _feed(client, 3) is True
    assert client._seq_gaps == 1


def test_seq_tracker_duplicate_not_fanned_out():
    """The same event twice (both registrations of a scope move match it)
    reaches the fan-out once."""
    client = _tracking_client()
    got = []
    client._fanout['task_status'].append(lambda *a: got.append(a[3]['uid']))
    meta = {'seq': 3, 'ts': 0.0, 'session': 'sid-t'}
    for _ in range(2):
        client._on_plugin_event('ep', 'rhapsody', 'task_status',
                                {'uid': 'x', 'state': 'RUNNING',
                                 'sid': 'sid-t'}, meta)
    assert got == ['x']


def test_seq_tracker_reset():
    """A session switch or close restarts the tracker."""
    client = _tracking_client()
    _feed(client, 5)
    assert client._seq_last == 5
    client._reset_seq()
    assert client._seq_last is None
    assert _feed(client, 9) is False             # starts afresh


def test_seq_tracker_spans_topics_and_ignores_others():
    """One seq runs across all topics of a session: no false gaps.  Other
    sessions and session-less events are not tracked."""
    client = _tracking_client()
    topics = ['session_status', 'task_status', 'task_status_batch',
              'task_status', 'some_other_topic', 'task_status_batch']
    for seq, topic in enumerate(topics):
        assert _feed(client, seq, topic=topic) is False
    assert _feed(client, 50, session='sid-other') is False
    assert _feed(client, 99, session=None) is False
    assert _feed(client, len(topics)) is False
    assert client._seq_gaps == 0
    assert client._seq_last == len(topics)


def test_seq_gap_triggers_reconcile():
    """A broker drop seen as a seq gap recovers the lost completion at
    once — long before the idle timer would."""
    uids    = ['task.g0', 'task.g1', 'task.g2']
    session = _done_session('sid-gap', uids)
    rt      = _FakeRuntime()
    rt._cb.start()
    client, http = _reconciling_client(rt, session, idle=60.0)
    try:
        _track(client, uids)
        for seq, uid in enumerate(uids):
            if uid == 'task.g1':
                continue                         # lost in the broker
            payload = session._notification_payload(session._tasks[uid])
            rt.deliver('task_status', {**payload, 'sid': session.sid},
                       session=session.sid, seq=seq)
        done = client.wait_tasks(uids, timeout=10)
        assert [t['state'] for t in done] == ['DONE'] * 3
        assert client._seq_gaps == 1
        assert http.threads == ['orbit-rh-reconcile']
    finally:
        client.close()
        rt._cb.stop()


def test_register_session_moves_scope_before_waiting():
    """On a session switch the callback is scoped to the new sid before
    the client waits for session_status, so the new session's early
    events are not dropped as foreign."""
    rt     = _bare_runtime()
    resp   = MagicMock()
    resp.is_error, resp.status_code = False, 200
    resp.json = MagicMock(return_value={'sid': 'sid-new',
                                        'status': 'initializing'})
    http = MagicMock()
    http.post = MagicMock(return_value=resp)
    client = RhapsodyClient(http, '/rhapsody', broker_client=rt,
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid = 'sid-old'
    client._ensure_fan_out()
    client._seq_last = 7

    seen = []

    class _Outcomes(dict):
        def get(self, key, default=None):
            seen.append([s for _, _, s in
                         rt._callbacks[('ep', 'rhapsody', None)]])
            return ('ready', '')

    client._register_and_wait(None, 5, True, _Outcomes(), threading.Event())
    assert seen[0] == ['sid-new']
    assert client._seq_last is None


def test_close_keeps_callback_through_session_close():
    """close() unregisters the callback only after the session close, so
    the CANCELED states it produces are still delivered."""
    from radical.orbit.client import PluginClient

    rt     = _bare_runtime()
    client = RhapsodyClient(MagicMock(), '/rhapsody', broker_client=rt,
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid = 'sid-c'
    client._start_task_listener()
    during = []
    with patch.object(PluginClient, 'close',
                      lambda self: during.append(dict(rt._callbacks))):
        client.close()
    assert ('ep', 'rhapsody', None) in during[0]
    assert rt._callbacks == {}


def test_reregister_and_close_leave_no_stale_callbacks():
    """A session switch moves the one runtime callback to the new sid;
    close() removes it — no local callbacks or patterns pile up."""
    rt     = _bare_runtime()
    client = RhapsodyClient(MagicMock(), '/rhapsody', broker_client=rt,
                            endpoint_id='ep', plugin_name='rhapsody')
    client._sid = 'sid-a'
    client._start_task_listener()
    client._start_task_listener()                # idempotent
    key = ('ep', 'rhapsody', None)
    assert [s for _, _, s in rt._callbacks[key]] == ['sid-a']

    client._sid = 'sid-b'                        # what register_session does
    client._start_task_listener()
    assert [s for _, _, s in rt._callbacks[key]] == ['sid-b']

    client._sid = None                           # close: nothing to POST
    client.close()
    assert rt._callbacks == {}
    assert client._reconcile_thread is None

    # the shared pattern was subscribed once and unsubscribed once
    kinds = [type(c.args[0]).__name__ for c in rt._emit.call_args_list]
    assert kinds == ['Subscribe', 'Unsubscribe']


def test_two_clients_share_the_subscription_across_resync():
    """Two clients on one runtime share the (endpoint, plugin) pattern:
    closing one keeps the broker subscription — also after a fresh or a
    resumed resync — and closing the last removes it."""
    from radical.orbit.broker_events import EventRouter

    rt     = _bare_runtime()
    router = EventRouter({}, lambda *a: None, MagicMock(), 8, lambda: None)
    router._subscriptions['consumer'] = []

    def _apply():                               # the broker's side
        for c in rt._emit.call_args_list:
            msg = c.args[0]
            router.update_subscription(
                'consumer', {'patterns': [p.model_dump() for p in
                                          msg.patterns]},
                type(msg).__name__ == 'Subscribe')
        rt._emit.reset_mock()

    def _subscribed():
        return router._matches(router._subscriptions['consumer'],
                               'ep', 'rhapsody', 'task_status')

    clients = []
    for sid in ('sid-a', 'sid-b'):
        c = RhapsodyClient(MagicMock(), '/rhapsody', broker_client=rt,
                           endpoint_id='ep', plugin_name='rhapsody')
        c._sid = sid
        c._start_task_listener()
        clients.append(c)
    _apply()
    assert len(router._subscriptions['consumer']) == 1

    rt._resumed = True                          # resumed: patterns kept
    rt._resync_subscriptions()
    _apply()
    assert len(router._subscriptions['consumer']) == 1

    router._subscriptions['consumer'] = []      # fresh: broker forgot
    rt._resumed = False
    rt._resync_subscriptions()
    _apply()
    assert len(router._subscriptions['consumer']) == 1

    clients[0]._sid = None                      # nothing to POST on close
    clients[0].close()
    _apply()
    assert _subscribed()                        # the other client keeps it
    clients[1]._sid = None
    clients[1].close()
    _apply()
    assert router._subscriptions['consumer'] == []


_TIMERS: list = []


def _join_timers():
    while _TIMERS:
        _TIMERS.pop().join(timeout=5)


@pytest.fixture
def join_timers():
    """Join the register_session test timers even when the test fails."""
    yield
    _join_timers()


def _client_registering_as(sid, events_during_post):
    """A RhapsodyClient whose register_session POST returns *sid* while the
    given session_status events are delivered (some after a short delay;
    tests call ``_join_timers`` at the end)."""
    client = _make_rhapsody_client()
    client._bc, client._endpoint_id, client._plugin_name = MagicMock(), "ep", "rhapsody"
    cbs = []
    client.register_notification_callback = lambda cb, topic=None: cbs.append(cb)
    client.unregister_notification_callback = lambda cb, topic=None: None
    client._start_task_listener = lambda: None

    resp = MagicMock()
    resp.is_error, resp.status_code = False, 200
    resp.json = MagicMock(return_value={"sid": sid, "status": "initializing"})

    def post(url, json=None):
        for delay, data in events_during_post:
            fire = (lambda d=data: cbs[0]("ep", "rhapsody", "session_status", d))
            if delay:
                timer = threading.Timer(delay, fire)
                _TIMERS.append(timer)
                timer.start()
            else:
                fire()
        return resp

    client._http.post = post
    return client


def test_register_session_ignores_other_sessions_status(join_timers):
    """Another session's failed/ready event neither fails nor unblocks us."""
    client = _client_registering_as("s-b", [
        (0,   {"sid": "s-a", "status": "failed", "error": "not mine"}),
        (0,   {"sid": "s-a", "status": "ready"}),
        (0.2, {"sid": "s-b", "status": "ready"})])
    client.register_session(init_timeout=5)
    assert client.sid == "s-b"


def test_register_session_own_failure_raises(join_timers):
    client = _client_registering_as("s-b", [
        (0,   {"sid": "s-a", "status": "ready"}),
        (0.1, {"sid": "s-b", "status": "failed", "error": "boom"})])
    with pytest.raises(RuntimeError, match="boom"):
        client.register_session(init_timeout=5)


def test_register_session_other_ready_does_not_unblock():
    client = _client_registering_as("s-b", [(0, {"sid": "s-a", "status": "ready"})])
    with pytest.raises(RuntimeError, match="timed out"):
        client.register_session(init_timeout=0.5)


def test_register_session_sidless_status_accepted(join_timers):
    """An older endpoint sends session_status without sid: still accepted."""
    client = _client_registering_as("s-b", [(0.1, {"status": "ready"})])
    client.register_session(init_timeout=5)
    assert client.sid == "s-b"


def test_register_session_failed_post_unregisters_callback():
    """A failing POST must not leave the session_status callback behind."""
    client = _client_registering_as("s-b", [])
    removed = []
    client.unregister_notification_callback = \
        lambda cb, topic=None: removed.append(topic)

    def boom(url, json=None):
        raise RuntimeError("POST failed")

    client._http.post = boom
    with pytest.raises(RuntimeError, match="POST failed"):
        client.register_session(init_timeout=5)
    assert removed == ["session_status"]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])



def test_register_session_reconnect_to_failed_session_raises_at_once():
    """POST says 'failed' (reconnect to a failed session): raise the real
    error right away instead of waiting for an event that went out earlier."""
    client = _client_registering_as("s-b", [])
    resp = MagicMock()
    resp.is_error, resp.status_code = False, 200
    resp.json = MagicMock(return_value={"sid": "s-b", "status": "failed",
                                        "error": "dragon init boom"})
    client._http.post = lambda url, json=None: resp
    t0 = time.monotonic()
    with pytest.raises(RuntimeError, match="dragon init boom"):
        client.register_session(init_timeout=30)
    assert time.monotonic() - t0 < 5
