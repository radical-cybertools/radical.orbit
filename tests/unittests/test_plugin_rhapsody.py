'''
Unit tests for the Rhapsody Edge plugin.

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

    with patch('radical.edge.plugin_rhapsody.rh', _mock_rh):
        yield

    # clean up
    sys.modules.pop('rhapsody', None)


# Now import after the mock is in place
from radical.edge.plugin_rhapsody import (  # noqa: E402
    PluginRhapsody,
    RhapsodySession,
    RhapsodyClient,
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
    """Non-JSON-serializable return_value must be stringified."""
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
    assert isinstance(d["return_value"], str)
    assert "DataReference" in d["return_value"]


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

    # Simulate what the Edge backend (edge.py) would do
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

    with patch('radical.edge.plugin_rhapsody.log') as mock_log, \
         patch('builtins.print') as mock_print:
        result = await session.close()

    assert result == {}
    assert session._active is False
    assert session._telemetry is None
    mock_rh_session.close.assert_called_once()
    mock_log.info.assert_called_once_with(expected)
    mock_print.assert_called_once_with(expected, flush=True)


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
    result = client.wait_tasks(["t.001"])
    assert isinstance(result, list)
    mock_call = client._http.post.call_args
    assert "uids" in mock_call[1]["json"]
    assert "timeout" not in mock_call[1]["json"]


def test_rhapsody_client_wait_tasks_with_timeout():
    client = _make_rhapsody_client([])
    client.wait_tasks(["t.001"], timeout=30.0)
    mock_call = client._http.post.call_args
    assert mock_call[1]["json"].get("timeout") == 30.0


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
# don't expose `Session.start_telemetry`.
# ---------------------------------------------------------------------------
#
# Bug surfaced during the local e2e smoke (memory/project_bridge_dispatcher.md):
# the rhapsody plugin called `self._rh_session.start_telemetry(...)`
# unconditionally; older rhapsody installs raise AttributeError.  Fix
# guards the call with `getattr(session, 'start_telemetry', None)`.

@pytest.mark.asyncio
async def test_initialize_without_start_telemetry(monkeypatch):
    '''Session init must succeed when the rhapsody Session has no
    ``start_telemetry`` method (older rhapsody installs lack it).
    Regression for the bug surfaced during the local e2e smoke.'''
    from radical.edge import plugin_rhapsody as prh

    fake_backend = MagicMock(spec=[])  # no __await__, no register_callback
    fake_session = MagicMock(spec=[])  # IMPORTANT: spec=[] → no start_telemetry

    monkeypatch.setattr(prh.rh, 'get_backend',
                        MagicMock(return_value=fake_backend))
    monkeypatch.setattr(prh.rh, 'Session',
                        MagicMock(return_value=fake_session))

    sess = RhapsodySession('sess.no_telem', backend_names=['fake'])
    # _REAL_INITIALIZE was captured at import time, before the autouse
    # stub fixture replaced RhapsodySession.initialize.  Bind it
    # manually to bypass the per-test stub.
    await _REAL_INITIALIZE(sess)

    assert sess._telemetry is None
    assert sess._rh_session is fake_session
    assert sess._init_error is None
    assert sess._init_ready.is_set()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
