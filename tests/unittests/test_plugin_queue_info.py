#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.orbit
from radical.orbit.plugin_queue_info import PluginQueueInfo, QueueInfoSession

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


def test_queue_info_session_initialization():
    '''
    Test QueueInfoSession initialization.
    '''
    mock_backend = Mock()
    session = QueueInfoSession("test_session_001", backend=mock_backend)

    assert session._sid == "test_session_001"
    assert session._active is True
    assert session._backend == mock_backend


@pytest.mark.asyncio
async def test_queue_info_session_close():
    '''
    Test closing a QueueInfoSession.
    '''
    mock_backend = Mock()
    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.close()

    assert result == {}
    assert session._active is False
    # Backend is now shared, so it should still be set after close
    assert session._backend == mock_backend


@pytest.mark.asyncio
async def test_queue_info_session_get_info():
    '''
    Test getting queue info.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {"test": {}}})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.get_info()

    assert "queues" in result
    mock_backend.get_info.assert_called_once_with(user=None, force=False)


@pytest.mark.asyncio
async def test_queue_info_session_get_info_force():
    '''
    Test getting queue info with force refresh.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {}})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.get_info(force=True)

    mock_backend.get_info.assert_called_once_with(user=None, force=True)


@pytest.mark.asyncio
async def test_queue_info_session_get_info_closed_session():
    '''
    Test that get_info raises error when session is closed.
    '''
    mock_backend = Mock()
    session = QueueInfoSession("test_session_001", backend=mock_backend)
    await session.close()

    with pytest.raises(RuntimeError, match="session is closed"):
        await session.get_info()


@pytest.mark.asyncio
async def test_queue_info_session_list_jobs():
    '''
    Test listing jobs.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.list_jobs("test_queue")

    assert "jobs" in result
    mock_backend.list_jobs.assert_called_once_with("test_queue", None, False)


@pytest.mark.asyncio
async def test_queue_info_session_list_jobs_with_user():
    '''
    Test listing jobs filtered by user.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.list_jobs("test_queue", user="testuser", force=True)

    mock_backend.list_jobs.assert_called_once_with("test_queue", "testuser", True)


@pytest.mark.asyncio
async def test_queue_info_session_list_allocations():
    '''
    Test listing allocations.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.list_allocations()

    assert "allocations" in result
    mock_backend.list_allocations.assert_called_once_with(None, False)


@pytest.mark.asyncio
async def test_queue_info_session_list_allocations_with_user():
    '''
    Test listing allocations filtered by user.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})

    session = QueueInfoSession("test_session_001", backend=mock_backend)

    result = await session.list_allocations(user="testuser", force=True)

    mock_backend.list_allocations.assert_called_once_with("testuser", True)


@patch('radical.orbit.plugin_queue_info.make_queue_info')
def test_plugin_queue_info_initialization(mock_factory):
    '''
    Test PluginQueueInfo initialization.
    '''
    mock_backend = Mock()
    mock_factory.return_value = mock_backend

    app = FastAPI()
    plugin = PluginQueueInfo(app)

    assert plugin._instance_name == "queue_info"
    assert plugin._sessions == {}
    # Backend is now created at plugin level and shared
    assert plugin._backend == mock_backend
    mock_factory.assert_called_once_with(conf_path=None)

    # Check that direct-dispatch routes were registered
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    assert any("register_session" in p for p in route_pats)
    assert any("unregister_session" in p for p in route_pats)
    assert any("get_info" in p for p in route_pats)
    assert any("list_jobs" in p for p in route_pats)
    assert any("list_allocations" in p for p in route_pats)


@patch('radical.orbit.plugin_queue_info.make_queue_info')
def test_plugin_queue_info_custom_name_and_conf(mock_factory):
    '''
    Test PluginQueueInfo with custom name and backend config (slurm_conf
    alias is the legacy kwarg).
    '''
    mock_backend = Mock()
    mock_factory.return_value = mock_backend

    app = FastAPI()
    plugin = PluginQueueInfo(app, instance_name="custom_queue", slurm_conf="/custom/slurm.conf")

    assert plugin._instance_name == "custom_queue"
    # Backend is created with the conf path forwarded as conf_path
    mock_factory.assert_called_once_with(conf_path="/custom/slurm.conf")


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_register_session(mock_factory):
    '''
    Test registering a new session.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    request = Mock(spec=Request)

    data = await plugin.register_session(request)

    assert isinstance(data, dict)
    sid = data['sid']

    assert sid in plugin._sessions
    assert sid.startswith("session.")

    # Verify session created with backend
    mock_factory.assert_called_once()


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_unregister_session(mock_factory):
    '''
    Test unregistering a session.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Unregister it
    request.path_params = {"sid": sid}
    response = await plugin.unregister_session(request)

    assert isinstance(response, dict)
    assert sid not in plugin._sessions


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_get_info(mock_factory):
    '''
    Test get_info endpoint.
    '''
    mock_backend = Mock()
    mock_backend.get_info = Mock(return_value={"queues": {}})
    mock_factory.return_value = mock_backend

    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Get info
    request.path_params = {"sid": sid}
    request.query_params = {}

    response = await plugin.get_info(request)

    assert isinstance(response, dict)
    
    # Check backend call
    mock_backend.get_info.assert_called_with(user=None, force=False)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_list_jobs(mock_factory):
    '''
    Test list_jobs endpoint.
    '''
    mock_backend = Mock()
    mock_backend.list_jobs = Mock(return_value={"jobs": []})
    mock_factory.return_value = mock_backend

    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # List jobs
    request.path_params = {"sid": sid, "queue": "test_queue"}
    request.query_params = {}

    response = await plugin.list_jobs(request)

    assert isinstance(response, dict)
    
    # Check backend call
    mock_backend.list_jobs.assert_called_with("test_queue", None, False)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_list_allocations(mock_factory):
    '''
    Test list_allocations endpoint.
    '''
    mock_backend = Mock()
    mock_backend.list_allocations = Mock(return_value={"allocations": []})
    mock_factory.return_value = mock_backend

    app = FastAPI()
    plugin = PluginQueueInfo(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # List allocations
    request.path_params = {"sid": sid}
    request.query_params = {}

    response = await plugin.list_allocations(request)

    assert isinstance(response, dict)
    
    # Check backend call
    mock_backend.list_allocations.assert_called_with(None, False)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_queue_info.make_queue_info')
async def test_plugin_queue_info_unknown_session_error(mock_factory):
    '''
    Test that operations on unknown session raise HTTPException.
    '''
    app = FastAPI()
    plugin = PluginQueueInfo(app)

    request = Mock(spec=Request)
    request.path_params = {"sid": "unknown_session"}
    request.query_params = {}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.get_info(request)

    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# QueueInfoSession.cancel_job (Tier 2)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch('radical.orbit.batch_system_slurm.subprocess.run')
async def test_queue_info_session_cancel_job_success(mock_run):
    """cancel_job dispatches to the active batch system (SLURM here)."""
    from radical.orbit import batch_system as _bs
    from radical.orbit.batch_system_slurm import SlurmBatchSystem
    _bs._DETECTED = SlurmBatchSystem()
    try:
        mock_run.return_value = Mock(returncode=0, stderr='')
        session = QueueInfoSession("sid-cancel", backend=Mock())
        result = await session.cancel_job("12345")
        assert result == {'job_id': '12345', 'status': 'canceled'}
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert 'scancel' in args
        assert '12345' in args
    finally:
        _bs._DETECTED = None


@pytest.mark.asyncio
@patch('radical.orbit.batch_system_slurm.subprocess.run')
async def test_queue_info_session_cancel_job_failure(mock_run):
    """cancel_job raises HTTPException on scheduler failure."""
    from fastapi import HTTPException
    from radical.orbit import batch_system as _bs
    from radical.orbit.batch_system_slurm import SlurmBatchSystem
    _bs._DETECTED = SlurmBatchSystem()
    try:
        mock_run.return_value = Mock(returncode=1, stderr='Job not found')
        session = QueueInfoSession("sid-cancel2", backend=Mock())
        with pytest.raises(HTTPException) as exc_info:
            await session.cancel_job("99999")
        assert exc_info.value.status_code == 500
        assert "scancel failed" in exc_info.value.detail
    finally:
        _bs._DETECTED = None


@pytest.mark.asyncio
async def test_queue_info_session_list_all_jobs():
    """list_all_jobs delegates to backend.list_all_jobs."""
    mock_backend = Mock()
    mock_backend.list_all_jobs = Mock(return_value={"jobs": [{"id": "1"}]})
    session = QueueInfoSession("sid-alljobs", backend=mock_backend)
    result = await session.list_all_jobs(user="alice", force=True)
    assert result == {"jobs": [{"id": "1"}]}
    mock_backend.list_all_jobs.assert_called_once_with("alice", True)


# ---------------------------------------------------------------------------
# QueueInfoClient — session-less HTTP wrappers (Tier 1)
# ---------------------------------------------------------------------------

def _make_queue_info_client(json_resp, status_code=200):
    """Return a QueueInfoClient backed by a mock httpx.Client."""
    import httpx
    from radical.orbit.plugin_queue_info import QueueInfoClient
    mock_resp = Mock()
    mock_resp.is_error = (status_code >= 400)
    mock_resp.status_code = status_code
    mock_resp.json = Mock(return_value=json_resp)
    mock_http = Mock()
    mock_http.get = Mock(return_value=mock_resp)
    mock_http.post = Mock(return_value=mock_resp)
    client = QueueInfoClient(mock_http, "/queue_info")
    client._sid = "sid-123"
    return client



def test_queue_info_client_job_allocation_none():
    client = _make_queue_info_client({"allocation": None})
    assert client.job_allocation() is None


def test_queue_info_client_job_allocation_dict():
    alloc = {"n_nodes": 4, "runtime": 3600}
    client = _make_queue_info_client({"allocation": alloc})
    assert client.job_allocation() == alloc


def test_queue_info_client_cancel_job():
    client = _make_queue_info_client({"job_id": "42", "status": "canceled"})
    result = client.cancel_job("42")
    assert result["status"] == "canceled"
    client._http.post.assert_called_once()


def test_queue_info_client_list_all_jobs():
    client = _make_queue_info_client({"jobs": []})
    result = client.list_all_jobs()
    assert "jobs" in result


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
