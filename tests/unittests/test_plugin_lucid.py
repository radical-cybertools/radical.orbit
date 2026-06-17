#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import pytest

pytest.importorskip('radical.pilot')

import radical.orbit
from radical.orbit.plugin_lucid import PluginLucid, LucidSession

from unittest.mock import Mock, AsyncMock, patch, MagicMock
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


@patch('radical.orbit.plugin_lucid.rp')
def test_lucid_session_initialization(mock_rp):
    '''
    Test LucidSession initialization.
    '''
    # Mock radical.pilot objects
    mock_session = Mock()
    mock_pmgr = Mock()
    mock_tmgr = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr

    session = LucidSession("test_session_001")

    assert session._sid == "test_session_001"
    assert session._session == mock_session
    assert session._pmgr == mock_pmgr
    assert session._tmgr == mock_tmgr

    mock_rp.Session.assert_called_once()
    mock_rp.PilotManager.assert_called_once_with(session=mock_session)
    mock_rp.TaskManager.assert_called_once_with(session=mock_session)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_lucid_session_close(mock_rp):
    '''
    Test closing a LucidSession.
    '''
    mock_session = Mock()
    mock_session.close = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    session = LucidSession("test_session_001")

    result = await session.close()

    assert result == {}
    assert session._session is None
    assert session._pmgr is None
    assert session._tmgr is None


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_lucid_session_pilot_submit(mock_rp):
    '''
    Test submitting a pilot.
    '''
    mock_pilot = Mock()
    mock_pilot.uid = "pilot.0000"

    mock_pmgr = Mock()
    mock_pmgr.submit_pilots = Mock(return_value=mock_pilot)

    mock_tmgr = Mock()
    mock_tmgr.add_pilots = Mock()

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.PilotDescription.return_value = Mock()

    session = LucidSession("test_session_001")

    description = {"resource": "local.localhost", "cores": 4}
    result = await session.pilot_submit(description)

    assert result == {"pid": "pilot.0000"}
    mock_rp.PilotDescription.assert_called_once_with(description)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_lucid_session_pilot_submit_closed_session(mock_rp):
    '''
    Test that pilot_submit raises error when session is closed.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    session = LucidSession("test_session_001")
    await session.close()

    with pytest.raises(RuntimeError, match="session is closed"):
        await session.pilot_submit({})


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_lucid_session_task_submit(mock_rp):
    '''
    Test submitting a task.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"

    mock_tmgr = Mock()
    mock_tmgr.submit_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.TaskDescription.return_value = Mock()

    session = LucidSession("test_session_001")

    description = {"executable": "/bin/echo", "arguments": ["hello"]}
    result = await session.task_submit(description)

    assert result == {"tid": "task.0000"}
    mock_rp.TaskDescription.assert_called_once_with(description)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_lucid_session_task_wait(mock_rp):
    '''
    Test waiting for a task.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"
    mock_task.state = "DONE"
    mock_task.as_dict.return_value = {"uid": "task.0000", "state": "DONE"}

    mock_tmgr = Mock()
    mock_tmgr.wait_tasks = Mock()
    mock_tmgr.get_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr

    session = LucidSession("test_session_001")

    result = await session.task_wait("task.0000")

    assert result["tid"] == "task.0000"
    assert result["task"]["state"] == "DONE"


@patch('radical.orbit.plugin_lucid.rp')
def test_plugin_lucid_initialization(mock_rp):
    '''
    Test PluginLucid initialization.
    '''
    app = FastAPI()
    plugin = PluginLucid(app)

    assert plugin._instance_name == "lucid"
    assert plugin._sessions == {}
    # Check that direct-dispatch routes were registered
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    assert any("register_session" in p for p in route_pats)
    assert any("unregister_session" in p for p in route_pats)
    assert any("pilot_submit" in p for p in route_pats)
    assert any("task_submit" in p for p in route_pats)
    assert any("task_wait" in p for p in route_pats)
    assert any("version" in p for p in route_pats)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_plugin_lucid_register_session(mock_rp):
    '''
    Test registering a new session.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    request = Mock(spec=Request)

    data = await plugin.register_session(request)

    assert isinstance(data, dict)
    sid = data['sid']

    assert sid in plugin._sessions


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_plugin_lucid_unregister_session(mock_rp):
    '''
    Test unregistering a session.
    '''
    mock_rp.Session.return_value = Mock()
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

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
@patch('radical.orbit.plugin_lucid.rp')
async def test_plugin_lucid_pilot_submit(mock_rp):
    '''
    Test pilot submission endpoint.
    '''
    mock_pilot = Mock()
    mock_pilot.uid = "pilot.0000"

    mock_pmgr = Mock()
    mock_pmgr.submit_pilots = Mock(return_value=mock_pilot)

    mock_tmgr = Mock()
    mock_tmgr.add_pilots = Mock()

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = mock_pmgr
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.PilotDescription.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Submit pilot
    request.path_params = {"sid": sid}
    request.json = AsyncMock(return_value={"description": {"resource": "local"}})

    response = await plugin.pilot_submit(request)

    assert isinstance(response, dict)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_plugin_lucid_task_submit(mock_rp):
    '''
    Test task submission endpoint.
    '''
    mock_task = Mock()
    mock_task.uid = "task.0000"

    mock_tmgr = Mock()
    mock_tmgr.submit_tasks = Mock(return_value=mock_task)

    mock_session = Mock()

    mock_rp.Session.return_value = mock_session
    mock_rp.PilotManager.return_value = Mock()
    mock_rp.TaskManager.return_value = mock_tmgr
    mock_rp.TaskDescription.return_value = Mock()

    app = FastAPI()
    plugin = PluginLucid(app)

    # Register a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Submit task
    request.path_params = {"sid": sid}
    request.json = AsyncMock(return_value={"description": {"executable": "/bin/echo"}})

    response = await plugin.task_submit(request)

    assert isinstance(response, dict)


@pytest.mark.asyncio
@patch('radical.orbit.plugin_lucid.rp')
async def test_plugin_lucid_unknown_session_error(mock_rp):
    '''
    Test that operations on unknown session raise HTTPException.
    '''
    app = FastAPI()
    plugin = PluginLucid(app)

    from radical.orbit.plugin_session_base import PluginSession

    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward("unknown_session", PluginSession.close)

    assert exc_info.value.status_code == 404


if __name__ == '__main__':

    pytest.main([__file__, '-v'])



