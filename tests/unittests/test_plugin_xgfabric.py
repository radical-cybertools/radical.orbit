#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.orbit
from radical.orbit.plugin_xgfabric import PluginXGFabric, XGFabricSession

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse


def test_xgfabric_session_initialization():
    '''
    Test XGFabricSession initialization.
    '''
    session = XGFabricSession("test_session_001")

    assert session._sid == "test_session_001"
    assert session._active is True


@pytest.mark.asyncio
async def test_xgfabric_session_close():
    '''
    Test closing an XGFabricSession.
    '''
    session = XGFabricSession("test_session_001")

    result = await session.close()

    assert result == {}
    assert session._active is False


def test_plugin_xgfabric_initialization():
    '''
    Test PluginXGFabric initialization.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    assert plugin._instance_name == "xgfabric"
    assert plugin._sessions == {}
    # Check that direct-dispatch routes were registered
    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    assert any("register_session" in p for p in route_pats)
    assert any("unregister_session" in p for p in route_pats)



@pytest.mark.asyncio
async def test_plugin_xgfabric_register_session():
    '''
    Test registering a new session.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Mock request
    request = Mock(spec=Request)

    data = await plugin.register_session(request)

    assert isinstance(data, dict)
    sid = data['sid']
    assert sid in plugin._sessions


@pytest.mark.asyncio
async def test_plugin_xgfabric_register_multiple_sessions():
    '''
    Test registering multiple sessions.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    for _ in range(3):
        await plugin.register_session(request)

    assert len(plugin._sessions) == 3


@pytest.mark.asyncio
async def test_plugin_xgfabric_unregister_session():
    '''
    Test unregistering a session.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register a session first
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Unregister it
    request.path_params = {"sid": sid}
    response = await plugin.unregister_session(request)

    assert isinstance(response, dict)
    assert sid not in plugin._sessions


@pytest.mark.asyncio
async def test_plugin_xgfabric_unregister_unknown_session():
    '''
    Test unregistering an unknown session raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)
    request.path_params = {"sid": "unknown_session"}

    with pytest.raises(HTTPException) as exc_info:
        await plugin.unregister_session(request)

    assert exc_info.value.status_code == 404
    assert "unknown session id" in exc_info.value.detail


@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_unknown_session():
    '''
    Test _forward with unknown session raises HTTPException.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward("unknown_session", XGFabricSession.close)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_plugin_xgfabric_forward_session_error():
    '''
    Test _forward method when session method raises error.
    '''
    app = FastAPI()
    plugin = PluginXGFabric(app)

    # Register and close a session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']
    
    await plugin._sessions[sid].close()

    # Try to use closed session — _check_active raises RuntimeError
    async def _failing_method(self):
        self._check_active()
        return {}

    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward(sid, _failing_method)

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_plugin_xgfabric_concurrent_registration():
    '''
    Test that concurrent session registration produces unique IDs.
    '''
    import asyncio

    app = FastAPI()
    plugin = PluginXGFabric(app)

    request = Mock(spec=Request)

    # Register multiple sessions concurrently
    tasks = [plugin.register_session(request) for _ in range(10)]
    await asyncio.gather(*tasks)

    # All should have unique IDs
    assert len(plugin._sessions) == 10


if __name__ == '__main__':

    pytest.main([__file__, '-v'])


# ---------------------------------------------------------------------------
# Config helper functions (pure, no I/O)
# ---------------------------------------------------------------------------

from radical.orbit.plugin_xgfabric import (
    ResourceConfig, WorkflowConfig,
    dict_to_resource_config, dict_to_config, config_to_dict,
    XGFabricClient,
)


def test_dict_to_resource_config_basic():
    d = {"name": "test", "bridge_url": "https://host:9000"}
    cfg = dict_to_resource_config(d)
    assert isinstance(cfg, ResourceConfig)
    assert cfg.name == "test"
    assert cfg.bridge_url == "https://host:9000"


def test_dict_to_resource_config_unknown_fields_ignored():
    d = {"name": "x", "unknown_field": "should_be_dropped"}
    cfg = dict_to_resource_config(d)
    assert cfg.name == "x"
    assert not hasattr(cfg, "unknown_field")


def test_dict_to_resource_config_defaults():
    cfg = dict_to_resource_config({})
    assert cfg.name == "default"
    assert cfg.bridge_cert is None


def test_dict_to_config_basic():
    d = {"name": "wf1", "num_simulations": 8}
    cfg = dict_to_config(d)
    assert isinstance(cfg, WorkflowConfig)
    assert cfg.name == "wf1"
    assert cfg.num_simulations == 8


def test_dict_to_config_unknown_fields_ignored():
    d = {"name": "wf", "not_a_field": 99}
    cfg = dict_to_config(d)
    assert cfg.name == "wf"
    assert not hasattr(cfg, "not_a_field")


def test_dict_to_config_int_conversion():
    """String numbers for int fields must be converted."""
    d = {"cspot_limit": "5", "num_simulations": "32", "batch_size": "8"}
    cfg = dict_to_config(d)
    assert cfg.cspot_limit == 5
    assert cfg.num_simulations == 32
    assert cfg.batch_size == 8


def test_config_to_dict_roundtrip():
    """config_to_dict → dict_to_config roundtrip preserves values."""
    original = WorkflowConfig(name="rt", cspot_limit=7, batch_size=2)
    d = config_to_dict(original)
    restored = dict_to_config(d)
    assert restored.name == original.name
    assert restored.cspot_limit == original.cspot_limit
    assert restored.batch_size == original.batch_size


# ---------------------------------------------------------------------------
# XGFabricClient — thin HTTP wrappers
# ---------------------------------------------------------------------------

def _make_xgfabric_client(json_resp=None, status_code=200):
    from unittest.mock import Mock
    if json_resp is None:
        json_resp = {"ok": True}
    mock_resp = Mock()
    mock_resp.is_error = (status_code >= 400)
    mock_resp.status_code = status_code
    mock_resp.json = Mock(return_value=json_resp)
    mock_http = Mock()
    mock_http.get = Mock(return_value=mock_resp)
    mock_http.post = Mock(return_value=mock_resp)
    client = XGFabricClient(mock_http, "/xgfabric")
    client._sid = "sid-xgf"
    return client


def test_xgfabric_client_get_workdir():
    client = _make_xgfabric_client({"workdir": "/data"})
    result = client.get_workdir()
    assert result["workdir"] == "/data"
    client._http.get.assert_called_once()


def test_xgfabric_client_set_workdir():
    client = _make_xgfabric_client({"path": "/new"})
    result = client.set_workdir("/new")
    client._http.post.assert_called_once()
    call_kwargs = client._http.post.call_args
    assert call_kwargs[1]["json"]["path"] == "/new"


def test_xgfabric_client_list_configs():
    client = _make_xgfabric_client([{"name": "cfg1"}])
    result = client.list_configs()
    assert isinstance(result, list)
    client._http.get.assert_called_once()


def test_xgfabric_client_load_config():
    client = _make_xgfabric_client({"name": "cfg1", "num_simulations": 16})
    result = client.load_config("cfg1")
    assert result["name"] == "cfg1"
    client._http.get.assert_called_once()


def test_xgfabric_client_save_config():
    client = _make_xgfabric_client({"saved": True})
    client.save_config({"name": "new_cfg"})
    client._http.post.assert_called_once()


def test_xgfabric_client_delete_config():
    client = _make_xgfabric_client({"deleted": True})
    client.delete_config("old_cfg")
    client._http.post.assert_called_once()


def test_xgfabric_client_get_status():
    status = {"state": "idle", "immediate_clusters": [], "allocate_clusters": []}
    client = _make_xgfabric_client(status)
    result = client.get_status()
    assert result["state"] == "idle"


def test_xgfabric_client_start_workflow():
    client = _make_xgfabric_client({"started": True})
    client.start_workflow(workflow="wf1", resource="res1")
    call_kwargs = client._http.post.call_args
    assert call_kwargs[1]["json"]["workflow"] == "wf1"


def test_xgfabric_client_stop_workflow():
    client = _make_xgfabric_client({"stopped": True})
    client.stop_workflow()
    client._http.post.assert_called_once()



