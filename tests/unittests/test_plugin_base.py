#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.orbit
import radical.orbit
from radical.orbit.plugin_base import Plugin
from radical.orbit.plugin_session_base import PluginSession

from fastapi import FastAPI, HTTPException
from starlette.routing import Route
from starlette.requests import Request
from unittest.mock import Mock
import uuid
import time
import pytest


def test_plugin_init_subclass_collision(caplog):
    '''
    Test that duplicate plugin names emit a warning during subclassing.
    '''
    import logging
    # The 'radical.orbit' logger runs with propagate=False (see logging_config),
    # so caplog's root handler never sees its records.  Attach caplog's handler
    # to the logger directly to capture regardless of propagation.
    logger = logging.getLogger("radical.orbit")
    logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger="radical.orbit"):
            class CollidingPluginA(Plugin):
                plugin_name = "collide"
                session_class = PluginSession

            class CollidingPluginB(Plugin):
                plugin_name = "collide"
                session_class = PluginSession
    finally:
        logger.removeHandler(caplog.handler)

    assert "Duplicate plugin_name 'collide' - overwriting" in caplog.text


def test_plugin_initialization():
    '''
    Test that Plugin initializes correctly with app and name.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.instance_name == "test_plugin"
    assert isinstance(plugin._uid, str)
    # Verify it's a valid UUID
    assert uuid.UUID(plugin._uid)
    assert plugin._namespace == f"/{plugin.instance_name}"


def test_plugin_uid_property():
    '''
    Test that the uid property returns the correct UUID.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.uid == plugin._uid
    assert isinstance(plugin.uid, str)
    # Verify it's a valid UUID
    uuid.UUID(plugin.uid)


def test_plugin_namespace_property():
    '''
    Test that the namespace property returns the correct namespace.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    expected_namespace = "/test_plugin"
    assert plugin.namespace == expected_namespace


def test_plugin_add_route_post():
    '''
    Test adding a POST route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_count = len(app.state.direct_routes)
    plugin.add_route_post("/test", test_handler)

    # Verify a new direct-dispatch route was added
    assert len(app.state.direct_routes) == initial_count + 1

    # Check the last added route
    method, pattern, param_names, handler = app.state.direct_routes[-1]
    assert method == "POST"
    assert pattern.match(f"{plugin.namespace}/test")
    assert handler is test_handler


def test_plugin_add_route_get():
    '''
    Test adding a GET route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_count = len(app.state.direct_routes)
    plugin.add_route_get("/test", test_handler)

    # Verify a new direct-dispatch route was added
    assert len(app.state.direct_routes) == initial_count + 1

    # Check the last added route
    method, pattern, param_names, handler = app.state.direct_routes[-1]
    assert method == "GET"
    assert pattern.match(f"{plugin.namespace}/test")
    assert handler is test_handler


def test_plugin_route_path_normalization():
    '''
    Test that double slashes in paths are normalized.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    # Add route with leading slash
    plugin.add_route_post("/test", test_handler)
    _, pattern1, _, _ = app.state.direct_routes[-1]

    # Verify no double slashes in the regex pattern source
    assert "//" not in pattern1.pattern

    # Add route without leading slash
    plugin.add_route_get("test2", test_handler)
    _, pattern2, _, _ = app.state.direct_routes[-1]

    # Verify no double slashes
    assert "//" not in pattern2.pattern


def test_plugin_multiple_routes():
    '''
    Test adding multiple routes to the same plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def handler1():
        return {"endpoint": "1"}

    async def handler2():
        return {"endpoint": "2"}

    async def handler3():
        return {"endpoint": "3"}

    initial_count = len(app.state.direct_routes)

    plugin.add_route_post("/endpoint1", handler1)
    plugin.add_route_get("/endpoint2", handler2)
    plugin.add_route_post("/endpoint3", handler3)

    # Verify all routes were added
    assert len(app.state.direct_routes) == initial_count + 3

    # Verify all routes match the plugin namespace
    for _, pattern, _, _ in app.state.direct_routes[-3:]:
        assert plugin.namespace.lstrip('/') in pattern.pattern


@pytest.mark.asyncio
async def test_plugin_session_management():
    '''
    Test base plugin session management.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession  # required — no fallback

    # Mock request for registration
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']
    assert sid in plugin._sessions
    assert isinstance(plugin._sessions[sid], PluginSession)

    # Test unregister
    request.path_params = {"sid": sid}
    await plugin.unregister_session(request)
    assert sid not in plugin._sessions


def test_plugin_unique_uids():
    '''
    Test that each plugin instance gets a unique UID.
    '''
    app = FastAPI()
    plugin1 = Plugin(app, "test_plugin")
    plugin2 = Plugin(app, "test_plugin")
    plugin3 = Plugin(app, "another_plugin")

    # All UIDs should be different
    assert plugin1.uid != plugin2.uid
    assert plugin1.uid != plugin3.uid
    assert plugin2.uid != plugin3.uid

    # Namespaces will be the same if names are the same
    assert plugin1.namespace == "/test_plugin"
    assert plugin2.namespace == "/test_plugin"
    assert plugin3.namespace == "/another_plugin"


@pytest.mark.asyncio
async def test_plugin_health_check():
    '''
    Test the health check endpoint.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Register a session first
    request = Mock(spec=Request)
    await plugin.register_session(request)

    # Call health check
    data = await plugin.health_check(request)

    assert data['status'] == 'healthy'
    assert data['plugin'] == 'test_plugin'
    assert data['active_sessions'] == 1
    assert 'uptime_seconds' in data


@pytest.mark.asyncio
async def test_plugin_session_ttl_expiration():
    '''
    Test that sessions expire after TTL.
    '''
    import time
    from fastapi import HTTPException

    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 1  # 1 second TTL

    # Register session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Session should be accessible immediately
    assert sid in plugin._sessions

    # Wait for TTL to expire
    time.sleep(1.5)

    # Session should be expired now — any forwarded call returns 410
    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward(sid, PluginSession.close)
    assert exc_info.value.status_code == 410  # Gone


@pytest.mark.asyncio
async def test_plugin_session_cleanup():
    '''
    Test cleanup of expired sessions.
    '''
    import time

    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 1

    # Create some sessions manually
    plugin._sessions["old_session"] = PluginSession("old_session")
    plugin._session_last_access["old_session"] = time.time() - 100  # Expired

    plugin._sessions["new_session"] = PluginSession("new_session")
    plugin._session_last_access["new_session"] = time.time()  # Fresh

    # Run cleanup
    cleaned = await plugin._cleanup_expired_sessions()

    assert cleaned == 1
    assert "old_session" not in plugin._sessions
    assert "new_session" in plugin._sessions


def test_plugin_session_ttl_default():
    '''
    Test that session_ttl has a sensible default.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    # Default should be 3600 (1 hour)
    assert plugin.session_ttl == 3600


@pytest.mark.asyncio
async def test_plugin_ui_config_endpoint():
    '''
    Test the ui_config endpoint.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Call ui_config endpoint
    request = Mock(spec=Request)
    data = await plugin.get_ui_config(request)

    # Verify response structure
    assert 'plugin_name' in data
    assert 'instance_name' in data
    assert 'version' in data
    assert 'ui' in data
    assert data['instance_name'] == 'test_plugin'


@pytest.mark.asyncio
async def test_plugin_ui_config_with_custom_config():
    '''
    Test ui_config endpoint with a custom ui_config.
    '''

    class CustomPlugin(Plugin):
        plugin_name = "custom"
        session_class = PluginSession
        ui_config = {
            "icon": "🔧",
            "title": "Custom Plugin",
            "description": "A custom plugin"
        }

    app = FastAPI()
    plugin = CustomPlugin(app, "custom")

    request = Mock(spec=Request)
    data = await plugin.get_ui_config(request)

    assert data['plugin_name'] == 'custom'
    assert data['ui']['icon'] == '🔧'
    assert data['ui']['title'] == 'Custom Plugin'


def test_plugin_ui_config_default():
    '''
    Test that ui_config has a sensible default (None).
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    # Default should be None
    assert plugin.ui_config is None


if __name__ == '__main__':

    test_plugin_initialization()
    test_plugin_uid_property()
    test_plugin_namespace_property()
    test_plugin_add_route_post()
    test_plugin_add_route_get()
    test_plugin_route_path_normalization()
    test_plugin_multiple_routes()
    test_plugin_unique_uids()

    print("All tests passed!")



