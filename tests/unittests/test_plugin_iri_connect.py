#!/usr/bin/env python

# pylint: disable=protected-access

import pytest

from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI

from radical.edge.plugin_base        import Plugin
from radical.edge.plugin_iri_connect import PluginIRIConnect


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _MockHost:
    """Minimal stand-in for BridgePluginHost."""

    def __init__(self):
        self._plugins = {}

    async def register_dynamic_plugin(self, cls, name, **kwargs):
        plugin = MagicMock()
        plugin.instance_name = name
        plugin._endpoint_key = kwargs.get('endpoint', '')
        plugin.version       = '0.0.1'
        self._plugins[name]  = plugin
        return plugin

    async def deregister_dynamic_plugin(self, name):
        self._plugins.pop(name, None)


@pytest.fixture
def bridge_app():
    app  = FastAPI()
    host = _MockHost()
    app.state.is_bridge    = True
    app.state.edge_service = host
    app.state.edge_name    = 'bridge'
    app.state.bridge_url   = ''
    return app, host


@pytest.fixture(autouse=True)
def _cleanup_registry():
    yield
    Plugin._registry.pop('iri_connect', None)


# ---------------------------------------------------------------------------
# Plugin basics
# ---------------------------------------------------------------------------

def test_is_enabled_on_bridge(bridge_app):
    app, _ = bridge_app
    assert PluginIRIConnect.is_enabled(app) is True


def test_is_disabled_on_edge():
    app = FastAPI()
    app.state.is_bridge = False
    assert PluginIRIConnect.is_enabled(app) is False


def test_init(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)
    assert plugin.instance_name == 'iri_connect'


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_endpoints(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)
    request = MagicMock()
    result = await plugin.list_endpoints(request)
    assert 'nersc' in result
    assert 'olcf'  in result
    assert result['nersc']['connected'] is False


# ---------------------------------------------------------------------------
# Connect / Disconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect(bridge_app):
    app, host = bridge_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'tok123'})
    result = await plugin.connect(request)
    assert result['instance'] == 'iri.nersc'
    assert result['status']   == 'connected'
    assert 'iri.nersc' in host._plugins


@pytest.mark.asyncio
async def test_connect_reconnect_updates_token(bridge_app):
    """A second connect for the same endpoint refreshes the bearer token
    in place (no 409) — clients can rotate stale credentials without
    disconnecting first."""
    app, host = bridge_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'old-token'})
    first = await plugin.connect(request)
    assert first['status']   == 'connected'
    assert first['instance'] == 'iri.nersc'

    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'new-token'})
    second = await plugin.connect(request)
    assert second['status']   == 'token_updated'
    assert second['instance'] == 'iri.nersc'

    # Same plugin instance kept; update_token was called with the new
    # value.  (The semantics of update_token itself live in
    # test_plugin_iri_instance.py — this test is about the connect route
    # contract.)
    host._plugins['iri.nersc'].update_token.assert_called_once_with('new-token')


@pytest.mark.asyncio
async def test_connect_bad_endpoint(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'bogus', 'token': 'tok'})
    with pytest.raises(Exception, match='Unknown endpoint'):
        await plugin.connect(request)


@pytest.mark.asyncio
async def test_connect_empty_token(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': ''})
    with pytest.raises(Exception, match='token must not be empty'):
        await plugin.connect(request)


@pytest.mark.asyncio
async def test_disconnect(bridge_app):
    app, host = bridge_app
    plugin = PluginIRIConnect(app)

    # Connect first
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'tok'})
    await plugin.connect(request)
    assert 'iri.nersc' in host._plugins

    # Disconnect
    request2 = MagicMock()
    request2.path_params = {'name': 'nersc'}
    result = await plugin.disconnect(request2)
    assert result['status'] == 'disconnected'
    assert 'iri.nersc' not in host._plugins


@pytest.mark.asyncio
async def test_disconnect_not_found(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.path_params = {'name': 'nersc'}
    with pytest.raises(Exception, match='not connected'):
        await plugin.disconnect(request)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status(bridge_app):
    app, host = bridge_app
    plugin = PluginIRIConnect(app)

    # Connect one endpoint
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'tok'})
    await plugin.connect(request)

    # Check status
    request2 = MagicMock()
    result = await plugin.get_status(request2)
    assert 'iri.nersc' in result['instances']


# ---------------------------------------------------------------------------
# register_session (dummy)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_session_dummy(bridge_app):
    app, _ = bridge_app
    plugin = PluginIRIConnect(app)
    request = MagicMock()
    result = await plugin.register_session(request)
    assert 'sid' in result
