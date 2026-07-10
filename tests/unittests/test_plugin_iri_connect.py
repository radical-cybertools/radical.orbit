#!/usr/bin/env python

# pylint: disable=protected-access

import pytest

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException

from radical.orbit                     import plugin_sfapi_instance as psf
from radical.orbit.plugin_base         import Plugin
from radical.orbit.plugin_iri_connect  import PluginIRIConnect


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _MockHost:
    """Minimal stand-in for BrokerPluginHost."""

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
def broker_app():
    app  = FastAPI()
    host = _MockHost()
    app.state.is_broker    = True
    app.state.endpoint_service = host
    app.state.endpoint_name    = 'broker'
    app.state.broker_url   = ''
    return app, host


@pytest.fixture(autouse=True)
def _cleanup_registry():
    yield
    Plugin._registry.pop('iri_connect', None)


# ---------------------------------------------------------------------------
# Plugin basics
# ---------------------------------------------------------------------------

def test_is_enabled_on_broker(broker_app):
    app, _ = broker_app
    assert PluginIRIConnect.is_enabled(app) is True


def test_is_disabled_on_endpoint():
    app = FastAPI()
    app.state.is_broker = False
    assert PluginIRIConnect.is_enabled(app) is False


def test_init(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)
    assert plugin.instance_name == 'iri_connect'


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_endpoints(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)
    request = MagicMock()
    result = await plugin.list_endpoints(request)
    assert 'nersc' in result
    assert 'olcf'  in result
    assert 'nersc-sfapi' in result
    assert result['nersc']['connected']        is False
    assert result['nersc-sfapi']['auth']       == 'sfapi'


# ---------------------------------------------------------------------------
# Connect / Disconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect(broker_app):
    app, host = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': 'tok123'})
    result = await plugin.connect(request)
    assert result['instance'] == 'iri.nersc'
    assert result['status']   == 'connected'
    assert 'iri.nersc' in host._plugins


@pytest.mark.asyncio
async def test_connect_reconnect_updates_token(broker_app):
    """A second connect for the same endpoint refreshes the bearer token
    in place (no 409) — clients can rotate stale credentials without
    disconnecting first."""
    app, host = broker_app
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
async def test_connect_bad_endpoint(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'bogus', 'token': 'tok'})
    with pytest.raises(Exception, match='Unknown endpoint'):
        await plugin.connect(request)


@pytest.mark.asyncio
async def test_connect_empty_token(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'token': ''})
    with pytest.raises(Exception, match='token must not be empty'):
        await plugin.connect(request)


@pytest.mark.asyncio
async def test_disconnect(broker_app):
    app, host = broker_app
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
async def test_disconnect_not_found(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.path_params = {'name': 'nersc'}
    with pytest.raises(Exception, match='not connected'):
        await plugin.disconnect(request)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status(broker_app):
    app, host = broker_app
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
async def test_register_session_dummy(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)
    request = MagicMock()
    result = await plugin.register_session(request)
    assert 'sid' in result


# ---------------------------------------------------------------------------
# SFAPI connect dispatch (auth == 'sfapi')
# ---------------------------------------------------------------------------

def _mock_token_manager():
    '''Patch SFAPITokenManager with a stub whose mint() succeeds.'''
    mgr = MagicMock()
    mgr.mint   = AsyncMock(return_value='tok')
    mgr.aclose = AsyncMock()
    return mgr


@pytest.mark.asyncio
async def test_connect_sfapi_missing_credentials(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={'endpoint': 'nersc-sfapi'})
    with pytest.raises(HTTPException) as ei:
        await plugin.connect(request)
    assert ei.value.status_code == 400
    assert 'client_id' in ei.value.detail


@pytest.mark.asyncio
async def test_connect_sfapi_success_registers_instance(broker_app):
    app, host = broker_app
    plugin = PluginIRIConnect(app)

    mgr = _mock_token_manager()
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc-sfapi', 'client_id': 'cid',
        'private_key': 'PEM'})

    with patch.object(psf, 'SFAPITokenManager', return_value=mgr):
        result = await plugin.connect(request)

    assert result['status']   == 'connected'
    assert result['instance'] == 'iri.nersc-sfapi'
    assert 'iri.nersc-sfapi' in host._plugins
    mgr.mint.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_sfapi_eager_mint_failure_not_registered(broker_app):
    app, host = broker_app
    plugin = PluginIRIConnect(app)

    mgr = MagicMock()
    mgr.mint   = AsyncMock(side_effect=HTTPException(status_code=401,
                                                     detail='bad creds'))
    mgr.aclose = AsyncMock()
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc-sfapi', 'client_id': 'cid',
        'private_key': 'PEM'})

    with patch.object(psf, 'SFAPITokenManager', return_value=mgr):
        with pytest.raises(HTTPException) as ei:
            await plugin.connect(request)

    assert ei.value.status_code == 401
    # mint-before-register: nothing was registered, nothing to deregister
    assert 'iri.nersc-sfapi' not in host._plugins
    mgr.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_sfapi_reconnect_rotates_credentials(broker_app):
    app, host = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc-sfapi', 'client_id': 'cid',
        'private_key': 'PEM'})
    with patch.object(psf, 'SFAPITokenManager',
                      return_value=_mock_token_manager()):
        await plugin.connect(request)

    # second connect with new creds -> credentials_updated
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc-sfapi', 'client_id': 'cid2',
        'private_key': 'PEM2'})
    with patch.object(psf, 'SFAPITokenManager',
                      return_value=_mock_token_manager()):
        result = await plugin.connect(request)

    assert result['status'] == 'credentials_updated'
    host._plugins['iri.nersc-sfapi'].update_credentials \
        .assert_called_once_with('cid2', 'PEM2')


@pytest.mark.asyncio
async def test_connect_sfapi_importerror_maps_to_501(broker_app):
    app, _ = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc-sfapi', 'client_id': 'cid',
        'private_key': 'PEM'})

    # _HAVE_AUTHLIB False -> real SFAPITokenManager ctor raises ImportError
    with patch.object(psf, '_HAVE_AUTHLIB', False):
        with pytest.raises(HTTPException) as ei:
            await plugin.connect(request)
    assert ei.value.status_code == 501


@pytest.mark.asyncio
async def test_connect_bearer_endpoints_unaffected(broker_app):
    '''globus / s3m endpoints still take the bearer path unchanged.'''
    app, host = broker_app
    plugin = PluginIRIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={'endpoint': 'olcf', 'token': 't'})
    result = await plugin.connect(request)
    assert result['status'] == 'connected'
    assert 'iri.olcf' in host._plugins
