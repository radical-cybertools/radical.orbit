#!/usr/bin/env python

# pylint: disable=protected-access

import pytest

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException

from radical.orbit                      import plugin_sfapi_instance as psf
from radical.orbit.plugin_base          import Plugin
from radical.orbit.plugin_sfapi_connect import PluginSFAPIConnect


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
    app.state.is_broker        = True
    app.state.endpoint_service = host
    app.state.endpoint_name    = 'broker'
    app.state.broker_url       = ''
    return app, host


@pytest.fixture(autouse=True)
def _cleanup_registry():
    yield
    Plugin._registry.pop('sfapi_connect', None)


def _mock_token_manager():
    '''A SFAPITokenManager stub whose mint() succeeds.'''
    mgr = MagicMock()
    mgr.mint   = AsyncMock(return_value='tok')
    mgr.aclose = AsyncMock()
    return mgr


# ---------------------------------------------------------------------------
# Plugin basics
# ---------------------------------------------------------------------------

def test_is_enabled_on_broker(broker_app):
    app, _ = broker_app
    assert PluginSFAPIConnect.is_enabled(app) is True


def test_is_disabled_on_endpoint():
    app = FastAPI()
    app.state.is_broker = False
    assert PluginSFAPIConnect.is_enabled(app) is False


def test_init(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)
    assert plugin.instance_name == 'sfapi_connect'


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_endpoints(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)
    request = MagicMock()
    result = await plugin.list_endpoints(request)
    assert 'nersc' in result
    assert result['nersc']['auth']      == 'sfapi'
    assert result['nersc']['connected'] is False
    assert 'api.nersc.gov' in result['nersc']['url']


# ---------------------------------------------------------------------------
# Connect / disconnect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_bad_endpoint(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'bogus', 'client_id': 'cid', 'private_key': 'PEM'})
    with pytest.raises(HTTPException) as ei:
        await plugin.connect(request)
    assert ei.value.status_code == 400
    assert 'Unknown endpoint' in ei.value.detail


@pytest.mark.asyncio
async def test_connect_missing_credentials(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={'endpoint': 'nersc'})
    with pytest.raises(HTTPException) as ei:
        await plugin.connect(request)
    assert ei.value.status_code == 400
    assert 'client_id' in ei.value.detail


@pytest.mark.asyncio
async def test_connect_success_registers_instance(broker_app):
    app, host = broker_app
    plugin = PluginSFAPIConnect(app)

    mgr = _mock_token_manager()
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})

    with patch.object(psf, 'SFAPITokenManager', return_value=mgr):
        result = await plugin.connect(request)

    assert result['status']   == 'connected'
    assert result['instance'] == 'sfapi.nersc'
    assert 'sfapi.nersc' in host._plugins
    mgr.mint.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_eager_mint_failure_not_registered(broker_app):
    app, host = broker_app
    plugin = PluginSFAPIConnect(app)

    mgr = MagicMock()
    mgr.mint   = AsyncMock(side_effect=HTTPException(status_code=401,
                                                     detail='bad creds'))
    mgr.aclose = AsyncMock()
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})

    with patch.object(psf, 'SFAPITokenManager', return_value=mgr):
        with pytest.raises(HTTPException) as ei:
            await plugin.connect(request)

    assert ei.value.status_code == 401
    # mint-before-register: nothing was registered, nothing to deregister
    assert 'sfapi.nersc' not in host._plugins
    mgr.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_connect_reconnect_rotates_credentials(broker_app):
    app, host = broker_app
    plugin = PluginSFAPIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})
    with patch.object(psf, 'SFAPITokenManager',
                      return_value=_mock_token_manager()):
        first = await plugin.connect(request)
    assert first['status'] == 'connected'

    # second connect with new creds -> credentials_updated (verified first)
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid2', 'private_key': 'PEM2'})
    mgr2 = _mock_token_manager()
    with patch.object(psf, 'SFAPITokenManager', return_value=mgr2):
        result = await plugin.connect(request)

    assert result['status'] == 'credentials_updated'
    # new creds minted (verified) BEFORE rotating, and the manager closed
    mgr2.mint.assert_awaited_once()
    mgr2.aclose.assert_awaited_once()
    host._plugins['sfapi.nersc'].update_credentials \
        .assert_called_once_with('cid2', 'PEM2')


@pytest.mark.asyncio
async def test_connect_importerror_maps_to_501(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)

    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})

    # _HAVE_AUTHLIB False -> real SFAPITokenManager ctor raises ImportError
    with patch.object(psf, '_HAVE_AUTHLIB', False):
        with pytest.raises(HTTPException) as ei:
            await plugin.connect(request)
    assert ei.value.status_code == 501


@pytest.mark.asyncio
async def test_disconnect(broker_app):
    app, host = broker_app
    plugin = PluginSFAPIConnect(app)

    # Connect first
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})
    with patch.object(psf, 'SFAPITokenManager',
                      return_value=_mock_token_manager()):
        await plugin.connect(request)
    assert 'sfapi.nersc' in host._plugins

    # Disconnect
    request2 = MagicMock()
    request2.path_params = {'name': 'nersc'}
    result = await plugin.disconnect(request2)
    assert result['status'] == 'disconnected'
    assert 'sfapi.nersc' not in host._plugins


@pytest.mark.asyncio
async def test_disconnect_not_found(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)

    request = MagicMock()
    request.path_params = {'name': 'nersc'}
    with pytest.raises(HTTPException, match='not connected'):
        await plugin.disconnect(request)


# ---------------------------------------------------------------------------
# Status (sfapi.* filter)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_status_filters_own_prefix(broker_app):
    app, host = broker_app
    plugin = PluginSFAPIConnect(app)

    # Connect one sfapi endpoint
    request = MagicMock()
    request.json = AsyncMock(return_value={
        'endpoint': 'nersc', 'client_id': 'cid', 'private_key': 'PEM'})
    with patch.object(psf, 'SFAPITokenManager',
                      return_value=_mock_token_manager()):
        await plugin.connect(request)

    # A foreign iri.* instance must NOT leak into sfapi_connect's status
    host._plugins['iri.nersc'] = MagicMock(version='0.0.1', _endpoint_key='n')

    result = await plugin.get_status(MagicMock())
    assert 'sfapi.nersc' in result['instances']
    assert 'iri.nersc'   not in result['instances']


# ---------------------------------------------------------------------------
# register_session (dummy)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_session_dummy(broker_app):
    app, _ = broker_app
    plugin = PluginSFAPIConnect(app)
    result = await plugin.register_session(MagicMock())
    assert 'sid' in result
