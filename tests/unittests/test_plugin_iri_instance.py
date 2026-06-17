#!/usr/bin/env python

# pylint: disable=protected-access

import pytest

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI

from radical.edge.plugin_iri_instance import (
    PluginIRIInstance,
    IRIInstanceSession,
    _iri_raise,
)
from radical.edge.iri_endpoints import IRI_ENDPOINTS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bridge_app():
    app = FastAPI()
    app.state.is_bridge    = True
    app.state.edge_service = MagicMock()
    app.state.edge_name    = 'bridge'
    app.state.bridge_url   = ''
    return app


# ---------------------------------------------------------------------------
# PluginIRIInstance construction
# ---------------------------------------------------------------------------

def test_instance_init(bridge_app):
    plugin = PluginIRIInstance(bridge_app, 'iri.nersc',
                              endpoint='nersc', token='tok123')
    assert plugin.instance_name == 'iri.nersc'
    assert plugin._endpoint_key == 'nersc'
    assert plugin._auto_sid in plugin._sessions
    assert plugin.session_ttl == 0


def test_instance_bad_endpoint(bridge_app):
    with pytest.raises(Exception, match='Unknown endpoint'):
        PluginIRIInstance(bridge_app, 'iri.bad',
                          endpoint='bad', token='tok')


def test_instance_empty_token(bridge_app):
    with pytest.raises(Exception, match='token must not be empty'):
        PluginIRIInstance(bridge_app, 'iri.nersc',
                          endpoint='nersc', token='')


def test_instance_ui_config_dynamic(bridge_app):
    plugin = PluginIRIInstance(bridge_app, 'iri.nersc',
                              endpoint='nersc', token='tok')
    assert 'NERSC' in plugin.ui_config['title']


def test_instance_no_plugin_name():
    """PluginIRIInstance has no plugin_name — not auto-registered."""
    assert not hasattr(PluginIRIInstance, 'plugin_name')


def test_update_token_rotates_plugin_and_session(bridge_app):
    """``update_token`` refreshes the bearer token on the plugin and on
    the auto-session's outbound httpx client (Authorization header)."""
    plugin = PluginIRIInstance(bridge_app, 'iri.nersc',
                              endpoint='nersc', token='old-token')
    sess = plugin._sessions[plugin._auto_sid]
    assert plugin._token == 'old-token'
    assert sess._token   == 'old-token'
    assert sess._http.headers['Authorization'] == 'Bearer old-token'

    plugin.update_token('new-token')

    assert plugin._token == 'new-token'
    assert sess._token   == 'new-token'
    assert sess._http.headers['Authorization'] == 'Bearer new-token'


# ---------------------------------------------------------------------------
# register_session returns pre-created SID
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_session_returns_auto_sid(bridge_app):
    plugin = PluginIRIInstance(bridge_app, 'iri.nersc',
                              endpoint='nersc', token='tok')
    request = MagicMock()
    result = await plugin.register_session(request)
    assert result['sid'] == plugin._auto_sid


# ---------------------------------------------------------------------------
# IRIInstanceSession
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_session_close():
    session = IRIInstanceSession('s1', endpoint='nersc', token='tok')
    await session.close()
    assert not session._active


# ---------------------------------------------------------------------------
# _iri_raise utility
# ---------------------------------------------------------------------------

def test_iri_raise_success():
    resp = MagicMock()
    resp.is_success = True
    _iri_raise(resp)  # should not raise


def test_iri_raise_401():
    resp = MagicMock()
    resp.is_success   = False
    resp.status_code  = 401
    with pytest.raises(Exception, match='token expired'):
        _iri_raise(resp)


def test_iri_raise_404():
    resp = MagicMock()
    resp.is_success   = False
    resp.status_code  = 404
    with pytest.raises(Exception, match='not found'):
        _iri_raise(resp)


def test_iri_raise_500():
    resp = MagicMock()
    resp.is_success        = False
    resp.status_code       = 500
    resp.text              = 'internal error'
    resp.json.return_value = {'detail': 'endpoint error'}
    with pytest.raises(Exception, match='endpoint error'):
        _iri_raise(resp)


# ---------------------------------------------------------------------------
# Route handlers (mocked httpx)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_resources_route(bridge_app):
    plugin  = PluginIRIInstance(bridge_app, 'iri.nersc',
                               endpoint='nersc', token='tok')
    session = plugin._sessions[plugin._auto_sid]

    mock_resp = MagicMock()
    mock_resp.is_success = True
    mock_resp.json.return_value = {'resources': [{'name': 'perlmutter'}]}

    with patch.object(session._http, 'get', new_callable=AsyncMock,
                      return_value=mock_resp):
        request = MagicMock()
        request.query_params = {'resource_type': 'compute'}
        result = await plugin.list_resources(request)
        assert result['resources'][0]['name'] == 'perlmutter'


@pytest.mark.asyncio
async def test_submit_job_route(bridge_app):
    plugin  = PluginIRIInstance(bridge_app, 'iri.nersc',
                               endpoint='nersc', token='tok')
    session = plugin._sessions[plugin._auto_sid]

    mock_resp = MagicMock()
    mock_resp.is_success = True
    mock_resp.json.return_value = {'job_id': 'j1', 'status': {'state': 'new'}}

    with patch.object(session._http, 'post', new_callable=AsyncMock,
                      return_value=mock_resp):
        request = MagicMock()
        request.path_params = {'resource_id': 'perlmutter'}
        request.json = AsyncMock(return_value={'job_spec': {'executable': '/bin/bash'}})
        result = await plugin.submit_job(request)
        assert result['job_id'] == 'j1'
