#!/usr/bin/env python

# pylint: disable=protected-access

import time

import pytest

from unittest.mock import MagicMock, patch

from fastapi import FastAPI, HTTPException

from radical.edge.plugin_globus import (
    PluginGlobus,
    GlobusSession,
    detect_local_collection,
    _globus_http_exc,
    _as_dict,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def edge_app():
    app = FastAPI()
    app.state.is_bridge    = False
    app.state.edge_service = MagicMock()
    app.state.edge_name    = 'edge1'
    app.state.bridge_url   = ''
    return app


@pytest.fixture
def bridge_app():
    app = FastAPI()
    app.state.is_bridge    = True
    app.state.edge_service = MagicMock()
    return app


def _make_session(local_collection=None):
    '''Build a GlobusSession with globus_sdk patched, then a fake TransferClient.'''
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        session = GlobusSession('s1', access_token='tok',
                                local_collection=local_collection)
    session._tc = MagicMock()
    return session


# ---------------------------------------------------------------------------
# is_enabled — edge-only
# ---------------------------------------------------------------------------

def test_is_enabled_edge(edge_app):
    assert PluginGlobus.is_enabled(edge_app) is True


def test_is_enabled_bridge(bridge_app):
    assert PluginGlobus.is_enabled(bridge_app) is False


def test_is_disabled_without_sdk(edge_app):
    with patch('radical.edge.plugin_globus.globus_sdk', None):
        assert PluginGlobus.is_enabled(edge_app) is False


# ---------------------------------------------------------------------------
# Auth selection at session construction
# ---------------------------------------------------------------------------

def test_session_access_token():
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        GlobusSession('s1', access_token='tok')
        gs.AccessTokenAuthorizer.assert_called_once_with('tok')
        gs.RefreshTokenAuthorizer.assert_not_called()


def test_session_refresh_token():
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        GlobusSession('s1', refresh_token='rt', client_id='cid')
        gs.NativeAppAuthClient.assert_called_once_with('cid')
        gs.RefreshTokenAuthorizer.assert_called_once()
        gs.AccessTokenAuthorizer.assert_not_called()


def test_session_no_credential():
    with patch('radical.edge.plugin_globus.globus_sdk'):
        with pytest.raises(ValueError, match='provide either'):
            GlobusSession('s1')


# ---------------------------------------------------------------------------
# _resolve — local collection handling
# ---------------------------------------------------------------------------

def test_resolve_explicit():
    session = _make_session()
    assert session._resolve('uuid-123') == 'uuid-123'


def test_resolve_local_configured():
    session = _make_session(local_collection='home-uuid')
    assert session._resolve('local') == 'home-uuid'
    assert session._resolve(None)    == 'home-uuid'


def test_resolve_local_missing():
    session = _make_session()
    with pytest.raises(HTTPException) as exc:
        session._resolve('local')
    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# Session operations (fake TransferClient)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_transfer():
    session = _make_session()
    session._tc.submit_transfer.return_value = {
        'task_id': 't1', 'submission_id': 'sub1'}

    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        result = await session.submit_transfer(
            'src-uuid', 'dst-uuid',
            [{'source': '/a', 'destination': '/b'}], label='job')

    assert result['task_id'] == 't1'
    assert result['status']  == 'ACTIVE'
    assert 't1' in session._tasks
    gs.TransferData.assert_called_once()


@pytest.mark.asyncio
async def test_submit_transfer_no_items():
    session = _make_session()
    with pytest.raises(HTTPException) as exc:
        await session.submit_transfer('s', 'd', [])
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_submit_transfer_bad_item():
    session = _make_session()
    with patch('radical.edge.plugin_globus.globus_sdk'):
        with pytest.raises(HTTPException) as exc:
            await session.submit_transfer('s', 'd', [{'source': '/a'}])
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_get_task_updates_tracked_status():
    session = _make_session()
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': ''}
    session._tc.get_task.return_value = {'task_id': 't1', 'status': 'SUCCEEDED'}
    result = await session.get_task('t1')
    assert result['status']            == 'SUCCEEDED'
    assert session._tasks['t1']['status'] == 'SUCCEEDED'


@pytest.mark.asyncio
async def test_task_wait():
    session = _make_session()
    session._tc.task_wait.return_value = True
    result = await session.task_wait('t1', timeout=5)
    assert result == {'task_id': 't1', 'completed': True}


@pytest.mark.asyncio
async def test_cancel_task():
    session = _make_session()
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': ''}
    session._tc.cancel_task.return_value = {'code': 'Canceled'}
    await session.cancel_task('t1')
    assert 't1' not in session._tasks


@pytest.mark.asyncio
async def test_operation_ls():
    session = _make_session(local_collection='home-uuid')
    session._tc.operation_ls.return_value = {
        'path': '/~/', 'DATA': [{'name': 'f1', 'type': 'file'}]}
    result = await session.operation_ls('local', '/~/')
    assert result['collection'] == 'home-uuid'
    assert result['entries'][0]['name'] == 'f1'


@pytest.mark.asyncio
async def test_submit_delete():
    session = _make_session()
    session._tc.submit_delete.return_value = {'task_id': 'd1'}

    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        result = await session.submit_delete('coll', ['/x'], recursive=True)

    assert result['task_id'] == 'd1'
    assert 'd1' in session._tasks
    gs.DeleteData.assert_called_once()


@pytest.mark.asyncio
async def test_endpoint_search():
    session = _make_session()
    session._tc.endpoint_search.return_value = {
        'DATA': [{'display_name': 'NERSC DTN'}]}
    result = await session.endpoint_search('nersc')
    assert result['endpoints'][0]['display_name'] == 'NERSC DTN'


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

def test_globus_http_exc_consent_required():
    exc            = MagicMock()
    exc.code       = 'ConsentRequired'
    exc.info.consent_required = MagicMock(required_scopes=['scope-x'])
    mapped = _globus_http_exc(exc, 'submit_transfer')
    assert mapped.status_code == 401
    assert 'data access consent' in mapped.detail


def test_globus_http_exc_404():
    exc             = MagicMock(spec=['http_status', 'message', 'code'])
    exc.code        = 'NotFound'
    exc.http_status = 404
    exc.message     = 'gone'
    mapped = _globus_http_exc(exc)
    assert mapped.status_code == 404


def test_globus_http_exc_502_default():
    exc             = MagicMock(spec=['http_status', 'message', 'code'])
    exc.code        = 'EndpointError'
    exc.http_status = 500
    exc.message     = 'boom'
    mapped = _globus_http_exc(exc)
    assert mapped.status_code == 502
    assert 'boom' in mapped.detail


def test_as_dict_response_object():
    resp = MagicMock()
    resp.data = {'task_id': 't1'}
    assert _as_dict(resp) == {'task_id': 't1'}


def test_as_dict_plain_dict():
    assert _as_dict({'a': 1}) == {'a': 1}


# ---------------------------------------------------------------------------
# Local-collection auto-detection (env -> GCP -> config file -> None)
# ---------------------------------------------------------------------------

def test_detect_env_first(monkeypatch):
    monkeypatch.setenv('RADICAL_EDGE_GLOBUS_COLLECTION', 'env-uuid')
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        assert detect_local_collection() == 'env-uuid'
        gs.LocalGlobusConnectPersonal.assert_not_called()


def test_detect_gcp(monkeypatch):
    monkeypatch.delenv('RADICAL_EDGE_GLOBUS_COLLECTION', raising=False)
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = 'gcp-uuid'
        assert detect_local_collection() == 'gcp-uuid'


def test_detect_config_file(monkeypatch, tmp_path):
    monkeypatch.delenv('RADICAL_EDGE_GLOBUS_COLLECTION', raising=False)
    cfg = tmp_path / 'globus.json'
    cfg.write_text('{"local_collection": "cfg-uuid"}')
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = None
        with patch('radical.edge.plugin_globus.GLOBUS_CONFIG_FILE', str(cfg)):
            assert detect_local_collection() == 'cfg-uuid'


def test_detect_none(monkeypatch):
    monkeypatch.delenv('RADICAL_EDGE_GLOBUS_COLLECTION', raising=False)
    with patch('radical.edge.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = None
        with patch('radical.edge.plugin_globus.GLOBUS_CONFIG_FILE',
                   '/nonexistent/globus.json'):
            assert detect_local_collection() is None


def test_ui_module_present():
    assert PluginGlobus.ui_module.endswith('data/plugins/globus.js')


# ---------------------------------------------------------------------------
# register_session — auth payload handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_session_requires_credential(edge_app):
    plugin  = PluginGlobus(edge_app)
    request = MagicMock()
    request.json = MagicMock(return_value={})
    # request.json is awaited in the handler
    async def _json():
        return {}
    request.json = _json
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(request)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_register_session_access_token(edge_app):
    plugin  = PluginGlobus(edge_app)
    request = MagicMock()
    async def _json():
        return {'access_token': 'tok'}
    request.json = _json
    with patch('radical.edge.plugin_globus.globus_sdk'):
        result = await plugin.register_session(request)
    assert result['sid'] in plugin._sessions


@pytest.mark.asyncio
async def test_register_session_default_collection(edge_app):
    plugin = PluginGlobus(edge_app)
    plugin._default_collection = 'cfg-uuid'
    request = MagicMock()
    async def _json():
        return {'access_token': 'tok'}
    request.json = _json
    with patch('radical.edge.plugin_globus.globus_sdk'):
        result  = await plugin.register_session(request)
    session = plugin._sessions[result['sid']]
    assert session._local_collection == 'cfg-uuid'


# ---------------------------------------------------------------------------
# Route handler delegation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_route(edge_app):
    plugin = PluginGlobus(edge_app)
    session = _make_session()
    session._tc.submit_transfer.return_value = {'task_id': 't9'}
    plugin._sessions['sX'] = session
    plugin._session_last_access['sX'] = time.time()

    request = MagicMock()
    request.path_params = {'sid': 'sX'}
    async def _json():
        return {'source': 's', 'destination': 'd',
                'items': [{'source': '/a', 'destination': '/b'}]}
    request.json = _json

    with patch('radical.edge.plugin_globus.globus_sdk'):
        result = await plugin.submit_transfer(request)
    assert result['task_id'] == 't9'
