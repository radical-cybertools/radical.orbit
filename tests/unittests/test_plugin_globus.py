#!/usr/bin/env python

# pylint: disable=protected-access

import json
import time

import pytest

from unittest.mock import MagicMock, patch

from fastapi import FastAPI, HTTPException

from radical.orbit.plugin_globus import (
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
def endpoint_app():
    app = FastAPI()
    app.state.is_broker    = False
    app.state.endpoint_service = MagicMock()
    app.state.endpoint_name    = 'endpoint1'
    app.state.broker_url   = ''
    return app


@pytest.fixture
def broker_app():
    app = FastAPI()
    app.state.is_broker    = True
    app.state.endpoint_service = MagicMock()
    return app


def _make_session(local_collection=None):
    '''Build a GlobusSession with globus_sdk patched, then a fake TransferClient.'''
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        session = GlobusSession('s1', access_token='tok',
                                local_collection=local_collection)
    session._tc = MagicMock()
    return session


# ---------------------------------------------------------------------------
# is_enabled — endpoint-only
# ---------------------------------------------------------------------------

def test_is_enabled_endpoint(endpoint_app):
    assert PluginGlobus.is_enabled(endpoint_app) is True


def test_is_enabled_broker(broker_app):
    assert PluginGlobus.is_enabled(broker_app) is False


def test_is_disabled_without_sdk(endpoint_app):
    with patch('radical.orbit.plugin_globus.globus_sdk', None):
        assert PluginGlobus.is_enabled(endpoint_app) is False


# ---------------------------------------------------------------------------
# Auth selection at session construction
# ---------------------------------------------------------------------------

def test_session_access_token():
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        GlobusSession('s1', access_token='tok')
        gs.AccessTokenAuthorizer.assert_called_once_with('tok')
        gs.RefreshTokenAuthorizer.assert_not_called()


def test_session_refresh_token():
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        GlobusSession('s1', refresh_token='rt', client_id='cid')
        gs.NativeAppAuthClient.assert_called_once_with('cid')
        gs.RefreshTokenAuthorizer.assert_called_once()
        gs.AccessTokenAuthorizer.assert_not_called()


def test_session_no_credential():
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        with pytest.raises(ValueError, match='provide either'):
            GlobusSession('s1')


def test_close_clients_is_best_effort():
    '''One client's close() raising must not skip the other's.'''
    session = _make_session()
    session._auth_client = MagicMock()
    session._tc.close.side_effect = RuntimeError('boom')
    session._close_clients()
    session._auth_client.close.assert_called_once()


def test_session_refresh_failure_keeps_refresh_error():
    '''A failing close during the refresh-failure cleanup must not mask
    the refresh error.'''
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.RefreshTokenAuthorizer.side_effect = RuntimeError('refresh failed')
        gs.NativeAppAuthClient.return_value.close.side_effect = OSError('x')
        with pytest.raises(RuntimeError, match='refresh failed'):
            GlobusSession('s1', refresh_token='rt', client_id='cid')


@pytest.mark.asyncio
async def test_invalid_grant_401_only_for_auth_errors(monkeypatch):
    '''A 400 ``invalid_grant`` maps to 401 only for Globus Auth errors; the
    same body from the Transfer API stays a 502.'''
    pytest.importorskip('globus_sdk')
    _stub_http(monkeypatch, (400, {'error': 'invalid_grant',
                                   'code': 'BadRequest'}))
    session = GlobusSession('s1', access_token='tok')
    try:
        with pytest.raises(HTTPException) as exc:
            await session.get_task('tid-1')
        assert exc.value.status_code == 502
    finally:
        await session.close()


def test_session_refresh_failure_closes_auth_client():
    '''A failing initial token refresh must not leak the auth client.'''
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.RefreshTokenAuthorizer.side_effect = RuntimeError('refresh failed')
        with pytest.raises(RuntimeError, match='refresh failed'):
            GlobusSession('s1', refresh_token='rt', client_id='cid')
        gs.NativeAppAuthClient.return_value.close.assert_called_once()


def _stub_http(monkeypatch, reply):
    '''Stub globus-sdk's HTTP layer (``requests.Session.send``).

    *reply* is a requests exception to raise, a ``(status, json)`` response,
    or a callable ``req -> (status, json)``.  Returns the list of sent
    ``(method, url, body, timeout)`` tuples.
    '''
    import requests
    sent = []

    def fake_send(_self, req, **kw):
        sent.append((req.method, req.url, req.body, kw.get('timeout')))
        if isinstance(reply, Exception):
            raise reply
        status, data = reply(req) if callable(reply) else reply
        resp = requests.Response()
        resp.status_code = status
        resp._content    = json.dumps(data).encode()
        resp.headers['Content-Type'] = 'application/json'
        resp.request, resp.url = req, req.url
        return resp

    monkeypatch.setattr(requests.Session, 'send', fake_send)
    return sent


def _globus_reply(req):
    '''Canned successful Globus Auth / Transfer replies.'''
    if req.url.endswith('/oauth2/token'):
        return 200, {'access_token': 'at', 'expires_in': 3600,
                     'resource_server': 'transfer.api.globus.org',
                     'scope': 'urn:globus:auth:scope:transfer.api.globus.org:all',
                     'token_type': 'Bearer', 'other_tokens': []}
    if req.url.endswith('/submission_id'):
        return 200, {'value': 'sid-1'}
    return 200, {'task_id': 'tid-1', 'submission_id': 'sid-1'}


@pytest.mark.parametrize('creds', [
    {'access_token': 'tok'},
    {'refresh_token': 'rt', 'client_id': 'cid'},   # token refresh at init
])
@pytest.mark.asyncio
async def test_real_sdk_requests(monkeypatch, creds):
    '''Unmocked globus-sdk (3.x or 4.x), stubbed at the HTTP layer: the
    60 s timeout reaches every request (token refresh included), and the
    transfer / delete payloads the SDK sends carry a submission_id (4.x
    dropped the client argument of TransferData / DeleteData that used to
    fetch it).'''
    pytest.importorskip('globus_sdk')

    # A different SDK default proves the session sets the timeout itself.
    monkeypatch.setenv('GLOBUS_SDK_HTTP_TIMEOUT', '5')
    sent = _stub_http(monkeypatch, _globus_reply)
    session = GlobusSession('s1', **creds)
    session._start_polling = MagicMock()
    # globus-sdk 4.x clients have close(); 3.x clients have none.
    clients = [c for c in (session._tc, session._auth_client)
               if hasattr(c, 'close')]
    for c in clients:
        monkeypatch.setattr(c, 'close', MagicMock(wraps=c.close))
    try:
        await session.submit_transfer(
            'src', 'dst', [{'source': '/a', 'destination': '/b'}], label='l')
        await session.submit_delete('src', ['/a'], recursive=True)
    finally:
        await session.close()

    for c in clients:
        c.close.assert_called_once()
    if hasattr(session._tc, 'close'):
        assert len(clients) == 1 + ('refresh_token' in creds)

    assert {t for *_, t in sent} == {60}
    assert any(url.endswith('/oauth2/token') for _, url, *_ in sent) \
        == ('refresh_token' in creds)
    posts = {url.rsplit('/', 1)[-1]: json.loads(body)
             for method, url, body, _ in sent
             if url.endswith(('/transfer', '/delete'))}
    assert posts['transfer']['submission_id']   == 'sid-1'
    assert posts['transfer']['source_endpoint'] == 'src'
    assert posts['transfer']['DATA'][0]['source_path'] == '/a'
    assert posts['delete']['submission_id'] == 'sid-1'
    assert posts['delete']['recursive']     is True


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

    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
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
    with patch('radical.orbit.plugin_globus.globus_sdk'):
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

    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
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
# finding 10: shared PluginSession.start_status_poller adoption
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_polling_uses_shared_status_poller():
    session = _make_session()
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': 'x'}

    session._start_polling()
    try:
        task = session._status_poller_task
        assert task is not None
        assert not task.done()
        # Idempotent — a second call while one is live keeps the same task.
        session._start_polling()
        assert session._status_poller_task is task
    finally:
        session.stop_status_poller()


@pytest.mark.asyncio
async def test_fetch_task_status_reports_change_and_updates_meta():
    session = _make_session()
    session._tc.get_task.return_value = {
        'status': 'SUCCEEDED', 'bytes_transferred': 10,
        'files_transferred': 1, 'nice_status': None}
    meta = {'status': 'ACTIVE', 'label': 'job'}

    res = await session._fetch_task_status('t1', meta)

    assert res is not None
    assert meta['status'] == 'SUCCEEDED'
    payload = session._task_payload('t1', meta, res)
    assert payload == {
        'task_id'          : 't1',
        'status'           : 'SUCCEEDED',
        'label'            : 'job',
        'bytes_transferred': 10,
        'files_transferred': 1,
        'nice_status'      : None,
    }


@pytest.mark.asyncio
async def test_fetch_task_status_no_change_returns_none():
    session = _make_session()
    session._tc.get_task.return_value = {'status': 'ACTIVE'}
    meta = {'status': 'ACTIVE', 'label': ''}

    res = await session._fetch_task_status('t1', meta)

    assert res is None
    assert meta['status'] == 'ACTIVE'


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
    monkeypatch.setenv('RADICAL_ORBIT_GLOBUS_COLLECTION', 'env-uuid')
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        assert detect_local_collection() == 'env-uuid'
        gs.LocalGlobusConnectPersonal.assert_not_called()


def test_detect_gcp(monkeypatch):
    monkeypatch.delenv('RADICAL_ORBIT_GLOBUS_COLLECTION', raising=False)
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = 'gcp-uuid'
        assert detect_local_collection() == 'gcp-uuid'


def test_detect_config_file(monkeypatch, tmp_path):
    monkeypatch.delenv('RADICAL_ORBIT_GLOBUS_COLLECTION', raising=False)
    cfg = tmp_path / 'globus.json'
    cfg.write_text('{"local_collection": "cfg-uuid"}')
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = None
        with patch('radical.orbit.plugin_globus.GLOBUS_CONFIG_FILE', str(cfg)):
            assert detect_local_collection() == 'cfg-uuid'


def test_detect_none(monkeypatch):
    monkeypatch.delenv('RADICAL_ORBIT_GLOBUS_COLLECTION', raising=False)
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.LocalGlobusConnectPersonal.return_value.endpoint_id = None
        with patch('radical.orbit.plugin_globus.GLOBUS_CONFIG_FILE',
                   '/nonexistent/globus.json'):
            assert detect_local_collection() is None


def test_ui_module_present():
    assert PluginGlobus.ui_module.endswith('data/plugins/globus.js')


# ---------------------------------------------------------------------------
# register_session — auth payload handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_session_requires_credential(endpoint_app):
    plugin  = PluginGlobus(endpoint_app)
    request = MagicMock()
    request.json = MagicMock(return_value={})
    # request.json is awaited in the handler
    async def _json():
        return {}
    request.json = _json
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(request)
    assert exc.value.status_code == 400


@pytest.mark.parametrize('reply, status', [
    ((400, {'error': 'invalid_grant'}), 401),
    ('network', 502),
])
@pytest.mark.asyncio
async def test_register_session_refresh_errors(endpoint_app, monkeypatch,
                                               reply, status):
    '''Errors from the constructor-time token refresh (real globus-sdk) map
    to HTTP errors: a rejected refresh token to 401, a network error to 502.'''
    pytest.importorskip('globus_sdk')
    import requests
    if reply == 'network':
        reply = requests.ConnectionError('unreachable')
        # globus-sdk retries network errors with backoff sleeps.
        monkeypatch.setattr('time.sleep', lambda _s: None)
    _stub_http(monkeypatch, reply)

    plugin  = PluginGlobus(endpoint_app)
    request = MagicMock()
    async def _json():
        return {'refresh_token': 'rt', 'client_id': 'cid'}
    request.json = _json
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(request)
    assert exc.value.status_code == status
    assert not plugin._sessions


@pytest.mark.asyncio
async def test_register_session_access_token(endpoint_app):
    plugin  = PluginGlobus(endpoint_app)
    request = MagicMock()
    async def _json():
        return {'access_token': 'tok'}
    request.json = _json
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        result = await plugin.register_session(request)
    assert result['sid'] in plugin._sessions


@pytest.mark.asyncio
async def test_register_session_default_collection(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    plugin._default_collection = 'cfg-uuid'
    request = MagicMock()
    async def _json():
        return {'access_token': 'tok'}
    request.json = _json
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        result  = await plugin.register_session(request)
    session = plugin._sessions[result['sid']]
    assert session._local_collection == 'cfg-uuid'


@pytest.mark.asyncio
async def test_register_session_records_policy_and_owner(endpoint_app):
    '''finding 9: register_session goes through the base's _record_session /
    _normalize_session_policy, not a bare _sessions/_session_last_access
    write — so a globus session gets the same lifetime/owner bookkeeping
    every other plugin's sessions get.'''
    plugin  = PluginGlobus(endpoint_app)
    request = MagicMock()
    request.headers = {'x-orbit-src': 'ep1'}
    async def _json():
        return {'access_token': 'tok', 'lifetime': 'persistent'}
    request.json = _json

    with patch('radical.orbit.plugin_globus.globus_sdk'):
        result = await plugin.register_session(request)

    sid    = result['sid']
    record = plugin._records[sid]
    assert record.owner    == 'ep1'
    assert record.lifetime == 'persistent'
    assert record.ttl is None
    # The back-compat last-access view still resolves through the record.
    assert plugin._session_last_access[sid] == record.last_access


# ---------------------------------------------------------------------------
# Route handler delegation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_route(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
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

    with patch('radical.orbit.plugin_globus.globus_sdk'):
        result = await plugin.submit_transfer(request)
    assert result['task_id'] == 't9'
