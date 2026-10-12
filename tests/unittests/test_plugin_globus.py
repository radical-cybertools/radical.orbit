#!/usr/bin/env python

# pylint: disable=protected-access

import time

import pytest

from unittest.mock import MagicMock, patch

from fastapi import FastAPI, HTTPException

from radical.orbit import plugin_globus as pg
from radical.orbit.plugin_globus import (
    PluginGlobus,
    GlobusSession,
    GlobusClient,
    GLOBUS_WAIT_SLICE,
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
    assert result == {'task_id': 't1', 'completed': True, 'timeout': 5}


@pytest.mark.asyncio
async def test_task_wait_clamped_below_call_timeout():
    """A long wait is bounded per call so the routed call is not reaped,
    and the response reports the bound applied."""
    session = _make_session()
    session._tc.task_wait.return_value = False
    res = await session.task_wait('t1', timeout=600, polling_interval=60)
    kwargs = session._tc.task_wait.call_args.kwargs
    assert kwargs['timeout']          == GLOBUS_WAIT_SLICE
    assert kwargs['polling_interval'] == GLOBUS_WAIT_SLICE
    assert res == {'task_id': 't1', 'completed': False,
                   'timeout': GLOBUS_WAIT_SLICE}

    # a tiny interval would multiply Globus API round trips per wait
    await session.task_wait('t1', timeout=600, polling_interval=1)
    assert session._tc.task_wait.call_args.kwargs['polling_interval'] \
        == pg.GLOBUS_WAIT_POLL

    # a lower host-derived bound wins
    res = await session.task_wait('t1', timeout=600, max_wait=3)
    assert res['timeout'] == 3


@pytest.mark.parametrize('call_timeout, expected', [
    (None,  GLOBUS_WAIT_SLICE),      # no hosting runtime (broker, tests)
    (600.0, GLOBUS_WAIT_SLICE),      # runtime default: constant wins
    (15.0,  5),                      # low runtime timeout shrinks the slice
    (1.0,   1),
])
def test_resolve_wait_slice(call_timeout, expected):
    app = FastAPI()
    if call_timeout is not None:
        app.state.endpoint_service = MagicMock(_call_timeout=call_timeout)
    assert pg._resolve_wait_slice(app) == expected


def _wait_client(replies, monkeypatch):
    """GlobusClient whose wait route answers with *replies* in order.

    Each reply is ``(status, body, elapsed)``; *elapsed* (default: the
    requested wait) advances a fake clock, as does ``time.sleep``.
    """
    now     = [0.0]
    replies = iter(replies)

    def _post(url, json):
        status, body, elapsed = next(replies)
        now[0] += json['timeout'] if elapsed is None else elapsed
        return MagicMock(status_code=status, is_error=status >= 400,
                         json=MagicMock(return_value=body))

    def _sleep(t):
        now[0] += t

    monkeypatch.setattr(pg.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(pg.time, 'sleep', _sleep)
    http = MagicMock()
    http.post.side_effect = _post
    client = GlobusClient(http, '/globus')
    client._sid = 'sid'
    return client, http, now


_ACTIVE = {'task_id': 't1', 'completed': False}
_DONE   = {'task_id': 't1', 'completed': True}


def _sent(http):
    return [c.kwargs['json']['timeout'] for c in http.post.call_args_list]


def test_client_task_wait_loops_until_completed(monkeypatch):
    client, http, _ = _wait_client([(200, _ACTIVE, None),
                                    (200, _DONE,   None)], monkeypatch)
    assert client.task_wait('t1', timeout=300) == _DONE
    assert _sent(http) == [GLOBUS_WAIT_SLICE, GLOBUS_WAIT_SLICE]


def test_client_task_wait_stops_at_deadline(monkeypatch):
    """No final 1 s round trip once less than a second is left."""
    client, http, _ = _wait_client([(200, _ACTIVE, None),
                                    (200, _ACTIVE, 9.5)], monkeypatch)
    res = client.task_wait('t1', timeout=20)
    assert res['completed'] is False
    assert _sent(http) == [GLOBUS_WAIT_SLICE, GLOBUS_WAIT_SLICE]


def test_client_task_wait_slice_overruns_clock(monkeypatch):
    """A slice that takes far longer than requested ends the wait at the
    deadline instead of issuing another call."""
    client, http, _ = _wait_client([(200, _ACTIVE, 40.0)], monkeypatch)
    assert client.task_wait('t1', timeout=30)['completed'] is False
    assert http.post.call_count == 1


def test_client_task_wait_retries_lost_slice(monkeypatch):
    """A 504 (reaped in transit) mid-loop is retried, not raised."""
    client, http, _ = _wait_client([(200, _ACTIVE, None),
                                    (504, {'detail': 'call timed out'}, 30.0),
                                    (200, _DONE,   None)], monkeypatch)
    assert client.task_wait('t1', timeout=120) == _DONE
    assert http.post.call_count == 3


def test_client_task_wait_raises_other_errors(monkeypatch):
    client, http, _ = _wait_client([(404, {'detail': 'no session'}, 0.0)],
                                   monkeypatch)
    with pytest.raises(RuntimeError, match='HTTP 404'):
        client.task_wait('t1', timeout=60)


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
