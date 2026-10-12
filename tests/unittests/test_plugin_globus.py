#!/usr/bin/env python

# pylint: disable=protected-access

import asyncio
import time

import httpx
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
    assert result == {'task_id': 't1', 'completed': True}


@pytest.mark.asyncio
async def test_task_wait_clamped_below_call_timeout():
    """A long wait is bounded per call so the routed call is not reaped."""
    session = _make_session()
    session._tc.task_wait.return_value = False
    res = await session.task_wait('t1', timeout=600, polling_interval=60)
    kwargs = session._tc.task_wait.call_args.kwargs
    assert kwargs['timeout']          == GLOBUS_WAIT_SLICE
    assert kwargs['polling_interval'] == GLOBUS_WAIT_SLICE
    assert res == {'task_id': 't1', 'completed': False}

    # the caller's interval is honoured down to the sdk minimum of 1 s
    await session.task_wait('t1', timeout=600, polling_interval=2)
    assert session._tc.task_wait.call_args.kwargs['polling_interval'] == 2
    await session.task_wait('t1', timeout=0, polling_interval=0)
    kwargs = session._tc.task_wait.call_args.kwargs
    assert (kwargs['timeout'], kwargs['polling_interval']) == (1, 1)


def _wait_client(replies, monkeypatch, undershoot=0.0):
    """GlobusClient whose get_task route answers with *replies* in order.

    Each reply is ``(status, body)``, ``(status, body, elapsed, headers)``
    or an exception to raise; *elapsed* (default 0) advances a fake clock,
    as does ``time.sleep`` (whose arguments are recorded).
    """
    now     = [0.0]
    sleeps  = []
    replies = iter(replies)

    def _get(url):
        reply = next(replies)
        if isinstance(reply, Exception):
            now[0] += 1.0
            raise reply
        status, body, elapsed, headers = (tuple(reply) + (0.0, {}))[:4]
        now[0] += elapsed
        return MagicMock(status_code=status, is_error=status >= 400,
                         headers=headers, json=MagicMock(return_value=body))

    def _sleep(t):
        sleeps.append(t)
        now[0] += t - undershoot        # a real sleep may wake a bit early

    monkeypatch.setattr(pg.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(pg.time, 'sleep', _sleep)
    http = MagicMock()
    http.get.side_effect = _get
    client = GlobusClient(http, '/globus')
    client._sid = 'sid'
    return client, http, sleeps


_ACTIVE = {'task_id': 't1', 'status': 'ACTIVE'}
_DONE   = {'task_id': 't1', 'status': 'SUCCEEDED'}
_WAITED = {'task_id': 't1', 'completed': True}
_TIMED  = {'task_id': 't1', 'completed': False}


def test_client_task_wait_polls_until_done(monkeypatch):
    client, http, sleeps = _wait_client([(200, _ACTIVE), (200, _ACTIVE),
                                         (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=300) == _WAITED
    assert sleeps == [10, 10]


def test_client_task_wait_one_poll_per_interval(monkeypatch):
    """Globus sees one status call per interval, plus one at the deadline."""
    client, http, sleeps = _wait_client([(200, _ACTIVE)] * 7, monkeypatch)
    assert client.task_wait('t1', timeout=60, polling_interval=10) == _TIMED
    assert http.get.call_count == 7             # t = 0, 10, ..., 60
    assert sleeps == [10] * 6


def test_client_task_wait_fixed_schedule(monkeypatch):
    """Poll latency does not shift the schedule (polls at 0, 10, 20)."""
    client, http, sleeps = _wait_client([(200, _ACTIVE, 3.0, {}),
                                         (200, _ACTIVE, 3.0, {}),
                                         (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [7.0, 7.0]


def test_client_task_wait_last_poll_at_deadline(monkeypatch):
    """A deadline between slots gets one last poll at the deadline."""
    client, http, sleeps = _wait_client([(200, _ACTIVE)] * 3, monkeypatch)
    assert client.task_wait('t1', timeout=15) == _TIMED
    assert sleeps == [10.0, 5.0]
    assert http.get.call_count == 3


def test_client_task_wait_stops_after_deadline_poll(monkeypatch):
    """A sleep that wakes just before the deadline must not cause one more
    tiny sleep and an extra poll."""
    client, http, _ = _wait_client([(200, _ACTIVE)] * 4, monkeypatch,
                                   undershoot=0.001)
    assert client.task_wait('t1', timeout=15) == _TIMED
    assert http.get.call_count == 3             # t = 0, 10, 15 (deadline)


def test_client_task_wait_inactive_counts_as_completed(monkeypatch):
    client, _, _ = _wait_client([(200, {'status': 'INACTIVE'})], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED


def test_client_task_wait_zero_timeout_probes_once(monkeypatch):
    client, http, _ = _wait_client([(200, _ACTIVE)], monkeypatch)
    assert client.task_wait('t1', timeout=0) == _TIMED
    assert http.get.call_count == 1


def test_client_task_wait_slow_poll_ends_at_deadline(monkeypatch):
    """A poll that takes past the deadline ends the wait, no extra call."""
    client, http, _ = _wait_client([(200, _ACTIVE, 40.0, {})], monkeypatch)
    assert client.task_wait('t1', timeout=30) == _TIMED
    assert http.get.call_count == 1


def test_client_task_wait_retries_504(monkeypatch):
    client, http, sleeps = _wait_client(
        [(200, _ACTIVE), (504, {'detail': 'lost'}, 30.0, {}), (200, _DONE)],
        monkeypatch)
    assert client.task_wait('t1', timeout=120) == _WAITED
    assert sleeps == [10, 10]


def test_client_task_wait_503_honours_retry_after(monkeypatch):
    client, http, sleeps = _wait_client(
        [(503, {'detail': 'cap'}, 0.0, {'retry-after': '3'}), (200, _DONE)],
        monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [3.0]


def test_client_task_wait_503_without_retry_after_waits_for_slot(monkeypatch):
    """No Retry-After: retry at the next scheduled poll, not after 1 s."""
    client, http, sleeps = _wait_client([(503, {}), (200, _DONE)],
                                        monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [10.0]


@pytest.mark.parametrize('value, expected', [
    ('3',    3.0), ('0.2', 1.0),          # floor at 1 s
    ('0', None), ('-5', None), ('nan', None), ('inf', None), ('x', None),
    (None, None),
])
def test_retry_after_parsing(value, expected):
    resp = MagicMock(headers={} if value is None else {'retry-after': value})
    assert pg._retry_after(resp) == expected


def test_client_task_wait_503_retry_after_capped_by_deadline(monkeypatch):
    client, http, sleeps = _wait_client(
        [(503, {}, 0.0, {'retry-after': '30'}), (200, _ACTIVE)], monkeypatch)
    assert client.task_wait('t1', timeout=5) == _TIMED
    assert sleeps == [5]


def test_client_task_wait_raises_lost_poll_at_deadline(monkeypatch):
    """If the last poll before the deadline was lost, raise it rather than
    report a silent completed=False."""
    client, _, _ = _wait_client(
        [(200, _ACTIVE), (504, {'detail': 'call timed out'}, 30.0, {})],
        monkeypatch)
    with pytest.raises(RuntimeError, match='HTTP 504'):
        client.task_wait('t1', timeout=30)


def test_client_task_wait_429_backs_off(monkeypatch):
    """Globus rate limit: Retry-After if given, else one interval."""
    client, http, sleeps = _wait_client(
        [(429, {}, 0.0, {'retry-after': '7'}), (429, {}), (200, _DONE)],
        monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [7.0, 3.0]            # then back on the 10 s schedule


@pytest.mark.parametrize('exc', [TimeoutError('call'), ConnectionError('ws'),
                                 httpx.ConnectError('down')])
def test_client_task_wait_retries_transport_loss(monkeypatch, exc):
    client, http, sleeps = _wait_client([exc, (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert http.get.call_count == 2


def test_client_task_wait_raises_transport_loss_at_deadline(monkeypatch):
    client, _, _ = _wait_client([(200, _ACTIVE, 9.0, {}),
                                 TimeoutError('call timed out')],
                                monkeypatch)
    with pytest.raises(TimeoutError):
        client.task_wait('t1', timeout=10)


@pytest.mark.parametrize('body', [{}, {'status': 'SOMETHING_NEW'}])
def test_client_task_wait_unknown_status_not_completed(monkeypatch, body):
    """Completion needs a known non-ACTIVE status."""
    client, _, _ = _wait_client([(200, body)] * 2, monkeypatch)
    assert client.task_wait('t1', timeout=10) == _TIMED


@pytest.mark.parametrize('status', [404, 502])
def test_client_task_wait_raises_other_errors(monkeypatch, status):
    """502 is not retried: it is how the plugin reports Globus errors."""
    client, http, _ = _wait_client([(status, {'detail': 'boom'})],
                                   monkeypatch)
    with pytest.raises(RuntimeError, match=f'HTTP {status}'):
        client.task_wait('t1', timeout=60)
    assert http.get.call_count == 1


@pytest.mark.asyncio
async def test_poller_notifies_after_get_task_saw_terminal():
    """A get_task call (e.g. a client's task_wait poll) that sees the
    terminal status first must not swallow the poller's notification."""
    session = _make_session()
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': ''}
    session._tc.get_task.return_value = {'task_id': 't1',
                                         'status' : 'SUCCEEDED'}
    await session.get_task('t1')
    meta = session._tasks['t1']
    assert meta['status'] == 'SUCCEEDED'

    res = await session._fetch_task_status('t1', meta)
    assert res is not None
    assert session._task_payload('t1', meta, res)['status'] == 'SUCCEEDED'
    assert await session._fetch_task_status('t1', meta) is None   # once


@pytest.mark.asyncio
async def test_poller_cached_terminal_skips_failing_fetch(monkeypatch):
    """A terminal status cached by get_task is notified once without a
    fetch, so a failing fetch (expired token) cannot poll forever."""
    monkeypatch.setattr(pg, 'GLOBUS_POLL_INTERVAL', 0.01)
    session = _make_session()
    session.notify = MagicMock()
    session._tasks['t1'] = {'status': 'SUCCEEDED', 'label': ''}   # cached
    session._tc.get_task.side_effect = RuntimeError('401 token expired')

    session._start_polling()
    await asyncio.wait_for(session._status_poller_task, timeout=2.0)

    session._tc.get_task.assert_not_called()
    session.notify.assert_called_once()
    topic, payload = session.notify.call_args.args
    assert (topic, payload['status']) == ('transfer_status', 'SUCCEEDED')


def test_globus_http_exc_forwards_retry_after():
    exc             = MagicMock(spec=['http_status', 'message', 'code',
                                      'headers'])
    exc.code        = 'RateLimited'
    exc.message     = 'slow down'
    exc.headers     = {'Retry-After': '9'}
    exc.http_status = 429
    assert _globus_http_exc(exc).headers == {'Retry-After': '9'}
    exc.http_status = 503
    assert _globus_http_exc(exc, read_only=True).headers \
        == {'Retry-After': '9'}


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


@pytest.mark.parametrize('http_status, read_only, expected', [
    (500,  True,  503),   # Globus 5xx on a status query: transient
    (503,  True,  503),
    (500,  False, 502),   # mutating call: a retry could duplicate work
    (503,  False, 502),
    (400,  True,  502),   # other unmapped errors stay 502
    (None, True,  502),
])
def test_globus_http_exc_unmapped(http_status, read_only, expected):
    exc             = MagicMock(spec=['http_status', 'message', 'code'])
    exc.code        = 'EndpointError'
    exc.http_status = http_status
    exc.message     = 'boom'
    mapped = _globus_http_exc(exc, read_only=read_only)
    assert mapped.status_code == expected
    assert 'boom' in mapped.detail


@pytest.mark.asyncio
@pytest.mark.parametrize('exc_name, read_only, expected', [
    ('GlobusConnectionError', True,  503),
    ('GlobusTimeoutError',    True,  503),
    ('GlobusConnectionError', False, 502),   # mutating call: not retryable
    (None,                    True,  502),   # anything else unexpected
])
async def test_call_maps_network_errors(exc_name, read_only, expected):
    pytest.importorskip('globus_sdk')
    import globus_sdk
    session = _make_session()
    if exc_name:
        err = getattr(globus_sdk, exc_name)('net down', Exception('x'))
    else:
        err = ValueError('odd')

    def _boom():
        raise err

    with patch('radical.orbit.plugin_globus.globus_sdk', globus_sdk):
        with pytest.raises(HTTPException) as info:
            await session._call(_boom, context='x', read_only=read_only)
    assert info.value.status_code == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('method, args, read_only', [
    ('get_task',       ('t1',),                          True),
    ('task_wait',      ('t1',),                          True),
    ('cancel_task',    ('t1',),                          False),
    ('submit_transfer', ('src', 'dst', [{'source': '/a',
                                         'destination': '/b'}]), False),
])
async def test_status_calls_read_only_mutations_not(method, args, read_only):
    """Only status queries may report a transient Globus failure as 503."""
    session = _make_session()
    with patch.object(session, '_call') as call, \
            patch('radical.orbit.plugin_globus.globus_sdk'):
        call.return_value = {'task_id': 't1', 'status': 'ACTIVE'}
        try:
            await getattr(session, method)(*args)
        except Exception:
            pass
    assert call.call_count >= 1
    assert all(c.kwargs.get('read_only', False) is read_only
               for c in call.call_args_list)


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
