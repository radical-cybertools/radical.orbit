#!/usr/bin/env python

# pylint: disable=protected-access

import asyncio
import json
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


def test_session_client_failure_closes_auth_client():
    '''A failure after the auth client exists (here: TransferClient) closes
    it and re-raises the original error.'''
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.TransferClient.side_effect = RuntimeError('tc failed')
        with pytest.raises(RuntimeError, match='tc failed'):
            GlobusSession('s1', refresh_token='rt', client_id='cid')
        gs.NativeAppAuthClient.return_value.close.assert_called_once()


@pytest.mark.parametrize('body, offloaded', [
    ({'access_token': 'tok'},                     False),
    ({'refresh_token': 'rt', 'client_id': 'cid'}, True),
])
@pytest.mark.asyncio
async def test_register_session_offloads_only_refresh(endpoint_app, body,
                                                      offloaded):
    '''Only refresh-token sessions (initial token fetch) go to a thread.'''
    plugin  = PluginGlobus(endpoint_app)
    request = MagicMock()
    async def _json():
        return body
    request.json = _json

    async def _to_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch('radical.orbit.plugin_globus.globus_sdk'), \
         patch('radical.orbit.plugin_globus.asyncio.to_thread',
               side_effect=_to_thread) as tt:
        result = await plugin.register_session(request)
    assert result['sid'] in plugin._sessions
    assert tt.called is offloaded


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
        [(503, {'detail': 'cap'}, 0.0, {'retry-after': '12'}), (200, _DONE)],
        monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [12.0]


def test_client_task_wait_short_retry_after_keeps_interval(monkeypatch):
    """A Retry-After shorter than the interval must not poll faster."""
    client, http, sleeps = _wait_client(
        [(503, {}, 0.0, {'retry-after': '1'}), (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [10.0]


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


def test_client_task_wait_lost_last_poll_after_answer(monkeypatch):
    """An earlier poll got through: a lost last poll is just a timeout."""
    client, _, _ = _wait_client(
        [(200, _ACTIVE), (504, {'detail': 'call timed out'}, 30.0, {})],
        monkeypatch)
    assert client.task_wait('t1', timeout=30) == _TIMED


def test_client_task_wait_raises_when_no_poll_got_through(monkeypatch):
    """No poll got through before the deadline: raise the last loss rather
    than report a silent completed=False."""
    client, _, _ = _wait_client(
        [(504, {'detail': 'call timed out'}, 30.0, {}),
         (504, {'detail': 'call timed out'}, 30.0, {})], monkeypatch)
    with pytest.raises(RuntimeError, match='HTTP 504'):
        client.task_wait('t1', timeout=40)


def test_client_task_wait_429_backs_off(monkeypatch):
    """Globus rate limit: the later of Retry-After and the next slot."""
    client, http, sleeps = _wait_client(
        [(429, {}, 0.0, {'retry-after': '7'}),      # shorter: slot at 10
         (429, {}, 0.0, {'retry-after': '13'}),     # longer: until 23
         (429, {}),                                  # no header: slot at 30
         (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert sleeps == [10.0, 13.0, 7.0]


@pytest.mark.parametrize('exc', [TimeoutError('call'), ConnectionError('ws'),
                                 httpx.ConnectError('down')])
def test_client_task_wait_retries_transport_loss(monkeypatch, exc):
    client, http, sleeps = _wait_client([exc, (200, _DONE)], monkeypatch)
    assert client.task_wait('t1', timeout=60) == _WAITED
    assert http.get.call_count == 2


def test_client_task_wait_raises_transport_loss_at_deadline(monkeypatch):
    client, _, _ = _wait_client([TimeoutError('call timed out')] * 3,
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
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': ''}
    session._tc.get_task.return_value = {
        'task_id': 't1', 'status': 'FAILED', 'nice_status': 'PERMISSION_DENIED',
        'bytes_transferred': 5, 'files_transferred': 1}
    await session.get_task('t1')                     # caches the full result
    session._tc.get_task.reset_mock()
    session._tc.get_task.side_effect = RuntimeError('401 token expired')

    session._start_polling()
    await asyncio.wait_for(session._status_poller_task, timeout=2.0)

    session._tc.get_task.assert_not_called()
    session.notify.assert_called_once()
    topic, payload = session.notify.call_args.args
    assert topic == 'transfer_status'
    assert payload['status']      == 'FAILED'
    assert payload['nice_status'] == 'PERMISSION_DENIED'   # full result
    assert payload['bytes_transferred'] == 5


@pytest.mark.asyncio
async def test_poller_stale_fetch_keeps_cached_terminal():
    """A get_task that sees SUCCEEDED while the poller's fetch is in flight
    must not be overwritten by that fetch's stale ACTIVE."""
    session = _make_session()
    session.notify = MagicMock()
    meta = {'status': 'ACTIVE', 'label': ''}
    session._tasks['t1'] = meta

    def _stale_fetch(task_id):
        meta['status'] = 'SUCCEEDED'            # concurrent get_task lands
        meta['result'] = {'task_id': 't1', 'status': 'SUCCEEDED',
                          'nice_status': 'OK'}
        return {'task_id': 't1', 'status': 'ACTIVE'}

    session._tc.get_task.side_effect = _stale_fetch
    res = await session._fetch_task_status('t1', meta)     # same sweep
    assert meta['status'] == 'SUCCEEDED'
    assert res['nice_status'] == 'OK'
    assert meta['notified'] == 'SUCCEEDED'
    assert 'result' not in meta                 # dropped after notification
    assert await session._fetch_task_status('t1', meta) is None


@pytest.mark.asyncio
async def test_poller_drops_result_after_terminal_notification():
    session = _make_session()
    meta = {'status': 'ACTIVE', 'label': ''}
    session._tasks['t1'] = meta
    session._tc.get_task.return_value = {'task_id': 't1', 'status': 'FAILED'}
    assert await session._fetch_task_status('t1', meta) is not None
    meta.pop('seen')
    assert meta == {'status': 'FAILED', 'notified': 'FAILED', 'label': ''}

    await session.get_task('t1')                # a later client poll
    assert 'result' not in meta


@pytest.mark.asyncio
async def test_cache_keeps_result_only_for_terminal():
    session = _make_session()
    session._tasks['t1'] = {'status': 'ACTIVE', 'label': ''}
    session._tc.get_task.return_value = {'task_id': 't1', 'status': 'INACTIVE'}
    await session.get_task('t1')
    session._tasks['t1'].pop('seen')
    assert session._tasks['t1'] == {'status': 'INACTIVE', 'label': ''}

    session._tc.get_task.return_value = {'task_id': 't1', 'status': 'FAILED'}
    await session.get_task('t1')
    assert session._tasks['t1']['result']['status'] == 'FAILED'


@pytest.mark.asyncio
async def test_poller_corrects_stale_cached_status():
    """A fetched status that differs from the cache updates it, even when it
    equals the last notified one (INACTIVE cached, task ACTIVE again)."""
    session = _make_session()
    meta = {'status': 'INACTIVE', 'notified': 'ACTIVE', 'label': ''}
    session._tc.get_task.return_value = {'task_id': 't1', 'status': 'ACTIVE'}
    assert await session._fetch_task_status('t1', meta) is None
    assert meta['status'] == 'ACTIVE'


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
        # globus-sdk retries network errors with backoff sleeps; skip them
        # (RequestsTransport._retry_sleep exists on 3.x and 4.x).
        from globus_sdk.transport import RequestsTransport
        monkeypatch.setattr(RequestsTransport, '_retry_sleep',
                            lambda *_a, **_k: None)
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


def _register_request(body, src=None):
    request = MagicMock()
    request.headers = {'x-orbit-src': src} if src else {}
    async def _json():
        return body
    request.json = _json
    return request


@pytest.mark.asyncio
async def test_register_session_rejects_default_sid(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        with pytest.raises(HTTPException) as exc:
            await plugin.register_session(
                _register_request({'access_token': 'tok', 'sid': 'default'}))
    assert exc.value.status_code == 400
    assert 'default' not in plugin._sessions






@pytest.mark.asyncio
async def test_register_session_any_holder_rotates_credential(endpoint_app):
    '''Any holder of the sid may rotate (as any may attach), and a
    reconnect's policy fields are ignored: the timeout was set at creation.
    Owner-less (gateway) sessions rotate too.'''
    plugin = PluginGlobus(endpoint_app)
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.TransferClient.side_effect = lambda **_: MagicMock()
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 's1',
                               'lifetime': 'persistent'}))
        session = plugin._sessions['s1']
        old_tc  = session._tc
        result  = await plugin.register_session(
            _register_request({'access_token': 'tok2', 'sid': 's1'}, 'ep2'))
    assert result == {'sid': 's1'}
    assert plugin._sessions['s1'] is session
    assert session._tc is not old_tc
    assert plugin._timeout(plugin._records['s1']) == 0


@pytest.mark.asyncio
async def test_register_session_same_owner_rotates_credential(endpoint_app):
    '''A same-owner re-register keeps the session object (tracked tasks,
    poller) and swaps in the new clients, retiring the old ones.'''
    plugin = PluginGlobus(endpoint_app)
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.TransferClient.side_effect = lambda **_: MagicMock()
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 's1',
                               'local_collection': 'coll-a'}, 'ep1'))
        session = plugin._sessions['s1']
        session._tasks['t1'] = {'notified': None}
        old_tc  = session._tc
        result  = await plugin.register_session(
            _register_request({'access_token': 'tok2', 'sid': 's1'}, 'ep1'))

    assert result == {'sid': 's1'}
    assert plugin._sessions['s1'] is session
    assert 't1' in session._tasks
    assert session._tc is not old_tc
    assert session._local_collection == 'coll-a'
    assert plugin._records['s1'].owner == 'ep1'

    # the old client may still serve an offloaded call: retired, not closed
    old_tc.close.assert_not_called()
    await session.close()
    old_tc.close.assert_called_once()




@pytest.mark.asyncio
async def test_register_session_default_sid_400_before_policy(endpoint_app):
    '''`default` with a non-persistent lifetime is the documented 400, not
    the policy normalizer's 409.'''
    plugin = PluginGlobus(endpoint_app)
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 'default',
                               'lifetime': 'ephemeral'}))
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_register_session_non_dict_body(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(_register_request(['tok']))
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_routed_call_on_default_sid_is_404(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    with pytest.raises(HTTPException) as exc:
        await plugin._forward('default', GlobusSession.list_tasks)
    assert exc.value.status_code == 404
    assert 'default' not in plugin._sessions


def test_client_reregister_sends_sid():
    '''A second register_session on a client re-sends its sid, so the
    server rotates the credential instead of minting a new session.'''
    client = GlobusClient.__new__(GlobusClient)
    client._sid  = None
    client._http = MagicMock()
    client._url  = lambda path: path
    client._raise = lambda resp: None
    client._http.post.return_value.status_code = 200
    client._http.post.return_value.json.return_value = {'sid': 's1'}

    client.register_session(access_token='tok')
    assert 'sid' not in client._http.post.call_args.kwargs['json']
    client.register_session(access_token='tok2')
    assert client._http.post.call_args.kwargs['json']['sid'] == 's1'


@pytest.mark.asyncio
async def test_register_session_concurrent_create_becomes_rotation(
        endpoint_app, monkeypatch):
    '''A sid created by another client while this register's credential was
    being built is not overwritten: the new credential is rotated into it.'''
    plugin = PluginGlobus(endpoint_app)
    built  = []
    orig   = plugin._create_session

    def _create(sid, **kwargs):
        session = orig(sid, **kwargs)
        built.append(session)
        if len(built) == 1:
            # a concurrent register by ep1 lands during the build
            plugin._record_session(sid, _make_session(), 'ephemeral', None,
                                   'ep1')
        return session

    monkeypatch.setattr(plugin, '_create_session', _create)
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        result = await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 's1'}, 'ep2'))
    assert result == {'sid': 's1'}
    assert plugin._sessions['s1'] is not built[0]       # not overwritten
    assert plugin._records['s1'].owner == 'ep1'


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






@pytest.mark.asyncio
async def test_register_session_expired_sid_is_not_revived(endpoint_app):
    '''A ttl-expired session the sweeper has not removed yet is ended,
    and the rotation is refused (410, with the reason) — neither revived nor
    silently replaced by an empty session under the same sid.'''
    plugin = PluginGlobus(endpoint_app)
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 's1',
                               'lifetime': 'ttl', 'ttl': 60}, 'ep1'))
        plugin._records['s1'].last_access -= 120
        with pytest.raises(HTTPException) as exc:
            await plugin.register_session(
                _register_request({'access_token': 'tok2', 'sid': 's1',
                                   'lifetime': 'ttl', 'ttl': 60}, 'ep1'))
    assert exc.value.status_code == 410
    assert 'expired' in exc.value.detail
    assert 's1' not in plugin._sessions


@pytest.mark.asyncio
async def test_register_session_non_string_sid(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': ['x']}))
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_rotation_releases_retired_clients_after_grace(monkeypatch):
    '''Retired clients stay open while an offloaded call may still use them,
    then are closed on a timer — not kept until session close.'''
    monkeypatch.setattr(pg, 'GLOBUS_HTTP_TIMEOUT', 0.002)
    session = _make_session()
    first   = session._tc
    session.adopt_credential(_make_session(), keep_collection=True)
    first.close.assert_not_called()
    await asyncio.sleep(0.1)
    first.close.assert_called_once()
    assert session._retired == []           # released entries are pruned


@pytest.mark.asyncio
async def test_close_cancels_release_timers():
    session = _make_session()
    first   = session._tc
    session.adopt_credential(_make_session(), keep_collection=True)
    ((_, timer),) = session._retired
    await session.close()
    assert timer.cancelled()
    first.close.assert_called_once()


@pytest.mark.asyncio
async def test_register_session_cancelled_build_closes_clients(
        endpoint_app, monkeypatch):
    '''A refresh-token register cancelled mid-build: the offloaded thread
    finishes, and what it built is closed, not leaked.'''
    import threading
    plugin  = PluginGlobus(endpoint_app)
    release = threading.Event()
    built   = []
    orig    = plugin._create_session

    def _create(sid, **kwargs):
        release.wait(5)
        built.append(orig(sid, **kwargs))
        return built[-1]

    monkeypatch.setattr(plugin, '_create_session', _create)
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        task = asyncio.ensure_future(plugin.register_session(
            _register_request({'refresh_token': 'rt', 'client_id': 'c'})))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        for _ in range(100):
            if built and built[0]._tc.close.called:
                break
            await asyncio.sleep(0.01)
    built[0]._tc.close.assert_called_once()
    assert not plugin._sessions


@pytest.mark.asyncio
async def test_register_session_empty_sid(endpoint_app):
    plugin = PluginGlobus(endpoint_app)
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': ''}))
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_register_session_cancelled_recheck_closes_clients(
        endpoint_app, monkeypatch):
    plugin = PluginGlobus(endpoint_app)
    built  = []
    orig   = plugin._create_session

    def _create(sid, **kwargs):
        built.append(orig(sid, **kwargs))
        return built[-1]

    async def _cancelled(sid):
        raise asyncio.CancelledError()

    monkeypatch.setattr(plugin, '_create_session', _create)
    monkeypatch.setattr(plugin, '_live_session',   _cancelled)
    with patch('radical.orbit.plugin_globus.globus_sdk'):
        with pytest.raises(asyncio.CancelledError):
            await plugin.register_session(
                _register_request({'access_token': 'tok'}))
    built[0]._tc.close.assert_called_once()
    assert not plugin._sessions




def test_client_globus_403_is_raised_not_fallen_back():
    '''A Globus-side 403 (credential refused) surfaces; it is not mistaken
    for the ownership refusal.'''
    client = GlobusClient.__new__(GlobusClient)
    client._sid   = 's1'
    client._http  = MagicMock()
    client._url   = lambda path: path
    raised = []
    client._raise = lambda resp: raised.append(resp.status_code)
    denied = MagicMock(status_code=403,
                       text='Globus register_session: forbidden')
    denied.json.return_value = {'sid': 's1'}
    client._http.post.return_value = denied

    client.register_session(refresh_token='rt', client_id='c')
    assert client._http.post.call_count == 1
    assert raised == [403]


def test_client_new_session_sends_no_sid():
    client = GlobusClient.__new__(GlobusClient)
    client._sid   = 's1'
    client._http  = MagicMock()
    client._url   = lambda path: path
    client._raise = lambda resp: None
    client._http.post.return_value = MagicMock(status_code=200)
    client._http.post.return_value.json.return_value = {'sid': 's2'}

    client.register_session(access_token='other-identity', new_session=True)
    assert 'sid' not in client._http.post.call_args.kwargs['json']
    assert client._sid == 's2'
    # the old session may be shared: it is not released implicitly
    assert client._http.post.call_count == 1




@pytest.mark.asyncio
async def test_rotation_refused_when_session_ends_during_build(
        endpoint_app, monkeypatch):
    '''An unregister (or expiry) landing while a rotation's credential is
    built must not be undone by recreating the session.'''
    plugin = PluginGlobus(endpoint_app)
    with patch('radical.orbit.plugin_globus.globus_sdk') as gs:
        gs.TransferClient.side_effect = lambda **_: MagicMock()
        await plugin.register_session(
            _register_request({'access_token': 'tok', 'sid': 's1'}, 'ep1'))
        orig  = plugin._create_session
        built = []

        def _create(sid, **kwargs):
            built.append(orig(sid, **kwargs))
            plugin._sessions.pop(sid)           # unregistered meanwhile
            plugin._records.pop(sid)
            return built[-1]

        monkeypatch.setattr(plugin, '_create_session', _create)
        with pytest.raises(HTTPException) as exc:
            await plugin.register_session(
                _register_request({'access_token': 'tok2', 'sid': 's1'},
                                  'ep1'))
    assert exc.value.status_code == 410
    assert 's1' not in plugin._sessions
    built[0]._tc.close.assert_called_once()


def test_client_raises_when_its_session_ended():
    '''A rotation of an ended session raises (410, with the reason); it
    does not silently swap in a new, empty session.'''
    client = GlobusClient.__new__(GlobusClient)
    client._sid   = 's1'
    client._http  = MagicMock()
    client._url   = lambda path: path
    raised = []
    client._raise = lambda resp: raised.append(resp.status_code)
    ended = MagicMock(status_code=410, text='session s1 expired ...')
    ended.json.return_value = {'sid': 's1'}
    client._http.post.return_value = ended

    client.register_session(access_token='tok2')
    assert client._http.post.call_count == 1
    assert raised == [410]


@pytest.mark.asyncio
async def test_globus_is_busy_until_a_transfer_is_reported_done():
    '''A transfer pins its session until reported terminal; INACTIVE (waits
    on a human) does not pin.'''
    pytest.importorskip('globus_sdk')
    session = GlobusSession('s1', access_token='tok')
    try:
        assert session.is_busy() is False
        session._tasks['t1'] = {'notified': 'ACTIVE', 'seen': time.time()}
        assert session.is_busy() is True
        session._tasks['t1'] = {'notified': 'INACTIVE'}
        assert session.is_busy() is False
        session._tasks['t1'] = {'notified': 'SUCCEEDED'}
        assert session.is_busy() is False
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_globus_stale_status_does_not_keep_the_session_busy(monkeypatch):
    '''A transfer whose status has not been fetched for GLOBUS_STALE_STATUS
    (e.g. the poll fails on an expired token) does not pin the session.'''
    pytest.importorskip('globus_sdk')
    session = GlobusSession('s1', access_token='tok')
    try:
        session._tasks['t1'] = {'notified': 'ACTIVE', 'seen': time.time()}
        assert session.is_busy() is True
        session._tasks['t1']['seen'] = time.time() - pg.GLOBUS_STALE_STATUS - 1
        assert session.is_busy() is False
    finally:
        await session.close()
