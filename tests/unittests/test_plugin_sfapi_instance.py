#!/usr/bin/env python

# pylint: disable=protected-access

import logging

import pytest

from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException

from cryptography.hazmat.primitives            import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from radical.orbit import plugin_sfapi_instance as psf
from radical.orbit.plugin_sfapi_instance import (
    PluginSFAPIInstance,
    SFAPIInstanceSession,
    SFAPITokenManager,
    _render_batch_script,
    _extract_jobid,
    _normalize_job,
    _job_state,
    _sfapi_raise,
    SFAPI_JOB_STATES_TERMINAL,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def rsa_pem():
    '''A throwaway RSA private key in PEM form (for constructor PEM parsing).'''
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()).decode()


@pytest.fixture
def broker_app():
    app = FastAPI()
    app.state.is_broker        = True
    app.state.endpoint_service = MagicMock()
    app.state.endpoint_name    = 'broker'
    app.state.broker_url       = ''
    return app


def _mock_tokens():
    '''A token manager stub whose token()/mint() are AsyncMocks.'''
    tokens = MagicMock(spec=SFAPITokenManager)
    tokens.token = AsyncMock(return_value='access-tok')
    tokens.mint  = AsyncMock(return_value='access-tok')
    tokens.aclose = AsyncMock()
    return tokens


def _session(endpoint='nersc'):
    return SFAPIInstanceSession('s1', endpoint=endpoint, tokens=_mock_tokens())


def _resp(status_code=200, json_data=None, is_success=None):
    r = MagicMock()
    r.status_code = status_code
    r.is_success  = (200 <= status_code < 300) if is_success is None \
                                               else is_success
    r.json.return_value = json_data
    r.text = ''
    return r


# ---------------------------------------------------------------------------
# sbatch renderer
# ---------------------------------------------------------------------------

def test_render_batch_script_fields_and_order():
    spec = {
        'name'       : 'job1',
        'executable' : '/bin/echo',
        'arguments'  : ['hello', 'world'],
        'directory'  : '/scratch/run',
        'resources'  : {'node_count': 2, 'exclusive_node_use': True},
        'attributes' : {'duration'     : 90,           # 90s -> 2 min (ceil)
                        'account'      : 'm5290',
                        'constraint'   : 'gpu',
                        'queue_name'   : 'debug',
                        'reservation'  : 'resv1',
                        'gpus_per_node': 4},
        'environment': {},
    }
    script = _render_batch_script(spec)
    lines  = script.splitlines()

    assert lines[0] == '#!/bin/bash'
    sbatch = [l for l in lines if l.startswith('#SBATCH')]
    assert sbatch == [
        '#SBATCH --export=NONE',
        '#SBATCH --job-name=job1',
        '#SBATCH --nodes=2',
        '#SBATCH --time=2',
        '#SBATCH --account=m5290',
        '#SBATCH --constraint=gpu',
        '#SBATCH --qos=debug',
        '#SBATCH --reservation=resv1',
        '#SBATCH --gpus-per-node=4',
        '#SBATCH --exclusive',
        '#SBATCH --chdir=/scratch/run',
    ]


def test_render_batch_script_qos_prefers_explicit():
    spec = {'executable': '/bin/true',
            'attributes': {'qos': 'premium', 'queue_name': 'debug'}}
    assert '#SBATCH --qos=premium' in _render_batch_script(spec)
    assert '--qos=debug' not in _render_batch_script(spec)


def test_render_batch_script_omits_absent_fields():
    spec   = {'executable': '/bin/true'}
    script = _render_batch_script(spec)
    for flag in ('--job-name', '--nodes', '--time', '--account',
                 '--constraint', '--qos', '--reservation',
                 '--gpus-per-node', '--exclusive', '--chdir'):
        assert flag not in script
    # --export=NONE is unconditional: deterministic job env by design
    assert '#SBATCH --export=NONE' in script


def test_render_batch_script_env_quoting():
    spec = {'executable' : '/bin/true',
            'environment': {'A'    : 'has space',
                            'B'    : 'semi;colon && rm',
                            'TOKEN': 'abc123'}}
    script = _render_batch_script(spec)
    assert "export A='has space'" in script
    assert "export B='semi;colon && rm'" in script
    assert 'export TOKEN=abc123' in script


def test_render_batch_script_exec_arg_quoting():
    spec = {'executable': '/bin/echo',
            'arguments' : ['a b', '$HOME', ';rm']}
    script = _render_batch_script(spec)
    last   = script.splitlines()[-1]
    assert last == "exec /bin/echo 'a b' '$HOME' ';rm'"


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------

def test_extract_jobid_from_json_string():
    assert _extract_jobid('{"jobid": "12345"}') == '12345'
    assert _extract_jobid({'jobid': 999})       == '999'


def test_extract_jobid_missing():
    assert _extract_jobid('{"status": "ERROR"}') == ''
    assert _extract_jobid(None)                   == ''
    assert _extract_jobid('not json')             == ''


def test_job_state_splits_compound():
    assert _job_state({'state': 'CANCELLED by 12345'}) == 'cancelled'
    assert _job_state({'state': 'RUNNING'})            == 'running'
    assert _job_state({})                              == ''


def test_normalize_job_aliases():
    payload = {'status': 'ok',
               'output': [{'jobid': '77', 'state': 'COMPLETED'}]}
    norm    = _normalize_job(payload)
    assert norm['job_id']         == '77'
    assert norm['status']         == {'state': 'completed'}
    # raw payload preserved alongside the aliases
    assert norm['output'][0]['jobid'] == '77'


def test_normalize_job_empty_output():
    norm = _normalize_job({'status': 'ok', 'output': []}, job_id='42')
    assert norm['job_id'] == '42'
    assert norm['status'] == {'state': ''}


def test_terminal_states_membership():
    for st in ('completed', 'failed', 'cancelled', 'timeout',
               'node_fail', 'out_of_memory', 'preempted', 'deadline'):
        assert st in SFAPI_JOB_STATES_TERMINAL
    assert 'running' not in SFAPI_JOB_STATES_TERMINAL


# ---------------------------------------------------------------------------
# _sfapi_raise
# ---------------------------------------------------------------------------

def test_sfapi_raise_success():
    _sfapi_raise(_resp(200))  # no raise


def test_sfapi_raise_401():
    r = _resp(401)
    r.json.side_effect = ValueError('no json')
    with pytest.raises(HTTPException) as ei:
        _sfapi_raise(r, 'get_job_status')
    assert ei.value.status_code == 401
    assert 'token expired' in ei.value.detail


def test_sfapi_raise_500_uses_upstream_message():
    r = _resp(500)
    r.json.return_value = {'detail': 'boom'}
    with pytest.raises(HTTPException) as ei:
        _sfapi_raise(r, 'submit_job')
    assert ei.value.status_code == 502
    assert 'boom' in ei.value.detail


# ---------------------------------------------------------------------------
# submit flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_job_success_form_post_and_jobid():
    session = _session()
    submit  = _resp(200, {'id': 'task-1', 'status': 'completed',
                          'result': '{"jobid": "55"}'})

    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=submit) as req:
        out = await session.submit_job('perlmutter',
                                       {'executable': '/bin/true',
                                        'name': 'j'})

    assert out == {'job_id': '55', 'status': {'state': 'pending'}}
    assert '55' in session._jobs
    # POST with form data (data=), never json=
    method, url = req.call_args.args
    assert method == 'POST'
    assert url    == '/compute/jobs/perlmutter'
    assert 'data' in req.call_args.kwargs
    assert 'json' not in req.call_args.kwargs
    assert req.call_args.kwargs['data']['isPath'] == 'false'
    assert req.call_args.kwargs['data']['job'].startswith('#!/bin/bash')
    session._jobs.clear()
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_polls_task_until_complete():
    session  = _session()
    submit   = _resp(200, {'id': 'task-2', 'status': 'pending'})
    poll1    = _resp(200, {'id': 'task-2', 'status': 'pending'})
    poll2    = _resp(200, {'id': 'task-2', 'status': 'completed',
                           'result': '{"jobid": "88"}'})

    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      side_effect=[submit, poll1, poll2]), \
         patch.object(psf.asyncio, 'sleep', new_callable=AsyncMock):
        out = await session.submit_job('perlmutter',
                                       {'executable': '/bin/true'})
    assert out['job_id'] == '88'
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_task_failed_raises_502_with_result():
    session = _session()
    submit  = _resp(200, {'id': 'task-3', 'status': 'failed',
                          'result': 'sbatch: error: bad account'})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=submit):
        with pytest.raises(HTTPException) as ei:
            await session.submit_job('perlmutter', {'executable': '/bin/true'})
    assert ei.value.status_code == 502
    assert 'bad account' in ei.value.detail
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_non_object_reply_raises_502():
    # a successful submit reply that is not a JSON object must map to a
    # clean 502, not an AttributeError inside the handler
    session = _session()
    submit  = _resp(200, ['not', 'an', 'object'])
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=submit):
        with pytest.raises(HTTPException) as ei:
            await session.submit_job('perlmutter', {'executable': '/bin/true'})
    assert ei.value.status_code == 502
    assert 'non-object' in ei.value.detail
    await session._http.aclose()


@pytest.mark.asyncio
async def test_list_replies_tolerate_non_container_json():
    # scalar JSON bodies (string / number) must yield empty lists, not raise
    session = _session()
    scalar  = _resp(200, 'maintenance')
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=scalar):
        assert (await session.list_resources())['resources'] == []
        assert (await session.list_incidents())['incidents'] == []
        assert (await session.list_projects())['projects']   == []
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_completed_without_jobid_raises_502():
    session = _session()
    submit  = _resp(200, {'id': 'task-4', 'status': 'completed',
                          'result': '{"status": "ERROR", "error": "nope"}'})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=submit):
        with pytest.raises(HTTPException) as ei:
            await session.submit_job('perlmutter', {'executable': '/bin/true'})
    assert ei.value.status_code == 502
    assert 'without a jobid' in ei.value.detail
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_poll_timeout_reports_task_id():
    session = _session()
    submit  = _resp(200, {'id': 'task-5', 'status': 'pending'})
    final   = _resp(200, {'id': 'task-5', 'status': 'pending'})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      side_effect=[submit, final]), \
         patch.object(psf, 'SFAPI_TASK_POLL_TIMEOUT', 0):
        with pytest.raises(HTTPException) as ei:
            await session.submit_job('perlmutter', {'executable': '/bin/true'})
    assert ei.value.status_code == 502
    assert 'task-5' in ei.value.detail
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_tolerates_transient_task_404():
    session = _session()
    submit  = _resp(200, {'id': 'task-6', 'status': 'pending'})
    miss    = _resp(404)
    done    = _resp(200, {'id': 'task-6', 'status': 'completed',
                          'result': '{"jobid": "61"}'})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      side_effect=[submit, miss, done]), \
         patch.object(psf.asyncio, 'sleep', new_callable=AsyncMock):
        out = await session.submit_job('perlmutter', {'executable': '/bin/true'})
    assert out['job_id'] == '61'
    await session._http.aclose()


@pytest.mark.asyncio
async def test_submit_job_script_never_logged(caplog):
    '''[R6] the rendered script embeds the broker token — it must appear in
    the sbatch body but never in any log record.'''
    session = _session()
    token   = 'super-secret-broker-token'
    submit  = _resp(200, {'id': 'task-7', 'status': 'completed',
                          'result': '{"jobid": "70"}'})
    spec    = {'executable' : '/bin/true',
               'environment': {'RADICAL_ORBIT_BROKER_TOKEN': token}}

    # sanity: the token really is in the rendered script
    assert token in _render_batch_script(spec)

    with caplog.at_level(logging.DEBUG, logger='radical.orbit'):
        with patch.object(session._http, 'request', new_callable=AsyncMock,
                          return_value=submit):
            await session.submit_job('perlmutter', spec)

    for rec in caplog.records:
        assert token not in rec.getMessage()
    session._jobs.clear()
    await session._http.aclose()


# ---------------------------------------------------------------------------
# status / list / cancel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_job_status_uses_sacct_and_normalizes():
    session = _session()
    resp    = _resp(200, {'status': 'ok',
                          'output': [{'jobid': '9', 'state': 'RUNNING'}]})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=resp) as req:
        out = await session.get_job_status('perlmutter', '9')
    assert req.call_args.kwargs['params'] == {'sacct': 'true'}
    assert out['job_id'] == '9'
    assert out['status'] == {'state': 'running'}
    await session._http.aclose()


@pytest.mark.asyncio
async def test_list_jobs_aliases_entries():
    session = _session()
    resp    = _resp(200, {'output': [{'jobid': '1', 'state': 'PENDING'},
                                     {'jobid': '2', 'state': 'RUNNING'}]})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=resp):
        out = await session.list_jobs('perlmutter')
    assert [j['job_id'] for j in out['jobs']] == ['1', '2']
    assert out['jobs'][0]['status'] == {'state': 'pending'}
    await session._http.aclose()


@pytest.mark.asyncio
async def test_cancel_job_pops_and_accepts_202():
    session = _session()
    session._jobs['5'] = {'resource_id': 'perlmutter', 'state': 'running'}
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=_resp(202)):
        out = await session.cancel_job('perlmutter', '5')
    assert out['status'] == 'canceled'
    assert '5' not in session._jobs
    await session._http.aclose()


# ---------------------------------------------------------------------------
# poller
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_job_status_empty_output_no_change():
    session = _session()
    meta    = {'resource_id': 'perlmutter', 'state': 'pending'}
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=_resp(200, {'output': []})):
        assert await session._fetch_job_status('9', meta) is None
    assert meta['state'] == 'pending'
    await session._http.aclose()


@pytest.mark.asyncio
async def test_fetch_job_status_reports_state_change():
    session = _session()
    meta    = {'resource_id': 'perlmutter', 'state': 'pending', 'name': 'j'}
    resp    = _resp(200, {'output': [{'jobid': '9', 'state': 'COMPLETED'}]})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=resp):
        fields = await session._fetch_job_status('9', meta)
    assert meta['state'] == 'completed'
    payload = session._job_payload('9', meta, fields)
    assert payload == {'job_id'     : '9',
                       'state'      : 'completed',
                       'resource_id': 'perlmutter',
                       'name'       : 'j',
                       'details'    : fields}
    await session._http.aclose()


@pytest.mark.asyncio
async def test_fetch_job_status_cancelled_compound_state():
    session = _session()
    meta    = {'resource_id': 'perlmutter', 'state': 'running'}
    resp    = _resp(200, {'output': [{'state': 'CANCELLED by 123'}]})
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      return_value=resp):
        await session._fetch_job_status('9', meta)
    assert meta['state'] == 'cancelled'
    await session._http.aclose()


# ---------------------------------------------------------------------------
# 401 retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_request_retries_once_on_401():
    session = _session()
    with patch.object(session._http, 'request', new_callable=AsyncMock,
                      side_effect=[_resp(401), _resp(200, {'ok': 1})]) as req:
        resp = await session._request('GET', '/status')
    assert resp.is_success
    assert req.call_count == 2
    session._tokens.mint.assert_awaited_once()
    await session._http.aclose()


# ---------------------------------------------------------------------------
# token manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_manager_refresh_before_expiry():
    mgr = SFAPITokenManager('cid', 'pem', 'https://oidc/token')
    clock = {'t': 1000.0}
    mgr._clock = lambda: clock['t']

    calls = []

    async def fake_fetch():
        calls.append(1)
        return {'access_token': f'tok{len(calls)}',
                'expires_at': clock['t'] + 600}

    mgr._fetch = fake_fetch

    assert await mgr.token() == 'tok1'
    # still valid — no new fetch
    assert await mgr.token() == 'tok1'
    assert len(calls) == 1
    # advance past the refresh margin -> refetch
    clock['t'] += 600
    assert await mgr.token() == 'tok2'
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_token_manager_single_flight():
    import asyncio
    mgr = SFAPITokenManager('cid', 'pem', 'https://oidc/token')

    calls = []

    async def fake_fetch():
        calls.append(1)
        await asyncio.sleep(0)
        return {'access_token': 'tok', 'expires_in': 600}

    mgr._fetch = fake_fetch
    await asyncio.gather(mgr.token(), mgr.token(), mgr.token())
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_token_manager_oauth_error_maps_to_401():
    mgr = SFAPITokenManager('cid', 'pem', 'https://oidc/token')

    async def fake_fetch():
        raise psf.OAuth2Error(error='invalid_client',
                              description='bad key')

    mgr._fetch = fake_fetch
    with pytest.raises(HTTPException) as ei:
        await mgr.mint()
    assert ei.value.status_code == 401


@pytest.mark.asyncio
async def test_token_manager_missing_authlib_raises_importerror():
    with patch.object(psf, '_HAVE_AUTHLIB', False):
        with pytest.raises(ImportError):
            SFAPITokenManager('cid', 'pem', 'https://oidc/token')


# ---------------------------------------------------------------------------
# plugin construction ([R2] validate before routes registered)
# ---------------------------------------------------------------------------

def test_plugin_init_ok(broker_app, rsa_pem):
    plugin = PluginSFAPIInstance(broker_app, 'sfapi.nersc',
                                 endpoint='nersc',
                                 client_id='cid', private_key=rsa_pem)
    assert plugin._endpoint_key == 'nersc'
    assert plugin._auto_sid in plugin._sessions
    assert plugin.session_ttl == 0
    assert 'SFAPI' in plugin.ui_config['title']


def test_plugin_no_plugin_name():
    assert not hasattr(PluginSFAPIInstance, 'plugin_name')


def test_plugin_bad_pem_raises_before_routes(broker_app):
    before = len(broker_app.state.direct_routes) \
             if hasattr(broker_app.state, 'direct_routes') else 0
    with pytest.raises(HTTPException) as ei:
        PluginSFAPIInstance(broker_app, 'sfapi.nersc',
                            endpoint='nersc',
                            client_id='cid', private_key='not-a-pem')
    assert ei.value.status_code == 400
    after = len(broker_app.state.direct_routes) \
            if hasattr(broker_app.state, 'direct_routes') else 0
    assert after == before


def test_plugin_missing_client_id_raises(broker_app, rsa_pem):
    with pytest.raises(HTTPException) as ei:
        PluginSFAPIInstance(broker_app, 'sfapi.nersc',
                            endpoint='nersc',
                            client_id='', private_key=rsa_pem)
    assert ei.value.status_code == 400


def test_plugin_unknown_endpoint_rejected(broker_app, rsa_pem):
    with pytest.raises(HTTPException) as ei:
        PluginSFAPIInstance(broker_app, 'sfapi.bogus',
                            endpoint='bogus',
                            client_id='cid', private_key=rsa_pem)
    assert ei.value.status_code == 400


@pytest.mark.asyncio
async def test_plugin_update_credentials(broker_app, rsa_pem):
    plugin = PluginSFAPIInstance(broker_app, 'sfapi.nersc',
                                 endpoint='nersc',
                                 client_id='cid', private_key=rsa_pem)
    sess = plugin._sessions[plugin._auto_sid]
    sess._tokens = MagicMock()
    plugin.update_credentials('cid2', rsa_pem)
    assert plugin._client_id == 'cid2'
    sess._tokens.update_credentials.assert_called_once_with(
        'cid2', rsa_pem.strip())


@pytest.mark.asyncio
async def test_plugin_register_session_returns_auto_sid(broker_app, rsa_pem):
    plugin = PluginSFAPIInstance(broker_app, 'sfapi.nersc',
                                 endpoint='nersc',
                                 client_id='cid', private_key=rsa_pem)
    result = await plugin.register_session(MagicMock())
    assert result['sid'] == plugin._auto_sid
