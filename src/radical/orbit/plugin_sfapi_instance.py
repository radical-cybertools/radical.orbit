'''
SFAPI Instance Plugin — per-endpoint direct-SFAPI integration for NERSC.

Dynamically registered by ``PluginSFAPIConnect`` via
``register_dynamic_plugin(PluginSFAPIInstance, 'sfapi.nersc', ...)``.
It is the direct-SFAPI sibling of ``PluginIRIInstance``: same route surface
and Explorer page, but it launches jobs through NERSC's Superfacility API
(``api.nersc.gov``) instead of the IRI facility API.

Credential lifecycle
--------------------
The OAuth2 client id and RSA private key (PEM) are passed at construction
time by ``sfapi_connect`` and live in broker process memory only (inside the
:class:`SFAPITokenManager`).  They are **never** written to disk.  Short-lived
access tokens are minted / refreshed via ``authlib`` (client-credentials with
``private_key_jwt``) and also stay in memory only.

Design notes
------------
* **No ``plugin_name`` class attribute** — the class is not auto-registered
  in the global ``Plugin._registry``.  Instances are created exclusively by
  ``PluginSFAPIConnect``.
* A single pre-created session is stored under a fixed SID; ``register_session``
  always returns it (routes omit ``{sid}``), so the Explorer's
  ``api.getSession()`` flow works unchanged.
* The rendered sbatch script embeds the broker token, so it is **never**
  logged and never echoed into error messages.
'''

import asyncio
import json
import logging
import math
import os
import shlex
import time

from typing import Any, Dict, Optional

import httpx

from fastapi import FastAPI, HTTPException, Request

from cryptography.hazmat.primitives.serialization import load_pem_private_key

from .http_utils          import make_async_http_client
from .plugin_session_base import PluginSession
from .plugin_base         import Plugin
from .sfapi_endpoints     import SFAPI_ENDPOINTS
from .plugin_iri_instance import IRIInstanceClient, _iri_extract_message

log = logging.getLogger('radical.orbit')

# authlib is an optional-but-guarded dependency (house pattern: it lives in
# requirements.txt unconditionally, the import is ImportError-tolerant here,
# and sfapi_connect maps a missing dependency to HTTP 501).
try:
    from authlib.integrations.httpx_client import AsyncOAuth2Client
    from authlib.oauth2.rfc7523            import PrivateKeyJWT
    from authlib.oauth2                    import OAuth2Error
    from authlib.integrations.base_client  import OAuthError
    _HAVE_AUTHLIB = True
except ImportError:
    AsyncOAuth2Client = None           # type: ignore
    PrivateKeyJWT     = None           # type: ignore
    OAuth2Error       = Exception      # type: ignore
    OAuthError        = Exception      # type: ignore
    _HAVE_AUTHLIB     = False

# Background job-poll interval (seconds)
SFAPI_POLL_INTERVAL = 10.0

# Submit → Task polling: SFAPI's submit returns a Task; poll it until the job
# id materializes.  Capped so a stuck task never blocks the request forever.
SFAPI_TASK_POLL_INTERVAL =   1.0
SFAPI_TASK_POLL_TIMEOUT  = 120.0

# Refresh an access token once fewer than this many seconds of validity remain.
SFAPI_TOKEN_REFRESH_MARGIN = 60.0

# Terminal Slurm/sacct states (lower-case, compound-state prefix already split).
SFAPI_JOB_STATES_TERMINAL = {'completed', 'failed', 'cancelled', 'timeout',
                             'node_fail', 'out_of_memory', 'preempted',
                             'deadline'}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _sfapi_raise(resp: httpx.Response, context: str = '') -> None:
    '''Map SFAPI HTTP errors to HTTPExceptions (mirrors ``_iri_raise``).

    Reuses ``_iri_extract_message`` so an upstream diagnostic (JSON
    ``detail``/``title``/``message``, or a raw body snippet) is appended to
    the generic message rather than swallowed.
    '''
    if resp.is_success:
        return

    prefix = f'SFAPI {context}: ' if context else 'SFAPI: '
    sc     = resp.status_code
    msg    = _iri_extract_message(resp)
    suffix = f' — {msg}' if msg else ''

    if   sc == 401:
        raise HTTPException(status_code=401, detail=f'{prefix}token expired or invalid{suffix}')
    elif sc == 403:
        raise HTTPException(status_code=403, detail=f'{prefix}forbidden{suffix}')
    elif sc == 404:
        raise HTTPException(status_code=404, detail=f'{prefix}resource or job not found{suffix}')
    elif sc == 429:
        raise HTTPException(status_code=429, detail=f'{prefix}rate limited by SFAPI endpoint{suffix}')
    else:
        raise HTTPException(status_code=502, detail=f'{prefix}{msg}')


def _truncate(text: Any, limit: int = 200) -> str:
    '''Collapse whitespace and truncate an arbitrary value to a short string.'''
    if text is None:
        return ''
    if not isinstance(text, str):
        try:
            text = json.dumps(text)
        except (TypeError, ValueError):
            text = str(text)
    return ' '.join(text.split())[:limit]


def _job_state(fields: Any) -> str:
    '''Normalize a Slurm/sacct ``state`` to a lower-case token.

    sacct may return compound states like ``CANCELLED by 12345`` — only the
    leading word carries the state, so split on whitespace first.
    '''
    if not isinstance(fields, dict):
        return ''
    raw   = str(fields.get('state') or '')
    parts = raw.split()
    return parts[0].lower() if parts else ''


def _job_fields(payload: Any) -> Dict[str, Any]:
    '''Extract the per-job field dict from an SFAPI status/list payload.

    A status reply is ``{status, output: [ {..fields..} ], error}``; a list
    entry is a bare field dict.  Empty ``output`` (job not yet in sacct)
    yields ``{}``.
    '''
    if not isinstance(payload, dict):
        return {}
    output = payload.get('output')
    if isinstance(output, list):
        if output and isinstance(output[0], dict):
            return output[0]
        return {}
    if isinstance(output, dict):
        return output
    return payload


def _normalize_job(payload: Any, job_id: str = '') -> Dict[str, Any]:
    '''Add Explorer-friendly ``job_id`` and ``status.state`` aliases.

    Keeps the raw SFAPI payload intact and layers normalized keys on top so
    the shared ``iri_instance.js`` page renders unchanged.
    '''
    if not isinstance(payload, dict):
        return {'job_id': job_id, 'status': {'state': ''}, 'raw': payload}

    result = dict(payload)
    fields = _job_fields(payload)
    jid    = job_id or fields.get('jobid') or fields.get('job_id') \
                    or result.get('jobid') or ''
    result['job_id'] = str(jid) if jid else job_id
    result['status'] = {'state': _job_state(fields)}
    return result


def _extract_jobid(result: Any) -> str:
    '''Pull the Slurm job id out of a completed task's ``result`` payload.

    A completed task normally carries a JSON string with a ``jobid``; a task
    can also complete with an error payload and no jobid (sbatch rejection),
    in which case this returns ``''``.
    '''
    if result is None:
        return ''
    if isinstance(result, dict):
        data = result
    else:
        try:
            data = json.loads(result)
        except (TypeError, ValueError):
            return ''
    if not isinstance(data, dict):
        return ''
    jid = data.get('jobid') or data.get('jobId') or data.get('job_id')
    return str(jid) if jid not in (None, '') else ''


def _render_batch_script(job_spec: Dict[str, Any]) -> str:
    '''Render a PSiJ-flavored job spec into an sbatch script.

    Absent fields are omitted; ``#SBATCH`` lines follow a fixed, deterministic
    order.  Environment values and exec arguments are ``shlex.quote``-d so
    spaces and shell metacharacters survive intact.

    NOTE: the result embeds the broker token (via the job environment); the
    caller must never log it or echo it into an error message.
    '''
    spec  = job_spec or {}
    attrs = spec.get('attributes') or {}
    res   = spec.get('resources')  or {}

    lines = ['#!/bin/bash']

    def sb(flag: str, val: Any) -> None:
        lines.append(f'#SBATCH {flag}={val}')

    # Deterministic environment: do NOT inherit the submitter's (server-side
    # captured) login environment — a polluted env silently wedges dragon's
    # backend bring-up.  Everything the job needs is exported explicitly in
    # the script body below.
    sb('--export', 'NONE')

    name = spec.get('name')
    if name:
        sb('--job-name', name)

    nodes = res.get('node_count')
    if nodes:
        sb('--nodes', int(nodes))

    duration = attrs.get('duration')
    if duration:
        minutes = int(math.ceil(float(duration) / 60.0))
        sb('--time', max(minutes, 1))

    account = attrs.get('account')
    if account:
        sb('--account', account)

    constraint = attrs.get('constraint')
    if constraint:
        sb('--constraint', constraint)

    # At NERSC the "queue" is a QOS; prefer an explicit qos, else queue_name.
    qos = attrs.get('qos') or attrs.get('queue_name')
    if qos:
        sb('--qos', qos)

    reservation = attrs.get('reservation')
    if reservation:
        sb('--reservation', reservation)

    gpus = attrs.get('gpus_per_node')
    if gpus:
        sb('--gpus-per-node', gpus)

    if res.get('exclusive_node_use'):
        lines.append('#SBATCH --exclusive')

    directory = spec.get('directory')
    if directory:
        sb('--chdir', directory)

    lines.append('')
    for key, val in (spec.get('environment') or {}).items():
        lines.append(f'export {key}={shlex.quote(str(val))}')

    lines.append('')
    executable = spec.get('executable', '')
    argv       = [executable] + list(spec.get('arguments') or [])
    lines.append('exec ' + ' '.join(shlex.quote(str(a)) for a in argv))

    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# Token manager
# ---------------------------------------------------------------------------

class SFAPITokenManager:
    '''Mint / refresh short-lived SFAPI access tokens (in memory only).

    OAuth2 client-credentials with ``private_key_jwt`` via ``authlib``.
    Refreshes when fewer than ``SFAPI_TOKEN_REFRESH_MARGIN`` seconds of
    validity remain; an ``asyncio.Lock`` collapses concurrent callers onto a
    single fetch.  Token-endpoint and transport errors are mapped to clear
    HTTPExceptions (401 / 502) so they never fall through to ``_forward``'s
    generic 500.
    '''

    def __init__(self, client_id: str, private_key: str, token_url: str):
        if not _HAVE_AUTHLIB:
            raise ImportError("sfapi support requires 'authlib'")

        self._client_id    = client_id
        self._private_key  = private_key
        self._token_url    = token_url
        self._oauth        = None
        self._access_token = ''
        self._expires_at   = 0.0
        self._lock         = asyncio.Lock()
        self._clock        = time.time   # injectable for tests

    def _build_client(self) -> 'AsyncOAuth2Client':
        return AsyncOAuth2Client(
            client_id                  = self._client_id,
            client_secret              = self._private_key,
            token_endpoint_auth_method = PrivateKeyJWT(self._token_url),
            grant_type                 = 'client_credentials',
            token_endpoint             = self._token_url,
            timeout                    = 30.0,
        )

    async def _fetch(self) -> Dict[str, Any]:
        '''Fetch a fresh token dict from the OIDC endpoint (raw authlib call).'''
        if self._oauth is None:
            self._oauth = self._build_client()
        return await self._oauth.fetch_token(self._token_url)

    async def _refresh(self) -> None:
        try:
            tok = await self._fetch()
        except (OAuth2Error, OAuthError) as exc:
            raise HTTPException(
                status_code=401,
                detail=f'SFAPI token request rejected: {exc}') from exc
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f'SFAPI token endpoint unreachable: {exc}') from exc

        access = tok.get('access_token')
        if not access:
            raise HTTPException(
                status_code=502,
                detail='SFAPI token response carried no access_token')

        self._access_token = access
        expires_at         = tok.get('expires_at')
        if expires_at is None:
            expires_at = self._clock() + float(tok.get('expires_in', 600))
        self._expires_at = float(expires_at)

    async def token(self) -> str:
        '''Return a valid access token, refreshing it when near expiry.'''
        async with self._lock:
            if self._access_token and \
                    self._clock() < self._expires_at - SFAPI_TOKEN_REFRESH_MARGIN:
                return self._access_token
            await self._refresh()
            return self._access_token

    async def mint(self) -> str:
        '''Force a fresh token fetch (eager credential verification).'''
        async with self._lock:
            await self._refresh()
            return self._access_token

    def update_credentials(self, client_id: str, private_key: str) -> None:
        '''Swap the client id / private key and drop the cached token.'''
        self._client_id    = client_id
        self._private_key  = private_key
        self._access_token = ''
        self._expires_at   = 0.0
        if self._oauth is not None:
            # authlib stores these on the client; mutate in place so the next
            # fetch uses the new credentials without rebuilding the client.
            self._oauth.client_id     = client_id
            self._oauth.client_secret = private_key

    async def aclose(self) -> None:
        if self._oauth is not None:
            await self._oauth.aclose()
            self._oauth = None


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class SFAPIInstanceSession(PluginSession):
    '''Per-endpoint SFAPI session: job submission + resource info.'''

    def __init__(self, sid: str, endpoint: str, tokens: SFAPITokenManager):
        super().__init__(sid)

        self._endpoint_key = endpoint
        self._endpoint     = SFAPI_ENDPOINTS[endpoint]
        self._tokens       = tokens

        self._http = make_async_http_client(
            base_url = self._endpoint['url'],
            timeout  = 30.0,
        )

        # job_id -> {resource_id, state, name, ...}
        self._jobs: Dict[str, Dict[str, Any]] = {}

    # -- authenticated request helper ---------------------------------------

    async def _request(self, method: str, url: str,
                       **kwargs: Any) -> httpx.Response:
        '''Issue an authenticated SFAPI request; refresh + retry once on 401.'''
        tok     = await self._tokens.token()
        headers = dict(kwargs.pop('headers', None) or {})
        headers['Authorization'] = f'Bearer {tok}'
        resp = await self._http.request(method, url, headers=headers, **kwargs)

        if resp.status_code == 401:
            # Token revoked / expired despite the refresh margin — force a
            # fresh mint and retry the request exactly once.
            tok = await self._tokens.mint()
            headers['Authorization'] = f'Bearer {tok}'
            resp = await self._http.request(method, url,
                                            headers=headers, **kwargs)
        return resp

    # -- job submission -----------------------------------------------------

    async def submit_job(self, resource_id: str,
                         job_spec: Dict[str, Any]) -> Dict[str, Any]:
        self._check_active()
        script = _render_batch_script(job_spec)
        try:
            resp = await self._request(
                'POST', f'/compute/jobs/{resource_id}',
                data={'job': script, 'isPath': 'false'})
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI submit_job: {exc}') from exc

        _sfapi_raise(resp, 'submit_job')

        task = resp.json()
        if not isinstance(task, dict):
            raise HTTPException(
                status_code=502,
                detail='SFAPI submit_job: unexpected non-object reply '
                       'from the task endpoint')
        task_id       = str(task.get('id') or task.get('task_id') or '')
        outcome, task = await self._poll_task(resource_id, task_id, task)

        if outcome == 'timeout':
            raise HTTPException(
                status_code=502,
                detail=f'SFAPI submit_job: task {task_id} did not complete '
                       f'within {int(SFAPI_TASK_POLL_TIMEOUT)}s; the '
                       f'submission may still land — reconcile via task id '
                       f'{task_id}')
        if outcome != 'completed':
            raise HTTPException(
                status_code=502,
                detail=f'SFAPI submit_job: task {task_id} {outcome}: '
                       f'{_truncate(task.get("result"))}')

        job_id = _extract_jobid(task.get('result'))
        if not job_id:
            raise HTTPException(
                status_code=502,
                detail=f'SFAPI submit_job: task {task_id} completed without '
                       f'a jobid: {_truncate(task.get("result"))}')

        self._jobs[job_id] = {
            'resource_id': resource_id,
            'state'      : 'pending',
            'name'       : job_spec.get('name', ''),
            'executable' : job_spec.get('executable', ''),
        }
        self._start_polling()

        log.info('[sfapi/%s] session %s: submitted job %s to %s',
                 self._endpoint_key, self._sid, job_id, resource_id)
        return {'job_id': job_id, 'status': {'state': 'pending'}}

    async def _poll_task(self, resource_id: str, task_id: str,
                         task: Dict[str, Any]) -> tuple:
        '''Poll a submit Task until it is terminal or the window expires.

        Returns ``(outcome, task)`` where *outcome* is one of ``completed`` /
        ``failed`` / ``cancelled`` / ``error`` / ``timeout``.  A transient 404
        on the task route (task not yet visible) is tolerated within the
        window; a final fetch is attempted before declaring a timeout.
        '''
        start = time.monotonic()
        while True:
            status = str(task.get('status') or '').lower()
            if status == 'completed':
                return 'completed', task
            if status in ('failed', 'cancelled', 'error'):
                return status, task

            if time.monotonic() - start >= SFAPI_TASK_POLL_TIMEOUT:
                task   = await self._fetch_task(resource_id, task_id, task)
                status = str(task.get('status') or '').lower()
                if status == 'completed':
                    return 'completed', task
                if status in ('failed', 'cancelled', 'error'):
                    return status, task
                return 'timeout', task

            await asyncio.sleep(SFAPI_TASK_POLL_INTERVAL)
            task = await self._fetch_task(resource_id, task_id, task)

    async def _fetch_task(self, resource_id: str, task_id: str,
                          current: Dict[str, Any]) -> Dict[str, Any]:
        '''Fetch one task; tolerate transient 404 / transport errors.'''
        try:
            resp = await self._request('GET', f'/tasks/{task_id}')
        except httpx.RequestError:
            return current
        if resp.status_code == 404:
            return current
        _sfapi_raise(resp, 'submit_job')
        data = resp.json()
        return data if isinstance(data, dict) else current

    async def get_job_status(self, resource_id: str,
                             job_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request(
                'GET', f'/compute/jobs/{resource_id}/{job_id}',
                params={'sacct': 'true'})
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI get_job_status: {exc}') from exc
        _sfapi_raise(resp, 'get_job_status')
        return _normalize_job(resp.json(), job_id)

    async def list_jobs(self, resource_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request('GET', f'/compute/jobs/{resource_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI list_jobs: {exc}') from exc
        _sfapi_raise(resp, 'list_jobs')
        data   = resp.json()
        output = data.get('output') if isinstance(data, dict) else data
        jobs   = output if isinstance(output, list) else []
        return {'jobs': [_normalize_job(j) for j in jobs]}

    async def cancel_job(self, resource_id: str,
                         job_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request(
                'DELETE', f'/compute/jobs/{resource_id}/{job_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI cancel_job: {exc}') from exc
        if resp.status_code not in (200, 202, 204):
            _sfapi_raise(resp, 'cancel_job')

        self._jobs.pop(job_id, None)
        log.info('[sfapi/%s] session %s: canceled job %s on %s',
                 self._endpoint_key, self._sid, job_id, resource_id)
        return {'job_id': job_id, 'status': 'canceled'}

    # -- resource info ------------------------------------------------------

    async def list_resources(self,
                             resource_type: str = 'compute') -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request('GET', '/status')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI list_resources: {exc}') from exc
        _sfapi_raise(resp, 'list_resources')
        data = resp.json()
        if   isinstance(data, list):
            resources = data
        elif isinstance(data, dict):
            resources = data.get('resources', data.get('systems', []))
        else:
            resources = []
        for r in resources:
            if isinstance(r, dict) and 'current_status' not in r:
                r['current_status'] = r.get('status', '')
        return {'resources': resources}

    async def get_resource(self, resource_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request('GET', f'/status/{resource_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI get_resource: {exc}') from exc
        _sfapi_raise(resp, 'get_resource')
        data = resp.json()
        if isinstance(data, dict) and 'current_status' not in data:
            data['current_status'] = data.get('status', '')
        return data

    async def list_incidents(self) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request('GET', '/status/outages')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI list_incidents: {exc}') from exc
        _sfapi_raise(resp, 'list_incidents')
        data = resp.json()
        if   isinstance(data, list):
            incidents = data
        elif isinstance(data, dict):
            incidents = data.get('outages', data.get('incidents', data))
        else:
            incidents = []
        return {'incidents': incidents}

    async def list_projects(self) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._request('GET', '/account/projects')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'SFAPI list_projects: {exc}') from exc
        _sfapi_raise(resp, 'list_projects')
        data = resp.json()
        if   isinstance(data, list):
            projects = data
        elif isinstance(data, dict):
            projects = data.get('projects', data)
        else:
            projects = []
        return {'projects': projects}

    async def list_allocations(self, project_id: str) -> Dict[str, Any]:
        # SFAPI has no separate allocations route — filter the projects list.
        projects = (await self.list_projects()).get('projects', [])
        allocs   = [p for p in projects
                    if isinstance(p, dict) and project_id in (
                        str(p.get('id', '')), str(p.get('name', '')),
                        str(p.get('repo_name', '')))]
        return {'allocations': allocs}

    # -- lifecycle ----------------------------------------------------------

    async def close(self) -> dict:
        # Close both HTTP clients (API client + token manager's authlib
        # client); poller cancellation is handled by the base close().
        await self._http.aclose()
        await self._tokens.aclose()
        return await super().close()

    # -- background polling -------------------------------------------------

    def _start_polling(self) -> None:
        '''(Re)start the shared status poller for the session's active jobs.'''
        self.start_status_poller(
            interval=SFAPI_POLL_INTERVAL,
            items=lambda: self._jobs,
            is_terminal=lambda meta: meta.get('state')
                                     in SFAPI_JOB_STATES_TERMINAL,
            fetch=self._fetch_job_status,
            to_payload=self._job_payload,
            topic='job_status',
            name=f'sfapi/{self._endpoint_key}')

    async def _fetch_job_status(self, job_id: str,
                                meta: Dict[str, Any]) -> Optional[Any]:
        '''Poll one job (sacct-backed); return the job fields on a state change.

        Polling MUST use ``?sacct=true`` — the squeue-backed default makes a
        finished job *disappear* instead of reporting its terminal state.
        Empty ``output`` (job not yet in sacct right after submit) means "no
        change".
        '''
        resource_id = meta['resource_id']
        resp = await self._request(
            'GET', f'/compute/jobs/{resource_id}/{job_id}',
            params={'sacct': 'true'})
        if not resp.is_success:
            return None

        fields = _job_fields(resp.json())
        if not fields:
            return None

        new_state = _job_state(fields)
        if new_state and new_state != meta.get('state'):
            meta['state'] = new_state
            return fields
        return None

    @staticmethod
    def _job_payload(job_id: str, meta: Dict[str, Any], status: Any) -> dict:
        '''Build the ``job_status`` notification payload for one job.'''
        return {
            'job_id'     : job_id,
            'state'      : meta['state'],
            'resource_id': meta['resource_id'],
            'name'       : meta.get('name', ''),
            'details'    : status,
        }


# ---------------------------------------------------------------------------
# Plugin (no plugin_name — not auto-registered)
# ---------------------------------------------------------------------------

class PluginSFAPIInstance(Plugin):
    '''Per-endpoint direct-SFAPI plugin, dynamically registered by sfapi_connect.

    Shares the Explorer page and route surface with ``PluginIRIInstance`` and
    reuses ``IRIInstanceClient`` on the client side.
    '''

    session_class = SFAPIInstanceSession
    client_class  = IRIInstanceClient
    version       = '0.0.1'
    session_ttl   = 0  # no expiry — plugin lifecycle managed by sfapi_connect
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'iri_instance.js')

    def __init__(self, app: FastAPI, instance_name: str,
                 endpoint: str = '', client_id: str = '',
                 private_key: str = '',
                 token_manager: Optional[SFAPITokenManager] = None):

        # Validate everything (incl. parsing the PEM) BEFORE super().__init__()
        # so a failed construction leaves no orphaned routes in the shared
        # direct-routes table.
        if endpoint not in SFAPI_ENDPOINTS:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown endpoint {endpoint!r}. '
                       f'Valid: {list(SFAPI_ENDPOINTS.keys())}')
        entry = SFAPI_ENDPOINTS[endpoint]
        if not client_id or not client_id.strip():
            raise HTTPException(status_code=400,
                                detail='client_id must not be empty')
        if not private_key or not private_key.strip():
            raise HTTPException(status_code=400,
                                detail='private_key must not be empty')
        try:
            load_pem_private_key(private_key.strip().encode(), password=None)
        except Exception as exc:
            raise HTTPException(status_code=400,
                                detail=f'invalid private key: {exc}') from exc

        label = entry['label']
        self.ui_config = {
            'icon'       : '🔐',
            'title'      : f'SFAPI — {label}',
            'description': f'SFAPI endpoint: {label}',
        }

        super().__init__(app, instance_name)

        self._endpoint_key = endpoint
        self._client_id    = client_id.strip()

        tokens = token_manager or SFAPITokenManager(
            self._client_id, private_key.strip(), entry['token_url'])

        # Pre-create the single session
        self._auto_sid = f'session.{endpoint}'
        session = self._create_session(
            self._auto_sid, endpoint=endpoint, tokens=tokens)
        self._sessions[self._auto_sid]            = session
        self._session_last_access[self._auto_sid] = time.time()

        # Routes (no {sid}, identical surface to PluginIRIInstance)
        self.add_route_get ('resources',                     self.list_resources)
        self.add_route_get ('resource/{resource_id}',        self.get_resource)
        self.add_route_post('submit/{resource_id}',          self.submit_job)
        self.add_route_get ('status/{resource_id}/{job_id}', self.get_job_status)
        self.add_route_post('jobs/{resource_id}',            self.list_jobs)
        self.add_route_post('cancel/{resource_id}/{job_id}', self.cancel_job)
        self.add_route_get ('incidents',                     self.list_incidents)
        self.add_route_get ('projects',                      self.list_projects)
        self.add_route_get ('allocations/{project_id}',      self.list_allocations)

    # -- credential rotation ------------------------------------------------

    def update_credentials(self, client_id: str, private_key: str) -> None:
        '''Swap the SFAPI credentials on the live session's token manager.

        Called by ``sfapi_connect.connect`` on a re-connect for an already
        registered instance (after the new credentials have been verified).
        '''
        self._client_id = client_id.strip()
        sess = self._sessions.get(self._auto_sid)
        if sess:
            sess._tokens.update_credentials(client_id.strip(),
                                            private_key.strip())

    # -- session override ---------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Return the pre-created session ID (single session per instance).'''
        return {'sid': self._auto_sid}

    # -- route handlers (delegate to auto-session) --------------------------

    async def list_resources(self, request: Request) -> dict:
        resource_type = request.query_params.get('resource_type', 'compute')
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.list_resources, resource_type)

    async def get_resource(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.get_resource, resource_id)

    async def submit_job(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        body        = await request.json()
        job_spec    = body.get('job_spec', body)
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.submit_job,
            resource_id, job_spec)

    async def get_job_status(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        job_id      = request.path_params['job_id']
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.get_job_status,
            resource_id, job_id)

    async def list_jobs(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.list_jobs, resource_id)

    async def cancel_job(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        job_id      = request.path_params['job_id']
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.cancel_job,
            resource_id, job_id)

    async def list_incidents(self, request: Request) -> dict:
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.list_incidents)

    async def list_projects(self, request: Request) -> dict:
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.list_projects)

    async def list_allocations(self, request: Request) -> dict:
        project_id = request.path_params['project_id']
        return await self._forward(
            self._auto_sid, SFAPIInstanceSession.list_allocations, project_id)
