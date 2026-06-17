'''
IRI Instance Plugin — per-endpoint IRI integration.

Dynamically registered by ``PluginIRIConnect`` via
``register_dynamic_plugin(PluginIRIInstance, 'iri.nersc', ...)``.
Each instance handles a single IRI endpoint (NERSC, OLCF, …) and
combines resource info and job submission on one page.

Token lifecycle
---------------
The token is passed at construction time by ``iri_connect`` and lives in
bridge process memory (inside the httpx client) for the lifetime of the
plugin instance.  It is **never** written to disk.

Design notes
------------
* **No ``plugin_name`` class attribute** — the class is not auto-registered
  in the global ``Plugin._registry``.  Instances are created exclusively by
  ``PluginIRIConnect``.
* A single pre-created session is stored under a fixed SID.  ``register_session``
  always returns this SID so the Explorer's ``api.getSession()`` flow works
  unchanged.
* Routes omit ``{sid}`` — all requests use the one internal session.
'''

import asyncio
import logging
import os
import time

from typing import Any, Dict

import httpx

from fastapi import FastAPI, HTTPException, Request

from .http_utils import make_async_http_client

from .plugin_session_base import PluginSession
from .plugin_base          import Plugin
from .client               import PluginClient
from .iri_endpoints        import IRI_ENDPOINTS, IRI_JOB_STATES_TERMINAL

log = logging.getLogger('radical.edge')

# Background poll interval (seconds)
IRI_POLL_INTERVAL = 10.0


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _iri_extract_message(resp: httpx.Response) -> str:
    '''Extract a human-readable error message from an IRI error response.'''
    import json as _json
    try:
        body = resp.json()
        msg  = body.get('detail', body.get('title', resp.text[:200]))
        # detail may itself embed JSON (e.g. S3M wraps inner errors)
        if isinstance(msg, str) and '{' in msg:
            try:
                inner     = _json.loads(msg[msg.index('{'):])
                inner_msg = inner.get('message', inner.get('detail', ''))
                if inner_msg:
                    msg = msg[:msg.index('{')].rstrip(': ') + ': ' + inner_msg
            except (ValueError, TypeError):
                pass
        return str(msg)
    except Exception:
        return resp.text[:200]


def _iri_raise(resp: httpx.Response, context: str = '') -> None:
    '''Map IRI API HTTP errors to HTTPExceptions.'''
    if resp.is_success:
        return

    prefix = f'IRI {context}: ' if context else 'IRI: '
    sc     = resp.status_code

    if   sc == 401:
        raise HTTPException(status_code=401, detail=f'{prefix}token expired or invalid')
    elif sc == 403:
        raise HTTPException(status_code=403, detail=f'{prefix}forbidden')
    elif sc == 404:
        raise HTTPException(status_code=404, detail=f'{prefix}resource or job not found')
    elif sc == 429:
        raise HTTPException(status_code=429, detail=f'{prefix}rate limited by IRI endpoint')
    else:
        msg = _iri_extract_message(resp)
        raise HTTPException(status_code=502, detail=f'{prefix}{msg}')


# ---------------------------------------------------------------------------
# Session (merged IRISession + IRIInfoSession)
# ---------------------------------------------------------------------------

class IRIInstanceSession(PluginSession):
    '''Per-endpoint IRI session: job submission + resource info.'''

    def __init__(self, sid: str, endpoint: str, token: str):
        super().__init__(sid)

        self._endpoint_key = endpoint
        self._endpoint     = IRI_ENDPOINTS[endpoint]
        self._token        = token

        self._http = make_async_http_client(
            base_url = self._endpoint['url'],
            headers  = {'Authorization': f'Bearer {self._token}'},
            timeout  = 30.0,
        )

        # job_id -> {resource_id, state, name, ...}
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._poll_task = None

    # -- job submission (from IRISession) -----------------------------------

    async def submit_job(self, resource_id: str,
                         job_spec: Dict[str, Any]) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.post(
                f'/api/v1/compute/job/{resource_id}', json=job_spec)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI submit_job: {exc}') from exc

        _iri_raise(resp, 'submit_job')
        data   = resp.json()
        job_id = data.get('job_id') or data.get('id') or str(data)

        self._jobs[job_id] = {
            'resource_id': resource_id,
            'state'      : data.get('status', {}).get('state', 'new')
                           if isinstance(data.get('status'), dict) else 'new',
            'name'       : job_spec.get('name', ''),
            'executable' : job_spec.get('executable', ''),
        }
        self._start_polling()

        log.info('[iri/%s] session %s: submitted job %s to %s',
                 self._endpoint_key, self._sid, job_id, resource_id)
        return {'job_id': job_id, 'status': data.get('status', {})}

    async def get_job_status(self, resource_id: str,
                             job_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get(
                f'/api/v1/compute/status/{resource_id}/{job_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI get_job_status: {exc}') from exc
        _iri_raise(resp, 'get_job_status')
        return resp.json()

    async def list_jobs(self, resource_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.post(
                f'/api/v1/compute/status/{resource_id}', json={})
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI list_jobs: {exc}') from exc
        _iri_raise(resp, 'list_jobs')
        return resp.json()

    async def cancel_job(self, resource_id: str,
                         job_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.delete(
                f'/api/v1/compute/cancel/{resource_id}/{job_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI cancel_job: {exc}') from exc
        if resp.status_code not in (200, 202, 204):
            _iri_raise(resp, 'cancel_job')

        self._jobs.pop(job_id, None)
        log.info('[iri/%s] session %s: canceled job %s on %s',
                 self._endpoint_key, self._sid, job_id, resource_id)
        return {'job_id': job_id, 'status': 'canceled'}

    # -- resource info (from IRIInfoSession) --------------------------------

    async def list_resources(self, resource_type: str = 'compute') -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get('/api/v1/status/resources',
                                         params={'resource_type': resource_type})
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI list_resources: {exc}') from exc
        _iri_raise(resp, 'list_resources')
        resources = resp.json()
        if not isinstance(resources, list):
            resources = resources.get('resources', [])
        return {'resources': resources}

    async def get_resource(self, resource_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get(f'/api/v1/status/resources/{resource_id}')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI get_resource: {exc}') from exc
        _iri_raise(resp, 'get_resource')
        return resp.json()

    async def list_incidents(self) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get('/api/v1/status/incidents')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI list_incidents: {exc}') from exc
        _iri_raise(resp, 'list_incidents')
        data      = resp.json()
        incidents = data if isinstance(data, list) else data.get('incidents', data)
        return {'incidents': incidents}

    async def list_projects(self) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get('/api/v1/account/projects')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI list_projects: {exc}') from exc
        _iri_raise(resp, 'list_projects')
        data     = resp.json()
        projects = data if isinstance(data, list) else data.get('projects', data)
        return {'projects': projects}

    async def list_allocations(self, project_id: str) -> Dict[str, Any]:
        self._check_active()
        try:
            resp = await self._http.get(
                f'/api/v1/account/projects/{project_id}/project_allocations')
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502,
                                detail=f'IRI list_allocations: {exc}') from exc
        _iri_raise(resp, 'list_allocations')
        data        = resp.json()
        allocations = data if isinstance(data, list) else data.get('allocations', data)
        return {'allocations': allocations}

    # -- lifecycle ----------------------------------------------------------

    async def close(self) -> dict:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        await self._http.aclose()
        return await super().close()

    # -- background polling -------------------------------------------------

    def _start_polling(self) -> None:
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_jobs())

    async def _poll_jobs(self) -> None:
        while True:
            try:
                await asyncio.sleep(IRI_POLL_INTERVAL)
                if not self._active:
                    break

                active = {jid: m for jid, m in list(self._jobs.items())
                          if m.get('state') not in IRI_JOB_STATES_TERMINAL}
                if not active:
                    break

                for job_id, meta in active.items():
                    resource_id = meta['resource_id']
                    try:
                        resp = await self._http.get(
                            f'/api/v1/compute/status/{resource_id}/{job_id}')
                        if not resp.is_success:
                            continue
                        data      = resp.json()
                        status    = data.get('status', data)
                        new_state = (status.get('state', '')
                                     if isinstance(status, dict) else '').lower()

                        if new_state and new_state != meta.get('state'):
                            old_state     = meta['state']
                            meta['state'] = new_state
                            log.debug('[iri/%s] job %s: %s -> %s',
                                      self._endpoint_key, job_id,
                                      old_state, new_state)
                            if self._plugin:
                                self._plugin._dispatch_notify('job_status', {
                                    'job_id'     : job_id,
                                    'state'      : new_state,
                                    'resource_id': resource_id,
                                    'name'       : meta.get('name', ''),
                                    'details'    : status,
                                })
                    except Exception as exc:
                        log.debug('[iri/%s] poll error for job %s: %s',
                                  self._endpoint_key, job_id, exc)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.debug('[iri/%s] _poll_jobs error: %s',
                          self._endpoint_key, exc)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class IRIInstanceClient(PluginClient):
    '''Client-side interface for an IRI instance plugin.'''

    def list_resources(self, resource_type: str = 'compute') -> Dict[str, Any]:
        resp = self._http.get(self._url('resources'),
                              params={'resource_type': resource_type})
        self._raise(resp)
        return resp.json()

    def get_resource(self, resource_id: str) -> Dict[str, Any]:
        resp = self._http.get(self._url(f'resource/{resource_id}'))
        self._raise(resp, f'get_resource {resource_id!r}')
        return resp.json()

    def submit_job(self, resource_id: str,
                   job_spec: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._http.post(
            self._url(f'submit/{resource_id}'), json={'job_spec': job_spec})
        self._raise(resp, f'submit_job {resource_id!r}')
        return resp.json()

    def get_job_status(self, resource_id: str, job_id: str) -> Dict[str, Any]:
        resp = self._http.get(self._url(f'status/{resource_id}/{job_id}'))
        self._raise(resp, f'get_job_status {job_id!r}')
        return resp.json()

    def list_jobs(self, resource_id: str) -> Dict[str, Any]:
        resp = self._http.post(self._url(f'jobs/{resource_id}'), json={})
        self._raise(resp, f'list_jobs {resource_id!r}')
        return resp.json()

    def cancel_job(self, resource_id: str, job_id: str) -> Dict[str, Any]:
        resp = self._http.post(self._url(f'cancel/{resource_id}/{job_id}'))
        self._raise(resp, f'cancel_job {job_id!r}')
        return resp.json()

    def list_incidents(self) -> Dict[str, Any]:
        resp = self._http.get(self._url('incidents'))
        self._raise(resp)
        return resp.json()

    def list_projects(self) -> Dict[str, Any]:
        resp = self._http.get(self._url('projects'))
        self._raise(resp)
        return resp.json()

    def list_allocations(self, project_id: str) -> Dict[str, Any]:
        resp = self._http.get(self._url(f'allocations/{project_id}'))
        self._raise(resp, f'list_allocations {project_id!r}')
        return resp.json()


# ---------------------------------------------------------------------------
# Plugin (no plugin_name — not auto-registered)
# ---------------------------------------------------------------------------

class PluginIRIInstance(Plugin):
    '''Per-endpoint IRI plugin, dynamically registered by iri_connect.'''

    session_class = IRIInstanceSession
    client_class  = IRIInstanceClient
    version       = '0.0.1'
    session_ttl   = 0  # no expiry — plugin lifecycle managed by iri_connect
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'iri_instance.js')

    def __init__(self, app: FastAPI, instance_name: str,
                 endpoint: str = '', token: str = ''):

        if endpoint not in IRI_ENDPOINTS:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown endpoint {endpoint!r}. '
                       f'Valid: {list(IRI_ENDPOINTS.keys())}')
        if not token or not token.strip():
            raise HTTPException(status_code=400, detail='token must not be empty')

        label = IRI_ENDPOINTS[endpoint]['label']
        self.ui_config = {
            'icon'       : '🔬',
            'title'      : f'IRI \u2014 {label}',
            'description': f'IRI endpoint: {label}',
        }

        super().__init__(app, instance_name)

        self._endpoint_key = endpoint
        self._token        = token.strip()

        # Pre-create the single session
        self._auto_sid = f'session.{endpoint}'
        session = self._create_session(
            self._auto_sid, endpoint=endpoint, token=self._token)
        self._sessions[self._auto_sid]            = session
        self._session_last_access[self._auto_sid] = time.time()

        # Routes (no {sid} — always use the single session)
        self.add_route_get ('resources',                   self.list_resources)
        self.add_route_get ('resource/{resource_id}',      self.get_resource)
        self.add_route_post('submit/{resource_id}',        self.submit_job)
        self.add_route_get ('status/{resource_id}/{job_id}', self.get_job_status)
        self.add_route_post('jobs/{resource_id}',          self.list_jobs)
        self.add_route_post('cancel/{resource_id}/{job_id}', self.cancel_job)
        self.add_route_get ('incidents',                   self.list_incidents)
        self.add_route_get ('projects',                    self.list_projects)
        self.add_route_get ('allocations/{project_id}',    self.list_allocations)

    # -- token rotation -----------------------------------------------------

    def update_token(self, new_token: str) -> None:
        '''Replace the bearer token used for outbound calls to this endpoint.

        Updates both the plugin's stored token and the live ``httpx`` client
        on the auto-session, so subsequent IRI calls use the new credential
        immediately.  Called by ``iri_connect.connect`` when a re-connect
        request arrives for an already-registered instance.
        '''
        self._token = new_token.strip()
        sess = self._sessions.get(self._auto_sid)
        if sess:
            sess._token = self._token
            sess._http.headers['Authorization'] = f'Bearer {self._token}'

    # -- session override ---------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Return the pre-created session ID (single session per instance).'''
        return {'sid': self._auto_sid}

    # -- route handlers (delegate to auto-session) --------------------------

    async def list_resources(self, request: Request) -> dict:
        resource_type = request.query_params.get('resource_type', 'compute')
        return await self._forward(
            self._auto_sid, IRIInstanceSession.list_resources, resource_type)

    async def get_resource(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        return await self._forward(
            self._auto_sid, IRIInstanceSession.get_resource, resource_id)

    async def submit_job(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        body        = await request.json()
        job_spec    = body.get('job_spec', body)
        return await self._forward(
            self._auto_sid, IRIInstanceSession.submit_job, resource_id, job_spec)

    async def get_job_status(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        job_id      = request.path_params['job_id']
        return await self._forward(
            self._auto_sid, IRIInstanceSession.get_job_status, resource_id, job_id)

    async def list_jobs(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        return await self._forward(
            self._auto_sid, IRIInstanceSession.list_jobs, resource_id)

    async def cancel_job(self, request: Request) -> dict:
        resource_id = request.path_params['resource_id']
        job_id      = request.path_params['job_id']
        return await self._forward(
            self._auto_sid, IRIInstanceSession.cancel_job, resource_id, job_id)

    async def list_incidents(self, request: Request) -> dict:
        return await self._forward(
            self._auto_sid, IRIInstanceSession.list_incidents)

    async def list_projects(self, request: Request) -> dict:
        return await self._forward(
            self._auto_sid, IRIInstanceSession.list_projects)

    async def list_allocations(self, request: Request) -> dict:
        project_id = request.path_params['project_id']
        return await self._forward(
            self._auto_sid, IRIInstanceSession.list_allocations, project_id)
