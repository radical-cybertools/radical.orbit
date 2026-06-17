'''
Globus plugin — file staging via Globus Online (Transfer API).

Edge-side plugin (not loaded on the bridge).  Globus moves data
*collection-to-collection* out of band, so this plugin is an orchestrator:
it submits transfers between two Globus collections (identified by UUID) and
monitors task state.  Bytes never flow through the edge or the bridge.

Auth lifecycle
--------------
A Globus Transfer token is supplied at ``register_session`` time, as either:

* ``access_token``                        — wrapped in an ``AccessTokenAuthorizer``;
                                            expires (~48h) and is not renewed —
                                            the client re-registers with a fresh
                                            token when it lapses; or
* ``refresh_token`` + ``client_id``       — wrapped in a ``RefreshTokenAuthorizer``
                                            which transparently renews access
                                            tokens, so long-running transfers
                                            survive expiry.

The credential lives in the edge process memory (inside the ``TransferClient``)
for the lifetime of the session and is **never** written to disk.

Collections
-----------
Collection UUIDs are passed explicitly on the wire.  The literal string
``"local"`` resolves to the edge's configured local collection
(``RADICAL_EDGE_GLOBUS_COLLECTION`` env var, or a ``local_collection`` override
supplied at ``register_session``).

Implementation note
-------------------
``globus-sdk`` is synchronous; every Transfer call is offloaded with
``asyncio.to_thread`` so the edge event loop stays responsive.
'''

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'

import asyncio
import json
import logging
import os
import time
import uuid

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request

from .plugin_base         import Plugin
from .plugin_session_base import PluginSession
from .client              import PluginClient

try:
    import globus_sdk
except ImportError:
    globus_sdk = None

log = logging.getLogger('radical.edge')

# Background poll interval for active transfer tasks (seconds).
GLOBUS_POLL_INTERVAL = 10.0

# Terminal Globus task statuses (no further polling needed).
GLOBUS_TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELED', 'CANCELLED'}

# Local-collection config file (lowest-priority auto-detection source).
GLOBUS_CONFIG_FILE = '~/.radical/edge/globus.json'


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def detect_local_collection() -> Optional[str]:
    '''Best-effort discovery of the edge's local Globus collection UUID.

    Precedence: ``RADICAL_EDGE_GLOBUS_COLLECTION`` env var →
    Globus Connect Personal (``LocalGlobusConnectPersonal``) →
    config file (``~/.radical/edge/globus.json`` with a ``local_collection``
    key) → ``None``.  Facility GCS/DTN collections are not locally
    discoverable; for those, set the env var or the config file.
    '''
    env = os.environ.get('RADICAL_EDGE_GLOBUS_COLLECTION')
    if env and env.strip():
        log.debug('[globus] local collection from env: %s', env.strip())
        return env.strip()

    if globus_sdk is not None:
        try:
            ep = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
            if ep:
                log.debug('[globus] local collection from GCP: %s', ep)
                return ep
        except Exception as exc:
            log.debug('[globus] GCP detection failed: %s', exc)

    try:
        path = os.path.expanduser(GLOBUS_CONFIG_FILE)
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            lc = (data.get('local_collection') or '').strip()
            if lc:
                log.debug('[globus] local collection from %s: %s', path, lc)
                return lc
    except Exception as exc:
        log.debug('[globus] config-file detection failed: %s', exc)

    return None


def _as_dict(res: Any) -> dict:
    '''Normalize a globus-sdk response (or a plain dict, in tests) to a dict.'''
    return res.data if hasattr(res, 'data') else res


def _globus_http_exc(exc: Exception, context: str = '') -> HTTPException:
    '''Map a globus-sdk API error to an HTTPException.

    ``ConsentRequired`` (the ``data_access`` consent needed for mapped
    collections) is surfaced as a clear 401 rather than an opaque 502, so the
    caller knows to re-acquire a token carrying the collection's data_access
    scope.
    '''
    prefix = f'Globus {context}: ' if context else 'Globus: '

    consent = getattr(getattr(exc, 'info', None), 'consent_required', None)
    if getattr(exc, 'code', '') == 'ConsentRequired' or consent:
        scopes = getattr(consent, 'required_scopes', None)
        return HTTPException(
            status_code=401,
            detail=f'{prefix}data access consent required — re-acquire a token '
                   f'carrying the collection data_access scope '
                   f'(required_scopes={scopes})')

    sc  = getattr(exc, 'http_status', 502) or 502
    msg = getattr(exc, 'message', None) or str(exc)

    if   sc == 401: return HTTPException(status_code=401, detail=f'{prefix}token expired or invalid')
    elif sc == 403: return HTTPException(status_code=403, detail=f'{prefix}forbidden')
    elif sc == 404: return HTTPException(status_code=404, detail=f'{prefix}not found')
    elif sc == 429: return HTTPException(status_code=429, detail=f'{prefix}rate limited by Globus')
    else:           return HTTPException(status_code=502, detail=f'{prefix}{msg}')


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class GlobusSession(PluginSession):
    '''Per-client Globus Transfer session.

    Holds one ``TransferClient`` bound to the supplied credential, tracks
    submitted tasks, and emits ``transfer_status`` notifications on state
    changes via a background poller.
    '''

    def __init__(self, sid: str,
                 access_token: Optional[str]     = None,
                 refresh_token: Optional[str]    = None,
                 client_id: Optional[str]        = None,
                 local_collection: Optional[str] = None):
        super().__init__(sid)

        if globus_sdk is None:
            raise RuntimeError('globus-sdk is not installed')

        self._local_collection = local_collection or None

        if access_token:
            authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
        elif refresh_token and client_id:
            authorizer = globus_sdk.RefreshTokenAuthorizer(
                refresh_token, globus_sdk.NativeAppAuthClient(client_id))
        else:
            raise ValueError(
                "provide either 'access_token' or 'refresh_token'+'client_id'")

        self._tc = globus_sdk.TransferClient(authorizer=authorizer)

        # task_id -> {status, label}
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._poll_task: Optional[asyncio.Task] = None

    # -- helpers ------------------------------------------------------------

    def _resolve(self, collection: Optional[str]) -> str:
        '''Resolve ``"local"`` (or empty) to the configured local collection.'''
        if collection in (None, '', 'local'):
            if not self._local_collection:
                raise HTTPException(
                    status_code=400,
                    detail='no local collection configured; pass an explicit '
                           'collection UUID')
            return self._local_collection
        return collection

    async def _call(self, fn, *args, context: str = '', **kwargs) -> Any:
        '''Offload a synchronous globus-sdk call and map its errors.'''
        self._check_active()
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except HTTPException:
            raise
        except Exception as exc:
            if globus_sdk is not None \
                    and isinstance(exc, globus_sdk.GlobusAPIError):
                raise _globus_http_exc(exc, context) from exc
            raise HTTPException(status_code=502,
                                detail=f'Globus {context}: {exc}') from exc

    # -- transfer + monitoring ----------------------------------------------

    async def submit_transfer(self, source: str, destination: str,
                              items: List[Dict[str, Any]],
                              label: Optional[str]      = None,
                              sync_level: Optional[Any] = None) -> dict:
        self._check_active()
        if not items:
            raise HTTPException(status_code=400, detail='no transfer items provided')

        src = self._resolve(source)
        dst = self._resolve(destination)

        td_kwargs: Dict[str, Any] = {'source_endpoint'     : src,
                                     'destination_endpoint': dst}
        if label      is not None: td_kwargs['label']      = label
        if sync_level is not None: td_kwargs['sync_level'] = sync_level

        tdata = globus_sdk.TransferData(self._tc, **td_kwargs)
        for it in items:
            s = it.get('source')
            d = it.get('destination')
            if not s or not d:
                raise HTTPException(
                    status_code=400,
                    detail="each item needs 'source' and 'destination'")
            tdata.add_item(s, d, recursive=bool(it.get('recursive', False)))

        res     = _as_dict(await self._call(
            self._tc.submit_transfer, tdata, context='submit_transfer'))
        task_id = res.get('task_id')

        self._tasks[task_id] = {'status': 'ACTIVE', 'label': label or ''}
        self._start_polling()

        log.info('[globus] session %s: submitted transfer %s (%s -> %s)',
                 self._sid, task_id, src, dst)
        return {'task_id'      : task_id,
                'submission_id': res.get('submission_id'),
                'status'       : 'ACTIVE'}

    async def get_task(self, task_id: str) -> dict:
        res = _as_dict(await self._call(
            self._tc.get_task, task_id, context='get_task'))
        if task_id in self._tasks and res.get('status'):
            self._tasks[task_id]['status'] = res['status']
        return res

    async def task_wait(self, task_id: str, timeout: int = 60,
                        polling_interval: int = 10) -> dict:
        done = await self._call(self._tc.task_wait, task_id, context='task_wait',
                                timeout=timeout, polling_interval=polling_interval)
        return {'task_id': task_id, 'completed': bool(done)}

    async def cancel_task(self, task_id: str) -> dict:
        res = _as_dict(await self._call(
            self._tc.cancel_task, task_id, context='cancel_task'))
        self._tasks.pop(task_id, None)
        log.info('[globus] session %s: cancelled task %s', self._sid, task_id)
        return res

    async def list_tasks(self, limit: int = 100) -> dict:
        res = _as_dict(await self._call(
            self._tc.task_list, context='task_list', limit=limit))
        return {'tasks': res.get('DATA', res) if isinstance(res, dict) else res}

    # -- filesystem ops -----------------------------------------------------

    async def operation_ls(self, collection: str,
                          path: Optional[str] = None) -> dict:
        coll = self._resolve(collection)
        res  = _as_dict(await self._call(
            self._tc.operation_ls, coll, context='operation_ls', path=path))
        return {'collection': coll,
                'path'      : res.get('path', path),
                'entries'   : res.get('DATA', [])}

    async def operation_mkdir(self, collection: str, path: str) -> dict:
        coll = self._resolve(collection)
        return _as_dict(await self._call(
            self._tc.operation_mkdir, coll, path, context='operation_mkdir'))

    async def operation_rename(self, collection: str,
                              oldpath: str, newpath: str) -> dict:
        coll = self._resolve(collection)
        return _as_dict(await self._call(
            self._tc.operation_rename, coll, oldpath, newpath,
            context='operation_rename'))

    async def submit_delete(self, collection: str, paths: List[str],
                           recursive: bool          = False,
                           label: Optional[str]     = None) -> dict:
        self._check_active()
        if not paths:
            raise HTTPException(status_code=400, detail='no delete paths provided')

        coll       = self._resolve(collection)
        dd_kwargs: Dict[str, Any] = {'endpoint' : coll,
                                     'recursive': bool(recursive)}
        if label is not None: dd_kwargs['label'] = label

        ddata = globus_sdk.DeleteData(self._tc, **dd_kwargs)
        for p in paths:
            ddata.add_item(p)

        res     = _as_dict(await self._call(
            self._tc.submit_delete, ddata, context='submit_delete'))
        task_id = res.get('task_id')
        if task_id:
            self._tasks[task_id] = {'status': 'ACTIVE', 'label': label or ''}
            self._start_polling()
        return {'task_id'      : task_id,
                'submission_id': res.get('submission_id'),
                'status'       : 'ACTIVE'}

    # -- discovery ----------------------------------------------------------

    async def endpoint_search(self, filter_text: Optional[str] = None,
                             limit: int = 25) -> dict:
        res = _as_dict(await self._call(
            self._tc.endpoint_search, context='endpoint_search',
            filter_fulltext=filter_text, limit=limit))
        return {'endpoints': res.get('DATA', []) if isinstance(res, dict) else res}

    async def get_endpoint(self, endpoint_id: str) -> dict:
        coll = self._resolve(endpoint_id)
        return _as_dict(await self._call(
            self._tc.get_endpoint, coll, context='get_endpoint'))

    # -- lifecycle ----------------------------------------------------------

    async def close(self) -> dict:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        return await super().close()

    # -- background polling -------------------------------------------------

    def _start_polling(self) -> None:
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_tasks())

    async def _poll_tasks(self) -> None:
        while True:
            try:
                await asyncio.sleep(GLOBUS_POLL_INTERVAL)
                if not self._active:
                    break

                active = {tid: m for tid, m in list(self._tasks.items())
                          if m.get('status') not in GLOBUS_TERMINAL}
                if not active:
                    break

                for task_id, meta in active.items():
                    try:
                        res    = _as_dict(await asyncio.to_thread(
                            self._tc.get_task, task_id))
                        status = res.get('status')
                        if status and status != meta.get('status'):
                            old           = meta['status']
                            meta['status'] = status
                            log.debug('[globus] task %s: %s -> %s',
                                      task_id, old, status)
                            if self._plugin:
                                self._plugin._dispatch_notify('transfer_status', {
                                    'task_id'          : task_id,
                                    'status'           : status,
                                    'label'            : meta.get('label', ''),
                                    'bytes_transferred': res.get('bytes_transferred'),
                                    'files_transferred': res.get('files_transferred'),
                                    'nice_status'      : res.get('nice_status'),
                                })
                    except Exception as exc:
                        log.debug('[globus] poll error for task %s: %s',
                                  task_id, exc)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.debug('[globus] _poll_tasks error: %s', exc)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class GlobusClient(PluginClient):
    '''Client-side interface for the Globus plugin.'''

    def register_session(self,
                         access_token: Optional[str]     = None,
                         refresh_token: Optional[str]    = None,
                         client_id: Optional[str]        = None,
                         local_collection: Optional[str] = None) -> None:
        '''Register a Globus session.

        Supply either ``access_token`` or ``refresh_token`` + ``client_id``.
        ``local_collection`` overrides the edge's configured default.
        '''
        payload: Dict[str, Any] = {}
        if access_token:     payload['access_token']     = access_token
        if refresh_token:    payload['refresh_token']    = refresh_token
        if client_id:        payload['client_id']        = client_id
        if local_collection: payload['local_collection'] = local_collection

        resp = self._http.post(self._url('register_session'), json=payload)
        self._raise(resp)
        self._sid = resp.json()['sid']

    def submit_transfer(self, source: str, destination: str,
                       items: List[Dict[str, Any]],
                       label: Optional[str]      = None,
                       sync_level: Optional[Any] = None) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'submit/{self._sid}'), json={
            'source'     : source,
            'destination': destination,
            'items'      : items,
            'label'      : label,
            'sync_level' : sync_level,
        })
        self._raise(resp, 'submit_transfer')
        return resp.json()

    def get_task(self, task_id: str) -> dict:
        self._require_session()
        resp = self._http.get(self._url(f'task/{self._sid}/{task_id}'))
        self._raise(resp, f'get_task {task_id!r}')
        return resp.json()

    def task_wait(self, task_id: str, timeout: int = 60,
                 polling_interval: int = 10) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'wait/{self._sid}/{task_id}'), json={
            'timeout': timeout, 'polling_interval': polling_interval})
        self._raise(resp, f'task_wait {task_id!r}')
        return resp.json()

    def cancel_task(self, task_id: str) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'cancel/{self._sid}/{task_id}'))
        self._raise(resp, f'cancel_task {task_id!r}')
        return resp.json()

    def list_tasks(self, limit: int = 100) -> dict:
        self._require_session()
        resp = self._http.get(self._url(f'tasks/{self._sid}'),
                              params={'limit': limit})
        self._raise(resp, 'list_tasks')
        return resp.json()

    def ls(self, collection: str, path: Optional[str] = None) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'ls/{self._sid}'),
                              json={'collection': collection, 'path': path})
        self._raise(resp, 'ls')
        return resp.json()

    def mkdir(self, collection: str, path: str) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'mkdir/{self._sid}'),
                              json={'collection': collection, 'path': path})
        self._raise(resp, 'mkdir')
        return resp.json()

    def rename(self, collection: str, oldpath: str, newpath: str) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'rename/{self._sid}'), json={
            'collection': collection, 'oldpath': oldpath, 'newpath': newpath})
        self._raise(resp, 'rename')
        return resp.json()

    def delete(self, collection: str, paths: List[str],
              recursive: bool = False, label: Optional[str] = None) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'delete/{self._sid}'), json={
            'collection': collection, 'paths': paths,
            'recursive' : recursive,  'label': label})
        self._raise(resp, 'delete')
        return resp.json()

    def endpoint_search(self, filter_text: Optional[str] = None,
                       limit: int = 25) -> dict:
        self._require_session()
        resp = self._http.post(self._url(f'endpoint_search/{self._sid}'),
                              json={'filter_text': filter_text, 'limit': limit})
        self._raise(resp, 'endpoint_search')
        return resp.json()

    def get_endpoint(self, endpoint_id: str) -> dict:
        self._require_session()
        resp = self._http.get(self._url(f'endpoint/{self._sid}/{endpoint_id}'))
        self._raise(resp, f'get_endpoint {endpoint_id!r}')
        return resp.json()


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginGlobus(Plugin):
    '''Globus Online file-staging plugin (edge-side).'''

    plugin_name   = 'globus'
    session_class = GlobusSession
    client_class  = GlobusClient
    version       = '0.0.1'
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'globus.js')

    ui_config = {
        'icon'       : '🌐',
        'title'      : 'Globus Transfer',
        'description': 'Stage files between Globus collections.',
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Edge-only, and only when globus-sdk is importable.'''
        return bool(globus_sdk) and not getattr(app.state, 'is_bridge', False)

    def __init__(self, app: FastAPI, instance_name: str = 'globus'):
        super().__init__(app, instance_name)

        # Edge's local collection UUID (optional), overridable per session.
        # Auto-detected via env var → GCP → config file.
        self._default_collection = detect_local_collection()
        if self._default_collection:
            log.info('[globus] local collection: %s', self._default_collection)

        self.add_route_post('submit/{sid}',                self.submit_transfer)
        self.add_route_get ('task/{sid}/{task_id}',        self.get_task)
        self.add_route_post('wait/{sid}/{task_id}',        self.task_wait)
        self.add_route_post('cancel/{sid}/{task_id}',      self.cancel_task)
        self.add_route_get ('tasks/{sid}',                 self.list_tasks)
        self.add_route_post('ls/{sid}',                    self.operation_ls)
        self.add_route_post('mkdir/{sid}',                 self.operation_mkdir)
        self.add_route_post('rename/{sid}',                self.operation_rename)
        self.add_route_post('delete/{sid}',                self.submit_delete)
        self.add_route_post('endpoint_search/{sid}',       self.endpoint_search)
        self.add_route_get ('endpoint/{sid}/{endpoint_id}', self.get_endpoint)

    # -- session registration (carries the auth payload) --------------------

    async def register_session(self, request: Request) -> dict:
        '''Register a session with a Globus credential.

        Body: ``{"access_token": ...}`` or
        ``{"refresh_token": ..., "client_id": ...}``, plus optional
        ``local_collection``.
        '''
        try:
            data = await request.json()
        except Exception:
            data = {}

        access_token  = data.get('access_token')
        refresh_token = data.get('refresh_token')
        client_id     = data.get('client_id')
        local_coll    = data.get('local_collection') or self._default_collection

        if not access_token and not (refresh_token and client_id):
            raise HTTPException(
                status_code=400,
                detail="provide either 'access_token' or "
                       "'refresh_token'+'client_id'")

        self._ensure_cleanup_task()
        sid = f'session.{uuid.uuid4().hex[:8]}'
        try:
            session = self._create_session(
                sid, access_token=access_token, refresh_token=refresh_token,
                client_id=client_id, local_collection=local_coll)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        self._sessions[sid]            = session
        self._session_last_access[sid] = time.time()
        log.info('[globus] Registered session %s', sid)
        return {'sid': sid}

    # -- route handlers -----------------------------------------------------

    async def submit_transfer(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        return await self._forward(
            sid, GlobusSession.submit_transfer,
            body.get('source'), body.get('destination'),
            body.get('items', []), body.get('label'), body.get('sync_level'))

    async def get_task(self, request: Request) -> dict:
        sid     = request.path_params['sid']
        task_id = request.path_params['task_id']
        return await self._forward(sid, GlobusSession.get_task, task_id)

    async def task_wait(self, request: Request) -> dict:
        sid     = request.path_params['sid']
        task_id = request.path_params['task_id']
        try:
            body = await request.json()
        except Exception:
            body = {}
        return await self._forward(
            sid, GlobusSession.task_wait, task_id,
            int(body.get('timeout', 60)), int(body.get('polling_interval', 10)))

    async def cancel_task(self, request: Request) -> dict:
        sid     = request.path_params['sid']
        task_id = request.path_params['task_id']
        return await self._forward(sid, GlobusSession.cancel_task, task_id)

    async def list_tasks(self, request: Request) -> dict:
        sid   = request.path_params['sid']
        limit = int(request.query_params.get('limit', 100))
        return await self._forward(sid, GlobusSession.list_tasks, limit)

    async def operation_ls(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        return await self._forward(
            sid, GlobusSession.operation_ls,
            body.get('collection'), body.get('path'))

    async def operation_mkdir(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        path = body.get('path')
        if not path:
            raise HTTPException(status_code=400, detail="missing 'path'")
        return await self._forward(
            sid, GlobusSession.operation_mkdir, body.get('collection'), path)

    async def operation_rename(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        old  = body.get('oldpath')
        new  = body.get('newpath')
        if not old or not new:
            raise HTTPException(status_code=400,
                                detail="missing 'oldpath' or 'newpath'")
        return await self._forward(
            sid, GlobusSession.operation_rename,
            body.get('collection'), old, new)

    async def submit_delete(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        return await self._forward(
            sid, GlobusSession.submit_delete,
            body.get('collection'), body.get('paths', []),
            bool(body.get('recursive', False)), body.get('label'))

    async def endpoint_search(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        body = await request.json()
        return await self._forward(
            sid, GlobusSession.endpoint_search,
            body.get('filter_text'), int(body.get('limit', 25)))

    async def get_endpoint(self, request: Request) -> dict:
        sid         = request.path_params['sid']
        endpoint_id = request.path_params['endpoint_id']
        return await self._forward(
            sid, GlobusSession.get_endpoint, endpoint_id)
