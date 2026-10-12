'''
Globus plugin — file staging via Globus Online (Transfer API).

Endpoint-side plugin (not loaded on the broker).  Globus moves data
*collection-to-collection* out of band, so this plugin is an orchestrator:
it submits transfers between two Globus collections (identified by UUID) and
monitors task state.  Bytes never flow through the endpoint or the broker.

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

The credential lives in the endpoint process memory (inside the ``TransferClient``)
for the lifetime of the session and is **never** written to disk.

Collections
-----------
Collection UUIDs are passed explicitly on the wire.  The literal string
``"local"`` resolves to the endpoint's configured local collection
(``RADICAL_ORBIT_GLOBUS_COLLECTION`` env var, or a ``local_collection`` override
supplied at ``register_session``).

Implementation note
-------------------
``globus-sdk`` is synchronous; every Transfer call is offloaded with
``asyncio.to_thread`` so the endpoint event loop stays responsive.
'''

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'

import asyncio
import concurrent.futures
import json
import logging
import math
import os
import time
import uuid

from typing import Any, Dict, List, Optional

import httpx

from fastapi import FastAPI, HTTPException, Request

from .plugin_base         import Plugin
from .plugin_session_base import PluginSession
from .client              import PluginClient

try:
    import globus_sdk
except ImportError:
    globus_sdk = None

log = logging.getLogger('radical.orbit')

# Background poll interval for active transfer tasks (seconds).
GLOBUS_POLL_INTERVAL = 10.0

# Terminal Globus task statuses (no further polling needed).
GLOBUS_TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELED', 'CANCELLED'}

# Statuses that end ``task_wait``: the task has left ACTIVE.  INACTIVE
# (e.g. an expired credential) is not terminal but needs intervention.
GLOBUS_WAIT_DONE = GLOBUS_TERMINAL | {'INACTIVE'}

# Local-collection config file (lowest-priority auto-detection source).
GLOBUS_CONFIG_FILE = '~/.radical/orbit/globus.json'

# Upper bound for one call of the server-side ``wait`` route (seconds), kept
# for direct HTTP callers.  The broker reaps a routed call after
# ``BrokerTuning.call_timeout`` (30 s by default) with HTTP 504 while the
# transfer keeps running.  ``GlobusClient.task_wait`` does not use this
# route: it paces short ``get_task`` calls itself.
GLOBUS_WAIT_SLICE = 10

# Poll failures ``GlobusClient.task_wait`` retries until its deadline:
# 504 (call reaped or handler timed out), 503 (broker / runtime overload
# fast-fail, or a transient Globus network error / 5xx on a status query)
# and 429 (Globus rate limit), plus transport-level losses.  Not 502: the broker sends it for an
# unknown endpoint, and this plugin maps other unmapped Globus errors to it.
_WAIT_RETRY_STATUS = {429, 503, 504}
_WAIT_RETRY_EXC    = (TimeoutError, ConnectionError, httpx.TransportError,
                      # distinct from TimeoutError before Python 3.11
                      asyncio.TimeoutError, concurrent.futures.TimeoutError)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _retry_after(resp) -> Optional[float]:
    """Seconds from a response's ``Retry-After`` header (at least 1), or
    ``None`` when it is absent or not a positive finite number."""
    try:
        val = float(resp.headers.get('retry-after'))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val) or val <= 0:
        return None
    return max(1.0, val)


def detect_local_collection() -> Optional[str]:
    '''Best-effort discovery of the endpoint's local Globus collection UUID.

    Precedence: ``RADICAL_ORBIT_GLOBUS_COLLECTION`` env var →
    Globus Connect Personal (``LocalGlobusConnectPersonal``) →
    config file (``~/.radical/orbit/globus.json`` with a ``local_collection``
    key) → ``None``.  Facility GCS/DTN collections are not locally
    discoverable; for those, set the env var or the config file.
    '''
    env = os.environ.get('RADICAL_ORBIT_GLOBUS_COLLECTION')
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


def _globus_http_exc(exc: Exception, context: str = '',
                     read_only: bool = False) -> HTTPException:
    '''Map a globus-sdk API error to an HTTPException.

    ``ConsentRequired`` (the ``data_access`` consent needed for mapped
    collections) is surfaced as a clear 401 rather than an opaque 502, so the
    caller knows to re-acquire a token carrying the collection's data_access
    scope.

    A Globus-side 5xx becomes 503 (retryable) only for *read_only* calls; a
    mutating call keeps 502, since a retry could duplicate its effect.
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

    sc  = getattr(exc, 'http_status', None) or 0
    msg = getattr(exc, 'message', None) or str(exc)

    # pass Globus's Retry-After on, so a retrying client honours it
    ra    = (getattr(exc, 'headers', None) or {}).get('Retry-After')
    retry = {'Retry-After': str(ra)} if ra else None

    if   sc == 401: return HTTPException(status_code=401, detail=f'{prefix}token expired or invalid')
    elif sc == 403: return HTTPException(status_code=403, detail=f'{prefix}forbidden')
    elif sc == 404: return HTTPException(status_code=404, detail=f'{prefix}not found')
    elif sc == 429: return HTTPException(status_code=429, detail=f'{prefix}rate limited by Globus',
                                         headers=retry)
    elif sc >= 500 and read_only:
        return HTTPException(status_code=503, detail=f'{prefix}{msg}',
                             headers=retry)
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

        # Bound every HTTP call so a hung Globus endpoint cannot block the
        # offload thread indefinitely (mirrors the IRI client's timeout=30).
        self._tc = globus_sdk.TransferClient(
            authorizer=authorizer,
            transport_params={"http_timeout": 60})

        # task_id -> {status, label}
        self._tasks: Dict[str, Dict[str, Any]] = {}

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

    async def _call(self, fn, *args, context: str = '',
                    read_only: bool = False, **kwargs) -> Any:
        '''Offload a synchronous globus-sdk call and map its errors.

        For *read_only* calls (status queries), transient Globus failures
        (network errors, Globus 5xx) map to a retryable 503; for anything
        else they stay 502, since a retry could duplicate the effect.
        '''
        self._check_active()
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except HTTPException:
            raise
        except Exception as exc:
            if globus_sdk is not None \
                    and isinstance(exc, globus_sdk.GlobusAPIError):
                raise _globus_http_exc(exc, context, read_only) from exc
            if read_only and globus_sdk is not None \
                    and isinstance(exc, globus_sdk.NetworkError):
                # connection error / timeout towards Globus: transient
                raise HTTPException(status_code=503,
                                    detail=f'Globus {context}: {exc}') from exc
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
        if not task_id:
            raise HTTPException(
                status_code=502,
                detail='Globus submit_transfer: missing task_id in response')

        self._tasks[task_id] = {'status': 'ACTIVE', 'label': label or ''}
        self._start_polling()

        log.info('[globus] session %s: submitted transfer %s (%s -> %s)',
                 self._sid, task_id, src, dst)
        return {'task_id'      : task_id,
                'submission_id': res.get('submission_id'),
                'status'       : 'ACTIVE'}

    async def get_task(self, task_id: str) -> dict:
        res = _as_dict(await self._call(
            self._tc.get_task, task_id, context='get_task', read_only=True))
        if task_id in self._tasks:
            self._cache_result(self._tasks[task_id], res)
        return res

    async def task_wait(self, task_id: str, timeout: int = 60,
                        polling_interval: int = 10) -> dict:
        '''Wait for the task to leave ``ACTIVE``, at most
        ``GLOBUS_WAIT_SLICE`` seconds.

        Returns ``{task_id, completed}`` with globus-sdk semantics:
        ``completed`` is true for *any* non-ACTIVE status, including
        ``INACTIVE`` (e.g. an expired credential) and statuses unknown to
        this plugin.  ``GlobusClient.task_wait`` does not use this route and
        completes only on ``GLOBUS_WAIT_DONE``.
        '''
        # Bound the blocking wait below the routed-call timeout (see
        # GLOBUS_WAIT_SLICE); globus-sdk requires values >= 1.
        timeout          = max(1, min(timeout, GLOBUS_WAIT_SLICE))
        polling_interval = max(1, min(polling_interval, timeout))
        done = await self._call(self._tc.task_wait, task_id, context='task_wait',
                                read_only=True, timeout=timeout,
                                polling_interval=polling_interval)
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

    # -- background polling -------------------------------------------------
    #
    # Lifecycle (task cancellation on close) is handled by the base
    # ``PluginSession.close()`` / ``stop_status_poller()`` — no override
    # needed here.

    def _start_polling(self) -> None:
        '''(Re)start the shared status poller for the session's active tasks.'''
        self.start_status_poller(
            interval=GLOBUS_POLL_INTERVAL,
            items=lambda: self._tasks,
            is_terminal=lambda meta: meta.get('notified') in GLOBUS_TERMINAL,
            fetch=self._fetch_task_status,
            to_payload=self._task_payload,
            topic='transfer_status',
            name='globus')

    async def _fetch_task_status(self, task_id: str,
                                 meta: Dict[str, Any]) -> Optional[dict]:
        '''Poll one task; return the raw Globus result when its status changed.

        Compares against the last *notified* status, not ``meta['status']``:
        ``get_task`` (e.g. a client's ``task_wait`` polls) writes the latter,
        and must not swallow the change notification.  A terminal status
        already cached by ``get_task`` is notified without fetching, so a
        failing fetch (e.g. an expired token) cannot keep the task polled.
        '''
        if not self._terminal_pending(meta):
            res    = _as_dict(await asyncio.to_thread(self._tc.get_task,
                                                      task_id))
            status = res.get('status')
            if not status:
                return None
            # keep the cache current (e.g. INACTIVE back to ACTIVE)
            self._cache_result(meta, res)
            if meta['status'] == status:
                if status == meta.get('notified', 'ACTIVE'):
                    return None
                meta['notified'] = status
                if status in GLOBUS_TERMINAL:
                    meta.pop('result', None)    # cached just above
                return res
            # else a concurrent get_task cached the end while we fetched
            if not self._terminal_pending(meta):
                return None

        # notify a cached terminal status (from get_task) without a fetch
        meta['notified'] = meta['status']
        return meta.pop('result', None) \
            or {'task_id': task_id, 'status': meta['status']}

    @staticmethod
    def _terminal_pending(meta: Dict[str, Any]) -> bool:
        '''A terminal status is cached but not yet notified.'''
        return meta.get('status') in GLOBUS_TERMINAL \
            and meta.get('status') != meta.get('notified')

    @staticmethod
    def _cache_result(meta: Dict[str, Any], res: dict) -> None:
        '''Cache a ``get_task`` result in the task meta for the poller.

        Never downgrades a cached terminal status (a stale ACTIVE fetch
        racing a ``get_task`` that saw the end).  The full result is kept
        only for a terminal status not yet notified, for the notification.
        '''
        status = res.get('status')
        if not status:
            return
        if meta.get('status') in GLOBUS_TERMINAL \
                and status not in GLOBUS_TERMINAL:
            return
        meta['status'] = status
        if status in GLOBUS_TERMINAL \
                and meta.get('notified') not in GLOBUS_TERMINAL:
            meta['result'] = res

    @staticmethod
    def _task_payload(task_id: str, meta: Dict[str, Any], res: dict) -> dict:
        '''Build the ``transfer_status`` notification payload for one task.'''
        return {
            'task_id'          : task_id,
            'status'           : meta['status'],
            'label'            : meta.get('label', ''),
            'bytes_transferred': res.get('bytes_transferred'),
            'files_transferred': res.get('files_transferred'),
            'nice_status'      : res.get('nice_status'),
        }


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
        ``local_collection`` overrides the endpoint's configured default.
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
        '''Wait up to *timeout* seconds for the task to leave ``ACTIVE``.

        Polls ``get_task`` on a fixed schedule, every *polling_interval*
        (at least 1 s) from the start, so no routed call blocks near the
        broker's call timeout and Globus sees one status call per interval;
        a slow poll does not shift the schedule.  Polls at least once
        (``timeout=0`` is a status probe) and once more at the deadline, but
        never starts a poll past it.

        A failed poll is retried within *timeout* when it is transient: HTTP
        504, 503 or 429 (at the next scheduled poll, or after a longer
        ``Retry-After``), or a transport-level loss (timeout, connection
        error).  If no poll got through before the deadline, the last such
        error is raised; other errors raise at once.

        Returns ``{task_id, completed}``.  ``completed`` is true once the
        task reports a status in ``GLOBUS_WAIT_DONE``: SUCCEEDED, FAILED,
        CANCELED, or INACTIVE (e.g. an expired credential) — call
        ``get_task`` for the status itself.  A missing or unknown status
        does not count (unlike the endpoint's ``wait`` route, which follows
        globus-sdk: any non-ACTIVE status).
        '''
        self._require_session()
        interval = max(1, polling_interval)
        start    = time.monotonic()
        deadline = start + timeout
        slot     = 0                    # index of the current scheduled poll
        last     = False                # this poll is the one at the deadline
        answered = False                # some poll in this wait succeeded
        while True:
            error = None                # the last poll's transient failure
            retry = None                # server-requested pause, if any
            try:
                resp = self._http.get(self._url(f'task/{self._sid}/{task_id}'))
            except _WAIT_RETRY_EXC as exc:
                error = exc
            else:
                if resp.status_code in _WAIT_RETRY_STATUS:
                    error = resp
                    retry = _retry_after(resp)
                else:
                    self._raise(resp, f'task_wait {task_id!r}')
                    answered = True
                    if resp.json().get('status') in GLOBUS_WAIT_DONE:
                        return {'task_id': task_id, 'completed': True}

            now = time.monotonic()
            if last or now >= deadline:
                # a lost last poll is only an error if no poll got through
                if error is not None and not answered:
                    if isinstance(error, Exception):
                        raise error
                    self._raise(error, f'task_wait {task_id!r}')
                return {'task_id': task_id, 'completed': False}

            if error is not None:
                log.debug('[globus] task_wait %s: poll failed (%s), retrying',
                          task_id, getattr(error, 'status_code', error))
            # next slot of the fixed schedule (a slot index, so an early
            # wake-up cannot repeat a slot), or later if the server asked
            # for a longer pause; the deadline gets a last poll
            slot = max(slot + 1, int((now - start) // interval) + 1)
            due  = start + slot * interval
            if retry is not None:
                due = max(due, now + retry)
            if due >= deadline:
                due, last = deadline, True
            time.sleep(due - now)

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
    '''Globus Online file-staging plugin (endpoint-side).'''

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
        '''Endpoint-only, and only when globus-sdk is importable.'''
        return bool(globus_sdk) and not getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI, instance_name: str = 'globus'):
        super().__init__(app, instance_name)

        # Endpoint's local collection UUID (optional), overridable per session.
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
        ``local_collection`` and the usual session-policy fields
        (``sid``/``lifetime``/``ttl`` — see ``Plugin.register_session``).

        Unlike the base ``register_session``, this override needs the
        credential fields out of the body *before* the session object can be
        constructed, so it drives session creation itself — but it still
        records the result through the base's ``_record_session`` /
        ``_normalize_session_policy`` so a globus session gets the same
        lifetime/owner bookkeeping every other plugin's sessions get.
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
        owner              = self._request_owner(request)
        sid, lifetime, ttl = self._normalize_session_policy(data)
        if sid is None:
            sid = f'session.{uuid.uuid4().hex[:8]}'

        try:
            session = self._create_session(
                sid, access_token=access_token, refresh_token=refresh_token,
                client_id=client_id, local_collection=local_coll)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        self._record_session(sid, session, lifetime, ttl, owner)
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
