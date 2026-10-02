
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'

'''
LUCID portal plugin: a mock-up science portal for Cell Painting analysis.

The plugin runs on a *local* endpoint (e.g. a laptop) registered on the
broker, and is itself a client: on ``submit`` it drives the flow in
:mod:`radical.orbit.lucid_flow` against a remote HPC endpoint's ``globus``,
``rhapsody`` and ``staging`` plugins -- stage-in from the LUCID Globus
collection, one CellProfiler task per selected well, stage-out to
``/tmp_radical/<run-id>/`` on the collection, and preview images fetched back
for inline display.  The Explorer page (``data/plugins/lucid.js``) provides
the form, a plate grid for well selection, a progress bar and the results.

The remote endpoints are reached through the runtime this plugin is served
by.  ``$RADICAL_ORBIT_LUCID_BROKER`` points the flow at a different broker
instead (e.g. when the portal UI is served by a local broker).

One run at a time; run state is kept in memory, so a reloaded page
re-attaches to the running (or last) run.  The Globus Transfer refresh token
is read from ``~/.globus/auth_tokens.json`` on this host at submit time; it is
never sent to the browser, and is handed to the target endpoint's ``globus``
session (held in memory there, as for any ``globus`` plugin client).
'''

import asyncio
import base64
import logging
import os
import tempfile
import threading

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from .plugin_session_base import PluginSession
from .plugin_base         import Plugin
from .client              import PluginClient
from .                    import lucid_flow as lf

log = logging.getLogger('radical.orbit')

_BROKER_ENV = 'RADICAL_ORBIT_LUCID_BROKER'


# ------------------------------------------------------------------------------
#
class LucidSession(PluginSession):
    '''Thin per-client session: all run state lives on the plugin.'''

    def __init__(self, sid: str):
        super().__init__(sid)

    async def get_config(self) -> dict:
        return await self._plugin._config()

    async def submit(self, params: dict) -> dict:
        return await self._plugin._submit(params)

    async def get_status(self) -> dict:
        return self._plugin._status()

    async def get_image(self, run_id: str, name: str) -> dict:
        return self._plugin._image(run_id, name)


# ------------------------------------------------------------------------------
#
class LucidClient(PluginClient):
    '''Application-side client for the ``lucid`` portal plugin.'''

    def config(self) -> dict:
        '''Defaults, analyses, plate layout and candidate endpoints.'''
        self._require_session()
        resp = self._http.get(self._url(f'config/{self.sid}'))
        self._raise(resp, 'config')
        return resp.json()

    def submit(self, endpoint: str, wells: list, **params) -> dict:
        '''Start a run; returns ``{run_id}``.  Optional ``params``:
        ``collection``, ``pipeline_path``, ``images_path``, ``analysis``.'''
        self._require_session()
        resp = self._http.post(self._url(f'submit/{self.sid}'),
                               json={'endpoint': endpoint, 'wells': wells,
                                     **params})
        self._raise(resp, 'submit')
        return resp.json()

    def status(self) -> dict:
        '''Snapshot of the current (or last) run.'''
        self._require_session()
        resp = self._http.get(self._url(f'status/{self.sid}'))
        self._raise(resp, 'status')
        return resp.json()

    def image(self, run_id: str, name: str) -> bytes:
        '''One fetched result image of a finished run.'''
        self._require_session()
        resp = self._http.get(self._url(f'image/{self.sid}/{run_id}/{name}'))
        self._raise(resp, 'image')
        return base64.b64decode(resp.json()['data'])


# ------------------------------------------------------------------------------
#
class PluginLucid(Plugin):
    '''
    LUCID Cell Painting portal (mock-up): input selection, target endpoint,
    analysis choice, submit, progress, and inline results.
    '''

    plugin_name   = 'lucid'
    session_class = LucidSession
    client_class  = LucidClient
    version       = '0.2.0'

    ui_config = {
        'icon'           : '🔬',
        'title'          : 'LUCID Cell Painting',
        'description'    : 'Mock-up portal: stage LUCID image data to an HPC '
                           'endpoint, run a Cell Painting analysis, stage '
                           'the results back and show them.',
        'custom_template': True,
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''The portal is a client of remote endpoints -- not a broker plugin.'''
        return not getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI, instance_name: str = 'lucid'):
        super().__init__(app, instance_name)

        self._run    : Optional[lf.LucidRun]      = None
        self._thread : Optional[threading.Thread] = None
        self._state  : Optional[Dict[str, Any]]   = None
        self._rt_own = None                        # runtime to $RADICAL_ORBIT_LUCID_BROKER
        self._lock   = threading.Lock()
        self._imgdir = tempfile.mkdtemp(prefix='orbit-lucid-')

        self.add_route_get ('config/{sid}',                self.get_config)
        self.add_route_post('submit/{sid}',                self.submit)
        self.add_route_get ('status/{sid}',                self.get_status)
        self.add_route_get ('image/{sid}/{run_id}/{name}', self.get_image)

    # --------------------------------------------------------------------------
    # routes
    #
    async def get_config(self, request: Request) -> dict:
        return await self._forward(request.path_params['sid'],
                                   LucidSession.get_config)

    async def submit(self, request: Request) -> dict:
        try:
            params = await request.json()
        except Exception:
            params = None
        if not isinstance(params, dict):
            raise HTTPException(status_code=400, detail='JSON object expected')
        return await self._forward(request.path_params['sid'],
                                   LucidSession.submit, params)

    async def get_status(self, request: Request) -> dict:
        return await self._forward(request.path_params['sid'],
                                   LucidSession.get_status)

    async def get_image(self, request: Request) -> dict:
        pp = request.path_params
        return await self._forward(pp['sid'], LucidSession.get_image,
                                   pp['run_id'], pp['name'])

    # --------------------------------------------------------------------------
    # implementation
    #
    def _runtime(self):
        '''The runtime the flow drives remote endpoints through.'''
        url = os.environ.get(_BROKER_ENV)
        if url:
            if self._rt_own is None:
                from .runtime import EndpointRuntime
                rt = EndpointRuntime(broker_url=url)
                rt.start(wait=True)
                self._rt_own = rt
            return self._rt_own
        rt = getattr(self._app.state, 'endpoint_service', None)
        if rt is None:
            raise HTTPException(status_code=503,
                                detail='no endpoint runtime available')
        return rt

    async def _config(self) -> dict:
        rt    = await asyncio.to_thread(self._runtime)
        topo  = await asyncio.to_thread(rt.topology)
        cands = lf.candidate_endpoints(topo)
        return {'defaults' : lf.DEFAULTS,
                'analyses' : [{k: a[k] for k in ('id', 'label', 'enabled')}
                              for a in lf.ANALYSES],
                'rows'     : lf.ROWS,
                'cols'     : lf.COLS,
                'wells'    : lf.WELLS,
                'endpoints': cands,
                'endpoint' : cands[0] if cands else None}

    async def _submit(self, params: dict) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                raise HTTPException(status_code=409,
                                    detail=f'run {self._run.run_id} is still '
                                           'active')
        endpoint = params.get('endpoint')
        if not endpoint:
            raise HTTPException(status_code=400, detail='no endpoint selected')
        try:
            refresh = lf.transfer_refresh_token()
            rt      = await asyncio.to_thread(self._runtime)
            topo    = await asyncio.to_thread(rt.topology)
            if endpoint not in lf.candidate_endpoints(topo):
                raise lf.LucidError(f'endpoint {endpoint!r} does not serve '
                                    f'{list(lf.NEEDED_PLUGINS)}')
            kw  = {k: params[k] for k in ('collection', 'pipeline_path',
                                          'images_path', 'analysis')
                   if params.get(k)}
            run = lf.LucidRun(rt, endpoint, params.get('wells') or [],
                              refresh_token=refresh, local_dir=self._imgdir,
                              on_update=self._on_update, **kw)
        except lf.LucidError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        with self._lock:
            self._run    = run
            self._state  = run.snapshot()
            self._thread = threading.Thread(target=run.run, daemon=True,
                                            name=f'lucid-{run.run_id}')
            self._thread.start()
        log.info('[lucid] started run %s on %s (%d wells)', run.run_id,
                 endpoint, len(run.state['wells']))
        return {'run_id': run.run_id}

    def _on_update(self, state: dict) -> None:
        with self._lock:
            self._state = state
        self._dispatch_notify('run_status', self._public(state))

    @staticmethod
    def _public(state: dict) -> dict:
        '''Run state for clients: image names instead of local paths.'''
        out = dict(state)
        out['images'] = [os.path.basename(p) for p in state.get('images', [])]
        return out

    def _status(self) -> dict:
        with self._lock:
            state = self._state
        if state is None:
            return {'status': 'idle'}
        return self._public(state)

    def _image(self, run_id: str, name: str) -> dict:
        with self._lock:
            state = self._state
        if not state or state.get('run_id') != run_id:
            raise HTTPException(status_code=404, detail=f'unknown run {run_id}')
        paths = {os.path.basename(p): p for p in state.get('images', [])}
        if name not in paths:
            raise HTTPException(status_code=404, detail=f'no image {name}')
        with open(paths[name], 'rb') as fin:
            data = base64.b64encode(fin.read()).decode()
        return {'name': name, 'mime': 'image/png', 'data': data}

    async def shutdown(self) -> None:
        if self._rt_own is not None:
            try:
                await asyncio.to_thread(self._rt_own.stop)
            except Exception as e:
                log.warning('[lucid] runtime stop failed: %s', e)
        await super().shutdown()
