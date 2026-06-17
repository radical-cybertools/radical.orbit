'''
IRI Connect Plugin — endpoint configurator for dynamic IRI instances.

Bridge-only plugin that lets users connect to IRI endpoints (NERSC, OLCF, …).
On successful connect it dynamically registers a ``PluginIRIInstance`` under
the name ``iri.<endpoint>`` (e.g. ``iri.nersc``), which then appears as a
first-class node in the Explorer tree.

Disconnect removes the dynamic instance and its sessions.
'''

import logging
import os

from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request

from .client              import PluginClient
from .plugin_base         import Plugin
from .plugin_iri_instance import PluginIRIInstance, IRIInstanceClient
from .iri_endpoints       import IRI_ENDPOINTS

log = logging.getLogger('radical.edge')


class IRIConnectClient(PluginClient):
    '''Client-side helper for the ``iri_connect`` bridge plugin.

    ``connect()`` returns a ready-to-use :class:`IRIInstanceClient` bound to
    the dynamically registered ``iri.<endpoint>`` plugin instance.
    '''

    def list_endpoints(self) -> Dict[str, Any]:
        resp = self._http.get(self._url('endpoints'))
        self._raise(resp)
        return resp.json()

    def get_status(self) -> Dict[str, Any]:
        resp = self._http.get(self._url('status'))
        self._raise(resp)
        return resp.json()

    def disconnect(self, endpoint: str) -> Dict[str, Any]:
        name = endpoint if endpoint.startswith('iri.') else f'iri.{endpoint}'
        resp = self._http.post(self._url(f'disconnect/{name}'))
        self._raise(resp, f'disconnect {name!r}')
        return resp.json()

    def connect(self, endpoint: str, token: str) -> 'IRIInstanceClient':
        '''Connect to an IRI endpoint and return a client for the instance.

        Idempotent: if the instance is already up, the bridge refreshes the
        token in place and returns ``status='token_updated'``.  Either way
        we return a fresh client bound to the running instance.
        '''
        resp = self._http.post(self._url('connect'),
                               json={'endpoint': endpoint, 'token': token})
        self._raise(resp, f'connect {endpoint!r}')

        iname     = f'iri.{endpoint}'
        namespace = f'/{self._edge_id}/{iname}'
        client    = IRIInstanceClient(
            self._http, namespace,
            bridge_client=self._bc,
            edge_id=self._edge_id,
            plugin_name=iname)
        client.register_session()
        return client


class PluginIRIConnect(Plugin):
    '''Bridge-only endpoint configurator for IRI.'''

    plugin_name   = 'iri_connect'
    session_class = None
    client_class  = IRIConnectClient
    version       = '0.0.1'
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'iri_connect.js')

    ui_config = {
        'icon'       : '🔌',
        'title'      : 'IRI Connect',
        'description': 'Connect to IRI endpoints (NERSC, OLCF, …).',
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        return getattr(app.state, 'is_bridge', False)

    def __init__(self, app: FastAPI, instance_name: str = 'iri_connect'):
        super().__init__(app, instance_name)

        self.add_route_get ('endpoints',              self.list_endpoints)
        self.add_route_post('connect',                self.connect)
        self.add_route_post('disconnect/{name}',      self.disconnect)
        self.add_route_get ('status',                 self.get_status)

    # -- helpers ------------------------------------------------------------

    def _host(self):
        '''Return the BridgePluginHost (our plugin host).'''
        host = getattr(self._app.state, 'edge_service', None)
        if host is None:
            raise HTTPException(status_code=500,
                                detail='No plugin host available')
        return host

    def _instance_key(self, endpoint: str) -> str:
        return f'iri.{endpoint}'

    # -- routes -------------------------------------------------------------

    async def list_endpoints(self, request: Request) -> dict:
        '''Session-less: return available IRI endpoints and their status.'''
        host    = self._host()
        result  = {}
        for key, ep in IRI_ENDPOINTS.items():
            iname = self._instance_key(key)
            result[key] = {
                'label'    : ep['label'],
                'url'      : ep['url'],
                'auth'     : ep.get('auth', ''),
                'connected': iname in host._plugins,
            }
        return result

    async def connect(self, request: Request) -> dict:
        '''Connect to an IRI endpoint.

        Expects JSON body: ``{"endpoint": "nersc", "token": "<bearer>"}``.
        Creates a dynamic ``iri.<endpoint>`` plugin instance.
        '''
        try:
            data = await request.json()
        except Exception:
            data = {}

        endpoint = data.get('endpoint', '')
        token    = data.get('token', '')

        if endpoint not in IRI_ENDPOINTS:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown endpoint {endpoint!r}. '
                       f'Valid: {list(IRI_ENDPOINTS.keys())}')

        if not token or not token.strip():
            raise HTTPException(status_code=400,
                                detail='token must not be empty')

        iname = self._instance_key(endpoint)
        host  = self._host()

        # Idempotent reconnect: if the instance is already up, refresh its
        # bearer token in place rather than refusing.  This lets clients
        # rotate stale credentials without first having to disconnect.
        if iname in host._plugins:
            host._plugins[iname].update_token(token.strip())
            log.info('[iri_connect] Updated token for %s', iname)
            return {'instance': iname, 'status': 'token_updated'}

        await host.register_dynamic_plugin(
            PluginIRIInstance, iname,
            endpoint=endpoint, token=token.strip())

        log.info('[iri_connect] Connected %s', iname)
        return {'instance': iname, 'status': 'connected'}

    async def disconnect(self, request: Request) -> dict:
        '''Disconnect an IRI endpoint instance.'''
        name = request.path_params['name']
        host = self._host()

        # Allow both 'iri.nersc' and just 'nersc'
        if not name.startswith('iri.'):
            name = f'iri.{name}'

        if name not in host._plugins:
            raise HTTPException(status_code=404,
                                detail=f'{name} not connected')

        await host.deregister_dynamic_plugin(name)
        log.info('[iri_connect] Disconnected %s', name)
        return {'instance': name, 'status': 'disconnected'}

    async def get_status(self, request: Request) -> dict:
        '''Return list of active iri.* instances.'''
        host = self._host()
        instances: Dict[str, dict] = {}
        for pname, plugin in host._plugins.items():
            if pname.startswith('iri.'):
                instances[pname] = {
                    'endpoint': getattr(plugin, '_endpoint_key', ''),
                    'version' : plugin.version,
                }
        return {'instances': instances}

    async def register_session(self, request: Request) -> dict:
        '''No sessions needed — return a dummy SID for Explorer compat.'''
        return {'sid': 'iri_connect.static'}
