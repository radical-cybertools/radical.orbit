'''
IRI Connect Plugin — endpoint configurator for dynamic IRI instances.

Broker-only plugin that lets users connect to IRI endpoints (NERSC, OLCF, …).
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

log = logging.getLogger('radical.orbit')


def _instance_key(endpoint: str) -> str:
    '''Build the ``iri.<endpoint>`` dynamic-instance name from a bare endpoint
    key.  Idempotent — an already-prefixed name is returned unchanged, so
    callers can pass either ``'nersc'`` or ``'iri.nersc'``.  The single place
    both the client and the plugin build this string.
    '''
    return endpoint if endpoint.startswith('iri.') else f'iri.{endpoint}'


class IRIConnectClient(PluginClient):
    '''Client-side helper for the ``iri_connect`` broker plugin.

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
        name = _instance_key(endpoint)
        resp = self._http.post(self._url(f'disconnect/{name}'))
        self._raise(resp, f'disconnect {name!r}')
        return resp.json()

    def connect(self, endpoint: str, token: str) -> 'IRIInstanceClient':
        '''Connect to an IRI endpoint and return a client for the instance.

        Idempotent: if the instance is already up, the broker refreshes the
        token in place and returns ``status='token_updated'``.  Either way
        we return a fresh client bound to the running instance.
        '''
        resp = self._http.post(self._url('connect'),
                               json={'endpoint': endpoint, 'token': token})
        self._raise(resp, f'connect {endpoint!r}')

        iname     = _instance_key(endpoint)
        namespace = f'/{self._endpoint_id}/{iname}'
        client    = IRIInstanceClient(
            self._http, namespace,
            broker_client=self._bc,
            endpoint_id=self._endpoint_id,
            plugin_name=iname)
        client.register_session()
        return client


class PluginIRIConnect(Plugin):
    '''Broker-only endpoint configurator for IRI.'''

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
        return getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI, instance_name: str = 'iri_connect'):
        super().__init__(app, instance_name)

        self.add_route_get ('endpoints',              self.list_endpoints)
        self.add_route_post('connect',                self.connect)
        self.add_route_post('disconnect/{name}',      self.disconnect)
        self.add_route_get ('status',                 self.get_status)

    # -- helpers ------------------------------------------------------------

    def _host(self):
        '''Return the BrokerPluginHost (our plugin host).'''
        host = getattr(self._app.state, 'endpoint_service', None)
        if host is None:
            raise HTTPException(status_code=500,
                                detail='No plugin host available')
        return host

    @staticmethod
    def _instance(host, name: str):
        '''Look up one dynamically-registered plugin instance by name.

        The single place this plugin reaches into the host's instance table
        (``host._plugins``); every route below goes through here or
        :meth:`_instances` instead of reading the private dict itself.
        '''
        return host._plugins.get(name)

    @staticmethod
    def _instances(host) -> Dict[str, Any]:
        '''The host's live plugin-instance table (dynamic + static).'''
        return host._plugins

    # -- routes -------------------------------------------------------------

    async def list_endpoints(self, request: Request) -> dict:
        '''Session-less: return available IRI endpoints and their status.'''
        host    = self._host()
        result  = {}
        for key, ep in IRI_ENDPOINTS.items():
            iname = _instance_key(key)
            result[key] = {
                'label'    : ep['label'],
                'url'      : ep['url'],
                'auth'     : ep.get('auth', ''),
                'connected': self._instance(host, iname) is not None,
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

        iname    = _instance_key(endpoint)
        host     = self._host()
        instance = self._instance(host, iname)

        # Idempotent reconnect: if the instance is already up, refresh its
        # bearer token in place rather than refusing.  This lets clients
        # rotate stale credentials without first having to disconnect.
        if instance is not None:
            instance.update_token(token.strip())
            log.info('[iri_connect] Updated token for %s', iname)
            return {'instance': iname, 'status': 'token_updated'}

        await host.register_dynamic_plugin(
            PluginIRIInstance, iname,
            endpoint=endpoint, token=token.strip())

        log.info('[iri_connect] Connected %s', iname)
        return {'instance': iname, 'status': 'connected'}

    async def disconnect(self, request: Request) -> dict:
        '''Disconnect an IRI endpoint instance.'''
        name = _instance_key(request.path_params['name'])
        host = self._host()

        if self._instance(host, name) is None:
            raise HTTPException(status_code=404,
                                detail=f'{name} not connected')

        await host.deregister_dynamic_plugin(name)
        log.info('[iri_connect] Disconnected %s', name)
        return {'instance': name, 'status': 'disconnected'}

    async def get_status(self, request: Request) -> dict:
        '''Return list of active iri.* instances.'''
        host = self._host()
        instances: Dict[str, dict] = {}
        for pname, plugin in self._instances(host).items():
            if pname.startswith('iri.'):
                instances[pname] = {
                    'endpoint': getattr(plugin, '_endpoint_key', ''),
                    'version' : plugin.version,
                }
        return {'instances': instances}

    async def register_session(self, request: Request) -> dict:
        '''No sessions needed — return a dummy SID for Explorer compat.'''
        return {'sid': 'iri_connect.static'}
