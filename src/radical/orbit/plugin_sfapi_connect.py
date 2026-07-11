'''
SFAPI Connect Plugin — endpoint configurator for dynamic SFAPI instances.

Broker-only plugin that lets users launch and drive HPC endpoints through
NERSC's Superfacility API (``api.nersc.gov``).  It is the direct-SFAPI sibling
of ``iri_connect``: same route surface and Explorer page, but it authenticates
with an OAuth2 client-credentials flow (``private_key_jwt``) instead of a
paste-able bearer token.

It exists because NERSC's IRI facility API is broken server-side for the
``/compute/*`` routes; the SFAPI path reaches the same Perlmutter scheduler
directly and was live-validated end-to-end (endpoint launched via SFAPI,
registered at the broker, ran a Rhapsody workload).

Instances
---------
On a successful ``connect`` the plugin dynamically registers a
:class:`~radical.orbit.plugin_sfapi_instance.PluginSFAPIInstance` under the
name ``sfapi.<endpoint>`` (e.g. ``sfapi.nersc``), which then appears as a
first-class node in the Explorer tree.  ``disconnect`` removes the dynamic
instance and its sessions.

Credential lifecycle
--------------------
``connect(endpoint, client_id, private_key)`` takes an OAuth2 client id plus
an RSA private key (PEM text).  Both are held in **broker process memory only**
(inside a :class:`~radical.orbit.plugin_sfapi_instance.SFAPITokenManager`) and
are **never** written to disk.  Short-lived access tokens are minted / refreshed
via ``authlib`` and also stay in memory only.  A missing ``authlib`` maps to
HTTP 501.

Because private keys must never be pasted into a browser, the Explorer page for
this plugin (``data/plugins/sfapi_connect.js``) lists endpoints and their status
but carries **no** connect form — connecting is done programmatically through
the client API.

Connect semantics (mint-before-register)
-----------------------------------------
``connect`` mints exactly one access token from the supplied credentials
*before* touching the topology:

* On a **fresh** connect the pre-verified token manager is handed to the new
  instance, so bad credentials fail fast with **401** and never leave a
  half-registered instance behind (no deregister path, no register/deregister
  race with a concurrent reconnect).
* On a **reconnect** (instance already up) the new credentials are verified the
  same way, then rotated in place via ``update_credentials`` — clients can
  rotate stale / expired credentials without disconnecting first.

Client API
----------
:class:`SFAPIConnectClient`: ``list_endpoints()``, ``connect(endpoint,
client_id, private_key)`` → an :class:`IRIInstanceClient` bound to the new
``sfapi.<endpoint>`` instance (route surface is identical to the IRI instance),
``disconnect(endpoint)``, ``get_status()``.
'''

import logging
import os

from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request

from .client                import PluginClient
from .plugin_base           import Plugin
from .plugin_iri_instance   import IRIInstanceClient
from .sfapi_endpoints       import SFAPI_ENDPOINTS

log = logging.getLogger('radical.orbit')


def _instance_key(endpoint: str) -> str:
    '''Build the ``sfapi.<endpoint>`` dynamic-instance name from a bare
    endpoint key.  Idempotent — an already-prefixed name is returned unchanged,
    so callers can pass either ``'nersc'`` or ``'sfapi.nersc'``.  The single
    place both the client and the plugin build this string.
    '''
    return endpoint if endpoint.startswith('sfapi.') else f'sfapi.{endpoint}'


class SFAPIConnectClient(PluginClient):
    '''Client-side helper for the ``sfapi_connect`` broker plugin.

    ``connect()`` returns a ready-to-use :class:`IRIInstanceClient` bound to
    the dynamically registered ``sfapi.<endpoint>`` plugin instance (the SFAPI
    instance shares the IRI instance's route surface).
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

    def connect(self, endpoint: str, client_id: str,
                private_key: str) -> 'IRIInstanceClient':
        '''Connect to an SFAPI endpoint and return a client for the instance.

        Authenticates with ``client_id`` + ``private_key`` (PEM text).
        Idempotent: if the instance is already up, the broker verifies the new
        credentials and rotates them in place (``status='credentials_updated'``).
        Either way we return a fresh client bound to the running instance.
        '''
        body: Dict[str, Any] = {'endpoint'   : endpoint,
                                'client_id'  : client_id,
                                'private_key': private_key}

        resp = self._http.post(self._url('connect'), json=body)
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


class PluginSFAPIConnect(Plugin):
    '''Broker-only endpoint configurator for direct SFAPI.'''

    plugin_name   = 'sfapi_connect'
    session_class = None
    client_class  = SFAPIConnectClient
    version       = '0.0.1'
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'sfapi_connect.js')

    ui_config = {
        'icon'       : '🔐',
        'title'      : 'SFAPI Connect',
        'description': 'Connect to SFAPI endpoints (NERSC).',
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        return getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI, instance_name: str = 'sfapi_connect'):
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
        '''Session-less: return available SFAPI endpoints and their status.'''
        host    = self._host()
        result  = {}
        for key, ep in SFAPI_ENDPOINTS.items():
            iname = _instance_key(key)
            result[key] = {
                'label'    : ep['label'],
                'url'      : ep['url'],
                'auth'     : 'sfapi',
                'connected': self._instance(host, iname) is not None,
            }
        return result

    async def connect(self, request: Request) -> dict:
        '''Connect (or rotate credentials on) a direct-SFAPI endpoint.

        Expects JSON body:
        ``{"endpoint": "nersc", "client_id": ..., "private_key": ...}``.
        Creates a dynamic ``sfapi.<endpoint>`` plugin instance.

        An access token is minted from the given credentials *before* the
        instance is registered / rotated, so bad credentials fail fast with
        401 and never leave a half-registered instance behind (no deregister
        path, no register/deregister race with a concurrent reconnect).
        '''
        try:
            data = await request.json()
        except Exception:
            data = {}

        endpoint = data.get('endpoint', '')

        if endpoint not in SFAPI_ENDPOINTS:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown endpoint {endpoint!r}. '
                       f'Valid: {list(SFAPI_ENDPOINTS.keys())}')

        client_id   = data.get('client_id', '')
        private_key = data.get('private_key', '')

        if not client_id or not client_id.strip():
            raise HTTPException(status_code=400,
                                detail='client_id must not be empty')
        if not private_key or not private_key.strip():
            raise HTTPException(status_code=400,
                                detail='private_key must not be empty')

        client_id   = client_id.strip()
        private_key = private_key.strip()

        iname    = _instance_key(endpoint)
        host     = self._host()
        instance = self._instance(host, iname)

        try:
            from .plugin_sfapi_instance import (PluginSFAPIInstance,
                                                SFAPITokenManager)
        except ImportError as exc:
            raise HTTPException(
                status_code=501,
                detail="sfapi support requires 'authlib'") from exc

        token_url = SFAPI_ENDPOINTS[endpoint]['token_url']

        # Mint one token to verify the credentials before touching topology.
        try:
            mgr = SFAPITokenManager(client_id, private_key, token_url)
        except ImportError as exc:
            raise HTTPException(
                status_code=501,
                detail="sfapi support requires 'authlib'") from exc

        try:
            await mgr.mint()
        except Exception:
            await mgr.aclose()
            raise

        # Idempotent reconnect: verified new credentials — rotate in place.
        if instance is not None:
            await mgr.aclose()
            instance.update_credentials(client_id, private_key)
            log.info('[sfapi_connect] Updated credentials for %s', iname)
            return {'instance': iname, 'status': 'credentials_updated'}

        # Fresh connect: hand the pre-verified token manager to the instance.
        try:
            await host.register_dynamic_plugin(
                PluginSFAPIInstance, iname,
                endpoint=endpoint, client_id=client_id,
                private_key=private_key, token_manager=mgr)
        except Exception:
            await mgr.aclose()
            raise

        log.info('[sfapi_connect] Connected %s', iname)
        return {'instance': iname, 'status': 'connected'}

    async def disconnect(self, request: Request) -> dict:
        '''Disconnect an SFAPI endpoint instance.'''
        name = _instance_key(request.path_params['name'])
        host = self._host()

        if self._instance(host, name) is None:
            raise HTTPException(status_code=404,
                                detail=f'{name} not connected')

        await host.deregister_dynamic_plugin(name)
        log.info('[sfapi_connect] Disconnected %s', name)
        return {'instance': name, 'status': 'disconnected'}

    async def get_status(self, request: Request) -> dict:
        '''Return list of active sfapi.* instances.'''
        host = self._host()
        instances: Dict[str, dict] = {}
        for pname, plugin in self._instances(host).items():
            if pname.startswith('sfapi.'):
                instances[pname] = {
                    'endpoint': getattr(plugin, '_endpoint_key', ''),
                    'version' : plugin.version,
                }
        return {'instances': instances}

    async def register_session(self, request: Request) -> dict:
        '''No sessions needed — return a dummy SID for Explorer compat.'''
        return {'sid': 'sfapi_connect.static'}
