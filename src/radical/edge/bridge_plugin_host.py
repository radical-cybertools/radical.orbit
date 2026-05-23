
import logging
import os

from typing             import Any, Callable, Dict, List, Optional, Tuple

from fastapi            import FastAPI, HTTPException
from starlette.responses import JSONResponse

from radical.edge.plugin_base      import Plugin
from radical.edge.plugin_host_base import PluginHostBase
from radical.edge.service          import RequestShim
from radical.edge.ui_schema        import ui_config_to_dict

log = logging.getLogger("radical.edge.bridge")


class BridgePluginHost(PluginHostBase):
    """Lightweight plugin host for running plugins directly on the bridge.

    Satisfies the ``Plugin`` contract (``app.state.edge_service`` with a
    ``send_notification`` method) without the WebSocket / reconnection
    machinery of a full ``EdgeService``.
    """

    def __init__(self, plugin_names        : List[str],
                       broadcast_fn        : Callable,
                       edge_name           : str      = 'bridge',
                       on_topology_changed : Optional[Callable] = None,
                       bridge_url          : str      = ''):

        self._name                : str      = edge_name
        self._broadcast_fn        : Callable = broadcast_fn
        self._on_topology_changed : Optional[Callable] = on_topology_changed
        self._plugins             : Dict[str, Plugin] = {}

        # Internal FastAPI app — plugins register routes here.
        # bridge_url is the bridge's own loopback-reachable URL so that
        # bridge-hosted plugins (e.g. task_dispatcher) can construct a
        # BridgeClient pointing at this same bridge for cross-edge calls.
        self._app = FastAPI(title='Bridge Plugin Host')
        self._app.state.edge_service = self
        self._app.state.edge_name    = edge_name
        self._app.state.bridge_url   = bridge_url
        self._app.state.is_bridge    = True

        self._load_plugins_from_filter(plugin_names)

        # Reference the live list — not a copy — so dynamically registered
        # plugin routes are visible immediately.
        self._direct_routes: list = getattr(self._app.state, 'direct_routes', [])

    # ------------------------------------------------------------------
    # topology announcement  (PluginHostBase contract)
    # ------------------------------------------------------------------

    async def _announce_topology(self) -> None:
        """Broadcast topology update to all SSE clients.

        Uses the ``on_topology_changed`` callback if provided (allows the
        bridge script to update its global state and broadcast the full
        topology).  Falls back to a direct SSE broadcast of the bridge's
        own topology info.
        """
        try:
            if self._on_topology_changed:
                await self._on_topology_changed()
            else:
                await self._broadcast_fn('topology',
                                         self.get_topology_info())
        except Exception as exc:
            log.warning('[BridgePluginHost] Topology broadcast failed: %s', exc)

    # ------------------------------------------------------------------
    # notification shim  (called by Plugin.send_notification)
    # ------------------------------------------------------------------

    async def send_notification(self, plugin_name: str, topic: str,
                                data: Dict[str, Any]) -> None:
        try:
            await self._broadcast_fn('notification', {
                'edge'  : self._name,
                'plugin': plugin_name,
                'topic' : topic,
                'data'  : data,
            })
        except Exception as e:
            log.warning('[BridgePluginHost] Notification send failed: %s', e)

    # ------------------------------------------------------------------
    # route dispatch
    # ------------------------------------------------------------------

    def match_route(self, method: str, path: str
                    ) -> Tuple[Optional[Callable], Optional[dict]]:
        for rt_method, pattern, param_names, handler in self._direct_routes:
            if rt_method == method:
                m = pattern.match(path)
                if m:
                    return handler, dict(zip(param_names, m.groups()))
        return None, None

    async def handle_request(self, method: str, path: str,
                             headers: dict, body_bytes: bytes,
                             query_string: str = ''):

        import urllib.parse
        query_params = dict(urllib.parse.parse_qsl(query_string)) \
                       if query_string else {}

        handler, path_params = self.match_route(method, path)
        if handler is None:
            raise HTTPException(status_code=404,
                                detail=f'No route: {method} {path}')

        content_type = headers.get('content-type', 'application/json')
        shim = RequestShim(path_params, query_params, body_bytes, content_type)

        try:
            result = await handler(shim)
        except HTTPException:
            raise
        except Exception as e:
            log.exception('[BridgePluginHost] Handler error: %s %s', method, path)
            raise HTTPException(status_code=500, detail=str(e)) from e

        if hasattr(result, 'status_code'):
            return result
        return JSONResponse(result)

    # ------------------------------------------------------------------
    # topology
    # ------------------------------------------------------------------

    def get_topology_info(self) -> dict:
        return {
            'endpoint': {'type': 'radical.edge.bridge'},
            'plugins' : {
                pname: {
                    'type'     : pname,
                    'namespace': f'/{self._name}{plugin.namespace}',
                    'version'  : plugin.version,
                    'enabled'  : True,
                    'ui_config': ui_config_to_dict(
                        getattr(plugin, 'ui_config', None)),
                }
                for pname, plugin in self._plugins.items()
            },
        }

    async def on_topology_change(self, edges: dict) -> None:
        for pname, plugin in self._plugins.items():
            try:
                await plugin.on_topology_change(edges)
            except Exception as e:
                log.warning('[BridgePluginHost] %s topology handler failed: %s',
                            pname, e)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def get_ui_modules(self) -> Dict[str, str]:
        """Return {plugin_name: js_content} for plugins with UI modules."""
        modules: Dict[str, str] = {}
        for pname, plugin in self._plugins.items():
            ui_path = getattr(plugin.__class__, 'ui_module', None)
            if ui_path and os.path.isfile(ui_path):
                try:
                    with open(ui_path, encoding='utf-8') as fh:
                        modules[pname] = fh.read()
                except Exception:
                    log.warning('[BridgePluginHost] Could not read ui_module '
                                'for %s: %s', pname, ui_path)
        return modules

    @property
    def plugins(self) -> Dict[str, Plugin]:
        return self._plugins
