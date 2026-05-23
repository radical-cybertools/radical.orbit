
import fnmatch
import logging

from importlib.metadata import entry_points
from typing             import Any, Dict, List, Type

from fastapi import FastAPI

from radical.edge.plugin_base import Plugin

log = logging.getLogger('radical.edge')


# ---------------------------------------------------------------------------
# Default plugin set per host role.
#
# Wildcards (fnmatch-style) are accepted; missing plugins are silently
# skipped at expansion time so that an environment without (say) Rhapsody
# installed still gets a working default set.
# ---------------------------------------------------------------------------

DEFAULT_PLUGINS_BY_ROLE: Dict[str, List[str]] = {
    'bridge'    : ['iri*',     'staging', 'sysinfo', 'task_dispatcher'],
    'login'     : ['psij',     'staging', 'sysinfo', 'queue_info'],
    'compute'   : ['rhapsody', 'staging', 'sysinfo', 'queue_info'],
    'standalone': ['psij',     'staging', 'sysinfo', 'rhapsody', 'queue_info'],
}


# ---------------------------------------------------------------------------
# Utility functions (shared by BridgePluginHost and EdgeService)
# ---------------------------------------------------------------------------

def _expand_special_tokens(requested: list, app: FastAPI,
                           available: list) -> list:
    """Expand the special tokens ``'all'`` and ``'default'`` in a plugin list.

    ``'all'``     -> every registered plugin name.
    ``'default'`` -> the role-specific default set (see
                     ``DEFAULT_PLUGINS_BY_ROLE``).  Wildcards in that set
                     (e.g. ``'iri*'``) are expanded against ``available``.
                     Missing entries are silently skipped — a plugin that
                     isn't installed shouldn't break the default load.

    Other tokens are passed through unchanged for ``_resolve_plugin_names``
    to handle (exact match, prefix match, or wildcard glob).
    """
    expanded = []
    for token in requested:
        if token == 'all':
            expanded.extend(available)
        elif token == 'default':
            from .utils import host_role
            role = host_role(app)['role']
            for name in DEFAULT_PLUGINS_BY_ROLE.get(role, []):
                if '*' in name:
                    expanded.extend(fnmatch.filter(available, name))
                elif name in available:
                    expanded.append(name)
                # else: plugin not installed — silently skip
        else:
            expanded.append(token)
    return expanded


def _resolve_plugin_names(requested: list, available: list) -> list:
    """Resolve a requested plugin list against the available plugin names.

    Token forms accepted:
      - exact match: ``'sysinfo'``
      - prefix match: ``'sys'`` -> ``'sysinfo'``
      - wildcard glob (fnmatch): ``'iri*'`` -> all names starting with ``iri``

    Special tokens ``'all'`` / ``'default'`` are not handled here; the
    caller should run :func:`_expand_special_tokens` first.

    Args:
        requested: List of tokens after special-token expansion.
        available: Full list of registered plugin names.

    Returns:
        Ordered list of resolved plugin names, deduplicated.

    Raises:
        ValueError: If a token matches nothing or is ambiguous.
    """
    result: list = []
    for token in requested:
        if '*' in token or '?' in token:
            matches = fnmatch.filter(available, token)
            if not matches:
                raise ValueError(
                    f"No plugin matches pattern '{token}'. "
                    f"Available: {', '.join(sorted(available))}")
            result.extend(matches)
            continue
        if token in available:
            result.append(token)
            continue
        matches = [p for p in available if p.startswith(token)]
        if not matches:
            raise ValueError(
                f"No plugin matches '{token}'. "
                f"Available: {', '.join(sorted(available))}")
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous plugin name '{token}': matches {sorted(matches)}")
        result.append(matches[0])

    # Dedupe, preserving order
    seen: set = set()
    out:  list = []
    for x in result:
        if x not in seen:
            out.append(x); seen.add(x)
    return out


def _discover_entry_points() -> None:
    """Discover and import external plugins via ``radical.edge.plugins`` entry points.

    Importing the entry-point module triggers ``Plugin.__init_subclass__``,
    which auto-registers the plugin class in ``Plugin._registry``.
    """
    try:
        for ep in entry_points(group='radical.edge.plugins'):
            try:
                ep.load()
                log.info('[PluginHost] Discovered external plugin: %s', ep.name)
            except Exception:
                log.exception('[PluginHost] Failed to load entry point: %s',
                              ep.name)
    except Exception:
        log.debug('[PluginHost] No external plugins found')


# ---------------------------------------------------------------------------
# PluginHostBase — mixin for BridgePluginHost and EdgeService
# ---------------------------------------------------------------------------

class PluginHostBase:
    """Shared plugin loading and dynamic registration logic.

    Both ``BridgePluginHost`` and ``EdgeService`` manage a set of plugins
    attached to a ``FastAPI`` app.  This mixin extracts the common parts:

    * Static loading from a filter list (startup)
    * Dynamic registration / deregistration (runtime)
    * Abstract topology announcement hook

    **Contract:** the using class must set ``self._app`` (``FastAPI``) and
    ``self._plugins`` (``Dict[str, Plugin]``) before calling any methods
    defined here.
    """

    # Declared for type-checkers; set by the using class.
    _app     : FastAPI
    _plugins : Dict[str, Plugin]

    # ------------------------------------------------------------------
    # static plugin loading (startup)
    # ------------------------------------------------------------------

    def _load_plugins_from_filter(self, plugin_filter: List[str]) -> None:
        """Discover entry points, resolve names, instantiate enabled plugins.

        Args:
            plugin_filter: List of plugin name tokens.  Accepts exact names,
                prefix matches (``sys`` -> ``sysinfo``), wildcards
                (``iri*``), and the special tokens ``all`` and ``default``.
        """
        _discover_entry_points()

        available = Plugin.get_plugin_names()
        expanded  = _expand_special_tokens(plugin_filter, self._app, available)
        to_load   = _resolve_plugin_names(expanded, available)
        log.info('[PluginHost] Loading plugins: %s', to_load)

        for pname in to_load:
            try:
                pcls = Plugin.get_plugin_class(pname)
                if pcls is None:
                    log.warning('[PluginHost] No class for plugin: %s', pname)
                    continue
                if not pcls.is_enabled(self._app):
                    log.info('[PluginHost] Skipping plugin '
                             '(not applicable here): %s', pname)
                    continue
                pinstance = pcls(app=self._app)
                self._plugins[pname] = pinstance
                log.info('[PluginHost] Loaded plugin: %s', pname)
            except Exception:
                log.exception('[PluginHost] Failed to load plugin: %s', pname)

    # ------------------------------------------------------------------
    # dynamic plugin registration (runtime)
    # ------------------------------------------------------------------

    async def register_dynamic_plugin(
        self,
        cls            : Type[Plugin],
        instance_name  : str,
        **kwargs       : Any,
    ) -> Plugin:
        """Instantiate a plugin at runtime and announce it.

        Args:
            cls:            Plugin class to instantiate.
            instance_name:  Unique name for this instance (e.g. ``'iri.nersc'``).
            **kwargs:       Extra keyword arguments forwarded to the plugin
                            constructor (after ``app`` and ``instance_name``).

        Returns:
            The newly created plugin instance.

        Raises:
            ValueError: If ``instance_name`` is already registered.
        """
        if instance_name in self._plugins:
            raise ValueError(
                f"Plugin instance '{instance_name}' already registered. "
                f"Deregister the existing instance first.")

        plugin = cls(app=self._app, instance_name=instance_name, **kwargs)
        self._plugins[instance_name] = plugin
        log.info('[PluginHost] Dynamically registered plugin: %s', instance_name)

        await self._announce_topology()
        return plugin

    async def deregister_dynamic_plugin(self, instance_name: str) -> None:
        """Remove a dynamically registered plugin instance.

        Closes all sessions on the plugin, strips the plugin's routes from
        the host's direct-dispatch table (otherwise a re-register would
        leave the dead plugin's stale routes ahead of the new ones in
        match order), and announces the topology change.  If
        *instance_name* is not found this is a silent no-op.
        """
        plugin = self._plugins.pop(instance_name, None)
        if plugin is None:
            return

        # Close all active sessions
        for sid in list(plugin._sessions):
            session = plugin._sessions.pop(sid, None)
            if session:
                try:
                    await session.close()
                except Exception as exc:
                    log.warning('[PluginHost] Error closing session %s/%s: %s',
                                instance_name, sid, exc)

        # Cancel cleanup task if running
        if plugin._cleanup_task and not plugin._cleanup_task.done():
            plugin._cleanup_task.cancel()

        # Strip the plugin's routes from the direct-dispatch table.
        # ``Plugin._register_direct`` records every entry it adds in the
        # plugin's own ``_owned_routes`` list, so we know exactly which
        # entries to drop here.  Without this, a subsequent
        # register_dynamic_plugin call would leave the dead plugin's
        # routes ahead of the new ones, and requests would dispatch onto
        # an instance whose ``_sessions`` is empty.
        direct_routes = getattr(self._app.state, 'direct_routes', None)
        owned         = getattr(plugin, '_owned_routes', None)
        if direct_routes is not None and owned:
            owned_ids = {id(entry) for entry in owned}
            direct_routes[:] = [e for e in direct_routes
                                  if id(e) not in owned_ids]
            log.debug('[PluginHost] Stripped %d direct routes for %s',
                      len(owned_ids), instance_name)
            owned.clear()

        log.info('[PluginHost] Deregistered plugin: %s', instance_name)
        await self._announce_topology()

    # ------------------------------------------------------------------
    # topology announcement (abstract)
    # ------------------------------------------------------------------

    async def _announce_topology(self) -> None:
        """Broadcast a topology change to connected clients.

        Subclass must override:
        * **BridgePluginHost** — SSE broadcast to Explorer clients.
        * **EdgeService** — ``topology`` WebSocket message to bridge.
        """
        raise NotImplementedError
