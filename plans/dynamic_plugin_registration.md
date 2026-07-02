# Dynamic Plugin Registration — Design Notes

Captured from active session to preserve full reasoning for pickup in a new session.

---

## 1. Motivation

The immediate driver is the `iri_connect` pattern, but the mechanism is general.

**Problem with the current `iri` / `iri_info` split:**
- Two plugins, two pages, awkward multi-endpoint handling in JS
- `api.getSession` in the Explorer caches exactly one session per plugin per
  edge — can't hold sessions for NERSC and OLCF simultaneously
- Non-JS clients must know about the endpoint concept implicitly

**Proposed model:**
- A new `iri_connect` plugin (bridge-only) acts as an endpoint configurator
- On successful connect, it dynamically registers a new plugin instance named
  `iri.nersc`, `iri.olcf`, etc. with the bridge
- Each such instance handles *both* resource listing and job submission for
  that specific endpoint on a single page
- The instance appears as a first-class node in the Explorer's left-side tree
- Python clients can discover it via `edge.get_plugin('iri.nersc')` without
  special-casing

---

## 2. Why Dynamic Registration Works

FastAPI/Starlette iterates `app.routes` on every incoming request — it does
not compile routes into a fixed lookup table at startup.  Appending routes at
runtime (via `app.add_api_route()` or `app.routes.append(...)`) is picked up
immediately on the next request.  No restart, no reload required.

The Plugin base class already calls `app.add_api_route()` in
`add_route_get` / `add_route_post`.  So instantiating a plugin at runtime
automatically registers its routes.  The only missing piece is:

1. A method on the plugin host to instantiate + register a plugin after startup
2. A topology broadcast to tell Explorer tabs (and the bridge, from the edge)
   that a new plugin appeared

---

## 3. Shared `PluginHostBase`

Both `BridgePluginHost` (`bridge_plugin_host.py`) and `EdgeService`
(`service.py`) currently duplicate plugin-management logic:
- Loading plugin classes
- Checking `is_enabled()`
- Instantiating with `instance_name`
- Maintaining `self._plugins` dict

The only thing that differs between them is *how they announce topology
changes*:
- **Bridge** → SSE broadcast to all connected Explorer clients
- **Edge** → WebSocket `topology_update` message to the bridge

Proposed extraction: a `PluginHostBase` mixin in a new file
`plugin_host_base.py`:

```python
class PluginHostBase:
    """Mixin: shared plugin loading and dynamic registration."""

    def _init_plugin_host(self, app: FastAPI):
        self._app     = app       # already set by both hosts
        self._plugins = {}        # name -> Plugin instance

    def load_plugins(self, plugin_classes: list):
        for cls in plugin_classes:
            if cls.is_enabled(self._app):
                self._load_plugin(cls)

    def _load_plugin(self, cls, instance_name: str = None, **kwargs):
        name   = instance_name or cls.plugin_name
        plugin = cls(self._app, instance_name=name, **kwargs)
        self._plugins[name] = plugin
        log.info('[PluginHost] loaded plugin %s', name)
        return plugin

    async def register_dynamic_plugin(self, cls, instance_name: str,
                                       **kwargs) -> 'Plugin':
        """Instantiate a plugin at runtime and announce it to the topology."""
        plugin = self._load_plugin(cls, instance_name, **kwargs)
        await self._announce_topology()
        return plugin

    async def deregister_dynamic_plugin(self, instance_name: str):
        """Remove a dynamic plugin instance.

        NOTE: FastAPI route removal is possible (pop from app.routes) but
        requires care — see Section 6 below.  For now this can mark the
        plugin inactive and return 410 Gone from all its routes, deferring
        actual route removal.
        """
        plugin = self._plugins.pop(instance_name, None)
        if plugin:
            await plugin.close()
            await self._announce_topology()

    async def _announce_topology(self):
        """Override in subclass: bridge uses SSE, edge uses WS message."""
        raise NotImplementedError
```

`BridgePluginHost` extends this and implements:
```python
async def _announce_topology(self):
    await self._broadcast_sse_topology()   # existing SSE path
```

`EdgeService` uses it as a mixin and implements:
```python
async def _announce_topology(self):
    await self._ws_send({'type': 'topology_update',
                         'plugins': self._build_plugin_info()})
```

**Composition note:** `EdgeService` is not *only* a plugin host — it also owns
the WebSocket connection, heartbeat, request dispatch, etc.  Use as a mixin
(multiple inheritance), not a base class replacement:
```python
class EdgeService(PluginHostBase, ...):
    ...
```
The mixin only touches `self._app` and `self._plugins`, which both classes
already have — no collision.

---

## 4. New WebSocket Message Type

The edge currently sends `register`, `response`, `notification`, `pong` to
the bridge.  Add:

```
topology_update  (edge → bridge)
    payload: { "plugins": { "plugin_name": { "namespace": ...,
                                              "ui_config": ...,
                                              "version": ... } } }
```

The bridge handles this by merging the new plugin info into its topology state
for that edge and re-broadcasting the SSE topology event to all Explorer
clients.

This is needed only for dynamic edge-side plugins (not needed for the
immediate `iri_connect` use case, which is bridge-only).

---

## 5. Plugin Instance Naming

`plugin_name` is currently a class attribute (e.g. `plugin_name = 'iri'`).
Dynamic instances need per-instance names: `iri.nersc`, `iri.olcf`.

The `Plugin.__init__` already accepts `instance_name` and uses it for the
namespace if provided.  Confirm: verify in `plugin_base.py` that
`self._instance_name = instance_name or self.plugin_name` is the pattern
and that routes are registered under `self._instance_name` (not the class
attribute).  If not, that's the only change needed to `plugin_base.py`.

---

## 6. Route Removal (Deregistration)

Adding routes at runtime is safe.  Removing them is trickier — Starlette
iterates `app.routes` directly, so:

```python
app.routes[:] = [r for r in app.routes if not r.path.startswith(f'/{name}/')]
```

This works but is fragile.  Two safer options:

**Option A (recommended for now):** Don't remove routes.  On deregister, mark
the plugin as inactive; all its route handlers return `410 Gone`.  Routes
accumulate until bridge restart — acceptable since the set of endpoints is
small and restarts clear everything.

**Option B (clean but more work):** Wrap each plugin's routes in a
`MountedRouter` that can be disabled.  More effort, deferred.

---

## 7. `iri_connect` Plugin Design

### Python: `plugin_iri_connect.py`

- Bridge-only (`is_enabled` checks `is_bridge`)
- Single route: `POST connect` — accepts `{endpoint, token}`, validates,
  calls `self._host.register_dynamic_plugin(PluginIRIInstance, 'iri.' + ep,
  endpoint=ep, token=token)`
- Also: `POST disconnect/{instance_name}` — deregisters
- Also: `GET status` — returns list of active `iri.*` instances

### JS: `iri_connect.js`

- Shows the endpoint table (active radio | name | URL | token set/✔)
  — essentially what `iri_info.js` now has for the endpoint table
- On connect success: Explorer topology update arrives via SSE, new
  `iri.nersc` node appears in left-side tree automatically
- Does *not* need to navigate — the user clicks the new tree node themselves

### Merged `PluginIRIInstance` / `plugin_iri_instance.py`

Replaces both `plugin_iri.py` and `plugin_iri_info.py`:
- Takes `endpoint` and `token` at construction time (not at session time)
- Single session per instance (one endpoint = one session)
- Combines: resource listing (compute + storage), job submission, job monitor,
  projects & allocations
- `ui_config` title dynamically set to `'IRI — NERSC'` etc.

This means `iri.js` and `iri_info.js` are eventually replaced by a single
`iri_instance.js`.  The current `plugin_iri.py` and `plugin_iri_info.py` can
be kept temporarily as the static fallback until `iri_connect` is working,
then removed.

---

## 8. Files to Create / Modify

| File | Action | Notes |
|------|---------|-------|
| `src/radical/edge/plugin_host_base.py` | **Create** | `PluginHostBase` mixin |
| `src/radical/edge/bridge_plugin_host.py` | **Modify** | extend mixin, implement `_announce_topology` |
| `src/radical/edge/service.py` | **Modify** | use mixin, implement `_announce_topology`, handle `topology_update` WS message |
| `src/radical/edge/models.py` | **Modify** | add `topology_update` message type |
| `src/radical/edge/plugin_base.py` | **Verify/fix** | confirm `instance_name` used for routes (not class `plugin_name`) |
| `src/radical/edge/plugin_iri_connect.py` | **Create** | bridge-only connector plugin |
| `src/radical/edge/plugin_iri_instance.py` | **Create** | merged per-endpoint plugin |
| `src/radical/edge/data/plugins/iri_connect.js` | **Create** | endpoint table UI |
| `src/radical/edge/data/plugins/iri_instance.js` | **Create** | merged resource+job UI |
| `src/radical/edge/__init__.py` | **Modify** | add new plugin imports |
| `tests/unittests/test_plugin_host_base.py` | **Create** | unit tests for mixin |
| `tests/unittests/test_plugin_iri_connect.py` | **Create** | unit tests |
| (later) `plugin_iri.py`, `plugin_iri_info.py` | **Remove** | once iri_connect works |
| (later) `iri.js`, `iri_info.js` | **Remove** | once iri_instance works |

---

## 9. Implementation Order

1. **`plugin_host_base.py`** — the mixin; write tests first
2. **Migrate `BridgePluginHost`** to extend it; run existing tests
3. **Migrate `EdgeService`** to use it as mixin; add `topology_update` WS
   handling; run existing tests
4. **`plugin_base.py` audit** — confirm instance_name routing (low risk, quick)
5. **`plugin_iri_instance.py`** — merged endpoint plugin; tests
6. **`plugin_iri_connect.py`** — connector plugin using
   `register_dynamic_plugin`; tests
7. **JS**: `iri_connect.js` (endpoint table), `iri_instance.js` (merged page)
8. **Remove old** `plugin_iri.py`, `plugin_iri_info.py`, `iri.js`, `iri_info.js`

Steps 1–4 are pure refactor with no user-visible change.
Steps 5–7 are the new functionality.
Step 8 is cleanup.

---

## 10. Open Questions

- **Same endpoint twice?** If user connects NERSC a second time (e.g. with a
  new token), overwrite the existing `iri.nersc` instance or error?
  Recommendation: overwrite (deregister old, register new).

- **`iri_connect` in the tree?** It's a bridge plugin, so it will appear under
  the bridge node in the Explorer tree alongside `iri.nersc` etc.  That's
  fine — it's the management entry point.

- **Deregistration UI?** A disconnect button on the `iri.nersc` page itself,
  or only via `iri_connect`?  Probably both: the instance page has a
  disconnect button that calls back to `iri_connect`'s disconnect endpoint.

- **Token persistence across bridge restart?** Not supported (tokens are
  in-memory only, by design).  After a bridge restart the user re-connects
  via `iri_connect`.  The active endpoint table in the UI can be pre-populated
  from `localStorage` tokens, making this a single click.

- **Edge-side use case?** The same mechanism enables dynamic edge plugins
  (e.g. a user-defined executor that registers itself at runtime).  The
  `topology_update` WS message is the only extra piece needed.  Not needed
  for `iri_connect` but worth keeping the design compatible.

---

## 11. Current State of Relevant Files

As of this session:

- `plugin_iri.py` — complete, bridge-only, job submission
- `plugin_iri_info.py` — complete, bridge-only, resource info
- `iri_info.js` — has new endpoint table UI (radio | name | url | set/✔),
  token popup with Connect button, per-endpoint `_sessions` map;
  Incidents section removed
- `iri.js` — has old-style single-endpoint Connection card (not yet updated
  to the new table pattern)
- `plugin_host_base.py` — does not exist yet
- `plugin_iri_connect.py` — does not exist yet
- `plugin_iri_instance.py` — does not exist yet
