# Dynamic Plugin Registration — Implementation Plan

Based on design doc: `dynamic_plugin_registration.md`

Two passes: Pass 1 is pure refactor (no user-visible change), Pass 2 adds new functionality.

---

## Pass 1: Refactor (Steps 1–4)

All existing tests (231+) must stay green throughout.

### Step 1: Create `plugin_host_base.py`

**New file:** `src/radical/edge/plugin_host_base.py`

Two module-level utility functions (moved/extracted):

- `_resolve_plugin_names(requested, available)` — moved from `service.py:80-115`.
  Currently imported by `bridge_plugin_host.py` from `service.py`; after the move
  both hosts import from the new module.
- `_discover_entry_points()` — extracted from the identical entry-point-discovery
  blocks in `BridgePluginHost._load_plugins()` (lines 54-64) and
  `EdgeService._load_plugins()` (lines 186-197).

**`PluginHostBase` mixin** — assumes `self._app` (FastAPI) and `self._plugins`
(dict) exist on the instance:

| Method | Description |
|--------|-------------|
| `_load_plugins_from_filter(plugin_filter)` | Discover entry points, resolve names, instantiate enabled plugins, store in `self._plugins`. |
| `async register_dynamic_plugin(cls, instance_name, **kwargs)` | Error if `instance_name` already exists. Instantiate `cls(app=self._app, instance_name=instance_name, **kwargs)`, store, call `_announce_topology()`. |
| `async deregister_dynamic_plugin(instance_name)` | Pop from `self._plugins`, close plugin, call `_announce_topology()`. |
| `async _announce_topology()` | Abstract — `raise NotImplementedError`. |

**Tests:** `tests/unittests/test_plugin_host_base.py`

- `_resolve_plugin_names` — exact, prefix, ambiguous, 'all' cases.
- `_discover_entry_points` — smoke test (no-op in test env).
- `_load_plugins_from_filter` — mock plugin class.
- `register_dynamic_plugin` — instance added, `_announce_topology` called, duplicate rejected.
- `deregister_dynamic_plugin` — instance removed, `close()` called, `_announce_topology` called.

### Step 2: Migrate `BridgePluginHost`

**Modify:** `src/radical/edge/bridge_plugin_host.py`

- `class BridgePluginHost(PluginHostBase):`
- Replace `__init__` inline `self._plugins = {}` + `self._load_plugins()` with
  `self._load_plugins_from_filter(plugin_names)`.
- Delete `_load_plugins()` method (now inherited).
- Implement `_announce_topology()` → `await self._broadcast_fn('topology', ...)`.
- Update import: `from .plugin_host_base import PluginHostBase, _resolve_plugin_names`.

### Step 3: Migrate `EdgeService`

**Modify:** `src/radical/edge/service.py`

- `class EdgeService(PluginHostBase):`
- Replace `__init__` inline `self._plugins = {}` + `self._load_plugins()` with
  `self._load_plugins_from_filter(self._plugin_filter)`.
- Delete `_load_plugins()` method (now inherited).
- Move `_resolve_plugin_names` out (now in `plugin_host_base.py`). Keep `RequestShim`.
- Implement `_announce_topology()` → send `topology_update` WS message to bridge.

**Modify:** `src/radical/edge/models.py`

- Add `TopologyUpdateMessage` (edge → bridge):
  `type: Literal["topology_update"]`, `plugins: Dict[str, Dict[str, Any]]`.
- Add to `_EDGE_MSG_TYPES` and `EdgeToBridgeMessage` union.
- Bridge-side handling deferred to Pass 2.

### Step 4: Audit `plugin_base.py`

Already verified — **no changes needed**:
- `Plugin.__init__(app, instance_name)` uses `instance_name` for namespace/routes.
- `get_ui_config` returns both `plugin_name` (class) and `instance_name` (instance).

---

## Pass 2: New Functionality (Steps 5–8)

### Step 5: Create `plugin_iri_instance.py`

**New file:** `src/radical/edge/plugin_iri_instance.py`

Merged per-endpoint plugin replacing both `plugin_iri.py` and `plugin_iri_info.py`.

- Takes `endpoint` and `token` at construction time (not at session time).
- Single internal session per instance (one endpoint = one session).
- Combines: resource listing (compute + storage), job submission, job monitoring,
  incidents, projects, allocations.
- Constructor: `__init__(self, app, instance_name, endpoint, token)`.
- `ui_config` title dynamically set: `'IRI — NERSC'`, `'IRI — OLCF'`, etc.
- Session class merges `IRISession` + `IRIInfoSession` logic.
- Client class merges `IRIClient` + `IRIInfoClient`.
- Notification topic: `job_status` (same as current `plugin_iri.py`).

**Tests:** `tests/unittests/test_plugin_iri_instance.py`

### Step 6: Create `plugin_iri_connect.py`

**New file:** `src/radical/edge/plugin_iri_connect.py`

Bridge-only connector plugin — the endpoint configurator.

- `plugin_name = 'iri_connect'`
- `is_enabled` → bridge only
- Routes:
  - `POST connect` — accepts `{endpoint, token}`, validates, calls
    `self._host.register_dynamic_plugin(PluginIRIInstance, 'iri.' + ep, endpoint=ep, token=token)`.
    Rejects if `'iri.' + ep` already registered (10.1: require disconnect first).
  - `POST disconnect/{instance_name}` — calls `self._host.deregister_dynamic_plugin(instance_name)`.
  - `GET status` — returns list of active `iri.*` instances.
- Needs reference to the `BridgePluginHost` (its plugin host) to call
  `register_dynamic_plugin`. Available via `self._app.state.edge_service`
  (already set by both hosts).

**Tests:** `tests/unittests/test_plugin_iri_connect.py`

### Step 7: JS UI files

**New file:** `src/radical/edge/data/plugins/iri_connect.js`

- Endpoint table: radio | name | URL | token set/checkmark
- Token input popup (reuse pattern from `iri_info.js`)
- Connect button → `POST /{bridge}/iri_connect/connect`
- On success: topology SSE event arrives, new `iri.nersc` node appears in
  Explorer tree automatically
- Pre-populate token from `localStorage['iri_tokens']` (10.4)
- Disconnect button per active endpoint

**New file:** `src/radical/edge/data/plugins/iri_instance.js`

- Single page combining resource listing + job submission + monitoring
- Resources section (compute + storage)
- Job submission form
- Job list with status polling via SSE notifications
- Projects & allocations viewer
- Disconnect button (calls back to `iri_connect` disconnect endpoint) (10.3)

### Step 8: Remove old IRI plugins

- Delete `plugin_iri.py`, `plugin_iri_info.py`
- Delete `iri.js`, `iri_info.js`
- Update `__init__.py`: remove `PluginIRI`, `PluginIRIInfo` imports; add
  `PluginIRIConnect`, `PluginIRIInstance`.
- Update existing tests or remove/replace as needed.

---

## Decisions (from design doc Section 10)

| # | Question | Decision |
|---|----------|----------|
| 10.1 | Same endpoint twice? | Reject — require disconnect first |
| 10.2 | `iri_connect` in the tree? | Yes, appears under bridge node |
| 10.3 | Deregistration UI? | Both: button on `iri_connect` page AND on instance page |
| 10.4 | Token persistence? | No server-side persistence. Client-side `localStorage` pre-populates tokens for one-click reconnect after bridge restart |
| 10.5 | Edge-side dynamic plugins? | Keep design compatible — `PluginHostBase` mixin works for both hosts, `TopologyUpdateMessage` exists |
