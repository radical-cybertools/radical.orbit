# Plan: #105 — Replace plugin-scoped env vars with per-plugin options

Issue: https://github.com/radical-cybertools/radical.orbit/issues/105
Status check (2026-08-25): **still open** — all three env vars are live in the
tree (`plugin_psij.py:73` `RADICAL_ORBIT_PSIJ_KEEP_FILES` read at module
import; `plugin_globus.py:87` `RADICAL_ORBIT_GLOBUS_COLLECTION`;
`plugin_rhapsody.py:1456` `RADICAL_ORBIT_RHAPSODY_BACKEND`), and there is no
per-plugin config mechanism. Note: PR #108 has since added another env knob
(`plugin_rhapsody` notify batch window, "endpoint-level env knob") — fold it
into the same migration.

## Design

### 1. Config plumbing (the real work)

**API shape** — `EndpointRuntime(plugins=...)` (and the `Broker` /
`EmbeddedBroker` plugin filter) accepts a mixed list:

```python
EndpointRuntime(plugins=['psij', {'rhapsody': {'backend': 'concurrent'}}])
# or the all-dict form:
EndpointRuntime(plugins={'rhapsody': {'backend': 'concurrent'}, 'psij': {}})
```

Internally normalize to `Dict[name_token, dict]` early (name tokens keep
wildcard/`all`/`default` semantics; options attach only to exact names —
attaching options to a wildcard/special token is a `ValueError`).

**CLI shape** — extend `--plugins` on both bins:

```
--plugins 'rhapsody:backend=concurrent:notify_batch_bytes=65536,psij:keep_files=1'
```

i.e. `name[:key=value]*` per comma-separated token. Values parsed as str;
plugins coerce. (Alternative considered: a separate `--plugin-opt name.key=val`
flag — more verbose but quote-free; decide at review. The inline form matches
the issue text.)

**Plumbing path** — `plugin_filter` currently flows as `List[str]` through
`PluginHostBase._load_plugins_from_filter` (plugin_host_base.py:159) from
`runtime.py:219` (endpoint) and `broker_plugin_host.py:51` (broker).
Change:

- `_load_plugins_from_filter(self, plugin_filter, plugin_options=None)` —
  keep the name-token list for expansion/resolution (unchanged logic), add a
  parallel `Dict[str, dict]` of options keyed by resolved plugin name.
- Instantiation becomes `pcls(app=self._app, options=opts.get(pname, {}))`.
- `Plugin.__init__` (plugin_base.py:189) grows `options: Optional[dict] = None`,
  stored as `self._options` (default `{}`). Base class validates nothing;
  plugins read their own keys and raise `ValueError` on unknown keys (typo
  protection — cheap and worth it).
- `register_dynamic_plugin` forwards `options` too (kwargs already pass
  through).

### 2. Migrate the four knobs

| env var | plugin option | notes |
|---|---|---|
| `RADICAL_ORBIT_RHAPSODY_BACKEND` | `rhapsody: backend` | operator-side default when client's `register_session` names none; per-session `backends` stays and wins |
| `RADICAL_ORBIT_PSIJ_KEEP_FILES` | `psij: keep_files` (bool) | **must stop being read at module import** (plugin_psij.py:73) — move `_KEEP_PSIJ_FILES` to an instance attr; sessions get it injected like other session config |
| `RADICAL_ORBIT_GLOBUS_COLLECTION` | `globus: local_collection` | keep `~/.radical/orbit/globus.json` read-fallback and per-session `local_collection` override; precedence: session override > plugin option > json file |
| rhapsody notify batch window (PR #108's env knob) | `rhapsody: notify_batch_bytes` | same operator-side character; migrate while there |

### 3. Remove the env vars

Once options exist and docs/tests updated: delete env reads, update
CLAUDE.md + docs/source (tutorial_plugin.rst gets a short "plugin options"
section — the tutorial math plugin can demo one option).

## Ordering / commits

1. Plumbing: options dict from API+CLI → `Plugin.__init__` (+ unit tests for
   the CLI parser and the normalize step; no behavior change yet).
2. Migrate rhapsody backend + psij keep_files (+ tests: option respected,
   module-import read gone).
3. Migrate globus local_collection + rhapsody notify window; remove all env
   reads; docs sweep.

## Tests

- Parser: `'rhapsody:backend=concurrent,psij'` → names + options; error on
  `'iri*:x=1'`.
- `_load_plugins_from_filter` passes options through; unknown plugin option
  raises at load, is caught by the existing per-plugin try/except, and does
  not kill the host.
- psij: `keep_files` honored per-instance (no module-level state).
- rhapsody: option is the default, `register_session(backends=...)` overrides.
- globus: precedence chain (session > option > json).

## Risks / notes

- Grep for the env vars in examples/, docs/, wrapper scripts, HPC configs
  before removal (e.g. `examples/amsc.py`, endpoint wrapper) — the operator
  side sets these on real machines today; the CLI syntax must be reachable
  from `submit_tunneled`-spawned children's argv (check how child endpoints
  get `--plugins` today).
- `task_dispatcher` writes `RADICAL_ORBIT_RHAPSODY_BACKEND` into child env
  records (plugin_task_dispatcher.py:1497) — that write path must switch to
  the new CLI option syntax for the child endpoint.
