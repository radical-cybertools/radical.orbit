# Plan: #11 — Plugin manager hardening (load order, error isolation, reload)

Issue: https://github.com/radical-cybertools/radical.orbit/issues/11
Status check (2026-08-25): **partial** — much of the original ask has been
built since (Q1 issue, predates the star-architecture flip):

Already there (`plugin_host_base.py`):
- Entry-point discovery with per-entry try/except isolation (line 114).
- Per-plugin instantiation isolation: `__init__` raising is caught and
  logged, remaining plugins load (lines 174–188).
- Deterministic load order (requested order, dedup-preserving,
  `_resolve_plugin_names`).
- Dynamic register/deregister with full teardown + route stripping
  (`register_dynamic_plugin` / `deregister_dynamic_plugin`, lines 194–262).
- Runtime-path isolation: `BrokerPluginHost.handle_request` /
  `on_topology_change` / `shutdown` wrap per-plugin calls.

Real remaining gaps:

## Gap 1 — a bad plugin *name* kills the whole host

`_resolve_plugin_names` raises `ValueError` on any unmatched token
(plugin_host_base.py:101) before anything loads; both bins catch it and
`sys.exit(1)`. So a typo or a not-installed optional plugin in `--plugins`
aborts the endpoint/broker — the one non-isolated load path.

Decision to make: is fail-fast on a typo actually *desired*? Proposal —
split the cases:
- Unmatched **exact name**: keep the hard error (a typo should not silently
  degrade; an operator asking for `psij` and getting no psij is worse).
- Unmatched **wildcard**: already required non-empty; relax to
  WARNING + skip (a glob is inherently "whatever is installed", the
  `default` set already skips missing entries silently).
- Add `--plugins-lenient` / `plugins_strict=False` escape hatch if a
  degraded-but-up endpoint is wanted (useful for `submit_tunneled` children
  where a crash costs an allocation). Default stays strict.

## Gap 2 — reload semantics (dev-only acceptable per issue)

Define reload as deregister + re-register of the *same* plugin name,
without re-importing modules (module-level state caveat documented):

- `PluginHostBase.reload_plugin(name)` = `deregister_dynamic_plugin(name)`
  then instantiate the registered class again (fresh instance, fresh
  sessions), single topology announce at the end.
- Statically-loaded plugins qualify too (they live in the same `_plugins`
  dict); the only extra care is plugins with background prefetch
  (sysinfo/queue_info) — their `shutdown()` must already cancel those
  (verify; fix if not).
- Explicitly document: this does NOT pick up code changes (no module
  re-import — `importlib.reload` interacts badly with
  `__init_subclass__` registration and is out of scope; dev iteration is
  restart-the-endpoint).
- Optional dev convenience: expose reload over the admin surface
  (gateway `POST /plugin/reload/{name}`) — decide at review whether the
  attack/complexity surface is worth it; core API first.

## Gap 3 — missing tests (cheap, do regardless)

- Plugin `__init__` raises → other plugins still load (the untested
  log.exception branch at plugin_host_base.py:187).
- Entry point raising on `ep.load()` → discovery continues.
- Deregister-then-register same name → clean instance, no stale
  direct-dispatch routes (route stripping at lines 244–259 is untested).
- Load-order assertion at host level (requested order == `_plugins` order).
- New: the Gap-1 lenient/strict behaviors; the Gap-2 reload lifecycle
  (sessions closed, routes fresh, topology announced once).

## Ordering

1. Gap 3 tests for existing behavior (pins the contract, no code change).
2. Gap 1 (small, decide the strictness matrix in review).
3. Gap 2 reload (medium; document the no-reimport limitation in
   docs/source/tutorial_plugin.rst).

## Effort

Small-medium overall; each gap is independently landable. The issue's
original scope is otherwise complete — after Gaps 1+3 land this could be
closed with the reload part split into its own follow-up if it stalls.
