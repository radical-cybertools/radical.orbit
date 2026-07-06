# Review — plugin framework + shared infrastructure (KISS / student readability)

**Scope:** `plugin_base.py`, `plugin_session_base.py`, `plugin_host_base.py`,
`ui_schema.py`, `utils.py`, `logging_config.py`, `exceptions.py`,
`http_utils.py`, `_prof.py`, `__init__.py` (plus read-only checks of
`dispatch.py`, `broker_plugin_host.py`, `runtime.py`, `broker.py`,
`plugin_sysinfo.py` to verify how the framework is actually consumed).

## Overall impression

The *authoring recipe* for a new plugin is genuinely small — subclass `Plugin`,
set `plugin_name`/`session_class`, call `add_route_get/post`, use `_forward`
(see `plugin_sysinfo.py:568-649`, ~80 lines) — and `plugin_session_base.py`,
`http_utils.py`, `dispatch.py`, and `_prof.py` are exemplary in size and
clarity. But the base class a student inherits is 925 lines, roughly half of
which is session-lifetime/ownership machinery (three lifetimes × owner
tracking × a reserved `default` session × reclaim-drain timers × an idle
cleanup loop) spread over ~15 private methods and three parallel dicts. Worse
for comprehension: every route is registered **twice** — once into FastAPI and
once into a hand-rolled regex table — and only the hand-rolled table carries
production traffic; the FastAPI half exists for tests, yet it is what a student
sees first and it drags `fastapi`/`starlette` types through the whole plugin
API. Around this core sit several pieces of unused or decorative
infrastructure (`exceptions.py`, the correlation-ID machinery, the Pydantic UI
schema's validator) that mislead a reader about what the real contracts are.

---

## Finding 1 — Dual route registration: a FastAPI layer that production traffic never uses

- **Location:** `plugin_base.py:257-301` (`add_route_post/get`,
  `_wrap_handler`, `_register_direct`); consumed only via
  `dispatch.py:82-97` + `broker_plugin_host.py:109-135` +
  `runtime.py:652` in production.
- **What & why it hurts:** Every plugin route is registered twice: (a) into
  the FastAPI app via `self._app.add_route(...)` with a `_wrap_handler`
  JSONResponse shim, and (b) into a hand-rolled `(method, regex, params,
  handler)` table (`app.state.direct_routes`) with its own `{param}`→regex
  compiler. I verified that **no production request ever traverses path (a)**:
  the endpoint runtime never runs an HTTP server (no uvicorn in
  `runtime.py`), and the broker's plugin host builds a private
  `FastAPI(title='Broker Plugin Host')` (`broker_plugin_host.py:42`) that is
  never served — gateway traffic reaches plugins via
  `broker.py:797 → handle_request → match_route → RequestShim`. The FastAPI
  registration exists solely so unit tests can use `TestClient` (60 usages
  across 8 test files; `_wrap_handler`'s own docstring says so). The cost to a
  student is large: two request objects (`starlette.Request` vs
  `dispatch.RequestShim`), two response conventions (plain dict vs
  `JSONResponse`, reconciled by `hasattr(result, 'status_code')` duck-typing
  in *two* places, `plugin_base.py:284` and `broker_plugin_host.py:133`), a
  `FastAPI` constructor parameter that is really just a `state` bag, and a
  hand-written regex router that re-implements what Starlette already does —
  all while the reader cannot tell which path is real. FastAPI's actual
  features (validation, DI, OpenAPI) are unused (`add_route` is raw Starlette).
- **Simpler alternative:** Keep exactly one dispatch path — the direct table
  (it is the simpler of the two and already carries all traffic). Drop
  `self._app.add_route(...)` and `_wrap_handler`; replace the `app: FastAPI`
  parameter with a small plain `HostContext` object (`.state`,
  `.direct_routes`) so plugin authors need no FastAPI at all. Point tests at
  `handle_request`/`match_route` (they already exercise the production path in
  `broker_plugin_host` tests) or at a ~20-line test harness that wraps the
  direct table. `HTTPException` can stay (it is a fine status-code carrier) or
  become a two-field local class.
- **Essential?** No. The "uniform message contract" requirement needs one
  dispatch path; the second one is test scaffolding that leaked into the
  production API surface.
- **Severity:** high
- **Effort:** large

## Finding 2 — The session lifetime/owner model dominates the base class every plugin author must read

- **Location:** `plugin_base.py:348-914` — `register_session` (50-line
  docstring), `_normalize_session_policy`, `_open_session`,
  `_record_session`, `_check_owner`, `_check_policy_conflict`,
  `_ensure_default_session`, `_session_expired`,
  `_cleanup_expired_sessions`, `_cleanup_loop`, `_owner_has_ephemeral`,
  `_arm_reclaim_drain`, `_cancel_reclaim_drain`, `_reclaim_drain`,
  `_reclaim_owner_sessions`, `shutdown`.
- **What & why it hurts:** ~450 of 925 lines implement: three lifetime
  policies (`ephemeral`/`ttl`/`persistent`), owner-bound vs owner-less
  ephemeral semantics (two *different* expiry mechanisms for the same
  lifetime value — liveness-driven drain timers vs idle timeout, chosen at
  `plugin_base.py:829-834`), a reserved `default` session special-cased in
  four separate places (`_normalize_session_policy:450`,
  `_open_session:490`, `_ensure_default_session:562`, `_forward:776`),
  per-owner `asyncio` drain timers, and a lazily-started 5-second cleanup
  loop. A student who only wants to add a route must scroll past all of it;
  a student debugging "why did my session disappear" must hold five distinct
  expiry rules in their head. Some of this state is also duplicated in
  English three times over (class docstring, `register_session` docstring,
  per-method docstrings), which makes the file longer without making the
  rules easier to find.
- **Simpler alternative:** The *behavior* is largely required (see below),
  but the same rules fit in far less machinery: (1) one `SessionRecord`
  dataclass (`session, lifetime, ttl, owner, last_access`) in a single dict
  (see Finding 6); (2) one `_expiry_deadline(record) -> float|None` function
  replacing the branchy `_session_expired`, with the owner-drain expressed
  as "on `lost`, stamp `drain_deadline = now + reclaim_drain` on the
  affected records; on `present`, clear it" — checked by the *existing*
  5-second cleanup loop instead of spawning one `asyncio.Task` per lost
  owner (`_drain_timers`, `_arm/_cancel/_reclaim_drain` all disappear; a
  ≤5 s reclaim slop is irrelevant against a 300 s drain); (3) make
  `default` a plain persistent record created in `__init__` (the plugin
  instance is constructed before any request, so on-demand creation, the
  lock, and three of the four special cases vanish); (4) state the policy
  table once, in one docstring, as a 5-row table.
- **Essential?** Mostly yes — this is use case 7 / the "long-lived resources
  reclaimed only when their owner truly goes away" requirement, and the
  suspect→lost→drain decoupling from plugin behavior is the liveness
  requirement. But the per-owner timer tasks, the four-way `default`
  special-casing, and the triple documentation are accidental; the same
  requirement is satisfiable with one record type, one sweep loop, and one
  deadline function.
- **Severity:** high
- **Effort:** medium

## Finding 3 — Two notification APIs, with a private cross-object call as the documented one, plus a silent-drop window

- **Location:** `plugin_base.py:317-346` (`_dispatch_notify`),
  `plugin_base.py:651-666` (`send_notification`),
  `plugin_session_base.py:36-59` (docs telling users to call
  `self._plugin._dispatch_notify`), stale comment at
  `plugin_xgfabric.py:1370` ("the base class injects the `_notify` callback"
  — no such attribute exists).
- **What & why it hurts:** A plugin author learns *two* ways to notify:
  plugins `await self.send_notification(...)`, sessions call
  `self._plugin._dispatch_notify(...)` — the officially documented pattern is
  a session reaching into another object's underscore-private method. Naming
  is dishonest twice over: a "private" method is the public session API, and
  a code comment references a `_notify` callback that was evidently renamed
  away. Two further subtleties bite debuggers: (a) `_dispatch_notify` caches
  `_main_loop` only on its first call *from an async context* — until then,
  notifications from background threads are **silently dropped** at debug
  level (`plugin_base.py:340-346`); (b) `send_notification` duck-types the
  host seam with `inspect.isawaitable(result)` (`plugin_base.py:660-664`)
  because the two hosts expose sync vs async `send_notification`.
- **Simpler alternative:** Give `PluginSession` a public
  `def notify(self, topic, data)` that forwards to the plugin (one
  documented call for everyone; `_dispatch_notify` becomes an internal
  detail). Have the host inject the loop at plugin construction (the host
  knows its loop) so the thread path never silently drops. Make both hosts'
  `send_notification` async (the runtime's can be an async one-liner around
  its sync handoff) and delete the `isawaitable` branch. Fix or delete the
  stale `_notify` comment.
- **Essential?** The push-event capability is required (use case 4); the
  dual API, the private-call convention, the drop window, and the sync/async
  duck-typing are all accidental.
- **Severity:** medium
- **Effort:** small

## Finding 4 — `exceptions.py` is dead code that misdocuments the real error contract (and shadows builtins)

- **Location:** `exceptions.py:1-153` (whole module); only importer outside
  the module is its own test, `tests/unittests/test_exceptions.py`.
- **What & why it hurts:** A 153-line exception taxonomy with error codes and
  an `exception_to_http_status` mapper is referenced by **zero** production
  code — plugins actually raise `fastapi.HTTPException` with literal status
  codes (`plugin_base.py:452,542,590,...`), and sessions raise plain
  `RuntimeError` (`plugin_session_base.py:103`) even though
  `SessionClosedError` exists here. A student who finds this module will
  reasonably assume it *is* the error contract and design against it. Two
  classes shadow Python builtins (`ConnectionError`, `TimeoutError`) — a
  latent `except ConnectionError` foot-gun the moment anyone does adopt them.
- **Simpler alternative:** Delete the module and its test (the real contract
  is "raise `HTTPException(status, detail)`" — document that one sentence in
  the `Plugin` docstring). If a typed hierarchy is genuinely wanted later,
  introduce it when the first production call site exists, and don't reuse
  builtin names.
- **Essential?** No requirement references it; the codebase demonstrably
  works without it.
- **Severity:** medium
- **Effort:** small

## Finding 5 — `logging_config.py`: global logging reconfigured at import time, a plugin name hardcoded in core infra, and dead correlation-ID machinery

- **Location:** `logging_config.py:164-166` (auto-configure on import, ending
  in `logging.basicConfig(force=True, ...)` at line 156);
  `logging_config.py:139` (`for name in ('radical.orbit', 'rhapsody')`);
  `logging_config.py:21-38` + `64-75` (correlation-ID contextvar — verified:
  `set_correlation_id` is never called anywhere in `src/` or `bin/`).
- **What & why it hurts:** (a) Importing the package as a *library* (e.g.
  `from radical.orbit import EndpointRuntime` in a user's application, which
  the client docs advertise) force-resets the host application's root logging
  configuration — invasive, and invisible until someone's own logs vanish.
  (b) `'rhapsody'` — a dependency of one plugin — is baked into the shared
  logging module; the brief's "core stays semantics-agnostic" rule is
  violated by core infra knowing a plugin's library by name. The defensive
  handler-protection dance (lines 129-156) is itself justified by third
  parties calling `basicConfig(force=True)`… which this very module also does
  to everyone else. (c) ~35 lines of correlation-ID contextvar + per-record
  formatting support serve no caller — speculative.
- **Simpler alternative:** Library side: `logging.getLogger('radical.orbit').addHandler(logging.NullHandler())`
  and nothing else at import; call `configure_logging()` explicitly from the
  `bin/` entry points (which already re-call it after argparse). Let the
  rhapsody *plugin* protect the `rhapsody` logger if it needs to. Delete the
  correlation-ID block until something sets one.
- **Essential?** Robust file logging under Dragon's `basicConfig` stomp is a
  real operational need (the comment names the observed failure) — but it
  needs to protect only this package's logger, be triggered from entry
  points, and doesn't justify the plugin-name hardcode or the unused
  contextvar.
- **Severity:** medium
- **Effort:** small

## Finding 6 — Three parallel session dicts and four hand-copied pop/close/log blocks; `deregister_dynamic_plugin` re-implements `shutdown` through privates

- **Location:** `plugin_base.py:179-195` (`_sessions`,
  `_session_last_access`, `_session_policy`); the pop-all-three + close +
  log block copied at `plugin_base.py:585-593`, `743-753`, `852-862`,
  `906-914`; `plugin_host_base.py:241-274` (`deregister_dynamic_plugin`).
- **What & why it hurts:** Session state is sharded across three dicts keyed
  by `sid`, justified only by a comment calling `_session_last_access` "the
  hot map" — a micro-optimization no measurement supports (dict field access
  vs dict lookup is noise next to a network hop). Every removal site must
  remember to pop all three; four near-identical blocks already exist, and
  the fifth site forgot: `deregister_dynamic_plugin` pops only
  `plugin._sessions` (`plugin_host_base.py:247`), leaving `_session_policy`
  and `_session_last_access` populated, and also skips the `_drain_timers`
  cancellation that `Plugin.shutdown` performs — because it re-implements
  teardown by reaching into `plugin._sessions` / `plugin._cleanup_task`
  privates instead of calling the public `await plugin.shutdown()` that does
  exactly this job (harmless today only because the plugin object is
  discarded, but it's a drift trap and a wrong example for readers).
- **Simpler alternative:** One `self._sessions: Dict[str, SessionRecord]`
  (record = session + lifetime + ttl + owner + last_access) and one
  `async def _drop_session(sid, reason)` used by unregister / reclaim /
  cleanup / shutdown. In `deregister_dynamic_plugin`, replace lines 246-257
  with `await plugin.shutdown()` and keep only the route-stripping (which is
  legitimately host-side).
- **Essential?** No — pure accidental duplication; the behavior is identical
  under the merged form.
- **Severity:** medium
- **Effort:** small

## Finding 7 — Plugin-selection mini-DSL plus two discovery mechanisms

- **Location:** `plugin_host_base.py:35-117`
  (`_expand_special_tokens`, `_resolve_plugin_names`),
  `plugin_host_base.py:120-135` (`_discover_entry_points`),
  `__init__.py:23-50` (hardcoded eager plugin imports).
- **What & why it hurts:** To predict which plugins load, a student must
  learn five token forms (`'all'`, `'default'`, exact, *prefix match with
  ambiguity errors* — `'sys'` → `'sysinfo'` — and fnmatch globs), plus
  role-dependent default sets whose entries may themselves be globs and are
  silently skipped when missing. That's a small configuration language for
  what is usually "load these plugins". Prefix matching in particular is CLI
  sugar that buys almost nothing and costs an extra error mode
  (`plugin_host_base.py:106-108`). Separately, the registry is populated by
  *two* mechanisms — every built-in plugin eagerly imported in
  `__init__.py`, and entry-point discovery for external ones — so "where did
  this plugin come from" has two answers, and `__init__.py`'s
  `try/except ImportError: pass` blocks (lines 32-50) silently hide a
  *broken* optional plugin (any transitive ImportError, not just a missing
  dependency, makes the plugin vanish without a log line).
- **Simpler alternative:** Support exact names + `'all'` + `'default'`; keep
  globs only if `'iri*'` in the default sets truly needs them (or list the
  iri names explicitly); drop prefix matching. Log a warning in the
  `except ImportError` blocks naming the plugin and the error. Longer term,
  route built-ins through the same entry-points mechanism so there is one
  discovery story.
- **Essential?** Some filtering is needed (different hosts load different
  plugin sets). The prefix-match form and the silent import swallow are not.
- **Severity:** medium
- **Effort:** medium

## Finding 8 — The Pydantic UI schema is decorative: production never validates, real plugins bypass it

- **Location:** `ui_schema.py:13-159`; escape hatch at
  `ui_config_to_dict:156-157` (`isinstance(config, dict): return config`);
  `validate_ui_config:162-178` is called only by
  `tests/unittests/test_ui_schema.py` (verified — no production caller).
- **What & why it hurts:** ~145 lines of `BaseModel`s with `Field`
  descriptions promise a validated schema, but the single production
  consumer (`ui_config_to_dict`, used by `get_ui_config` and
  `get_topology_info`) passes any dict through **untouched**, and the
  in-tree plugins define `ui_config` as raw dicts (e.g.
  `plugin_sysinfo.py:580-592`) — so in practice nothing is ever validated
  and the models' only production role is `model_dump` for the rare plugin
  that does use them. A student reading `ui_schema.py` infers a contract the
  system doesn't enforce; typos in a dict `ui_config` sail through exactly
  as they would without this module. The UI vocabulary (forms, monitors,
  layouts, button styles) is also a gateway/Explorer concern living in the
  core package.
- **Simpler alternative:** Either enforce it — one line in `get_ui_config`:
  `ui_config_to_dict(validate_ui_config(self.ui_config))` — or shrink to
  honesty: document `ui_config` as "an opaque dict the Explorer interprets;
  see the Explorer docs", keep `ui_config_to_dict`, and delete the unused
  models/validator. Given the brief's core-stays-agnostic rule, the schema
  (if kept) belongs beside the gateway/Explorer, not in the plugin core.
- **Essential?** The compat-ingress requirement needs plugins to *carry* a UI
  hint; it does not need an unenforced 145-line schema in the core.
- **Severity:** medium
- **Effort:** small

## Finding 9 — Legacy `BRIDGE` naming doubles the entire config surface in `utils.py`

- **Location:** `utils.py:46-71` (9 primary + 9 legacy constants),
  fallback logic threaded through `_env:74-76`, `_read_url_file:85-104`,
  `_read_token_file:326-346`, `_resolve_path_value:212-235`; near-duplicate
  functions `_read_url_file` / `_read_token_file` and
  `write_broker_url_file` / `write_broker_token_file`.
- **What & why it hurts:** Every env var and file has a shadow predecessor,
  and every resolver carries a silent-fallback branch — a student tracing
  "where does my token come from" must follow a 6-step precedence (CLI >
  env > legacy env > file > legacy file > generated) that the docstrings
  state in three different partial forms. The fallback shape is also
  hand-copied per file kind: `_read_url_file` and `_read_token_file` are the
  same function twice (one comment even says "consistent with
  `_read_url_file`" instead of sharing code), as are the two atomic-write
  helpers (differing in one `chmod` constant).
- **Simpler alternative:** One `_read_config_file(path, legacy) -> str|None`
  and one `_write_file_atomic(path, text, mode)` collapse four functions to
  two. For an academic project with a small deployment base, schedule the
  `BRIDGE` names for deletion (a one-release warning when a legacy source is
  used would ease the exit); if they must stay, at least confine the
  fallback to the two shared helpers so it is written once.
- **Essential?** Not architecturally — it is migration convenience. It can
  be kept far more cheaply than it currently is.
- **Severity:** medium
- **Effort:** small

## Finding 10 — Two plugin-constructor conventions; `instance_name` ceremony for the common case

- **Location:** `plugin_base.py:164` (`__init__(self, app, instance_name)`,
  required positional); `plugin_host_base.py:190` (static load calls
  `pcls(app=self._app)`); `plugin_host_base.py:225` (dynamic load calls
  `cls(app=..., instance_name=..., **kwargs)`); e.g.
  `plugin_sysinfo.py:594-598` (`super().__init__(app, 'sysinfo')` —
  restating `plugin_name` as a string literal).
- **What & why it hurts:** A new plugin author must know that a *statically
  loaded* plugin's `__init__` takes only `app` (or the host's
  `pcls(app=...)` call breaks), while a *dynamically registered* one must
  accept `instance_name` — two signatures for one base class, discoverable
  only by reading both host call sites. And in the overwhelmingly common
  single-instance case the subclass re-types its own `plugin_name` as a
  literal, which can silently drift from the class attribute (registry key
  ≠ URL namespace).
- **Simpler alternative:** `def __init__(self, app, instance_name=None)` in
  the base, defaulting to `type(self).plugin_name`. Then every plugin —
  static or dynamic — has the same signature, `super().__init__(app)`
  suffices, and the multi-instance case still works.
- **Essential?** Multi-instance support is needed (`iri.<endpoint>`); the
  dual convention is not.
- **Severity:** low (but it is the very first thing a plugin author touches)
- **Effort:** small

## Finding 11 — `_default_lock` guards a race that cannot occur, teaching the wrong concurrency model

- **Location:** `plugin_base.py:186-188, 562-580`
  (`_default_lock`, `_ensure_default_session`).
- **What & why it hurts:** The double-checked `asyncio.Lock` implies the
  check-then-create of the `default` session can interleave. It can't: all
  plugin dispatch for a given instance runs on one event loop, and between
  the `self._sessions.get(DEFAULT_SID)` check and `_record_session` there is
  no `await` (session construction is synchronous) — coroutines on one loop
  only interleave at `await` points. A student sees the lock and concludes
  either that plugins are called from multiple loops (false, and a scary
  false belief) or that asyncio needs locks around plain dict ops (also
  false). If Finding 2's suggestion (create `default` in `__init__`) is
  taken, both lock and method disappear anyway.
- **Simpler alternative:** Delete the lock; or better, pre-create the
  `default` session in `__init__` and delete `_ensure_default_session`
  entirely.
- **Essential?** No — no real race exists on a single loop.
- **Severity:** low
- **Effort:** small

## Finding 12 — Domain plugin names hardcoded in shared infrastructure (`DEFAULT_PLUGINS_BY_ROLE`)

- **Location:** `plugin_host_base.py:23-28`.
- **What & why it hurts:** The core host mixin knows the names `psij`,
  `rhapsody`, `queue_info`, `iri*`, `task_dispatcher` — domain semantics in
  the semantics-agnostic layer the brief says must not carry them. Adding a
  use case ("new plugins, not core changes") today means editing this core
  table to be part of any default set; and the broker/login/compute role
  taxonomy is welded to a specific HPC worldview inside the *generic* plugin
  host.
- **Simpler alternative:** Move the table to the `bin/` entry points (which
  own deployment policy anyway) and pass the resolved list in, or let each
  plugin class declare `default_roles = ('login', 'compute')` and have the
  host derive the set from the registry — then a new plugin joins the
  defaults by declaring itself, with zero core edits.
- **Essential?** Sensible defaults are needed; their *location* in the core
  is not.
- **Severity:** low
- **Effort:** small

## Finding 13 — Assorted small swallows and dead weight

- **Location & items:**
  - `plugin_base.py:401-406` — a malformed `register_session` body
    (`await request.json()` raising) is silently coerced to `{}` and treated
    as "default policy" instead of a 400; a client typo becomes an
    ephemeral session with no error.
  - `plugin_base.py:217-240` — four role properties, each doing a local
    `from .utils import host_role` and a full `host_role(app)` dict build
    per access; one cached `self._role` (or a single `role` property
    returning the string) would read straighter. Cost is minor
    (`detect_batch_system` is cached) but the quadruplicated import/property
    ceremony is pure boilerplate.
  - `plugin_session_base.py:85-93` — `close()` returns `{}` "indicating
    successful closure"; the value is never used and the docstring dignifies
    a meaningless return. Return `None`.
  - `__init__.py:17` — `Broker = Broker` is a no-op self-assignment kept for
    a comment about a future rename; either alias a distinct name or drop
    the line.
- **What & why it hurts:** Each is small, but together they add noise and
  (for the JSON swallow) hide user errors in the framework's front door.
- **Simpler alternative:** As listed per item.
- **Essential?** No.
- **Severity:** low
- **Effort:** small

---

## What is right-sized (for balance)

`plugin_session_base.py` (105 lines), `dispatch.py` (RequestShim +
`match_route`, ~100 lines), `http_utils.py` (a real NAT-keepalive problem
solved in 50 lines with an honest docstring and idempotent-only retry), and
`_prof.py` (8-line optional-dependency stub) are each exactly as big as their
job. The direct-dispatch design itself — plain regex table, plain handler
call — is the *simple* half of Finding 1 and worth keeping as the only half.
