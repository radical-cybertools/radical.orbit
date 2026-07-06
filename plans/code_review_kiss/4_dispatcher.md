# Review 4 — Task dispatcher subsystem + event-replay plugin

## Overall impression

This is by far the heaviest subsystem in the codebase: ~3,400 lines across six
dispatcher modules for what is, functionally, "submit a batch job, wait for the
child endpoint, forward tasks to it, record completions". The dispatcher is a
full workflow manager — pools, pilot fleets, a pluggable autoscaling-strategy
framework with entry points, a hand-rolled WAL+snapshot database, four
background sweeper loops, and a second parallel dispatch mode — living *inside
the broker* through privileged seams that no ordinary plugin gets. Much of this
is speculative generality serving a "research surface" that currently has one
real strategy; a student tracing "how does a task get dispatched" must cross
seven layers of indirection and two scheduling idioms. The replay plugin, by
contrast, is close to right-sized: bounded buffers with honest eviction
accounting — its one over-build is the server-side cursor store. Both files
also carry visible mechanical-rename damage ("lendpointr", "wendpoint") and
docstrings that contradict the code.

---

## Finding 1 — The dispatcher is a workflow manager embedded in the broker, not really "a plugin"

- **Location**: `src/radical/orbit/plugin_task_dispatcher.py:441-532` (class
  `PluginTaskDispatcher`, esp. `__init__` lines 482-483 taking
  `app.state.broker_caller` / `app.state.broker_tap`), `is_enabled` at 456-467;
  wiring confirmed in `broker_plugin_host.py:50-51`, `broker.py:453`.
- **What & why it hurts**: The plugin only functions when handed two privileged
  broker-internal seams — the in-process routing caller and the raw event tap —
  that exist for it (and replay) alone. It is broker-resident by fiat
  (`is_enabled` → broker only), so all pool/pilot/task state, four background
  loops, and a durable store now live in the hub process, coupling broker
  restarts to workflow state (hence the whole `_replay_state` recovery
  machinery, Finding 3). The brief's core requirement is that the hub stays
  small and semantics-agnostic and that domain semantics arrive as plugins *at
  the edges*; use case 1 explicitly models a workflow manager as a
  *participant*. Everything this plugin does through `CallerHTTP` +
  `_caller_client` (lines 719-737) is re-plumbing so it can drive the same
  public `PSIJClient`/`RhapsodyClient` API that any external consumer already
  gets for free via `EndpointRuntime.get_plugin(...)` plus
  `register_callback(...)` for completions.
- **Simpler alternative**: Make the dispatcher an ordinary participant (or even
  a standalone consumer application shipped under `examples/`): an
  `EndpointRuntime` that calls `psij`/`rhapsody` plugin clients and subscribes
  to `task_status` notifications. The `broker_caller`/`CallerHTTP` seam, the
  raw-tap subscription, `_on_event` filtering, and the broker-host coupling all
  disappear; the broker sheds its largest resident. If broker co-location is
  kept for latency, at minimum route through the same subscribe/notification
  path other consumers use instead of the unfiltered raw tap.
- **Essential?**: The *capability* (elastic pilot pools) serves use cases 1-2.
  Its residency in the hub and its use of privileged seams are not required by
  any listed requirement — the star model was designed so exactly this kind of
  logic could live at an edge.
- **Severity**: high
- **Effort**: large

## Finding 2 — Pluggable-strategy framework (ABC + 8-callable context + triple loader) for one real strategy

- **Location**: `task_dispatcher_strategy.py` (whole file, 345 lines):
  `StrategyContext.__init__` 57-80 (eight injected callables),
  `load_strategy` 271-317 (dotted-path + builtin registry + entry-point group);
  builtin registry `_builtin_strategies` 253-268 duplicated a third time as
  entry points in `setup.py:241-247`; consumed at
  `plugin_task_dispatcher.py:155-170` where `PoolState` wraps itself in eight
  lambdas.
- **What & why it hurts**: There is exactly one production strategy
  (`conservative`); the second exists "to demonstrate the research-facing ABC"
  (`task_dispatcher_strategy_examples.py:4-8`). To support this, the code
  ships: an ABC with six hooks, a `StrategyContext` that re-exposes `PoolState`
  through eight constructor-injected closures, and a three-mechanism resolver
  in which the two built-ins are registered **three different ways** (builtin
  dict, `setup.py` entry points, and reachable by dotted path). A student
  asking "which code decides when a pilot is submitted?" must first understand
  a plugin-loading framework *inside* a plugin — a second-level plugin system
  the architecture never asks for.
- **Simpler alternative**: Pass `PoolState` (or a tiny read-only snapshot
  dataclass) to the strategy directly — the eight lambdas become attribute
  access. Collapse resolution to: `if ':' in spec: import module:Class;
  else: BUILTINS[spec]` — delete the entry-point group from both
  `load_strategy` and `setup.py` (third-party strategies can always use the
  dotted form). That removes ~150 lines and one of the three registration
  sites. If only one strategy is realistically used, go further: make the
  strategy three plain methods on the plugin and reintroduce pluggability when
  a second real policy exists.
- **Essential?**: No requirement in the brief calls for pluggable autoscaling
  policies. The docstring's justification ("keeps the research surface
  independent of plumbing") is a project-internal goal; even granting it, the
  dotted-path import alone satisfies it.
- **Severity**: high
- **Effort**: medium

## Finding 3 — Hand-rolled WAL database: append log + snapshot + two-trigger compaction + O_APPEND-truncate trick

- **Location**: `task_dispatcher_state.py:155-336` (`StateLog`), esp. the
  persistent-handle/truncate subtlety at 184-198 and 279-287; consumed by
  `_compaction_sweeper` (`plugin_task_dispatcher.py:814-840`), compaction
  constants 91-93, and `PoolState.compact_logs` 267-282.
- **What & why it hurts**: To persist two small dicts per pool (a handful of
  pilots, at most thousands of task records), the code implements a miniature
  LSM store: append-only JSONL, last-write-wins replay, atomic snapshot files,
  a size-AND-age compaction policy, a dedicated 5-minute sweeper loop, and a
  genuinely subtle invariant — truncating *through* a still-open `O_APPEND`
  handle and relying on POSIX append semantics to land the next write at the
  new EOF (documented across three comment blocks because it needs them). A
  student debugging state recovery must understand snapshot-overlay ordering,
  compaction triggers, and file-handle lifetime — for data that would fit in
  one JSON file per pool.
- **Simpler alternative**: One `state.json` per pool, rewritten atomically
  (tempfile + `os.replace` — the code already has this exact routine in
  `snapshot()`) on every mutation, or debounced to once a second. Replay is
  `json.load`. That deletes the append log, `needs_compaction`, the truncate
  trick, the compaction sweeper, and two of the three tuning constants — ~200
  lines and an entire failure mode. At this write rate (per task transition)
  the full rewrite is microseconds.
- **Essential?**: Durability across restart is real (use case 7). The WAL
  *shape* is not — it is an optimization for write volumes this plugin will
  never see.
- **Severity**: high
- **Effort**: medium

## Finding 4 — "Endpoint mode": a second dispatch path with its own durable ledger threaded through five routes

- **Location**: `plugin_task_dispatcher.py:498-510` (`_endpoint_mode_log` +
  `_endpoint_mode_tasks`), `_route_submit_endpoint_mode` 1065-1132, special
  cases in `_route_get_task` 1141-1155, `_route_cancel_task` 1171-1185,
  `_route_stage_in` 1213-1217, `_route_stage_out` 1267-1271, terminal handling
  1839-1853, compaction 827-835; record type `EndpointModeRecord`
  (`task_dispatcher_state.py:132-148`).
- **What & why it hurts**: `submit_task(endpoint=...)` bypasses everything the
  dispatcher exists for (pools, pilots, strategy, state) and becomes a
  transparent proxy to the target endpoint's rhapsody — "no pool, no state log,
  no pilot fleet", per its own docstring. Yet it drags in: a dedicated
  persisted ledger with tombstone records, its own replay filter, its own
  compaction branch, an if-branch at the top of five separate routes, and
  re-emitted notifications under the dispatcher's name. Any consumer can
  already reach that rhapsody directly (`rt.get_plugin(endpoint, 'rhapsody')`)
  — the mode adds a hop and a restart-recovery story to a call path that did
  not need either.
- **Simpler alternative**: Delete the mode; document "for direct execution,
  talk to the endpoint's rhapsody plugin". If a broker-side alias is truly
  wanted, it should be a stateless pass-through (~40 lines: resolve client,
  forward, return) with no ledger — a proxy that must survive restarts is a
  contradiction of "transparent".
- **Essential?**: No. Nothing in the brief needs the hub to proxy
  plugin-to-plugin calls the routing layer already provides.
- **Severity**: high
- **Effort**: medium

## Finding 5 — A dispatched task crosses seven indirection layers, including an attribute-poke transport seam

- **Location**: dispatch chain: `_drain_pending` 1714-1731 → `_assign`
  1733-1761 → `run_coroutine_threadsafe(_do_rhapsody_submit)` 1758-1761 →
  `_get_rhapsody_client` 1484-1509 → `_caller_client` 719-737 → `CallerHTTP`;
  pilot chain: `ctx.submit_pilot` (`task_dispatcher_strategy.py:122`) →
  `PoolState._strategy_submit_pilot` 197-233 → `_schedule_pilot_submit`
  1431-1446 → `_do_pilot_submit` 1560-1610. The seam hack:
  `client._async_http = caller_http   # route the a<method> cores here`
  (`plugin_task_dispatcher.py:736`).
- **What & why it hurts**: Answering "where does the task actually get sent?"
  requires holding seven frames across three files, two of which exist only to
  bounce control (`PoolState._strategy_*` hooks forward straight to plugin
  `_schedule_*` methods, which forward to `_do_*` coroutines). The transport is
  wired by assigning a private attribute (`_async_http`) on a client object
  built for a different transport — behavior a reader cannot discover from
  either class's definition. Line 736's comment is the only documentation.
- **Simpler alternative**: Since everything already runs on one loop (Finding
  6), fold `PoolState._strategy_submit_pilot`/`_cancel`/`_drain` and the
  `_schedule_*` pair away: `ctx.submit_pilot` → one plugin method that builds
  the record and `asyncio.create_task(self._do_pilot_submit(...))`. Give the
  client classes an explicit constructor parameter (`async_http=`) instead of
  the post-construction attribute poke.
- **Essential?**: Reusing the real `PSIJClient`/`RhapsodyClient` cores is a
  good instinct (one payload/parse implementation); the hop count and the
  private-attribute wiring are accidental.
- **Severity**: medium
- **Effort**: medium

## Finding 6 — Concurrency story contradicts itself: `run_coroutine_threadsafe` from the loop's own thread, lazy start on first request

- **Location**: `plugin_task_dispatcher.py:1444-1446, 1452-1454, 1758-1761`
  (`asyncio.run_coroutine_threadsafe(..., self._main_loop)`); PoolState
  docstring 123-126 ("all mutations happen from the plugin's asyncio event
  loop thread"); `_ensure_started` 687-716 (sets `_loops_started = True` then
  un-sets it if no loop; called at the top of every route); `_materialise_pool`
  617-623 (second, conditional task-spawn path with a swallowed
  `RuntimeError`).
- **What & why it hurts**: The stated model is single-threaded-on-the-loop, and
  the call sites *are* on the loop (routes, tick loops, tap callback) — yet
  scheduling uses the cross-thread API, which tells a reader "another thread is
  involved here" when none is. Combined with the `_main_loop` capture, the
  set-then-unset `_loops_started` dance, and tick loops being started from two
  different places (with a `try/except RuntimeError: pass` for a test-client
  edge case), a student cannot tell what the actual threading contract is.
- **Simpler alternative**: `asyncio.create_task(...)` at every scheduling site;
  start the background loops once from the plugin-host's startup hook (the
  broker constructs hosted plugins on the running loop —
  `plugin_replay.py:385-387` relies on exactly that), deleting
  `_ensure_started` calls from all nine routes and the second spawn path in
  `_materialise_pool`.
- **Essential?**: No. The decoupling requirement (plugin must not block
  liveness) is satisfied by running on the plugin-host loop at all; the
  threadsafe idiom adds no safety on-loop.
- **Severity**: medium
- **Effort**: small

## Finding 7 — Four-plus background loops where one housekeeping loop would do

- **Location**: `plugin_task_dispatcher.py:700-708`: per-pool `_tick_loop`
  (739-754) + `_handshake_sweeper` (756-779) + `_state_sweeper` (789-812) +
  `_compaction_sweeper` (814-840); plus the replay plugin's `_sweep_loop`
  (`plugin_replay.py:448-459`).
- **What & why it hurts**: Five concurrently-sleeping coroutine loops, each
  with its own try/except/CancelledError skeleton and its own cadence
  constant, for work that is all "every N seconds, call a sync function".
  Debugging "why didn't X happen" means finding which of five loops owns X.
  The handshake sweeper even runs at tick cadence — it could *be* the tick.
- **Simpler alternative**: One `async def _housekeeping()` ticking every 5 s:
  call each pool's `on_tick`+drain+termination, the handshake check, and —
  on modulo counters — compaction (every 60th tick) and pruning (daily). One
  loop, one error handler, one place to look. (Compaction and pruning largely
  vanish anyway under Findings 3 and 8.)
- **Essential?**: Periodic work is essential; five independent loops are not.
- **Severity**: medium
- **Effort**: small

## Finding 8 — Speculative telemetry: arrivals ring, lag history, adaptive handshake timeout, arrival-rate forecasting

- **Location**: `plugin_task_dispatcher.py:98-104` (constants), 151-152 +
  190-193 + 250-259 (arrivals/lag buffers), `_effective_handshake_timeout`
  781-787 (`max(300, 2·avg lag)`); sole real consumer is the *example*
  strategy's queueing forecast `projected_arrivals = arrival_rate * avg_lag`
  (`task_dispatcher_strategy_examples.py:142-151`).
- **What & why it hurts**: The dispatcher maintains two sliding-window
  histories with trim policies and exposes them via two context accessors —
  and the only code that reads `arrivals_window` is a demo strategy; the only
  adaptive use of lag history is stretching a 5-minute timeout that a constant
  serves fine. This is workload-modeling infrastructure for research that has
  not arrived, and every reader pays for it now.
- **Simpler alternative**: Delete `arrivals`, `lag_history`,
  `record_arrival`, `record_pilot_lag`, both context accessors, and
  `_effective_handshake_timeout` (use `_HANDSHAKE_TIMEOUT_SEC`). Reintroduce
  with the strategy that needs them, in that strategy.
- **Essential?**: No requirement needs arrival forecasting; handshake timeout
  needs one constant.
- **Severity**: medium
- **Effort**: small

## Finding 9 — Example strategy ships in the production package, duplicating the real one

- **Location**: `task_dispatcher_strategy_examples.py` (whole file);
  duplication with `task_dispatcher_strategy_conservative.py`: `_PRE_ACTIVE`
  (examples:32 / conservative:44), `pick_dispatch` bodies (examples:91-106 /
  conservative:180-204), the free-capacity/in-flight/max-pilots cap stanza
  (examples:138-161 / conservative:143-163).
- **What & why it hurts**: A file whose docstring says it exists "to
  demonstrate the research-facing ABC" is installed, entry-point-registered,
  and selectable in production config — so a student must treat 173 lines of
  demo policy (with its own forecasting math, Finding 8) as live code, and the
  scale-up gating logic now exists in two drifting copies.
- **Simpler alternative**: Move it to `tests/` or `docs/` as the conformance
  fixture it already is; or extract the shared cap-check stanza into a helper
  on the base class and shrink both strategies to their actual policy delta
  (~20 lines each).
- **Severity**: medium
- **Effort**: small

## Finding 10 — Doc rot: rename-corrupted words, a class docstring that contradicts `is_enabled`, and phantom `pools.json`

- **Location**: "lendpointr"/"wendpoint" (mechanically mangled "ledger" and
  "wedge"): `plugin_task_dispatcher.py:7,819`,
  `task_dispatcher_state.py:134-141,225`. Class docstring
  `plugin_task_dispatcher.py:442` says "Endpoint-side task dispatcher" while
  `is_enabled` (457-467) is broker-only. Module docstring line 4 says pools
  are "declared in ``pools.json``", but nothing ever loads that file —
  `load_pools` (`task_dispatcher_config.py:86-123`) has no callers
  (grep-verified); pools arrive only via `register_session`. Comments also
  cite unreachable design docs (`plans/...` at lines 28-29,
  `memory/project_broker_dispatcher.md` at 82, 464, and section refs "§5.1",
  "§6.4").
- **What & why it hurts**: The prose a student leans on hardest is wrong in
  three load-bearing places: the plugin's one-line identity, its configuration
  source, and record-type names ("Lendpointr entry" is unparseable). Dead
  `load_pools` keeps the false `pools.json` story alive.
- **Simpler alternative**: Fix the mangled words; make the class docstring say
  "Broker-hosted"; delete `load_pools` or wire it up; replace design-doc
  pointers with the actual invariant in one sentence.
- **Essential?**: No — pure debt.
- **Severity**: medium
- **Effort**: small

## Finding 11 — `default_pool_config` promises a queue override that never happens

- **Location**: `task_dispatcher_config.py:278-307`, docstring 285-288: "the
  dispatcher will override this with an endpoint-appropriate value during
  materialisation". `_materialise_pool` (`plugin_task_dispatcher.py:573-624`)
  auto-resolves only `endpoint_name`; `queue='default'` flows unchanged into
  the psij JobSpec (`_build_job_spec` 1545-1548, `'queue_name': ...`).
- **What & why it hurts**: The zero-config path every student tries first
  (register with no pools → auto `default` pool) will submit pilots to a batch
  queue literally named `default`, which fails on most schedulers — and the
  docstring assures the reader the code handles it. Debugging this means
  disproving a comment.
- **Simpler alternative**: Either implement the override (e.g. ask the target
  endpoint's `queue_info` for a default) or — simpler — drop the promise and
  fail fast at materialisation with "default pool needs an explicit queue".
- **Essential?**: The default-pool convenience is fine; the false comment is a
  latent bug.
- **Severity**: medium
- **Effort**: small

## Finding 12 — Replay: server-side cursor store duplicates state the client already tracks (and isn't durable despite the pitch)

- **Location**: `plugin_replay.py:166-181` (`_Cursor`), 431-437
  (`_prune_cursors` + `cursor_ttl`), 529-533 (`drop_cursor` route), fetch
  cursor logic 504-515; client-side: `replay_iter` 289-312 keeps its own
  `ack`/position locally the whole time. Module docstring line 22 sells
  "durable ephemeral-event delivery".
- **What & why it hurts**: The cursor is an in-memory int keyed by a
  client-chosen name — it does not survive broker restart (so "durable" is
  only the buffer, not delivery), and the shipped consumer (`replay_iter`)
  tracks its position itself anyway, sending `ack_seq` back just to keep the
  server copy warm. For this one int the plugin carries a cursor class, a TTL
  constant, a prune pass in the sweeper, a `drop_cursor` route, and the
  ACK-ordering subtlety in `_route_fetch`. The splice protocol already demands
  the consumer dedup by `seq`, i.e. the consumer must track position
  regardless.
- **Simpler alternative**: Make `fetch` stateless: require `after_seq`, return
  `{events, next_seq, gap}`; the client stores its own position (it does).
  Delete `_Cursor`, `_prune_cursors`, `cursor_ttl`, `drop_cursor`, and the
  ack-first ordering comment. At-least-once semantics are unchanged — a client
  that doesn't advance `after_seq` re-fetches the same batch.
- **Essential?**: Replay-after-reconnect is essential (use case 7). The
  server-side cursor adds no durability (dies with the broker) and no
  capability the client doesn't already have.
- **Severity**: medium
- **Effort**: small

## Finding 13 — `_drain_pending`'s `safety = 10_000` band-aid instead of a progress guarantee

- **Location**: `plugin_task_dispatcher.py:1714-1731`.
- **What & why it hurts**: The loop trusts the strategy to eventually return
  `None`, and guards against a buggy one with a magic 10,000-iteration fuse.
  The `continue` on a stale (non-QUEUED) pick means a strategy that keeps
  returning the same stale task spins 10k calls per drain, silently. A safety
  counter with no log line is the worst of both: it neither proves progress
  nor reports the violation.
- **Simpler alternative**: Bound by something meaningful — at most
  `len(pending_queue)` dispatches per drain — and `log.warning` + break when a
  pick repeats or is stale, so a broken strategy is visible instead of
  rate-limited.
- **Severity**: low
- **Effort**: small

## Finding 14 — Session-isolation story has a session-less back door, and staging re-implements a sibling plugin

- **Location**: `_route_pool_detail` (`plugin_task_dispatcher.py:961-970`) —
  "first owner found" returns *any session's* pool (verbose: pilots + last 50
  task records) by name with no sid, despite "strict per-session isolation"
  everywhere else (e.g. 894, 973). Staging: `_route_stage_in`/`_route_stage_out`
  1206-1297 — base64-file-in-JSON transfer with filename validation duplicated
  verbatim at 1238 and 1273, re-implementing the concern of the dedicated
  `staging` plugin.
- **What & why it hurts**: The one route that ignores the sid-keying undercuts
  the model the rest of the file works hard to enforce (and leaks another
  session's task commands/paths to any authenticated caller). The in-plugin
  staging path is a second file-transfer implementation for students to learn,
  with copy-pasted validation.
- **Simpler alternative**: Make `pool/{name}` sid-scoped like `fleet/{sid}` (or
  return the non-verbose summary only). Extract `_check_filename(name)` once;
  longer-term, route scratch-dir staging through the existing `staging`
  plugin's client instead of a parallel base64 path.
- **Essential?**: Shared-FS staging for tasks is a real need; a second
  implementation and a cross-session read route are not.
- **Severity**: low
- **Effort**: small

## Finding 15 — Replay `select()`/gap subtleties rely on easy-to-misread expressions

- **Location**: `plugin_replay.py:150-159` — the "always deliver at least one
  event" rule is encoded as `if out and (len(out) >= max_events or nbytes +
  size > max_bytes): break`; gap detection at 525 (`lowest > lower + 1`)
  compares against the *unfiltered* buffer floor even when `patterns` filter
  the stream; 409-411 re-msgpacks every event solely to measure size.
- **What & why it hurts**: Each is defensible and documented, but all three
  are the kind of expression a student will "fix" wrongly: the bound check
  ordering is load-bearing (moving it breaks the oversized-event guarantee),
  `gap` can read as false-alarm-prone under filters, and the per-event
  re-encode looks like a bug ("why pack an event we already have?").
- **Simpler alternative**: Split the select bound into named steps
  (`fits = len(out) < max_events and nbytes + size <= max_bytes; if not fits
  and out: break`); a one-line comment on `gap` noting it is
  filter-independent by design; use `len(repr(event))` or the frame size the
  broker already computed, if available, rather than a second msgpack pass.
- **Essential?**: The oversized-event guarantee and gap observability are
  essential to the replay contract; only the phrasing is accidental.
- **Severity**: low
- **Effort**: small
