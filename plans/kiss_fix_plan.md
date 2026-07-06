# KISS-fix implementation plan (branch feature/broker_13)

Actions the review (`plans/code_review_kiss.md` + `plans/code_review_kiss/`) fed
into fixes. Scope agreed with the user: **A1–A9, B1–B4, C (all), D (simplify where
no requirement exists)**. **E is explicitly deferred** for discussion.

## Decisions (defaults taken while the user was away — overridable)

- **C3 (dispatcher WAL → atomic-JSON store) is IN SCOPE** (user decision, overriding
  the earlier hold): rewrite `task_dispatcher_state.py`'s store as one `state.json`
  per pool, rewritten atomically (`tempfile` + `os.replace` — the routine already
  exists in `snapshot()`), debounced to ~1/sec if per-mutation is too chatty; recovery
  = `json.load`. Delete the append-JSONL log, the size/age compaction policy, the
  compaction sweeper, the `O_APPEND`-truncate trick, and the two compaction constants.
  Greenfield (no on-disk data to migrate). If E1 later moves the dispatcher off the
  broker, this store travels with it.
- **D-strategy: delete the framework, inline one policy.** Remove the strategy ABC,
  the triple loader (builtin dict / entry points / dotted path), the `setup.py`
  entry-point group, the example strategy, and the arrival/lag telemetry; keep the
  conservative behavior as plain built-in dispatcher logic, to be re-genericized
  when real requirements exist.
- **B5 is out of scope** (user listed B1–B4). A5 uses the minimal standalone fix
  (add the owner check to rhapsody's reconnect branch), not the base-hook refactor.
- Out of scope (not A/B/C/D; revisit with E): R5#2 (three job-state vocabularies),
  R5#3 (two backend hierarchies), staging→`_forward` behavior change.

## Bug fixes (A)

- **A1** queue `"default"` submitted verbatim → `task_dispatcher_config.py` /
  `plugin_task_dispatcher.py`: fail fast at materialisation ("default pool needs an
  explicit queue") or resolve a real default; drop the false docstring promise.
- **A2** `deregister_dynamic_plugin` leaks `_session_policy`/`_session_last_access`
  and skips drain-timer cancel → call `await plugin.shutdown()`; keep only route
  stripping host-side (`plugin_host_base.py`).
- **A3** `_host_broadcast` no-ops `topic=='topology'` → make it `async def`, handle
  the topology topic for real (schedule `_broadcast_topology`); drop the
  coroutine-returning-sync shim (`broker.py`, `broker_plugin_host.py`).
- **A4** psij `job_status` double-fire → one `PSIJSession._notify_state(job_id,
  status)` used by both the submit callback and the poll loop; single dedupe store
  (`plugin_psij.py`).
- **A5** rhapsody reconnect skips `_check_owner` → call it in rhapsody's override
  (`plugin_rhapsody.py`).
- **A6** `_inflight` unbounded/timeout-less → give forwarded entries a deadline and
  evict via the existing sweep (fold into the single-table shape if convenient, else
  a bounded map with a periodic reap); rename the "per-src cap" honestly (`broker.py`).
- **A7** silent drops → log at WARNING on: callback-queue drop, thread-origin
  notification before the loop is primed (fix by host-injecting the loop), gateway
  events before the loop is captured (`runtime_support.py`, `plugin_base.py`,
  `gateway.py`). See also C1/C2 which remove two of these queues.
- **A8** `_route_pool_detail` cross-session read → make it sid-scoped like
  `fleet/{sid}` (`plugin_task_dispatcher.py`).
- **A9** cluster classification hardcodes `'ucsb' in name` → replace with an explicit
  per-endpoint config flag (`cluster_type: immediate|allocate` in the resource
  config, honored by the classifier). A site that *has* `queue_info` (UCSB) can then
  be forced `immediate` by config; the "has queue_info + is_enabled → allocate" rule
  stays the default when no override is set (`plugin_xgfabric.py` + its config).

## Themes (B)

- **B1** doc/comment sweep, tree-wide: strip milestone/plan tags ("M0 lesson", "pre-
  flip item", `plans/…`, `memory/…`, §refs), fix references to the deleted
  `service.py`/`EndpointService`, repair rename manglings ("lendpointr"→ledger,
  "wendpoint"→wedge), delete docstrings that state the opposite of the code.
- **B2** delete verified-dead code: `exceptions.py` (+ its test), the correlation-ID
  contextvar in `logging_config.py`, `validate_ui_config` + unused UI models,
  `protocol.peek_routing` + `channel`/`capabilities`/`is_binary`, rhapsody
  `_watch_task` subsystem + `_assert_json_serializable`, `load_pools`/`pools.json`,
  `BrokerPluginHost.on_topology_changed`, the four queue_info back-compat shims,
  xgfabric's created-but-unused HTTP client + cert threading.
- **B3** de-duplicate: one `BoundedDropOldestQueue` (new `queues.py`) for
  broker_events/gateway/replay; one `PluginSession.start_status_poller(...)` for
  globus/iri/psij (logs repeated failures at WARNING); one `run_cmd`/`run_cmd_strict`
  in `batch_system.py` for the backends; one `_drop_session` in `plugin_base.py`;
  the queue_info user/force boilerplate and the response-dict→Response unpack into
  small helpers.
- **B4** sync/async client simplification (**user agrees with reviewer**): delete
  `_run_sync`'s `coro.send(None)` driver and the per-method `a<method>` twins in
  `plugin_psij.py`/`plugin_rhapsody.py`; keep the plain sync `PluginClient`; move the
  "don't block the host loop" concern into the **one** place that has it — the
  broker-hosted dispatcher wraps its sync client calls (e.g. `asyncio.to_thread`).
  (`client.py`, `runtime_client.py`, `plugin_psij.py`, `plugin_rhapsody.py`,
  `plugin_task_dispatcher.py`.)

## Over-built essentials (C) — C3 held

- **C1** `Handoff` → `loop.call_soon_threadsafe` / one `asyncio.Queue`
  (`runtime_support.py`, `runtime.py`).
- **C2** `CallbackDispatcher` → `queue.Queue(maxsize)` + daemon worker + WARNING on
  drop (`runtime_support.py`, `runtime.py`).
- **C4** delete the loop-lag watchdog + suppression window + injected clock
  (`broker.py`).
- **C5** one-counter request backpressure (drop the semaphore/second knob)
  (`runtime.py`).
- **C6** session-model slim: one `SessionRecord`, one `_expiry_deadline`, reuse the
  5 s sweep for owner-drain (stamp a `drain_deadline` on `lost`, clear on `present`;
  delete the per-owner `asyncio` drain tasks and `_default_lock`), create `default`
  in `__init__`, state the policy table once (`plugin_base.py`).
- **C7** replay: stateless `fetch(after_seq)` returning `{events, next_seq, gap}`;
  delete `_Cursor`/`_prune_cursors`/`cursor_ttl`/`drop_cursor` (`plugin_replay.py`).
- **C8** split `_tunnel_watcher` per mode (`_watch_forward`/`_watch_reverse`); move
  rendezvous-file plumbing into `tunnel.py` (`plugin_psij.py`, `tunnel.py`).
- **C9** `_parse_allocated_port` → reuse the existing stderr-drain thread
  (`tunnel.py`).
- Also the dispatcher's 5 background loops → one housekeeping tick (R4#7)
  (`plugin_task_dispatcher.py`).

## Speculative generality (D)

- Strategy framework → single inline policy (see Decisions).
- Notification-batch knobs → module constants; drop the per-session params
  (`plugin_rhapsody.py`).
- Client-side template compression + thread-pool submit → keep only frame-size
  batching (sequential) (`plugin_rhapsody.py`).
- Runtime 16-param ctor / broker 18-param ctor → module constants for the test
  seams; keep only what an operator sets (`runtime.py`, `broker.py`).
- Plugin-selection DSL → exact + `all` + `default` (drop ambiguous prefix-match);
  log a WARNING in the `except ImportError` swallow (`plugin_host_base.py`,
  `__init__.py`).
- sysinfo ceremonial `except (…, Exception)` tuples + inner thread pool → plain
  `except Exception` + sequential detection on the existing prefetch thread
  (`plugin_sysinfo.py`).

## Orchestration (two waves; file-ownership disjoint within a wave)

**Wave 1 (parallel; defines the shared helpers):**
- FW (Opus): `plugin_base.py`, `plugin_host_base.py`, `plugin_session_base.py` —
  C6, A2, A7-notify, B3-teardown, **defines `start_status_poller`**, D-plugin-select,
  B1/B2 herein.
- CORE (Opus): **new `queues.py`**, `broker.py`, `broker_events.py`,
  `broker_plugin_host.py`, `protocol.py` — A3, A6, C4, D-broker-ctor, B2-protocol,
  B1, refactor broker_events onto the shared queue.
- BACKENDS (Sonnet): `batch_system*.py`, `queue_info*.py`, `plugin_queue_info.py` —
  B3-subprocess (**defines `run_cmd`**), B3-queue_info-boilerplate, B2-shims,
  move SLURM-only helpers out of the base, B1.

**Wave 2 (after Wave 1 green; import the Wave-1 helpers):**
- RUNTIME (Opus): `runtime.py`, `runtime_support.py`, `runtime_client.py`,
  `client.py`, `gateway.py` — C1, C2, C5, B4-client, A7-gateway, D-runtime-ctor,
  gateway→shared queue, B1.
- RHAPSODY (Opus): `plugin_rhapsody.py` — A5, B2, B4-twins, D-knobs+compression,
  B3-poller, B1.
- PSIJ (Opus): `plugin_psij.py`, `tunnel.py` — A4, C8, C9, B4-twins, D-imports/
  poll/route-consistency, B3-poller, B1.
- DISPATCHER (Opus): `plugin_task_dispatcher.py`, `task_dispatcher_config.py`,
  `task_dispatcher_strategy*.py`, `task_dispatcher_state.py` (**C3 store rewrite** +
  telemetry removal), `plugin_replay.py`, `setup.py` — A1, A8, D-strategy, 5→1 loops,
  B2, C3, C7, replay→shared queue, B1.
- EDGE (Sonnet): `plugin_xgfabric.py`, `plugin_globus.py`, `plugin_iri_connect.py`,
  `plugin_iri_instance.py`, `plugin_lucid.py`, `plugin_sysinfo.py` — A9, B2-xg,
  B3-poller (globus/iri), D-sysinfo, minor nits, B1.

## Residual tracked follow-ups (branch 13 is green; these are deferred, not blocking)

- **C6 finish**: a small `_LastAccessView` write-through shim remains in
  `plugin_base.py` so globus/iri/rhapsody source could stay green during Wave 1;
  globus now records policy/owner via the base, so the shim's last remaining
  writers are few — remove it (and re-evaluate `_default_lock`/`_ensure_default_session`
  vs. rhapsody's lazy default) in a small follow-up.
- **Endpoint-mode** (dispatcher finding 4) was left in place (E-adjacent) rather than
  deleted — revisit with E.
- **Unused Pydantic UI models** in `ui_schema.py` (only `UIConfig`/`ui_config_to_dict`
  have callers) — a possible further trim, left conservative.

## Acceptance

Per wave and at the end: `PYTHONPATH=$PWD/src ve3/bin/python -m pytest
tests/unittests/ -q` green (tests for deleted features deleted; behavior changes get
updated tests), `ve3/bin/python -m flake8 src/ bin/` clean, the docs build stays
warning-clean (only the `radical.pilot` import residual), and examples `py_compile`.
Supervisor reviews every wave's diff and iterates before moving on. Everything lands
on `feature/broker_13`; E is discussed before any of its items.
