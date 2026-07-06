# Review — Integration plugins (rhapsody, xgfabric, globus, lucid, iri_connect, iri_instance, iri_endpoints)

## Overall impression

This scope is sharply bimodal. `plugin_lucid.py`, `plugin_globus.py`, `plugin_iri_connect.py`, and `plugin_iri_instance.py` are close to the ideal shape for a plugin: session holds domain state, thin route handlers call `_forward`, a client mirrors the routes — a student can read each in one pass (lucid especially is a model 240-line plugin). `plugin_rhapsody.py` (1712 lines) is the opposite: roughly half of it is performance machinery — a hand-rolled two-stage submit pipeline, client-side template compression, thread-pooled batch submission, a notification batcher with two tunable knobs, dual-lock waiter registries, and sync/async twin methods — none of which is demanded by an architectural requirement, plus outright dead code (an unused per-task watcher subsystem) and debugging probes left in the init path. `plugin_xgfabric.py` is a legitimate domain workflow and its phase-by-phase control flow is readable, but it drags vestigial plumbing (an HTTP client that is created and closed but never used, cert resolution that goes nowhere, two adapter classes wrapping one method call). The framework itself is used consistently in the small plugins and inconsistently in the two big ones — rhapsody and globus each reimplement parts of the base `register_session` lifecycle by hand, in two different ways.

---

## Findings

### 1. Rhapsody server-side submit is a hand-rolled two-stage pipeline with a duplicated epilogue

- **Location**: `src/radical/orbit/plugin_rhapsody.py:359-450` (`RhapsodySession.submit_tasks`), module `plugin_rhapsody`.
- **What & why it hurts**: The submit loop overlaps deserialization of chunk N+1 with backend submission of chunk N using `prev_submit_fut` / `prev_tasks` carried across iterations. The "register results" block (lines 409–417) is copy-pasted after the loop (lines 436–443) because the pipeline leaves one submit in flight when the loop ends. A student tracing "what happens to my task" must mentally simulate a software pipeline across loop iterations, and any edit must be made in two places. The 16 interleaved `prof.prof(...)` calls make the actual logic even harder to see.
- **Simpler alternative**: A sequential loop per chunk: `tasks = await asyncio.to_thread(self._prepare_batch, chunk)` → `await self._rh_session.submit_tasks(tasks)` → register results. One block, no carried futures, no duplicated epilogue. The event-loop-responsiveness concern is already satisfied by the `to_thread` offload and the `await` on the backend submit.
- **Essential?** No. The requirement served (failure detection decoupled from plugin behavior) only demands that the work loop not block — which the thread offload alone provides. Overlapping deser with submit is a throughput optimization the brief's requirements never ask for.
- **Severity**: high. **Effort**: small.

### 2. Dead per-task watcher subsystem (and the live one reaches into rhapsody's private state manager)

- **Location**: `src/radical/orbit/plugin_rhapsody.py:514-557` (`_watch_task`), `:41` (`WATCH_CONCURRENCY`), `:147/216` (`_watch_sem`), `:272` (stale comment "terminal is handled by _watch_task"); `:590-593` (`_watch_batch` using `self._rh_session._state_manager.get_wait_future`).
- **What & why it hurts**: `_watch_task` is never called — only `_watch_batch` is (line 447). Yet `_watch_task`, its semaphore, and the `WATCH_CONCURRENCY` constant all remain, and a comment at line 272 still routes the reader to the dead path. A student auditing "how does a completion notification happen" finds two watcher mechanisms and must discover by grep that one is vestigial. Meanwhile the live `_watch_batch` bypasses rhapsody's public `wait_tasks` API and pulls per-task futures from `self._rh_session._state_manager` — a private attribute of a third-party library — which is exactly the kind of coupling that breaks silently on a dependency upgrade and is undebuggable for a student who only knows the documented API.
- **Simpler alternative**: Delete `_watch_task`, `_watch_sem`, `WATCH_CONCURRENCY`, and fix the line-272 comment. For `_watch_batch`, either use the public `wait_tasks` per completion or, if incremental drain truly needs futures, isolate the private access in one commented helper (`_task_future(t)`) so the coupling is a single greppable line.
- **Essential?** No for the dead code. The private-API reach is arguably domain-essential (incremental completion drain) but should be contained and flagged.
- **Severity**: high. **Effort**: small.

### 3. Client-side template compression + homogeneity detection + two duplicated thread-pool submit blocks

- **Location**: `src/radical/orbit/plugin_rhapsody.py:1099-1253` (`RhapsodyClient.submit_tasks`, `_submit_template`), module `plugin_rhapsody`.
- **What & why it hurts**: A *client helper* performs: per-task cloudpickle encoding, client-side UID minting, an O(N·K) homogeneity check comparing every field of every task by identity-then-equality (lines 1139–1144), template compression, byte-size-estimated batching via `len(str(td))` (a rough proxy for msgpack size), and concurrent submission through a `ThreadPoolExecutor` with `max_workers=len(batches)` (unbounded for many batches). The `ThreadPoolExecutor`/`as_completed`/error-collection/result-reordering block appears **twice**, nearly verbatim (1180–1202 and 1230–1253). The server then has a mirror decompression path (`submit_tasks` handler lines 1670–1680, `pre_expanded` threading through `_prepare_batch`). That is three cooperating optimizations spread across client and server for one API call.
- **Simpler alternative**: Sequential batch submission (a plain `for b in batches: results += _submit_batch(b)`) removes both thread-pool blocks. If template compression measurably matters for the target workloads, keep it but extract one `_submit_concurrently(fn, parts)` helper so the pool logic exists once; if it doesn't, drop it and the server's `pre_expanded` plumbing with it.
- **Essential?** Partially: the frame-size cap is a real protocol constraint, so *splitting* into batches is essential ("A protocol-level frame-size cap bounds each frame"). Concurrent submission and template compression are not — no requirement demands client-side throughput tricks.
- **Severity**: high. **Effort**: medium.

### 4. Client wait machinery: two locks with documented lock-ordering instead of one Condition

- **Location**: `src/radical/orbit/plugin_rhapsody.py:813-859` (`_completed`/`_waiters` state, `_on_task_done`), `:1255-1354` (`wait_tasks`), module `plugin_rhapsody`.
- **What & why it hurts**: The SSE-based wait uses a completed-dict guarded by `_completed_lock`, a waiter registry `[(pending_set, Event)]` guarded by `_waiters_lock`, and comments in two places explaining the required lock acquisition order ("Lock order: completed → waiters"). Any code that needs prose to explain its lock ordering is past the student-comprehension budget for a client convenience wrapper; a mistake here deadlocks the caller's application thread silently.
- **Simpler alternative**: One `threading.Condition` around `_completed`: `_on_task_done` does `with cond: update _completed; cond.notify_all()`; `wait_tasks` does `with cond: while not all(u in self._completed for u in uids): cond.wait(remaining)`. This deletes the waiter registry, both explicit locks, the registration/removal try/finally, and both lock-order comments, with identical semantics.
- **Essential?** The behavior (blocking wait fed by push notifications rather than polling — requirement: long-running work streams status back) is essential; the two-lock implementation is not.
- **Severity**: medium-high. **Effort**: small.

### 5. Sync/async twin client methods that will drift

- **Location**: `src/radical/orbit/plugin_rhapsody.py:960-1047` (`_poll_session_ready` / `_apoll_session_ready`, `register_session` / `aregister_session`, `submit_tasks` / `asubmit_tasks`, `get_task` / `aget_task`, `cancel_task` / `acancel_task`), module `plugin_rhapsody`.
- **What & why it hurts**: Five methods exist twice, each async twin docstring explaining it "mirrors" its sync sibling for the broker-hosted dispatcher. The mirrors are already semantically unequal (async submit skips template compression and batching; async register skips SSE), so a student cannot assume `aX` == `X`, and every behavior fix must be evaluated for two code paths. No other plugin in this scope carries an async client shadow, so rhapsody is also the inconsistent one.
- **Simpler alternative**: If the broker-hosted dispatcher genuinely needs an async path, that is a *framework* concern: `PluginClient` already has `_arequest`; give it generic async `aget/apost` passthroughs and let the dispatcher hit routes directly, instead of each plugin hand-writing a partial async client. Otherwise delete the twins.
- **Essential?** The broker hosting plugins is architectural, but nothing in the requirements says plugin *clients* must exist in two colors — a simpler single implementation of the requirement exists at the framework layer.
- **Severity**: medium. **Effort**: medium.

### 6. Debug probes and profiler ceremony left in the production init/submit paths

- **Location**: `src/radical/orbit/plugin_rhapsody.py:228-248` (Dragon hostname/GPU probe in `initialize`), `:162-170` (`prof` property with a four-level `getattr` chain), ~20 `prof.prof(...)` call sites, `:1662-1677` (`if prof: prof.prof(...)` in the route handler).
- **What & why it hurts**: `initialize()` contains a 20-line "Probe" block whose comment narrates a past debugging session ("A mismatch silently turns HOST_NAME placement into a no-op and tasks pile up..."). It imports `dragon.native.machine` inline and logs every node's GPUs at INFO on every session init. The `prof` property digs `self._plugin._app.state.endpoint_service._prof` through four `getattr`s with a fallback constructor — opaque service-location. Profiling call sites outnumber logic lines in some methods.
- **Simpler alternative**: Delete the probe block (or move it behind an explicit debug flag/env var in a standalone diagnostic function). Have the plugin inject the profiler into the session at `_create_session` time (it already injects `_plugin`), replacing the getattr chain with `self._prof`.
- **Essential?** No requirement covers profiling instrumentation or environment probing.
- **Severity**: medium. **Effort**: small.

### 7. Notification-batching knobs plumbed end-to-end as speculative configurability

- **Location**: `src/radical/orbit/plugin_rhapsody.py:43-44` (constants), `:113-116, 149-153` (session ctor params), `:452-512` (`_queue_notification` / `_schedule_flush` / `_flush_notifications`), `:861-911` (client `register_session` params), `:1546-1549` (server handler extraction), module `plugin_rhapsody`.
- **What & why it hurts**: `notify_batch_window` and `notify_batch_size` are threaded through five layers — client API → HTTP body → route handler → session ctor → batcher — so that a caller could tune SSE coalescing per session. No caller in the repo passes them. Each layer's signature, docstring, and default handling pays for a knob nobody turns. The batcher itself also always schedules a delayed-flush task per queued notification ("Always schedule — it's cheap"), creating N asyncio tasks for N notifications rather than one timer.
- **Simpler alternative**: Make window/size module constants (they already exist as such); delete the parameters from the client, handler, and ctor. In `_queue_notification`, only schedule a flush task when one is not already pending (a single `_flush_scheduled` bool).
- **Essential?** Batching itself is defensible under the frame-size/efficiency constraint; per-session configurability is speculative generality.
- **Severity**: medium. **Effort**: small.

### 8. Rhapsody reimplements the base session lifecycle inside its `register_session`

- **Location**: `src/radical/orbit/plugin_rhapsody.py:1514-1632` (`PluginRhapsody.register_session`, `_ensure_default_session`), vs `plugin_base.py:348-511` (`register_session` / `_open_session`), module `plugin_rhapsody`.
- **What & why it hurts**: The override re-does body parsing, `_normalize_session_policy`, the DEFAULT_SID branch, sid minting, the reconnect branch with `_check_policy_conflict`, and `_record_session` — duplicating most of `Plugin._open_session` but *without* the owner check (`_check_owner`) the base performs on reconnect, so rhapsody sessions silently lack the owner-reattach protection every base-path plugin gets. The genuine delta is small: pass ctor kwargs, kick `_init_session`, and report a status field.
- **Simpler alternative**: Extend the base with the two hooks this actually needs — e.g. let `_open_session` accept session-ctor kwargs and call an overridable `_on_session_created(sid, session)` — then rhapsody's override shrinks to extracting `backends` and returning the extra `status` field, and the owner check comes back for free.
- **Essential?** Async backend init (returning before Dragon is up so the socket isn't held) is essential to the "slow plugin must not look like a dead host" requirement; hand-copying the lifecycle is not.
- **Severity**: medium (with a real behavioral gap, not just style). **Effort**: medium.

### 9. Globus bypasses the framework's session bookkeeping entirely

- **Location**: `src/radical/orbit/plugin_globus.py:571-606` (`PluginGlobus.register_session`), module `plugin_globus`.
- **What & why it hurts**: The override mints a sid and writes `self._sessions[sid]` / `self._session_last_access[sid]` directly, never calling `_record_session` or `_normalize_session_policy`. Globus sessions therefore have no lifetime/owner record — no `ephemeral`/`ttl`/`persistent` policy, no owner-loss reclaim, no reconnect-with-sid — making globus the one plugin whose sessions obey different lifecycle rules than the framework documents. A student who learned the session model from `plugin_base.py` will mispredict globus behavior. (Contrast with rhapsody, which at least reuses the policy helpers — the two big plugins reach around the same base path in two *different* ways.)
- **Simpler alternative**: Same fix as finding 8: a base hook for "session ctor needs request-body kwargs" (here, the credential). Then globus's override becomes: extract/validate the token fields, delegate to the base path with kwargs.
- **Essential?** Passing the credential at registration (never to disk) is essential; skipping the lifecycle record is not — it is a side effect of the base API not accepting ctor kwargs.
- **Severity**: medium. **Effort**: small (once the base hook from finding 8 exists).

### 10. Background poll-loop scaffolding triplicated across globus, iri_instance, and psij

- **Location**: `src/radical/orbit/plugin_globus.py:364-406` (`_start_polling` / `_poll_tasks`), `src/radical/orbit/plugin_iri_instance.py:266-316` (`_start_polling` / `_poll_jobs`), plus the same pattern in `plugin_psij.py:417-436`; modules `plugin_globus`, `plugin_iri_instance`.
- **What & why it hurts**: The skeleton — `_start_polling` guard, `while True: sleep(interval); break if inactive; filter non-terminal items; per-item fetch; if state changed, update + `_dispatch_notify`; swallow per-item errors; break on CancelledError` — is structurally identical in three plugins; only the fetch call, terminal-state set, and payload differ. Every new integration plugin will copy it a fourth time, and each copy independently swallows *all* poll errors at `log.debug` (globus:399-401, 405-406; iri:308-310, 314-316), so a permanently failing poller (expired token, wrong URL) is invisible unless debug logging is on — a real debuggability trap given both plugins hold expiring bearer tokens.
- **Simpler alternative**: One helper on `PluginSession`, e.g. `start_status_poller(interval, items, is_terminal, fetch, to_payload, topic)` that owns the task lifecycle and cancellation, and logs repeated failures at WARNING (or emits a notification) after N consecutive errors. Each plugin then contributes only its domain fragment.
- **Essential?** Polling remote APIs for status → push notifications is essential (requirement: long-running work streams status to consumers). The triplication and debug-level swallowing are not.
- **Severity**: medium. **Effort**: medium.

### 11. XGFabric vestigial plumbing: dead HTTP client, cert resolution to nowhere, adapter classes over one method

- **Location**: `src/radical/orbit/plugin_xgfabric.py:233,252-258` (unused `self._http`, `_http_get`, `_http_post`, `_verify` — created, never called, only `aclose()`d at :1214), `:568-571` (`broker_cert` resolved/logged, then unused), `:1352-1361` (`_create_session` resolving `resolve_broker_cert` to feed that unused field), `:59-84` (`_RuntimeEndpointAdapter` / `_RuntimeBrokerAdapter`), module `plugin_xgfabric`.
- **What & why it hurts**: A student reading the session ctor sees an async HTTP client, a TLS-verify helper, and broker URL/cert threading and reasonably assumes the workflow talks HTTP to the broker — it never does; all cross-endpoint traffic rides `self._runtime`. The two adapter classes exist only to preserve a `list_endpoints()` / `get_endpoint_client(name).get_plugin(...)` calling convention (the long comment at :38-46 admits this), adding a layer between the reader and the one real call `runtime.get_plugin(endpoint, plugin)`. `_get_plugin` (:699-705) then re-wraps the adapter anyway. `broker_url` is genuinely needed (passed to the pilot's `--url`), but cert resolution and the HTTP client are inert.
- **Simpler alternative**: Delete `_http`/`_http_get`/`_http_post`/`_verify` and the `broker_cert` threading. Delete both adapters and `self._bc`; have `_get_plugin` call `self._runtime.get_plugin(name, plugin)` and `_is_endpoint_online` check `name in self._runtime.topology()` directly.
- **Essential?** No. Cross-endpoint access through the runtime consumer facade is the essential part and is already what happens under the wrappers.
- **Severity**: medium. **Effort**: small.

### 12. XGFabric config resolution duplicated in two styles, plus a hardcoded site name in classification

- **Location**: `src/radical/orbit/plugin_xgfabric.py:320-340` (`load_config`) vs `:490-508` (`_load_resource_config` — same alias-map + path-probing logic, the second compressed into a nested ternary at :502-504); `:421-423` (`if 'ucsb' in endpoint_name: ... immediate`), module `plugin_xgfabric`.
- **What & why it hurts**: The builtin-alias → absolute-path → config-dir resolution exists twice; the second copy's three-way conditional-expression is genuinely hard to parse. And the cluster classifier special-cases any endpoint whose *name contains* `'ucsb'` — an invisible site-specific rule that will misclassify (`'ucsb-test'` on a SLURM machine stays "immediate") and that no config surface exposes; the surrounding logic already has the right general rule (queue_info presence).
- **Simpler alternative**: One `_resolve_config_path(name, builtin_map) -> Path | builtin-dict` helper used by both; write the path logic as plain if/elif. Replace the `'ucsb'` substring test with the existing plugin-based rule, moving any per-site forcing into `ResourceConfig.cluster_configs` (which already exists for per-endpoint overrides, e.g. a `cluster_type: immediate` key).
- **Essential?** No — the general rule is already implemented one branch below.
- **Severity**: medium. **Effort**: small.

### 13. Dead helper `_assert_json_serializable` in rhapsody

- **Location**: `src/radical/orbit/plugin_rhapsody.py:57-74`, module `plugin_rhapsody`.
- **What & why it hurts**: Defined, documented, never called anywhere in the repo. A student hunting the serialization path will read it and look for its callers in vain; it also shadows the real mechanism (`_json_safe`), inviting confusion about which one runs.
- **Simpler alternative**: Delete it.
- **Essential?** No.
- **Severity**: low. **Effort**: small.

### 14. Minor framework-consistency nits in lucid and iri_connect

- **Location**: `src/radical/orbit/plugin_lucid.py:117-118,129-130,141-142` (`if not self.sid: raise RuntimeError` instead of the base `self._require_session()`); `:203,220` (local `json = await request.json()` shadowing the `json` module name pattern used elsewhere); `src/radical/orbit/plugin_iri_connect.py:200-202` (`register_session` returning a dummy sid `'iri_connect.static'` that exists in no session table) and `:61-67` vs `:110-111` (client builds `f'iri.{endpoint}'` inline while the plugin has `_instance_key` for the same string); `plugin_iri_connect.py:126,159,180,194` reading `host._plugins` (a private dict of the host) in four places.
- **What & why it hurts**: Each is small, but they are exactly the divergences that make a student unsure whether the framework helper is optional. The dummy sid is a silent contract bend ("every plugin has sessions") solved differently than iri_instance's fixed-sid approach one file over; the `host._plugins` reads couple the plugin to the host's internals.
- **Simpler alternative**: Use `_require_session()` in lucid; rename the local `json`; give the host a small public `has_plugin(name)` / `plugins()` accessor for iri_connect; pick one place to build the `iri.<name>` string.
- **Essential?** No. (The dynamic-instance pattern itself — iri_connect registering `PluginIRIInstance` via `register_dynamic_plugin` — is clean and *is* the right use of the framework; only the private-dict access and the sid dummy chip at it.)
- **Severity**: low. **Effort**: small.

### 15. XGFabric: needless `async` on pure-sync closures inside `_get_connected_endpoints`

- **Location**: `src/radical/orbit/plugin_xgfabric.py:415-430` (`async def _classify` — contains no `await`), module `plugin_xgfabric`.
- **What & why it hurts**: `_classify` is declared `async` and awaited at three call sites but is entirely synchronous; a student sees `await _classify(...)` and assumes I/O happens there. Nested closures (`_cluster`, `_classify`) inside an already-async method add two indentation levels for what is a pure function of its inputs.
- **Simpler alternative**: Make `_classify` a plain method (or module function) taking `(endpoints_info, resource_config)`; drop the `await`s.
- **Essential?** No.
- **Severity**: low. **Effort**: small.
