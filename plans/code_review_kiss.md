# ORBIT — KISS / readability code review (synthesis)

Six independent no-context reviewers (Fable), one per module cluster, each judging
the code only against a use-case/requirements brief with a heavy KISS + academic-
readability mandate — with the implementation plans and our design discussions
explicitly withheld, so nothing is accepted on cited-but-unavailable authority.

**Full per-cluster findings** (with `file:line`, "simpler alternative", and
essential/accidental judgement for every item) live alongside this file in
`plans/code_review_kiss/{1_wire_broker,2_runtime_gateway,3_plugin_framework,4_dispatcher,5_hpc_plugins,6_integration_plugins}.md`;
`plans/code_review_kiss/0_brief.md` is the exact brief the reviewers were given
(and the only non-code input they had). This file is the ranked, de-duplicated
synthesis for our discussion. **No code has been changed.** 87 findings total
(~18 high, ~44 medium, ~25 low).

## Overall verdict

The **core is leaner than its reputation**: the wire envelope, the broker routing
loop, both `bin/` scripts, `dispatch.py`, `plugin_session_base.py`, `http_utils.py`,
`_prof.py`, and the small plugins (lucid, sysinfo, staging, globus-happy-path,
queue_info-happy-path) are cited by the reviewers as right-sized and readable. The
architecturally load-bearing complexity — the two-loop transport isolation, the
callback-dispatcher thread, the `_RuntimeHTTP` transport seam — is repeatedly
**defended as essential**.

The problems cluster in three bands, in priority order:

1. **Documentation debt and dead code** that actively misleads a reader — the
   cheapest, safest, highest-readability wins. Nearly unanimous across reviewers.
2. **Over-built *implementations* of genuinely-essential behaviors** — the
   requirement is real, but a student-simpler form exists (custom queues, WAL store,
   watchdog, two-tier backpressure, the sync/async coroutine driver).
3. **Speculative generality and a few structural bets** — machinery no current
   requirement needs (pluggable-strategy framework, telemetry forecasting, dual
   route registration), plus a handful of decisions worth re-litigating (dispatcher-
   in-broker, endpoint-mode).

A separate, important output: the reviewers found **real latent bugs** (below) that
merit fixing on correctness grounds independent of the KISS discussion.

---

## A. Latent bugs found (fix regardless of the KISS decisions)

| # | Bug | Location | Sev |
|---|-----|----------|-----|
| A1 | Zero-config path submits pilots to a batch queue literally named `"default"` — the docstring promises an override that `_materialise_pool` never performs. Fails on most schedulers; the first thing a student hits. | `task_dispatcher_config.py:285-288`, `plugin_task_dispatcher.py:573-624` | high |
| A2 | `deregister_dynamic_plugin` pops only `_sessions`, leaking `_session_policy` + `_session_last_access` and skipping drain-timer cancellation; it hand-reimplements teardown through privates instead of `await plugin.shutdown()`. | `plugin_host_base.py:241-274` | med |
| A3 | `_host_broadcast` no-ops on `topic=='topology'` despite a docstring promising a rebroadcast → dynamically-registered hosted plugins (the `iri.<endpoint>` flow) never trigger a topology update. Also a sync-fn-returning-a-coroutine shim. | `broker.py:820-845` | high |
| A4 | psij `job_status` can double-fire: the submit-callback closure and the poll loop keep **independent** dedupe stores. | `plugin_psij.py:274-300` vs `439-466` | med |
| A5 | Rhapsody's `register_session` override skips the base `_check_owner`, so rhapsody sessions silently lack the owner-reattach (403) protection every other plugin gets. | `plugin_rhapsody.py:1514-1632` | med |
| A6 | `_inflight` correlation table has no cap and no timeout — a forwarded request whose responder never answers leaks forever; the "per-src cap" only guards the broker's own calls. | `broker.py:237, 928-955` | med |
| A7 | Silent drops with no log line: user callbacks (`CallbackDispatcher`), thread-origin notifications before `_main_loop` is primed (`_dispatch_notify`), gateway events before the loop is lazily captured. Each is an undebuggable "my callback never fired". | `runtime_support.py:108-114`, `plugin_base.py:340-346`, `gateway.py:275-277` | med |
| A8 | `_route_pool_detail` returns *any* session's pool (pilots + last 50 task records) by name with no sid — a cross-session read backdoor under an otherwise strict-isolation model. | `plugin_task_dispatcher.py:961-970` | low/sec |
| A9 | xgfabric classifies any endpoint whose name *contains* `'ucsb'` as immediate — an invisible site rule that misfires (`'ucsb-test'`), when the general `queue_info`-presence rule one branch below already covers it. | `plugin_xgfabric.py:421-423` | low |

---

## B. Cross-cutting themes (highest leverage — each spans many modules)

### B1. Documentation debt that misleads (unanimous; low effort, low risk)
Docstrings and comments across the tree cite **artifacts a student cannot see**:
`service.py` / `EndpointService` (deleted; referenced throughout `runtime.py`,
`dispatch.py`, `runtime.py` cert logic), milestone tags ("M0 lesson #2", "pre-flip
item 1", "M5/M6/M7"), plan/memory doc pointers (`plans/...`, `memory/...`, "§5.1"),
and mechanical-**rename manglings** ("lendpointr"=ledger, "wendpoint"=wedge in the
dispatcher). Several docstrings state the *opposite* of the code (A3; the
`task_dispatcher` class docstring says "Endpoint-side" while it is broker-only; the
`pools.json` config story is dead). *Reviewers: R1#6, R2#4, R3 (throughout), R4#10;
also found independently in `plugin_base.py` during this session.*
→ **Recommend: a single docstring/comment sweep. Strip milestone/plan tags, fix the
deleted-module references, repair the mangled words, delete docstrings that lie.**

### B2. Dead code presented as real contract (high leverage)
Verified-unused modules/machinery that a student will study and design against:
`exceptions.py` (153 lines, zero prod importers; shadows builtins `ConnectionError`/
`TimeoutError`) [R3#4]; the correlation-ID contextvar in `logging_config.py` [R3#5];
`validate_ui_config` + the Pydantic UI models (prod passes dicts through untouched)
[R3#8]; `protocol.peek_routing` (the "broker fast path" the broker never calls) +
`channel`/`capabilities`/`is_binary` reserved fields [R1#2]; the `_watch_task`
subsystem in rhapsody (dead, with a live comment pointing at it) [R6#2];
`_assert_json_serializable` [R6#13]; `load_pools`/`pools.json` [R4#10];
`BrokerPluginHost.on_topology_changed` (always `None`) [R1#12]; four back-compat
shims in queue_info [R5#8]; xgfabric's created-but-never-used HTTP client + cert
resolution [R6#11].
→ **Recommend: delete, or (for reserved protocol fields) accept they return in a v2
bump. Biggest single readability win after B1.**

### B3. The same small structure re-implemented N times (duplication)
- **Bounded drop-oldest queue** written 4×: `broker_events._OutQueue`,
  `gateway._SSEQueue`, `plugin_replay`, `runtime_support` (threaded) — with subtly
  divergent drop policies [R1#7, R2#16].
- **Background status-poll loop** triplicated across globus / iri_instance / psij,
  each independently swallowing *all* errors at debug level (a failing poller on an
  expiring token is invisible) [R6#10]; the dispatcher separately runs 5 such loops
  where 1 housekeeping tick would do [R4#7].
- **`subprocess.run` wrapper** re-typed ~8× across the batch/queue backends, already
  diverging (timeouts 10 vs 60, `check=True` vs manual) [R5#9].
- **Session teardown block** (pop-three-dicts + close + log) copied 4× in
  `plugin_base.py`, and the 5th site (A2) forgot part of it [R3#6].
- **queue_info handler/param boilerplate** 5× server + 5× client [R5#10];
  **response-dict→Response unpack** 3× [R2#14].
→ **Recommend: one shared helper each. Mechanical, low-risk, and each removes a
place for silent drift.**

### B4. Sync/async client duplication — *and this challenges a change we shipped this session (PR #76)*
Three reviewers independently flag the `_run_sync` + `_arequest` + `a<method>`-twin
pattern as too clever and inconsistent: `_run_sync` drives an async core with raw
`coro.send(None)` relying on a cross-module "never suspends" invariant [R2#2]; only
psij and rhapsody carry async twins while every sibling client is plain sync, so the
rule for "when does a method need an `a`-twin" is invisible and the twins already
behave differently from their sync siblings [R5#6, R6#5]. Their proposed simpler
shape: **push the "don't block the host loop" concern into the one place that has it —
the broker-hosted dispatcher — (e.g. `asyncio.to_thread` at the dispatcher layer, or
generic async passthroughs on `PluginClient`) and delete the per-method twins from
plugin files.** *(Note: R5#13 also flags that the route-path constants added in #76
make psij inconsistent with sibling plugins that inline paths.)*
→ **Flagged for discussion: this partially re-opens the transport-convergence design
I approved earlier today. The reviewers make a credible case it went one layer too
deep. Worth weighing before we build further on it.**

### B5. Plugins reaching around the base session lifecycle (with a real behavioral gap)
rhapsody [R6#8] and globus [R6#9] each re-implement `register_session` by hand in
*different* ways, because the base `_open_session` can't accept session-constructor
kwargs — rhapsody duplicates most of the base path (and loses the owner check, A5);
globus skips the lifetime/owner record entirely. One small base hook
(`_open_session(..., **ctor_kwargs)` + an overridable `_on_session_created`) fixes
both and restores the owner protection.
→ **Recommend: add the base hook; collapse both overrides.** Highest-value framework
change after the dead-code sweep, because it makes *every* future plugin simpler.

---

## C. Over-built implementations of essential behaviors (requirement real, form heavy)

Each of these serves a requirement the reviewers agree is essential — the challenge
is to the *implementation*, and each has a concrete stdlib-shaped alternative.

| Item | Essential requirement | Simpler form | Reviewer |
|------|----------------------|--------------|----------|
| Custom `Handoff` coalesced queue (and its false "bounded" claim — counts overflow, never drops; unread counter) | transport thread must never block | `loop.call_soon_threadsafe` / one `asyncio.Queue`; ~50 fewer lines | R2#3 |
| `CallbackDispatcher` hand-rolled CV worker + silent drop | user callbacks off the work loop | `queue.Queue` + daemon worker + a `log.warning` on drop | R2#7 |
| Hand-rolled WAL store (append-JSONL + snapshot + 2-trigger compaction + O_APPEND-truncate trick + sweeper) | pool/pilot durability across restart | one `state.json` per pool, atomic rewrite (routine already exists); replay = `json.load` | R4#3 |
| Loop-lag watchdog + suppression window + injected clock (a 3rd liveness layer) | don't mass-declare lost after a stall | suspect+grace already covers it (timers live on the stalled loop); delete, or a 1-line `resumed_at` stamp | R1#5 |
| Two-tier request backpressure (counter + semaphore + 2 knobs) | 503 fast-fail so a flood can't wedge the work loop | one counter, one cap | R2#6 |
| Session model: ~450 of `Plugin`'s 925 lines (per-owner drain *tasks*, `default` special-cased 4×, `_default_lock` guarding a race that can't occur on one loop) | owner-loss reclaim (use case 7) | one `SessionRecord`, one deadline fn, reuse the existing 5s sweep, `default` created in `__init__` | R3#2, R3#11, R3#6 |
| Server-side replay cursor store (not durable despite the "durable" pitch — dies with the broker; the client already tracks position) | replay-after-reconnect | stateless `fetch(after_seq)`; delete cursor/TTL/prune/drop_cursor | R4#12 |
| `_tunnel_watcher`: 230-line, 4-concern coroutine serving both modes | outbound-only firewall traversal | split per mode; move rendezvous-file plumbing into `tunnel.py` | R5#1 |
| `_parse_allocated_port`: 65 lines of non-blocking-fd + `select` machinery | parse sshd port with timeout | reuse the existing stderr-drain thread; ~15 straight-line lines | R5#7 |

---

## D. Speculative generality (no current requirement)

- **Pluggable-strategy framework** for the dispatcher: ABC + 8-callable `StrategyContext`
  + triple loader (builtin dict / entry points / dotted path), all for **one** real
  strategy; the 2nd exists only to demo the ABC and ships in the production package,
  duplicating the real one. [R4#2, R4#8 telemetry/forecasting, R4#9 example strategy]
- **Notification-batching knobs** threaded through 5 layers; no caller sets them [R6#7].
- **Client-side template compression + homogeneity check + thread-pool submit** (block
  duplicated verbatim ×2) in the rhapsody client; only frame-size *batching* is required
  [R6#3].
- **16-param `EndpointRuntime` ctor** / **18-param `Broker` ctor**, most being test
  seams living in the production API [R2#5, R1#14].
- **Plugin-selection mini-DSL** (5 token forms incl. ambiguous prefix-match) + two
  discovery mechanisms + silent `except ImportError` that hides *broken* plugins [R3#7].

---

## E. Structural bets worth re-litigating (bigger, discuss first)

These challenge decisions we made deliberately; they may be right, but the reviewers'
independent push-back is worth taking seriously.

1. **Dispatcher hosted *inside* the broker via privileged `broker_caller`/`broker_tap`
   seams** vs. being an ordinary participant (an `EndpointRuntime` that consumes the
   public psij/rhapsody clients + `register_callback`). The reviewer argues the star
   model was built precisely so this workflow logic could live at an edge, and that
   broker-residency is what forces the durable-store recovery machinery. [R4#1] —
   *largest single simplification available; also the most consequential to discuss.*
2. **Dual route registration** (FastAPI + a hand-rolled regex table) where **production
   only ever uses the direct table** — the FastAPI half is test scaffolding that leaked
   into the plugin-authoring API (drags `fastapi`/`starlette` types, two request/response
   conventions, duck-typing in two places). Replace the `app: FastAPI` ctor param with a
   small `HostContext`; point tests at the production dispatch path. [R3#1] — large
   effort, but it's the plugin interface every future use case touches.
3. **"Endpoint mode"** — a second dispatch path that its own docstring calls a
   transparent proxy, yet carries a durable ledger, replay filter, compaction branch,
   and an if-branch atop 5 routes; consumers can already reach that rhapsody directly.
   [R4#4]
4. **Two correlation tables → one**; **`get_ui_modules` thread-hop that blocks whichever
   loop it runs on** (the wrap-in-coroutine doesn't make the file I/O non-blocking);
   **`_synthesize_lost` re-derived in every consumer** vs. the broker emitting one real
   tombstone. [R1#3, R1#10, R2#8]
5. **`logging_config` reconfigures global logging at import** (invasive when ORBIT is
   used as a library, which the docs advertise) and **hardcodes the plugin name
   `'rhapsody'`** in core infra [R3#5]; **`DEFAULT_PLUGINS_BY_ROLE` bakes domain plugin
   names into the semantics-agnostic host** [R3#12] — both violate "core stays
   semantics-agnostic".

---

## F. What earns its keep (defended by the reviewers — do NOT simplify)

- The **transport-thread / work-loop split** and the **callback-dispatcher thread** (as
  threads) — directly serve "a slow/blocking plugin must not look like a dead host". The
  fat is in their *queue implementations* (C), not the split.
- `_RuntimeHTTP` / `_pack_request` — the minimal seam that lets 11 plugin clients ride
  the WS unchanged; the right way to meet the uniform-contract requirement.
- The **msgpack envelope**, the **lean routing loop** (`_route_frame` is ~15 lines), and
  **both `bin/` scripts**.
- **`dispatch.py`**, **`plugin_session_base.py`**, **`http_utils.py`**, **`_prof.py`**,
  and the **small plugins** (lucid is called a model 240-line plugin) — right-sized.
- The gateway's **auth middleware + header hygiene** — proportionate and well-commented.
- The **dynamic-instance pattern** (`iri_connect` → `PluginIRIInstance` via
  `register_dynamic_plugin`) — cited as the *right* use of the framework.

---

## Suggested triage for our discussion

- **Tier 0 — just do it** (safe, high readability, low risk): B1 (doc/rename sweep),
  B2 (delete dead code), the A-list bugs A1/A3/A4/A5 (small, correctness).
- **Tier 1 — clear wins, modest effort**: B3 (de-duplicate the 4 structures), B5 (base
  session-kwargs hook), C's watchdog / backpressure / replay-cursor / handoff / callback
  queue, D's ctor-knobs and example-strategy relocation.
- **Tier 2 — decide first, then possibly large**: B4 (re-open the sync/async convergence),
  E1 (dispatcher as participant), E2 (single dispatch path), E3 (drop endpoint-mode),
  the session-model slimming, the two-hierarchy scheduler merge (R5#3), pluggable-strategy
  collapse (D).

Nothing here is actioned. Recommend we walk Tier 0/1 for quick agreement, then debate
Tier 2 item by item — several Tier 2 items touch decisions we made for reasons the
reviewers were deliberately not shown, so those deserve a genuine two-sided weighing.
