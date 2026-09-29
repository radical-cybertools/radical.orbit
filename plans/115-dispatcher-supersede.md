# Plan: #115 — TaskDispatcher: tasks superseding older ones of the same simulation

Issue: https://github.com/radical-cybertools/radical.orbit/issues/115
Status check (2026-08-25): **not started** — no supersede/simulation-identity
concept anywhere in the dispatcher. `TaskRecord`
(`task_dispatcher_state.py:109`) has `task_id` as the only identity; the only
"new submission meets old record" logic is the same-`task_id` resubmit ladder
(`_route_submit` lines 984–998, `_route_submit_rh` 1128–1134), which
*attaches* to a live task — the opposite of superseding.

## Use case (from the issue)

xGFabric: data event → request resources → sim runs. A newer data item for
the *same simulation* arrives while the old task is queued/running (or while
the allocation is still coming up) → the new task should supersede (cancel)
the outdated ones.

## Design

### 1. Identity: a `sim_id` grouping key + monotone ordering

Add to `TaskRecord` (task_dispatcher_state.py):

- `sim_id: Optional[str] = None` — the grouping key ("which simulation this
  task belongs to"). `None` = no grouping, current behavior.
- `generation: int = 0` — caller-supplied monotone version (the data-event
  sequence). Fall back to `arrival_ts` ordering when callers don't set it.

`records_from`/`records_to` (task_dispatcher_state.py:185–207) are generic
dataclass round-trips, so new fields with defaults are backward-compatible
with existing on-disk pool ledgers — verify with a load-old-ledger test.

Wire-in points:
- exec-style `_route_submit` (plugin_task_dispatcher.py:946): accept
  `sim_id` / `generation` in the request body.
- rhapsody dialect `_route_submit_rh` (1083): read them from each task dict
  (top-level keys; they ride in `task_dict` verbatim anyway, but promote to
  record fields explicitly).
- `TaskDispatcherClient.submit_task(s)`: pass-through kwargs.

### 2. Supersede semantics (at submit time)

On submit of a task with `sim_id` set, within the same `(sid, pool)` scope
(cross-pool supersede is out of scope v1):

1. Find live siblings: records with the same `sim_id`, state in
   {QUEUED, RUNNING}, and `generation` (or `arrival_ts`) **older** than the
   incoming task.
2. For each, call the existing `_cancel_task(pool_state, rec)` primitive
   (line 1991) — it already handles QUEUED (pure state flip + notify) vs
   RUNNING (best-effort `rh.cancel_task` on the child endpoint +
   `pilot.in_flight` decrement). Both submit routes are already `async`.
3. Mark the cancelled records with `superseded_by: task_id` (new optional
   `TaskRecord` field) and cancel-reason in `error` (e.g.
   `'superseded by <task_id>'`) so clients can distinguish supersede-cancel
   from operator-cancel in notifications.
4. **Stale-arrival guard** (the important inverse): if an incoming task's
   `generation` is *older* than an already-present sibling (late/reordered
   event), do not run it — record it directly as CANCELED/superseded (or
   reject with a clear response; decide at review — recording it keeps the
   ledger complete, which suits the research-vehicle use).
5. One `persist()` + one `_drain_pending` per touched pool, as today.

Terminal-race note: a supersede-cancelled RUNNING task may still complete on
the endpoint before the cancel lands; `_handle_task_terminal` (1822) must not
resurrect a CANCELED-superseded record (check current terminal handling —
add a guard if terminal events overwrite state unconditionally).

### 3. Opt-in / config

Supersede activates purely on presence of `sim_id` on the incoming task —
no pool-config knob needed for v1 (no `sim_id`, no behavior change). If a
mode toggle is wanted later ('cancel' vs 'drop-new' vs 'keep-both'), it goes
in `PoolConfig.strategy_config` (task_dispatcher_config.py:52).

### 4. Not policy-layer

Considered putting this in `DispatchPolicy` — rejected: supersede is a
correctness/lifecycle rule at submit time, not a placement choice.
`pick_dispatch` still sees only the surviving pending tasks. (Policies can
later use `sim_id` for smarter placement; out of scope.)

## Tests (tests/unittests/test_plugin_task_dispatcher.py + state tests)

- Submit newer generation while sibling QUEUED → old CANCELED with
  `superseded_by`, new runs; notification carries the supersede reason.
- Sibling RUNNING → `rh.cancel_task` invoked (mock), in_flight decremented.
- Stale arrival (older generation) → not executed.
- No `sim_id` → resubmit ladder behavior byte-identical (regression).
- Same `task_id` + `sim_id` interplay: `task_id` ladder wins first (attach),
  supersede only fires across *different* task_ids.
- Ledger: old-format JSON (no new fields) loads; round-trip with new fields.
- `arrival_ts` fallback ordering when `generation` unset on both.
- Terminal event for a superseded task does not overwrite CANCELED.

## Effort

Medium (~1 focused PR): state fields + both submit routes + cancel-reason
plumbing + tests. The heavy lifting (`_cancel_task`, persistence, notify)
already exists.
