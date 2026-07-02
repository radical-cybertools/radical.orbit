# Task Dispatcher + Makeflow Integration — Implementation Plan

Goal: run Makeflow DAGs across multiple HPC resources using radical.edge as
the execution substrate. Makeflow keeps full ownership of DAG resolution,
scheduling, priorities, retries, and `makeflowlog` recovery. radical.edge
supplies: (a) a new edge-side **task dispatcher** plugin that owns a fleet
of pilots with pluggable autoscaling/routing strategies; (b) a client-side
wrapper CLI that turns each Makeflow rule into a dispatcher submission plus
file staging; (c) a `.makeflow` preprocessor that rewrites rule commands.

Three passes. Pass 1 is the plugin core. Pass 2 is the client-side CLI and
Makeflow integration. Pass 3 is the pluggable-strategy research surface.

All existing tests (231+) must stay green throughout.

---

## Architecture at a glance

```
Client host
  Makeflow (-T local, unchanged)         persistent via *.makeflowlog
    ↓   rewritten rule command
  radical-edge-run  --edge=<e> --pool=<p> \
                    --in=... --out=... --priority=... -- <cmd>
    ↓   bridge RPC (stage_in → submit → wait (SSE) → stage_out)
┌──────────── Edge on login node ────────────┐
│  plugin_task_dispatcher  (new)              │
│   • pool config  (pilot sizes, walltime,    │
│                   backend, max_pilots, …)   │
│   • strategy     (pluggable; default =      │
│                   ConservativeStrategy)     │
│   • pilot fleet  (ledger, psij submissions) │
│   • task queue   (priority; pending/active) │
│   • staging area (shared-FS scratch)        │
└──────┬──────────────────────────────────────┘
       │ psij.submit_tunneled  →  child edges as pilots
       ▼
  Pilot #1 … Pilot #N   (child edges on compute nodes)
    loaded plugins: rhapsody + staging
    rhapsody backend per pool config
```

---

## Naming

- Plugin file:     `src/radical/edge/plugin_task_dispatcher.py`
- Plugin class:    `PluginTaskDispatcher`
- Session class:   `TaskDispatcherSession`
- Client class:    `TaskDispatcherClient`
- `plugin_name`:   `"task_dispatcher"`
- Wrapper CLI:     `bin/radical-edge-run`
- Preprocessor:    `bin/radical-edge-makeflow-prep`
- Pilot wrapper:   `bin/radical-edge-pilot-wrapper.sh`
- Pool config:     `~/.radical/edge/task_dispatcher/pools.json`
- State dir:       `~/.radical/edge/task_dispatcher/state/`
- Scratch dir:     `~/.radical/edge/task_dispatcher/scratch/<pool>/<task_id>/`

---

## Pass 1 — Dispatcher Plugin

### Step 1: Pool config schema and loader

**New file:** `src/radical/edge/task_dispatcher_config.py`

```python
@dataclass
class PilotSize:
    nodes: int
    cpus_per_node: int
    rhapsody_backend: str               # required — no cascade, explicit per size
    gpus_per_node: int = 0
    walltime_sec: int = 3600

@dataclass
class PoolConfig:
    name: str                                 # unique per edge
    queue: str                                 # batch queue name
    account: str | None                        # charge account / project
    pilot_sizes: dict[str, PilotSize]          # named sizes; strategy picks by hint
    default_size: str                          # key into pilot_sizes
    min_pilots: int = 0
    max_pilots: int = 4
    scratch_base: str | None = None            # None → default scratch path
    strategy: str = 'conservative'             # entry-point name or 'module:ClassName'
    strategy_config: dict = field(default_factory=dict)
```

Backend is declared **explicitly** on every `PilotSize` — no pool-level
default, no cascade. Single-backend pools repeat the same string across
their sizes; this is a deliberate verbosity trade to keep the pilot ↔
backend mapping obvious and to avoid silent-inheritance confusion when
mixed-backend pools get added. The config loader rejects any `PilotSize`
where `rhapsody_backend` is missing or empty.

- `load_pools(path)` reads JSON (stdlib), returns `dict[str, PoolConfig]`.
- Raises `ValueError` with actionable messages on schema errors.
- No hot-reload in Pass 1. Reload requires dispatcher restart.
- Named `pilot_sizes` gives the strategy a menu — default `pilot_sizes = {"default": PilotSize(...)}` keeps config minimal when only one size is needed.

**Tests:** `tests/unittests/test_task_dispatcher_config.py`
- parse / round-trip / schema errors / multiple pools on one edge / multiple sizes per pool.

---

### Step 2: Strategy interface

**New file:** `src/radical/edge/task_dispatcher_strategy.py`

Strategy owns three concerns:

1. **Pilot submission** — what pilot to submit, when, how big.
2. **Task dispatch** — which task off the pending queue runs on which pilot.
3. **Pilot termination** — when to cancel a pilot (beyond walltime expiry).

Pilot replacement emerges from (1)+(3); no separate hook needed.

Abstract base:

```python
class DispatchStrategy(ABC):
    def __init__(self, pool: PoolConfig, cfg: dict): ...

    # -- signals (drive decisions) --------------------------------------

    @abstractmethod
    def on_task_arrived(self, ctx: StrategyContext, task: TaskRecord) -> None:
        """A new task entered the pending queue."""

    @abstractmethod
    def on_pilot_state(self, ctx: StrategyContext, pilot: PilotRecord,
                       old_state: str, new_state: str) -> None:
        """A pilot transitioned (PENDING→STARTING→ACTIVE→DONE|FAILED)."""

    @abstractmethod
    def on_task_finished(self, ctx: StrategyContext, task: TaskRecord,
                         pilot: PilotRecord) -> None:
        """A task reached terminal state. Pilot has freed capacity."""

    def on_tick(self, ctx: StrategyContext) -> None:
        """Periodic wake-up (~5 s). Default no-op; override for rate-
        limiting or time-triggered policies."""

    # -- dispatch decision ----------------------------------------------

    @abstractmethod
    def pick_dispatch(self, ctx: StrategyContext) -> \
            tuple[TaskRecord, PilotRecord] | None:
        """Select a (task, pilot) pair from the pending queue to dispatch
        now. Return None when nothing should be dispatched (queue empty,
        or strategy chooses to hold). Called repeatedly until it returns
        None, so a strategy may drain multiple tasks per invocation
        window."""

    # -- termination decision -------------------------------------------

    def should_terminate_pilot(self, ctx: StrategyContext,
                               pilot: PilotRecord) -> bool:
        """Default: never (pilot expires at walltime). Override for
        drain-on-idle, post-failure abandonment, dynamic right-sizing."""
        return False
```

`StrategyContext` (plugin-provided; read-only state + narrow action hooks):

| Accessor / Action            | Returns / Effect |
|------------------------------|------------------|
| `ctx.now()`                  | Monotonic timestamp |
| `ctx.pool`                   | `PoolConfig` |
| `ctx.pending_queue()`        | `list[TaskRecord]`, priority-ordered snapshot |
| `ctx.pilots()`               | `list[PilotRecord]`, all non-terminal pilots |
| `ctx.arrivals_window(sec)`   | `list[float]` arrival timestamps over last `sec` |
| `ctx.pilot_lag_history()`    | `list[float]` observed PENDING→ACTIVE durations |
| `ctx.submit_pilot(size_key)` | Schedule a new pilot submission. `size_key` selects from `pool.pilot_sizes`; `None` → `pool.default_size` |
| `ctx.cancel_pilot(pid)`      | Terminate a pilot (cancel its psij job) |
| `ctx.drain_pilot(pid)`       | Stop routing new tasks to this pilot; running tasks finish |
| `ctx.logger`                 | Plugin logger |

Strategy never touches psij, rhapsody, or the bridge directly — only the
context. Keeps the research surface clean and isolates strategies from
plumbing changes.

Strategies are loaded via Python entry point
`radical.edge.task_dispatcher.strategies` or by dotted `"module:ClassName"`
in the pool config.

---

### Step 3: Default `ConservativeStrategy`

**New file:** `src/radical/edge/task_dispatcher_strategy_conservative.py`

Policy:
- `on_task_arrived`: no eager scale-up. `pick_dispatch` will route the task
  if a pilot has capacity; otherwise it stays queued.
- `on_tick`: if `pending_queue > sum(free_capacity_of_active_pilots)` **and**
  `submitted_not_yet_active < min(max_in_flight_submissions,
  max_pilots - fleet_size)`, call `ctx.submit_pilot(None)` once. Next tick
  may submit another; conservative = bounded in-flight submissions.
- `on_pilot_state`: when a pilot turns ACTIVE, the next `pick_dispatch`
  invocations drain the pending queue onto it. No direct action taken here.
- `on_task_finished`: no direct action; freed capacity will be used by the
  next `pick_dispatch`.
- `pick_dispatch`: highest-priority pending task (tie-breaker: arrival
  order) paired with an ACTIVE pilot having `in_flight < capacity`.
  Among candidate pilots, prefer fewest `in_flight`; break ties by
  youngest (most remaining walltime).
- `should_terminate_pilot`: always False. Pilots expire naturally.

Strategy knobs (`strategy_config`):
- `min_dwell_sec=30`     — min time to wait after submitting a pilot before
                           submitting another
- `max_in_flight_submissions=2`
- `router_preference='least_loaded'`  (alt: `'youngest'`)

**Tests:** `tests/unittests/test_task_dispatcher_strategy_conservative.py`
- arrivals without scale-up when capacity exists
- single-pilot submission under sustained backlog
- bounded in-flight submissions (`min_dwell_sec` respected)
- `pick_dispatch` priority ordering
- routing prefers least-loaded / youngest per config
- no termination of idle pilots

---

### Step 4: Persistent state & records

**New file:** `src/radical/edge/task_dispatcher_state.py`

```python
@dataclass
class PilotRecord:
    pid: str                         # dispatcher-local id
    pool: str
    psij_job_id: str
    child_edge_name: str | None
    state: str                       # PENDING|STARTING|ACTIVE|DRAINING|DONE|FAILED
    submitted_at: float
    active_at: float | None
    capacity: int                    # concurrent tasks it can run
    in_flight: int
    started_tasks: int               # monotonic counter
    walltime_deadline: float

@dataclass
class TaskRecord:
    task_id: str
    pool: str
    cmd: list[str]
    cwd: str
    priority: int                    # higher = earlier
    inputs: list[str]
    outputs: list[str]
    state: str                       # QUEUED|RUNNING|DONE|FAILED|CANCELED
    pilot_id: str | None
    submitted_at: float
    started_at: float | None
    finished_at: float | None
    exit_code: int | None
    arrival_ts: float
```

Persistence: one JSONL file per record type under
`~/.radical/edge/task_dispatcher/state/<pool>/`. Append-only event log
(`pilot.log`, `task.log`); a snapshot is taken every N minutes or on
clean shutdown. On plugin startup the log is replayed into memory; any
pilot whose child edge no longer shows up in the bridge topology is
marked `DONE` if its tasks produced outputs, else `FAILED`.

Recovery contract: Makeflow's own log plus dispatcher's log together
cover end-to-end recovery. The dispatcher never **re-executes** a task
whose record is `DONE`; it returns cached exit_code on repeated submit.

**Tests:** `tests/unittests/test_task_dispatcher_state.py`
- append/replay / crash mid-log / stale pilot reconciliation.

---

### Step 5: Plugin skeleton

**New file:** `src/radical/edge/plugin_task_dispatcher.py`

```python
class PluginTaskDispatcher(Plugin):
    plugin_name   = 'task_dispatcher'
    session_class = TaskDispatcherSession
    client_class  = TaskDispatcherClient
    version       = '0.0.1'

    @classmethod
    def is_enabled(cls, app) -> bool:
        # login-node edges only (dispatcher submits pilot jobs)
        from .batch_system import detect_batch_system
        return not detect_batch_system().in_allocation()
```

One internal client held by the plugin instance:

- `self._bc` — `BridgeClient(url=<bridge_url>)` used for **everything**:
  - Same-edge psij calls (pilot submit/cancel/status) via
    `self._bc.get_edge_client(self_edge_name).get_plugin('psij')`.
  - Child-edge rhapsody calls via
    `self._bc.get_edge_client(child_edge).get_plugin('rhapsody')`.
  - SSE subscription to hear `task_status` notifications from pilots.

  Rationale: pilot submission latency is queue-bound (minutes–hours); bridge
  RTT is milliseconds. A separate loopback transport saves nothing and
  doubles the error-handling surface. Reentrancy (edge → bridge → same edge)
  works under async without deadlock as long as handlers `await` rather
  than sync-block.

  `self_edge_name` comes from `self._app.state.edge_name`, set in
  `EdgeService.__init__` (service.py:127) *before* plugins are loaded
  (service.py:141). Available synchronously at plugin `__init__`.

Routes (under `/task_dispatcher/`):

| Route                                  | Method | Purpose |
|----------------------------------------|--------|---------|
| `pools`                                | GET    | List configured pools and their live state |
| `pool/{name}`                          | GET    | Detailed pool state (pilots, counts, queue depth) |
| `submit/{sid}`                         | POST   | Submit task — `{task_id, pool, cmd, inputs, outputs, priority, resources}` |
| `cancel/{sid}/{task_id}`               | POST   | Cancel a task |
| `task/{sid}/{task_id}`                 | GET    | Get task record |
| `stage_in/{sid}/{task_id}`             | POST   | JSON body `{pool, filename, content_b64, overwrite?}`. Writes one file into the pool's scratch dir; returns `{cwd, size}`. NOTE: bridge-WS forwards JSON, so multipart is not usable; bulk tar-stream / dedicated binary staging plugin is the future optimization path |
| `stage_out/{sid}/{task_id}/{name}`     | GET    | Returns `{filename, size, content_b64}` JSON |
| `fleet/{sid}`                          | GET    | Current fleet snapshot (all pools) |
| `pilot_handshake/{sid}`                | POST   | Called by a newly-started pilot to bind itself to its `PilotRecord`. Payload: `{pilot_id, child_edge, capacity, startup_time}` |
| (built-in) `register_session` etc.     |        | From `Plugin` base |

Notification topics:

| Topic                     | Fired on                                   |
|---------------------------|--------------------------------------------|
| `task_status`             | `QUEUED → RUNNING → DONE/FAILED/CANCELED`  |
| `pilot_status`            | `PENDING → STARTING → ACTIVE → DRAINING → DONE/FAILED` |
| `autoscale_decision`      | When strategy calls `ctx.submit_pilot()`   |

The first is what the wrapper subscribes to. The latter two are for UI /
research observability.

---

### Step 6: Pilot lifecycle

#### Submission

When the strategy calls `ctx.submit_pilot(size_key)`:
1. Dispatcher generates `pilot_id = f"pilot.{uuid4().hex[:10]}"` and
   deterministic `child_edge = f"{pool}_{pilot_id}"`.
2. Builds a psij `JobSpec` from the chosen `PilotSize`:
   - `executable = /absolute/path/to/radical-edge-pilot-wrapper.sh`
   - `arguments  = []`
   - `environment = {...}` — all bootstrap values (see below)
   - resource fields from `PilotSize` (nodes, cpus, gpus, walltime)
   - queue/account from `PoolConfig`
3. Creates `PilotRecord(pid=pilot_id, state="PENDING", ...)` and appends
   to the state log.
4. Calls the local psij plugin via the dispatcher's shared `BridgeClient`:
   ```python
   psij = self._bc.get_edge_client(self_edge_name).get_plugin('psij')
   job  = psij.submit_tunneled(job_spec, tunnel=True)
   ```
   Routes through the bridge rather than loopback — RTT is negligible next
   to queue wait.
5. Stores psij job id in the `PilotRecord`.

#### Bootstrap env vars passed to the pilot

All prefixed `RADICAL_EDGE_*` to avoid collision with user environment.

| Var                              | Purpose |
|----------------------------------|---------|
| `RADICAL_EDGE_PILOT_ID`          | Pilot identity — used in handshake |
| `RADICAL_EDGE_EDGE_NAME`         | Name under which the pilot registers with the bridge |
| `RADICAL_EDGE_PARENT_EDGE`       | Parent edge name — target of handshake POST |
| `RADICAL_EDGE_DISPATCHER_SID`    | Parent dispatcher session id |
| `RADICAL_EDGE_BRIDGE_URL`        | Bridge URL |
| `RADICAL_EDGE_BRIDGE_CA`         | CA bundle path (optional, for mTLS) |
| `RADICAL_EDGE_POOL`              | Pool name (logging / self-identification) |
| `RADICAL_EDGE_RHAPSODY_BACKEND`  | Backend for the pilot's rhapsody session — the `rhapsody_backend` value of the `PilotSize` this pilot was submitted for |
| `RADICAL_EDGE_SCRATCH_BASE`      | Shared-FS scratch path for this pool |

Values are strings <1KB total. CLI args are not used (process-table leakage;
escaping hazards).

#### Pilot wrapper (`bin/radical-edge-pilot-wrapper.sh`)

```sh
#!/usr/bin/env bash
set -euo pipefail
export RADICAL_EDGE_EDGE_NAME        # consumed by service startup
# start a child edge service preloaded with [rhapsody, staging]:
exec radical-edge-wrapper.sh \
    --plugins rhapsody,staging \
    --edge-name "$RADICAL_EDGE_EDGE_NAME" \
    --bridge-url "$RADICAL_EDGE_BRIDGE_URL" \
    --handshake-callback
```

A `--handshake-callback` flag on `radical-edge-wrapper.sh` (small addition
to the existing wrapper) triggers a handshake POST once the service is
registered with the bridge. The handshake client is inline Python (avoids
shipping another binary):

```python
import os, time
from radical.edge import BridgeClient
bc = BridgeClient(url=os.environ['RADICAL_EDGE_BRIDGE_URL'])
td = bc.get_edge_client(os.environ['RADICAL_EDGE_PARENT_EDGE']).get_plugin('task_dispatcher')
td.pilot_handshake(
    sid          = os.environ['RADICAL_EDGE_DISPATCHER_SID'],
    pilot_id     = os.environ['RADICAL_EDGE_PILOT_ID'],
    child_edge   = os.environ['RADICAL_EDGE_EDGE_NAME'],
    capacity     = _detect_capacity(),         # from cpu_count/gpu_count
    startup_time = time.monotonic() - _job_start_ts(),
)
```

#### Handshake handler (dispatcher side)

1. Look up `PilotRecord` by `pilot_id` from the log. 404 if unknown
   (rogue or late handshake).
2. Update: `child_edge_name=child_edge`, `capacity=capacity`,
   `state="ACTIVE"`, `active_at=now`.
3. Append transition to log.
4. Emit `pilot_status` SSE notification.
5. Call `strategy.on_pilot_state(ctx, record, "STARTING", "ACTIVE")`.
6. Call strategy's `pick_dispatch` in a loop to drain pending queue onto
   the new pilot.

#### Handshake-absent fallback

If handshake is missing after `max(2 * avg_observed_lag, 60s)`:
- Query psij for the job state.
- Job `DONE`/`FAILED` → mark `PilotRecord` `FAILED`; re-enqueue its
  assigned tasks (if any — should be none, since it never handshook).
- Job still `PENDING`/`RUNNING` → keep waiting up to walltime; on walltime
  expiry with no handshake → `FAILED`.

`on_topology_change` is used as a redundant signal: if a child edge with a
name matching `<pool>_pilot_*` appears before handshake lands, the
dispatcher notes the match but still awaits handshake for capacity data.

#### Termination

Strategy-driven. `ctx.cancel_pilot(pid)` cancels the psij job via the
loopback HTTP client. No `DRAINING` state is a mandatory lifecycle step;
strategies that want drain-semantics can call `ctx.drain_pilot(pid)`
separately, which only flips a per-pilot `accepting_new_tasks=False` flag.
A pilot that is not accepting new tasks is invisible to `pick_dispatch`
for new assignments; running tasks finish normally.

**Tests:** `tests/unittests/test_task_dispatcher_pilot_lifecycle.py`
- mocked psij submit; simulated handshake arrival; capacity recording;
  handshake timeout → FAILED; cancel-driven termination; drain flag.

---

### Step 7: Dispatch loop & queue

Task arrival flow:
1. `submit_task` handler validates payload. If cached record exists:
   - `DONE` → return cached result (crash-recovery idempotency).
   - `FAILED`/`CANCELED` → overwrite and re-execute (Makeflow retry).
   - `RUNNING`/`QUEUED` → attach; return current state (wrapper reconnect).
2. Otherwise create `TaskRecord(state="QUEUED", priority=P)`, append to
   log, insert into priority-ordered pending queue.
3. Call `strategy.on_task_arrived(ctx, task)`.
4. Call `strategy.pick_dispatch(ctx)` in a loop until it returns `None`:
   - each hit → `_assign(task, pilot)` → POST to child edge's
     `rhapsody.submit_tasks` via `self._bc` → record `state="RUNNING"`,
     `pilot_id=...`, emit dispatcher's `task_status` SSE.

Paired FIXMEs mark the "per-task backend override" extension point. Each
points at the other by file path so a reader landing on one finds the
other without grep.

- **Dispatcher side — `plugin_task_dispatcher.py::_assign`** (implementer
  view):
  ```python
  # FIXME(per-task-backend): the rhapsody backend used for this task is
  # implicitly inherited from pilot.rhapsody_backend (chosen at pilot
  # submit time via PilotSize.rhapsody_backend). A future extension would
  # call self._strategy.pick_backend(task, pilot) here and, if it returns
  # non-None, override the task's target backend before submit_tasks.
  # Paired extension-point doc in:
  #   task_dispatcher_strategy.py::DispatchStrategy (search FIXME per-task-backend)
  ```

- **Strategy side — `task_dispatcher_strategy.py::DispatchStrategy`**
  (research-facing view):
  ```python
  # FIXME(per-task-backend): a future hook
  #   def pick_backend(self, ctx, task, pilot) -> str | None
  # would let a strategy override the rhapsody backend on a per-task
  # basis rather than inheriting pilot.rhapsody_backend. Not part of the
  # v1 ABC. Implementation site marked with the same tag in:
  #   plugin_task_dispatcher.py::PluginTaskDispatcher._assign (search FIXME per-task-backend)
  ```

Shared `FIXME(per-task-backend)` tag makes `grep -r "FIXME(per-task-backend)"`
surface both sites together.
5. Return `{task_id, state, pool}` to the wrapper.

Completion flow:
1. Pilot's rhapsody emits `task_status` SSE on the bridge.
2. Dispatcher's `self._bc` SSE listener (registered at session startup)
   receives it; callback matches rhapsody task uid → dispatcher
   `TaskRecord`.
3. Update state, set `finished_at`, `exit_code`; append to log.
4. Emit dispatcher-level `task_status` SSE (with dispatcher's task_id).
5. Decrement `pilot.in_flight`; call `strategy.on_task_finished(...)`.
6. Call `strategy.pick_dispatch(ctx)` loop to drain queue onto freed
   capacity.

Cancel: dispatcher calls `rhapsody.cancel_task` on the pilot edge via
`self._bc`; on success marks record `CANCELED`; `strategy.on_task_finished`
is called.

Priority ordering: `TaskRecord.priority` is an integer passed through from
Makeflow's `PRIORITY = N` directive. Default queue ordering is
`(-priority, arrival_ts)`. Strategies may override by reading
`ctx.pending_queue()` and picking whatever task they want in
`pick_dispatch`.

---

### Step 8: Staging within a pool

Login-node edge and its pilots share a filesystem on the same cluster
(the `sysinfo` plugin's detector already confirms this). Pattern:

- `stage_in` handler:
  - Reads uploaded files, writes them into
    `pool.scratch_base/<task_id>/` (shared-FS path).
  - Returns that path as the `cwd` for the task.
- Task execution: rhapsody runs with `cwd=<that path>`.
- `stage_out` handler: reads `<cwd>/<filename>` and streams to the client.

No pilot-side staging plugin activity needed for intra-pool movement —
the pilot's rhapsody simply reads/writes the shared-FS directory.

Cross-pool file movement is the wrapper's job: Makeflow's declared inputs
and outputs mean the wrapper pulls outputs back to the client after a
rule finishes, then pushes them up to the next rule's pool. No change
required in the dispatcher.

Scratch cleanup: dispatcher purges `<task_id>` directories older than a
configurable TTL (default 7 days) on each tick. Not aggressive — disk on
HPC scratch is cheap, debugging matters.

---

## Pass 2 — Client-side wrapper + Makeflow integration

### Step 9: `radical-edge-run` CLI

**New file:** `bin/radical-edge-run` (Python script, shebanged)

Usage:

```
radical-edge-run \
  --edge=<edge_name>              # radical.edge edge hosting the dispatcher
  --pool=<pool_name>              # pool defined on that edge's dispatcher
  [--priority=<int>]              # default 0
  [--in <f1> [<f2> ...]]          # declared inputs
  [--out <f1> [<f2> ...]]         # declared outputs
  [--resources key=val ...]       # optional resource overrides
  -- <cmd> [args ...]
```

Flow:
1. `BridgeClient()` (URL from env); `ec = bc.get_edge_client(edge)`;
   `td = ec.get_plugin('task_dispatcher')`; `td.register_session()` the
   first time in a run (pooled and cached per-process by a tiny on-disk
   lock file under `$TMPDIR/radical-edge-run-<run_id>.sid`).
2. `task_id = sha1(cmd + "\0" + "\0".join(sorted(inputs)) + "\0" +
   "\0".join(sorted(outputs)) + "\0" + run_id)[:16]`. `run_id` comes from
   the preprocessor (see Step 10); it is a hash of the input `.makeflow`
   path + mtime. Stable across retries of the same run, changes on edit.
   Dispatcher behavior on resubmit with same task_id:
     - `DONE` → return cached (crash-recovery idempotency)
     - `FAILED`/`CANCELED` → re-execute (Makeflow retry)
     - `RUNNING` → attach to existing wait (wrapper reconnect)
3. Multipart POST inputs to `/task_dispatcher/stage_in/{sid}/{task_id}`.
   NOTE: current implementation sends one file at a time; a bulk
   tar-stream path is the documented optimization when rules have many
   small inputs.
4. `td.submit_task(pool, task_id, cmd, cwd (returned by stage_in),
   priority, inputs, outputs)`.
5. Block on SSE `task_status` for `task_id` until terminal.
   **v1 limitation** (documented in wrapper docstring, example README, and
   "Non-goals" below): live stdout/stderr streaming is not implemented.
   Only the terminal snapshot arrives with the task_status notification.
   For long-running rules, users inspect files under the shared-FS scratch
   dir (`<scratch>/<task_id>/{stdout,stderr}`) via the `staging` plugin
   or ssh. A `TODO(streaming)` comment at the SSE wait loop marks where
   incremental log-chunk handling would be inserted; paired rhapsody-side
   changes would emit a new `task_log_chunk` notification topic.
6. On `DONE`: GET each declared output via `/stage_out/{sid}/{task_id}/<name>`
   into the local Makeflow workdir. On failure: leave outputs missing so
   Makeflow marks the rule failed.
7. On SIGTERM (Makeflow cancelling): call `cancel_task`, exit non-zero.
8. Exit with the task's exit code.

**Tests:** `tests/integration/test_radical_edge_run.py`
- end-to-end against an in-process fake bridge + stubbed dispatcher.
- handles cancel, failure, retry-idempotence (same task_id → cached
  result after DONE).

---

### Step 10: `radical-edge-makeflow-prep` CLI

**New file:** `bin/radical-edge-makeflow-prep` (Python)

Usage:

```
radical-edge-makeflow-prep INPUT.makeflow \
  --output PREPPED.makeflow \
  [--default-edge EDGE] [--default-pool POOL] \
  [--override rule_output=EDGE:POOL ...]
```

Behavior:
- Parse `INPUT.makeflow` rule-by-rule (classic make syntax only in Pass 2;
  JX deferred).
- Read two directives attached per rule (scoped like `CATEGORY`):
  - `EDGE = "edge_name"`
  - `POOL = "pool_name"`
- Read per-rule `PRIORITY = <int>` (optional; passed through unchanged).
- Compute `run_id = sha1(abs_path + mtime_ns)[:16]` once per invocation.
- Rewrite command string:
  ```
  radical-edge-run --edge=EDGE --pool=POOL --priority=P \
                   --run-id=RUN_ID \
                   --in <i1> <i2> --out <o1> <o2> -- <original cmd>
  ```
- Write to `PREPPED.makeflow`; preserve comments, rule ordering, original
  resource directives that Makeflow needs for scheduling.
- Error actionably if a rule has neither directive nor an override; print
  the offending rule range.

Users then run: `makeflow PREPPED.makeflow` as usual.

**Tests:** `tests/unittests/test_makeflow_prep.py`
- directive parsing, scoping, override map, error messages, idempotent
  re-prep (already-prepped rules are passed through).

---

### Step 11: Convenience script

**New file:** `bin/radical-edge-makeflow` (Python)

Single-step wrapper: prep in a temp file, run `makeflow` with any
pass-through flags, clean up. Thin, optional.

---

### Step 12: Example

**New file:** `examples/example_makeflow_multiedge/`

- `workflow.makeflow` — 4 rules across 2 pools on 2 edges, includes one
  cross-pool file dependency.
- `README.md` — step-by-step: how to start the two edges, register
  dispatcher plugins, define pools, prep, run, interpret results. Includes
  a note on the v1 limitation around live stdout/stderr tailing (inspect
  shared-FS scratch dir for live progress).
- `pools.json` — sample pool config with two pools per edge (e.g.,
  `cpu_small`, `cpu_large`) and two named `pilot_sizes` per pool.

---

## Pass 3 — Pluggable strategy research surface

### Step 13: Entry-point registration for strategies

**Modify:** `pyproject.toml`

Add group:

```toml
[project.entry-points."radical.edge.task_dispatcher.strategies"]
conservative = "radical.edge.task_dispatcher_strategy_conservative:ConservativeStrategy"
```

Third-party / in-tree strategies register under the same group. Dispatcher
resolves `strategy` field via `importlib.metadata.entry_points(...)` first,
then falls back to dotted `module:ClassName`.

---

### Step 14: Strategy documentation and sample

**New file:** `docs/task_dispatcher_strategy.md`

- Full `DispatchStrategy` ABC reference.
- `StrategyContext` method reference (signatures + semantics).
- Guarantees: thread-safety model, event ordering, what happens if a
  callback raises.
- One worked example strategy: `aggressive_scale_to_backlog` showing
  immediate scale-up on any backlog with a max-in-flight cap.

**New file:** `src/radical/edge/task_dispatcher_strategy_examples.py`

Ships one additional non-default strategy as a reference, exercised in
unit tests but not wired as a default.

---

### Step 15: Strategy conformance test harness

**New file:** `tests/unittests/test_task_dispatcher_strategy_conformance.py`

A parametric test suite any strategy can opt into via an entry point:
arrival patterns (burst, steady, bimodal), pilot startup-lag profiles,
failure injection. Measures: p50/p95 task-wait, pilot utilization,
strategy decision count. Not a correctness test — a benchmark. Useful
for researchers comparing strategies.

---

## Failure modes & recovery — matrix

| Event | Detection | Action |
|-------|-----------|--------|
| Pilot fails before ACTIVE | psij terminal state arrives first | Mark pilots FAILED; reassign queued tasks; strategy sees `on_pilot_state` |
| Pilot fails mid-task | rhapsody task FAILED via SSE | Mark task FAILED; Makeflow retries per its policy; wrapper re-enqueues → different pilot |
| Pilot walltime reached | psij terminal state arrives | Running tasks reported as FAILED by rhapsody session close → Makeflow retries on a different pilot. Strategy may proactively drain ahead of walltime via `ctx.drain_pilot` |
| Dispatcher plugin restart | State log replayed | Reconcile against bridge topology; orphan pilots marked FAILED if child edge gone |
| Edge service restart | All pilots lose parent; pilots still registered w/ bridge survive | On restart, reconnect; replay state log |
| Client (wrapper) SIGTERM from Makeflow | Wrapper catches signal | Cancels task; dispatcher propagates to pilot; exit non-zero so Makeflow retries |
| Client crash mid-run | Makeflow log intact; dispatcher state intact | Re-running Makeflow replays DAG; dispatcher returns cached results for DONE tasks |
| Cross-pool file missing | Wrapper `stage_out` failure | Exits non-zero; Makeflow retries (pool files still available for diagnosis) |

---

## Directory & file summary

```
src/radical/edge/
  plugin_task_dispatcher.py                 # Pass 1.5
  task_dispatcher_config.py                 # Pass 1.1
  task_dispatcher_strategy.py               # Pass 1.2
  task_dispatcher_strategy_conservative.py  # Pass 1.3
  task_dispatcher_strategy_examples.py      # Pass 3.14
  task_dispatcher_state.py                  # Pass 1.4

bin/
  radical-edge-run                          # Pass 2.9
  radical-edge-makeflow-prep                # Pass 2.10
  radical-edge-makeflow                     # Pass 2.11
  radical-edge-pilot-wrapper.sh             # Pass 1.6

tests/unittests/
  test_task_dispatcher_config.py
  test_task_dispatcher_strategy_conservative.py
  test_task_dispatcher_state.py
  test_task_dispatcher_pilot_lifecycle.py
  test_makeflow_prep.py
  test_task_dispatcher_strategy_conformance.py  # Pass 3.15
tests/integration/
  test_radical_edge_run.py

examples/example_makeflow_multiedge/
  workflow.makeflow
  pools.yaml
  README.md

docs/task_dispatcher_strategy.md             # Pass 3.14

pyproject.toml                               # entry-point group (Pass 3.13)
```

Existing files modified:
- `pyproject.toml` — entry point group only.
- `src/radical/edge/__init__.py` — export `PluginTaskDispatcher`,
  `TaskDispatcherClient`.
- `CLAUDE.md` — add the new plugin section.

Existing files untouched:
- `plugin_rhapsody.py`, `plugin_psij.py`, `plugin_staging.py`, bridge
  code, edge service, models, UI.

---

## PR slicing

| PR | Contents | Pre-req |
|----|----------|---------|
| 1  | Pass 1 Steps 1–4 (config + strategy ABC + default + state) | — |
| 2  | Pass 1 Steps 5–8 (plugin + pilot lifecycle + router + staging) | PR 1 |
| 3  | Pass 2 Steps 9–11 (CLIs) | PR 2 |
| 4  | Pass 2 Step 12 (example) | PR 3 |
| 5  | Pass 3 Steps 13–15 (strategy registration, docs, conformance harness) | PR 2 |

PR 1 is pure library code with unit tests and no radical.edge wiring;
safe to land first and reuse standalone.

---

## Resolved decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Same-edge psij access path | Route through the bridge via the dispatcher's shared `BridgeClient`. Queue wait dominates by 3–5 orders of magnitude over bridge RTT; a separate loopback transport saves nothing and doubles the error-handling surface. |
| 2 | Topology-only vs `BridgeClient` | Single `BridgeClient` handles all outbound calls (same-edge psij, child-edge rhapsody, SSE). `on_topology_change` is used as a redundant signal for pilot-appeared events. |
| 3 | Child-edge binding | Deterministic edge-name pattern `<pool>_<pilot_id>` + explicit handshake POST from pilot to dispatcher with capacity + startup_time. |
| 4 | Task ID salt | `run_id = sha1(abs_makeflow_path + mtime_ns)[:16]`, computed by preprocessor, baked into every rewritten command as `--run-id=...`. |
| 5 | Priority semantics | Pass through Makeflow's integer `PRIORITY` unchanged; strategy interprets. |
| 6 | Config format | JSON (stdlib, matches repo convention). No YAML dep. |
| 7 | Pilot bootstrap | All `RADICAL_EDGE_*` env vars via psij `JobSpec.environment`. No config file, no CLI args. |
| 8 | Stage-in transport | Multipart per-file for v1; code docstring marks the bulk tar-stream optimization path. |
| 9 | Live stdout/stderr | Not in v1. Terminal capture only. Scratch dir on shared FS is the workaround for live inspection. Rationale: cross-plugin rhapsody extension not worth the complexity for a non-correctness feature. |
| 10 | Pilot termination | No mandatory `DRAINING` state. Strategy owns termination via `ctx.cancel_pilot(pid)` and `ctx.drain_pilot(pid)` (non-mandatory flag). |

Strategy scope (confirmed):
- Pilot submission (what, when, how big — via `ctx.submit_pilot(size_key)`)
- Task dispatch (which task → which pilot — via `pick_dispatch`)
- Pilot termination (when to cancel — via `should_terminate_pilot` and
  `ctx.cancel_pilot`/`ctx.drain_pilot`)

Deferred extension candidate: per-task backend choice (dispatch to
`concurrent` vs `dragon_v3` on the same pilot). Conflates task-placement
with pilot-placement; add post-v1 as a strategy extension without
breaking the ABC.

---

## Non-goals for this effort

- Makeflow JX input support. Classic syntax only in v1.
- Direct edge ↔ edge file transfer. Cross-pool files bounce through
  client. Future work.
- Cross-pool autoscaling (a strategy that submits pilots on a *different*
  edge because this one's queue is backed up). Strategy is scoped to one
  pool on one edge.
- A bridge-hosted dispatcher. Promotion path exists (move the same code
  into a bridge plugin) but is out of scope for v1.
- Dynamic pool reconfiguration. Pool config is read at plugin startup.
- **Live stdout/stderr streaming** during task execution. Only the terminal
  snapshot arrives with the `task_status` SSE notification. Inspection of
  in-progress logs is done via the shared-FS scratch directory. Adding
  live streaming is a non-trivial cross-plugin change (rhapsody-side log
  chunking + new SSE topic + wrapper tee) and deferred until needed.
- **Per-task rhapsody backend selection**. Backend is fixed per pilot at
  pilot-submit time via `PilotSize.rhapsody_backend` (required field; no
  cascade). A future strategy extension (`strategy.pick_backend(task,
  pilot)`) can override per task; paired `FIXME(per-task-backend)` markers
  in `plugin_task_dispatcher.py::_assign` and
  `task_dispatcher_strategy.py::DispatchStrategy` cross-reference each
  other so the two sites stay in sync.

---

## Success criteria for v1

1. Four-rule Makeflow workflow spans two edges with cross-pool file
   staging, completes with declared outputs present, `makeflowlog` shows
   all rules `SUCCEEDED`.
2. Killing and restarting the Makeflow client resumes the run without
   re-executing completed rules.
3. Autoscaling submits at most `max_pilots` pilots under sustained
   backlog; idle pools have zero pilots.
4. Replacing `strategy: conservative` with `strategy: aggressive` in
   `pools.yaml` changes scaling behavior with zero code changes to the
   dispatcher plugin.
5. Unit-test coverage ≥85% on new modules; all existing tests pass.
