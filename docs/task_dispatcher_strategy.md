# Task-dispatcher strategies

The task dispatcher delegates three concerns to a pluggable
``DispatchStrategy`` instance, one per pool:

1. **Pilot submission** — what pilot to submit, when, how big.
2. **Task dispatch**    — which task off the queue runs on which pilot.
3. **Pilot termination** — when to cancel a pilot (beyond walltime).

Pilot replacement emerges from submission + termination; no separate
hook is needed.

Strategies never touch psij, rhapsody, or the broker directly.  All
side-effecting actions flow through the ``StrategyContext`` supplied
by the dispatcher.  This keeps the research surface stable across
plumbing changes.

## Selecting a strategy

In ``pools.json``:

```text
{
  "pools": [{
    "name": "cpu",
    "queue": "batch",
    "default_size": "s",
    "pilot_sizes": { ... },
    "strategy": "conservative",
    "strategy_config": {
      "min_dwell_sec": 30,
      "max_in_flight_submissions": 2,
      "router_preference": "least_loaded"
    }
  }]
}
```

Name resolution happens in three stages (see
``task_dispatcher_strategy.load_strategy``):

1. If ``strategy`` contains ``:``, it's treated as a
   ``"module.path:ClassName"`` and imported directly.  This is the
   escape hatch for in-repo experiments.
2. Otherwise the **built-in registry** is consulted
   (``task_dispatcher_strategy._builtin_strategies``).  This ships
   the default ``conservative`` strategy and the reference
   ``aggressive_scale_to_backlog`` strategy so they work without a
   ``pip install`` step in editable checkouts.
3. Otherwise Python entry points in the
   ``radical.orbit.task_dispatcher.strategies`` group are consulted.
   This is how third-party strategies shipped as separate packages
   become discoverable.

## The policy base class

The real surface is ``DispatchPolicy`` in
``src/radical/orbit/task_dispatcher_policy.py`` — a plain base class, not
an ABC, with inert defaults so a subclass overrides only what it needs.
Policies are resolved through a small **manual registry** in that module
(``_BUILTINS`` plus ``register_policy`` for tests and embedders); there is
no entry-point discovery.

```python
class DispatchPolicy:
    def __init__(self, pool: PoolConfig, cfg: dict,
                 now: Callable[[], float] = time.time) -> None: ...

    @property
    def max_requeues(self) -> int: ...           # default 1

    def on_pilot_state(self, pilot, old_state, new_state) -> None: ...
    def on_tick(self, pool_state, submit_pilot) -> None: ...
    def pick_dispatch(self, pool_state) \
            -> tuple[TaskRecord, PilotRecord] | None: ...
```

There is no ``ctx`` object and no ``should_terminate_pilot``: a policy
reads live state straight off the ``PoolState`` handed to each method, and
pilots expire at walltime.

### Invocation contract

- One instance per pool, constructed as ``PolicyClass(pool_config,
  strategy_config)``.  Constructors must be **cheap and side-effect-free**:
  the pool parser trial-instantiates the policy to validate
  ``strategy_config`` at declaration time, and a raise there rejects the
  pool declaration with a 400.
- Every method runs on the dispatcher plugin's event loop, so no
  in-policy locking is needed.
- ``pick_dispatch`` is called in a loop until it returns ``None``, bounded
  by the pending-queue length.  The dispatcher performs the assignment and
  every state mutation — policies only *choose*.
- Policies must **not** mutate pool state.  Scale-up is requested only
  through the ``submit_pilot`` callable handed to ``on_tick``:

  ```python
  submit_pilot(size_key: str | None = None, *,
               member_id: str | None = None) -> str
  ```

  ``size_key`` keeps position 0, so an existing positional call keeps its
  meaning; ``member_id`` is keyword-only and ``None`` selects the pool's
  primary member.
- ``max_requeues`` is read by the dispatcher when a pilot loss re-queues a
  task.  Past the cap the task is failed with ``requeued too often (pilot
  lost)`` instead of bouncing forever.

### The pool handle

``PoolState`` (in ``plugin_task_dispatcher.py``) is what a policy sees:

```python
ps.pending_queue()               # [TaskRecord] QUEUED, priority-ordered
ps.live_pilots()                 # [PilotRecord] non-terminal

ps.members()                     # [PoolMember] in declaration order
ps.member(mid)                   # PoolMember | None  ('' = implicit)
ps.live_pilots_for(mid)          # [PilotRecord] for one member
ps.member_budget_left(mid, now)  # float | None  (None = no budget)
ps.size_of(pilot)                # PilotSize | None (snapshot first)
```

Accessors return snapshots — safe to iterate, not to cache across calls.

### Pools are capability classes

A pool is a **capability class**, not a site: it has one or more
**members**, each one an ``(endpoint, queue, account, pilot-size menu,
attributes, budget)`` tuple, and a GPU pilot on two different clusters can
live in the same pool.  A legacy single-site declaration (no ``members``
key) keeps its scalar fields and gains exactly one *implicit* member
synthesised from them, so a policy only ever reads ``ps.members()`` — one
shape at runtime, two on the wire.

Matching is the single function
``task_dispatcher_match.satisfies(requirements, attributes, size)``, used
both with a member's declared attributes + default size (which member to
grow) and with a pilot's attribute/size **snapshot** (which pilot may run
a task).  A pilot snapshots its member's attributes at submit time and
keeps them for its life, so re-declaring a member with new ``software``
does not retro-fit running pilots — intended: the pilot's node really does
have the old software.

A ``TaskRecord`` may carry a ``requirements`` dict
(``cores``/``gpus``/``mem_gb``/``ranks``/``mpi``/``software``/``labels``),
validated at submit time.  Resource *shape* is matched; resource
*occupancy* is not: it is **forwarded, not enforced**.  Capacity in this
document is still a **task count** — ``pilot.capacity = nodes *
cpus_per_node``, ``free_capacity() = capacity - in_flight``.  Nothing
reserves cores or GPUs; enforcement is deferred (see
``plans/120-task-requirements-passthrough.md``, "Deferred").

## Shipped strategies

### ``conservative`` (default)

File: ``src/radical/orbit/task_dispatcher_strategy_conservative.py``.

Favors efficient utilization over low latency.

- No eager scale-up on arrival.  ``pick_dispatch`` routes into existing
  capacity first.
- Scale-up only on tick, one pilot at a time, with a configurable
  ``min_dwell_sec`` between submissions.
- Bounded in-flight submissions (``max_in_flight_submissions``) so a
  brief burst cannot inflate the fleet.
- ``should_terminate_pilot`` always returns False — pilots expire at
  walltime.
- Routing: configurable ``least_loaded`` (default) or ``youngest``; ties
  break on ``member_id``.
- ``pick_dispatch`` filters pilots on ``free_capacity() > 0`` **and** on
  ``satisfies(task.requirements, pilot.attributes, ps.size_of(pilot))``.
  The slot test is still pure task counting: a task's ``cores``/``gpus``
  are matched against the pilot's *shape*, never against its occupancy, so
  a 4-GPU pilot will happily accept a fifth 1-GPU task.  Resource-aware
  reservation is deferred.

**Per member.**  Every piece of bookkeeping — dwell, in-flight
submissions, the pilot ceiling, the failure backoff, the budget — is per
member.  For a legacy single-site pool (one implicit member) each is
arithmetically identical to the pre-121 pool-level version.  ``on_tick``
does at most one submission per tick, in this order:

1. **Floor** — the members below their ``min_pilots``, served in
   declaration order (a floor is a debt, not a preference).  If none of
   them clears the guards in step 3, the tick **falls through** to the
   backlog step rather than returning: a member whose site is down sits
   below its floor forever, and stopping there would let one dead site
   starve every sibling.

   **Legacy behaviour change (plan 121).**  Before 121 the conservative
   policy returned immediately when nothing was pending, so ``min_pilots``
   was never acted on and a warm floor did not exist.  It does now, for
   legacy single-site pools too: a pool declaring ``min_pilots: 2`` will
   submit pilots proactively with an empty queue.  The default is ``0``,
   so a pool that never set it is unaffected.
2. **Backlog** — otherwise, scale up when a pending task no *live* pilot
   could ever serve exists, or when ``len(pending)`` exceeds the free
   capacity of the pilots that *can* serve something pending.  An idle CPU
   pilot is not capacity for a GPU task.  Candidates are the members whose
   attributes and default size satisfy the head of that backlog.
3. **Guards**, per member: failure backoff, ``max_in_flight_submissions``,
   ``member.max_pilots``, ``min_dwell_sec`` since *that member's* last
   submit, and a declared budget with nothing left.  Plus a pool-wide
   ceiling of ``max(max_in_flight_submissions, len(members))``, so a class
   pool never warms up more slowly than the single-member pools it
   replaces and never puts more than one submission per member in flight.
4. **Rank** by ``member_preference``, ties broken on ``member_id``.

**Skip, don't block (deliberate).**  Pre-121, a top-priority task with no
available pilot returned ``None`` and stalled the whole drain.  In a class
pool a GPU task must not block CPU tasks, so an unservable task is
*skipped* and the next one considered.  That is a priority inversion — a
low-priority task that fits can run before a high-priority one that does
not — accepted for class pools and documented here; a future
``strict_priority`` knob is the escape hatch.  The drain loop is still
bounded by the pending-queue length, so the cost is O(pending²) worst case
(tens, at demo scale).

Config knobs:

| key                       | default         | meaning |
|---------------------------|-----------------|---------|
| ``min_dwell_sec``         | ``30.0``        | min time between submissions, **per member** |
| ``max_in_flight_submissions`` | ``2``       | max simultaneously-PENDING pilots per member (and a pool-wide floor of ``len(members)``) |
| ``router_preference``     | ``least_loaded``| which pilot: alt ``youngest`` |
| ``max_consecutive_failures`` | ``3``        | failures before a member backs off |
| ``failure_backoff_sec``   | ``60.0``        | how long a member backs off |
| ``member_preference``     | ``budget``      | which member to grow: alt ``least_loaded`` |
| ``max_requeues``          | ``1``           | pilot losses a task survives before it fails |

### ``aggressive_scale_to_backlog``

File: ``src/radical/orbit/task_dispatcher_strategy_examples.py``.

Favors low queue latency over efficient utilization.  Demonstration /
research reference.

- Arrival-triggered scaling: every ``on_task_arrived`` may submit a
  pilot if projected backlog (pending + expected arrivals during
  startup lag) exceeds active capacity.
- No dwell gate — up to ``max_in_flight_submissions`` pilots may be
  submitted simultaneously.
- Idle termination: pilots active with zero in-flight and no pending
  tasks for ``idle_timeout_sec`` are cancelled.
- Routing: youngest pilot first (most remaining walltime).

Config knobs:

| key                       | default   | meaning |
|---------------------------|-----------|---------|
| ``max_in_flight_submissions`` | ``4`` | max simultaneously-PENDING pilots |
| ``idle_timeout_sec``      | ``90.0``  | drain-on-idle threshold |
| ``arrivals_window_sec``   | ``30.0``  | lookback for arrival-rate estimate |

## Writing a new strategy

Subclass ``DispatchStrategy`` and implement the abstract methods.  The
minimum:

```python
from radical.orbit.task_dispatcher_strategy import (
    DispatchStrategy, StrategyContext)
from radical.orbit.task_dispatcher_state    import (
    PILOT_ACTIVE, TASK_QUEUED)


class MyStrategy(DispatchStrategy):

    def on_task_arrived (self, ctx, task):              pass
    def on_pilot_state  (self, ctx, p, old, new):       pass
    def on_task_finished(self, ctx, task, pilot):       pass

    def pick_dispatch(self, ctx):
        pending = [t for t in ctx.pending_queue()
                   if t.state == TASK_QUEUED]
        active  = [p for p in ctx.pilots()
                   if p.state == PILOT_ACTIVE and p.free_capacity() > 0]
        if not pending or not active:
            return None
        return pending[0], active[0]
```

Point the pool at it in ``pools.json``:

```json
"strategy": "my_package.my_module:MyStrategy"
```

or register it as an entry point and use its short name:

```toml
# pyproject.toml in your package
[project.entry-points."radical.orbit.task_dispatcher.strategies"]
my_strategy = "my_package.my_module:MyStrategy"
```

then:

```json
"strategy": "my_strategy"
```

## Conformance harness

``tests/unittests/test_task_dispatcher_strategy_conformance.py`` runs
any strategy on a deterministic discrete-event simulator and asserts
baseline properties:

- ``test_no_crash_no_arrivals`` — strategy does not submit pilots when
  no tasks exist
- ``test_submits_pilot_under_backlog`` — strategy eventually submits at
  least one pilot under a 20-task backlog
- ``test_respects_max_pilots`` — live fleet never exceeds ``max_pilots``
- ``test_completes_steady_workload`` — at least 80% of a steady arrival
  stream completes before the test window closes
- ``test_burst_drains`` — a 50-task burst fully drains within a bounded
  simulated window

Adding a new strategy to the harness is one line: append an entry to
``STRATEGIES`` at the top of the file.

Extra tests contrast strategy personalities
(``TestPolicyDifferences``).  They illustrate how the same harness
differentiates policies measurably (aggressive submits more pilots
under a burst, conservative never terminates pilots, etc.).

## Future extension points

The insertion sites for per-task backend selection (formerly carried as
paired ``FIXME(per-task-backend)`` markers, since removed):

- ``src/radical/orbit/plugin_task_dispatcher.py`` —
  ``PluginTaskDispatcher._claim`` and its caller ``_drain_pending``
  (the old ``_assign`` no longer exists)
- ``src/radical/orbit/task_dispatcher_policy.py`` — ``DispatchPolicy``
  (the ABC moved out of the removed ``task_dispatcher_strategy.py``)

A natural next hook would be
``strategy.pick_backend(ctx, task, pilot) -> str | None``, letting a
strategy override the rhapsody backend on a per-task basis rather
than inheriting ``pilot.rhapsody_backend``.  Not part of the v1 ABC.
Note that ``backend_kwargs()`` in ``plugin_task_dispatcher.py`` keys on
``pilot.rhapsody_backend`` today, so a per-task backend override would
have to move that call to the same hook.
