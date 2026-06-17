# Task-dispatcher strategies

The task dispatcher delegates three concerns to a pluggable
``DispatchStrategy`` instance, one per pool:

1. **Pilot submission** — what pilot to submit, when, how big.
2. **Task dispatch**    — which task off the queue runs on which pilot.
3. **Pilot termination** — when to cancel a pilot (beyond walltime).

Pilot replacement emerges from submission + termination; no separate
hook is needed.

Strategies never touch psij, rhapsody, or the bridge directly.  All
side-effecting actions flow through the ``StrategyContext`` supplied
by the dispatcher.  This keeps the research surface stable across
plumbing changes.

## Selecting a strategy

In ``pools.json``:

```json
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

## The ABC

```python
class DispatchStrategy(ABC):
    def __init__(self, pool: PoolConfig, cfg: dict) -> None: ...

    # Signals — dispatcher calls these when state changes
    @abstractmethod
    def on_task_arrived (ctx, task): ...
    @abstractmethod
    def on_pilot_state  (ctx, pilot, old_state, new_state): ...
    @abstractmethod
    def on_task_finished(ctx, task, pilot): ...
    def on_tick         (ctx) -> None: ...   # default no-op

    # Decisions — dispatcher asks these when it needs to act
    @abstractmethod
    def pick_dispatch(ctx) -> tuple[TaskRecord, PilotRecord] | None: ...
    def should_terminate_pilot(ctx, pilot) -> bool: ...   # default False
```

### Invocation contract

- **Signals** fire in the dispatcher's event loop thread.  Strategies
  may record state and schedule work via ``ctx`` but must not block.
- **``pick_dispatch``** is called in a loop after each dispatch-relevant
  event — arrival, pilot state change, task completion, tick.  It
  returns at most one ``(task, pilot)`` pair per call; the dispatcher
  repeats until it returns ``None``.
- **``on_tick``** runs on a ~5 s cadence per pool.  Use it only when a
  strategy needs time-triggered behaviour that event callbacks can't
  provide (rate limiting, deadline checks, idle timeouts).
- **``should_terminate_pilot``** is consulted periodically.  A pilot
  that returns ``True`` is cancelled via ``ctx.cancel_pilot`` by the
  dispatcher.  Default is False — pilots expire at walltime only.

### The context

```python
ctx.pool                         # PoolConfig
ctx.logger                       # logging.Logger
ctx.now()                        # wall-clock seconds since epoch

ctx.pending_queue()              # [TaskRecord] priority-ordered
ctx.pilots()                     # [PilotRecord] non-terminal
ctx.arrivals_window(seconds)     # [float] arrival timestamps in window
ctx.pilot_lag_history()          # [float] observed PENDING→ACTIVE lags

ctx.submit_pilot(size_key=None)  # schedule a new pilot; returns pid
ctx.cancel_pilot(pid)            # cancel one
ctx.drain_pilot(pid)             # stop routing new tasks to one
```

Accessors return snapshots — they're safe to iterate but shouldn't be
cached across strategy calls.  Action hooks are non-blocking; the
actual psij submission / bridge call happens on a worker.

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
- Routing: configurable ``least_loaded`` (default) or ``youngest``.

Config knobs:

| key                       | default         | meaning |
|---------------------------|-----------------|---------|
| ``min_dwell_sec``         | ``30.0``        | min time between submissions |
| ``max_in_flight_submissions`` | ``2``       | max simultaneously-PENDING pilots |
| ``router_preference``     | ``least_loaded``| alt: ``youngest`` |

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

Marked with ``FIXME(per-task-backend)`` in code:

- ``src/radical/orbit/plugin_task_dispatcher.py::PluginTaskDispatcher._assign``
- ``src/radical/orbit/task_dispatcher_strategy.py::DispatchStrategy``

A natural next hook would be
``strategy.pick_backend(ctx, task, pilot) -> str | None``, letting a
strategy override the rhapsody backend on a per-task basis rather
than inheriting ``pilot.rhapsody_backend``.  Not part of the v1 ABC;
the paired FIXMEs exist so the insertion sites stay in sync when the
extension lands.
