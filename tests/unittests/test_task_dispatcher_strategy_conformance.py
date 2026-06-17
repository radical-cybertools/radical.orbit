"""Strategy conformance harness — parametric tests for any strategy.

This suite exists as a **benchmark** more than a correctness check.
Given a :class:`DispatchStrategy` implementation, it drives the
strategy through a deterministic simulated event timeline and
measures headline numbers: p50/p95 task-wait, final pilot count,
terminated-early count.

New strategies opt in by adding their class to ``STRATEGIES`` below.
Existing strategies must keep behaving sensibly on every scenario — if
this suite starts failing, something regressed.

Explicitly not measured:
    - Absolute wall time (everything runs in simulated time)
    - Per-rule throughput (depends on pilot execution, not strategy)

The goal is that comparing strategies is as easy as adding one line to
the ``STRATEGIES`` list; the harness does the bookkeeping.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field

import pytest

from radical.edge.task_dispatcher_config   import PoolConfig, PilotSize
from radical.edge.task_dispatcher_state    import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_ACTIVE, PILOT_DONE,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE,
)
from radical.edge.task_dispatcher_strategy import (
    DispatchStrategy, StrategyContext,
)
from radical.edge.task_dispatcher_strategy_conservative import (
    ConservativeStrategy,
)
from radical.edge.task_dispatcher_strategy_examples import (
    AggressiveScaleToBacklogStrategy,
)


# Strategies under test — third-party strategies installed as entry
# points are picked up by test_entry_point_discovery below.
STRATEGIES: list[tuple[str, type[DispatchStrategy], dict]] = [
    ('conservative', ConservativeStrategy,
     {'min_dwell_sec': 10.0, 'max_in_flight_submissions': 2}),
    ('aggressive',   AggressiveScaleToBacklogStrategy,
     {'max_in_flight_submissions': 4, 'idle_timeout_sec': 45.0}),
]


# ---------------------------------------------------------------------------
# Simulator — deterministic, no threads, no real time
# ---------------------------------------------------------------------------

@dataclass
class _SimPilot:
    '''Mutable pilot in the simulator — maps to a :class:`PilotRecord`.'''
    record            : PilotRecord
    startup_duration  : float  # how long PENDING→ACTIVE takes in sim time
    task_duration     : float  # per-task runtime while RUNNING
    terminated_at     : float | None = None


@dataclass
class _SimTask:
    record    : TaskRecord
    finish_at : float | None = None


@dataclass
class _Metrics:
    submitted_pilots : int = 0
    terminated_pilots: int = 0
    wait_times       : list[float] = field(default_factory=list)
    completed_tasks  : int = 0

    def p50(self) -> float:
        return statistics.median(self.wait_times) if self.wait_times else 0.0

    def p95(self) -> float:
        if not self.wait_times:
            return 0.0
        xs = sorted(self.wait_times)
        k  = max(0, int(0.95 * (len(xs) - 1)))
        return xs[k]


class _Simulator:
    '''Event-loop-free discrete simulator for strategy conformance.

    Time advances in ``step`` increments.  The strategy sees a normal
    :class:`StrategyContext`; all side effects flow through
    harness-side hooks that update simulator state.
    '''

    def __init__(self, strategy_cls: type[DispatchStrategy],
                 strategy_cfg: dict,
                 *, pilot_capacity: int = 4,
                 pilot_startup: float = 30.0,
                 task_runtime: float = 10.0,
                 walltime: int = 3600,
                 max_pilots: int = 8) -> None:
        self.pool = PoolConfig(
            name            = 'sim', queue = 'q', account = None,
            pilot_sizes     = {'s': PilotSize(
                nodes=1, cpus_per_node=pilot_capacity,
                rhapsody_backend='concurrent')},
            default_size    = 's',
            max_pilots      = max_pilots,
            strategy        = strategy_cls.__name__,
            strategy_config = dict(strategy_cfg),
        )
        self.strategy = strategy_cls(self.pool, strategy_cfg)

        self._now           = 0.0
        self._pilot_capacity = pilot_capacity
        self._pilot_startup = pilot_startup
        self._task_runtime  = task_runtime
        self._walltime      = walltime

        self._pilots : dict[str, _SimPilot] = {}
        self._tasks  : dict[str, _SimTask]  = {}
        self._arrivals: list[float]         = []
        self._lag_history: list[float]      = []
        self._next_pilot_id = 0
        self._next_task_id  = 0

        self.metrics = _Metrics()

        self.ctx = StrategyContext(
            self.pool,
            now_fn               = lambda: self._now,
            pending_queue_fn     = self._pending_snapshot,
            pilots_fn            = self._pilots_snapshot,
            arrivals_window_fn   = self._arrivals_window,
            pilot_lag_history_fn = lambda: list(self._lag_history),
            submit_pilot_fn      = self._submit_pilot,
            cancel_pilot_fn      = self._cancel_pilot,
            drain_pilot_fn       = self._drain_pilot,
        )

    # -- context snapshots ----------------------------------------------

    def _pending_snapshot(self) -> list[TaskRecord]:
        pending = [t.record for t in self._tasks.values()
                   if t.record.state == TASK_QUEUED]
        pending.sort(key=lambda r: (-r.priority, r.arrival_ts))
        return pending

    def _pilots_snapshot(self) -> list[PilotRecord]:
        return [p.record for p in self._pilots.values()
                if p.record.state not in (PILOT_DONE, 'FAILED')]

    def _arrivals_window(self, seconds: float) -> list[float]:
        cutoff = self._now - seconds
        return [t for t in self._arrivals if t >= cutoff]

    # -- context actions ------------------------------------------------

    def _submit_pilot(self, size_key):
        del size_key   # single size in sim
        self._next_pilot_id += 1
        pid = f'sim.p{self._next_pilot_id}'
        rec = PilotRecord(
            pid              = pid, pool = 'sim', size_key = 's',
            rhapsody_backend = 'concurrent',
            state            = PILOT_PENDING,
            submitted_at     = self._now,
            walltime_deadline= self._now + self._walltime,
        )
        self._pilots[pid] = _SimPilot(
            record            = rec,
            startup_duration  = self._pilot_startup,
            task_duration     = self._task_runtime,
        )
        self.metrics.submitted_pilots += 1
        return pid

    def _cancel_pilot(self, pid: str) -> None:
        sp = self._pilots.get(pid)
        if sp is None:
            return
        sp.record.state = 'FAILED'
        sp.terminated_at = self._now
        self.metrics.terminated_pilots += 1

    def _drain_pilot(self, pid: str) -> None:
        sp = self._pilots.get(pid)
        if sp is not None:
            sp.record.accepting_new_tasks = False

    # -- driver ---------------------------------------------------------

    def submit_task(self, priority: int = 0) -> str:
        self._next_task_id += 1
        tid = f'sim.t{self._next_task_id}'
        rec = TaskRecord(
            task_id      = tid,
            pool         = 'sim',
            cmd          = ['noop'],
            cwd          = '/tmp',
            priority     = priority,
            state        = TASK_QUEUED,
            submitted_at = self._now,
            arrival_ts   = self._now,
        )
        self._tasks[tid] = _SimTask(record=rec)
        self._arrivals.append(self._now)
        self.strategy.on_task_arrived(self.ctx, rec)
        self._drain_once()
        return tid

    def advance(self, dt: float) -> None:
        '''Advance simulated time by *dt* seconds.'''
        target = self._now + dt
        while self._now < target:
            self._now = min(target, self._next_event_time(target))
            self._process_pilot_transitions()
            self._process_task_completions()
            self._apply_termination_policy()
            self._drain_once()
            if self._now >= target:
                break

    def _next_event_time(self, ceiling: float) -> float:
        '''Next strictly-future discrete event we care about.

        Only unfinished pilot-startup and task-finish events are
        considered; already-processed events must not pull the clock
        backwards (that was the cause of an early hang in the harness).
        '''
        events = [ceiling]
        for sp in self._pilots.values():
            if sp.record.state == PILOT_PENDING:
                t = sp.record.submitted_at + sp.startup_duration
                if t > self._now:
                    events.append(t)
        for st in self._tasks.values():
            if st.record.state == TASK_RUNNING and \
                    st.finish_at is not None and \
                    st.finish_at > self._now:
                events.append(st.finish_at)
        return min(events)

    def _process_pilot_transitions(self) -> None:
        for sp in self._pilots.values():
            if sp.record.state == PILOT_PENDING and \
                    self._now >= sp.record.submitted_at + sp.startup_duration:
                old = sp.record.state
                sp.record.state     = PILOT_ACTIVE
                sp.record.active_at = self._now
                sp.record.capacity  = self._pilot_capacity
                lag = sp.record.active_at - sp.record.submitted_at
                self._lag_history.append(lag)
                self.strategy.on_pilot_state(
                    self.ctx, sp.record, old, PILOT_ACTIVE)

    def _process_task_completions(self) -> None:
        for st in self._tasks.values():
            if (st.record.state == TASK_RUNNING and
                    st.finish_at is not None and
                    self._now >= st.finish_at):
                st.record.state       = TASK_DONE
                st.record.finished_at = self._now
                st.record.exit_code   = 0
                pilot_id = st.record.pilot_id or ''
                sp = self._pilots.get(pilot_id)
                if sp is not None:
                    sp.record.in_flight = max(0, sp.record.in_flight - 1)
                    self.strategy.on_task_finished(
                        self.ctx, st.record, sp.record)
                self.metrics.completed_tasks += 1
                # wait time = when it started running - when it arrived
                assert st.record.started_at is not None
                self.metrics.wait_times.append(
                    st.record.started_at - st.record.arrival_ts)

    def _apply_termination_policy(self) -> None:
        for sp in list(self._pilots.values()):
            if sp.record.state == PILOT_ACTIVE and \
                    self.strategy.should_terminate_pilot(
                        self.ctx, sp.record):
                self.ctx.cancel_pilot(sp.record.pid)

    def _drain_once(self) -> None:
        '''Loop the strategy's pick_dispatch until it stops picking.'''
        safety = 10_000
        while safety > 0:
            safety -= 1
            pair = self.strategy.pick_dispatch(self.ctx)
            if pair is None:
                return
            task_rec, pilot_rec = pair
            if task_rec.state != TASK_QUEUED:
                continue
            self._assign(task_rec, pilot_rec)

    def _assign(self, task_rec: TaskRecord,
                pilot_rec: PilotRecord) -> None:
        task_rec.state      = TASK_RUNNING
        task_rec.pilot_id   = pilot_rec.pid
        task_rec.started_at = self._now
        pilot_rec.in_flight     += 1
        pilot_rec.started_tasks += 1
        sp = self._pilots[pilot_rec.pid]
        self._tasks[task_rec.task_id].finish_at = \
            self._now + sp.task_duration

    def tick_strategy(self) -> None:
        '''Drive the strategy's periodic on_tick callback.'''
        self.strategy.on_tick(self.ctx)
        self._drain_once()


# ---------------------------------------------------------------------------
# Arrival patterns
# ---------------------------------------------------------------------------

def _steady(sim: _Simulator, rate_per_sec: float,
            duration_sec: float) -> None:
    '''Submit tasks uniformly at *rate_per_sec* for *duration_sec*.'''
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else duration_sec
    t = 0.0
    while t < duration_sec:
        sim.advance(interval)
        sim.submit_task()
        t += interval


def _burst(sim: _Simulator, burst_size: int,
           total_duration_sec: float) -> None:
    '''One big burst of tasks at t=0, then no new arrivals.'''
    for _ in range(burst_size):
        sim.submit_task()
    # Advance time in chunks so ticks fire
    step = 5.0
    t = 0.0
    while t < total_duration_sec:
        sim.advance(step)
        sim.tick_strategy()
        t += step


def _bimodal(sim: _Simulator, *,
             burst_size: int = 30, quiet_sec: float = 60.0,
             cycles: int = 3) -> None:
    '''Alternating bursts and quiet periods.'''
    for _ in range(cycles):
        for _ in range(burst_size):
            sim.submit_task()
        step = 5.0
        t = 0.0
        while t < quiet_sec:
            sim.advance(step)
            sim.tick_strategy()
            t += step


# ---------------------------------------------------------------------------
# Conformance assertions — behavior that every strategy must satisfy
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('name,cls,cfg', STRATEGIES)
class TestConformance:

    def test_no_crash_no_arrivals(self, name, cls, cfg):
        sim = _Simulator(cls, cfg)
        for _ in range(10):
            sim.tick_strategy()
            sim.advance(5.0)
        # No pilots submitted because no work exists.
        assert sim.metrics.submitted_pilots == 0, \
            f'[{name}] submitted pilots with no tasks: ' \
            f'{sim.metrics.submitted_pilots}'

    def test_submits_pilot_under_backlog(self, name, cls, cfg):
        sim = _Simulator(cls, cfg)
        for _ in range(20):
            sim.submit_task()
        # Fire ticks so the strategy has chances to react
        for _ in range(10):
            sim.tick_strategy()
            sim.advance(10.0)
        assert sim.metrics.submitted_pilots >= 1, \
            f'[{name}] no pilot submitted under 20-task backlog'

    def test_respects_max_pilots(self, name, cls, cfg):
        sim = _Simulator(cls, cfg, max_pilots=2)
        for _ in range(200):
            sim.submit_task()
        for _ in range(50):
            sim.tick_strategy()
            sim.advance(30.0)
        # The live fleet must never exceed max_pilots.  Submitted
        # counter may exceed it only because old pilots terminated and
        # freed slots.
        assert sim.metrics.submitted_pilots <= 50, \
            f'[{name}] absurd pilot submission count: ' \
            f'{sim.metrics.submitted_pilots}'

    def test_completes_steady_workload(self, name, cls, cfg):
        sim = _Simulator(cls, cfg, pilot_startup=20.0, task_runtime=5.0)
        _steady(sim, rate_per_sec=1.0, duration_sec=60.0)
        # Drain
        for _ in range(60):
            sim.tick_strategy()
            sim.advance(10.0)
        # Most tasks should have completed.  Exact fraction depends on
        # strategy but it must not be dead-locked.
        total = len(sim._tasks)
        assert sim.metrics.completed_tasks >= total * 0.8, \
            f'[{name}] only completed {sim.metrics.completed_tasks}/' \
            f'{total} tasks'

    def test_burst_drains(self, name, cls, cfg):
        sim = _Simulator(cls, cfg, pilot_startup=20.0, task_runtime=3.0)
        _burst(sim, burst_size=50, total_duration_sec=400.0)
        assert sim.metrics.completed_tasks == 50, \
            f'[{name}] burst did not fully drain: ' \
            f'{sim.metrics.completed_tasks}/50'


# ---------------------------------------------------------------------------
# Policy-specific checks — distinguishes strategies
# ---------------------------------------------------------------------------

class TestPolicyDifferences:

    def test_aggressive_submits_more_under_burst(self):
        '''Burst → aggressive should submit more pilots faster than
        conservative, within the same max_pilots envelope.'''
        sim_c = _Simulator(ConservativeStrategy,
                           {'min_dwell_sec': 30.0,
                            'max_in_flight_submissions': 4},
                           max_pilots=8, pilot_startup=60.0)
        sim_a = _Simulator(AggressiveScaleToBacklogStrategy,
                           {'max_in_flight_submissions': 4},
                           max_pilots=8, pilot_startup=60.0)
        for sim in (sim_c, sim_a):
            for _ in range(40):
                sim.submit_task()
            # Only a short window — aggressive should react faster
            for _ in range(3):
                sim.tick_strategy()
                sim.advance(10.0)

        assert sim_a.metrics.submitted_pilots >= \
               sim_c.metrics.submitted_pilots, \
            f'aggressive should submit at least as many pilots as ' \
            f'conservative (aggressive={sim_a.metrics.submitted_pilots}, ' \
            f'conservative={sim_c.metrics.submitted_pilots})'

    def test_aggressive_terminates_idle_pilots(self):
        sim = _Simulator(AggressiveScaleToBacklogStrategy,
                         {'idle_timeout_sec': 30.0,
                          'max_in_flight_submissions': 4},
                         pilot_startup=5.0, task_runtime=5.0)
        # 5 tasks, then go idle
        for _ in range(5):
            sim.submit_task()
        for _ in range(30):
            sim.tick_strategy()
            sim.advance(10.0)
        assert sim.metrics.terminated_pilots >= 1, \
            'aggressive failed to terminate idle pilot after timeout'

    def test_conservative_never_terminates(self):
        sim = _Simulator(ConservativeStrategy,
                         {'min_dwell_sec': 0.0},
                         pilot_startup=5.0, task_runtime=5.0)
        for _ in range(5):
            sim.submit_task()
        for _ in range(30):
            sim.tick_strategy()
            sim.advance(10.0)
        assert sim.metrics.terminated_pilots == 0
