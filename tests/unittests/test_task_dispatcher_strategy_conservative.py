"""Unit tests for ConservativeStrategy.

Covers: no eager scale-up, bounded in-flight pilot submissions, dwell
suppression, max_pilots bound, pick_dispatch priority ordering,
router_preference variants, default no-op should_terminate_pilot.
"""

import pytest

from radical.edge.task_dispatcher_config import PoolConfig, PilotSize
from radical.edge.task_dispatcher_state import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE, PILOT_DONE, PILOT_FAILED,
    TASK_QUEUED, TASK_DONE,
)
from radical.edge.task_dispatcher_strategy import (
    StrategyContext, StrategyNotFound, load_strategy,
)
from radical.edge.task_dispatcher_strategy_conservative import (
    ConservativeStrategy,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _pool(**overrides) -> PoolConfig:
    defaults = dict(
        name='cpu', queue='batch', account=None,
        pilot_sizes={'s': PilotSize(nodes=1, cpus_per_node=4,
                                    rhapsody_backend='concurrent')},
        default_size='s', max_pilots=4,
    )
    defaults.update(overrides)
    return PoolConfig(**defaults)


class _Harness:
    """Minimal StrategyContext driver — lets tests control state + time."""

    def __init__(self, pool: PoolConfig):
        self.pool     = pool
        self.time     = 1000.0
        self.tasks    : list[TaskRecord]  = []
        self.pilots   : list[PilotRecord] = []
        self.submitted: list[str | None]  = []
        self.cancelled: list[str]         = []
        self.drained  : list[str]         = []

        self.ctx = StrategyContext(
            pool,
            now_fn               = lambda: self.time,
            pending_queue_fn     = lambda: list(self.tasks),
            pilots_fn            = lambda: list(self.pilots),
            arrivals_window_fn   = lambda s: [],
            pilot_lag_history_fn = lambda: [],
            submit_pilot_fn      = self._submit,
            cancel_pilot_fn      = lambda p: self.cancelled.append(p),
            drain_pilot_fn       = lambda p: self.drained.append(p),
        )

    def _submit(self, size_key: str | None) -> str:
        self.submitted.append(size_key)
        pid = f'p.sub{len(self.submitted)}'
        return pid

    def add_task(self, **overrides):
        defaults = dict(task_id=f't.{len(self.tasks)}', pool=self.pool.name,
                        cmd=['echo', 'x'], cwd='/tmp',
                        priority=0, arrival_ts=self.time,
                        state=TASK_QUEUED)
        defaults.update(overrides)
        self.tasks.append(TaskRecord(**defaults))

    def add_pilot(self, **overrides):
        defaults = dict(pid=f'p.{len(self.pilots)}', pool=self.pool.name,
                        size_key='s', rhapsody_backend='concurrent',
                        state=PILOT_ACTIVE, capacity=2, in_flight=0,
                        walltime_deadline=self.time + 3600,
                        submitted_at=self.time, active_at=self.time + 50)
        defaults.update(overrides)
        self.pilots.append(PilotRecord(**defaults))

    def advance(self, seconds: float):
        self.time += seconds


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestInit:

    def test_accepts_least_loaded(self):
        ConservativeStrategy(_pool(), {'router_preference': 'least_loaded'})

    def test_accepts_youngest(self):
        ConservativeStrategy(_pool(), {'router_preference': 'youngest'})

    def test_rejects_unknown_router_preference(self):
        with pytest.raises(ValueError, match='router_preference'):
            ConservativeStrategy(_pool(), {'router_preference': 'random'})


# ---------------------------------------------------------------------------
# on_tick scaling decisions
# ---------------------------------------------------------------------------

class TestOnTick:

    def test_no_pending_no_submit(self):
        h = _Harness(_pool())
        s = ConservativeStrategy(h.pool, {'min_dwell_sec': 0.0})
        s.on_tick(h.ctx)
        assert h.submitted == []

    def test_existing_capacity_absorbs_backlog(self):
        h = _Harness(_pool())
        for _ in range(2):
            h.add_task()
        h.add_pilot(capacity=4, in_flight=0)
        s = ConservativeStrategy(h.pool, {'min_dwell_sec': 0.0})
        s.on_tick(h.ctx)
        assert h.submitted == []

    def test_submits_when_pending_exceeds_capacity(self):
        h = _Harness(_pool())
        for _ in range(10):
            h.add_task()
        s = ConservativeStrategy(h.pool, {'min_dwell_sec': 0.0})
        s.on_tick(h.ctx)
        assert h.submitted == [None]   # None → pool.default_size

    def test_in_flight_submissions_bounded(self):
        h = _Harness(_pool())
        for _ in range(20):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 1})
        s.on_tick(h.ctx)
        assert h.submitted == [None]

        # Simulate that the first submission landed as a PENDING pilot.
        h.add_pilot(state=PILOT_PENDING, capacity=0, in_flight=0)
        s.on_tick(h.ctx)
        assert len(h.submitted) == 1        # bounded

    def test_starting_pilot_also_counts_as_in_flight(self):
        h = _Harness(_pool())
        for _ in range(10):
            h.add_task()
        h.add_pilot(state=PILOT_STARTING, capacity=0)
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 1})
        s.on_tick(h.ctx)
        assert h.submitted == []   # STARTING counts

    def test_dwell_blocks_rapid_resubmit(self):
        h = _Harness(_pool())
        for _ in range(20):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec': 60.0,
            'max_in_flight_submissions': 10,
        })
        s.on_tick(h.ctx)
        assert len(h.submitted) == 1

        h.advance(10)  # < 60
        s.on_tick(h.ctx)
        assert len(h.submitted) == 1   # dwell blocks

        h.advance(100)  # > 60 since last submit
        s.on_tick(h.ctx)
        assert len(h.submitted) == 2

    def test_max_pilots_bound_respected(self):
        h = _Harness(_pool(max_pilots=3))
        for _ in range(100):
            h.add_task()
        # Fill the fleet
        for _ in range(3):
            h.add_pilot(capacity=1, in_flight=1)
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 99})
        s.on_tick(h.ctx)
        assert h.submitted == []

    def test_terminal_pilots_do_not_count(self):
        h = _Harness(_pool(max_pilots=2))
        for _ in range(100):
            h.add_task()
        # One terminal, no live pilots → should submit
        h.add_pilot(state=PILOT_DONE, capacity=0)
        s = ConservativeStrategy(h.pool, {'min_dwell_sec': 0.0})
        s.on_tick(h.ctx)
        assert h.submitted == [None]


# ---------------------------------------------------------------------------
# pick_dispatch
# ---------------------------------------------------------------------------

class TestPickDispatch:

    def test_no_pending_returns_none(self):
        h = _Harness(_pool())
        h.add_pilot()
        s = ConservativeStrategy(h.pool, {})
        assert s.pick_dispatch(h.ctx) is None

    def test_no_active_pilot_returns_none(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(state=PILOT_PENDING, capacity=0)
        s = ConservativeStrategy(h.pool, {})
        assert s.pick_dispatch(h.ctx) is None

    def test_skips_full_pilot(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(capacity=1, in_flight=1)  # full
        s = ConservativeStrategy(h.pool, {})
        assert s.pick_dispatch(h.ctx) is None

    def test_highest_priority_first(self):
        h = _Harness(_pool())
        h.add_task(task_id='t.low',  priority=1, arrival_ts=100)
        h.add_task(task_id='t.high', priority=10, arrival_ts=200)
        h.add_task(task_id='t.mid',  priority=5, arrival_ts=150)
        h.add_pilot(capacity=10, in_flight=0)
        s = ConservativeStrategy(h.pool, {})
        pair = s.pick_dispatch(h.ctx)
        assert pair is not None
        task, _ = pair
        assert task.task_id == 't.high'

    def test_ties_broken_by_arrival(self):
        h = _Harness(_pool())
        h.add_task(task_id='t.later', priority=5, arrival_ts=200)
        h.add_task(task_id='t.earlier', priority=5, arrival_ts=100)
        h.add_pilot(capacity=10, in_flight=0)
        s = ConservativeStrategy(h.pool, {})
        pair = s.pick_dispatch(h.ctx)
        assert pair is not None
        task, _ = pair
        assert task.task_id == 't.earlier'

    def test_least_loaded_router(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(pid='p.busy', capacity=4, in_flight=3)
        h.add_pilot(pid='p.free', capacity=4, in_flight=0)
        s = ConservativeStrategy(h.pool, {'router_preference': 'least_loaded'})
        pair = s.pick_dispatch(h.ctx)
        assert pair is not None
        _, pilot = pair
        assert pilot.pid == 'p.free'

    def test_youngest_router(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(pid='p.old',   capacity=4, in_flight=0,
                    walltime_deadline=2000)
        h.add_pilot(pid='p.young', capacity=4, in_flight=0,
                    walltime_deadline=5000)
        s = ConservativeStrategy(h.pool, {'router_preference': 'youngest'})
        pair = s.pick_dispatch(h.ctx)
        assert pair is not None
        _, pilot = pair
        assert pilot.pid == 'p.young'

    def test_skips_non_queued_task(self):
        h = _Harness(_pool())
        h.add_task(state=TASK_DONE)         # already done
        h.add_pilot(capacity=10, in_flight=0)
        s = ConservativeStrategy(h.pool, {})
        assert s.pick_dispatch(h.ctx) is None


# ---------------------------------------------------------------------------
# should_terminate_pilot default
# ---------------------------------------------------------------------------

class TestTermination:

    def test_default_never_terminates(self):
        h = _Harness(_pool())
        h.add_pilot(state=PILOT_ACTIVE, in_flight=0)
        s = ConservativeStrategy(h.pool, {})
        assert s.should_terminate_pilot(h.ctx, h.pilots[0]) is False


# ---------------------------------------------------------------------------
# Failure-backoff guard
# ---------------------------------------------------------------------------

class TestFailureBackoff:

    def _failed_pilot(self, h: '_Harness') -> PilotRecord:
        h.add_pilot(state=PILOT_FAILED, started_tasks=0)
        return h.pilots[-1]

    def test_below_threshold_does_not_pause(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):  # 2 < 3 threshold
            p = self._failed_pilot(h)
            s.on_pilot_state(h.ctx, p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h.ctx)
        assert h.submitted == [None]

    def test_threshold_pauses_submissions(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(3):
            p = self._failed_pilot(h)
            s.on_pilot_state(h.ctx, p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h.ctx)
        assert h.submitted == []

    def test_backoff_expires(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 2,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):
            p = self._failed_pilot(h)
            s.on_pilot_state(h.ctx, p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h.ctx)
        assert h.submitted == []
        h.advance(61.0)
        s.on_tick(h.ctx)
        assert h.submitted == [None]

    def test_active_resets_failure_counter(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):
            p = self._failed_pilot(h)
            s.on_pilot_state(h.ctx, p, PILOT_PENDING, PILOT_FAILED)
        h.add_pilot(state=PILOT_ACTIVE, started_tasks=0)
        s.on_pilot_state(h.ctx, h.pilots[-1], PILOT_STARTING, PILOT_ACTIVE)
        # one more failure should not trip the guard (counter reset)
        p = self._failed_pilot(h)
        s.on_pilot_state(h.ctx, p, PILOT_PENDING, PILOT_FAILED)
        # ACTIVE pilot has 2 free capacity, but pending=5 > free=2,
        # so on_tick should still submit
        s.on_tick(h.ctx)
        assert h.submitted == [None]

    def test_failure_after_running_tasks_does_not_count(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = ConservativeStrategy(h.pool, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 2,
            'failure_backoff_sec'     : 60.0,
        })
        # pilots that ran tasks before failing don't count as
        # "submission failure" — they did productive work
        for _ in range(5):
            h.add_pilot(state=PILOT_FAILED, started_tasks=3)
            s.on_pilot_state(h.ctx, h.pilots[-1],
                             PILOT_ACTIVE, PILOT_FAILED)
        s.on_tick(h.ctx)
        assert h.submitted == [None]


# ---------------------------------------------------------------------------
# load_strategy integration
# ---------------------------------------------------------------------------

class TestLoadStrategy:

    def test_conservative_is_builtin(self):
        pool = _pool()
        s = load_strategy('conservative', pool, {})
        assert isinstance(s, ConservativeStrategy)

    def test_unknown_reports_builtins(self):
        pool = _pool()
        with pytest.raises(StrategyNotFound, match='conservative'):
            load_strategy('nope', pool, {})

    def test_dotted_spec_loads(self):
        pool = _pool()
        s = load_strategy(
            'radical.edge.task_dispatcher_strategy_conservative'
            ':ConservativeStrategy', pool, {})
        assert isinstance(s, ConservativeStrategy)
