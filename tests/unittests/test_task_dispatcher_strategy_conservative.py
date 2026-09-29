"""Unit tests for ConservativePolicy — the dispatcher's one dispatch policy.

Covers: no eager scale-up, bounded in-flight pilot submissions, dwell
suppression, max_pilots bound, pick_dispatch priority ordering,
router_preference variants, and the consecutive-failure backoff guard.

``ConservativePolicy`` is a plain class (no ABC, no pluggable-strategy
loader): it reads live state directly off a ``pool_state``-shaped object
(``pending_queue()`` / ``live_pilots()``, matching
:class:`~radical.orbit.plugin_task_dispatcher.PoolState`) and requests
submissions through a callable passed to :meth:`on_tick`.  The ``_Harness``
below stands in for a real ``PoolState``.
"""

import pytest

from radical.orbit.task_dispatcher_config import (
    PoolConfig, PilotSize, PoolMember,
)
from radical.orbit.task_dispatcher_state import (
    PilotRecord, TaskRecord,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE, PILOT_DONE, PILOT_FAILED,
    PILOT_LIVE_STATES,
    TASK_QUEUED, TASK_DONE,
)
from radical.orbit.task_dispatcher_strategy_conservative import (
    ConservativePolicy,
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
    """Minimal ``pool_state``-shaped driver — lets tests control state + time.

    Mirrors the read methods :class:`PoolState` exposes to the policy:
    ``pending_queue()`` (QUEUED tasks, priority-ordered), ``live_pilots()``
    (non-terminal pilots) and the six member helpers plan 121 §5 adds —
    plus a ``submit_pilot`` callable to pass into
    :meth:`ConservativePolicy.on_tick`.
    """

    def __init__(self, pool: PoolConfig):
        self.pool      = pool
        self.time      = 1000.0
        self.tasks     : list[TaskRecord]  = []
        self.pilots    : list[PilotRecord] = []
        self.submitted : list[str | None]  = []
        self.submitted_members: list[str | None] = []
        # member_id -> node-hours already consumed (tests set this directly)
        self.node_hours: dict[str, float]  = {}

    def pending_queue(self) -> list[TaskRecord]:
        pending = [t for t in self.tasks if t.state == TASK_QUEUED]
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))
        return pending

    def live_pilots(self) -> list[PilotRecord]:
        return [p for p in self.pilots if p.state in PILOT_LIVE_STATES]

    # -- member surface (plan 121 §5) -----------------------------------
    # A legacy pool has exactly one implicit member, so a single-member
    # harness drives the policy through exactly the pre-121 arithmetic.

    def members(self) -> list[PoolMember]:
        return list(self.pool.members.values())

    def member(self, mid) -> PoolMember | None:
        return self.pool.member(mid)

    def live_pilots_for(self, mid) -> list[PilotRecord]:
        return [p for p in self.live_pilots() if p.member_id == mid]

    def member_budget_left(self, mid, now=None) -> float | None:
        m     = self.member(mid)
        total = (m.budget or {}).get('node_hours') if m else None
        if not total:
            return None
        return total - self.node_hours.get(mid, 0.0)

    def size_of(self, pilot) -> PilotSize | None:
        if pilot.cpus_per_node:
            return PilotSize(nodes=pilot.nodes,
                             cpus_per_node=pilot.cpus_per_node,
                             gpus_per_node=pilot.gpus_per_node,
                             rhapsody_backend=pilot.rhapsody_backend)
        m = self.member(pilot.member_id)
        return m.pilot_sizes.get(pilot.size_key) if m else None

    def submit_pilot(self, size_key: str | None = None, *,
                     member_id: str | None = None) -> str:
        self.submitted.append(size_key)
        self.submitted_members.append(member_id)
        return f'p.sub{len(self.submitted)}'

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


def _policy(h: _Harness, cfg: dict) -> ConservativePolicy:
    return ConservativePolicy(h.pool, cfg, now=lambda: h.time)


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestInit:

    def test_accepts_least_loaded(self):
        ConservativePolicy(_pool(), {'router_preference': 'least_loaded'})

    def test_accepts_youngest(self):
        ConservativePolicy(_pool(), {'router_preference': 'youngest'})

    def test_rejects_unknown_router_preference(self):
        with pytest.raises(ValueError, match='router_preference'):
            ConservativePolicy(_pool(), {'router_preference': 'random'})


# ---------------------------------------------------------------------------
# on_tick scaling decisions
# ---------------------------------------------------------------------------

class TestOnTick:

    def test_no_pending_no_submit(self):
        h = _Harness(_pool())
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []

    def test_existing_capacity_absorbs_backlog(self):
        h = _Harness(_pool())
        for _ in range(2):
            h.add_task()
        h.add_pilot(capacity=4, in_flight=0)
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []

    def test_submits_when_pending_exceeds_capacity(self):
        h = _Harness(_pool())
        for _ in range(10):
            h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]   # None → pool.default_size

    def test_in_flight_submissions_bounded(self):
        h = _Harness(_pool())
        for _ in range(20):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 1})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]

        # Simulate that the first submission landed as a PENDING pilot.
        h.add_pilot(state=PILOT_PENDING, capacity=0, in_flight=0)
        s.on_tick(h, h.submit_pilot)
        assert len(h.submitted) == 1        # bounded

    def test_starting_pilot_also_counts_as_in_flight(self):
        h = _Harness(_pool())
        for _ in range(10):
            h.add_task()
        h.add_pilot(state=PILOT_STARTING, capacity=0)
        s = _policy(h, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 1})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []   # STARTING counts

    def test_dwell_blocks_rapid_resubmit(self):
        h = _Harness(_pool())
        for _ in range(20):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec': 60.0,
            'max_in_flight_submissions': 10,
        })
        s.on_tick(h, h.submit_pilot)
        assert len(h.submitted) == 1

        h.advance(10)  # < 60
        s.on_tick(h, h.submit_pilot)
        assert len(h.submitted) == 1   # dwell blocks

        h.advance(100)  # > 60 since last submit
        s.on_tick(h, h.submit_pilot)
        assert len(h.submitted) == 2

    def test_max_pilots_bound_respected(self):
        h = _Harness(_pool(max_pilots=3))
        for _ in range(100):
            h.add_task()
        # Fill the fleet
        for _ in range(3):
            h.add_pilot(capacity=1, in_flight=1)
        s = _policy(h, {
            'min_dwell_sec': 0.0, 'max_in_flight_submissions': 99})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []

    def test_terminal_pilots_do_not_count(self):
        h = _Harness(_pool(max_pilots=2))
        for _ in range(100):
            h.add_task()
        # One terminal, no live pilots → should submit
        h.add_pilot(state=PILOT_DONE, capacity=0)
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]


# ---------------------------------------------------------------------------
# pick_dispatch
# ---------------------------------------------------------------------------

class TestPickDispatch:

    def test_no_pending_returns_none(self):
        h = _Harness(_pool())
        h.add_pilot()
        s = _policy(h, {})
        assert s.pick_dispatch(h) is None

    def test_no_active_pilot_returns_none(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(state=PILOT_PENDING, capacity=0)
        s = _policy(h, {})
        assert s.pick_dispatch(h) is None

    def test_skips_full_pilot(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(capacity=1, in_flight=1)  # full
        s = _policy(h, {})
        assert s.pick_dispatch(h) is None

    def test_highest_priority_first(self):
        h = _Harness(_pool())
        h.add_task(task_id='t.low',  priority=1, arrival_ts=100)
        h.add_task(task_id='t.high', priority=10, arrival_ts=200)
        h.add_task(task_id='t.mid',  priority=5, arrival_ts=150)
        h.add_pilot(capacity=10, in_flight=0)
        s = _policy(h, {})
        pair = s.pick_dispatch(h)
        assert pair is not None
        task, _ = pair
        assert task.task_id == 't.high'

    def test_ties_broken_by_arrival(self):
        h = _Harness(_pool())
        h.add_task(task_id='t.later', priority=5, arrival_ts=200)
        h.add_task(task_id='t.earlier', priority=5, arrival_ts=100)
        h.add_pilot(capacity=10, in_flight=0)
        s = _policy(h, {})
        pair = s.pick_dispatch(h)
        assert pair is not None
        task, _ = pair
        assert task.task_id == 't.earlier'

    def test_least_loaded_router(self):
        h = _Harness(_pool())
        h.add_task()
        h.add_pilot(pid='p.busy', capacity=4, in_flight=3)
        h.add_pilot(pid='p.free', capacity=4, in_flight=0)
        s = _policy(h, {'router_preference': 'least_loaded'})
        pair = s.pick_dispatch(h)
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
        s = _policy(h, {'router_preference': 'youngest'})
        pair = s.pick_dispatch(h)
        assert pair is not None
        _, pilot = pair
        assert pilot.pid == 'p.young'

    def test_skips_non_queued_task(self):
        h = _Harness(_pool())
        h.add_task(state=TASK_DONE)         # already done
        h.add_pilot(capacity=10, in_flight=0)
        s = _policy(h, {})
        assert s.pick_dispatch(h) is None


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
        s = _policy(h, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):  # 2 < 3 threshold
            p = self._failed_pilot(h)
            s.on_pilot_state(p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]

    def test_threshold_pauses_submissions(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(3):
            p = self._failed_pilot(h)
            s.on_pilot_state(p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []

    def test_backoff_expires(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 2,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):
            p = self._failed_pilot(h)
            s.on_pilot_state(p, PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []
        h.advance(61.0)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]

    def test_active_resets_failure_counter(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 3,
            'failure_backoff_sec'     : 60.0,
        })
        for _ in range(2):
            p = self._failed_pilot(h)
            s.on_pilot_state(p, PILOT_PENDING, PILOT_FAILED)
        h.add_pilot(state=PILOT_ACTIVE, started_tasks=0)
        s.on_pilot_state(h.pilots[-1], PILOT_STARTING, PILOT_ACTIVE)
        # one more failure should not trip the guard (counter reset)
        p = self._failed_pilot(h)
        s.on_pilot_state(p, PILOT_PENDING, PILOT_FAILED)
        # ACTIVE pilot has 2 free capacity, but pending=5 > free=2,
        # so on_tick should still submit
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]

    def test_failure_after_running_tasks_does_not_count(self):
        h = _Harness(_pool())
        for _ in range(5):
            h.add_task()
        s = _policy(h, {
            'min_dwell_sec'           : 0.0,
            'max_consecutive_failures': 2,
            'failure_backoff_sec'     : 60.0,
        })
        # pilots that ran tasks before failing don't count as
        # "submission failure" — they did productive work
        for _ in range(5):
            h.add_pilot(state=PILOT_FAILED, started_tasks=3)
            s.on_pilot_state(h.pilots[-1], PILOT_ACTIVE, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == [None]


# ---------------------------------------------------------------------------
# Capability-class pools: per-member scale-up and matching (plan 121 §5)
# ---------------------------------------------------------------------------

def _mem(mid, **overrides) -> PoolMember:
    defaults = dict(
        member_id=mid, endpoint_name=f'ep_{mid}', queue='q', account=None,
        pilot_sizes={'s': PilotSize(nodes=1, cpus_per_node=4,
                                    rhapsody_backend='concurrent')},
        default_size='s', min_pilots=0, max_pilots=4)
    defaults.update(overrides)
    return PoolMember(**defaults)


def _class_pool(*members, **overrides) -> PoolConfig:
    defaults = dict(
        name='fed', queue='q', account=None,
        pilot_sizes=members[0].pilot_sizes, default_size='s',
        max_pilots=4, pool_class='gpu', multi_member=True,
        members={m.member_id: m for m in members})
    defaults.update(overrides)
    return PoolConfig(**defaults)


class TestMemberFloor:

    def test_min_pilots_honoured_per_member(self):
        h = _Harness(_class_pool(_mem('a', min_pilots=1),
                                 _mem('b', min_pilots=1)))
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['a']
        h.add_pilot(member_id='a', state=PILOT_PENDING)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['a', 'b']

    def test_floor_fires_without_any_pending_task(self):
        h = _Harness(_class_pool(_mem('a', min_pilots=1)))
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['a']

    def test_legacy_pool_has_no_floor_by_default(self):
        """min_pilots defaults to 0, so a legacy pool is untouched."""
        h = _Harness(_pool())
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted == []


class TestMemberBacklog:

    def _sw_pool(self):
        return _class_pool(
            _mem('a', attributes={'software': ['lammps']}),
            _mem('b', attributes={'software': ['pytorch']}))

    def test_grows_the_member_that_matches(self):
        h = _Harness(self._sw_pool())
        h.add_task(requirements={'software': ['pytorch']})
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_no_matching_member_submits_nothing(self):
        h = _Harness(self._sw_pool())
        h.add_task(requirements={'software': ['vasp']})
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == []

    def test_unservable_task_grows_even_with_idle_capacity(self):
        """An idle CPU pilot is not capacity for a GPU task."""
        h = _Harness(self._sw_pool())
        h.add_pilot(member_id='a', attributes={'software': ['lammps']},
                    capacity=8, in_flight=0, cpus_per_node=4, nodes=1)
        h.add_task(requirements={'software': ['pytorch']})
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_servable_backlog_within_capacity_does_not_grow(self):
        h = _Harness(self._sw_pool())
        h.add_pilot(member_id='a', attributes={'software': ['lammps']},
                    capacity=8, in_flight=0, cpus_per_node=4, nodes=1)
        h.add_task(requirements={'software': ['lammps']})
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == []


class TestMemberGuards:

    def test_budget_exhausted_member_is_skipped(self):
        h = _Harness(_class_pool(_mem('a', budget={'node_hours': 10.0}),
                                 _mem('b')))
        h.node_hours['a'] = 10.0
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_dwell_is_per_member(self):
        """Member a inside its dwell window must not block member b."""
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_task()
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 30.0, 'member_preference':
                        'least_loaded'})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['a']
        h.add_pilot(member_id='a', state=PILOT_ACTIVE, capacity=0)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['a', 'b']

    def test_member_max_pilots_is_per_member(self):
        h = _Harness(_class_pool(_mem('a', max_pilots=1), _mem('b')))
        h.add_pilot(member_id='a', state=PILOT_ACTIVE, capacity=0)
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_failure_backoff_is_per_member(self):
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0,
                        'max_consecutive_failures': 1,
                        'failure_backoff_sec': 60.0})
        h.add_pilot(member_id='a', state=PILOT_FAILED, started_tasks=0)
        s.on_pilot_state(h.pilots[-1], PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_pool_ceiling_bounds_total_in_flight(self):
        """R5: every member may have one submission in flight, no more."""
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_task()
        h.add_pilot(member_id='a', state=PILOT_PENDING)
        h.add_pilot(member_id='b', state=PILOT_PENDING)
        s = _policy(h, {'min_dwell_sec': 0.0,
                        'max_in_flight_submissions': 1})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == []


class TestMemberPreference:

    def test_budget_prefers_the_member_with_more_headroom(self):
        h = _Harness(_class_pool(_mem('a', budget={'node_hours': 10.0}),
                                 _mem('b', budget={'node_hours': 10.0})))
        h.node_hours['a'] = 9.0
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0, 'member_preference': 'budget'})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_least_loaded_prefers_the_emptier_member(self):
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_pilot(member_id='a', state=PILOT_ACTIVE, capacity=0)
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0,
                        'member_preference': 'least_loaded'})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_ties_break_on_member_id(self):
        h = _Harness(_class_pool(_mem('zeta'), _mem('alpha')))
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['alpha']

    def test_rejects_unknown_member_preference(self):
        with pytest.raises(ValueError, match='member_preference'):
            ConservativePolicy(_pool(), {'member_preference': 'random'})


class TestMaxRequeues:

    def test_default_is_one(self):
        assert ConservativePolicy(_pool(), {}).max_requeues == 1

    def test_configurable(self):
        assert ConservativePolicy(_pool(), {'max_requeues': 3}).max_requeues \
            == 3


class TestAttributeAwareDispatch:

    def test_never_returns_a_pilot_missing_the_software(self):
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_pilot(member_id='a', attributes={'software': ['lammps']},
                    capacity=4, cpus_per_node=4, nodes=1)
        h.add_task(requirements={'software': ['pytorch']})
        s = _policy(h, {})
        assert s.pick_dispatch(h) is None

    def test_routes_to_the_matching_pilot(self):
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_pilot(member_id='a', attributes={'software': ['lammps']},
                    capacity=4, cpus_per_node=4, nodes=1)
        h.add_pilot(member_id='b', attributes={'software': ['pytorch']},
                    capacity=4, cpus_per_node=4, nodes=1)
        h.add_task(requirements={'software': ['pytorch']})
        task, pilot = s_pick(h)
        assert pilot.member_id == 'b'
        assert task is h.tasks[0]

    def test_unservable_head_task_is_skipped_not_blocking(self):
        """The deliberate semantic change: a GPU task must not block CPU
        tasks (plan 121 §5, risk R2)."""
        h = _Harness(_class_pool(_mem('a'), _mem('b')))
        h.add_pilot(member_id='a', attributes={'software': ['lammps']},
                    capacity=4, cpus_per_node=4, nodes=1)
        h.add_task(priority=9, requirements={'software': ['pytorch']})
        h.add_task(priority=0, requirements={'software': ['lammps']})
        task, pilot = s_pick(h)
        assert task.priority == 0            # the lower-priority one runs
        assert pilot.member_id == 'a'

    def test_size_snapshot_is_used_for_the_gpu_test(self):
        h = _Harness(_class_pool(_mem('a')))
        h.add_pilot(member_id='a', capacity=4, nodes=1,
                    cpus_per_node=4, gpus_per_node=0)
        h.add_task(requirements={'gpus': 1})
        assert _policy(h, {}).pick_dispatch(h) is None


def s_pick(h):
    got = _policy(h, {}).pick_dispatch(h)
    assert got is not None
    return got


class TestFloorDoesNotStarve:
    """A member stuck below its floor (dead site, exhausted budget, backoff)
    must not stop every sibling from ever growing."""

    def test_budget_dead_floor_member_falls_through_to_backlog(self):
        h = _Harness(_class_pool(
            _mem('a', min_pilots=1, budget={'node_hours': 1.0}),
            _mem('b')))
        h.node_hours['a'] = 1.0          # a can never be grown again
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_backed_off_floor_member_falls_through(self):
        h = _Harness(_class_pool(_mem('a', min_pilots=1), _mem('b')))
        h.add_task()
        s = _policy(h, {'min_dwell_sec': 0.0,
                        'max_consecutive_failures': 1,
                        'failure_backoff_sec': 60.0})
        h.add_pilot(member_id='a', state=PILOT_FAILED, started_tasks=0)
        s.on_pilot_state(h.pilots[-1], PILOT_PENDING, PILOT_FAILED)
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['b']

    def test_no_backlog_and_a_dead_floor_member_submits_nothing(self):
        h = _Harness(_class_pool(
            _mem('a', min_pilots=1, budget={'node_hours': 1.0})))
        h.node_hours['a'] = 1.0
        s = _policy(h, {'min_dwell_sec': 0.0})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == []

    def test_floor_is_served_in_declaration_order_not_by_preference(self):
        """The floor is a debt owed in declaration order."""
        h = _Harness(_class_pool(
            _mem('zeta', min_pilots=1, budget={'node_hours': 10.0}),
            _mem('alpha', min_pilots=1, budget={'node_hours': 10.0})))
        h.node_hours['zeta'] = 1.0       # alpha has more headroom
        s = _policy(h, {'min_dwell_sec': 0.0, 'member_preference': 'budget'})
        s.on_tick(h, h.submit_pilot)
        assert h.submitted_members == ['zeta']
