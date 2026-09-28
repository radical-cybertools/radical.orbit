'''
Task dispatcher — the conservative dispatch policy.

This is the dispatcher's default policy, registered as ``'conservative'``
in the manual registry in :mod:`~radical.orbit.task_dispatcher_policy`.
It reads live pool state directly off the :class:`PoolState` handed to it
and requests pilot submissions through a callable the dispatcher supplies.

A pool is a **capability class** with one or more members, so every piece of
bookkeeping below is **per member**: dwell, in-flight submissions, the pilot
ceiling and the failure backoff.  A legacy single-site pool has exactly one
(implicit) member, for which every one of those is arithmetically identical
to the pre-121 pool-level version.

Policy
------
- Scale-up only on the housekeeping tick, one submission per tick.
- Honour each member's ``min_pilots`` floor first (declaration order) --
  but fall through to the backlog step when no under-floor member clears
  its guards, so one dead site cannot starve its siblings.
- Otherwise scale up when the backlog exceeds the capacity that can
  actually *serve* it — a pending task no live pilot could ever run
  (attributes, size) counts as un-served however idle the fleet is.
- Grow the member that can run the task at the head of that backlog,
  ranked by ``member_preference`` (remaining budget, or load).
- Bound in-flight pilot submissions per member
  (``max_in_flight_submissions``, default 2) *and* pool-wide
  (``max(max_in_flight_submissions, len(members))``, so a class pool never
  warms up more slowly than the single-member pools it replaces).
- Respect ``min_dwell_sec`` between successive submissions **for the same
  member** — a new member does not wait behind a sibling's dwell window.
- ``pick_dispatch``: highest-priority pending task first (ties broken by
  arrival order); routed to an active pilot with free capacity **whose
  attributes and size satisfy the task's requirements**.  A task no pilot
  can serve is *skipped*, not head-of-line blocking (see below).
- Among candidate pilots, prefer fewest ``in_flight`` (``'least_loaded'``)
  or youngest (``'youngest'``), configurable via
  ``strategy_config.router_preference``; ties break on ``member_id``.
- Pilots are never terminated early; they expire at walltime.

Deliberate semantic change (plan 121 §5, risk R2)
-------------------------------------------------
Pre-121, a top-priority task with no available pilot returned ``None`` and
stalled the whole drain.  In a class pool a GPU task must not block CPU
tasks, so an unservable task is **skipped** and the next one is considered.
That *is* a priority inversion — a low-priority task that fits can run
before a high-priority one that does not.  It is correct for class pools
and documented as such; a future ``strict_priority`` knob is the escape
hatch.  The drain loop is still bounded by the pending-queue length, so the
cost is O(pending²) worst case at demo scale (tens).

Knobs (``strategy_config``)
---------------------------
- ``min_dwell_sec``            : float = 30    (per member)
- ``max_in_flight_submissions``: int   = 2     (per member; also a
                                 pool-wide floor of ``len(members)``)
- ``router_preference``        : str   = ``'least_loaded'``
                                  (or ``'youngest'``)
- ``max_consecutive_failures`` : int   = 3     (per member)
- ``failure_backoff_sec``      : float = 60
- ``member_preference``        : str   = ``'budget'``
                                  (or ``'least_loaded'``)
- ``max_requeues``             : int   = 1
'''

from __future__ import annotations

import logging
import time

from typing import TYPE_CHECKING, Callable

from .task_dispatcher_config import PoolConfig
from .task_dispatcher_match  import satisfies
from .task_dispatcher_policy import DispatchPolicy
from .task_dispatcher_state  import (
    PILOT_ACTIVE, PILOT_FAILED, PILOT_PENDING, PILOT_STARTING,
    TASK_QUEUED,
)

if TYPE_CHECKING:
    from .task_dispatcher_config import PoolMember
    from .task_dispatcher_state  import PilotRecord, TaskRecord

log = logging.getLogger('radical.orbit')


# Pilot states that count as "submitted but not yet active"
_PRE_ACTIVE = {PILOT_PENDING, PILOT_STARTING}


class ConservativePolicy(DispatchPolicy):
    '''Conservative scale-up + priority dispatch for one pool.

    One instance per :class:`PoolState`.  Reads live state off the pool
    handle passed to each method; requests submissions through the
    ``submit_pilot`` callable the dispatcher passes to :meth:`on_tick`.
    '''

    def __init__(self, pool: PoolConfig, cfg: dict,
                 now: Callable[[], float] = time.time) -> None:
        super().__init__(pool, cfg, now)

        self._min_dwell_sec       : float = float(
            cfg.get('min_dwell_sec', 30.0))
        self._max_in_flight_subs  : int   = int(
            cfg.get('max_in_flight_submissions', 2))
        self._router_preference   : str   = str(
            cfg.get('router_preference', 'least_loaded'))
        self._max_consecutive_failures: int = int(
            cfg.get('max_consecutive_failures', 3))
        self._failure_backoff_sec : float = float(
            cfg.get('failure_backoff_sec', 60.0))
        self._member_preference   : str   = str(
            cfg.get('member_preference', 'budget'))
        self._max_requeues        : int   = int(cfg.get('max_requeues', 1))

        if self._router_preference not in ('least_loaded', 'youngest'):
            raise ValueError(
                f"ConservativePolicy: unknown router_preference "
                f"{self._router_preference!r}; expected 'least_loaded' "
                f"or 'youngest'")

        if self._member_preference not in ('budget', 'least_loaded'):
            raise ValueError(
                f"ConservativePolicy: unknown member_preference "
                f"{self._member_preference!r}; expected 'budget' "
                f"or 'least_loaded'")

        # Per-member bookkeeping, keyed on the member id (``''`` for a
        # legacy pool's implicit member, on the pilot record and the
        # PoolMember alike).
        self._last_submit_ts      : dict[str, float] = {}
        # Failure-backoff guard: pause submissions for a member when N of
        # its pilots fail consecutively without ever reaching ACTIVE.  Any
        # of that member's pilots reaching ACTIVE resets its counter.
        self._consecutive_failures: dict[str, int]   = {}
        self._backoff_until       : dict[str, float] = {}
        self._backoff_logged      : dict[str, bool]  = {}

    @property
    def max_requeues(self) -> int:
        '''How often a pilot loss may re-queue one task before it fails.'''
        return self._max_requeues

    # -- signals ---------------------------------------------------------

    def on_pilot_state(self, pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        '''Track consecutive pilot failures and trip the backoff guard.

        Submission-side failure: pilot went PENDING/STARTING → FAILED without
        ever reaching ACTIVE and without running any tasks.  After
        ``max_consecutive_failures`` such failures we pause submissions **for
        that member** for ``failure_backoff_sec``.  Any of its pilots
        reaching ACTIVE resets its counter — a member whose endpoint is down
        must not silence a healthy sibling.
        '''
        mid = pilot.member_id

        if new_state == PILOT_ACTIVE:
            self._consecutive_failures[mid] = 0
            self._backoff_until[mid]        = 0.0
            self._backoff_logged[mid]       = False
            return

        if (new_state == PILOT_FAILED
                and old_state in (PILOT_PENDING, PILOT_STARTING)
                and pilot.started_tasks == 0):
            fails = self._consecutive_failures.get(mid, 0) + 1
            self._consecutive_failures[mid] = fails
            if fails >= self._max_consecutive_failures:
                self._backoff_until[mid]  = \
                    self._now() + self._failure_backoff_sec
                self._backoff_logged[mid] = False
                log.warning(
                    "conservative[%s]: %d consecutive pilot failures on "
                    "member %r; pausing submissions for %.0fs",
                    self._pool.name, fails, mid, self._failure_backoff_sec)

    # -- member helpers --------------------------------------------------

    def _in_backoff(self, mid: str, now_ts: float) -> bool:
        '''Return whether *mid* is inside its failure-backoff window.'''
        until = self._backoff_until.get(mid, 0.0)
        if now_ts >= until:
            return False
        if not self._backoff_logged.get(mid):
            log.info("conservative[%s]: member %r backoff active for "
                     "%.0fs more", self._pool.name, mid, until - now_ts)
            self._backoff_logged[mid] = True
        return True

    def _budget_fraction(self, pool_state, member: 'PoolMember',
                         now_ts: float) -> float:
        '''Return the member's remaining budget as a 0..1 fraction.

        A member with **no declared budget** sorts as ``1.0`` — i.e. as
        fully funded, so a declared allocation with headroom ranks level
        with it and only a *busier* unbounded member loses.
        '''
        total = (member.budget or {}).get('node_hours')
        if not total:
            return 1.0
        left = pool_state.member_budget_left(member.member_id, now_ts)
        if left is None:
            return 1.0
        return max(0.0, min(1.0, left / total))

    def _pass_guards(self, pool_state, candidates, now_ts):
        '''Return the candidates that clear every per-member submit guard.

        Failure backoff, ``max_in_flight_submissions``,
        ``member.max_pilots``, ``min_dwell_sec`` since *that member's* last
        submit, and an exhausted declared budget.  Declaration order is
        preserved.
        '''
        eligible = []
        for m in candidates:
            mid = m.member_id
            if self._in_backoff(mid, now_ts):
                continue
            mine = pool_state.live_pilots_for(mid)
            if len(mine) >= m.max_pilots:
                continue
            if sum(1 for p in mine if p.state in _PRE_ACTIVE) \
                    >= self._max_in_flight_subs:
                continue
            if now_ts - self._last_submit_ts.get(mid, 0.0) \
                    < self._min_dwell_sec:
                continue
            left = pool_state.member_budget_left(mid, now_ts)
            if left is not None and left <= 0:
                continue
            eligible.append(m)
        return eligible

    # -- decisions -------------------------------------------------------

    def on_tick(self, pool_state,
                submit_pilot: Callable[..., str]) -> None:
        '''Maybe submit one pilot for one member.

        Conservative: at most one submission per tick, bounded in-flight,
        respecting each member's ``min_dwell_sec``.  Order: ``min_pilots``
        floor, then backlog.
        '''
        now_ts  = self._now()
        members = pool_state.members()
        if not members:
            return

        # -- 1. floor: members below their min_pilots, declaration order --
        # The floor is tried first, but it must not be able to *starve* the
        # pool: a member whose site is down sits below its floor forever,
        # and returning here would stop every sibling from ever growing.
        # So a floor step that survives no guard falls through to backlog.
        reason   = 'min_pilots'
        floor    = [m for m in members
                    if len(pool_state.live_pilots_for(m.member_id))
                    < m.min_pilots]
        eligible = self._pass_guards(pool_state, floor, now_ts)

        if not eligible:
            # -- 2. backlog ----------------------------------------------
            reason  = 'backlog'
            pending = pool_state.pending_queue()
            if not pending:
                return

            live = pool_state.live_pilots()

            # A pending task no *live* pilot could ever take is un-served
            # however idle the fleet looks; and only a pilot that can serve
            # at least one pending task contributes its free slots.
            unservable = []
            servers    = set()
            for task in pending:
                served = False
                for p in live:
                    if satisfies(task.requirements, p.attributes,
                                 pool_state.size_of(p)) is None:
                        served = True
                        servers.add(p.pid)
                if not served:
                    unservable.append(task)

            free_capacity = sum(p.free_capacity() for p in live
                                if p.state == PILOT_ACTIVE
                                and p.pid in servers)
            if not unservable and len(pending) <= free_capacity:
                return  # existing capacity will absorb the backlog

            # Grow a member that can run the task at the head of the
            # backlog -- the oldest unservable one when there is one.
            head = unservable[0] if unservable else pending[0]
            candidates = [m for m in members
                          if m.reject_reason(head.requirements) is None]
            eligible = self._pass_guards(pool_state, candidates, now_ts)

        if not eligible:
            return

        # Pool-wide submission ceiling (plan 121 §14 R5): every member may
        # always have one submission in flight, and a pool with fewer
        # members than the knob still gets the knob.
        pool_ceiling   = max(self._max_in_flight_subs, len(members))
        in_flight_subs = sum(1 for p in pool_state.live_pilots()
                             if p.state in _PRE_ACTIVE)
        if in_flight_subs >= pool_ceiling:
            return

        # -- 3. rank -----------------------------------------------------
        # The floor is a debt owed in declaration order, so it is served in
        # that order; only the backlog step ranks by member_preference.
        if reason == 'backlog':
            def _key(m: 'PoolMember'):
                frac = self._budget_fraction(pool_state, m, now_ts)
                load = len(pool_state.live_pilots_for(m.member_id))
                if self._member_preference == 'least_loaded':
                    return (load, -frac, m.member_id)
                return (-frac, load, m.member_id)

            eligible.sort(key=_key)
        chosen = eligible[0]

        # -- 4. submit ---------------------------------------------------
        try:
            pid = submit_pilot(None, member_id=chosen.member_id)
            self._last_submit_ts[chosen.member_id] = now_ts
            log.info("conservative[%s]: submitted pilot %s "
                     "(member=%s, reason=%s, in_flight_subs=%d)",
                     self._pool.name, pid, chosen.member_id, reason,
                     in_flight_subs + 1)
        except Exception as e:
            log.warning("conservative[%s]: submit_pilot failed for member "
                        "%r: %s", self._pool.name, chosen.member_id, e)

    def pick_dispatch(self, pool_state) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        '''Return the highest-priority *servable* queued task + best pilot.

        Called repeatedly by the dispatcher until it returns ``None``.  A
        task whose requirements no candidate pilot satisfies is skipped
        rather than blocking the queue — see the module docstring.
        '''
        pending = [t for t in pool_state.pending_queue()
                   if t.state == TASK_QUEUED]
        if not pending:
            return None

        # pending_queue() is already priority-ordered; re-sort defensively.
        # Tie-break: earlier arrival first.
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))

        active = [p for p in pool_state.live_pilots()
                  if p.state == PILOT_ACTIVE and p.free_capacity() > 0]
        if not active:
            return None

        for task in pending:
            cands = [p for p in active
                     if satisfies(task.requirements, p.attributes,
                                  pool_state.size_of(p)) is None]
            if not cands:
                continue

            if self._router_preference == 'youngest':
                # Most remaining walltime = largest walltime_deadline
                cands.sort(key=lambda p: (-p.walltime_deadline,
                                          p.member_id or ''))
            else:  # 'least_loaded'
                cands.sort(key=lambda p: (p.in_flight, -p.walltime_deadline,
                                          p.member_id or ''))

            return task, cands[0]

        return None
