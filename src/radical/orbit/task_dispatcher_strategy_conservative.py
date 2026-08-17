'''
Task dispatcher — the conservative dispatch policy.

This is the dispatcher's default policy, registered as ``'conservative'``
in the manual registry in :mod:`~radical.orbit.task_dispatcher_policy`.
It reads live pool state directly off the :class:`PoolState` handed to it
and requests pilot submissions through a callable the dispatcher supplies.

Policy
------
- Scale-up only on the housekeeping tick, and only when
  ``pending > sum(free capacity of active pilots)``.
- Bound in-flight pilot submissions
  (``max_in_flight_submissions``, default 2).
- Respect ``min_dwell_sec`` between successive submissions.
- ``pick_dispatch``: highest-priority pending task first (ties broken by
  arrival order); routed to an active pilot with free capacity.
- Among candidate pilots, prefer fewest ``in_flight`` (``'least_loaded'``)
  or youngest (``'youngest'``), configurable via
  ``strategy_config.router_preference``.
- Pilots are never terminated early; they expire at walltime.

Knobs (``strategy_config``)
---------------------------
- ``min_dwell_sec``            : float = 30
- ``max_in_flight_submissions``: int   = 2
- ``router_preference``        : str   = ``'least_loaded'``
                                  (or ``'youngest'``)
- ``max_consecutive_failures`` : int   = 3
- ``failure_backoff_sec``      : float = 60
'''

from __future__ import annotations

import logging
import time

from typing import TYPE_CHECKING, Callable

from .task_dispatcher_config import PoolConfig
from .task_dispatcher_policy import DispatchPolicy
from .task_dispatcher_state  import (
    PILOT_ACTIVE, PILOT_FAILED, PILOT_PENDING, PILOT_STARTING,
    PILOT_LIVE_STATES, TASK_QUEUED,
)

if TYPE_CHECKING:
    from .task_dispatcher_state import PilotRecord, TaskRecord

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

        self._last_submit_ts      : float = 0.0
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

        if self._router_preference not in ('least_loaded', 'youngest'):
            raise ValueError(
                f"ConservativePolicy: unknown router_preference "
                f"{self._router_preference!r}; expected 'least_loaded' "
                f"or 'youngest'")

        # Failure-backoff guard: pause submissions when N consecutive pilots
        # fail without ever reaching ACTIVE.  Reset on any pilot into ACTIVE.
        self._consecutive_failures: int   = 0
        self._backoff_until       : float = 0.0
        self._backoff_logged      : bool  = False

    # -- signals ---------------------------------------------------------

    def on_pilot_state(self, pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        '''Track consecutive pilot failures and trip the backoff guard.

        Submission-side failure: pilot went PENDING/STARTING → FAILED without
        ever reaching ACTIVE and without running any tasks.  After
        ``max_consecutive_failures`` such failures we pause submissions for
        ``failure_backoff_sec``.  Any pilot reaching ACTIVE resets the counter.
        '''
        if new_state == PILOT_ACTIVE:
            self._consecutive_failures = 0
            self._backoff_until        = 0.0
            self._backoff_logged       = False
            return

        if (new_state == PILOT_FAILED
                and old_state in (PILOT_PENDING, PILOT_STARTING)
                and pilot.started_tasks == 0):
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._max_consecutive_failures:
                self._backoff_until  = self._now() + self._failure_backoff_sec
                self._backoff_logged = False
                log.warning(
                    "conservative[%s]: %d consecutive pilot failures; "
                    "pausing submissions for %.0fs",
                    self._pool.name, self._consecutive_failures,
                    self._failure_backoff_sec)

    # -- decisions -------------------------------------------------------

    def on_tick(self, pool_state,
                submit_pilot: Callable[[str | None], str]) -> None:
        '''Maybe submit one pilot if backlog exceeds capacity.

        Conservative: at most one submission per tick, bounded in-flight,
        respecting ``min_dwell_sec``.
        '''
        pending = pool_state.pending_queue()
        pilots  = pool_state.live_pilots()
        if not pending:
            return

        # Backoff guard: pause submissions while in failure backoff window.
        now_ts = self._now()
        if now_ts < self._backoff_until:
            if not self._backoff_logged:
                log.info("conservative[%s]: backoff active for %.0fs more",
                         self._pool.name, self._backoff_until - now_ts)
                self._backoff_logged = True
            return

        # Free capacity across active pilots
        free_capacity = sum(p.free_capacity() for p in pilots
                            if p.state == PILOT_ACTIVE)
        if len(pending) <= free_capacity:
            return  # existing capacity will absorb the backlog

        # How many pilots already submitted but not yet active?
        in_flight_subs = sum(1 for p in pilots if p.state in _PRE_ACTIVE)
        if in_flight_subs >= self._max_in_flight_subs:
            return

        # Live fleet cap vs pool.max_pilots
        live_count = sum(1 for p in pilots if p.state in PILOT_LIVE_STATES)
        if live_count >= self._pool.max_pilots:
            return

        # Dwell: don't resubmit too fast
        if now_ts - self._last_submit_ts < self._min_dwell_sec:
            return

        try:
            pid = submit_pilot(None)  # None → pool.default_size
            self._last_submit_ts = now_ts
            log.info("conservative[%s]: submitted pilot %s "
                     "(pending=%d, free=%d, in_flight_subs=%d)",
                     self._pool.name, pid, len(pending),
                     free_capacity, in_flight_subs + 1)
        except Exception as e:
            log.warning("conservative[%s]: submit_pilot failed: %s",
                        self._pool.name, e)

    def pick_dispatch(self, pool_state) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        '''Return the highest-priority queued task + best-available pilot.

        Called repeatedly by the dispatcher until it returns ``None``.
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

        if self._router_preference == 'youngest':
            # Most remaining walltime = largest walltime_deadline
            active.sort(key=lambda p: p.walltime_deadline, reverse=True)
        else:  # 'least_loaded'
            active.sort(key=lambda p: (p.in_flight, -p.walltime_deadline))

        return pending[0], active[0]
