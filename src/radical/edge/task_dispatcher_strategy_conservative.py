'''
Task dispatcher — default conservative strategy.

Policy
------
- Scale-up only on ``on_tick``, and only when
  ``pending > sum(free capacity of active pilots)``.
- Bound in-flight pilot submissions
  (``max_in_flight_submissions``, default 2).
- Respect ``min_dwell_sec`` between successive submissions.
- ``pick_dispatch``: highest-priority pending task first (ties broken by
  arrival order); routed to an active pilot with free capacity.
- Among candidate pilots, prefer fewest ``in_flight`` (``'least_loaded'``)
  or youngest (``'youngest'``), configurable via
  ``strategy_config.router_preference``.
- ``should_terminate_pilot``: never.  Pilots expire at walltime.

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

from typing import TYPE_CHECKING

from .task_dispatcher_config   import PoolConfig
from .task_dispatcher_state    import (
    PILOT_ACTIVE, PILOT_FAILED, PILOT_PENDING, PILOT_STARTING,
    PILOT_LIVE_STATES, TASK_QUEUED,
)
from .task_dispatcher_strategy import DispatchStrategy, StrategyContext

if TYPE_CHECKING:
    from .task_dispatcher_state import PilotRecord, TaskRecord


# Pilot states that count as "submitted but not yet active"
_PRE_ACTIVE = {PILOT_PENDING, PILOT_STARTING}


class ConservativeStrategy(DispatchStrategy):
    '''Default v1 strategy.  Conservative scale-up, priority dispatch.'''

    def __init__(self, pool: PoolConfig, cfg: dict) -> None:
        super().__init__(pool, cfg)
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
                f"ConservativeStrategy: unknown router_preference "
                f"{self._router_preference!r}; expected 'least_loaded' "
                f"or 'youngest'")

        # Failure-backoff guard: pause submissions when N consecutive
        # pilots fail without ever reaching ACTIVE.  Reset on any pilot
        # transition into ACTIVE.
        self._consecutive_failures: int   = 0
        self._backoff_until       : float = 0.0
        self._backoff_logged      : bool  = False

    # -- signals ---------------------------------------------------------

    def on_task_arrived(self, ctx: StrategyContext,
                        task: 'TaskRecord') -> None:
        '''No eager scale-up.  ``pick_dispatch`` routes if capacity exists.'''
        return None

    def on_pilot_state(self, ctx: StrategyContext,
                       pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        '''Track consecutive pilot failures and trip the backoff guard.

        Submission-side failure: pilot went PENDING/STARTING → FAILED
        without ever reaching ACTIVE and without running any tasks.
        After ``max_consecutive_failures`` such failures we pause
        submissions for ``failure_backoff_sec``.  Any pilot reaching
        ACTIVE resets the counter.
        '''
        if new_state == PILOT_ACTIVE:
            self._consecutive_failures = 0
            self._backoff_until        = 0.0
            self._backoff_logged       = False
            return None

        if (new_state == PILOT_FAILED
                and old_state in (PILOT_PENDING, PILOT_STARTING)
                and pilot.started_tasks == 0):
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._max_consecutive_failures:
                self._backoff_until  = ctx.now() + self._failure_backoff_sec
                self._backoff_logged = False
                ctx.logger.warning(
                    "conservative[%s]: %d consecutive pilot failures; "
                    "pausing submissions for %.0fs",
                    self.pool.name, self._consecutive_failures,
                    self._failure_backoff_sec)
        return None

    def on_task_finished(self, ctx: StrategyContext,
                         task: 'TaskRecord',
                         pilot: 'PilotRecord') -> None:
        '''No direct action; freed capacity available for next
        ``pick_dispatch``.'''
        return None

    def on_tick(self, ctx: StrategyContext) -> None:
        '''Maybe submit one pilot if backlog exceeds capacity.

        Conservative: at most one submission per tick, bounded
        in-flight, respecting ``min_dwell_sec``.
        '''
        pending      = ctx.pending_queue()
        pilots       = ctx.pilots()
        if not pending:
            return

        # Backoff guard: pause submissions while in failure backoff window.
        now_ts = ctx.now()
        if now_ts < self._backoff_until:
            if not self._backoff_logged:
                ctx.logger.info(
                    "conservative[%s]: backoff active for %.0fs more",
                    self.pool.name, self._backoff_until - now_ts)
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
        if live_count >= self.pool.max_pilots:
            return

        # Dwell: don't resubmit too fast
        now = ctx.now()
        if now - self._last_submit_ts < self._min_dwell_sec:
            return

        try:
            pid = ctx.submit_pilot(None)  # None → pool.default_size
            self._last_submit_ts = now
            ctx.logger.info(
                "conservative[%s]: submitted pilot %s "
                "(pending=%d, free=%d, in_flight_subs=%d)",
                self.pool.name, pid, len(pending),
                free_capacity, in_flight_subs + 1)
        except Exception as e:
            ctx.logger.warning(
                "conservative[%s]: submit_pilot failed: %s",
                self.pool.name, e)

    # -- decisions -------------------------------------------------------

    def pick_dispatch(self, ctx: StrategyContext) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        '''Highest-priority queued task + best-available active pilot.'''
        pending = [t for t in ctx.pending_queue()
                   if t.state == TASK_QUEUED]
        if not pending:
            return None

        # Pending queue is already priority-ordered by the dispatcher
        # (see design doc §5.1 and test_task_dispatcher_state), but we
        # re-sort defensively.  Tie-break: earlier arrival first.
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))

        active = [p for p in ctx.pilots()
                  if p.state == PILOT_ACTIVE and p.free_capacity() > 0]
        if not active:
            return None

        if self._router_preference == 'youngest':
            # Most remaining walltime = largest walltime_deadline
            active.sort(key=lambda p: p.walltime_deadline, reverse=True)
        else:  # 'least_loaded'
            active.sort(key=lambda p: (p.in_flight, -p.walltime_deadline))

        return pending[0], active[0]

    # -- termination -----------------------------------------------------

    # should_terminate_pilot: inherit default (always False)
