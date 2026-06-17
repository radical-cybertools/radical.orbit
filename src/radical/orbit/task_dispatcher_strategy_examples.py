'''
Task dispatcher — example non-default strategies.

Ships one reference strategy beyond the v1 default
(:class:`~radical.orbit.task_dispatcher_strategy_conservative.ConservativeStrategy`)
to demonstrate the research-facing ABC.  Registered as an entry point
``aggressive_scale_to_backlog`` so it can be selected in ``pools.json``
without a dotted module path.

Strategies in this file are exercised by
``tests/unittests/test_task_dispatcher_strategy_conformance.py`` — a
parametric benchmark harness that any new strategy can opt into.

See also: ``docs/task_dispatcher_strategy.md``.
'''

from __future__ import annotations

from typing import TYPE_CHECKING

from .task_dispatcher_config   import PoolConfig
from .task_dispatcher_state    import (
    PILOT_ACTIVE, PILOT_PENDING, PILOT_STARTING,
    PILOT_LIVE_STATES, TASK_QUEUED,
)
from .task_dispatcher_strategy import DispatchStrategy, StrategyContext

if TYPE_CHECKING:
    from .task_dispatcher_state import PilotRecord, TaskRecord


_PRE_ACTIVE = {PILOT_PENDING, PILOT_STARTING}


# ---------------------------------------------------------------------------
# Aggressive: scale on every backlog signal; drain idle pilots fast.
# ---------------------------------------------------------------------------

class AggressiveScaleToBacklogStrategy(DispatchStrategy):
    '''Favor low queue latency over efficient utilization.

    Policy contrasts with :class:`ConservativeStrategy` on three axes:

    1. **Arrival-triggered scaling**.  Every ``on_task_arrived`` may
       submit a pilot if the effective backlog (pending plus the
       arrival count in the recent window) exceeds active capacity.
    2. **No dwell gate**.  Up to ``max_in_flight_submissions`` pilots
       may be submitted concurrently without waiting.
    3. **Idle termination**.  ``should_terminate_pilot`` returns True
       when a pilot has been idle (zero in-flight, empty pool queue)
       for ``idle_timeout_sec``.

    Routing: youngest active pilot first (most remaining walltime).

    Knobs (``strategy_config``):
        max_in_flight_submissions : int   = 4
        idle_timeout_sec          : float = 90.0
        arrivals_window_sec       : float = 30.0
    '''

    def __init__(self, pool: PoolConfig, cfg: dict) -> None:
        super().__init__(pool, cfg)
        self._max_in_flight_subs = int(cfg.get('max_in_flight_submissions', 4))
        self._idle_timeout_sec   = float(cfg.get('idle_timeout_sec', 90.0))
        self._arrivals_window    = float(cfg.get('arrivals_window_sec', 30.0))

    # -- signals ---------------------------------------------------------

    def on_task_arrived(self, ctx: StrategyContext,
                        task: 'TaskRecord') -> None:
        self._maybe_scale_up(ctx)

    def on_pilot_state(self, ctx: StrategyContext,
                       pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        # A pilot failing early may justify another submission
        if new_state in ('FAILED', 'DONE'):
            self._maybe_scale_up(ctx)

    def on_task_finished(self, ctx: StrategyContext,
                         task: 'TaskRecord',
                         pilot: 'PilotRecord') -> None:
        return None

    def on_tick(self, ctx: StrategyContext) -> None:
        # Belt-and-braces: in case arrival events were dropped.
        self._maybe_scale_up(ctx)

    # -- dispatch --------------------------------------------------------

    def pick_dispatch(self, ctx: StrategyContext) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        pending = [t for t in ctx.pending_queue()
                   if t.state == TASK_QUEUED]
        if not pending:
            return None
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))

        active = [p for p in ctx.pilots()
                  if p.state == PILOT_ACTIVE and p.free_capacity() > 0]
        if not active:
            return None

        # Youngest = most remaining walltime
        active.sort(key=lambda p: p.walltime_deadline, reverse=True)
        return pending[0], active[0]

    # -- termination -----------------------------------------------------

    def should_terminate_pilot(self, ctx: StrategyContext,
                               pilot: 'PilotRecord') -> bool:
        if pilot.state != PILOT_ACTIVE:
            return False
        if pilot.in_flight > 0:
            return False
        # Don't terminate if tasks are pending and this pilot could take one
        pending = [t for t in ctx.pending_queue()
                   if t.state == TASK_QUEUED]
        if pending and pilot.free_capacity() > 0:
            return False
        if pilot.active_at is None:
            return False
        idle_for = ctx.now() - pilot.active_at
        # Only drain if the pilot has been active at least as long as it
        # has been idle — a pilot that came up 5 s ago with nothing to
        # do is still earning its keep because arrivals may be
        # imminent.
        return idle_for >= self._idle_timeout_sec

    # -- internals -------------------------------------------------------

    def _maybe_scale_up(self, ctx: StrategyContext) -> None:
        pending = ctx.pending_queue()
        pilots  = ctx.pilots()
        if not pending:
            return

        free_capacity = sum(p.free_capacity() for p in pilots
                            if p.state == PILOT_ACTIVE)

        # Projection: how many tasks will likely arrive while a new
        # pilot is booting?  Use current arrival rate over the last
        # window as a rough forecast.
        arrivals = ctx.arrivals_window(self._arrivals_window)
        arrival_rate = (len(arrivals) / self._arrivals_window
                        if self._arrivals_window > 0 else 0.0)
        lag_hist = ctx.pilot_lag_history()
        avg_lag  = (sum(lag_hist) / len(lag_hist)) if lag_hist else 60.0
        projected_arrivals = arrival_rate * avg_lag

        effective_backlog = len(pending) + projected_arrivals
        if effective_backlog <= free_capacity:
            return

        in_flight_subs = sum(1 for p in pilots if p.state in _PRE_ACTIVE)
        if in_flight_subs >= self._max_in_flight_subs:
            return

        live_count = sum(1 for p in pilots if p.state in PILOT_LIVE_STATES)
        if live_count >= self.pool.max_pilots:
            return

        try:
            pid = ctx.submit_pilot(None)
            ctx.logger.info(
                "aggressive[%s]: submitted pilot %s "
                "(pending=%d, projected=%.1f, free=%d, in_flight_subs=%d)",
                self.pool.name, pid, len(pending),
                projected_arrivals, free_capacity, in_flight_subs + 1)
        except Exception as e:
            ctx.logger.warning(
                "aggressive[%s]: submit_pilot failed: %s",
                self.pool.name, e)
