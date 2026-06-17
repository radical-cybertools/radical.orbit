'''
Task dispatcher — pluggable strategy ABC and context.

A strategy owns three concerns within its pool:

1. Pilot submission — what pilot to submit, when, how big.
2. Task dispatch   — which task off the queue runs on which pilot.
3. Pilot termination — when to cancel a pilot (beyond walltime expiry).

Strategies never touch psij, rhapsody, or the bridge directly.  All
side-effecting actions flow through :class:`StrategyContext`, which the
dispatcher supplies.  This keeps the research surface independent of
plumbing changes.

Strategy loading
----------------
The pool's ``strategy`` field is resolved by :func:`load_strategy` via
one of two mechanisms:

- Entry-point name — matches a registered entry point in the
  ``radical.edge.task_dispatcher.strategies`` group.
- Dotted ``"module:ClassName"`` — imported directly.
'''

from __future__ import annotations

import importlib
import logging

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

from .task_dispatcher_config import PoolConfig

if TYPE_CHECKING:
    from .task_dispatcher_state import PilotRecord, TaskRecord

log = logging.getLogger('radical.edge')


# ---------------------------------------------------------------------------
# StrategyContext — read-only state + narrow action hooks
# ---------------------------------------------------------------------------

class StrategyContext:
    '''Read-only state accessors plus narrow action hooks for a strategy.

    The dispatcher constructs one context per strategy invocation; the
    context is not meant to be held across calls.  Accessor methods
    return snapshots; action methods schedule dispatcher-side work (they
    do not block on the underlying batch system or bridge).

    The strategy must not import psij, rhapsody, httpx, or BridgeClient.
    All side effects are expressed through this object.
    '''

    def __init__(
        self,
        pool                : PoolConfig,
        *,
        now_fn              : Callable[[], float],
        pending_queue_fn    : Callable[[], list['TaskRecord']],
        pilots_fn           : Callable[[], list['PilotRecord']],
        arrivals_window_fn  : Callable[[float], list[float]],
        pilot_lag_history_fn: Callable[[], list[float]],
        submit_pilot_fn     : Callable[[str | None], str],
        cancel_pilot_fn     : Callable[[str], None],
        drain_pilot_fn      : Callable[[str], None],
        logger              : logging.Logger | None = None,
    ) -> None:
        self._pool                 = pool
        self._now_fn               = now_fn
        self._pending_queue_fn     = pending_queue_fn
        self._pilots_fn            = pilots_fn
        self._arrivals_window_fn   = arrivals_window_fn
        self._pilot_lag_history_fn = pilot_lag_history_fn
        self._submit_pilot_fn      = submit_pilot_fn
        self._cancel_pilot_fn      = cancel_pilot_fn
        self._drain_pilot_fn       = drain_pilot_fn
        self._logger               = logger or log

    # -- accessors --------------------------------------------------------

    @property
    def pool(self) -> PoolConfig:
        '''The :class:`PoolConfig` this strategy operates within.'''
        return self._pool

    @property
    def logger(self) -> logging.Logger:
        '''Logger scoped to the dispatcher plugin.'''
        return self._logger

    def now(self) -> float:
        '''Wall-clock timestamp (seconds since epoch).

        Wall-clock (not monotonic) so it is comparable with the
        persisted timestamps on :class:`PilotRecord`/:class:`TaskRecord`
        (e.g. ``pilot.active_at``), which must survive dispatcher
        restarts.
        '''
        return self._now_fn()

    def pending_queue(self) -> list['TaskRecord']:
        '''Snapshot of pending tasks for this pool, priority-ordered.'''
        return self._pending_queue_fn()

    def pilots(self) -> list['PilotRecord']:
        '''Snapshot of non-terminal pilots in this pool's fleet.'''
        return self._pilots_fn()

    def arrivals_window(self, seconds: float) -> list[float]:
        '''Arrival timestamps over the last *seconds* for this pool.'''
        return self._arrivals_window_fn(seconds)

    def pilot_lag_history(self) -> list[float]:
        '''Observed PENDING→ACTIVE durations for recent pilots (seconds).'''
        return self._pilot_lag_history_fn()

    # -- actions ----------------------------------------------------------

    def submit_pilot(self, size_key: str | None = None) -> str:
        '''Schedule submission of a new pilot of the given size.

        Args:
            size_key: Key into ``pool.pilot_sizes``.  ``None`` selects
                      ``pool.default_size``.

        Returns:
            The dispatcher-local ``pilot_id`` of the newly recorded
            pilot.  The actual psij submission happens asynchronously.
        '''
        return self._submit_pilot_fn(size_key)

    def cancel_pilot(self, pid: str) -> None:
        '''Terminate a pilot (cancel its batch job).  Best effort.'''
        self._cancel_pilot_fn(pid)

    def drain_pilot(self, pid: str) -> None:
        '''Stop routing new tasks to *pid*.  Running tasks finish.'''
        self._drain_pilot_fn(pid)


# ---------------------------------------------------------------------------
# DispatchStrategy — pluggable ABC
# ---------------------------------------------------------------------------

class DispatchStrategy(ABC):
    '''Pluggable autoscaling + task-routing + termination policy.

    One instance per pool.  The dispatcher drives the event callbacks
    and decision methods; the strategy uses the provided
    :class:`StrategyContext` for any side effects.

    Contract
    --------
    - Callbacks (``on_*``) are invoked in the dispatcher's event loop.
      They may schedule work via ``ctx`` but must not block.
    - ``pick_dispatch`` is called repeatedly after each dispatch-relevant
      event.  It returns at most one ``(task, pilot)`` pair per call;
      the dispatcher invokes it in a loop until it returns ``None``.
    - ``should_terminate_pilot`` is consulted periodically.  Default
      implementation returns ``False``.

    FIXME(per-task-backend):
        A future hook
            def pick_backend(self, ctx, task, pilot) -> str | None
        would let a strategy override the rhapsody backend on a per-task
        basis rather than inheriting ``pilot.rhapsody_backend`` (which is
        fixed at pilot-submit time from ``PilotSize.rhapsody_backend``).
        This is not part of the v1 ABC.  Implementation site marked with
        the same tag in:
          plugin_task_dispatcher.py::PluginTaskDispatcher._assign
        (search ``FIXME(per-task-backend)``).
    '''

    def __init__(self, pool: PoolConfig, cfg: dict[str, Any]) -> None:
        self._pool = pool
        self._cfg  = dict(cfg)

    @property
    def pool(self) -> PoolConfig:
        return self._pool

    @property
    def cfg(self) -> dict[str, Any]:
        '''Strategy-specific config dict from ``PoolConfig.strategy_config``.'''
        return self._cfg

    # -- signals ----------------------------------------------------------

    @abstractmethod
    def on_task_arrived(self, ctx: StrategyContext,
                        task: 'TaskRecord') -> None:
        '''A new task entered the pending queue.'''
        ...

    @abstractmethod
    def on_pilot_state(self, ctx: StrategyContext,
                       pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        '''A pilot transitioned (PENDING → STARTING → ACTIVE → DONE/FAILED).'''
        ...

    @abstractmethod
    def on_task_finished(self, ctx: StrategyContext,
                         task: 'TaskRecord',
                         pilot: 'PilotRecord') -> None:
        '''A task reached terminal state.  Pilot has freed capacity.'''
        ...

    def on_tick(self, ctx: StrategyContext) -> None:
        '''Periodic wake-up (~5 s).  Default no-op.

        Override when a strategy needs time-triggered behaviour (rate
        limiting, deadline checks, etc.) that cannot be driven by event
        callbacks alone.
        '''
        return None

    # -- decisions --------------------------------------------------------

    @abstractmethod
    def pick_dispatch(self, ctx: StrategyContext) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        '''Pick a (task, pilot) pair to dispatch now, or ``None`` to hold.

        Called repeatedly by the dispatcher until it returns ``None``.
        A strategy may drain multiple tasks per event by returning one
        pair per call.
        '''
        ...

    def should_terminate_pilot(self, ctx: StrategyContext,
                               pilot: 'PilotRecord') -> bool:
        '''Should this pilot be cancelled now?  Default: never.

        Override to implement drain-on-idle, post-failure abandonment,
        dynamic right-sizing, etc.  A pilot that returns ``True`` here
        is cancelled via ``ctx.cancel_pilot``.
        '''
        return False


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

ENTRY_POINT_GROUP = 'radical.edge.task_dispatcher.strategies'


class StrategyNotFound(ValueError):
    '''Raised when a strategy name cannot be resolved.'''
    pass


def _builtin_strategies() -> dict[str, str]:
    '''Names → ``"module:ClassName"`` for strategies shipped in-tree.

    Resolved lazily by :func:`load_strategy` so circular imports are
    avoided.  This mirrors the entry-point mechanism but covers the
    default strategies without requiring the package to be installed
    (which is the case in editable checkouts and test runs).
    '''
    return {
        'conservative':
            'radical.edge.task_dispatcher_strategy_conservative'
            ':ConservativeStrategy',
        'aggressive_scale_to_backlog':
            'radical.edge.task_dispatcher_strategy_examples'
            ':AggressiveScaleToBacklogStrategy',
    }


def load_strategy(spec: str,
                  pool: PoolConfig,
                  cfg: dict[str, Any]) -> DispatchStrategy:
    '''Resolve *spec* to a :class:`DispatchStrategy` instance.

    Resolution order:

    1. If *spec* contains ``:``, treat as ``"module.path:ClassName"`` and
       import directly.
    2. Else, consult the built-in strategy registry (see
       :func:`_builtin_strategies`).
    3. Else, look up in the
       ``radical.edge.task_dispatcher.strategies`` entry-point group
       (for strategies shipped by third-party packages).

    Raises :class:`StrategyNotFound` when nothing resolves.
    '''
    if ':' in spec:
        return _load_dotted(spec, pool, cfg)

    # 2. Built-ins
    builtins = _builtin_strategies()
    if spec in builtins:
        return _load_dotted(builtins[spec], pool, cfg)

    # 3. Entry points
    try:
        from importlib.metadata import entry_points
        eps = entry_points(group=ENTRY_POINT_GROUP)
    except Exception as e:
        raise StrategyNotFound(
            f"failed to query entry points for '{ENTRY_POINT_GROUP}': {e}"
        ) from e

    matching = [ep for ep in eps if ep.name == spec]
    if not matching:
        available = sorted(set(builtins) | {ep.name for ep in eps})
        raise StrategyNotFound(
            f"unknown strategy '{spec}'; available: {available or '(none)'}")

    try:
        cls = matching[0].load()
    except Exception as e:
        raise StrategyNotFound(
            f"failed to load strategy '{spec}' from entry point: {e}"
        ) from e
    return _instantiate_strategy(cls, spec, pool, cfg)


def _load_dotted(spec: str, pool: PoolConfig,
                 cfg: dict[str, Any]) -> DispatchStrategy:
    '''Import a ``module.path:ClassName`` spec and instantiate it.'''
    mod_path, _, cls_name = spec.partition(':')
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise StrategyNotFound(
            f"cannot import module '{mod_path}' for strategy '{spec}': {e}"
        ) from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise StrategyNotFound(
            f"module '{mod_path}' has no attribute '{cls_name}'")
    return _instantiate_strategy(cls, spec, pool, cfg)


def _instantiate_strategy(cls: Any, spec: str,
                          pool: PoolConfig,
                          cfg: dict[str, Any]) -> DispatchStrategy:
    '''Sanity-check *cls* and instantiate it.'''
    if not isinstance(cls, type) or not issubclass(cls, DispatchStrategy):
        raise StrategyNotFound(
            f"strategy '{spec}' resolved to {cls!r}, not a "
            f"DispatchStrategy subclass")
    return cls(pool, cfg)
