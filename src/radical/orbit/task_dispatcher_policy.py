'''
Task dispatcher — dispatch-policy base class and manual registry.

The dispatcher instantiates one policy per pool; which one is picked comes
from ``PoolConfig.strategy``, resolved through the small, deliberately
manual registry in this module — no entry points, no discovery, no plugin
loader.  To add a policy: write a class implementing the
:class:`DispatchPolicy` surface, then either add it to :data:`_BUILTINS`
(in-tree policies) or call :func:`register_policy` at runtime (tests,
embedders).  Runtime registrations take precedence over a same-named
builtin.

Registration timing matters for restart replay: the dispatcher replays
persisted pools during plugin construction, and a pool whose ``strategy``
is not registered *at that moment* is skipped with a warning — its durable
state stays on disk, but its pilots and tasks are not resumed.  Embedders
providing custom policies must therefore call :func:`register_policy`
before the broker constructs the task-dispatcher plugin.
'''

from __future__ import annotations

import importlib
import time

from typing import TYPE_CHECKING, Callable

from .task_dispatcher_config import PoolConfig, PoolConfigError

if TYPE_CHECKING:
    from .task_dispatcher_state import PilotRecord, TaskRecord

# Builtin policies: name → lazy ``(module, attr)`` spec, resolved on first
# use.  Lazy so this module never imports a policy implementation at import
# time — implementations import :class:`DispatchPolicy` from here.
_BUILTINS: dict[str, tuple[str, str]] = {
    'conservative': ('.task_dispatcher_strategy_conservative',
                     'ConservativePolicy'),
}

# Runtime registrations (tests, embedders); overrides same-named builtins.
_REGISTRY: dict[str, type] = {}


class DispatchPolicy:
    '''Base class for per-pool dispatch policies.

    Contract — what a policy may rely on, and what it must not do:

    - One instance per pool, constructed as
      ``PolicyClass(pool_config, strategy_config)``.
    - Every method is invoked on the dispatcher plugin's event loop, so no
      in-policy locking is needed.
    - ``pool_state.pending_queue()`` returns QUEUED tasks already
      priority-ordered (highest priority first, FIFO within a priority);
      ``pool_state.live_pilots()`` returns non-terminal pilots.
    - :meth:`pick_dispatch` is called in a loop until it returns ``None``,
      bounded by the pending-queue length; the dispatcher performs the
      actual assignment and state mutation — policies only *choose*.
    - Policies must not mutate pool state (tasks, pilots, persistence);
      scale-up is requested only through the ``submit_pilot`` callable
      handed to :meth:`on_tick`.

    The defaults are deliberately inert — no scale-up, no dispatch, no
    reaction to pilot transitions.  A subclass overrides what it needs.
    '''

    def __init__(self, pool: PoolConfig, cfg: dict,
                 now: Callable[[], float] = time.time) -> None:
        self._pool = pool
        self._cfg  = cfg
        self._now  = now

    def on_pilot_state(self, pilot: 'PilotRecord',
                       old_state: str, new_state: str) -> None:
        '''Observe one pilot state transition.  Default: ignore it.'''
        return None

    def on_tick(self, pool_state,
                submit_pilot: Callable[[str | None], str]) -> None:
        '''Housekeeping tick: maybe request pilots.  Default: never scale.'''
        return None

    def pick_dispatch(self, pool_state) -> \
            tuple['TaskRecord', 'PilotRecord'] | None:
        '''Return the next ``(task, pilot)`` pair.  Default: dispatch nothing.'''
        return None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def register_policy(name: str, cls: type) -> None:
    '''Register policy class *cls* under *name*.

    Overrides a same-named builtin.  Call before the task-dispatcher plugin
    is constructed if pools using this policy must survive a broker restart
    (see the module docstring on replay timing).
    '''
    if not name or not isinstance(name, str):
        raise ValueError('policy name must be a non-empty string')
    if not isinstance(cls, type):
        raise ValueError(f'policy {name!r}: expected a class, got {cls!r}')
    _REGISTRY[name] = cls


def known_policies() -> list[str]:
    '''Return the sorted, de-duplicated names of all registered policies.'''
    return sorted(set(_BUILTINS) | set(_REGISTRY))


def _resolve(name: str) -> type:
    '''Resolve *name* to a policy class (runtime registry wins).'''
    cls = _REGISTRY.get(name)
    if cls is not None:
        return cls
    spec = _BUILTINS.get(name)
    if spec is not None:
        module, attr = spec
        return getattr(importlib.import_module(module, __package__), attr)
    raise PoolConfigError(
        f"unknown dispatch strategy {name!r} "
        f"(known: {', '.join(known_policies())})")


def make_policy(pool_config: PoolConfig) -> DispatchPolicy:
    '''Instantiate the policy named by ``pool_config.strategy``.

    Raises :class:`~radical.orbit.task_dispatcher_config.PoolConfigError`
    when the name is not registered.
    '''
    cls = _resolve(pool_config.strategy)
    return cls(pool_config, pool_config.strategy_config)
