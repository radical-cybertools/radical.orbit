'''
Task dispatcher — pool configuration schema and loader.

A ``PoolConfig`` is a durable resource scope that owns a fleet of
pilots and a single dispatch strategy.  Each pool carries a menu of
named ``PilotSize`` entries; the strategy picks one by key when it
decides to submit a new pilot.  See ``plans/task_dispatcher_design.md``
and ``memory/project_bridge_dispatcher.md`` for the surrounding design.

The ``rhapsody_backend`` field on ``PilotSize`` is **required** — there
is deliberately no pool-level default and no cascade, to keep the
pilot-to-backend mapping explicit.

The ``endpoint_name`` field on ``PoolConfig`` is **optional**: when omitted,
the dispatcher selects a connected compute endpoint automatically (policy:
first by lexical name).  Sessions can declare arbitrary pool names; the
single reserved name :data:`DEFAULT_POOL_NAME` (``"default"``) is
materialised automatically by the dispatcher when a session registers
without declaring any pools.
'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import json


# Reserved pool name auto-materialised by the dispatcher when a
# session registers without declaring any pools.  See
# memory/project_bridge_dispatcher.md (Phase 4) for the lifecycle.
DEFAULT_POOL_NAME: str = 'default'


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class PilotSize:
    '''One named pilot shape in a pool's ``pilot_sizes`` menu.

    The dispatch strategy picks a size by its key name when submitting a
    pilot; the values here end up in the psij ``JobSpec`` and as env vars
    passed to the pilot wrapper (see design doc §6.1).
    '''
    nodes           : int
    cpus_per_node   : int
    rhapsody_backend: str                # required; no cascade
    gpus_per_node   : int = 0
    walltime_sec    : int = 3600


@dataclass
class PoolConfig:
    '''One pool — the unit of resource budget, policy, and task grouping.

    Pool identity is the tuple ``(name, endpoint_name)``.  When ``endpoint_name``
    is ``None`` at parse time the dispatcher resolves it at pool
    materialisation by picking a connected compute endpoint (lexically
    first); this lets clients declare resource intent without binding
    to a specific cluster up-front.
    '''
    name            : str                # unique within (name, endpoint_name) tuple
    queue           : str                # batch queue name
    account         : str | None         # charge account / project
    pilot_sizes     : dict[str, PilotSize]
    default_size    : str                # key into pilot_sizes
    endpoint_name       : str | None = None  # which endpoint runs psij; None → auto
    min_pilots      : int  = 0
    max_pilots      : int  = 4
    scratch_base    : str | None = None  # None → default scratch tree
    strategy        : str  = 'conservative'  # entry-point name or 'module:ClassName'
    strategy_config : dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class PoolConfigError(ValueError):
    '''Raised when pools.json is malformed or violates schema invariants.'''
    pass


def load_pools(path: str | Path) -> dict[str, PoolConfig]:
    '''Load and validate a ``pools.json`` file.

    Returns a mapping ``pool_name → PoolConfig``.  Raises
    :class:`PoolConfigError` with an actionable message on any schema
    violation.

    The file format is a JSON object whose top-level key is ``"pools"``
    holding an array of pool records.  Example::

        {
          "pools": [
            {
              "name": "cpu_small",
              "queue": "batch",
              "account": "proj123",
              "default_size": "s",
              "pilot_sizes": {
                "s": {"nodes": 1, "cpus_per_node": 64,
                      "rhapsody_backend": "concurrent"}
              },
              "min_pilots": 0,
              "max_pilots": 4,
              "strategy": "conservative"
            }
          ]
        }
    '''
    p = Path(path)
    if not p.is_file():
        raise PoolConfigError(f"pool config file not found: {p}")

    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise PoolConfigError(f"pool config {p} is not valid JSON: {e}") from e

    return parse_pools(raw, source=str(p))


def parse_pools(raw: Any, source: str = '<dict>') -> dict[str, PoolConfig]:
    '''Validate and parse a pre-loaded dict into a ``{name: PoolConfig}``.

    Factored out of ``load_pools`` so tests can exercise validation
    without touching the filesystem.
    '''
    if not isinstance(raw, dict):
        raise PoolConfigError(
            f"{source}: top-level must be a JSON object, got {type(raw).__name__}")

    pools_list = raw.get('pools')
    if not isinstance(pools_list, list):
        raise PoolConfigError(
            f"{source}: missing or non-list 'pools' field")

    pools: dict[str, PoolConfig] = {}
    for i, entry in enumerate(pools_list):
        if not isinstance(entry, dict):
            raise PoolConfigError(
                f"{source}: pools[{i}] must be an object, got "
                f"{type(entry).__name__}")
        pool = _parse_pool(entry, source=f"{source}: pools[{i}]")
        if pool.name in pools:
            raise PoolConfigError(
                f"{source}: duplicate pool name '{pool.name}'")
        pools[pool.name] = pool

    if not pools:
        raise PoolConfigError(f"{source}: no pools defined")

    return pools


def _parse_pool(d: Any, source: str) -> PoolConfig:
    '''Build a single :class:`PoolConfig` from a dict, validating fields.'''
    required = ('name', 'queue', 'default_size', 'pilot_sizes')
    for key in required:
        if key not in d:
            raise PoolConfigError(f"{source}: missing required field '{key}'")

    name = d['name']
    if not isinstance(name, str) or not name:
        raise PoolConfigError(f"{source}: 'name' must be a non-empty string")

    queue = d['queue']
    if not isinstance(queue, str) or not queue:
        raise PoolConfigError(f"{source}: 'queue' must be a non-empty string")

    account = d.get('account')
    if account is not None and not isinstance(account, str):
        raise PoolConfigError(
            f"{source}: 'account' must be a string or null")

    endpoint_name = d.get('endpoint_name')
    if endpoint_name is not None:
        if not isinstance(endpoint_name, str) or not endpoint_name:
            raise PoolConfigError(
                f"{source}: 'endpoint_name' must be a non-empty string or null")

    # pilot_sizes: dict[str, PilotSize]
    sizes_raw = d['pilot_sizes']
    if not isinstance(sizes_raw, dict) or not sizes_raw:
        raise PoolConfigError(
            f"{source}: 'pilot_sizes' must be a non-empty object")

    pilot_sizes: dict[str, PilotSize] = {}
    for size_name, size_dict in sizes_raw.items():
        pilot_sizes[size_name] = _parse_pilot_size(
            size_dict, source=f"{source}: pilot_sizes[{size_name!r}]")

    default_size = d['default_size']
    if default_size not in pilot_sizes:
        raise PoolConfigError(
            f"{source}: 'default_size' {default_size!r} not found in "
            f"pilot_sizes (available: {sorted(pilot_sizes)})")

    min_pilots = _parse_int(d.get('min_pilots', 0), 'min_pilots', source, min_value=0)
    max_pilots = _parse_int(d.get('max_pilots', 4), 'max_pilots', source, min_value=1)
    if min_pilots > max_pilots:
        raise PoolConfigError(
            f"{source}: min_pilots ({min_pilots}) > max_pilots ({max_pilots})")

    scratch_base = d.get('scratch_base')
    if scratch_base is not None and not isinstance(scratch_base, str):
        raise PoolConfigError(
            f"{source}: 'scratch_base' must be a string or null")

    strategy = d.get('strategy', 'conservative')
    if not isinstance(strategy, str) or not strategy:
        raise PoolConfigError(
            f"{source}: 'strategy' must be a non-empty string")

    strategy_config = d.get('strategy_config', {})
    if not isinstance(strategy_config, dict):
        raise PoolConfigError(
            f"{source}: 'strategy_config' must be an object")

    return PoolConfig(
        name            = name,
        queue           = queue,
        account         = account,
        pilot_sizes     = pilot_sizes,
        default_size    = default_size,
        endpoint_name       = endpoint_name,
        min_pilots      = min_pilots,
        max_pilots      = max_pilots,
        scratch_base    = scratch_base,
        strategy        = strategy,
        strategy_config = strategy_config,
    )


def _parse_pilot_size(d: Any, source: str) -> PilotSize:
    '''Build a single :class:`PilotSize` from a dict, validating fields.'''
    if not isinstance(d, dict):
        raise PoolConfigError(
            f"{source}: must be an object, got {type(d).__name__}")

    required = ('nodes', 'cpus_per_node', 'rhapsody_backend')
    for key in required:
        if key not in d:
            raise PoolConfigError(f"{source}: missing required field '{key}'")

    backend = d['rhapsody_backend']
    if not isinstance(backend, str) or not backend:
        raise PoolConfigError(
            f"{source}: 'rhapsody_backend' must be a non-empty string")

    return PilotSize(
        nodes            = _parse_int(d['nodes'],           'nodes',           source, min_value=1),
        cpus_per_node    = _parse_int(d['cpus_per_node'],   'cpus_per_node',   source, min_value=1),
        gpus_per_node    = _parse_int(d.get('gpus_per_node', 0), 'gpus_per_node',    source, min_value=0),
        walltime_sec     = _parse_int(d.get('walltime_sec', 3600), 'walltime_sec',   source, min_value=1),
        rhapsody_backend = backend,
    )


def _parse_int(val: Any, name: str, source: str, *, min_value: int) -> int:
    '''Coerce *val* to int, enforcing a minimum, raising a clear error.'''
    if isinstance(val, bool) or not isinstance(val, int):
        raise PoolConfigError(
            f"{source}: {name!r} must be an integer, got {type(val).__name__}")
    if val < min_value:
        raise PoolConfigError(
            f"{source}: {name!r} must be >= {min_value}, got {val}")
    return val


# ---------------------------------------------------------------------------
# Built-in default pool
# ---------------------------------------------------------------------------

def default_pool_config(queue: str = 'default') -> PoolConfig:
    '''Return the built-in fallback pool config (name="default").

    Materialised by the dispatcher when a session registers without
    declaring any pools.  ``endpoint_name`` is ``None`` so the dispatcher
    auto-selects a connected compute endpoint at materialisation time.

    *queue* defaults to ``"default"`` because the schema's ``queue``
    field is required non-empty; the dispatcher will override this
    with an endpoint-appropriate value during materialisation (caller
    can also pass a known queue here).
    '''
    return PoolConfig(
        name            = DEFAULT_POOL_NAME,
        queue           = queue,
        account         = None,
        pilot_sizes     = {
            'node': PilotSize(
                nodes            = 1,
                cpus_per_node    = 1,
                rhapsody_backend = 'concurrent',
            )
        },
        default_size    = 'node',
        endpoint_name       = None,
        min_pilots      = 0,
        max_pilots      = 1,
        strategy        = 'conservative',
        strategy_config = {},
    )
