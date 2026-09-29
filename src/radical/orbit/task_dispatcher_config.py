'''
Task dispatcher — pool configuration schema and parser.

A ``PoolConfig`` is a durable resource scope that owns a fleet of pilots and
the conservative dispatch policy.  Each pool carries a menu of named
``PilotSize`` entries; the policy picks one by key when it decides to submit a
new pilot.

The ``rhapsody_backend`` field on ``PilotSize`` is **required** — there
is deliberately no pool-level default and no cascade, to keep the
pilot-to-backend mapping explicit.

The ``endpoint_name`` field on ``PoolConfig`` is **optional**: when omitted,
the dispatcher selects a connected compute endpoint automatically (policy:
first by lexical name).  Sessions can declare arbitrary pool names; the
single reserved name :data:`DEFAULT_POOL_NAME` (``"default"``) is
materialised automatically by the dispatcher when a session registers
without declaring any pools.

Pools arrive only through ``register_session``; there is no on-disk pool
manifest to load.

A pool is a **capability class**, not a site: it owns one or more
:class:`PoolMember` entries, each of which is one
``(endpoint, queue, account, pilot-size menu, attributes, budget)`` tuple.
A GPU pilot on Bridges and a GPU pilot on Perlmutter can therefore live in
the same pool.  A *legacy* (single-site) declaration — no ``members`` key —
keeps its nine scalar fields and gains exactly one **implicit** member
(:data:`IMPLICIT_MEMBER`) synthesised from them, so every runtime code path
only ever reads ``config.members``: one shape at runtime, two on the wire.
'''

import re

from dataclasses import dataclass, field, asdict
from typing import Any

from .task_dispatcher_match import satisfies


# Reserved pool name auto-materialised by the dispatcher when a session
# registers without declaring any pools.
DEFAULT_POOL_NAME: str = 'default'

# The member id a legacy (single-site) pool's synthesised member carries.
# It is the same ``''`` every legacy pilot record carries, so no lookup has
# to normalise between the two.  ``MEMBER_RE`` cannot match it, so it can
# never collide with a declared member, and it never appears in a child
# endpoint name (a legacy pool is never promoted to a class pool -- see
# plan 121 §4.1).
IMPLICIT_MEMBER: str = ''

# Charset for a declared member id: endpoint names are built from it.
MEMBER_RE = re.compile(r'^[a-z0-9][a-z0-9_.-]*$')

# How a member's pilots come into being (plan 122).  ``submit``: the
# dispatcher asks the member's endpoint to submit a batch job and waits for
# the child endpoint that job registers.  ``endpoint``: the member's
# endpoint already runs *inside* its allocation and **is** the pilot, so the
# dispatcher adopts it instead of submitting a second process.
PILOT_SUBMIT  : str = 'submit'
PILOT_ENDPOINT: str = 'endpoint'
PILOT_MODES         = (PILOT_SUBMIT, PILOT_ENDPOINT)

# Charset for ``pool_class``.  The empty string is legal and means
# "unclassified" (every legacy pool); a non-empty name that does not match
# is an error -- never silently lowercased or coerced.
POOL_CLASS_RE = re.compile(r'^[a-z0-9_.-]*$')

# The legacy scalar projection of a class pool with no member to project
# from: the parser's placeholders (``__post_init__`` overwrites them) and an
# emptied pool's values (replay only).  Deterministic, so it persists and
# replays to an identical config.  Copy ``pilot_sizes`` on use.
_EMPTY_PROJECTION: dict[str, Any] = {
    'queue'        : DEFAULT_POOL_NAME,
    'account'      : None,
    'endpoint_name': None,
    'pilot_sizes'  : {},
    'default_size' : '',
    'min_pilots'   : 0,
    'max_pilots'   : 4,
    'scratch_base' : None,
}

# ``<pool>_<member_id>_<pid>`` becomes a broker participant name; keep the
# operator-chosen part bounded (plan 121 §14 R6).
MAX_POOL_MEMBER_NAME_LEN: int = 64


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class PilotSize:
    '''One named pilot shape in a pool's ``pilot_sizes`` menu.

    The dispatch policy picks a size by its key name when submitting a
    pilot; the values here end up in the psij ``JobSpec`` and as env vars
    passed to the pilot wrapper.
    '''
    nodes           : int
    cpus_per_node   : int
    rhapsody_backend: str                # required; no cascade
    gpus_per_node   : int = 0
    walltime_sec    : int = 3600


@dataclass
class PoolMember:
    '''One resource inside a capability-class pool.

    A member is the unit a pilot is submitted *to*: it binds an endpoint, a
    batch queue, a charge account, its own pilot-size menu, its own pilot
    floor/ceiling, its own scratch tree, and — new — declared *attributes*
    (matched against a task's ``requirements``) and a *budget*
    (allocations are per site, so budgets are per member, not per pool).

    ``attributes`` are **declared, not self-reported**: a pilot snapshots
    its member's attributes at submit time and keeps them for its life, so
    re-declaring a member with different ``software`` does not retro-fit
    running pilots.  ``site: str`` and ``software: [str]`` are conventions,
    not schema.

    ``shared_fs`` says whether the broker host and this member see the same
    ``scratch_base``.  When false the dispatcher never touches
    ``scratch_base`` locally; task inputs travel to the pilot through its
    own ``staging`` plugin instead.

    ``pilot`` says where this member's pilots come from (plan 122).  A
    ``submit`` member gets a batch job through its endpoint's ``psij``
    plugin; an ``endpoint`` member's endpoint runs *inside* its allocation
    and **is** the pilot, so the dispatcher adopts it.  Such a member holds
    exactly one pilot — its endpoint — so ``min_pilots`` and ``max_pilots``
    are forced to 1 here rather than trusted from the declaration.

    ``end_time`` is the absolute epoch at which the member's allocation
    ends, when it is known.  It is *not* a walltime: a member is re-declared
    with the same ``walltime_sec`` on every re-attach, so only an absolute
    instant keeps a re-adopted pilot from being given a deadline past the
    allocation it runs in.
    '''
    member_id     : str                     # unique within the pool; MEMBER_RE
    endpoint_name : str                     # required — no auto-pick for members
    queue         : str                     # non-empty, not the 'default' sentinel
    account       : str | None
    pilot_sizes   : dict[str, PilotSize]
    default_size  : str                     # key into pilot_sizes
    min_pilots    : int  = 0
    max_pilots    : int  = 4
    scratch_base  : str | None = None       # path ON THE MEMBER'S HOST
    shared_fs     : bool = True             # broker host and member share it
    attributes    : dict[str, Any] = field(default_factory=dict)
    budget        : dict[str, float] = field(default_factory=dict)
    pilot         : str  = PILOT_SUBMIT     # 'submit' | 'endpoint'
    end_time      : float | None = None     # allocation end, absolute epoch

    def __post_init__(self) -> None:
        '''Force the pilot floor and ceiling of an adopted member to 1.

        The single place the rule lives, so it holds for a parsed
        declaration, a replayed one and a hand-built :class:`PoolMember`
        alike: an ``endpoint`` member has exactly one pilot (its endpoint).
        ``min_pilots = 0`` would never adopt it until a backlog appeared,
        and ``max_pilots > 1`` would let the strategy ask for a second
        adoption of the same endpoint.
        '''
        if self.pilot == PILOT_ENDPOINT:
            self.min_pilots = 1
            self.max_pilots = 1

    def reject_reason(self, req: dict | None) -> str | None:
        '''Return why this member cannot run a task with *req*, or ``None``.

        Judged against the declared attributes and the **default** pilot
        size -- the pilot this member would be grown with.
        '''
        return satisfies(req, self.attributes,
                         self.pilot_sizes.get(self.default_size))


@dataclass
class PoolConfig:
    '''One pool — the unit of resource budget, policy, and task grouping.

    A pool is a **capability class** with one or more :class:`PoolMember`
    entries.  Pool identity is the tuple ``(owning_sid, name)``.

    Two declaration shapes, one runtime shape:

    - **legacy** (``multi_member`` false): the nine scalar fields below are
      authoritative and ``__post_init__`` synthesises exactly one implicit
      member (:data:`IMPLICIT_MEMBER`) from them.  When ``endpoint_name`` is
      ``None`` at parse time the dispatcher resolves it at pool
      materialisation by picking a connected compute endpoint (lexically
      first) — through :meth:`bind_endpoint`, so the implicit member is
      bound too.
    - **class pool** (``multi_member`` true): ``members`` is authoritative
      and the scalar fields are a read-only projection of the *primary*
      member (first by declaration order), recomputed on every parse.

    Either way every runtime code path reads ``members`` only.
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
    strategy        : str  = 'conservative'  # policy name (see task_dispatcher_policy)
    strategy_config : dict[str, Any] = field(default_factory=dict)
    # -- capability-class fields -------------------------------------------
    pool_class      : str  = ''          # '' = unclassified (legacy pools)
    members         : dict[str, PoolMember] = field(default_factory=dict)
    multi_member    : bool = False       # the declaration carried 'members'

    def __post_init__(self) -> None:
        '''Synthesise the implicit member, or project the primary member.

        This is the **single construction site** of the implicit member:
        :class:`PoolConfig` is instantiated directly in several places that
        never touch the parser (``default_pool_config``, tests, embedders),
        so the synthesis cannot live in ``_parse_pool``.  For the same
        reason a class pool's scalar projection is computed here, via
        :meth:`reproject`, and not in the parser.

        ``pilot_sizes`` is shared *by reference* with the member so the
        legacy projection and the member can never drift; ``endpoint_name``
        is a plain string, so it is instead written through both by
        :meth:`bind_endpoint`.
        '''
        if self.multi_member:
            self.reproject()
        elif not self.members:
            self.members = {IMPLICIT_MEMBER: PoolMember(
                member_id     = IMPLICIT_MEMBER,
                endpoint_name = self.endpoint_name or '',
                queue         = self.queue,
                account       = self.account,
                pilot_sizes   = self.pilot_sizes,
                default_size  = self.default_size,
                min_pilots    = self.min_pilots,
                max_pilots    = self.max_pilots,
                scratch_base  = self.scratch_base,
                shared_fs     = True)}

    def bind_endpoint(self, name: str) -> None:
        '''Bind a late-resolved ``endpoint_name`` to the pool *and* its member.

        Only meaningful for a legacy pool: a class pool's ``endpoint_name``
        is a projection of its primary member, never a binding, and the
        dispatcher skips the auto-pick for one.
        '''
        self.endpoint_name = name
        member = self.members.get(IMPLICIT_MEMBER)
        if member is not None:
            member.endpoint_name = name

    def reproject(self) -> None:
        '''Recompute the legacy scalar projection from the primary member.

        Construction does this once; a live class pool needs it
        again whenever its member set changes (a member added to an
        emptied pool, or the primary member removed), so the summary and
        every legacy consumer keep seeing a coherent projection.  A no-op
        for a legacy pool, whose scalars are authoritative.
        '''
        if not self.multi_member:
            return
        if not self.members:
            for key, val in _EMPTY_PROJECTION.items():
                setattr(self, key, val)
            self.pilot_sizes = {}
            return
        primary = next(iter(self.members.values()))
        self.queue         = primary.queue
        self.account       = primary.account
        self.endpoint_name = primary.endpoint_name
        self.pilot_sizes   = primary.pilot_sizes
        self.default_size  = primary.default_size
        self.min_pilots    = primary.min_pilots
        self.max_pilots    = primary.max_pilots
        self.scratch_base  = primary.scratch_base

    def primary_member(self) -> PoolMember:
        '''Return the first member by declaration order.

        Raises :class:`KeyError` for an emptied class pool (every member
        removed with ``force``), which only the replay path can produce.
        '''
        for member in self.members.values():
            return member
        raise KeyError(f'pool {self.name!r} has no members')

    def member(self, mid: str | None) -> 'PoolMember | None':
        '''Return the member with id *mid* (``''`` is the implicit one).'''
        return self.members.get(mid)

    def to_dict(self) -> dict:
        '''Return the persistence view of this config.

        ``multi_member`` is always written, and an explicit flag wins at
        parse time (see :func:`_parse_pool`), so a legacy pool's persisted
        implicit member is ignored on replay and re-synthesised — replay
        never has to guess the shape.
        '''
        return asdict(self)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class PoolConfigError(ValueError):
    '''Raised when a pool declaration violates schema invariants.'''
    pass


def parse_pools(raw: Any, source: str = '<dict>', *,
                allow_empty_members: bool = False) -> dict[str, PoolConfig]:
    '''Validate and parse a declaration dict into a ``{name: PoolConfig}``.

    *raw* is a JSON object whose ``"pools"`` key holds an array of pool
    records (the same shape a ``register_session`` body carries).  Raises
    :class:`PoolConfigError` with an actionable message on any schema
    violation.

    *allow_empty_members* accepts a class pool whose ``members`` list is
    empty.  It is **false on the declaration path** — ``register_session``
    must never accept a pool that can run nothing — and **true on the
    replay path**: removing the last member with ``force`` legitimately
    produces a member-less class pool, persists it, and that state file
    must replay rather than be dropped as unparseable (which would take
    the pool's pilot and task history with it).
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
        pool = _parse_pool(entry, source=f"{source}: pools[{i}]",
                           allow_empty_members=allow_empty_members)
        if pool.name in pools:
            raise PoolConfigError(
                f"{source}: duplicate pool name '{pool.name}'")
        pools[pool.name] = pool

    if not pools:
        raise PoolConfigError(f"{source}: no pools defined")

    return pools


def _parse_pool(d: Any, source: str, *,
                allow_empty_members: bool = False) -> PoolConfig:
    '''Build a single :class:`PoolConfig` from a dict, validating fields.

    The shape switch is **explicit, never inferred from the presence of
    members alone**: a declaration carrying ``members`` is a class pool,
    unless it also says ``multi_member: false``.  An explicit flag always
    wins and then the ``members`` key is *ignored* — defence in depth for
    an old or hand-edited state file, which then takes the identical legacy
    path it takes today.  A caller therefore never has to send the flag.
    '''
    multi_member = bool(d.get('multi_member', 'members' in d))

    required = ('name', 'members') if multi_member \
          else ('name', 'queue', 'default_size', 'pilot_sizes')
    for key in required:
        if key not in d:
            raise PoolConfigError(f"{source}: missing required field '{key}'")

    name = d['name']
    if not isinstance(name, str) or not name:
        raise PoolConfigError(f"{source}: 'name' must be a non-empty string")

    members: dict[str, PoolMember] = {}
    if multi_member:
        members  = _parse_members(d['members'], source, name,
                                  allow_empty=allow_empty_members)
        # Placeholders only: ``PoolConfig.__post_init__`` overwrites every
        # scalar with the primary member's projection.
        resource = {**_EMPTY_PROJECTION, 'pilot_sizes': {}}
    else:
        resource = _parse_resource(d, source, endpoint_required=False)

    pool_class = d.get('pool_class', '')
    if not isinstance(pool_class, str) or not POOL_CLASS_RE.match(pool_class):
        raise PoolConfigError(
            f"{source}: 'pool_class' must match {POOL_CLASS_RE.pattern} "
            f"(got {pool_class!r})")

    strategy = d.get('strategy', 'conservative')
    if not isinstance(strategy, str) or not strategy:
        raise PoolConfigError(
            f"{source}: 'strategy' must be a non-empty string")

    # Validate against the policy registry here so a bad name fails the
    # declaration (register_session → 400, no dangling session) instead of
    # blowing up at pool materialisation.  Import is function-local because
    # the policy module imports this one.
    from .task_dispatcher_policy import known_policies
    policies = known_policies()
    if strategy not in policies:
        raise PoolConfigError(
            f"{source}: unknown 'strategy' {strategy!r} "
            f"(known: {', '.join(policies)})")

    strategy_config = d.get('strategy_config', {})
    if not isinstance(strategy_config, dict):
        raise PoolConfigError(
            f"{source}: 'strategy_config' must be an object")

    cfg = PoolConfig(
        name            = name,
        **resource,
        strategy        = strategy,
        strategy_config = strategy_config,
        pool_class      = pool_class,
        members         = members,
        multi_member    = multi_member,
    )

    # Trial-instantiate the policy so an invalid ``strategy_config`` (e.g. a
    # bad ``router_preference``) also fails the declaration here — 400 at
    # register_session, no dangling session — rather than raising later at
    # pool materialisation.  Policy constructors are required to be cheap
    # and side-effect-free (see DispatchPolicy).
    from .task_dispatcher_policy import make_policy
    try:
        make_policy(cfg)
    except PoolConfigError:
        raise
    except Exception as e:
        raise PoolConfigError(
            f"{source}: strategy {strategy!r} rejected its "
            f"strategy_config: {e}") from e

    return cfg


def _parse_members(raw: Any, source: str, pool_name: str, *,
                   allow_empty: bool) -> dict[str, PoolMember]:
    '''Normalise a ``members`` declaration into an insertion-ordered dict.

    Accepts either a list of objects (each carrying ``member_id``) or a
    ``{member_id: object}`` map; both yield the same dict, and the *first*
    entry is the pool's primary member.
    '''
    if isinstance(raw, dict):
        entries = []
        for mid, entry in raw.items():
            if isinstance(entry, dict) and 'member_id' not in entry:
                entry = {**entry, 'member_id': mid}
            entries.append(entry)
    elif isinstance(raw, list):
        entries = list(raw)
    else:
        raise PoolConfigError(
            f"{source}: 'members' must be a list or an object, got "
            f"{type(raw).__name__}")

    if not entries and not allow_empty:
        raise PoolConfigError(f"{source}: 'members' must not be empty")

    members: dict[str, PoolMember] = {}
    for i, entry in enumerate(entries):
        member = parse_member(entry, source=f"{source}: members[{i}]",
                              pool_name=pool_name)
        if member.member_id in members:
            raise PoolConfigError(
                f"{source}: duplicate member_id {member.member_id!r}")
        members[member.member_id] = member
    return members


def _parse_resource(d: dict, source: str, *,
                    endpoint_required: bool) -> dict:
    '''Validate the eight resource fields a legacy pool and a member share.

    Returns them as keyword arguments for :class:`PoolConfig` or
    :class:`PoolMember`.  Only ``endpoint_name`` differs: a member must
    name one, a legacy pool may leave it ``None`` for the auto-pick.  The
    caller has already checked the required keys.
    '''
    endpoint_name = d.get('endpoint_name')
    if endpoint_required:
        if not isinstance(endpoint_name, str) or not endpoint_name:
            raise PoolConfigError(
                f"{source}: 'endpoint_name' must be a non-empty string")
    elif endpoint_name is not None:
        if not isinstance(endpoint_name, str) or not endpoint_name:
            raise PoolConfigError(
                f"{source}: 'endpoint_name' must be a non-empty string "
                f"or null")

    queue = d['queue']
    if not isinstance(queue, str) or not queue:
        raise PoolConfigError(f"{source}: 'queue' must be a non-empty string")

    account = d.get('account')
    if account is not None and not isinstance(account, str):
        raise PoolConfigError(f"{source}: 'account' must be a string or null")

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

    min_pilots = _parse_int(d.get('min_pilots', 0), 'min_pilots', source,
                            min_value=0)
    max_pilots = _parse_int(d.get('max_pilots', 4), 'max_pilots', source,
                            min_value=1)
    if min_pilots > max_pilots:
        raise PoolConfigError(
            f"{source}: min_pilots ({min_pilots}) > max_pilots ({max_pilots})")

    scratch_base = d.get('scratch_base')
    if scratch_base is not None and not isinstance(scratch_base, str):
        raise PoolConfigError(
            f"{source}: 'scratch_base' must be a string or null")

    return {'endpoint_name': endpoint_name,
            'queue'        : queue,
            'account'      : account,
            'pilot_sizes'  : pilot_sizes,
            'default_size' : default_size,
            'min_pilots'   : min_pilots,
            'max_pilots'   : max_pilots,
            'scratch_base' : scratch_base}


def parse_member(d: Any, source: str, *, pool_name: str) -> PoolMember:
    '''Build a single :class:`PoolMember` from a dict, validating fields.

    Also the public entry point for one member declaration (the
    add-member route).
    '''
    if not isinstance(d, dict):
        raise PoolConfigError(
            f"{source}: must be an object, got {type(d).__name__}")

    for key in ('member_id', 'endpoint_name', 'queue', 'default_size',
                'pilot_sizes'):
        if key not in d:
            raise PoolConfigError(f"{source}: missing required field '{key}'")

    member_id = d['member_id']
    if not isinstance(member_id, str) or not MEMBER_RE.match(member_id):
        raise PoolConfigError(
            f"{source}: 'member_id' must match {MEMBER_RE.pattern} "
            f"(got {member_id!r})")
    if len(pool_name) + len(member_id) > MAX_POOL_MEMBER_NAME_LEN:
        raise PoolConfigError(
            f"{source}: pool name plus 'member_id' must be at most "
            f"{MAX_POOL_MEMBER_NAME_LEN} characters "
            f"(got {len(pool_name) + len(member_id)})")

    resource = _parse_resource(d, source, endpoint_required=True)
    if resource['queue'] == DEFAULT_POOL_NAME:
        raise PoolConfigError(
            f"{source}: 'queue' must be a real batch queue, not the "
            f"{DEFAULT_POOL_NAME!r} sentinel")

    shared_fs = d.get('shared_fs', True)
    if not isinstance(shared_fs, bool):
        raise PoolConfigError(f"{source}: 'shared_fs' must be a boolean")

    attributes = d.get('attributes', {})
    if not isinstance(attributes, dict):
        raise PoolConfigError(f"{source}: 'attributes' must be an object")
    for key, val in attributes.items():
        if not isinstance(key, str):
            raise PoolConfigError(
                f"{source}: 'attributes' keys must be strings")
        ok = (isinstance(val, str)
              or (isinstance(val, (int, float)) and not isinstance(val, bool))
              or (isinstance(val, list)
                  and all(isinstance(v, str) for v in val)))
        if not ok:
            raise PoolConfigError(
                f"{source}: attribute {key!r} must be a string, a number, "
                f"or a list of strings")

    pilot = d.get('pilot', PILOT_SUBMIT)
    if pilot not in PILOT_MODES:
        raise PoolConfigError(
            f"{source}: 'pilot' must be one of {', '.join(PILOT_MODES)} "
            f"(got {pilot!r})")

    # An absolute epoch, never a duration: see PoolMember.end_time.  ``0``
    # is not a plausible allocation end and reads as "unknown", so it is
    # rejected rather than silently capping every deadline to the epoch.
    end_time = d.get('end_time')
    if end_time is not None:
        if isinstance(end_time, bool) or \
                not isinstance(end_time, (int, float)) or end_time <= 0:
            raise PoolConfigError(
                f"{source}: 'end_time' must be a positive epoch or null, "
                f"got {end_time!r}")
        end_time = float(end_time)

    budget = d.get('budget', {})
    if not isinstance(budget, dict):
        raise PoolConfigError(f"{source}: 'budget' must be an object")
    unknown = sorted(set(budget) - {'node_hours'})
    if unknown:
        raise PoolConfigError(
            f"{source}: unknown budget key {unknown[0]!r} "
            f"(known: node_hours)")
    if 'node_hours' in budget:
        val = budget['node_hours']
        if isinstance(val, bool) or not isinstance(val, (int, float)) \
                or val <= 0:
            raise PoolConfigError(
                f"{source}: budget 'node_hours' must be a positive number, "
                f"got {val!r}")

    return PoolMember(
        member_id     = member_id,
        **resource,
        shared_fs     = shared_fs,
        attributes    = dict(attributes),
        budget        = {k: float(v) for k, v in budget.items()},
        pilot         = pilot,
        end_time      = end_time,
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

    *queue* defaults to the sentinel ``"default"`` only because the schema
    requires a non-empty ``queue``.  That sentinel is **not** a real batch
    queue: the dispatcher refuses to submit a pilot for it and fails the
    pilot fast (see ``PluginTaskDispatcher._do_pilot_submit``).  Pass a real
    queue here, or re-declare the pool with an explicit ``queue``, to run
    pilots.
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
