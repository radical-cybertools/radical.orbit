# Plan 121 — Multi-member pools: a pool is a capability class

Repo/branch: `/home/merzky/radical/radical.orbit` @ `feature/atomic-federation`.
Consumes: `plans/120-task-requirements-passthrough.md` (per-task
cores/gpus/mem forwarded to rhapsody). Consumed by:
`/home/merzky/projects/atomic/plans/08-federation-class-pools.md`.

**Decision (Andre, 2026-09-07 — not up for relitigation).** A task-dispatcher
pool is a **capability class**, not a site. A GPU pilot on Bridges and a GPU
pilot on Perlmutter both live in the pool `fed-gpu`; CPU pilots in `fed-cpu`.
A pool has **members**; a member is one (endpoint, queue, account, pilot-size
menu, attributes, budget) tuple. Pilots carry their member's id and its
declared attributes. Tasks carry `requirements`. The policy chooses *which
member to grow* and *which pilot may run a task*.

---

## 1. Status check — what exists today (file:line evidence)

| Claim | Evidence |
|---|---|
| A pool binds exactly one endpoint | `task_dispatcher_config.py:53-72` — `PoolConfig` has scalar `endpoint_name`, `queue`, `account`, `pilot_sizes`, `default_size`, `min_pilots`, `max_pilots`, `scratch_base` |
| Pool identity is `(owning_sid, name)` | `plugin_task_dispatcher.py:552-554`, `:569-607` |
| **Pools *can* be added to a live session** (the 00-overview claim "no add-pool route" is stale) | `plugin_task_dispatcher.py:912-920` — `register_session` reconnects the sid, then materialises every declared pool; `_materialise_pool:584-587` returns the existing `PoolState` for a re-declared pool |
| **Members cannot be added** — a re-declaration of an *existing* pool is silently ignored | `plugin_task_dispatcher.py:584-587` (early return before any config merge) |
| Pilot child endpoint name | `plugin_task_dispatcher.py:1572` — `f'{pool_state.config.name}_{record.pid}'` |
| Pilot capacity = nodes × cpus_per_node, task = 1 slot — **and it stays that way**, see §1.1 | `plugin_task_dispatcher.py:1421-1432`; `task_dispatcher_state.py:118-122` (`free_capacity`) |
| `PilotRecord` has no member/attribute fields | `task_dispatcher_state.py:76-94` |
| `TaskRecord` has no `requirements` field | `task_dispatcher_state.py:125-149` |
| Unknown keys in a persisted record are dropped on load (schema additions are safe) | `task_dispatcher_state.py:201-208` |
| Pool state dir is `<root>/<sid>/<name>__<endpoint_name>` | `plugin_task_dispatcher.py:558-561` |
| Pool config persists as `asdict(config)` — new dataclass fields persist for free, **which is exactly the trap** for the synthesised implicit member (§3.1) | `plugin_task_dispatcher.py:209-212`, `task_dispatcher_state.py:247-255` |
| Replay re-parses the persisted config through `parse_pools` | `plugin_task_dispatcher.py:609-646` |
| Policy callable today is `submit_pilot(size_key)` | `plugin_task_dispatcher.py:1460-1462`; base contract `task_dispatcher_policy.py:83-91`; caller `task_dispatcher_strategy_conservative.py:187` |
| `pick_dispatch` head-of-line blocks: it returns `None` if the *top* task has no pilot | `task_dispatcher_strategy_conservative.py:204-224` |
| Pilot loss re-queues its non-terminal tasks (QUEUED, `pilot_id=None`) — **verified, no counter** | `plugin_task_dispatcher.py:1688-1697` |
| Session teardown cancels every live pilot of the session's pools | `plugin_task_dispatcher.py:2151-2179` |
| `stage_in` writes `<scratch_base>/<task_id>/<file>` **on the broker host**; `stage_out` reads `ps.scratch_base / task_id` | `plugin_task_dispatcher.py:1295-1303`, `:1320-1335` |
| `cwd` is **required** at submit, i.e. fixed before a pilot exists | `plugin_task_dispatcher.py:977-980`, record built `:1015-1027` |
| Task cwd reaches rhapsody twice (top-level + backend kwargs) | `plugin_task_dispatcher.py:1789-1799` |
| Verbose pool summary carries `pilots`, `pilot_history`, `recent_tasks` (cap 50), and a flat `pilot_sizes` | `plugin_task_dispatcher.py:2107-2147` |
| Node-hour accounting reads `pilot_history` + `pilot_sizes` | `federation_state.py:205-244`, consumer `plugin_federation.py:1126-1128` |
| Explorer pool card renders one endpoint/queue/account and one size table | `data/plugins/task_dispatcher.js:137-180` |
| `Plugin` base offers **only** `add_route_post` / `add_route_get` | `plugin_base.py:296-310` |
| …but direct dispatch and the gateway are method-generic (DELETE is reachable) | `plugin_base.py:327-341`; `broker_plugin_host.py:103-118`; `gateway.py:421` |
| Endpoint-mode submit bypasses pools completely | `plugin_task_dispatcher.py:973-985`, `:1038-1096` |
| Orphan (owner-less, replayed) pools are not ticked | `plugin_task_dispatcher.py:707-711` |

### 1.1 Scope — what 120 gives us, and what is explicitly deferred

`plans/120-task-requirements-passthrough.md` exists and carries Andre's
scope decision at its head (2026-09-07): **120 only wires, persists,
validates and forwards** per-task requirements to rhapsody. Dispatcher-side
core/GPU **reservation** and in-pilot **pinning** (120's PR2, and the
`reserved()`/`cores_total`/`gpus_total` machinery in 120 §PR1.3) are
**deferred**; oversubscription control stays with rhapsody even though the
`concurrent` / `dragon_v3` backends do not enforce it today. Tracked as an
Orbit TODO.

Consequences for 121 — these are constraints, not choices:

- **Capacity stays task-count based.** `pilot.capacity = nodes ×
  cpus_per_node` (`plugin_task_dispatcher.py:1424`) and
  `free_capacity() = capacity − in_flight` (`task_dispatcher_state.py:118-122`)
  are untouched. 121 adds **no** per-pilot core/GPU counters, no
  `reserved()` helper, no `fits()` occupancy test.
- **`pick_dispatch` matches on attributes only** — software ⊆ member
  software, labels, class — plus the unchanged `free_capacity() > 0` slot
  test. It does not track free cores or GPUs per pilot.
- **Shape validation happens once, at submit, per node**: a task whose
  `cores`/`gpus` exceed every member's pilot size is a `400` (120 §PR1.4,
  extended to members in §4.4 below), not a queued task that can never run.
- Everything a future reservation model would need is already in the record
  (`TaskRecord.requirements`, `PilotRecord.member_id`, the member's
  `PilotSize`); nothing here forecloses it. **Do not design it in this
  round** — see the 120 header and the Orbit TODO.

What 121 takes from 120 (marked **[120]** below):

> `POST submit/{sid}` accepts an optional `requirements` object
> `{cores, gpus, mem_gb, ranks, mpi}`, **unknown keys rejected with 400**
> (120 §PR1.1); it is persisted as `TaskRecord.requirements: dict`
> (120 §PR1.2) and mapped to backend kwargs in `_do_rhapsody_submit`
> (`plugin_task_dispatcher.py:1789-1799`).

**Two amendments 121 makes to 120** (both one-liners, agreed with that
plan's author — flag them before either lands):

1. §PR1.1's key whitelist gains `software: [str]` and
   `labels: {str: str|number}`. They are *matching* inputs: `backend_kwargs`
   (§PR1.5) ignores them, so nothing reaches rhapsody.
2. §PR1.6 says "software, node_hours → selection only, **never
   forwarded**". For `software` that is now wrong: with class pools the
   *dispatcher* is the component that matches software against a pilot's
   member attributes, so the federation must forward it. `node_hours` stays
   federation-only and is stripped before forwarding (plan 08 §4).

---

## 2. The model

```
pool  fed-gpu   class="gpu"   ── members ──┬── perlmutter  (endpoint ep_pm,  queue regular, account m1234,
                                           │                attributes {site: NERSC, software: [lammps, pytorch]})
                                           └── bridges     (endpoint ep_br,  queue GPU,     account abc123,
                                                            attributes {site: PSC,   software: [pytorch]})
pilots:  fed-gpu_perlmutter_p.1a2b…   member_id=perlmutter  attributes={…snapshot…}
tasks :  pool-level queue; bound to a pilot only at dispatch (`_claim`, :1751)
```

- **class** is an explicit string field (`PoolConfig.pool_class`), never
  derived inside the dispatcher. The federation computes it (`gpu` when
  `gpus_per_node > 0`, else `cpu`) and declares it; more classes can be
  declared later without a dispatcher change.
- **budget is per member** (allocations are per site), not per pool.
- **attributes are declared, not self-reported** — no handshake change, no
  protocol change. A pilot snapshots its member's attributes at submit time
  and keeps them for its life.

---

## 3. Schema

### 3.1 `task_dispatcher_config.py`

```python
@dataclass
class PoolMember:
    '''One resource inside a capability-class pool.'''
    member_id     : str                       # unique within the pool; MEMBER_RE
    endpoint_name : str                       # required — no auto-pick for members
    queue         : str                       # non-empty, not the 'default' sentinel
    account       : str | None
    pilot_sizes   : dict[str, PilotSize]
    default_size  : str                       # key into pilot_sizes
    min_pilots    : int  = 0
    max_pilots    : int  = 4
    scratch_base  : str | None = None         # path ON THE MEMBER'S HOST
    shared_fs     : bool = True               # broker host and member share scratch_base
    attributes    : dict[str, Any] = field(default_factory=dict)
                                              # free-form; 'software': [str] and
                                              # 'site': str are conventions, not schema
    budget        : dict[str, float] = field(default_factory=dict)   # {'node_hours': x}

@dataclass
class PoolConfig:
    name            : str
    queue           : str                     # compat projection of the primary member
    account         : str | None              # compat projection
    pilot_sizes     : dict[str, PilotSize]    # compat projection
    default_size    : str                     # compat projection
    endpoint_name   : str | None = None       # compat projection
    min_pilots      : int  = 0                # compat projection
    max_pilots      : int  = 4                # compat projection
    scratch_base    : str | None = None       # compat projection
    strategy        : str  = 'conservative'
    strategy_config : dict[str, Any] = field(default_factory=dict)
    # -- new --------------------------------------------------------------
    pool_class      : str  = ''               # '' = unclassified (legacy pools)
    members         : dict[str, PoolMember] = field(default_factory=dict)
    multi_member    : bool = False            # the declaration carried 'members'

    def __post_init__(self) -> None: ...             # synthesises the implicit member
    def primary_member(self) -> PoolMember: ...      # first member by insertion order
    def member(self, mid: str) -> PoolMember | None: ...
    def to_dict(self) -> dict: ...                   # persistence view, see below
```

**Invariant.** When `multi_member` is true, `members` is authoritative and the
nine legacy scalar fields are a *read-only projection of the primary member*,
recomputed on every parse. When it is false, `members` holds exactly one
implicit member built from the legacy fields, with
`member_id = IMPLICIT_MEMBER = '_'`. Every code path below therefore only ever
reads `config.members` — one shape at runtime, two shapes on the wire.

**`__post_init__` is the single construction site of the implicit member.**
`PoolConfig` is instantiated directly in several places that never touch the
parser — `default_pool_config` (`task_dispatcher_config.py:266-297`) and the
`PoolConfig(...)` literals in `test_task_dispatcher_strategy_conservative.py`,
`test_task_dispatcher_policy.py`, `test_plugin_task_dispatcher.py`,
`test_task_dispatcher_recovery.py` — so the synthesis cannot live in
`_parse_pool`:

```python
def __post_init__(self):
    if not self.members and not self.multi_member:
        self.members = {IMPLICIT_MEMBER: PoolMember(
            member_id     = IMPLICIT_MEMBER,
            endpoint_name = self.endpoint_name or '',
            queue         = self.queue,
            account       = self.account,
            pilot_sizes   = self.pilot_sizes,      # same object: the projection
            default_size  = self.default_size,     # and the member stay in sync
            min_pilots    = self.min_pilots,
            max_pilots    = self.max_pilots,
            scratch_base  = self.scratch_base,
            shared_fs     = True)}
```

`endpoint_name` is deliberately shared by reference-then-write: when
`_materialise_pool` auto-resolves it (`plugin_task_dispatcher.py:577-582`) it
must write **both** `cfg.endpoint_name` and
`cfg.members[IMPLICIT_MEMBER].endpoint_name` — do it through one helper
`cfg.bind_endpoint(name)`.

**`to_dict()` — persistence view (BLOCKING fix).** `PoolState.persist`
writes `asdict(self.config)` (`plugin_task_dispatcher.py:211`) and
`_replay_state` re-parses it (`:631`). With a plain `asdict`, a *legacy*
pool would persist its synthesised `{'_': …}` member, replay would see a
`members` key, treat the pool as a class pool, and reject `'_'` against
`MEMBER_RE` — every legacy pool would fail replay. Therefore:

```python
def to_dict(self):
    d = asdict(self)
    if not self.multi_member:
        d.pop('members')            # never persist the synthesised member
    return d
```

and `PoolState.persist` calls `self.config.to_dict()` instead of
`asdict(self.config)`. `multi_member` is always persisted (as `false` for a
legacy pool), so replay never has to guess.

### 3.2 Parser (`_parse_pool`, `task_dispatcher_config.py:119-223`)

- **Shape switch (explicit, never inferred from the presence of members
  alone):**
  ```python
  multi_member = bool(d.get('multi_member', 'members' in d))
  ```
  i.e. **a declaration carrying `members` is a class pool**, unless it also
  says `multi_member: false` — an explicit flag always wins, and then the
  `members` key is **ignored** (defence in depth for an old or hand-edited
  state file) and the legacy path runs unchanged. A caller therefore never
  has to send the flag; it exists so a persisted *legacy* pool that somehow
  acquired a `members` key still replays as a legacy pool.
- Required fields: `('name',)` plus **either** `members` (when
  `multi_member`) **or** the existing
  `('queue', 'default_size', 'pilot_sizes')`. Nothing else in the existing
  required-field logic moves — a legacy declaration takes the identical path
  it does today.
- `members` may be a list of objects (each carrying `member_id`) or a
  `{member_id: object}` map; both normalise to an insertion-ordered dict.
- **Signature: `_parse_pool(d, source, *, allow_empty_members=False)`**
  (and `parse_pools(raw, source, *, allow_empty_members=False)` threading it
  through). An empty `members` with `multi_member: true` is a
  `PoolConfigError` on the **declaration** path — `register_session` must
  never accept a pool that can run nothing — but is **accepted on the replay
  path**: `_pool_config_from_dict` (`plugin_task_dispatcher.py:642-646`)
  passes `allow_empty_members=True`, because `DELETE …` with `force: true`
  legitimately produces a member-less class pool, persists it, and that state
  file must replay rather than be skipped as unparseable
  (`_replay_state:633-640` swallows the error and drops the pool, taking its
  pilot and task history with it).
- New `_parse_member(d, source)` mirrors `_parse_pool`'s validation style:
  `member_id` matched against `MEMBER_RE = ^[a-z0-9][a-z0-9_.-]*$`, **with
  `IMPLICIT_MEMBER` (`'_'`) explicitly exempt** — it never appears in a
  class-pool declaration, but the exemption keeps a hand-written or
  round-tripped `'_'` from being a parse error; `endpoint_name` non-empty,
  `queue` non-empty and ≠ `'default'`, `pilot_sizes` non-empty via the
  existing `_parse_pilot_size` (`:226-248`), `default_size ∈ pilot_sizes`,
  `min_pilots ≤ max_pilots ≥ 1` via `_parse_int` (`:251-259`) — the member
  keeps `PoolConfig`'s `max_pilots >= 1` rule, there is no "quiesced"
  member; `attributes` a dict whose values are str / number / list-of-str,
  `budget` `{}` or `{'node_hours': positive number}`, `shared_fs` a bool.
- `pool_class`: optional string matching `^[a-z0-9_.-]*$` — the pattern
  admits the empty string, and `''` is the legal value for an *unclassified*
  pool (every legacy pool). A non-empty name that does not match is a
  `PoolConfigError` — **not** silently lowercased or coerced.
- **`strategy_config` for a class pool**: the federation keeps sending
  `_STRATEGY_CONFIG = {'min_dwell_sec': 5, 'max_in_flight_submissions': 1}`
  (`plugin_federation.py:132`). Under §5's per-member accounting that means
  *per member*: 5 s dwell each, one in-flight submission each — plus the
  pool-wide ceiling of R5, so a 5-member pool never has more than
  `max(max_in_flight_submissions, ...)` submissions in flight at once. That
  is the intended behaviour (a new member should not wait behind a sibling's
  dwell window) and it is what the demo's warm-up budget assumes.
- After members parse, project the primary member onto the legacy fields.
  Duplicate `member_id` → `PoolConfigError`.
- The policy trial-instantiation (`:208-221`) is unchanged and still runs last.

**Round-trip contract (test it both ways).**
`parse_pools({'pools': [cfg.to_dict()]})` must equal `cfg` for a legacy pool
*and* for a class pool. That is the exact path `_replay_state` takes.

### 3.3 `task_dispatcher_state.py`

```python
@dataclass
class PilotRecord:
    ...                                        # unchanged fields :77-94
    member_id     : str  = ''                  # '' → the implicit member
    attributes    : dict = field(default_factory=dict)  # snapshot at submit time
    endpoint_name : str  = ''                  # snapshot: who runs the psij job
    # -- size snapshot, taken at submit alongside size_key ------------------
    nodes         : int  = 0
    cpus_per_node : int  = 0
    gpus_per_node : int  = 0

@dataclass
class TaskRecord:
    ...                                        # unchanged fields :126-149
    requirements : dict = field(default_factory=dict)   # [120]
    member_id    : str | None = None           # set at dispatch, with pilot_id
    requeues     : int = 0                     # times a pilot loss re-queued it
    spooled      : list[str] = field(default_factory=list)  # dispatcher-held inputs
    cwd_assigned : bool = False                # cwd came from the dispatcher
```

**`endpoint_name` on the pilot** is the same kind of snapshot as
`attributes` and the size, and for the same reason: a pilot outlives its
member. `_do_pilot_cancel` and `_reconcile_pilot` (`:1606-1648`) need an
endpoint to reach psij *after* `DELETE …/members` has dropped the member
(§7 removes it before the cancels), so they must read it off the pilot.

**`spooled` is not `inputs`.** `TaskRecord.inputs` (`:134`) keeps its
existing, client-declared meaning (the names a client says the task
consumes; nothing in the dispatcher acts on it). `spooled` is the list of
files the dispatcher actually holds under
`<state_dir>/inputs/<task_id>/` (§4.3), and the placement step at dispatch
iterates **`spooled` only**. Conflating them would make a plain `inputs`
declaration look like a staging promise the dispatcher never made.

`record_from_dict` (`:201-208`) drops unknown keys and fills defaults, so
**old `state.json` files load unchanged**: a legacy pilot gets
`member_id=''` (→ the implicit member) and a zero size snapshot.

**Why the size snapshot (BLOCKING fix).** `size_key` alone is no longer
enough to size a pilot: sizes now live *per member*, so resolving a key
against the pool's flat (primary-member) `pilot_sizes` gives the wrong node
count for a mixed-node pool and gives **nothing at all** once the member has
been removed — and a removed member's pilots are exactly the ones whose
node-hours still have to be reported. So `_submit_pilot` copies
`nodes`/`cpus_per_node`/`gpus_per_node` off the chosen `PilotSize` onto the
record, and every consumer prefers the snapshot:

- `_activate_pilot` (`:1421-1432`): `capacity = pilot.nodes *
  pilot.cpus_per_node`, falling back to `ps.size_of(pilot)` when the
  snapshot is zero (a pre-121 record) and repairing the record in place.
- accounting: see below.
- `satisfies` at dispatch: the pilot's snapshot is the `size` argument.

Move `node_hours_from_history` (today `federation_state.py:205-244`) **into**
`task_dispatcher_state.py` as `node_hours(history, pilot_sizes=None,
now=None)` and re-export it from `federation_state`
(`from .task_dispatcher_state import node_hours as node_hours_from_history`)
so no federation caller changes. Its node count now resolves in this order:
`entry['nodes']` (the snapshot) → `pilot_sizes[entry['size_key']]['nodes']`
(a pre-121 history) → 0. `pilot_sizes` becomes optional. The dispatcher needs
this for the per-member summary and must not import the federation module
(layering).

New module `task_dispatcher_match.py` — the single implementation of
requirement matching, imported by the policy *and* (for the eligibility
explanation) by the federation:

```python
def satisfies(requirements: dict, attributes: dict,
              size: PilotSize | None) -> str | None:
    '''Return None when the requirements fit this shape, else a reason.

    Pure and stateless: it compares a task's declared needs against a
    *declaration* (a member's attributes + its pilot size, or a pilot's
    attribute snapshot + its size).  It knows nothing about occupancy —
    free capacity stays the dispatcher's existing task-count slot test
    (see §1.1).
    '''
```

Rules (deliberately the same vocabulary as
`federation_policy.BudgetLoadPolicy.reject_reason`, `federation_policy.py:128-164`,
so federation and dispatcher reasons read alike):

- `software`: `set(req) ⊆ set(attributes.get('software') or [])`, else
  `"software missing: X, Y"`.
- `cores`: `req.cores > 0` needs `size.cpus_per_node >= req.cores`, else
  `"cores N < M"`. **Per node**, matching 120 §PR1.4 — no shipped backend
  spreads one task across nodes in v1.
- `gpus`: `req.gpus > 0` needs `size.gpus_per_node >= req.gpus`, same rule.
  This is the *shape* test only; nothing counts GPUs already in use (§1.1).
- `mem_gb`: compared against `attributes['mem_gb_per_node']` when declared;
  ignored when not (a missing attribute never rejects a *numeric* requirement
  of 0 or less, and rejects a positive one only when the key exists).
- `labels`: a `{str: str|int|float}` map (**the same value types in 120, 121
  and 08** — no bools, no lists, no nesting; the 120 implementer has the
  same rule). Every `k: v` must satisfy `attributes[k] == v` or
  `v in attributes[k]` (list-valued attribute), else
  `"label k=v not matched"`. An undeclared label key rejects.
- `mpi` (bool, 120's key): when true the pilot's `rhapsody_backend` must not
  be `dragon_v1`, which slot-queues rather than places ranks
  (120 §PR1.5's table). Reason: `"backend dragon_v1 cannot run an mpi task"`.
  Pool-level gate in §4.4.
- **Every other key is ignored**, `ranks` and any key 121 does not know
  included. 120's parser is the gate that rejects unknown keys with a `400`;
  a *matcher* that also rejected them would double-own the whitelist and
  break the moment 120 adds one. Test it: `satisfies({'ranks': 4}, …)` is
  `None`.
- A requirement value ≤ 0 or an empty list is always satisfied.

`satisfies` is used with a *member*'s attributes+default size (scale-up
decision, eligibility display) and with a *pilot*'s attributes+size (dispatch
decision) — one function, two call sites.

---

## 4. New routes

Both are added on `PluginTaskDispatcher.__init__` next to the existing block
(`plugin_task_dispatcher.py:528-538`).

### 4.1 `POST pool/{sid}/{name}/members`

Body = one member declaration (§3.2 shape):

```json
{"member_id": "perlmutter", "endpoint_name": "ep_pm",
 "queue": "regular", "account": "m1234",
 "pilot_sizes": {"default": {"nodes": 1, "cpus_per_node": 128,
                             "gpus_per_node": 4, "walltime_sec": 1800,
                             "rhapsody_backend": "concurrent"}},
 "default_size": "default", "min_pilots": 0, "max_pilots": 2,
 "scratch_base": "/pscratch/…/atomic", "shared_fs": false,
 "attributes": {"site": "NERSC", "software": ["lammps", "pytorch"],
                "mem_gb_per_node": 256},
 "budget": {"node_hours": 40.0}}
```

Returns `200 {"pool": name, "member": <member dict>, "members": [<ids>],
"created": true|false}`.

- `404` unknown session (`_require_known_session`, `:2093-2097`) / unknown
  pool (`_find_pool`, `:552-554`).
- `400` on any schema violation (`PoolConfigError` → detail).
- **`409 "pool <name> is not a class pool; declare it with 'members'"` when
  the target pool has `multi_member == False`.** A single-member pool is
  **never promoted** in place: promotion would have to rename the implicit
  member, rewrite every live pilot's `child_endpoint_name` (impossible — the
  child is already registered under it), and move the pool's state dir. The
  federation always declares a class pool as a class pool from the start.
- `409 "member exists with a different declaration"` when `member_id` is
  present and the parsed member differs (compare `asdict`). An **identical**
  re-POST is a `200 {"created": false}` no-op — this is what makes the
  federation's restart replay idempotent. There is no partial/mutable
  update: a member is replaced by removing and re-adding it.
- Side effects: `ps.config.members[member_id] = member`; `ps.persist()`; a
  `pool_members` notification (`_dispatch_notify`) so the Explorer refreshes.
- No pilot is submitted here. The next housekeeping tick applies the member's
  `min_pilots` floor.

### 4.2 `DELETE pool/{sid}/{name}/members/{member_id}`

**Flags travel in the JSON body, not the query string** (BLOCKING fix):
`_DispatcherAPI._call` (`plugin_federation.py:220-228`) calls
`host.handle_request(method, path, headers, payload)` and never passes
`query_string`, which `BrokerPluginHost.handle_request` takes as a separate
argument (`broker_plugin_host.py:103-109`). A `?cancel_tasks=true` would be
part of the *path* and would simply not match the route. So:

```json
{"cancel_tasks": false, "force": false, "fail_unsatisfiable": true}
```

An empty body is `{}` (the defaults above). Returns `200 {"pool": name,
"member_id": mid, "pilots_cancelled": n, "tasks_requeued": n,
"tasks_failed": n}`.

- `404` unknown session / pool / member; `409` when it is the pool's last
  member and `force` is not true (a member-less pool can never run anything;
  the federation deletes the whole pool instead).
- **`fail_unsatisfiable` (default `true`)** governs the sweep in §7 step 4.
  `true` is the operator's "this member is gone for good" — a task only that
  member could run fails now, with a reason, instead of sitting QUEUED.
  `false` is "this member may come back": the tasks stay QUEUED and become
  dispatchable again when the member re-attaches. The federation passes
  `false` on its liveness path (an endpoint blip) and leaves the default on
  an explicit `leave` (plan 08 §4, §6).
- Drain semantics — §7.

**Base-class touch (required, 5 lines).** `plugin_base.py` gains
`add_route_delete` mirroring `add_route_get` (`:304-310`) with
`methods=["DELETE"]`. Direct dispatch (`_register_direct`, `:327-341`) and
`BrokerPluginHost.handle_request` (`broker_plugin_host.py:103-118`) are
already method-generic, and the gateway catch-all already lists `DELETE`
(`gateway.py:421`) — verified, no other change. This is the preferred form.
The fallback, if a reviewer objects to touching the base class, is
`POST pool/{sid}/{name}/members/{member_id}/remove` with the identical body
and semantics; the federation's `_DispatcherAPI` passes the method straight
through either way.

### 4.3 Task inputs travel with the submit

**Decision (supervisor, review round 1).** A pool-mode submit body may carry

```json
"inputs_b64": {"md.json": "<base64>", "config.yaml": "<base64>"}
```

Keys are validated with the existing `_check_filename` (`:1245-1255`).
Values are base64; a value that does not decode is a `400 "invalid base64:
<e>"`, exactly as `_route_stage_in` already answers (`:1288-1293`).

**Caps, measured on decoded bytes:**

- `_MAX_INPUT_BYTES = 2 * 1024 * 1024` per file. Derived from
  `protocol.FRAME_CAP` (4 MiB, `protocol.py:54`): the encoded form is 4/3 of
  the decoded one and shares the frame with the rest of the submit body, so
  the ceiling is `FRAME_CAP * 3 // 4 − 64 KiB` ≈ 2.9 MiB; 2 MiB is that,
  rounded down to a number an operator can remember.
- `_MAX_INPUTS_BYTES = 8 * 1024 * 1024` for the block.
- Either exceeded → `413`, naming the file and the limit.

It is **spooled**, not written to a task cwd: the dispatcher writes each file
to `<pool state_dir>/inputs/<task_id>/<name>` — broker-local by
construction, alongside the pool's own `state.json` — and records the names
in `TaskRecord.spooled` (§3.3; **not** `inputs`, which keeps its
client-declared meaning). A resubmit of a FAILED/CANCELED task
(`:998-1013`) overwrites the spool.

At dispatch, in `_do_rhapsody_submit` (`:1760-1831`), **before**
`rh.submit_tasks`, iterating `task.spooled`:

- member has `shared_fs: true` → copy each spooled file into the task's
  assigned cwd (which the dispatcher created, §8);
- member has `shared_fs: false` → `put(src=<spool path>,
  tgt=<cwd>/<name>, overwrite=True)` on the pilot's own `staging` plugin,
  through a new `_get_staging_client(child_endpoint)` that mirrors
  `_get_rhapsody_client` (`:837-863`) — same caller-backed
  `_make_child_client` (`:794-807`), same lazy `register_session`, same
  `(dst, 'staging', None)` cache key, returning `None` when the child is
  unreachable. That `put` is also what creates the directory remotely (§8).

**Spool lifetime.** One method, `ps.drop_spool(task_id)` (rmtree of
`<state_dir>/inputs/<task_id>`, idempotent), called from every terminal
path — `_handle_task_terminal` (`:1883-1933`), `_mark_task_failed`
(`:2040-2050`) and `_cancel_task` (`:2052-2082`) — plus
`rmtree(state_dir / 'inputs')` in `PoolState.close()` (`:214-216`, today a
no-op). The spool must survive a *re-queue* (the task will be dispatched
again, possibly to another member), so it is keyed on the task, not the
pilot, and only a terminal state releases it.

A failure here fails the task with `'could not place inputs on the pilot: …'`
(`_mark_task_failed`, `:2040-2050`) instead of running it with missing files.

This replaces the "client stages in, then the task runs" dance for class
pools: the client no longer has to know *where* the task will run before it
knows where the task will run. The dispatcher's own `stage_in` route stays
for the legacy shared-FS flow (§8).

### 4.4 Submit-time validation (extends 120 §PR1.4 to members)

120 rejects with `400` a task whose `cores`/`gpus` exceed **every**
`PilotSize` in the pool, comparing per node. With members, "every
`PilotSize` in the pool" becomes "every `PilotSize` of every declared
member". **Keep 120's message format**, qualifying the size name with its
member:

```
requirements: 2 gpus exceed every pilot_size (largest: 'bridges.gpu/default', 1 gpu/node)
```

(120's own test asserts the unqualified form; it needs the qualified name
once 121 lands — flagged to that author, it is a one-line fixture change.)

121 adds two rules in the same place:

- reject with `400` when no member can satisfy the task's `software` /
  `labels`, message
  `"no member satisfies the task requirements: software missing: lammps"`.
  A task that no *declared* member could ever run is a client error, and
  queueing it forever is the worse answer. The runtime sweep in §7 covers
  only the case where the capable member left *after* the submit.
- **mpi gate (D6):** reject with `400` iff **every** size of **every** member
  declares `rhapsody_backend: dragon_v1`, message
  `"an mpi task cannot run on this pool: every member's backend is
  dragon_v1"`. When some members can, the task is accepted and
  `pick_dispatch` simply never offers it a `dragon_v1` pilot (§3.3's `mpi`
  rule). Member-level, not pool-level, precisely because a class pool now
  mixes backends.

**Both pool-mode entry points.** The exec-style `_route_submit`
(`:960-1036`) **and** the rhapsody dialect `_route_submit_rh` (`:1097-1170`)
must run this validation: 120 promotes a `requirements` key off each
rhapsody task dict, so a rhapsody-dialect task can be just as unplaceable.
120's author has been told to forward `software`/`labels`; 121 names
`_route_submit_rh` explicitly so neither plan assumes the other covered it.

**Endpoint mode is unchanged**: it has no pool, so it keeps 120's *shape*
`400`s (a malformed `requirements` object) and gets **no** fit check and no
member validation — there is nothing to fit against
(`plugin_task_dispatcher.py:1038-1096`, 120 §PR1.1).

### 4.5 Client (`TaskDispatcherClient`)

- `submit_task` (`:298-330`): `cwd: str | None = None` — omitted from the
  payload when `None`, so a client can let the dispatcher place the task
  (§8); new `requirements` keyword is 120's; new `inputs_b64` keyword (§4.3).
- Two new verbs mirroring the routes:
  `add_member(pool: str, member: dict) -> dict` and
  `remove_member(pool: str, member_id: str, *, cancel_tasks=False,
  force=False, fail_unsatisfiable=True) -> dict`.
- **`inputs_b64` is pool-mode, exec-style only.** Sent with `endpoint=`
  (endpoint mode, `:983-985`) or on a rhapsody-dialect task
  (`_route_submit_rh`) it is a `400 "inputs_b64 is only supported for
  pool-mode exec tasks"` — endpoint mode has no scratch the dispatcher owns
  (`:1264-1268` already refuses staging there), and a rhapsody task's cwd is
  opaque to the dispatcher (§8 rule 2).

### 4.6 What did *not* need a route

`register_session` already adds *pools* to a live session — **confirmed**:
`register_session` parses the declaration, calls the base to reconnect the
sid, then materialises every declared pool (`:912-920`), and
`_materialise_pool` returns the existing `PoolState` for one it already owns
(`:584-587`). Note `parse_pools` rejects an empty `pools` list
(`task_dispatcher_config.py:113-114`), so a re-registration must always carry
the **full** list of the session's pools — which is exactly what plan 08 §6
does. Fix the stale "no add-pool route" claim in
`plans/task_dispatcher_design.md` §3.1, `plans/00-overview.md:52-54` and
`docs/`.

---

## 5. Policy contract v2

`task_dispatcher_policy.py`:

```python
class DispatchPolicy:
    def on_tick(self, pool_state,
                submit_pilot: Callable[..., str]) -> None: ...
    def pick_dispatch(self, pool_state) -> tuple[TaskRecord, PilotRecord] | None: ...
    def on_pilot_state(self, pilot, old_state, new_state) -> None: ...
```

The **callable changes shape, not the method signatures**:

```python
submit_pilot(size_key: str | None = None, *, member_id: str | None = None) -> str
```

`size_key` **keeps position 0** so every existing positional call keeps its
meaning — `submit_pilot(None)`
(`task_dispatcher_strategy_conservative.py:187`) and any out-of-tree
`submit_pilot('big')` are unchanged. `member_id` is keyword-only:
`None` → the pool's primary member (the implicit one for a legacy pool).
`_make_submit_pilot` (`plugin_task_dispatcher.py:1460-1462`) and
`_submit_pilot` (`:1464-1502`) resolve the member first, then
`member.pilot_sizes[size_key or member.default_size]`, raising `KeyError` on
an unknown member in the same style as the unknown-size raise (`:1473-1477`).

Base-class additions on `DispatchPolicy` (`task_dispatcher_policy.py:46-91`):

```python
@property
def max_requeues(self) -> int:      # default 1; see §7
    return 1
```

`_finalize_pilot` reads it off the pool's policy, so the re-queue cap is a
policy decision with a safe default for every policy that does not care.
`ConservativePolicy` overrides it from its `max_requeues` knob.

New read-only helpers on `PoolState` (`plugin_task_dispatcher.py:152-216`),
so a policy never touches `config.members` directly:

```python
ps.members()                      -> list[PoolMember]         # declaration order
ps.member(mid)                    -> PoolMember | None        # '' → implicit
ps.live_pilots_for(mid)           -> list[PilotRecord]
ps.member_node_hours(mid, now)    -> float
ps.member_budget_left(mid, now)   -> float | None             # None = no budget declared
ps.size_of(pilot)                 -> PilotSize | None         # snapshot first, then member menu
```

`ps.size_of` prefers the pilot's own size snapshot (§3.3) and falls back to
`member.pilot_sizes[pilot.size_key]`; it replaces the two places that index
the pool-level size menu (`_activate_pilot:1423`, `_build_job_spec` call site
`:1576`). **The fake `PoolStateHandle` in
`test_task_dispatcher_strategy_conservative.py:49-68` must grow all six** —
that, plus the `submit_pilot` keyword, is the whole test-side change.

### `ConservativePolicy` v2 (`task_dispatcher_strategy_conservative.py`)

Existing knobs keep their names, defaults and meaning
(`min_dwell_sec` 30, `max_in_flight_submissions` 2, `router_preference`
`least_loaded|youngest`, `max_consecutive_failures` 3,
`failure_backoff_sec` 60). Two new ones:

- `member_preference`: `'budget'` (default) | `'least_loaded'`
- `max_requeues`: int = 1  (§7)

**Per-member bookkeeping.** `_last_submit_ts` becomes
`dict[member_id, float]`; `in_flight_subs` and the `max_pilots` ceiling are
counted per member; `_consecutive_failures` / `_backoff_until` become
per-member dicts keyed off `pilot.member_id` in `on_pilot_state`
(`:97-123`). For a single-member pool every one of these is arithmetically
identical to today, so `test_task_dispatcher_strategy_conservative.py` passes
with only its fake `submit_pilot` signature updated (`:68`).

**`on_tick` (rewrite of `:127-196`), one submission per tick, in order:**

1. `pending = pool_state.pending_queue()`; per-member live counts.
2. **Floor.** First member (by declaration order) with
   `len(live_pilots_for(m)) < m.min_pilots` → candidate, reason `min_pilots`.
3. **Backlog.** Otherwise compute, per pending task, whether any *live*
   pilot could ever take it (`satisfies(task.requirements, pilot.attributes,
   size)` ignoring free capacity). Let `unservable` be the tasks for which
   none can, and `free_capacity` the sum over ACTIVE pilots that *can* serve
   at least one pending task. Scale up when `unservable` is non-empty **or**
   `len(pending) > free_capacity` — reason `backlog`.
   Candidate members = those where
   `satisfies(req_of_oldest_unservable_or_head_task, m.attributes,
   m.pilot_sizes[m.default_size]) is None`, minus members at
   `max_pilots`, minus members with a declared budget whose
   `member_budget_left(m) <= 0`, minus members in failure backoff.
4. Rank candidates: `member_preference == 'budget'` → most remaining
   node-hours first (a member with no declared budget sorts as `+inf`,
   i.e. after members with headroom? **no** — it sorts *last*, so a declared,
   funded allocation is preferred over an unbounded one only when the
   unbounded one is busier; concretely the key is
   `(-budget_fraction, live_pilots, member_id)` with
   `budget_fraction = 1.0` when no budget is declared);
   `'least_loaded'` → `(live_pilots, -budget_fraction, member_id)`.
   Ties always break on `member_id` — deterministic.
5. Guards, now per member: failure backoff, `max_in_flight_submissions`,
   `member.max_pilots`, `min_dwell_sec` since that member's last submit.
6. `submit_pilot(None, member_id=m.member_id)`; log with `member=` added to
   the existing line (`:189-193`).

**`pick_dispatch` (rewrite of `:198-224`).**

```
pending = [QUEUED tasks]  sorted (-priority, arrival_ts)      # as today, :211
for task in pending:
    cands = [p for p in live_pilots if p.state == ACTIVE
                 and p.free_capacity() > 0                    # unchanged slot test (§1.1)
                 and satisfies(task.requirements, p.attributes,
                               ps.size_of(p)) is None]
    if not cands: continue                                    # ← NEW: skip, don't block
    sort cands by router_preference (unchanged, :218-222),
         then by member_id for determinism
    return task, cands[0]
return None
```

**Deliberate semantic change:** today a top-priority task with no pilot
returns `None` and stalls the whole drain (`:212-216`). In a class pool a GPU
task must not block CPU tasks, so an unservable task is skipped. The drain
loop is still bounded by the pending-queue length
(`_drain_pending:1715-1718`), so the cost is O(pending²) worst case at
demo scale (tens) — acceptable; documented.

---

## 6. Pilots

- `_submit_pilot` (`:1464-1502`) takes `member_id`, stamps
  `record.member_id`, `record.attributes = dict(member.attributes)` and the
  `nodes`/`cpus_per_node`/`gpus_per_node` snapshot (§3.3) — none of which
  ever changes afterwards, even if the member is re-declared — and sizes
  from `member.pilot_sizes`.
- `_do_pilot_submit` (`:1546-1604`): `endpoint_name`, `queue` sentinel check
  (`:1559-1564`) and `account` all read from **the member**, not the pool.
- **Child endpoint name** (`:1572`):
  - non-`multi_member` pool → `f'{pool}_{pid}'` — byte-identical to today,
    so every existing test and deployment is untouched. Since a pool is
    never promoted (§4.1), the implicit member `'_'` never appears in a
    child-endpoint name;
  - class pool → `f'{pool}_{member_id}_{pid}'`.
  Encode this in one helper `child_endpoint_name(pool, member_id, pid)` so
  the federation and the campaign runner can reproduce it as a fallback.
- `_build_pilot_env` (`:1504-1517`) adds
  `RADICAL_ORBIT_MEMBER = member_id` and sets
  `RADICAL_ORBIT_SCRATCH_BASE` from **the member's** `scratch_base` (falling
  back to `PoolState.scratch_base`). This — and the cwd assignment in
  `_claim` (§8) — are the **only** two readers of `member.scratch_base`;
  nothing on the broker host ever `mkdir`s it (§8).
- `_build_job_spec` (`:1519-1544`) reads `queue_name`/`project` from the
  member.
- `_activate_pilot` (`:1421-1432`) uses the pilot's size snapshot, falling
  back to `ps.size_of(pilot)` and repairing the record (§3.3).
- `_do_pilot_cancel` / `_reconcile_pilot` (`:1606-1648`) resolve the psij
  client from **`pilot.endpoint_name`** (the snapshot, §3.3), falling back to
  `ps.member(pilot.member_id).endpoint_name` and then
  `ps.config.endpoint_name` for a pre-121 record with an empty snapshot.
  They must **not** look the member up first: §7 removes the member before
  awaiting the cancels, so a member lookup would return `None` exactly when
  the cancel matters most.

---

## 7. Member removal, draining, re-queue

Tasks are pool-level and bind to a pilot only at dispatch
(`_claim:1751-1758`) — so removing a member never orphans a queued task.

`DELETE …/members/{member_id}`:

**Order matters — drop the member first, then cancel** (BLOCKING fix). The
cancels are `await`ed (psij round-trips); a housekeeping tick landing in that
window would see the member still declared, still below its `min_pilots`
floor, and submit a *replacement* pilot for the member being removed. So:

1. **Synchronously, before the first `await`:** set
   `accepting_new_tasks = False` on every live pilot of the member
   (`PilotRecord:94`; `free_capacity()` returns 0 for a paused pilot,
   `task_dispatcher_state.py:118-122`), then drop the member from
   `config.members` and `ps.persist()`. Dropping the member stops the
   *policy* from growing it; pausing its pilots stops a `_drain_pending`
   from any other path — a task arriving on the event loop while the cancels
   are in flight — from dispatching onto a pilot that is about to die. There
   is no `draining` flag on the member: it is gone, which is the same
   guarantee with less state. (`_do_pilot_cancel` and `_reconcile_pilot`
   resolve the endpoint from the *pilot* snapshot, not the member — §6 — so
   cancelling a removed member's pilots still works.)
2. For each live pilot with that `member_id`: `await _do_pilot_cancel(...)`
   → `_mark_pilot_failed` → `_finalize_pilot` (`:1664-1703`) cancels the psij
   job and **re-queues** its non-terminal tasks (verified `:1688-1697`).
3. `_finalize_pilot` gains three things on the re-queue branch
   (`:1688-1697`): `t.requeues += 1`; **`t.member_id = None`** (it is set at
   dispatch beside `pilot_id`, §8, and must be cleared with it or a
   re-queued task reports a placement it no longer has); and, when
   `t.requeues > ps.policy.max_requeues` (base-class property, default 1,
   §5), the task is failed with
   `error = 'requeued too often (pilot lost)'` instead of re-queued. This is
   the "re-queued once" rule from the decision, and it also protects the
   pre-existing pilot-loss path. The task's **spool survives** (§4.3) — it
   is about to be dispatched somewhere else.
4. **Unsatisfiable sweep**, when `fail_unsatisfiable` is true (the default,
   §4.2) — the runtime counterpart of §4.4's submit-time `400`, catching
   tasks whose only capable member just left. For every QUEUED task, if no
   *remaining* member can ever serve it (`satisfies(req, m.attributes,
   default size)` fails for all `m`), fail it with
   `error = 'no member satisfies the task requirements: <reason>'`
   (`_mark_task_failed`, `:2040-2050`) — otherwise a task whose only
   `software` provider just left would sit QUEUED forever. With
   `fail_unsatisfiable: false` those tasks stay QUEUED and wait for the
   member to come back (the federation's liveness path, plan 08 §6).
   `cancel_tasks: true` fails *every* task that was re-queued off this
   member's pilots, satisfiable or not.
5. **`_drain_pending(ps)` — explicitly, at the end** (BLOCKING fix).
   `_finalize_pilot` re-queues tasks but does **not** drain
   (`:1688-1703`); today the only caller that drains after a pilot loss is
   the topology path (`_reconcile_pilots_for:1419`). Without this call the
   re-queued tasks wait up to one housekeeping tick
   (`_TICK_INTERVAL_SEC`) before they can land on a sibling member — and
   the harness test for "member removal drains" would be timing-dependent.
6. Notify `task_status` for each changed task (already done by
   `_finalize_pilot`; the sweep must do the same).

Session teardown (`_teardown_session_pools:2151-2179`) needs no change — it
already walks every pilot of every pool of the session.

---

## 8. Staging and task cwd — the honest part

Today `cwd` is required at submit (`:977-980`) and the pool scratch is
assumed shared with the pilot (`00-overview.md:110-113`). In a class pool the
member — and therefore the filesystem — is unknown at submit time.

**`PoolState.scratch_base` is always broker-local** (BLOCKING fix).
`PoolState.__init__` `mkdir`s `scratch_base` on the broker host
(`:174`) and `_scratch_for` derives it from `cfg.scratch_base` (`:563-567`)
— which, for a class pool, is the *projection of the primary member's* path,
i.e. a path on someone else's filesystem. So:

```python
def _scratch_for(self, cfg):
    if cfg.multi_member:
        return self._scratch_root / cfg.name        # broker-local, always
    return (Path(cfg.scratch_base).expanduser() if cfg.scratch_base
            else self._scratch_root / cfg.name)     # unchanged legacy path
```

`PoolState.scratch_base` is thereafter the pool's **broker-side** tree (the
input spool of §4.3 lives under the state dir, and shared-FS task cwds live
here). `member.scratch_base` is a path *on the member's host* with exactly
two readers: `_build_pilot_env` (§6) and the cwd assignment below. The
broker `mkdir`s a task cwd **only when the placed member declares
`shared_fs: true`** — never for a `shared_fs: false` member, whose
`scratch_base` names a directory on another machine.

**Rules:**

1. **Explicit `cwd` in the submit body** → used verbatim, as today. Refused
   with `400 "explicit cwd is not valid for a pool with non-shared members"`
   when any member of the pool has `shared_fs: false`.
2. **No `cwd`** — now permitted for `multi_member` pools (reorder the
   validation at `:977-980` so the pool is resolved before the `cwd` check;
   `TaskDispatcherClient.submit_task` takes `cwd: str | None = None`, §4.5).
   The record is created with `cwd=''` and `cwd_assigned=True` (§3.3); the
   dispatcher assigns
   `cwd = <member.scratch_base or PoolState.scratch_base>/<task_id>` **at
   dispatch** in `_claim` (`:1751-1758`), together with
   `task.member_id = pilot.member_id`, then persists (the drain already
   persists once, `:1743`) and notifies `task_status`. On a **re-dispatch**
   after a pilot loss `_claim` re-assigns the cwd iff `cwd_assigned` — a
   task moving from a shared to a non-shared member (or between two members
   with different scratch roots) must not carry the old member's path — and
   leaves a client-supplied cwd (`cwd_assigned == False`) alone. Verified
   consumer-side: the campaign runner re-reads `task_dict['cwd']` on every
   poll (`atomic_wm/campaign/runner.py:551-552`), so a late cwd is already
   handled.
   **Rhapsody-dialect tasks are excluded**: `_route_submit_rh`
   (`:1097-1170`) carries `cwd` inside the opaque `task_dict`, which is
   forwarded verbatim (`:1786-1787`) and never rewritten. They keep
   requiring a cwd from the client, and the 400 message says so. (Their
   `requirements` *are* validated, §4.4.)
3. **Directory creation.** `shared_fs: true` → the dispatcher creates the
   assigned cwd itself, in `_claim`, with
   `Path(task.cwd).mkdir(parents=True, exist_ok=True)`. Note this is *not*
   `PoolState.task_scratch_dir` (`:203-207`), which hard-codes
   `scratch_base / task_id`: the assigned cwd may sit under a member's
   `scratch_base` instead, so the mkdir is on the resolved path.
   `shared_fs: false` → the dispatcher creates **nothing** locally; the
   `staging.put` of the task's inputs (§4.3) creates the directory on the
   pilot. A task with no inputs on a non-shared member gets one explicit
   `put` of a zero-byte `.orbit-cwd` marker to the same effect — cheaper
   than adding a `mkdir` route to the staging plugin, which has only
   `put`/`get`/`list` (`plugin_staging.py:377-379`).
4. **Dispatcher `stage_in` / `stage_out`** (`:1257-1338`). Note
   `_route_stage_in` never looks up a `TaskRecord` today — it stages into
   `pool_state.task_scratch_dir(task_id)` for *any* id, and
   `test_stage_in_out_roundtrip` relies on that (it stages before any
   submit). So the new refusals are **conditional on a record existing**:
   - no `TaskRecord` for `task_id` → unchanged legacy behaviour;
   - record exists with `cwd == ''` → `409 "task not yet placed"`;
   - record exists and its member has `shared_fs: false` → `409 "staging
     for this task is not broker-local; use the pilot's staging plugin at
     <child_endpoint>"`;
   - otherwise unchanged, except both routes use `Path(rec.cwd)` when a
     record exists instead of recomputing `ps.scratch_base / task_id`
     (`:1295`, `:1324`) — a straight bug fix for any task with an explicit
     cwd.

This is the one place the decision costs real work: **result collection for
non-shared members is only possible through the pilot's staging plugin**, and
that path exists today only in the ATOMIC campaign plugin
(`atomic_wm/campaign/runner.py:611-635` — `pilot_staging` is already the first
of its three fallbacks). Inputs are solved for everyone by §4.3; outputs are
not. 121 documents the dispatcher's staging routes as *broker-local only* and
does not build a generic pull-through proxy; see §14 risk R3.

---

## 9. Verbose pool summary and accounting

**This section is frozen contract**: plan 08's `_refresh_usage` and the
Explorer both read the member dict verbatim, and 08 is being written against
it. Adding a key later is fine; renaming or removing one is not.

`_summarize_pool` (`:2107-2147`) keeps every existing key (the flat
`pilot_sizes`, `queue`, `account`, `endpoint_name`, `min/max_pilots` stay as
the primary-member projection — `federation.js` and `task_dispatcher.js` keep
working) and gains:

```jsonc
"pool_class": "gpu",
"multi_member": true,
"members": [
  {"member_id": "perlmutter", "endpoint_name": "ep_pm",
   "queue": "regular", "account": "m1234",
   "attributes": {...}, "budget": {"node_hours": 40.0},
   "min_pilots": 0, "max_pilots": 2, "shared_fs": false,
   "pilot_sizes": {...}, "default_size": "default",
   "live_pilots": 1, "pilots_active": 1,
   "node_hours_used": 1.25,          // server-side, see below
   "node_hours_remaining": 38.75,    // null when no budget declared
   "pilot_history": [ <pilot dicts for this member only> ]}
],
"node_hours_used": 3.10               // pool total, all members + departed ones
```

- Non-verbose summaries gain `pool_class`, `multi_member`,
  **`member_ids: [<id>, …]`** (a list of strings — named apart from the
  verbose `members`, which is a list of *objects*, so no consumer has to
  discover the type by inspection) and `max_pilots_total`
  (`Σ member.max_pilots` — the Explorer header needs it and `GET pools` must
  stay cheap).
- Pool-level `pilot_history` stays and every entry now carries `member_id`,
  `attributes` and its size snapshot — an existing consumer that ignores
  members still gets the correct pool total, **including pilots whose member
  has since been removed** (they are in the pool history but in no member's
  list; that is why the pool total is reported separately rather than being
  a sum of the member figures).
- `node_hours_used` per member is computed **server-side** by
  `node_hours(member_history, now=now)` (§3.3 — the size comes from each
  entry's own snapshot, so a mixed-node-count pool is correct and a departed
  member's pilots still size themselves). The Explorer and the federation
  read one number rather than each re-implementing the arithmetic.
  `federation_state.node_hours_from_history` keeps working unchanged for
  legacy single-member pools.

---

## 10. Persistence and replay

- `PoolState.persist` (`:209-212`) writes `self.config.to_dict()` (§3.1),
  **not** `asdict(self.config)`: a legacy pool must not persist its
  synthesised implicit member. `members`, `pool_class` and `multi_member`
  ride along for a class pool.
- `_replay_state` (`:609-646`) re-parses through `parse_pools` — the parser
  must therefore accept its own output, for **both** shapes. **Round-trip
  test required for each** (§13).
- **Old state files** (no `members` key, no `multi_member`) parse exactly as
  today (`multi_member` defaults to `'members' in d` = false) and gain one
  implicit member from `__post_init__`. Old `PilotRecord`s get
  `member_id=''` → implicit member, a zero size snapshot repaired at the
  next handshake (§3.3), and their stored `child_endpoint_name` untouched
  (`_reconcile_pilots_for` matches on the stored name, `:1391-1396`) — so
  pilots survive the upgrade.
- **Pool state directory — pass it in, do not recompute it** (BLOCKING fix).
  `_pool_dir` (`:558-561`) is called by `_materialise_pool` (`:589`) *and*
  implicitly by `_replay_state`, which walks the directories it finds
  (`:619-632`). Any rule that derives the directory from mutable config —
  `endpoint_name`, or a member list — can therefore diverge between the two
  and silently lose a pool's pilots and tasks. Fix both ends:
  - `_materialise_pool(self, sid, cfg, state_dir: Path | None = None)`;
    `_replay_state` passes the `pool_dir` it is iterating over, so replay
    always attaches to the file it just read, whatever the naming rule.
  - `_pool_dir` (used only when `state_dir is None`, i.e. a fresh
    declaration) becomes
    `tag = 'members' if cfg.multi_member else (cfg.endpoint_name or 'unbound')`.
    A legacy pool keeps its exact directory; a class pool gets one that does
    not move when its primary member leaves.
- `_materialise_pool` must **skip the auto-endpoint pick** (`:577-582`) when
  `cfg.multi_member` — `endpoint_name` is a projection there, not a binding.
  For a legacy pool the pick writes through `cfg.bind_endpoint(name)` so the
  implicit member is bound too (§3.1).
- **Re-declaration still does not merge members** (`:584-587` unchanged): the
  federation re-registers its session with the pool declarations *and then*
  POSTs every member (idempotent no-op when the dispatcher already replayed
  them). This is the required replay protocol and it is stated in plan 08 §6.

---

## 11. Explorer — `data/plugins/task_dispatcher.js`

`renderPoolCard` (`:137-180`):

- Header gains a class badge next to the strategy badge (`p.pool_class`) and
  reads `live / Σ member.max_pilots` when `p.multi_member`.
- The single `td-pool-meta` line (queue/account/min-max, `:168-172`) is
  replaced, for multi-member pools, by a **members table**: `member`,
  `endpoint`, `queue`, `account`, `attributes` (chips: `site=…`,
  `software: a, b`), `pilots live/max`, `node-hours used/left`.
- Each member row expands (a `<details>` element — no framework) into that
  member's size table, reusing the existing `sizeRows` renderer and
  `formatWalltime` (`:182-188`).
- Single-member pools render exactly as today (branch on `p.multi_member`).
- `GET pools` is non-verbose, so the members table needs `member_ids` + the
  summary fields listed in §9; the per-member `node_hours_used` requires the verbose
  route — fetch `pool/{sid}/{name}` lazily on expansion.
- Explorer caches plugin JS until a miss: **restart the broker after editing**
  (`00-overview.md:163-166`).

---

## 12. Docs to update

| File | Change |
|---|---|
| `docs/task_dispatcher_strategy.md` | §"The ABC" (`:55-75`) and §"Invocation contract" (`:76-90`) are already stale vs. the code — rewrite them against the real `DispatchPolicy` and the v2 `submit_pilot(size_key, *, member_id)`; document the pick-skip semantics and the new knobs `member_preference`, `max_requeues` in §"conservative" (`:114-156`) |
| `docs/rest_api.md` | the two member routes; `submit`'s optional `cwd` and its `inputs_b64` block (with the size cap and the `413`); the new `stage_in`/`stage_out` 409s |
| `plans/task_dispatcher_design.md` | §3.1 "Pool" (`:125-147`), §3.4 cardinality (`:191-215`), §8.1 pool config (`:526-559`), §9 persistent state (`:586-632`): pool = class, pool 1─N member 1─N pilot; correct the "no add-pool route" claim |
| `docs/plugin_federation.md` | §"Dispatcher changes this plugin required" (`:313-331`) — add the member routes; the rest is plan 08's business |
| `docs/architecture.md` | one paragraph if it names pools as site-bound |

---

## 13. Tests

Unit, per module (all under `tests/unittests/`, existing files):

- `test_task_dispatcher_config.py`: members parse (list and map form);
  duplicate `member_id` → error; bad `member_id` charset; `IMPLICIT_MEMBER`
  accepted; member with the `'default'` queue sentinel → error; member
  `max_pilots = 0` → error; legacy declaration → exactly one implicit member
  + `multi_member is False`; a legacy declaration that *also* carries a
  `members` key ignores it; an explicit `multi_member: false` beside a
  `members` key still parses legacy; `multi_member: true` with an empty
  `members` → error **from `register_session`** but **accepted with
  `allow_empty_members=True`** (the replay path); projection equals the
  primary member; **`parse(cfg.to_dict())`
  round-trips for a legacy pool AND for a class pool** (the replay path);
  `to_dict()` of a legacy pool has no `members` key; `PoolConfig(...)` built
  directly (no parser) has its implicit member; `default_pool_config()` has
  it too; a non-matching `pool_class` is rejected, not coerced.
- `test_task_dispatcher_state.py`: `PilotRecord` round-trip with
  `member_id`/`attributes`/`endpoint_name`/size snapshot; old dict without
  them loads with defaults;
  `TaskRecord.requeues`/`requirements`/`member_id`/`spooled`/`cwd_assigned`
  round-trip;
  `node_hours()` moved-in tests — from the snapshot, from a legacy
  `size_key` + `pilot_sizes`, and **a mixed-node-count history where the two
  give different totals** (keep the existing federation tests green through
  the re-export).
- **new** `test_task_dispatcher_match.py`: every rule in §3.3 — software
  subset, cores/gpus vs size, `mem_gb` present/absent, labels equal and
  list-membership, `≤0` requirement always satisfied, undeclared label
  rejects, `mpi` vs a `dragon_v1` backend, **keys outside the table
  (`ranks`, an unknown one) are ignored, not rejected**, reason strings.
- `test_task_dispatcher_strategy_conservative.py`: extend the fake
  `PoolStateHandle` (`:49-68`) with members; **all existing single-member
  tests must pass with only the fake `submit_pilot` signature updated**;
  new: floor honoured per member (two members, `min_pilots=1` each → two
  ticks, one submit each); backlog grows the member whose attributes match
  the pending task (task `software=[lammps]`, member A has it, B does not →
  A grows); budget-exhausted member is skipped; per-member dwell (member A
  inside its window does not block member B); `member_preference` ordering
  and `member_id` tie-break; `pick_dispatch` skips an unservable head task
  and dispatches the next one; `pick_dispatch` never returns a pilot whose
  attributes miss the task's software.
- `test_plugin_task_dispatcher.py`: submit `400`s when no member satisfies
  `software` (§4.4) and when `gpus` exceeds every member's size, both with
  the member named in the detail; the same for a `_route_submit_rh` task;
  submit succeeds and queues when one member matches; `POST members` creates
  / is idempotent on an identical re-POST / 409s on a differing
  re-declaration / **409s on a non-class pool** / 404s on unknown pool;
  `DELETE members` (flags in the **body**) cancels that member's pilots,
  re-queues their tasks, fails the now-unsatisfiable ones, 409s on the last
  member without `force`, and **drains** (a sibling member's ACTIVE pilot
  picks the re-queued task up without waiting for a tick);
  `cwd` optional for a class pool and assigned at dispatch; a rhapsody-dialect
  task still requires a cwd; `stage_in` with **no record** behaves exactly as
  today, `stage_in`/`stage_out` 409 for an unplaced task and for a non-shared
  member; `inputs_b64` spools into `<state_dir>/inputs/<task_id>/` and lands
  in `TaskRecord.spooled` (not `inputs`), is copied into a shared-FS cwd and
  `put`-ed to a non-shared member (fake staging client), a failure there
  fails the task, a bad base64 is a `400`, an oversize file a `413`, and it
  is a `400` in endpoint mode and on a rhapsody-dialect task; the spool is
  dropped on every terminal path but **survives a re-queue**; a re-dispatched
  task re-assigns its cwd iff `cwd_assigned`; an mpi task is `400`-ed only
  when every member is `dragon_v1`; a class pool's `PoolState.scratch_base` is
  under `<scratch_root>/<pool>` even when the primary member's
  `scratch_base` is `/gpfs/...`; the verbose summary's `members` block,
  per-member and pool-total `node_hours_used`, `max_pilots_total` in the
  non-verbose one; `requeues` cap fails a task on the second pilot loss.
- `test_task_dispatcher_recovery.py`: a state file written by a class pool
  replays with its members and pilots; a **pre-121** state file replays into
  one implicit member with its live pilot intact; a legacy pool
  **persist → replay → persist** cycle is stable (finding 1's regression);
  a class pool replays from the directory it was found in after its primary
  member changed; an **emptied** class pool (last member removed with
  `force`) replays instead of being skipped as unparseable.

Co-hosted harness (`test_task_dispatcher_broker.py`, fake psij `:71-97` and
fake pilot `:59-70`, `make_runtime` `:158-185`) — the acceptance tests.
**The harness needs one addition before any of them can run**: today's
`_FakePilot` (`:59-70`) only answers a `ping`, and nothing in the file ever
takes a task through `_do_rhapsody_submit`. Each test must therefore
(i) add a fake `rhapsody` plugin exposing `register_session` and
`submit_tasks` (msgpack bodies, mirroring `_FakePsij` at `:71-97`) plus a
`staging` plugin on the same fake pilot — the real one is in the default
plugin set (`plugin_host_base.py:23`), so either host it for real or fake
`put`/`get`/`list`; test 5 asserts against it — and
(ii) start a `make_runtime` endpoint under the exact
`child_endpoint_name` the dispatcher recorded — read it out of
`ps.pilots[pid].child_endpoint_name` after the fake psij accepted the
submission, rather than hard-coding the name (which is precisely what §6
changed). Budget for this in WP J.

1. **Routing by attribute.** One pool `fed-gpu` with two members `m_x`
   (`software: [x]`) and `m_y` (`software: [y]`), a fake pilot per member.
   Submit `software=[x]` → lands on `m_x`'s pilot; submit `software=[y]` →
   `m_y`'s. Assert `task.member_id` and the pilot's `child_endpoint_name`
   prefix.
2. **Scale-up picks the right member.** Empty fleet, one task with
   `software=[y]` → exactly one pilot submitted, for `m_y`.
3. **Member removal drains.** With a task RUNNING on `m_x`, `DELETE m_x` →
   its pilot FAILED, the task back to QUEUED with `requeues == 1`, then
   dispatched to `m_y` when it can serve it, or FAILED with
   `no member satisfies…` when it cannot.
4. **Budget per member.** Two members, one with an exhausted
   `node_hours` budget → scale-up chooses the other; the verbose summary
   reports the two `node_hours_used` figures independently.
5. **Inputs reach a non-shared member.** Submit with `inputs_b64` to a
   member declared `shared_fs: false`; assert the fake pilot's staging
   plugin received the `put` at `<cwd>/<name>` **before** `submit_tasks`,
   and that the dispatcher created nothing under the member's
   `scratch_base` on the broker host.

Green bar required: `PYTHONPATH=src ve3/bin/python -m pytest
tests/unittests/ -q` (1258+ today) and `ve3/bin/flake8 src/ bin/` clean.

---

## 14. Work packages, effort, risks

| WP | Content | Depends on | Effort |
|---|---|---|---|
| A | `PoolMember`, `PoolConfig` fields + `__post_init__` + `to_dict`, parser, projection, round-trip tests | — | 4 h |
| B | `task_dispatcher_match.py` + tests; `node_hours` move + snapshot-first sizing + re-export | — | 3 h |
| C | `PilotRecord`/`TaskRecord` fields incl. the size + endpoint snapshot and `spooled`/`cwd_assigned`, `PoolState` member helpers + broker-local scratch, `_materialise_pool(state_dir=…)`, replay | A | 4 h |
| D | Member routes + `add_route_delete` + client verbs + notifications | A, C | 3 h |
| E | Pilot path: `_submit_pilot`, `_do_pilot_submit`, naming, env, job spec, cancel/reconcile | C | 3 h |
| F | `ConservativePolicy` v2 + policy contract (`max_requeues`) + docs §12 row 1 | A, B, C | 4 h |
| G | cwd-at-dispatch + `cwd_assigned`, `inputs_b64` spool + caps + `_get_staging_client` + placement + `drop_spool`, staging 409s | C, E | 6 h |
| H | Verbose summary + per-member and pool-total accounting | C | 2 h |
| I | Explorer `task_dispatcher.js` | H | 2 h |
| J | Harness: fake rhapsody + staging plugins, child-endpoint runtime, tests 1-5 | D, E, F, G | 6 h |
| K | Docs (§12 rows 2-5) | all | 2 h |

≈ **39 h**. Three parallel tracks after A+C land: (F) policy, (D+E+G)
plumbing, (H+I) surfacing.

**Sequencing with 120 and 08** — see plan 08 §14; in short: 120 lands
`TaskRecord.requirements`, its validator (incl. the two extra keys of §1.1)
and the rhapsody mapping first — or 121 lands the field under the same name
and 120 rebases. **§3 (schema), §4 (routes) and §9 (the verbose member dict)
are frozen** — review rounds 1 and 2 are folded in — so plan 08 can be built
against a fake dispatcher API; integration last. Adding a key to any of the
three is fine; renaming or removing one needs a plan revision and a word
with whoever is writing 08. Neither plan implements reservation or pinning
this round (§1.1).

### Risks

- **R1 — 120 collision.** Both plans touch `TaskRecord`, the submit
  validation and `_do_rhapsody_submit`. Mitigation: 120 owns
  `TaskRecord.requirements`, its key whitelist (+ the two keys of §1.1) and
  the rhapsody mapping; 121 owns `member_id`/`requeues`, extends 120's
  validation across members (§4.4), and only *reads* requirements through
  `task_dispatcher_match.satisfies`. Whoever lands second rebases.
- **R1b — no reservation (deferred, §1.1).** Two GPU tasks can be dispatched
  to the same one-GPU pilot: the slot test counts tasks, and rhapsody's
  `concurrent`/`dragon_v3` backends do not enforce the forwarded
  requirements. This is a known, accepted gap for this round — the demo's
  synthetic GPU work does not touch a GPU. Do not paper over it with a
  half-reservation in the policy; it is one Orbit TODO, tracked against
  120's PR2.
- **R2 — `pick_dispatch` skip changes fairness.** A low-priority task that
  fits can now run before a high-priority one that does not. Correct for
  class pools, but it *is* a priority-inversion. Documented; a future
  `strict_priority: bool` knob is the escape hatch.
- **R3 — staging across hosts.** The dispatcher's own `stage_in`/`stage_out`
  stay broker-local; anything cross-host must use the pilot's staging plugin.
  For the demo this is already the campaign plugin's first path
  (`runner.py:611-635`), so nothing regresses; a generic Orbit client
  submitting to a non-shared class pool has no result-collection route from
  the dispatcher. Stated as a known limitation, not fixed here.
- **R4 — attribute drift.** A pilot's attributes are a snapshot; re-declaring
  a member with new `software` does not retro-fit running pilots. Intended
  (the pilot's node really does have the old software), but it must be in the
  docs or it reads as a bug.
- **R5 — per-member dwell weakens the global submission throttle.** N members
  can each submit within one dwell window, so a 5-member pool can put 5
  pilots in flight where the old pool put 1. Mitigation: keep
  `max_in_flight_submissions` *also* as a pool-level ceiling, spelled out
  because the round-1 wording was ambiguous:

  ```
  pool_ceiling = max(max_in_flight_submissions, len(members))
  ```

  Every member may always have **one** submission in flight (otherwise a
  class pool would warm up more slowly than the single-member pools it
  replaces), and a pool with fewer members than the knob still gets the
  knob. With the federation's `max_in_flight_submissions: 1` and 3 members
  the ceiling is 3; with one member it is 1 — exactly today's behaviour.
- **R6 — `member_id` in the child endpoint name.** Endpoint names must stay
  unique and charset-safe; `MEMBER_RE` enforces it, but a very long
  member id plus a long pool name makes an unwieldy name. Cap the combined
  length at 64 chars at member-declaration time.

### Rejected alternatives

- **Diff members on `register_session` re-declaration** (no new routes).
  Rejected: it silently changes the meaning of a re-declaration
  (`:584-587` is relied on for idempotent reconnect), and a stale replay
  declaration could drain live members.
- **One pool per member + a router above the dispatcher** (today's design).
  Rejected by the decision: it makes cross-site backfill and
  attribute-aware scale-up a federation concern, duplicating the dispatcher's
  queue.
- **Derive the class inside the dispatcher from `gpus_per_node`.** Rejected:
  the class must be an explicit, extensible field (`pool_class`) so future
  classes (`fed-largemem`, `fed-quantum`) need no dispatcher change.

---

## Implementation notes (deviations from the plan as written)

Implemented on `feature/class-pools` (worktree `radical.orbit-cp`), stacked
on `feature/task-requirements` (plan 120).  Recorded so a reader of the
diff is not surprised.

### Branch reality: there is no federation in this tree

This branch is `devel`-based, so `plugin_federation.py`,
`federation_state.py`, `federation_policy.py`, `docs/plugin_federation.md`
and `data/plugins/federation.js` **do not exist** here.  Consequences:

- **§3.3 `node_hours` is a new function, not a move.**  It is written into
  `task_dispatcher_state.py` as
  `node_hours(history, pilot_sizes=None, now=None)` with exactly the
  resolution order the plan specifies (`entry['nodes']` → `pilot_sizes[…]`
  → skip).  There is no `federation_state` re-export to add; when the two
  branches merge, `federation_state.py` should become
  `from .task_dispatcher_state import node_hours as node_hours_from_history`
  and its own copy deleted.
- **§9 `pilot_history` did not exist here either** (it is a fed-branch
  addition).  Since §9 is frozen contract and per-member accounting is
  impossible without it, it is implemented: `PilotRecord.finished_at` is
  new, set in `_finalize_pilot`, and the verbose summary carries both a
  pool-level and a per-member `pilot_history` of `asdict(PilotRecord)`
  views ordered by `submitted_at`.  Expect a textual merge conflict with
  the fed branch's version of the same two things.
- **§11/§12 federation items are out of scope here**: `federation.js` and
  `docs/plugin_federation.md` have nothing to update.  `docs/architecture.md`
  never describes pools as site-bound, so it needed no change.

### Error strings: legacy stays byte-identical

The plan (§4.4) says the "largest" size name becomes member-qualified and
that 120's exact-string fixtures change accordingly.  Implemented
**conditionally**: the name is qualified (`'m_y/d'`) only for a class pool,
and a legacy pool keeps the bare size key.  Rationale, in order of weight:

1. The hard rule "legacy behaviour byte-identical" outranks the fixture
   note, and 120's fit-message tests are written against a *legacy* pool.
2. It keeps the diff to 120's files minimal while a review of 120 runs
   concurrently in another worktree — no 120 test fixture changed.
3. `'_/s'` would leak the implicit-member sentinel into a user-facing
   message for every single-site pool, which reads as a bug.

Same treatment for the mpi gate: a class pool gets the plan's new string
(`an mpi task cannot run on this pool: every member's backend is
dragon_v1`, with the backend list derived rather than hard-coded), a legacy
pool keeps 120's `requirements: 'mpi' is unsupported on dragon_v1 (pool
'x', size 's')`.  The *condition* is identical in both cases ("every size
of every member"), which for one implicit member is exactly the pre-121
test.  Flipping this to unconditional qualification is a two-line change
(drop the `qualify` branch) plus the 120 fixtures.

### The attribute gate is class-pool only

§4.4's `no member satisfies the task requirements: …` fires only when
`multi_member` is true.  A legacy pool's implicit member declares no
attributes at all, so the gate would reject every `software`/`labels`
requirement 120 explicitly accepts and persists — and
`test_requirements_round_trip_through_get_task` submits exactly that to a
legacy pool and asserts a 200.  Note the runtime consequence, which is
intended and documented in `docs/rest_api.md`: `satisfies` *is* applied
uniformly at dispatch, so a `software` requirement on a legacy pool matches
no pilot.  Class pools are how software requirements are meant to be used.

### Smaller deviations

- **`_submit_pilot` signature.**  `_make_submit_pilot` returns a real
  closure (`def _submit(size_key=None, *, member_id=None)`) rather than a
  lambda, because `member_id` must be keyword-only.  `_submit_pilot` itself
  takes `member_id` as an ordinary third parameter (it is dispatcher
  -internal; only the policy-facing callable is keyword-only).
- **`RADICAL_ORBIT_MEMBER`** is set in the pilot env only for a class pool,
  so a legacy pilot's environment is byte-identical.  A legacy pool has one
  member; nothing on the pilot needs to be told which.
- **`PoolConfig.reproject()`** is new (not named in the plan).  The parser
  projects the primary member onto the scalar fields; a *live* pool needs
  the same after `POST`/`DELETE …/members` changes the member set, or the
  summary and every legacy consumer would show a departed member's queue.
- **`_finalize_pilot` sets `finished_at`** (needed by `node_hours`) and
  fails a task past `policy.max_requeues` with `requeued too often (pilot
  lost)`; the cap is read defensively (`try/except`) so a third-party
  policy without the property still works.
- **`_claim` returns `bool`.**  A dispatcher-assigned cwd is created there,
  and an `OSError` must fail the task rather than let it run with no
  directory; the drain loop skips a claim that did not stand.
- **`_place_inputs`** is a separate method rather than inline in
  `_do_rhapsody_submit`, and returns the still-runnable tasks.  A
  non-shared member with no staging client fails every task in the batch
  with `could not place inputs on the pilot: staging client unavailable`.
- **`stage_in` uses the record's `cwd`** when a record exists (§8 rule 4).
  This changed one pre-existing fixture:
  `test_stage_in_out_roundtrip` built a record whose `cwd` was `tmp_path`
  while writing the output under `scratch_base/<task_id>`; the record now
  names the directory the file actually lands in.  That is the "straight
  bug fix" §8 rule 4 calls for, and it is the **only** pre-existing test
  changed for behaviour (the conservative-policy harness changed only for
  the new `submit_pilot` signature and the six member helpers, as §5 says).
- **`_check_broker_local`** is the shared helper behind both staging 409s.
- **`node_hours` skips an entry with no usable start** (`active_at` and
  `submitted_at` both zero/absent), so a never-submitted record cannot be
  charged from the epoch to `now`.
- **The `DELETE` route has a `POST …/remove` twin.**  The plan offered the
  POST form as a fallback *if* a reviewer objected to touching
  `plugin_base`.  Both are registered: `add_route_delete` is the five-line
  base-class addition the plan preferred, and the POST twin costs one line
  and covers any transport that cannot send a DELETE body.

### Review round 2 (fable, on 2fa338e) — what changed

**BLOCKING: the staging allow-list.**  `plugin_staging.StagingSession`
hard-codes `_ALLOWED_BASES = [$HOME, /tmp]`, so the dispatcher's own input
placement to a non-shared member — a `put` of
`<member.scratch_base>/<task_id>/<file>`, i.e. `/pscratch/...` on a real
machine — was refused by its own staging plugin with *Path escapes allowed
directories*.  §4.3 was therefore unimplementable as written.  Fix: the
allow-list is now snapshotted **per session** and extended with
`realpath($RADICAL_ORBIT_SCRATCH_BASE)` when set.  The dispatcher already
sets that variable on every pilot it launches, to that pilot's member
`scratch_base` (`_build_pilot_env`), so the path is exactly the one the
member declared and nothing wider is opened up.  Two harness tests cover
it through the **real** `PluginStaging`, with the static bases narrowed to
a nonexistent directory so only the env var can let the put through (a
positive and a negative); `test_validate_path_outside_allowed_raises` had
to move its `_ALLOWED_BASES` patch *before* constructing the session.

**Rhapsody-dialect tasks on a non-shared member.**  Their `cwd` lives
inside the opaque task dict and is never rewritten, so `_place_inputs`
skips the `.orbit-cwd` marker for them (writing into a directory the
client owns would be a guess), and `_check_broker_local` treats a dialect
record exactly like *no* record — otherwise every dialect `stage_in` became
a 409 "task not yet placed", a legacy behaviour change.  Fixing that
surfaced a real bug of mine: a record with `cwd == ''` made `stage_in` /
`stage_out` compute `Path('')`, i.e. the **broker's working directory**.
Both routes now fall back to the pool scratch when `cwd` is empty.

**`node_hours` charges from `active_at` only.**  Queue time is not
allocation time, and a pilot that never reached ACTIVE consumed nothing.
The `submitted_at` fallback is gone (it both billed queue time and, for a
zero `submitted_at`, could charge from the epoch to `now`).  This is what
the federation's `node_hours_from_history` does, so the two agree before
the merge.  `test_falls_back_to_submitted_at` became
`test_queue_time_is_never_charged` plus
`test_charged_from_active_at_not_submitted_at`.

**The `min_pilots` floor must not starve siblings.**  `on_tick` collects
*all* under-floor members, runs them through the guards, and **falls
through to the backlog step** when none survives — a member whose site is
down, whose budget is spent, or which is in failure backoff sits below its
floor forever, and returning there stopped every sibling from growing.
The guards are factored out as `_pass_guards`; the floor is served in
declaration order (a debt, not a preference) while only the backlog step
ranks by `member_preference`.

**Legacy pools now get proactive `min_pilots` submissions — a deliberate
legacy behaviour change.**  Pre-121 the policy returned immediately when
nothing was pending, so `min_pilots` was never acted on and a warm floor
did not exist for any pool.  It does now, including for legacy single-site
pools: one declaring `min_pilots: 2` will submit pilots with an empty
queue.  The default is `0`, so a pool that never set it is unaffected.
Named in `docs/task_dispatcher_strategy.md`.

**Other round-2 fixes.**

- `_place_inputs` `mkdir`s the task cwd before the shared-FS copy loop: a
  *client-supplied* cwd is only a promise, and `_claim` creates only the
  ones it assigns itself.
- `_claim` calls `expanduser()` only for a `shared_fs` member.  A remote
  member's `~` means *its* home, and expanding it against the broker's was
  wrong.
- Legacy pilots stamp `member_id = ''`, not `'_'`, and a legacy pool
  reports `member_ids: []` and `members: []`.  The implicit member is an
  internal construct; the sentinel never reaches the wire, and pre-121
  records already carry `''`.  (`test_legacy_summary_reports_one_implicit_member`
  adjusted.)
- `_route_remove_member` with `cancel_tasks` fails the touched tasks
  **before the first await**.  Failing them after the cancels could kill a
  task that had already been re-queued and re-dispatched to a sibling in
  that window; doing it first also keeps `tasks_requeued` honest, since
  the re-queue branch skips terminal tasks.
- Submitting to an emptied class pool is a `400 pool 'x' has no members`
  (both submit routes, with or without a `requirements` block) instead of
  a task queued forever.
- `PoolConfig.reproject()` on an empty member set now writes exactly the
  placeholders `_parse_pool` projects, so an emptied pool persists and
  replays to an identical config.
- `NO_MPI_BACKENDS` moved above `satisfies`, its only consumer in that
  module.
- Add-member idempotence compares list-valued **attributes**
  order-insensitively (`_member_fingerprint`): a federation that rebuilds
  its declarations from a set would otherwise get a 409 for a member that
  has not changed.  The stored declaration keeps the order it was declared
  with; every other field is compared verbatim.
- `task_dispatcher.js`: the stale `<details>` comment is gone (member rows
  are followed by a nested size row, not a disclosure widget) and
  `.td-member-sizes` has minimal CSS.
- Docs: `pilot_history` is noted as **unbounded** (pre-existing — the pilot
  ledger is never pruned within a pool's lifetime, only whole state
  directories after 30 idle days), and the input spool is noted as written
  **synchronously** on the submit path by design (bounded at 8 MiB per
  submit; acking a task whose inputs are not yet on disk would let a crash
  between the two produce a task that runs with missing files).

### Test and lint status

`PYTHONPATH=src ve3/bin/python -m pytest tests/unittests/ -q`
→ **1330 passed, 4 skipped** (1113 before this plan; 1311 before review
round 2).
`ve3/bin/flake8 src/ bin/` → clean apart from the two pre-existing E226 in
`plugin_xgfabric.py`.  (Round 2 also removed three pre-existing F401/E401
findings from `tests/unittests/test_plugin_staging.py` as a side effect of
rewriting the test it patched.)
