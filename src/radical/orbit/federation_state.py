'''
Federation — resource records, usage accounting and the durable store.

Four record types make up a federation's state:

- :class:`ResourceRecord` — one joined resource: what it is, what its
  **members** are, and which dispatcher session serves them.
- :class:`MemberRecord` — one resource shape inside a resource: a queue, a
  pilot size, its own software/attributes/budget, and the capability
  **class** whose pool (``fed-<class>``) it belongs to.  Budget and
  node-hours live here, not on the resource — an allocation is per site.
- :class:`ResourceUsage`  — the derived, refreshed-on-read view of what a
  resource (or one of its members) has actually consumed and is running.
- :class:`SubmitLedgerEntry` — the federation's own record of one task it
  routed, so ``tasks_running`` / ``tasks_done`` survive the dispatcher's
  capped ``recent_tasks`` window (50 per pool).

Persistence is one ``state.json`` holding the resources plus the ledger,
rewritten atomically on every mutation — the same tempfile + ``os.replace``
helper the task dispatcher uses (:func:`~radical.orbit.task_dispatcher_state
.write_json_atomic`), so there is exactly one implementation of the atomic
write in the tree.

Accounting
----------
Node-hours are **derived**, never stored: a resource's usage is recomputed
from the dispatcher's ``pilot_history`` (see
:func:`~radical.orbit.task_dispatcher_state.node_hours`).  A pilot that never reached ``ACTIVE``
consumed nothing; a live one is measured against *now*; a finished one
against its ``finished_at``.  That makes the number monotone under repeated
reads and correct across a broker restart, with no accumulator to drift.
'''

from __future__ import annotations

import logging
import os
import re
import time

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from .task_dispatcher_config import PilotSize, PILOT_SUBMIT, PILOT_ENDPOINT
from .task_dispatcher_state  import write_json_atomic, read_json

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Vocabularies
# ---------------------------------------------------------------------------

# Join modes.  ``allocation``: the endpoint runs *inside* a compute
# allocation and the whole allocation is the resource — one pilot, started
# at join.  ``login``: the endpoint sits on a login node and declares what
# it may ask the batch system for; pilots are submitted on demand.
MODE_ALLOCATION = 'allocation'
MODE_LOGIN      = 'login'
MODES           = (MODE_ALLOCATION, MODE_LOGIN)

# Resource liveness, inherited from the serving endpoint's topology liveness.
LIVENESS_OK      = 'ok'
LIVENESS_SUSPECT = 'suspect'
LIVENESS_LOST    = 'lost'

# The **derived** state word, reported alongside ``liveness`` as ``state``
# (see :meth:`MemberRecord.state`).  It is every liveness value plus
# ``failing``: an endpoint that answers topology perfectly well while every
# pilot it is asked to submit dies.  ``liveness`` itself is left alone —
# it means "can we reach the endpoint", the routing policy is written
# against it, and a member whose site is merely full must not read as gone.
STATE_FAILING = 'failing'

# Two more derived words, alongside ``failing``.  ``stale``: the last usage
# refresh could not reach the dispatcher, so the numbers shown are the
# previous ones.  ``idle``: reachable, nothing held against it, and simply
# holding no pilot right now -- the join-to-first-pilot window, and the
# resting state of a login-mode shape with an empty queue.  Neither is a
# problem, and neither is ``ok`` either.
STATE_STALE = 'stale'
STATE_IDLE  = 'idle'

# Worst-first ranking of the derived state words, used to fold a resource's
# member states into the one word its row shows.  ``idle`` sits **below**
# ``ok``: a machine with one shape working and one resting is working, so a
# resource reads ``idle`` only when *every* shape of it is.  Anything
# unknown ranks lowest of all, so a word this version does not know can
# never hide one it does.
STATE_RANK = {LIVENESS_LOST: 5, STATE_FAILING: 4, LIVENESS_SUSPECT: 3,
              STATE_STALE: 2, LIVENESS_OK: 1, STATE_IDLE: 0}

# How many consecutive pilot failures make a member read as ``failing``.
# Mirrors the conservative policy's ``max_consecutive_failures`` default —
# the point at which that policy stops submitting — but the federation
# does not (and must not) read the dispatcher's strategy config, so a
# member paused by *any* backoff counts as failing whatever its count.
FAILING_PILOT_FAILURES = 3

# Resource names ride in URLs and in a member id, so they are restricted to
# a conservative, path-safe alphabet.  A resource name **may** contain dots.
NAME_RE = re.compile(r'^[a-z0-9_.-]+$')

# A member's short name must **not** contain a dot: the member id is
# ``<resource>.<member>`` and the resource half may itself carry dots, so the
# dot is unambiguously the separator only if the member half has none.  Split
# a member id with ``rpartition('.')``, never ``partition``.
MEMBER_NAME_RE = re.compile(r'^[a-z0-9][a-z0-9_-]*$')

# A capability class names a dispatcher pool (``fed-<class>``).  A name that
# does not match is a declaration error — never lower-cased or otherwise
# coerced, because silently accepting ``GPU`` would create a second,
# invisible ``fed-GPU`` pool nobody else routes to.
CLASS_RE = re.compile(r'^[a-z0-9][a-z0-9_-]*$')

# The member name a resource with exactly one (implicit) member gets.
DEFAULT_MEMBER = 'default'

# Capability keys whose value is a list of strings; everything else in a
# capability dict is a number.
LIST_CAPABILITIES = ('software',)


class FederationStateError(ValueError):
    '''Raised when a resource declaration violates a state invariant.'''
    pass


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@dataclass
class ResourceUsage:
    '''Derived usage for one resource — recomputed, never accumulated.

    ``stale`` marks a refresh that could not reach the dispatcher: the
    previous values are kept (a resource does not blink to zero because one
    poll timed out) and the flag says so.

    ``pilot_error`` / ``pilot_failures`` / ``paused_until`` are the
    dispatcher's answer to "why are there no pilots here": the reason the
    last pilot of this member died, how many died in a row, and until when
    the dispatcher has stopped trying.  All three are ``0``/``None`` for a
    member that is simply idle.
    '''
    node_hours_used     : float = 0.0
    node_hours_remaining: float = 0.0
    pilots_active       : int   = 0
    tasks_running       : int   = 0
    tasks_done          : int   = 0
    tasks_failed        : int   = 0
    stale               : bool  = False
    updated_at          : float = 0.0
    pilot_error         : str | None   = None
    pilot_failures      : int           = 0
    paused_until        : float | None  = None
    # Walltime the member's longest-lived pilot still has, in seconds --
    # the dispatcher's number, since only it knows a pilot's deadline.
    # ``None`` when it holds none (see :meth:`MemberRecord.remaining_sec`,
    # which prefers the allocation's own end where there is one).
    remaining_sec       : float | None  = None


@dataclass
class SubmitLedgerEntry:
    '''The federation's own record of one routed task.

    The dispatcher keeps only the 50 most recent tasks per pool in its
    verbose summary, so counting completed work off *that* would silently
    undercount a long run.  This ledger is the federation's answer: one
    entry per task it ever routed, updated whenever the task is polled.

    ``resource`` and ``member_id`` are **placement**, and placement is late:
    a class pool binds a task to a member only at dispatch.  Both are
    therefore nullable — the submit fills ``resource`` with the advisory
    top-scoring member's resource, and the first poll that reports a
    ``member_id`` overwrites both with the truth.  A ``leave`` re-points
    them to ``None`` rather than dropping the entry, because the task keeps
    running on a sibling member.
    '''
    task_id       : str
    resource      : str | None
    pool          : str   = ''
    dispatcher_sid: str   = ''
    state         : str   = 'QUEUED'
    submitted_at  : float = 0.0
    finished_at   : float | None = None
    member_id     : str | None = None
    cls           : str   = ''
    detail        : str   = ''


@dataclass
class MemberRecord:
    '''One resource shape inside a resource — the unit the dispatcher sees.

    A member is what an operator is willing to run: one queue, one pilot
    size, its own software and free-form attributes, and its own node-hour
    budget.  Its **class** (``cpu``, ``gpu``, …) selects the dispatcher pool
    ``fed-<class>`` it is added to, so every member of a class competes for
    the same task queue and the dispatcher — not the federation — chooses
    between them at dispatch time.

    ``cls`` is spelled ``class`` on the wire (``class`` is a Python
    keyword); :meth:`to_wire` renames it and :func:`member_from_dict`
    accepts either spelling.

    ``pilot`` says where this member's pilots come from.  An
    ``allocation``-mode resource's one member is ``endpoint``: the endpoint
    that joined already runs inside the allocation, and the dispatcher
    adopts it rather than starting a second process on it (Orbit plan 122).
    ``end_time`` is when that allocation ends, as an absolute epoch — a
    member is re-declared with its join-time ``walltime_sec`` on every
    re-attach, so only an absolute instant survives a re-adoption.
    '''
    member          : str
    member_id       : str = ''            # '<resource>.<member>', server-filled
    cls             : str = ''            # 'cpu' | 'gpu' | … ; wire key "class"
    pool_name       : str = ''            # 'fed-<class>', server-filled
    endpoint        : str = ''            # the endpoint serving it, server-filled
    pilot           : str = PILOT_SUBMIT  # 'submit' | 'endpoint'
    end_time        : float | None = None  # allocation end, absolute epoch
    queue           : str = ''
    account         : str | None = None
    nodes           : int = 1
    cpus_per_node   : int = 1
    gpus_per_node   : int = 0
    walltime_sec    : int = 3600
    min_pilots      : int = 0
    max_pilots      : int = 1
    rhapsody_backend: str = 'concurrent'
    scratch_base    : str | None = None   # on the MEMBER's host
    shared_fs       : bool = True
    software        : list = field(default_factory=list)
    attributes      : dict = field(default_factory=dict)
    budget          : dict = field(default_factory=dict)
    usage           : ResourceUsage = field(default_factory=ResourceUsage)
    liveness        : str = LIVENESS_OK

    def __post_init__(self) -> None:
        '''Force the pilot floor and ceiling of an ``endpoint`` member to 1.

        Mirrors ``PoolMember.__post_init__``: the dispatcher holds exactly
        one adopted pilot, so these are the bounds it enforces and the ones
        to report.  One place for the rule, whether the member was built
        from an allocation, a declared ``members`` entry, or loaded back.
        '''
        if self.pilot == PILOT_ENDPOINT:
            self.min_pilots = 1
            self.max_pilots = 1

    def budget_node_hours(self) -> float:
        '''Return this member's declared node-hour allowance (0.0 = none).'''
        try:
            return float((self.budget or {}).get('node_hours') or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def default_class(self) -> str:
        '''Return the class this member falls into when none is declared.

        A *default*, not a rule: an explicit ``class`` on the member always
        wins, so a new class needs no code change here.
        '''
        return 'gpu' if (self.gpus_per_node or 0) > 0 else 'cpu'

    def pilot_size(self) -> PilotSize:
        '''Return this member's single pilot shape.

        The same object the dispatcher's matcher compares a task's
        ``cores`` / ``gpus`` / ``mpi`` against, so federation and dispatcher
        answer the same question with the same data.
        '''
        return PilotSize(nodes            = int(self.nodes or 1),
                         cpus_per_node    = int(self.cpus_per_node or 1),
                         rhapsody_backend = self.rhapsody_backend or '',
                         gpus_per_node    = int(self.gpus_per_node or 0),
                         walltime_sec     = int(self.walltime_sec or 0))

    def match_attributes(self) -> dict:
        '''Return the attribute map a requirement is matched against.

        ``software`` is an attribute by convention (the dispatcher knows no
        federation vocabulary), so it is folded in here rather than kept as
        a second, parallel channel.
        '''
        attrs = dict(self.attributes or {})
        attrs['software'] = list(self.software or [])
        return attrs

    def remaining_sec(self) -> float | None:
        '''Return the seconds of runway this member has left, or ``None``.

        The allocation's own ``end_time`` answers it where there is one —
        that is the instant the endpoint disappears, whatever the pilot
        record says — recomputed on every read, since it is a countdown.
        Otherwise it is the dispatcher's number, persisted with the usage
        snapshot and overwritten on every refresh: the most walltime any of
        this member's live pilots still has, which is ``None`` when it
        holds none.  Never negative: an allocation past its
        end has no time left, it does not owe any.
        '''
        if self.end_time:
            return max(0.0, float(self.end_time) - time.time())
        return self.usage.remaining_sec

    def state(self) -> str:
        '''Return the derived state word: ``liveness``, or one of three.

        A member is ``failing`` when its endpoint is reachable (so liveness
        says ``ok``) but it is holding no pilot and the dispatcher has
        either paused submissions to it or watched
        :data:`FAILING_PILOT_FAILURES` of its pilots die in a row.  That is
        the case the demo hit: a member reported ``ok`` with 0 pilots for
        half an hour while every submit failed on a disk quota.

        It is ``stale`` when the last usage refresh could not reach the
        dispatcher — the numbers shown are the previous ones and the row
        should say so — and ``idle`` when it is simply holding no pilot with
        **no recorded failure**: the join-to-first-pilot window, an adopted
        endpoint waiting for its first tick, or a login-mode shape with an
        empty queue.  None of those is ``failing``, and a member must never
        flash red merely because no pilot is ACTIVE yet.

        Anything other than ``ok`` is passed through untouched — a lost
        endpoint is lost, and the fact that its last pilot also failed is
        not the headline.
        '''
        if self.liveness != LIVENESS_OK:
            return self.liveness
        usage = self.usage
        if usage.pilots_active:
            return self.liveness
        if usage.paused_until and usage.paused_until > time.time():
            return STATE_FAILING
        if (usage.pilot_failures or 0) >= FAILING_PILOT_FAILURES:
            return STATE_FAILING
        if usage.stale:
            return STATE_STALE
        if not usage.pilot_error:
            return STATE_IDLE
        return self.liveness

    def to_wire(self) -> dict:
        '''Return the client-facing view, with ``cls`` renamed to ``class``.

        ``state`` rides alongside ``liveness`` rather than replacing it:
        ``liveness`` is what the topology says and what the routing policy
        is written against, ``state`` is what a human should be shown.
        ``remaining_sec`` is derived here for the same reason it is not
        stored — it is a countdown, and a stored one is wrong by the age of
        the file.
        '''
        out = asdict(self)
        out['class']         = out.pop('cls')
        out['state']         = self.state()
        out['remaining_sec'] = self.remaining_sec()
        return out


def member_from_dict(data: dict) -> MemberRecord:
    '''Rebuild a :class:`MemberRecord` from JSON (wire *or* persisted).

    Accepts both the wire key ``class`` and the persisted key ``cls``, drops
    unknown keys, and rebuilds a missing ``usage`` block empty — so both a
    client declaration and an older ``state.json`` load through one path.
    '''
    data  = dict(data or {})
    known = set(MemberRecord.__dataclass_fields__)
    if 'class' in data:
        data.setdefault('cls', data.pop('class'))
    kw    = {k: v for k, v in data.items() if k in known}
    usage = kw.pop('usage', None)
    rec   = MemberRecord(**kw)
    if isinstance(usage, dict):
        ukeys = set(ResourceUsage.__dataclass_fields__)
        rec.usage = ResourceUsage(
            **{k: v for k, v in usage.items() if k in ukeys})
    return rec


@dataclass
class ResourceRecord:
    '''One joined resource.

    Client-declared fields (``name`` … ``pool``) come from the join body;
    the rest are server-filled and must not be accepted from a client.

    ``capabilities`` and ``budget`` stay on the record but are now the
    **aggregate view** of its members (see :meth:`aggregate`), so every
    existing consumer keeps rendering while the per-member view is added
    alongside.
    '''
    # -- declared at join --------------------------------------------------
    name        : str
    endpoint    : str
    mode        : str  = MODE_ALLOCATION
    site        : str  = ''
    kind        : str  = ''
    capabilities: dict = field(default_factory=dict)
    budget      : dict = field(default_factory=dict)
    scratch_base: str | None = None
    # does the BROKER host see ``scratch_base``?  False for a resource on
    # another machine, whose scratch tree only its own pilots can reach --
    # the broker then neither validates that path against its own roots nor
    # writes into it (task inputs travel through the pilot's staging plugin).
    shared_fs   : bool = True
    pool        : dict | None = None      # login mode: the pool declaration

    # -- server-filled ------------------------------------------------------
    joined_at     : float = 0.0
    dispatcher_sid: str   = ''
    pool_name     : str   = ''
    usage         : ResourceUsage = field(default_factory=ResourceUsage)
    liveness      : str   = LIVENESS_OK

    # The members this resource offers, keyed by short name, insertion
    # ordered.  Authoritative: everything the dispatcher is told about this
    # resource is built from here.
    members       : dict = field(default_factory=dict)

    # The exact pool declaration handed to the dispatcher at join, for a
    # record written **before** class pools.  Internal (see :meth:`to_wire`)
    # and kept only so the upgrade path can re-own and release the legacy
    # ``fed-<name>`` pool; a class-pool join leaves it empty, because the
    # declaration is rebuilt from ``members`` on every registration.
    pool_config   : dict  = field(default_factory=dict)

    # -- derived views -----------------------------------------------------

    def budget_node_hours(self) -> float:
        '''Return the declared node-hour allowance (0.0 when undeclared).'''
        try:
            return float((self.budget or {}).get('node_hours') or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def capability(self, key: str, default: Any = None) -> Any:
        '''Return one declared capability value.'''
        return (self.capabilities or {}).get(key, default)

    def member_list(self) -> list:
        '''Return this resource's members in declaration order.'''
        return list(self.members.values())

    def aggregate(self) -> None:
        '''Recompute the resource-level view from the members.

        ``cores`` = Σ ``nodes × cpus_per_node``, ``gpus`` = Σ ``nodes ×
        gpus_per_node``, ``software`` = union, ``budget.node_hours`` = Σ
        member budgets.  Every other declared capability (``mem_gb`` and
        friends) is left exactly as declared — the aggregate answers what
        the members add up to, it does not overwrite what an operator said
        about the machine.
        '''
        members = self.member_list()
        if not members:
            return

        caps = dict(self.capabilities or {})
        caps['cores'] = sum(int(m.nodes or 0) * int(m.cpus_per_node or 0)
                            for m in members)
        caps['gpus']  = sum(int(m.nodes or 0) * int(m.gpus_per_node or 0)
                            for m in members)

        software: list = []
        for m in members:
            for tag in (m.software or []):
                if tag not in software:
                    software.append(tag)
        caps['software'] = software
        self.capabilities = caps

        total = sum(m.budget_node_hours() for m in members)
        if total > 0:
            self.budget = {'node_hours': round(total, 6)}

    def to_dict(self) -> dict:
        '''Return the full persisted view of this record.'''
        return asdict(self)

    def state(self) -> str:
        '''Return the derived state word for the resource row.

        The **worst** of its members' states (:data:`STATE_RANK`: ``lost`` >
        ``failing`` > ``suspect`` > ``stale`` > ``ok`` > ``idle``), because
        the resource row is what a reader scans first and a machine on which
        one shape cannot start a pilot is not ``ok``.  The member rows say
        which one.  ``idle`` ranks below ``ok`` deliberately: one shape
        resting beside a working one does not make the machine idle, so the
        row says ``idle`` only when every shape does.  A resource whose own
        liveness is not ``ok`` reports that: it is not reachable, and what
        its shapes would do is moot.
        '''
        if self.liveness != LIVENESS_OK:
            return self.liveness
        states = [m.state() for m in self.member_list()]
        if not states:
            return self.liveness
        return max(states, key=lambda s: STATE_RANK.get(s, -1))

    def to_wire(self) -> dict:
        '''Return the client-facing view: everything except ``pool_config``.

        The resolved pool declaration is an implementation detail of how the
        federation drives the dispatcher; a client sees the resource, its
        ``members`` (as a **list**, each with its wire ``class``), its
        ``pool_name``, its ``dispatcher_sid`` and its derived ``state``.
        '''
        out = asdict(self)
        out.pop('pool_config', None)
        out['members'] = [m.to_wire() for m in self.member_list()]
        out['state']   = self.state()
        return out


def resource_attributes(site: Any = '', kind: Any = '',
                        mem_gb: Any = None) -> dict:
    '''Return the attribute map a *resource-wide* declaration implies.

    The one place the ``site`` / ``kind`` / ``mem_gb_per_node`` convention is
    spelled out, shared by the two callers that synthesise a member from a
    resource: :func:`_derive_member` (a pre-08 record read off disk) and
    ``PluginFederation._implicit_member`` (a join with no ``members`` list).

    **Empty and ``None`` values are dropped, and that is the whole point.**
    The dispatcher's ``parse_member`` accepts an attribute value that is a
    string, a number or a list of strings — a ``None`` (an undiscovered
    ``mem_gb``) is a 400.  Since a member declaration is re-sent in full on
    every registration, one such attribute anywhere in the state would fail
    *every* subsequent join and every restart replay, not just its own
    record's.  An empty string goes too: ``site: ''`` would match no label
    while looking like a declaration.
    '''
    attrs = {'site': site, 'kind': kind, 'mem_gb_per_node': mem_gb}
    return {k: v for k, v in attrs.items() if v is not None and v != ''}


def _derive_member(rec: ResourceRecord, pilot: str) -> MemberRecord:
    '''Synthesise the single member of a record written before class pools.

    The pool declaration stored at join (``pool_config``) is the authority:
    an allocation-mode size was derived from the endpoint's live allocation
    and cannot be recomputed offline.  The declared ``pool`` block is the
    fallback for a record that never got one.
    '''
    decl  = dict(rec.pool_config or {})
    sizes = decl.get('pilot_sizes') or {}
    size  = dict((sizes.get(decl.get('default_size') or 'default')
                  or next(iter(sizes.values()), None) or {}))
    if not size:
        size = dict(rec.pool or {})
    caps = rec.capabilities or {}
    pool = rec.pool or {}

    member = MemberRecord(
        pilot            = pilot,
        member           = DEFAULT_MEMBER,
        member_id        = f'{rec.name}.{DEFAULT_MEMBER}',
        endpoint         = rec.endpoint,
        queue            = str(decl.get('queue') or pool.get('queue') or ''),
        account          = decl.get('account', pool.get('account')),
        nodes            = int(size.get('nodes') or 1),
        cpus_per_node    = int(size.get('cpus_per_node') or 1),
        gpus_per_node    = int(size.get('gpus_per_node') or 0),
        walltime_sec     = int(size.get('walltime_sec') or 3600),
        min_pilots       = int(decl.get('min_pilots')
                               if decl.get('min_pilots') is not None
                               else pool.get('min_pilots') or 0),
        max_pilots       = int(decl.get('max_pilots')
                               if decl.get('max_pilots') is not None
                               else pool.get('max_pilots') or 1),
        rhapsody_backend = str(size.get('rhapsody_backend') or 'concurrent'),
        scratch_base     = rec.scratch_base,
        shared_fs        = True,
        software         = list(caps.get('software') or []),
        attributes       = resource_attributes(rec.site, rec.kind,
                                               caps.get('mem_gb')),
        budget           = dict(rec.budget or {}),
        liveness         = rec.liveness,
    )
    member.cls       = member.default_class()
    member.pool_name = f'fed-{member.cls}'
    member.usage     = ResourceUsage(**asdict(rec.usage))
    return member


def record_from_dict(data: dict) -> ResourceRecord:
    '''Rebuild a :class:`ResourceRecord` from persisted JSON.

    Unknown keys are dropped and a missing ``usage`` block is rebuilt empty,
    so an older ``state.json`` survives a schema addition.  A record with no
    ``members`` at all is **pre-08**: exactly one member is derived from its
    stored pool declaration (:func:`_derive_member`), so the rest of the
    federation only ever sees one shape.

    Two fields are **migrated** rather than defaulted, because their
    dataclass default would be actively wrong for an older record: a
    member's ``endpoint`` (the resource's, since a resource has one) and, in
    ``allocation`` mode, its ``pilot`` (``endpoint`` -- the endpoint is
    inside the allocation, whatever a pre-122 file says).
    '''
    known = {f for f in ResourceRecord.__dataclass_fields__}
    kw    = {k: v for k, v in (data or {}).items() if k in known}
    usage = kw.pop('usage', None)
    raw   = kw.pop('members', None)
    rec   = ResourceRecord(**kw)

    # A record written before ``pilot`` existed defaults to ``submit``, which
    # for an allocation-mode resource means "start a batch job on the compute
    # node you are already sitting on" -- precisely the second endpoint plan
    # 122 removed, re-POSTed on the first restart after the upgrade.  The
    # mode says what it is: the endpoint is inside the allocation, so it is
    # the pilot, whichever shape the record was loaded from.  Migrated
    # *before* the member is built, so ``MemberRecord.__post_init__`` forces
    # its 1/1 pilot bounds like any other endpoint member.
    pilot = PILOT_ENDPOINT if rec.mode == MODE_ALLOCATION else None

    if isinstance(usage, dict):
        ukeys = set(ResourceUsage.__dataclass_fields__)
        rec.usage = ResourceUsage(
            **{k: v for k, v in usage.items() if k in ukeys})

    members = list(raw.values()) if isinstance(raw, dict) else (raw or [])
    if members:
        for entry in members:
            if not isinstance(entry, dict):
                continue
            if pilot:
                entry = dict(entry, pilot=pilot)
            m = member_from_dict(entry)
            # A record written before members carried their own endpoint
            # gets the resource's -- which is the one they were always
            # served by, since a resource has exactly one.
            m.endpoint = m.endpoint or rec.endpoint
            rec.members[m.member] = m
    else:
        m = _derive_member(rec, pilot or PILOT_SUBMIT)
        rec.members[m.member] = m
    return rec


def ledger_from_dict(data: dict) -> SubmitLedgerEntry:
    '''Rebuild a :class:`SubmitLedgerEntry` from persisted JSON.'''
    known = set(SubmitLedgerEntry.__dataclass_fields__)
    return SubmitLedgerEntry(
        **{k: v for k, v in (data or {}).items() if k in known})


# ---------------------------------------------------------------------------
# Durable store
# ---------------------------------------------------------------------------

class FederationState:
    '''One ``state.json``: the resource registry plus the submit ledger.

    Rewritten atomically on every mutation (the write is microseconds at this
    scale); recovery is a single ``json.load``.  A single-owner discipline —
    every read and write happens on the plugin's event-loop thread — means no
    locking is needed.
    '''

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self.resources: dict[str, ResourceRecord]   = {}
        self.ledger   : dict[str, SubmitLedgerEntry] = {}

    @property
    def path(self) -> Path:
        '''Return the backing ``state.json`` path.'''
        return self._path

    def load(self) -> 'FederationState':
        '''Load the persisted payload (a missing/unreadable file is empty).'''
        payload = read_json(self._path, default={}) or {}
        self.resources = {
            name: record_from_dict(rec)
            for name, rec in (payload.get('resources') or {}).items()
            if isinstance(rec, dict)
        }
        self.ledger = {
            tid: ledger_from_dict(entry)
            for tid, entry in (payload.get('ledger') or {}).items()
            if isinstance(entry, dict)
        }
        return self

    def save(self) -> None:
        '''Rewrite ``state.json`` atomically.'''
        try:
            write_json_atomic(self._path, {
                'resources': {n: r.to_dict()
                              for n, r in self.resources.items()},
                'ledger'   : {t: asdict(e) for t, e in self.ledger.items()},
            })
        except OSError as e:
            log.warning('federation: could not persist %s: %s', self._path, e)

    # -- ledger helpers ---------------------------------------------------

    def ledger_for(self, resource: str) -> list[SubmitLedgerEntry]:
        '''Return every ledger entry routed to *resource*.'''
        return [e for e in self.ledger.values() if e.resource == resource]

    def ledger_for_member(self, member_id: str) -> list[SubmitLedgerEntry]:
        '''Return every ledger entry a poll placed on *member_id*.'''
        return [e for e in self.ledger.values() if e.member_id == member_id]

    @staticmethod
    def _counts(entries) -> tuple[int, int, int]:
        running = done = failed = 0
        for e in entries:
            if   e.state == 'DONE':                 done    += 1
            elif e.state in ('FAILED', 'CANCELED'): failed  += 1
            else:                                   running += 1
        return running, done, failed

    def task_counts(self, resource: str) -> tuple[int, int, int]:
        '''Return ``(running, done, failed)`` task counts for *resource*.

        "Running" is everything not yet terminal — a task queued behind a
        warming pilot is work in flight from the federation's point of view.
        '''
        return self._counts(self.ledger_for(resource))

    def member_task_counts(self, member_id: str) -> tuple[int, int, int]:
        '''Return ``(running, done, failed)`` counts for one member.

        Only tasks a poll actually **placed** on this member count here: a
        task that has not been dispatched has no member yet and shows on the
        resource row alone.
        '''
        return self._counts(self.ledger_for_member(member_id))

    def drop_resource(self, name: str, keep_active: bool = False) -> None:
        '''Forget a resource; *keep_active* decides its live ledger entries.

        With class pools a resource is not the owner of its tasks: a task
        it submitted sits in a **class** pool and may keep running, or start
        running, on a sibling member of another resource.  So a plain
        ``leave`` drops only the *terminal* entries and re-points the rest
        (``resource`` / ``member_id`` → ``None``, ``pool`` and
        ``dispatcher_sid`` untouched — both still valid).  The next poll
        fills the real placement back in.

        Dropping the live entries too (``keep_active=False``) is the full
        teardown, and is correct only when the caller has already had the
        dispatcher cancel them.
        '''
        self.resources.pop(name, None)
        for tid, e in list(self.ledger.items()):
            if e.resource != name:
                continue
            if not keep_active or e.state in ('DONE', 'FAILED', 'CANCELED'):
                self.ledger.pop(tid, None)
            else:
                e.resource  = None
                e.member_id = None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def allowed_bases() -> list[str]:
    '''Return the roots a federation-written path may live under.

    ``~`` and ``/tmp``, both through ``realpath`` — exactly the
    ``PluginStaging._ALLOWED_BASES`` rule, resolved the same way so a
    platform where ``/tmp`` is itself a symlink (``/private/tmp``) does not
    reject every legitimate path.
    '''
    return [os.path.realpath(os.path.expanduser('~')),
            os.path.realpath('/tmp')]


def validate_scratch_base(path: str, *, field: str = 'scratch_base') -> str:
    '''Return *path* expanded, or raise if it escapes ``~`` / ``/tmp``.

    Mirrors the staging plugin's rule: the broker itself creates and writes
    these trees, so neither a join nor a task submission may aim one at an
    arbitrary filesystem location.  The check is on the **realpath**, so a
    symlink cannot smuggle the target out; the value returned is the
    *expanded* form, which is what the operator declared and what the record
    should keep showing.
    '''
    if not isinstance(path, str) or not path:
        raise FederationStateError(f'{field} must be a non-empty string')
    expanded = os.path.expanduser(path)
    if not os.path.isabs(expanded):
        raise FederationStateError(
            f'{field} must be an absolute path (or use ~): {path!r}')
    resolved = os.path.realpath(expanded)
    for base in allowed_bases():
        if resolved == base or resolved.startswith(base + os.sep):
            return expanded
    raise FederationStateError(
        f'{field} must lie under {" or ".join(allowed_bases())}: {path!r} '
        f'(resolves to {resolved})')


def validate_scratch_for_host(path: Any, *, shared: bool = True,
                              field: str = 'scratch_base') -> str:
    '''Return *path* validated against the host that actually owns it.

    The one rule both the record-level and the member-level scratch go
    through, so the two cannot drift:

    - ``shared`` — the broker sees and writes this tree, so it obeys the
      broker-local containment rule (:func:`validate_scratch_base`);
    - not ``shared`` — the path names a directory on the *resource's* host.
      The broker can neither resolve nor reach it, so judging it against the
      broker's own ``~`` and ``/tmp`` would reject every legitimate remote
      path.  It only has to be absolute (or ``~``-prefixed, expanded later
      on that host), and it is kept **exactly as declared** — no
      ``expanduser``, no ``realpath``: both would answer for the wrong
      machine.
    '''
    if shared:
        return validate_scratch_base(path, field=field)
    if not isinstance(path, str) or not path:
        raise FederationStateError(f'{field} must be a non-empty string')
    if not (path.startswith('/') or path.startswith('~')):
        raise FederationStateError(
            f'{field} must be an absolute path (or use ~): {path!r}')
    return path


def validate_pool_int(decl: dict, key: str, *, default: int | None = None,
                      minimum: int = 0,
                      maximum: int | None = None,
                      label: str = 'pool') -> int:
    '''Return one integer field of a pool or member declaration, or raise.

    A bad value here would otherwise reach the dispatcher's own parser or an
    ``int()`` call deep in pool construction and surface as a 500; a
    declaration error deserves a 400.  *label* names the block in the error
    message, so a member's message says which member.
    '''
    if key not in decl or decl[key] is None:
        if default is None:
            raise FederationStateError(
                f"'{label}.{key}' is required")
        return default
    val = decl[key]
    if isinstance(val, bool) or not isinstance(val, int):
        raise FederationStateError(
            f"'{label}.{key}' must be an integer, got "
            f'{type(val).__name__}')
    if val < minimum:
        raise FederationStateError(
            f"'{label}.{key}' must be >= {minimum}, got {val}")
    if maximum is not None and val > maximum:
        raise FederationStateError(
            f"'{label}.{key}' must be <= {maximum}, got {val}")
    return val


def validate_member_name(name: Any) -> str:
    '''Validate a member's short name against :data:`MEMBER_NAME_RE`.'''
    if not isinstance(name, str) or not name:
        raise FederationStateError("'member' must be a non-empty string")
    if not MEMBER_NAME_RE.match(name):
        raise FederationStateError(
            f"'member' must match {MEMBER_NAME_RE.pattern} — no dot, the dot "
            f'separates resource and member (got {name!r})')
    return name


def validate_class(cls: Any, *, label: str = 'member') -> str:
    '''Validate a declared capability class against :data:`CLASS_RE`.

    A name that does not match is **refused**, never coerced: lower-casing
    ``GPU`` into ``gpu`` would hide the typo, and accepting it verbatim
    would create a second, invisible ``fed-GPU`` pool that nothing else
    routes to.
    '''
    if not isinstance(cls, str) or not cls:
        raise FederationStateError(f"'{label}.class' must be a non-empty "
                                   f'string')
    if not CLASS_RE.match(cls):
        raise FederationStateError(
            f"'{label}.class' must match {CLASS_RE.pattern} (got {cls!r}) — "
            f'a class names the pool fed-<class> and is never coerced')
    return cls


def validate_attributes(attrs: Any, *, label: str = 'member') -> dict:
    '''Validate a member's free-form attribute map.

    The dispatcher's matcher compares a task's ``labels`` against these, so
    the value types are exactly the ones it can compare: a string, a number,
    or a list of strings.  Anything else is a declaration error rather than
    an attribute that silently never matches.
    '''
    if attrs is None:
        return {}
    if not isinstance(attrs, dict):
        raise FederationStateError(f"'{label}.attributes' must be an object")
    out: dict = {}
    for key, val in attrs.items():
        if not isinstance(key, str) or not key:
            raise FederationStateError(
                f"'{label}.attributes' keys must be non-empty strings")
        if val is None:
            continue
        if isinstance(val, list):
            if not all(isinstance(v, str) for v in val):
                raise FederationStateError(
                    f"'{label}.attributes.{key}' must be a list of strings")
            out[key] = list(val)
        elif isinstance(val, str):
            out[key] = val
        elif isinstance(val, bool) or not isinstance(val, (int, float)):
            raise FederationStateError(
                f"'{label}.attributes.{key}' must be a string, a number or "
                f'a list of strings')
        else:
            out[key] = val
    return out


def validate_software(value: Any, *, label: str = 'member') -> list:
    '''Validate a member's ``software`` list.'''
    if value is None:
        return []
    if not isinstance(value, list) or \
            not all(isinstance(v, str) for v in value):
        raise FederationStateError(
            f"'{label}.software' must be a list of strings")
    return list(value)


def validate_capabilities(caps: Any) -> dict:
    '''Validate and normalise a declared capability dict.

    Numbers stay numbers; the list-valued keys (``software``) are coerced to
    a list of strings.  Anything else is a declaration error — a capability
    the policy cannot compare is worse than an absent one.
    '''
    if caps is None:
        return {}
    if not isinstance(caps, dict):
        raise FederationStateError("'capabilities' must be an object")
    out: dict = {}
    for key, val in caps.items():
        if not isinstance(key, str) or not key:
            raise FederationStateError(
                "capability keys must be non-empty strings")
        if key in LIST_CAPABILITIES or isinstance(val, list):
            if not isinstance(val, list) or \
                    not all(isinstance(v, str) for v in val):
                raise FederationStateError(
                    f"capability {key!r} must be a list of strings")
            out[key] = list(val)
        elif isinstance(val, bool) or not isinstance(val, (int, float)):
            raise FederationStateError(
                f"capability {key!r} must be a number or a list of strings")
        else:
            out[key] = val
    return out


def validate_budget(budget: Any, *, required: bool = False) -> dict:
    '''Validate a declared budget dict (``{"node_hours": <float>}``).'''
    if budget is None:
        budget = {}
    if not isinstance(budget, dict):
        raise FederationStateError("'budget' must be an object")
    nh = budget.get('node_hours')
    if nh is None:
        if required:
            raise FederationStateError(
                "'budget.node_hours' is required in login mode")
        return {}
    if isinstance(nh, bool) or not isinstance(nh, (int, float)) or nh <= 0:
        raise FederationStateError(
            "'budget.node_hours' must be a positive number")
    return {'node_hours': float(nh)}


def validate_name(name: Any) -> str:
    '''Validate a resource name against :data:`NAME_RE`.'''
    if not isinstance(name, str) or not name:
        raise FederationStateError("'name' must be a non-empty string")
    if not NAME_RE.match(name):
        raise FederationStateError(
            f"'name' must match {NAME_RE.pattern} (got {name!r})")
    return name
