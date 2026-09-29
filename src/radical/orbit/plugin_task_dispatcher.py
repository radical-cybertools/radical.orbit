'''
Task dispatcher plugin — elastic multi-pool task routing for radical.orbit.

Hosts one :class:`PoolState` per pool.  Each ``PoolState`` owns a dispatch
policy (resolved from ``PoolConfig.strategy`` through the manual registry
in :mod:`~radical.orbit.task_dispatcher_policy`; default ``conservative``),
a pilot ledger + pending task queue (one atomic ``state.json``), and a
shared-FS scratch area.

The dispatcher is a **broker-hosted plugin**: it runs on the broker's
plugin-host loop and reaches endpoints through the in-process broker caller.
Child plugin clients (``PSIJClient`` / ``RhapsodyClient``) are ordinary sync
clients whose transport is a blocking caller shim (:class:`_CallerSyncHTTP`);
the dispatcher drives their sync methods via ``asyncio.to_thread`` so the host
loop is never blocked.  Pilots are submitted via ``plugin_psij.submit_tunneled``
on a login-node endpoint.  When the pilot's child endpoint registers with the
broker, its appearance in the rich topology (``on_topology_change``) is the
dispatcher's signal that the pilot is ACTIVE — capacity is taken from the
pool's pilot-size config.  Tasks then flow via ``rhapsody.submit_tasks`` on the
child endpoint; completion arrives as broker ``event`` frames on the raw tap.

A member declared ``pilot: endpoint`` is **adopted** instead of submitted: its
endpoint already runs inside a compute allocation, so it *is* the pilot.  The
record is created PENDING with that endpoint as its own child and reaches
ACTIVE through the same topology path every other pilot takes — no psij job,
no second process, and one endpoint's liveness to track instead of two.

Pools are strictly per-session: keyed ``(owning_sid, pool_name)``, pool names
are session-local, and there is no cross-session attach.  A pool's pilots
follow the owning session's lifetime — session close (owner ``lost`` +
reclaim-drain, ttl expiry, or explicit ``cancel_all``) tears the pools down.
Pools arrive only via ``register_session``.
'''

from __future__ import annotations

import asyncio
import base64
import logging
import math
import os
import shutil
import threading
import time
import uuid

import msgpack

from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .client                            import PluginClient
from .plugin_base                       import Plugin
from .plugin_session_base               import PluginSession
from .plugin_rhapsody                   import (
    RhapsodyClient, WS_PAYLOAD_LIMIT, NOTIFY_BATCH_SIZE,
    _payload_size, _resolve_notify_window, _resolve_frame_cap, BUDGET_RATIO,
)
from .task_dispatcher_config            import (
    PoolConfig, PoolMember, PilotSize, PoolConfigError,
    PILOT_ENDPOINT, default_pool_config, parse_pools, parse_member,
)
from .task_dispatcher_match             import NO_MPI_BACKENDS
from .task_dispatcher_state             import (
    PilotRecord, TaskRecord, PoolStore, node_hours,
    records_from, read_json, write_json_atomic,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE,
    PILOT_DONE, PILOT_FAILED, PILOT_LIVE_STATES, PILOT_ERROR_MAX,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
    TASK_TERMINAL_STATES,
)
from .task_dispatcher_policy               import make_policy

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_STATE_ROOT  = Path('~/.radical/orbit/task_dispatcher/state'
                            ).expanduser()
_DEFAULT_SCRATCH_ROOT = Path('~/.radical/orbit/task_dispatcher/scratch'
                             ).expanduser()

# State-directory pruning: a pool directory not backing an active pool AND
# older than this threshold is removed by the housekeeping tick.
_STATE_PRUNE_DAYS    = 30
_PRUNE_INTERVAL_SEC  = 86400.0   # stale-dir pruning: once a day

# Housekeeping tick frequency.
_TICK_INTERVAL_SEC = 5.0

# Handshake timeout — a pilot that hasn't handshaken in this long is
# reconciled against psij job state.
_HANDSHAKE_TIMEOUT_SEC = 300.0

# Rhapsody-dialect bulk submit (see ``_route_submit_rh``): task dicts in
# rhapsody's own wire format, a ``pool`` key per task.  Route template shared
# with :meth:`TaskDispatcherClient.submit_tasks` so the two cannot drift.
ROUTE_SUBMIT_RH = 'submit_rh/{sid}'

# Keys a rhapsody child notification carries that are worth forwarding to
# the dispatcher's own subscribers (mirrors plugin_rhapsody's
# ``_NOTIFICATION_KEYS``): the uid/state pair plus result and error data.
_RH_FORWARD_KEYS = {'uid', 'state', 'exit_code',
                    'return_value', '_return_value_encoding',
                    'error', 'exception', 'traceback'}


# ---------------------------------------------------------------------------
# Per-task resource requirements
# ---------------------------------------------------------------------------
#
# Wire shape (every key optional; absent or ``null`` ⇒ ``{}`` ⇒ today's
# behaviour byte-for-byte):
#
#     {"cores": 1, "gpus": 0, "mem_gb": 0, "ranks": 1,
#      "mpi": false, "software": [], "labels": {}}
#
# ``software`` / ``labels`` are dispatcher-side placement attributes: they
# are validated and persisted here but never reach rhapsody (plan 121 is
# their consumer).  Anything not in the table is a 400 — a typo that
# silently drops a field is worse than a refused request.

_REQ_DEFAULTS: dict = {
    'cores'   : 1,
    'gpus'    : 0,
    'mem_gb'  : 0,
    'ranks'   : 1,
    'mpi'     : False,
    'software': [],
    'labels'  : {},
}

# Backends whose group launch needs a ``pmi`` value the dispatcher cannot
# infer (rhapsody dragon v1, ``TaskLauncherV1._launch_group_task``).
# ``mpi: true`` on a pool where *every* size of *every* member names one
# of these is refused at submit; where some members can, the task is
# accepted and the policy simply never offers it a dragon_v1 pilot.
# Defined once in ``task_dispatcher_match`` so the gate and the matcher
# cannot drift.
_NO_MPI_BACKENDS = NO_MPI_BACKENDS

# Task inputs carried inline on a pool-mode submit (``inputs_b64``),
# measured on the DECODED bytes.  ``protocol.FRAME_CAP`` is 4 MiB and the
# base64 form is 4/3 of the decoded one while sharing the frame with the
# rest of the submit body, so the true ceiling is
# ``FRAME_CAP * 3 // 4 - 64 KiB`` ~ 2.9 MiB; 2 MiB is that rounded down to
# a number an operator can remember.
#
# The per-submit total only bites past the default cap: the gateway reads
# at most ``protocol.FRAME_CAP`` of HTTP body, and a WS submit is bounded by
# the broker's ``BrokerTuning.frame_cap`` (default ``FRAME_CAP``), so with
# stock settings no path can deliver 8 MiB of decoded inputs.  It is the
# backstop for a broker tuned to a ``frame_cap`` above ~10.7 MiB (8 MiB
# decoded, base64-inflated), whose one submit could otherwise spool as much
# as that cap allows.
_MAX_INPUT_BYTES  = 2 * 1024 * 1024      # per file
_MAX_INPUTS_BYTES = 8 * 1024 * 1024      # per submit

# Marker file ``put`` to a non-shared member when a task carries no inputs,
# purely to create its cwd there: the staging plugin offers put/get/list
# and no mkdir, and one zero-byte put is cheaper than a new route.
_CWD_MARKER = '.orbit-cwd'


def child_endpoint_name(pool: str, member_id: str | None, pid: str) -> str:
    '''Return the participant name a pilot's child endpoint registers under.

    A **legacy** pool keeps the pre-121 ``<pool>_<pid>`` byte-identically,
    so every existing deployment and stored record still matches.  Since a
    single-member pool is never promoted to a class pool, the implicit
    member never appears in a name.  A **class pool** interposes the member
    id: ``<pool>_<member_id>_<pid>``.

    Exported so the federation and the campaign runner can reproduce the
    name as a fallback rather than re-deriving the rule.
    '''
    if not member_id:
        return f'{pool}_{pid}'
    return f'{pool}_{member_id}_{pid}'


class RequirementsError(ValueError):
    '''Raised when a ``requirements`` block violates the schema.

    Carries the exact user-facing detail string; the submit routes turn it
    into an HTTP 400 verbatim.
    '''
    pass


def _is_int(value: Any) -> bool:
    '''Return whether *value* is a real int.

    ``bool`` is excluded: ``isinstance(True, int)`` is ``True``, and
    ``True`` as a core count is a typo, not a request for one core.
    '''
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    '''Return whether *value* is an int or float, ``bool`` excluded.'''
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_requirements(raw: Any) -> dict:
    '''Validate a raw ``requirements`` block, returning a validated shallow copy.

    Absent or ``null`` yields ``{}`` — the "no declaration" marker that
    forwards byte-identically to pre-requirements behaviour.  This is the
    pool-independent *shape* layer only; the pool-dependent *fit* and
    *backend* gates live in :func:`check_requirements_against_pool`.

    One value is **derived** rather than merely checked: when ``ranks`` is
    given without ``cores``, the copy gets ``cores = max(1, ranks)``.
    ``{"ranks": 4}`` alone means "four processes", and refusing it against
    a ``cores`` default of 1 would be a trap.  An *explicit* ``cores``
    below ``ranks`` is still a 400 — that is a contradiction, not an
    omission.  The derived value is what gets persisted and forwarded.

    Raises :class:`RequirementsError` carrying the exact 400 detail string.
    '''
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RequirementsError('requirements: must be a mapping')

    unknown = sorted(set(raw) - set(_REQ_DEFAULTS))
    if unknown:
        raise RequirementsError(f"requirements: unknown key {unknown[0]!r}")

    req = dict(raw)

    for key in ('cores', 'ranks'):
        if key in req and not (_is_int(req[key]) and req[key] >= 1):
            raise RequirementsError(
                f"requirements: {key!r} must be a positive integer, "
                f"got {req[key]!r}")

    if 'gpus' in req and not (_is_int(req['gpus']) and req['gpus'] >= 0):
        raise RequirementsError(
            f"requirements: 'gpus' must be a non-negative integer, "
            f"got {req['gpus']!r}")

    # math.isfinite() also rejects NaN and +/-inf: both survive JSON via
    # Python's non-standard literals, and NaN would pass a bare `>= 0`.
    if 'mem_gb' in req and not (_is_number(req['mem_gb'])
                                and math.isfinite(req['mem_gb'])
                                and req['mem_gb'] >= 0):
        raise RequirementsError(
            f"requirements: 'mem_gb' must be a non-negative number, "
            f"got {req['mem_gb']!r}")

    if 'mpi' in req and not isinstance(req['mpi'], bool):
        raise RequirementsError("requirements: 'mpi' must be a boolean")

    if 'software' in req:
        sw = req['software']
        if not isinstance(sw, list) or not all(isinstance(s, str) for s in sw):
            raise RequirementsError(
                "requirements: 'software' must be a list of strings")

    if 'labels' in req:
        lb = req['labels']
        if not isinstance(lb, dict) or not all(
                isinstance(k, str) and (isinstance(v, str) or _is_number(v))
                for k, v in lb.items()):
            raise RequirementsError(
                "requirements: 'labels' must be a mapping of string to "
                "string|number")

    # 'ranks' without 'cores' means "N processes": derive the core count.
    if 'cores' not in req and req.get('ranks', 1) > 1:
        req['cores'] = req['ranks']

    r     = {**_REQ_DEFAULTS, **req}
    cores = r['cores']
    gpus  = r['gpus']
    ranks = r['ranks']

    # ``cores >= ranks`` keeps ``cores_per_rank = cores // ranks`` from ever
    # being 0; ``gpus % ranks == 0`` keeps ``gpus_per_rank`` an exact integer
    # for every backend (no float-vs-ceil divergence).
    if cores < ranks:
        raise RequirementsError(
            f"requirements: 'cores' ({cores}) must be >= 'ranks' ({ranks})")
    if gpus % ranks:
        raise RequirementsError(
            f"requirements: 'gpus' ({gpus}) must be divisible by "
            f"'ranks' ({ranks})")

    return req


def check_requirements_against_pool(req: dict, pool: PoolConfig) -> None:
    '''Reject a shape-valid *req* that no member of *pool* can ever host.

    Pool-dependent gates.  Two shapes, as everywhere in the dispatcher:

    *Class pool* — the gate is **the policy's own test**:
    :func:`_no_member_reason`, i.e. :func:`satisfies` against each
    member's attributes and its **default** pilot size, the only size the
    policy ever grows or dispatches to.  Judging on any other size would
    accept a task that then queues forever, and the member-removal sweep
    in ``_route_remove_member`` (the runtime counterpart of this gate,
    which asks the same question) would fail it on the next unrelated
    removal.

    *Legacy pool* (its single implicit member) — plan 120's gates and
    exact error strings, unchanged:

    - *Fit* — compared **per node** (``cores <= size.cpus_per_node``,
      ``gpus <= size.gpus_per_node``), because none of the shipped
      backends spreads one task across nodes here.  The pool passes when
      **any** size fits, and ``largest`` in the message is the max over
      the *failing* dimension.  ``ranks`` needs no check of its own:
      ``cores >= ranks`` (shape) together with ``cores <= cpus_per_node``
      (here) already implies ``ranks <= cpus_per_node``.  ``mem_gb`` has
      no fit check at all — :class:`PilotSize` carries no memory field.
      Note the ``ranks`` bound is only *approximate* for ``dragon_v1``,
      whose real hang condition is ``ranks`` above the **free** slot count
      at that instant (``DragonExecutionBackendV1._submit_task``); only
      enforcement (deferred) can bound that.
    - *Backend* — ``mpi: true`` is refused only when **every** pilot size
      names a backend in :data:`_NO_MPI_BACKENDS`.
    - *Attributes* — none: the implicit member declares no attributes, so
      120's "carried but not acted on" contract holds.

    Beware the built-in ``default`` pool: its single size has
    ``cpus_per_node = 1``, so any ``cores >= 2`` is a 400 there.

    Raises :class:`RequirementsError` carrying the exact 400 detail string.
    '''
    members = list(pool.members.values())
    if not members:
        # An emptied class pool (its last member removed with ``force``)
        # can run nothing.  Say so rather than queue a task forever --
        # this is the only path that can observe a member-less pool (a
        # legacy pool always has its implicit member), and it holds
        # whether or not the task declared requirements.
        raise RequirementsError(f'pool {pool.name!r} has no members')

    if pool.multi_member:
        reason = _no_member_reason(members, req)
        if reason:
            raise RequirementsError(f'{_NO_MEMBER_SATISFIES}: {reason}')
        return

    sizes = members[0].pilot_sizes
    r     = {**_REQ_DEFAULTS, **req}
    cores = r['cores']
    gpus  = r['gpus']

    def _largest(attr: str) -> tuple:
        '''Return the ``(size_key, value)`` maximising *attr* (name breaks ties).'''
        key = max(sorted(sizes), key=lambda k: getattr(sizes[k], attr))
        return key, getattr(sizes[key], attr)

    if not any(cores <= s.cpus_per_node for s in sizes.values()):
        key, val = _largest('cpus_per_node')
        raise RequirementsError(
            f"requirements: {cores} cores exceed every pilot_size "
            f"(largest: {key!r}, {val} cpus/node)")

    if not any(gpus <= s.gpus_per_node for s in sizes.values()):
        key, val = _largest('gpus_per_node')
        raise RequirementsError(
            f"requirements: {gpus} gpus exceed every pilot_size "
            f"(largest: {key!r}, {val} gpus/node)")

    if r['mpi'] and all(s.rhapsody_backend in _NO_MPI_BACKENDS
                        for s in sizes.values()):
        key = sorted(sizes)[0]
        raise RequirementsError(
            f"requirements: 'mpi' is unsupported on "
            f"{sizes[key].rhapsody_backend} (pool {pool.name!r}, "
            f"size {key!r})")


# The one prefix of "no member can run this task", whether the submit gate
# refuses it (400) or the member-removal sweep fails it.
_NO_MEMBER_SATISFIES = 'no member satisfies the task requirements'


def _no_member_reason(members: list[PoolMember],
                      req: dict | None) -> str | None:
    '''Return why no member in *members* can run *req*, or ``None``.

    The reason is the first member's (declaration order), or ``'no
    members remain'`` for an empty list.
    '''
    if not members:
        return 'no members remain'
    first = None
    for m in members:
        reason = m.reject_reason(req)
        if reason is None:
            return None
        first = first or reason
    return first


def _validated_requirements(raw: Any, pool: PoolConfig | None = None) -> dict:
    '''Validate *raw* (and fit it to *pool*, if given), or raise a 400.'''
    try:
        req = parse_requirements(raw)
        if pool is not None:
            check_requirements_against_pool(req, pool)
    except RequirementsError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return req


def backend_kwargs(req: dict, backend: str) -> dict:
    '''Map validated *req* onto one backend's ``task_backend_specific_kwargs``.

    Pure — no state, no I/O.  It lives in the dispatcher because the
    dispatcher is the only place that knows a task's backend
    (``PilotRecord.rhapsody_backend``, resolved from the chosen
    :class:`PilotSize` in ``_submit_pilot``).

    With ``R = ranks``, ``C = cores``, ``G = gpus`` (shape-validated so
    ``C >= R`` and ``G % R == 0``).  Checked against rhapsody-py 0.4.0,
    ``rhapsody.backends.execution``; re-check the cited symbols when the
    rhapsody pin moves:

    | backend        | emitted                                        | effect today |
    |----------------|------------------------------------------------|--------------|
    | `dragon_v2`    | `{'ranks': R, 'gpus_per_rank': G // R}`         | honoured natively (`DragonExecutionBackendV2._schedule_tasks`); spawns R replicas, MPI or not |
    | `radical_pilot`| `{'ranks': R, 'cores_per_rank': C // R, 'gpus_per_rank': G // R, 'mem_per_rank': int(mem_gb * 1024 / R)}` (MB per rank) | honoured natively — the dict feeds `rp.TaskDescription(from_dict=…)` (`RadicalExecutionBackend.build_task`) |
    | `dragon_v3`    | `{'type': 'mpi', 'ranks': R}` **only if** `mpi` | `ranks` is read ONLY under `type == 'mpi'` (`DragonExecutionBackendV3.build_task`); ignored otherwise |
    | `dragon_v1`    | `{'ranks': R}`                                 | spawns R replicas via a non-MPI ProcessGroup (`TaskLauncherV1._determine_task_type`) AND busy-waits on a global slot counter (`DragonExecutionBackendV1._submit_task`); `mpi` refused at submit |
    | `dask`         | `{'resources': {'GPU': G}}` when `G > 0`       | pre-checked; fails the task if unsatisfiable (`DaskExecutionBackend._submit_to_dask`) |
    | `concurrent`   | *(nothing)*                                    | reads only `shell`/`cwd`/`env` (`ConcurrentExecutionBackend._execute_command`) |

    ``ranks`` means "process replicas" and only incidentally "MPI ranks":
    ``dragon_v1`` and ``dragon_v2`` spawn R replicas either way, while
    ``dragon_v3`` spawns them only under ``type: 'mpi'``.

    ``software`` and ``labels`` are never emitted — they are
    dispatcher-side placement attributes.  Any value equal to the
    backend's own default (``ranks == 1``, ``cores_per_rank == 1``,
    ``gpus_per_rank == 0``, ``mem_per_rank == 0``, dask ``G == 0``) is
    omitted, so ``backend_kwargs({}, <any backend>) == {}`` and existing
    tasks forward byte-identically.

    Everything emitted is msgpack-primitive (int / str / dict) — the
    forwarded dict is msgpack-packed on the way to the pilot.
    '''
    r      = {**_REQ_DEFAULTS, **req}
    ranks  = r['ranks']
    cores  = r['cores']
    gpus   = r['gpus']
    mem_gb = r['mem_gb']
    mpi    = r['mpi']

    out: dict = {}

    if backend == 'dragon_v2':
        if ranks != 1:
            out['ranks'] = ranks
        if gpus:
            out['gpus_per_rank'] = gpus // ranks

    elif backend == 'radical_pilot':
        if ranks != 1:
            out['ranks'] = ranks
        if cores // ranks != 1:
            out['cores_per_rank'] = cores // ranks
        if gpus:
            out['gpus_per_rank'] = gpus // ranks
        mem_per_rank = int(mem_gb * 1024 / ranks)
        if mem_per_rank:
            out['mem_per_rank'] = mem_per_rank

    elif backend == 'dragon_v3':
        if mpi:
            out['type'] = 'mpi'
            if ranks != 1:
                out['ranks'] = ranks

    elif backend == 'dragon_v1':
        if ranks != 1:
            out['ranks'] = ranks

    elif backend == 'dask':
        if gpus:
            out['resources'] = {'GPU': gpus}

    # 'concurrent' — and any backend name we do not know — gets nothing.
    return out


# ---------------------------------------------------------------------------
# Transport: sync child-plugin clients over the broker caller
# ---------------------------------------------------------------------------

class _CallerSyncHTTP:
    '''Blocking transport over the broker's in-process caller.

    Installed as a child plugin client's ``self._http`` in the broker-hosted
    dispatcher.  Each verb routes over ``caller.call_threadsafe`` and blocks on
    the returned future's ``.result()`` — safe because the dispatcher drives
    every client method through ``asyncio.to_thread``, so the blocking happens
    on a worker thread and the host loop is never held.  Responses are wrapped
    in :class:`~radical.orbit.runtime_client.RuntimeResponse`, so the same
    ``_raise`` / parsers every other transport uses apply unchanged.
    '''

    def __init__(self, caller, dst: str, timeout: float | None = None) -> None:
        self._caller  = caller
        self._dst     = dst
        self._timeout = timeout

    def request(self, method: str, url: str, *, json=None, content=None,
                data=None, params=None, headers=None, **_kw):
        from .runtime_client import (
            _pack_request, unpack_response_dict, RuntimeResponse)
        path, body, hdrs = _pack_request(
            method, url, json=json, content=content, data=data,
            params=params, headers=headers)
        fut  = self._caller.call_threadsafe(
            self._dst, method, path, body=body, headers=hdrs,
            timeout=self._timeout)
        resp = fut.result(self._timeout)
        status, rhdrs, rbody = unpack_response_dict(resp)
        return RuntimeResponse(status, rhdrs, rbody)

    def get(self, url: str, **kw):
        return self.request('GET', url, **kw)

    def post(self, url: str, **kw):
        return self.request('POST', url, **kw)


# ---------------------------------------------------------------------------
# PoolState — per-pool runtime state
# ---------------------------------------------------------------------------

class PoolState:
    '''Plugin-level runtime state for one pool.

    Distinct from :class:`PoolConfig`, which is the static declaration.
    :class:`PoolState` holds the live fleet, pending queue, and policy
    instance, and persists everything to one atomic ``state.json``.

    Concurrency model: all mutations happen from the plugin's asyncio event
    loop thread, so no in-state locking is needed.
    '''

    def __init__(self, config: PoolConfig, state_dir: Path,
                 scratch_base: Path,
                 plugin: 'PluginTaskDispatcher',
                 owning_sid: str = '') -> None:
        self.config       = config
        self.state_dir    = state_dir
        self.scratch_base = scratch_base
        self.owning_sid   = owning_sid
        self._plugin      = plugin

        state_dir.mkdir(parents=True, exist_ok=True)
        scratch_base.mkdir(parents=True, exist_ok=True)

        # Single atomic per-pool store: config + pilots + tasks.
        self.store = PoolStore(state_dir / 'state.json')
        payload = self.store.load()
        self.pilots: dict[str, PilotRecord] = records_from(
            payload.get('pilots'), PilotRecord)
        self.tasks:  dict[str, TaskRecord]  = records_from(
            payload.get('tasks'), TaskRecord)

        # A pre-121 pilot record carries no size / endpoint snapshot:
        # backfill it once, here, from its member, so every later reader
        # takes the record as authoritative.  An adopted pilot (plan 122)
        # post-dates 121 and always carries its adoption-time snapshot --
        # even a zero one, which ``_activate_pilot`` must see and fail --
        # so it is never re-sized from a (possibly re-declared) member.
        for p in self.pilots.values():
            if p.adopted:
                continue
            member = config.members.get(p.member_id)
            if member is None:
                continue
            size = member.pilot_sizes.get(p.size_key)
            if not p.cpus_per_node and size is not None:
                p.nodes         = size.nodes
                p.cpus_per_node = size.cpus_per_node
                p.gpus_per_node = size.gpus_per_node
            if not p.endpoint_name:
                p.endpoint_name = member.endpoint_name

        # Policy resolved by name through the manual registry (see
        # task_dispatcher_policy; default 'conservative').
        self.policy = make_policy(config)

    def pending_queue(self) -> list[TaskRecord]:
        '''Return pending tasks for this pool, priority-ordered.

        The dispatcher sorts here so the policy sees one canonical ordering.
        '''
        pending = [t for t in self.tasks.values()
                   if t.state == TASK_QUEUED]
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))
        return pending

    def live_pilots(self) -> list[PilotRecord]:
        '''Return live (non-terminal) pilots in this pool.'''
        return [p for p in self.pilots.values()
                if p.state in PILOT_LIVE_STATES]

    # -- member surface --------------------------------------------------
    #
    # The read-only handle a dispatch policy sees, so it never touches
    # ``config.members`` directly.  A legacy pool has exactly one implicit
    # member, so every one of these is arithmetically identical to the
    # pre-121 pool-level version for it.

    def members(self) -> list[PoolMember]:
        '''Return this pool's members in declaration order.'''
        return list(self.config.members.values())

    def member(self, mid: str | None) -> PoolMember | None:
        '''Return one member by id (``''`` is the implicit one).'''
        return self.config.member(mid)

    def live_pilots_for(self, mid: str) -> list[PilotRecord]:
        '''Return this member's live pilots.

        A legacy (and pre-121) :class:`PilotRecord` carries
        ``member_id = ''`` -- the id its implicit member has.
        '''
        return [p for p in self.live_pilots() if p.member_id == mid]

    def pilot_history(self, mid: str | None = None) -> list[PilotRecord]:
        '''Return this pool's pilot records, oldest first.

        With *mid*, only that member's pilots.  Without it, **all** of
        them — including pilots whose member has since been removed, which
        is why the pool total is reported separately rather than summed
        from the member figures.
        '''
        pilots = self.pilots.values() if mid is None \
            else [p for p in self.pilots.values() if p.member_id == mid]
        return sorted(pilots, key=lambda p: p.submitted_at)

    def member_budget_left(self, mid: str,
                           now: float | None = None) -> float | None:
        '''Return the member's remaining node-hours, or ``None`` if unbounded.

        Consumption counts every one of the member's pilots, live included.
        '''
        member = self.member(mid)
        if member is None:
            return None
        total = (member.budget or {}).get('node_hours')
        if not total:
            return None
        return total - node_hours(self.pilot_history(mid), now=now)

    def size_of(self, pilot: PilotRecord) -> PilotSize:
        '''Return the pilot's submit-time shape (backfilled at load).

        The record's own snapshot is authoritative: sizes live *per
        member*, and a removed member's pilots still have to be sized.
        '''
        return PilotSize(nodes            = pilot.nodes,
                         cpus_per_node    = pilot.cpus_per_node,
                         gpus_per_node    = pilot.gpus_per_node,
                         rhapsody_backend = pilot.rhapsody_backend)

    # -- task scratch and input spool ------------------------------------

    def task_scratch_dir(self, task_id: str) -> Path:
        '''Return (creating it) the shared-FS scratch dir for one task.'''
        d = self.scratch_base / task_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def spool_dir(self, task_id: str) -> Path:
        '''Return the broker-local input spool dir for one task.

        Lives under the pool's *state* dir, beside its ``state.json``, so
        it is broker-local by construction whatever the member's
        filesystem looks like.
        '''
        return self.state_dir / 'inputs' / task_id

    def drop_spool(self, task_id: str) -> None:
        '''Delete a task's input spool.  Idempotent.

        Called from every terminal path.  The spool must **survive a
        re-queue** — the task is about to be dispatched somewhere else,
        possibly to another member — so it is keyed on the task, not the
        pilot, and only a terminal state releases it.
        '''
        try:
            shutil.rmtree(self.spool_dir(task_id))
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning('task_dispatcher: could not drop spool for %s: %s',
                        task_id, e)

    def persist(self) -> None:
        '''Rewrite this pool's ``state.json`` atomically.'''
        self.store.save(self.owning_sid, self.config.to_dict(),
                        self.pilots, self.tasks)

    def close(self) -> None:
        '''Release per-pool resources: drop the whole input spool.'''
        try:
            shutil.rmtree(self.state_dir / 'inputs')
        except FileNotFoundError:
            pass
        except OSError as e:
            log.warning('task_dispatcher: could not drop input spool of '
                        'pool %r: %s', self.config.name, e)


# ---------------------------------------------------------------------------
# Session — thin identity handle
# ---------------------------------------------------------------------------

class TaskDispatcherSession(PluginSession):
    '''Session handle — owns this session's pools by lifetime.

    Pool/pilot state lives on :class:`PluginTaskDispatcher`, keyed by
    ``(owning_sid, pool_name)``.  The session is the owner: closing it (owner
    ``lost`` + reclaim-drain, ttl expiry, ``unregister_session``, or
    ``cancel_all``) tears down *this session's* pools — cancelling their pilots
    and marking the durable store — so a pool's pilots follow the session
    lifetime policy.
    '''

    async def close(self) -> dict:
        '''Tear down this session's pools, then run the base close.'''
        if self._plugin is not None:
            await self._plugin._teardown_session_pools(self._sid)
        return await super().close()


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class TaskDispatcherClient(PluginClient):
    '''Application-side client for the task dispatcher plugin.'''

    def register_session(self, pools: list | dict | None = None,
                         sid: str | None = None,
                         lifetime: str | None = None,
                         ttl: float | None = None,
                         **_kwargs: Any) -> None:
        '''Register a session, optionally declaring per-workflow pools.

        *pools* may be a list of pool-config dicts or a dict containing a
        ``pools`` key.  ``None`` registers without declaring pools, which
        causes the dispatcher to auto-materialise this session's built-in
        ``default`` pool (idempotent across sessions).

        *sid* reconnects to (or names) an existing session -- pools are
        keyed per session, so a second client that should see a session's
        pools has to join it by sid rather than mint its own.  *lifetime* /
        *ttl* are the base session-policy fields.
        '''
        body: dict = {}
        if pools is not None:
            body['pools'] = pools
        if sid is not None:
            body['sid'] = sid
        if lifetime is not None:
            body['lifetime'] = lifetime
        if ttl is not None:
            body['ttl'] = ttl
        resp = self._http.post(self._url('register_session'), json=body)
        self._raise(resp)
        self._sid = resp.json()['sid']

    def list_pools(self) -> dict:
        '''List configured pools and their live state (session-less).'''
        resp = self._http.get(self._url('pools'))
        self._raise(resp)
        return resp.json()

    def fleet(self) -> dict:
        '''Return a snapshot of the fleet across all pools (requires session).'''
        self._require_session()
        resp = self._http.get(self._url(f'fleet/{self.sid}'))
        self._raise(resp)
        return resp.json()

    def pool_detail(self, name: str) -> dict:
        '''Return detailed state for one of this session's pools.'''
        self._require_session()
        resp = self._http.get(self._url(f'pool/{self.sid}/{name}'))
        self._raise(resp)
        return resp.json()

    def add_member(self, pool: str, member: dict) -> dict:
        '''Add one member to a class pool.

        Returns ``{pool, member, members, created, updated}``.  An identical
        re-POST is a ``created: False`` no-op, so a restart replay can just
        re-declare everything; one differing only in ``pilot`` answers
        ``updated: True`` and takes the new mode.  Anything else differing
        (``end_time`` included) is a 409.
        '''
        self._require_session()
        resp = self._http.post(
            self._url(f'pool/{self.sid}/{pool}/members'), json=member)
        self._raise(resp, f'add member to pool {pool!r}')
        return resp.json()

    def remove_member(self, pool: str, member_id: str, *,
                      cancel_tasks: bool = False,
                      force: bool = False,
                      fail_unsatisfiable: bool = True) -> dict:
        '''Remove one member from a class pool, draining its pilots.

        Returns ``{pool, member_id, pilots_cancelled, tasks_requeued,
        tasks_failed}``.  Flags travel in the body (the dispatcher routes
        on the path alone).  ``fail_unsatisfiable=False`` is the "this
        member may come back" form: tasks only it could run stay QUEUED.
        '''
        self._require_session()
        body = {'cancel_tasks'      : cancel_tasks,
                'force'             : force,
                'fail_unsatisfiable': fail_unsatisfiable}
        resp = self._http.request(
            'DELETE', self._url(f'pool/{self.sid}/{pool}/members/{member_id}'),
            json=body)
        self._raise(resp, f'remove member {member_id!r} from pool {pool!r}')
        return resp.json()

    def submit_task(self, task_id: str, cmd: list[str],
                    cwd: str | None = None, *,
                    pool: str | None = None, endpoint: str | None = None,
                    priority: int = 0,
                    inputs: list[str] | None = None,
                    outputs: list[str] | None = None,
                    requirements: dict | None = None,
                    inputs_b64: dict | None = None) -> dict:
        '''Submit one task to the dispatcher.

        Exactly one of *pool* or *endpoint* must be given:
            - *pool*: route through a dispatcher-managed pilot pool.
            - *endpoint*: bypass pool management and run directly on the
              target endpoint's rhapsody plugin.  Inputs/outputs are not
              supported in this mode (yet).

        *requirements* is the optional per-task resource shape
        (``cores``/``gpus``/``mem_gb``/``ranks``/``mpi``/``software``/
        ``labels``; see :func:`parse_requirements`).  It is added to the
        payload **only when not None**, so the wire body of a caller that
        does not use it stays byte-identical.  In endpoint mode it is
        shape-validated and then advisory only — the target's backend is
        not known to the dispatcher, so nothing is forwarded.  Note that a
        resubmit of a cached ``DONE``/``RUNNING``/``QUEUED`` task_id
        returns the cached record: changed *requirements* are ignored,
        exactly as a changed *priority* is.

        *cwd* is optional for a **class pool**: omit it and the dispatcher
        assigns one under the placed member's scratch at dispatch time,
        which is the only moment it knows which filesystem the task will
        run on.  Every other mode still requires it.

        *inputs_b64* is ``{filename: base64}`` of files that travel with
        the submit and are placed in the task's cwd before it runs (a
        ``put`` over the pilot's staging plugin for a non-shared member).
        Pool-mode exec-style only.
        '''
        self._require_session()
        if bool(pool) == bool(endpoint):
            raise ValueError(
                'submit_task requires exactly one of pool=... or endpoint=...')
        payload: dict = {
            'task_id' : task_id,
            'cmd'     : cmd,
            'priority': priority,
            'inputs'  : inputs or [],
            'outputs' : outputs or [],
        }
        # Omitted entirely when None, so a class pool can place the task
        # itself -- and so the wire body of an existing caller is unchanged.
        if cwd is not None:
            payload['cwd'] = cwd
        if requirements is not None:
            payload['requirements'] = requirements
        if inputs_b64 is not None:
            payload['inputs_b64'] = inputs_b64
        if pool is not None:
            payload['pool'] = pool
        else:
            payload['endpoint'] = endpoint
        resp = self._http.post(self._url(f'submit/{self.sid}'), json=payload)
        self._raise(resp, f'submit task {task_id!r}')
        return resp.json()

    def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        '''Bulk submit in the rhapsody execution dialect (pool mode).

        Same wire contract as :meth:`RhapsodyClient.submit_tasks` --
        cloudpickle serialization, client-assigned uids, frame-bounded
        batches -- plus a ``pool`` key per task naming its target pool.
        Mixed-pool batches are grouped by the dispatcher.  This is the
        verb rhapsody's ``OrbitExecutionBackend`` calls, which is what
        lets it point at the dispatcher unchanged.

        No signature change for per-task ``requirements``: the key rides in
        each task dict beside ``pool``, and the dispatcher pops both before
        the dict reaches ``BaseTask.from_dict``.  A caller-supplied
        ``task_backend_specific_kwargs`` wins per key over the mapping
        derived from ``requirements`` — the caller knows its backend.
        '''
        self._require_session()

        for td in task_dicts:
            if not td.get('pool'):
                raise ValueError("each task dict requires a 'pool' key")
            RhapsodyClient._serialize_task(td)
            if 'uid' not in td:
                td['uid'] = f'task.{uuid.uuid4().hex[:8]}'

        url = self._url(ROUTE_SUBMIT_RH.format(sid=self.sid))

        # frame-size-bounded batches, same split as the rhapsody client
        batches: list[list[dict]] = []
        batch: list[dict]         = []
        batch_bytes               = 0
        for td in task_dicts:
            td_size = len(str(td)) + 2
            if batch and batch_bytes + td_size > WS_PAYLOAD_LIMIT:
                batches.append(batch)
                batch       = []
                batch_bytes = 0
            batch.append(td)
            batch_bytes += td_size
        if batch:
            batches.append(batch)

        results: list[dict] = []
        for b in batches:
            resp = self._http.post(
                url,
                data=msgpack.packb({'tasks': b}, use_bin_type=True),
                headers={'Content-Type': 'application/msgpack'})
            self._raise(resp, f'submit {len(b)} task(s)')
            results.extend(resp.json())
        return results

    def get_task(self, task_id: str) -> dict:
        '''Fetch the current :class:`TaskRecord` for *task_id*.'''
        self._require_session()
        resp = self._http.get(self._url(f'task/{self.sid}/{task_id}'))
        self._raise(resp)
        return resp.json()

    def cancel_task(self, task_id: str) -> dict:
        '''Cancel a task.  Idempotent on already-terminal records.'''
        self._require_session()
        resp = self._http.post(self._url(f'cancel/{self.sid}/{task_id}'))
        self._raise(resp, f'cancel task {task_id!r}')
        return resp.json()

    def cancel_all(self) -> dict:
        '''Tear down this session's pools: cancel their pilots, drop the pools.

        The explicit reclaim path for ``persistent``/``default`` pools, which
        have no liveness-driven expiry.
        '''
        self._require_session()
        resp = self._http.post(self._url(f'cancel_all/{self.sid}'))
        self._raise(resp, 'cancel_all')
        return resp.json()

    def stage_in(self, pool: str, task_id: str, filename: str,
                 content: bytes, overwrite: bool = False) -> dict:
        '''Upload one file into a task's scratch dir.  Returns ``{cwd, size}``.

        v1 uses a single base64-in-JSON body per file; bulk-transfer
        optimisation is deferred.
        '''
        self._require_session()
        payload = {
            'pool'       : pool,
            'filename'   : filename,
            'content_b64': base64.b64encode(content).decode('ascii'),
            'overwrite'  : overwrite,
        }
        resp = self._http.post(
            self._url(f'stage_in/{self.sid}/{task_id}'), json=payload)
        self._raise(resp, f'stage_in {filename!r}')
        return resp.json()

    def stage_out(self, task_id: str, filename: str) -> bytes:
        '''Download one file from a task's scratch dir.  Returns raw bytes.'''
        self._require_session()
        resp = self._http.get(self._url(
            f'stage_out/{self.sid}/{task_id}/{filename}'))
        self._raise(resp, f'stage_out {filename!r}')
        body = resp.json()
        return base64.b64decode(body['content_b64'])


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginTaskDispatcher(Plugin):
    '''Broker-hosted task dispatcher: elastic pilot pools + per-pool policy.'''

    plugin_name   = 'task_dispatcher'
    session_class = TaskDispatcherSession
    client_class  = TaskDispatcherClient
    version       = '0.0.1'

    ui_config = {
        'icon'          : '📦',
        'title'         : 'Task Dispatcher',
        'description'   : 'Elastic autoscaling task dispatcher: pools, pilots.',
        'refresh_button': True,
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Return whether to load: broker hosts only.

        The dispatcher owns the global pool/pilot/task state, observes topology
        events directly, and proxies psij calls out to login-node endpoints.
        '''
        from .utils import host_role
        return host_role(app)['role'] == 'broker'

    def __init__(self, app: FastAPI,
                 instance_name: str = 'task_dispatcher',
                 state_root: str | os.PathLike | None = None,
                 scratch_root: str | os.PathLike | None = None) -> None:
        super().__init__(app, instance_name)

        self._state_root   = Path(state_root   or _DEFAULT_STATE_ROOT)
        self._scratch_root = Path(scratch_root or _DEFAULT_SCRATCH_ROOT)

        # Broker seam (broker-hosted only): the in-process caller handle and
        # the raw event tap, injected by BrokerPluginHost.  When absent the
        # dispatcher refuses endpoint calls cleanly (child clients return None).
        self._broker_caller = getattr(app.state, 'broker_caller', None)
        self._broker_tap    = getattr(app.state, 'broker_tap', None)
        self._untap         = None

        # Cached child-endpoint plugin clients (sync PSIJClient/RhapsodyClient
        # over the caller), keyed (dst, plugin, backend).  Invalidated for a
        # dst when it goes ``lost``.
        self._child_clients: dict[tuple, Any] = {}

        # endpoint_name → set of loaded plugin names, refreshed on every
        # topology change.  Used to auto-resolve a pool's endpoint_name and to
        # validate the target of an endpoint-mode task submission.
        self._connected_endpoints: dict[str, set[str]] = {}

        # The last rich topology, minus ``lost`` entries (a one-delivery
        # event, not a state).  Adopting an endpoint (plan 122) replays it
        # through ``_reconcile_pilots_for``, so a new record is activated
        # by exactly the rule the topology hook applies.
        self._participants: dict[str, dict] = {}

        # Endpoint-mode task tracking: task_id → target_endpoint_name.
        # Endpoint mode bypasses pool state — the dispatcher is a transparent
        # proxy to the target endpoint's rhapsody.  Backed by one atomic
        # ``endpoint_mode.json`` so an in-flight task survives a broker restart.
        self._endpoint_mode_path = self._state_root / 'endpoint_mode.json'
        self._endpoint_mode_tasks: dict[str, str] = dict(
            read_json(self._endpoint_mode_path, default={}) or {})

        # rhapsody-task-uid → (owning_sid, pool_name, task_id) so the event tap
        # can find the right TaskRecord when a pilot reports completion.
        self._uid_to_task: dict[str, tuple[str, str, str]] = {}

        # Rhapsody-dialect batching (same NOTIFY_WINDOW semantics as
        # plugin_rhapsody): terminal notifications coalesce into
        # ``task_status_batch`` frames, and ledger persists coalesce into one
        # write per dirty pool per flush -- the per-task fsync is the
        # dominant latency cost of pool mode.
        self._notify_window       = _resolve_notify_window()
        self._notify_batch_bytes  = _resolve_frame_cap(app) // BUDGET_RATIO
        self._rh_notify_buf: list[dict]           = []
        self._rh_notify_bytes                     = 0
        self._rh_notify_lock                      = threading.Lock()
        self._rh_flush_scheduled                  = False
        self._dirty_pools: set[tuple[str, str]]   = set()

        # Single housekeeping loop; started once a running loop exists.
        self._started = False
        self._housekeeping_task: asyncio.Task | None = None

        # Pool state, keyed strictly per session: {owning_sid: {pool_name:
        # PoolState}}.  Sessions declare pools via :meth:`register_session`.
        self._pool_states: dict[str, dict[str, PoolState]] = {}

        # Restart-time replay: rebuild pool/pilot/task bookkeeping for ALL
        # sessions from the sid-scoped durable store.
        self._replay_state()

        # Start the housekeeping loop + event tap if a loop is already running
        # (the broker constructs hosted plugins on the running host loop).
        self._maybe_start()

        # Routes
        self.add_route_get  ('pools',                         self._route_pools)
        self.add_route_get  ('pool/{sid}/{name}',             self._route_pool_detail)
        self.add_route_post ('pool/{sid}/{name}/members',     self._route_add_member)
        self.add_route_delete('pool/{sid}/{name}/members/{member_id}',
                             self._route_remove_member)
        self.add_route_get  ('fleet/{sid}',                   self._route_fleet)
        self.add_route_post ('submit/{sid}',                  self._route_submit)
        self.add_route_post (ROUTE_SUBMIT_RH,                 self._route_submit_rh)
        self.add_route_get  ('task/{sid}/{task_id}',          self._route_get_task)
        self.add_route_post ('cancel/{sid}/{task_id}',        self._route_cancel_task)
        self.add_route_post ('cancel_all/{sid}',              self._route_cancel_all)
        self.add_route_post ('stage_in/{sid}/{task_id}',      self._route_stage_in)
        self.add_route_get  ('stage_out/{sid}/{task_id}/{filename}',
                             self._route_stage_out)

    # -- pool bookkeeping helpers ---------------------------------------

    def _pools_for(self, sid: str) -> dict[str, 'PoolState']:
        '''Return this session's pools (empty for a fresh sid).'''
        return self._pool_states.setdefault(sid, {})

    def _all_pools(self):
        '''Iterate every pool across every session.'''
        for pools in list(self._pool_states.values()):
            for ps in list(pools.values()):
                yield ps

    def _find_pool(self, sid: str, name: str) -> 'PoolState | None':
        '''Return this session's pool by name, or ``None`` (session-local).'''
        return self._pool_states.get(sid, {}).get(name)

    # -- materialisation ------------------------------------------------

    def _pool_dir(self, sid: str, cfg: PoolConfig) -> Path:
        '''Return the sid-scoped on-disk state dir for a *freshly declared* pool.

        Used only when no directory is passed in (see
        :meth:`_materialise_pool`): replay always attaches to the directory
        it just read, so a naming rule that depends on mutable config can
        never make replay and declaration disagree and silently lose a
        pool's pilots and tasks.  A legacy pool keeps its exact pre-121
        directory; a class pool gets one that does not move when its
        primary member leaves.
        '''
        tag = 'members' if cfg.multi_member else (cfg.endpoint_name or 'unbound')
        return self._state_root / sid / f'{cfg.name}__{tag}'

    def _scratch_for(self, cfg: PoolConfig) -> Path:
        '''Return the **broker-local** scratch base for *cfg*.

        ``PoolState.__init__`` ``mkdir``s this on the broker host, so for a
        class pool it must never be the primary member's ``scratch_base`` —
        that is a path on someone else's filesystem.  A member's own
        ``scratch_base`` has exactly two readers: the pilot env and the cwd
        assignment at dispatch.
        '''
        if cfg.multi_member:
            return self._scratch_root / cfg.name
        return (Path(cfg.scratch_base).expanduser()
                if cfg.scratch_base
                else self._scratch_root / cfg.name)

    def _materialise_pool(self, sid: str, cfg: PoolConfig,
                          state_dir: Path | None = None) -> 'PoolState':
        '''Create (or return) this session's pool named ``cfg.name``.

        Pools are strictly per-session (keyed ``(sid, cfg.name)``): a
        same-named pool in another session is a *distinct* pool.  Re-declaring
        a pool already present under this session returns the existing
        :class:`PoolState` (idempotent reconnect) — members are **not**
        merged here; the federation POSTs them to the members route, which
        is a no-op when the dispatcher already replayed them.

        *state_dir* is passed by replay so the pool attaches to the exact
        directory its ``state.json`` was read from.
        '''
        if cfg.endpoint_name is None and not cfg.multi_member:
            # Only a legacy pool auto-picks: a class pool's endpoint_name
            # is a projection of its primary member, not a binding.  The
            # pick writes through bind_endpoint so the implicit member is
            # bound too.
            picked = self._pick_endpoint_name()
            if picked:
                cfg.bind_endpoint(picked)
                log.info('[%s] pool %r: endpoint_name auto-resolved to %r',
                         self.instance_name, cfg.name, picked)

        pools    = self._pools_for(sid)
        existing = pools.get(cfg.name)
        if existing is not None:
            return existing

        if state_dir is None:
            state_dir = self._pool_dir(sid, cfg)
        scratch_base = self._scratch_for(cfg)
        ps = PoolState(cfg, state_dir, scratch_base, self, owning_sid=sid)
        pools[cfg.name] = ps

        # Persist immediately so restart-time replay can rebuild this pool.
        ps.persist()

        # Restart recovery: rebuild the uid→task map from the replayed task log
        # so a terminal event for a task that was RUNNING before the restart
        # can still be correlated and advanced.
        for rec in ps.tasks.values():
            if rec.state == TASK_RUNNING and rec.rhapsody_uid:
                self._uid_to_task[rec.rhapsody_uid] = (sid, cfg.name, rec.task_id)

        log.info('[%s] materialised pool %r (sid=%s) → endpoint %r (sizes=%s)',
                 self.instance_name, cfg.name, sid, cfg.endpoint_name,
                 sorted(cfg.pilot_sizes))
        return ps

    def _replay_state(self) -> None:
        '''Rebuild pools for ALL sessions from the sid-scoped durable store.

        Scan ``<state_root>/<sid>/<pool>__<endpoint>/state.json``, reload each
        pool's config, and re-materialise its :class:`PoolState` (which loads
        the pilot/task maps).  Owner reconnection then reconciles naturally; a
        pool whose owner never returns is drained by the reclaim / ttl path.
        '''
        if not self._state_root.exists():
            return
        for sid_dir in sorted(self._state_root.iterdir()):
            if not sid_dir.is_dir():
                continue
            sid = sid_dir.name
            for pool_dir in sorted(sid_dir.iterdir()):
                cfg_path = pool_dir / 'state.json'
                if not (pool_dir.is_dir() and cfg_path.is_file()):
                    continue
                payload = read_json(cfg_path, default=None)
                if not isinstance(payload, dict) or 'config' not in payload:
                    continue
                try:
                    cfg = self._pool_config_from_dict(payload['config'])
                    self._materialise_pool(sid, cfg, state_dir=pool_dir)
                except Exception as e:
                    # Any failure to parse the config, instantiate the policy
                    # (unregistered strategy, bad strategy_config), or
                    # materialise the pool (e.g. OSError on mkdir) must not
                    # abort replay of every other pool — skip this one.
                    log.warning('task_dispatcher: skipping unreplayable pool '
                                '%s: %s', cfg_path, e)
                    continue

    @staticmethod
    def _pool_config_from_dict(d: dict) -> PoolConfig:
        '''Reconstruct a :class:`PoolConfig` from a persisted ``to_dict``.

        ``allow_empty_members=True``: removing the last member with
        ``force`` legitimately produces a member-less class pool and
        persists it, and that state file must replay rather than be
        skipped as unparseable — which would take the pool's pilot and
        task history with it.
        '''
        parsed = parse_pools({'pools': [d]}, source='replay',
                             allow_empty_members=True)
        return next(iter(parsed.values()))

    def _pick_endpoint_name(self) -> str | None:
        '''Auto-pick an endpoint_name for a pool declared without one.

        Policy: lexically first connected endpoint that isn't us (the broker
        endpoint).  Returns ``None`` when no eligible endpoint is available.
        '''
        self_endpoint = getattr(self._app.state, 'endpoint_name', None)
        candidates = sorted(e for e in self._connected_endpoints
                            if e != self_endpoint)
        return candidates[0] if candidates else None

    # -- lifecycle ------------------------------------------------------

    def _maybe_start(self) -> None:
        '''Idempotently start the housekeeping loop + broker event-tap sub.

        Started only when a loop is already running (the broker constructs
        hosted plugins on the running host loop).  Unit tests without a loop
        drive routes and callbacks directly.
        '''
        if self._started:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._started = True
        self._housekeeping_task = loop.create_task(self._housekeeping())

        # Subscribe to the broker's raw event tap for child-pilot task events.
        # The tap fires on the plugin-host loop — the dispatcher's own loop —
        # so terminal handling runs inline with no cross-thread marshalling.
        if self._broker_tap is not None and self._untap is None:
            self._untap = self._broker_tap(self._on_event)

    async def _housekeeping(self) -> None:
        '''One periodic loop: per-pool tick + drain, then handshake + prune.

        Replaces the four separate sweeper loops.  Ticks every
        ``_TICK_INTERVAL_SEC``; reconciles overdue pilot handshakes each tick;
        prunes stale state dirs at most once per ``_PRUNE_INTERVAL_SEC``.

        Only pools whose owning session is *live* are ticked.  ``_replay_state``
        re-materialises every on-disk pool at construction, before any client
        has re-registered its session — those orphans must not scale up on
        their own: a pool with a ``min_pilots`` floor whose owner never comes
        back would otherwise submit pilots forever.  An orphan is still
        drained-free and still pruned; it wakes when its owner re-registers.

        This covers the reserved ``default`` session too — it is created on
        demand, so a pool replayed under ``default`` also stays un-ticked
        until the first request mints that session.  Same rule, not an
        exception to it.
        '''
        last_prune = time.time()
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                now = time.time()
                for ps in list(self._all_pools()):
                    if ps.owning_sid not in self._sessions:
                        continue                 # replayed, owner-less pool
                    ps.policy.on_tick(ps, self._make_submit_pilot(ps))
                    self._drain_pending(ps)
                await self._reconcile_overdue_pilots(now)
                if now - last_prune >= _PRUNE_INTERVAL_SEC:
                    self._prune_stale_state_dirs()
                    last_prune = now
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] housekeeping error: %s',
                              self.instance_name, e)

    async def shutdown(self) -> None:
        '''Cancel the housekeeping loop and drop the event tap, then base close.'''
        # anything still coalescing gets its persist and its notification now
        self._flush_rh()
        if self._housekeeping_task is not None \
                and not self._housekeeping_task.done():
            self._housekeeping_task.cancel()
            try:    await self._housekeeping_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._untap is not None:
            try:    self._untap()
            except Exception:
                pass
            self._untap = None
        await super().shutdown()

    async def _reconcile_overdue_pilots(self, now: float) -> None:
        '''Reconcile PENDING/STARTING pilots overdue for a handshake.

        For each such pilot older than ``_HANDSHAKE_TIMEOUT_SEC``, query psij
        for its job state and mark it FAILED if the job is terminal; an
        adopted pilot has no job to query and is failed outright (see
        :meth:`_reconcile_pilot`).
        '''
        for pool_state in list(self._all_pools()):
            for pilot in list(pool_state.pilots.values()):
                if pilot.state not in (PILOT_PENDING, PILOT_STARTING):
                    continue
                if now - pilot.submitted_at < _HANDSHAKE_TIMEOUT_SEC:
                    continue
                await self._reconcile_pilot(pool_state, pilot)

    def _prune_stale_state_dirs(self) -> None:
        '''Remove state dirs for pools no longer active and older than 30 days.

        The store is sid-scoped: ``<state_root>/<sid>/<pool>__<endpoint>/``.  A
        pool dir is pruned iff it isn't backing an active pool AND its newest
        file is older than ``_STATE_PRUNE_DAYS``; an emptied sid dir is then
        removed too.
        '''
        if not self._state_root.exists():
            return
        cutoff = time.time() - _STATE_PRUNE_DAYS * 86400
        active = {str(ps.state_dir) for ps in self._all_pools()}
        for sid_dir in list(self._state_root.iterdir()):
            if not sid_dir.is_dir():
                continue
            for entry in list(sid_dir.iterdir()):
                if not entry.is_dir() or str(entry) in active:
                    continue
                try:
                    mtimes = [p.stat().st_mtime for p in entry.iterdir()]
                except (FileNotFoundError, PermissionError):
                    continue
                if not mtimes or max(mtimes) >= cutoff:
                    continue
                try:
                    shutil.rmtree(entry)
                    log.info('[%s] pruned stale state dir %s',
                             self.instance_name, entry)
                except OSError as e:
                    log.warning('[%s] could not prune %s: %s',
                                self.instance_name, entry, e)
            try:
                if sid_dir.is_dir() and not any(sid_dir.iterdir()):
                    sid_dir.rmdir()
            except OSError:
                pass

    # -- child plugin clients (sync, over the broker caller) --------------

    def _make_child_client(self, client_cls, plugin: str, dst: str):
        '''Build a sync plugin client whose transport is the broker caller.

        The dispatcher drives psij/rhapsody through the very same
        ``PSIJClient`` / ``RhapsodyClient`` sync helpers the user-thread clients
        run — one implementation of paths, payloads, parsing and error mapping.
        Only the transport differs: :class:`_CallerSyncHTTP` routes over the
        in-process caller.  The client's sync methods are always invoked from
        ``asyncio.to_thread`` so the blocking ``.result()`` never holds the host
        loop.
        '''
        http = _CallerSyncHTTP(self._broker_caller, dst)
        return client_cls(http, f'/{plugin}',
                          endpoint_id=dst, plugin_name=plugin)

    async def _get_child_client(self, dst: str, plugin: str, cls, key,
                                **register_kwargs):
        '''Return a caller-backed *cls* client for *plugin* on *dst*.

        Registers (once) a *plugin* session over the caller, passing
        *register_kwargs* to ``register_session``, and caches the client
        under *key*.  Returns ``None`` when no caller is wired or *dst* /
        its *plugin* is unreachable.
        '''
        if self._broker_caller is None:
            return None
        client = self._child_clients.get(key)
        if client is not None:
            return client
        client = self._make_child_client(cls, plugin, dst)
        try:
            await asyncio.to_thread(client.register_session,
                                    **register_kwargs)
        except Exception as e:
            log.warning('[%s] %s session unavailable on %s: %s',
                        self.instance_name, plugin, dst, e)
            return None
        self._child_clients[key] = client
        return client

    async def _get_psij_client(self, endpoint_name: str):
        '''Return a caller-backed :class:`PSIJClient` for *endpoint_name*.'''
        if not endpoint_name:
            log.warning('[%s] _get_psij_client called with empty endpoint_name',
                        self.instance_name)
            return None
        from .plugin_psij import PSIJClient
        return await self._get_child_client(
            endpoint_name, 'psij', PSIJClient, (endpoint_name, 'psij', None))

    async def _get_rhapsody_client(self, child_endpoint: str,
                                   backend: str | None = None):
        '''Return a caller-backed :class:`RhapsodyClient` for a child.

        The session is registered with *backend* when given, and cached per
        ``(dst, 'rhapsody', backend)``.
        '''
        from .plugin_rhapsody import RhapsodyClient
        return await self._get_child_client(
            child_endpoint, 'rhapsody', RhapsodyClient,
            (child_endpoint, 'rhapsody', backend),
            backends=[backend] if backend else None)

    async def _get_staging_client(self, child_endpoint: str):
        '''Return a caller-backed :class:`StagingClient` for a child.

        It is how a task's inputs (and, for an input-less task, its cwd)
        reach a member whose filesystem the broker does not share.
        '''
        from .plugin_staging import StagingClient
        return await self._get_child_client(
            child_endpoint, 'staging', StagingClient,
            (child_endpoint, 'staging', None))

    # -- routes --------------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Override the base ``register_session`` to accept pool declarations.

        Request body (JSON, all fields optional)::

            {"pools": [<PoolConfig>, ...]}

        Materialisation semantics (strict per-session isolation):

        - The base allocates/reconnects the session ``sid`` first; declared
          pools then materialise under **that** ``sid``.
        - Pools are keyed ``(owning_sid, pool_name)``: a same-named pool in
          another session is a *distinct*, isolated pool.  Re-declaring a pool
          this session already owns returns the existing one.
        - With no pools declared and no pool yet owned by this session, the
          dispatcher auto-materialises this session's own ``default`` pool.
        '''
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}

        pools_body = body.get('pools')

        # Validate/parse the declared pools BEFORE minting the session, so a
        # bad declaration 400s without leaving a dangling session behind.
        configs: dict = {}
        if pools_body is not None:
            if isinstance(pools_body, list):
                wrapped = {'pools': pools_body}
            elif isinstance(pools_body, dict) and 'pools' in pools_body:
                wrapped = pools_body
            else:
                raise HTTPException(
                    status_code=400,
                    detail="'pools' must be a list or {'pools': [...]}")
            try:
                configs = parse_pools(wrapped, source='register_session')
            except PoolConfigError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

        # Base sid-allocation / owner-check / cleanup runs first so pools can
        # be keyed on the resolved sid.
        result = await super().register_session(request)
        sid    = result['sid']

        if configs:
            for cfg in configs.values():
                self._materialise_pool(sid, cfg)
        elif pools_body is None and not self._pools_for(sid):
            # No pools declared; auto-materialise this session's default once.
            self._materialise_pool(sid, default_pool_config())

        return result

    async def _route_pools(self, request: Request) -> dict:
        '''List all pools across sessions, grouped by owning session id.'''
        return {
            'pools': {
                sid: {name: self._summarize_pool(ps)
                      for name, ps in pools.items()}
                for sid, pools in self._pool_states.items()
            }
        }

    async def _route_pool_detail(self, request: Request) -> dict:
        '''Return detailed state for one of this session's pools by name.

        Session-scoped (like ``fleet/{sid}``): a pool is addressable only under
        its owning session, never by bare name across sessions.
        '''
        sid  = request.path_params['sid']
        self._require_known_session(sid)
        name = request.path_params['name']
        ps = self._find_pool(sid, name)
        if ps is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown pool: {name}')
        return self._summarize_pool(ps, verbose=True)

    # -- member routes -------------------------------------------------

    def _require_class_pool(self, sid: str, name: str) -> PoolState:
        '''Return this session's *class* pool by name, or raise 404/409.'''
        self._require_known_session(sid)
        ps = self._find_pool(sid, name)
        if ps is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown pool: {name}')
        if not ps.config.multi_member:
            # A single-member pool is never promoted in place: promotion
            # would have to rename the implicit member, rewrite every live
            # pilot's child_endpoint_name (impossible -- the child is
            # already registered under it), and move the pool's state dir.
            raise HTTPException(
                status_code=409,
                detail=f"pool {name} is not a class pool; declare it with "
                       f"'members'")
        return ps

    @staticmethod
    def _member_fingerprint(member: PoolMember) -> dict:
        '''Return a declaration-equality view of a member.

        Used only to decide whether a re-POST is the *same* declaration.
        List-valued attributes (``software``, by convention) are compared
        **order-insensitively**: a federation that rebuilds its member
        declarations from a set or a dict on restart would otherwise emit
        ``['pytorch', 'lammps']`` where it once emitted the reverse and get
        a 409 for a member that has not changed.  Every other field is
        compared verbatim.
        '''
        d = asdict(member)
        d['attributes'] = {
            k: sorted(v) if isinstance(v, list) else v
            for k, v in (d.get('attributes') or {}).items()
        }
        return d

    async def _route_add_member(self, request: Request) -> dict:
        '''Add one member to a class pool.

        Body is one member declaration.  An **identical** re-POST is a
        ``200 {"created": false}`` no-op, which is what makes the
        federation's restart replay idempotent; one that differs **only** in
        ``pilot`` updates it in place and answers
        ``200 {"created": false, "updated": true}``; any other difference is
        a 409.  There is no other partial update: a member is replaced by
        removing and re-adding it.

        ``pilot`` may change in place (plan 122) because an upgraded
        federation re-POSTs its allocation members as ``endpoint`` against a
        pool this dispatcher replayed as ``submit``.  The comparison applies
        it to the existing member and re-runs its ``__post_init__``, so the
        bound check on a mode switch bites for ``endpoint`` -> ``submit``
        only: the existing member's forced 1/1 is what the new declaration
        must carry, and a switch that *also* changes a bound is a 409 like
        any other change.  For ``submit`` -> ``endpoint`` ``parse_member``
        has already forced the new declaration's bounds to 1/1 before the
        comparison, so whatever bounds it declared never differ.

        No pilot is submitted here — the next housekeeping tick applies
        the new member's ``min_pilots`` floor.  A member switched to
        ``pilot: endpoint`` therefore adopts its endpoint on that tick.
        '''
        sid  = request.path_params['sid']
        name = request.path_params['name']
        ps   = self._require_class_pool(sid, name)

        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            raise HTTPException(status_code=400,
                                detail='member declaration must be an object')

        try:
            member = parse_member(body, f'pool {name}: member',
                                  pool_name=name)
        except PoolConfigError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        existing = ps.config.members.get(member.member_id)
        created  = existing is None
        if not created:
            if self._member_fingerprint(replace(existing, pilot=member.pilot)) \
                    != self._member_fingerprint(member):
                raise HTTPException(
                    status_code=409,
                    detail='member exists with a different declaration')
            if existing.pilot == member.pilot:
                # everything else matched above: an identical re-POST
                return {'pool'   : name,
                        'member' : asdict(existing),
                        'members': list(ps.config.members),
                        'created': False,
                        'updated': False}

        # a new member, or only ``pilot`` differs: the new one holds from
        # the next tick on
        ps.config.members[member.member_id] = member
        ps.config.reproject()
        ps.persist()

        self._dispatch_notify('pool_members', {
            'pool'      : name,
            'sid'       : sid,
            'action'    : 'add' if created else 'update',
            'member_id' : member.member_id,
            'member_ids': list(ps.config.members),
        })
        log.info('[%s] pool %r (sid=%s): %s member %r on endpoint %r '
                 '(pilot=%s)', self.instance_name, name, sid,
                 'added' if created else 'updated', member.member_id,
                 member.endpoint_name, member.pilot)

        return {'pool'   : name,
                'member' : asdict(member),
                'members': list(ps.config.members),
                'created': created,
                'updated': not created}

    async def _route_remove_member(self, request: Request) -> dict:
        '''Remove one member from a class pool, draining its pilots.

        Body (all optional)::

            {"cancel_tasks": false, "force": false,
             "fail_unsatisfiable": true}

        Flags travel in the **body**, not the query string: the
        dispatcher's own HTTP surface routes on the path alone, so a
        ``?flag=true`` would simply not match the route.

        Order matters — the member is dropped **before** the first
        ``await``.  The cancels are psij round-trips, and a housekeeping
        tick landing in that window would otherwise see the member still
        declared, still below its ``min_pilots`` floor, and submit a
        replacement pilot for the member being removed.  Pausing its
        pilots first stops any other path from dispatching onto a pilot
        that is about to die.
        '''
        sid  = request.path_params['sid']
        name = request.path_params['name']
        mid  = request.path_params['member_id']
        ps   = self._require_class_pool(sid, name)

        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        cancel_tasks       = bool(body.get('cancel_tasks', False))
        force              = bool(body.get('force', False))
        fail_unsatisfiable = bool(body.get('fail_unsatisfiable', True))

        if mid not in ps.config.members:
            raise HTTPException(status_code=404,
                                detail=f'unknown member: {mid}')
        if len(ps.config.members) == 1 and not force:
            raise HTTPException(
                status_code=409,
                detail='cannot remove the last member of a pool without '
                       "'force'")

        # The reported counts are read off the task states at the end, not
        # tallied along the way: a task can also fail inside
        # ``_do_pilot_cancel`` (``_finalize_pilot``: re-queued too often).
        live = {tid for tid, t in ps.tasks.items() if not t.is_terminal()}

        # -- 1. synchronous, before the first await ----------------------
        doomed = ps.live_pilots_for(mid)
        for pilot in doomed:
            pilot.accepting_new_tasks = False
        ps.config.members.pop(mid)
        ps.config.reproject()
        try:
            ps.policy.on_member_removed(mid)
        except Exception as e:
            log.exception('[%s] on_member_removed raised: %s',
                          self.instance_name, e)
        ps.persist()

        doomed_pids = {p.pid for p in doomed}
        touched     = {t.task_id for t in ps.tasks.values()
                       if t.pilot_id in doomed_pids}

        # -- 2. cancel_tasks, still before the first await ---------------
        # These tasks must reach a terminal state now: the moment the
        # cancels below re-queue them, any other path on the event loop
        # may dispatch them to a sibling member, and failing them
        # afterwards would kill a task that is already running elsewhere.
        # Terminal tasks are skipped by the re-queue branch, so this also
        # keeps `tasks_requeued` honest.
        if cancel_tasks:
            for tid in sorted(touched):
                task = ps.tasks.get(tid)
                if task is not None and not task.is_terminal():
                    self._mark_task_failed(
                        ps, task, 'member removed with cancel_tasks')

        # -- 3. cancel the member's pilots (re-queues their tasks) -------
        cancelled = 0
        for pilot in doomed:
            try:
                await self._do_pilot_cancel(ps, pilot)
                cancelled += 1
            except Exception as e:
                log.warning('[%s] pilot %s cancel failed on member '
                            'removal: %s', self.instance_name, pilot.pid, e)

        # -- 4. unsatisfiable sweep --------------------------------------
        # The runtime counterpart of the submit-time gate: a task whose
        # only capable member just left would otherwise sit QUEUED
        # forever.  With fail_unsatisfiable=false those tasks stay QUEUED
        # and wait for the member to come back.
        if fail_unsatisfiable:
            members = ps.members()
            for task in list(ps.tasks.values()):
                if task.state != TASK_QUEUED:
                    continue
                reason = _no_member_reason(members, task.requirements)
                if reason is None:
                    continue
                self._mark_task_failed(
                    ps, task, f'{_NO_MEMBER_SATISFIES}: {reason}')

        def _count(tids, state):
            return sum(1 for tid in tids
                       if (ps.tasks.get(tid) is not None
                           and ps.tasks[tid].state == state))

        failed   = _count(live,    TASK_FAILED)
        requeued = _count(touched, TASK_QUEUED)
        ps.persist()

        self._dispatch_notify('pool_members', {
            'pool'      : name,
            'sid'       : sid,
            'action'    : 'remove',
            'member_id' : mid,
            'member_ids': list(ps.config.members),
        })

        # -- 5. drain explicitly: _finalize_pilot re-queues but does not
        # drain, so without this the re-queued tasks would wait a full
        # housekeeping tick before landing on a sibling member.
        self._drain_pending(ps)

        log.info('[%s] pool %r (sid=%s): removed member %r '
                 '(pilots=%d, requeued=%d, failed=%d)',
                 self.instance_name, name, sid, mid,
                 cancelled, requeued, failed)

        return {'pool'            : name,
                'member_id'       : mid,
                'pilots_cancelled': cancelled,
                'tasks_requeued'  : requeued,
                'tasks_failed'    : failed}

    async def _route_fleet(self, request: Request) -> dict:
        '''Return this session's fleet snapshot.  Strictly isolated.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        return {
            'pools': {
                name: self._summarize_pool(ps, verbose=True)
                for name, ps in self._pool_states.get(sid, {}).items()
            }
        }

    async def _route_submit(self, request: Request) -> dict:
        '''Submit a task in pool mode or endpoint mode.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        body = await request.json()

        pool_name   = body.get('pool')
        target_endpoint = body.get('endpoint')
        task_id     = body.get('task_id')
        cmd         = body.get('cmd')
        cwd         = body.get('cwd')

        # Mutual exclusion: exactly one of 'pool' / 'endpoint' is required.
        if bool(pool_name) == bool(target_endpoint):
            raise HTTPException(
                status_code=400,
                detail="submit requires exactly one of 'pool' or 'endpoint'")

        # Resolve the pool before the cwd check: a class pool may place the
        # task itself, so 'cwd' is optional there (plan 121 §8 rule 2).
        pool_state = self._find_pool(sid, pool_name) if pool_name else None
        if pool_name and not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')
        cwd_optional = (pool_state is not None
                        and pool_state.config.multi_member)

        if not task_id or not cmd or (not cwd and not cwd_optional):
            raise HTTPException(
                status_code=400,
                detail="submit requires 'task_id', 'cmd', 'cwd'")

        # ---------- endpoint mode: transparent proxy to target's rhapsody ----
        if target_endpoint:
            return await self._route_submit_endpoint_mode(
                target_endpoint, task_id, cmd, cwd, body)

        # ---------- pool mode: dispatcher-managed pilot fleet ------------
        priority = int(body.get('priority', 0))
        inputs   = list(body.get('inputs',  []) or [])
        outputs  = list(body.get('outputs', []) or [])

        # An explicit cwd names a path on a filesystem the dispatcher would
        # have to guess at: refuse it as soon as any member is non-shared.
        if cwd and any(not m.shared_fs for m in pool_state.members()):
            raise HTTPException(
                status_code=400,
                detail='explicit cwd is not valid for a pool with '
                       'non-shared members')

        # Validation runs BEFORE the resubmit cache ladder below, so a
        # malformed 'requirements' (or a bad 'inputs_b64') is a 400 even
        # when the task_id is a cached DONE — a bad request stays a bad
        # request.
        requirements = _validated_requirements(body.get('requirements'),
                                               pool_state.config)

        decoded = self._decode_inputs_b64(body.get('inputs_b64'))

        # Cached-state behaviour on resubmit:
        #   DONE            → return cached (crash-recovery)
        #   RUNNING/QUEUED  → attach to existing wait (wrapper reconnect)
        #   FAILED/CANCELED → overwrite, re-execute (retry)
        existing = pool_state.tasks.get(task_id)
        if existing is not None:
            if existing.state == TASK_DONE:
                log.info('[%s] task %s DONE cached; returning without '
                         're-execution', self.instance_name, task_id)
                return self._task_dict(existing)
            if existing.state in (TASK_RUNNING, TASK_QUEUED):
                # NOTE: the cached record wins — a resubmit with *changed*
                # requirements is silently ignored, exactly as a changed
                # priority is.  There is deliberately no mutation path.
                log.info('[%s] task %s already %s; attaching',
                         self.instance_name, task_id, existing.state)
                return self._task_dict(existing)
            # FAILED / CANCELED → re-execute: fall through and overwrite

        spooled = self._write_spool(pool_state, task_id, decoded)

        now = time.time()
        record = TaskRecord(
            task_id      = task_id,
            pool         = pool_name,
            owning_sid   = sid,
            cmd          = list(cmd),
            cwd          = str(cwd or ''),
            cwd_assigned = not cwd,
            priority     = priority,
            inputs       = inputs,
            outputs      = outputs,
            spooled      = spooled,
            requirements = requirements,
            state        = TASK_QUEUED,
            submitted_at = now,
            arrival_ts   = now,
        )
        pool_state.tasks[task_id] = record
        pool_state.persist()

        self._dispatch_notify('task_status', self._task_dict(record))

        # Drain any ready dispatches now (the policy scales up on the tick).
        self._drain_pending(pool_state)

        return self._task_dict(record)

    async def _route_submit_endpoint_mode(
            self, target_endpoint: str, task_id: str,
            cmd: list, cwd: str, body: dict) -> dict:
        '''Endpoint-mode submit: transparent proxy to target's rhapsody.

        No pool, no state log, no pilot fleet — the dispatcher just forwards the
        task to the target endpoint's rhapsody session and records
        ``task_id -> target_endpoint`` so subsequent get/cancel can route back.
        The mapping is cleared when the task hits a terminal state.

        ``requirements`` here is **advisory only**: it is shape-validated
        (so a typo is still a 400) and then dropped.  ``_get_rhapsody_client``
        is called with no backend argument, so the endpoint picks its own
        default and never reports it back — there is no backend name for
        :func:`backend_kwargs` to key on.  Nothing is stored, nothing is
        forwarded, and the forwarded task dict and the response body are
        unchanged from a submit without the key.
        '''
        plugins = self._connected_endpoints.get(target_endpoint)
        if plugins is None:
            raise HTTPException(
                status_code=404,
                detail=f'unknown endpoint: {target_endpoint}')
        if 'rhapsody' not in plugins:
            raise HTTPException(
                status_code=503,
                detail=f'endpoint {target_endpoint} cannot run tasks')
        if body.get('inputs') or body.get('outputs'):
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')
        if body.get('inputs_b64'):
            # Endpoint mode has no scratch the dispatcher owns -- staging
            # is already refused here for the same reason.
            raise HTTPException(
                status_code=400,
                detail='inputs_b64 is only supported for pool-mode '
                       'exec tasks')

        # Shape validation only — no pool, hence no fit check and no
        # backend gate.  A non-empty block earns one advisory log line.
        requirements = _validated_requirements(body.get('requirements'))
        if requirements:
            log.info('[%s] endpoint-mode task %s: requirements are advisory '
                     '(target backend unknown); nothing forwarded',
                     self.instance_name, task_id)

        rh = await self._get_rhapsody_client(target_endpoint)
        if rh is None:
            raise HTTPException(
                status_code=503,
                detail=f'rhapsody client unavailable on {target_endpoint}')

        task_dict = {
            'uid'       : task_id,
            'executable': cmd[0] if cmd else '',
            'arguments' : list(cmd[1:]) if len(cmd) > 1 else [],
            'cwd'       : cwd,
            'task_backend_specific_kwargs': {'cwd': cwd},
        }
        # map before submitting, as in pool mode: the task can finish before
        # the threaded submit call returns
        self._endpoint_mode_tasks[task_id] = target_endpoint
        self._persist_endpoint_mode()
        try:
            result = await asyncio.to_thread(rh.submit_tasks, [task_dict])
        except Exception as e:
            log.exception('[%s] endpoint-mode submit to %s failed: %s',
                          self.instance_name, target_endpoint, e)
            if self._endpoint_mode_tasks.pop(task_id, None):
                self._persist_endpoint_mode()
            raise HTTPException(
                status_code=502,
                detail=f'rhapsody submit failed on '
                       f'{target_endpoint}: {e}') from e

        return {
            'task_id' : task_id,
            'endpoint': target_endpoint,
            'state'   : TASK_RUNNING,
            'cmd'     : list(cmd),
            'cwd'     : str(cwd),
            'result'  : result[0] if result else None,
        }

    async def _route_submit_rh(self, request: Request) -> list[dict]:
        '''Bulk submit in the rhapsody execution dialect.

        Body: ``{"tasks": [<task dict>, ...]}`` — rhapsody's own wire format
        (cloudpickled fields as base64 strings, client-assigned ``uid``),
        plus a ``pool`` key per task.  A mixed-pool batch is grouped here;
        the dicts are forwarded verbatim to pilot rhapsody sessions, so the
        dispatcher never deserializes a function body.

        One ledger persist per touched pool, however many tasks the batch
        carries.  The response is rhapsody-shaped acks (``{uid, state}``),
        and completions arrive as ``task_status`` / ``task_status_batch``
        notifications under this plugin's namespace — the same consumer
        contract as plugin_rhapsody.
        '''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        data       = await request.json()
        task_dicts = data.get('tasks', [])

        # validate the whole batch before touching any state
        grouped: dict[str, list[tuple]] = {}
        for td in task_dicts:
            uid       = td.get('uid')
            pool_name = td.get('pool')
            if not uid or not pool_name:
                raise HTTPException(
                    status_code=400,
                    detail="each task requires 'uid' and 'pool'")
            ps = self._find_pool(sid, pool_name)
            if ps is None:
                raise HTTPException(
                    status_code=404,
                    detail=f'unknown pool: {pool_name}')
            # Validate requirements inside the whole-batch loop, so one bad
            # task rejects the batch before any state is touched.
            req = _validated_requirements(td.get('requirements'), ps.config)
            if td.get('inputs_b64'):
                # A rhapsody task's cwd is opaque to the dispatcher, so it
                # has nowhere to place them.
                raise HTTPException(
                    status_code=400,
                    detail='inputs_b64 is only supported for pool-mode '
                           'exec tasks')
            if ps.config.multi_member and not td.get('cwd'):
                # The dispatcher assigns a cwd at dispatch for exec-style
                # tasks only: a rhapsody task dict is forwarded verbatim
                # and never rewritten.
                raise HTTPException(
                    status_code=400,
                    detail="rhapsody-dialect tasks must carry an explicit "
                           "'cwd'; the dispatcher assigns one only for "
                           "exec-style tasks")
            grouped.setdefault(pool_name, []).append((td, req))

        now  = time.time()
        acks = []
        for pool_name, tds in grouped.items():
            pool_state = self._find_pool(sid, pool_name)
            if pool_state is None:       # validated above; mollify the checker
                continue
            fresh = False
            for td, req in tds:
                uid = str(td['uid'])

                # same resubmit semantics as exec-mode submit: DONE is
                # cached, live attaches, FAILED/CANCELED re-executes
                existing = pool_state.tasks.get(uid)
                if existing is not None and existing.state in (
                        TASK_DONE, TASK_RUNNING, TASK_QUEUED):
                    acks.append({'uid': uid, 'state': existing.state})
                    continue

                td = dict(td)
                td.pop('pool', None)
                # Promote 'requirements' to the record field and drop it from
                # the forwarded dict: BaseTask.from_dict keeps unknown keys
                # verbatim and nobody reads them, so a stray block would be a
                # silent no-op riding the wire.  The record gets the
                # validated block (with any derived 'cores'), not the raw one.
                td.pop('requirements', None)
                pool_state.tasks[uid] = TaskRecord(
                    task_id      = uid,
                    pool         = pool_name,
                    owning_sid   = sid,
                    cmd          = [],
                    cwd          = '',
                    task_dict    = td,
                    requirements = req,
                    state        = TASK_QUEUED,
                    submitted_at = now,
                    arrival_ts   = now,
                )
                acks.append({'uid': uid, 'state': TASK_QUEUED})
                fresh = True

            if fresh:
                pool_state.persist()          # once per pool, not per task
                self._drain_pending(pool_state)

        return acks

    async def _route_get_task(self, request: Request) -> dict:
        '''Return one task's record (pool mode) or rhapsody info (endpoint mode).'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']

        # Endpoint mode: forward to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await self._get_rhapsody_client(endpoint_name)
            if rh is None:
                raise HTTPException(
                    status_code=503,
                    detail=f'rhapsody client unavailable on {endpoint_name}')
            try:
                info = await asyncio.to_thread(rh.get_task, task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody get_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name,
                    'result': info}

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return self._task_dict(rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_task(self, request: Request) -> dict:
        '''Cancel one task (pool mode) or forward the cancel (endpoint mode).'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']

        # Endpoint mode: forward cancel to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await self._get_rhapsody_client(endpoint_name)
            if rh is None:
                raise HTTPException(
                    status_code=503,
                    detail=f'rhapsody client unavailable on {endpoint_name}')
            try:
                info = await asyncio.to_thread(rh.cancel_task, task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody cancel_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name,
                    'result': info}

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return await self._cancel_task(ps, rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_all(self, request: Request) -> dict:
        '''Tear down this session's pools (cancel pilots, drop the pools).

        The explicit reclaim path for ``persistent``/``default`` pools, which
        have no liveness-driven expiry.  Idempotent.
        '''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        n = await self._teardown_session_pools(sid)
        return {'sid': sid, 'pools_reclaimed': n}

    @staticmethod
    def _decode_inputs_b64(raw: Any) -> dict[str, bytes]:
        '''Validate and decode an ``inputs_b64`` block from a submit body.

        ``{"<filename>": "<base64>"}``.  Filenames go through the same
        :meth:`_check_filename` the staging route uses; a value that does
        not decode is a 400 with the same wording ``stage_in`` answers
        with.  Sizes are measured on the **decoded** bytes and capped per
        file (:data:`_MAX_INPUT_BYTES`) and per block
        (:data:`_MAX_INPUTS_BYTES`), both 413.
        '''
        if not raw:
            return {}
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400,
                                detail='inputs_b64 must be a mapping of '
                                       'filename to base64 content')

        out: dict[str, bytes] = {}
        total = 0
        for name, value in raw.items():
            PluginTaskDispatcher._check_filename(
                name if isinstance(name, str) else '')
            try:
                content = base64.b64decode(value, validate=True)
            except (ValueError, TypeError) as e:
                raise HTTPException(
                    status_code=400, detail=f'invalid base64: {e}') from e
            if len(content) > _MAX_INPUT_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f'inputs_b64 file {name!r} is '
                           f'{len(content)} bytes; the limit is '
                           f'{_MAX_INPUT_BYTES} bytes per file')
            total += len(content)
            if total > _MAX_INPUTS_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f'inputs_b64 exceeds {_MAX_INPUTS_BYTES} bytes '
                           f'in total at file {name!r}')
            out[name] = content
        return out

    @staticmethod
    def _write_spool(pool_state: PoolState, task_id: str,
                     decoded: dict[str, bytes]) -> list[str]:
        '''Write decoded inputs into the task's broker-local spool.

        A resubmit of a FAILED/CANCELED task overwrites the spool.
        Returns the names, for :attr:`TaskRecord.spooled`.
        '''
        if not decoded:
            return []
        spool = pool_state.spool_dir(task_id)
        try:
            shutil.rmtree(spool)
        except FileNotFoundError:
            pass
        try:
            spool.mkdir(parents=True, exist_ok=True)
            for name, content in decoded.items():
                (spool / name).write_bytes(content)
        except OSError as e:
            raise HTTPException(
                status_code=500,
                detail=f'could not spool task inputs: {e}') from e
        return list(decoded)

    @staticmethod
    def _check_broker_local(pool_state: PoolState,
                            rec: TaskRecord | None) -> None:
        '''Refuse dispatcher staging for a task the broker cannot reach.

        The dispatcher's ``stage_in``/``stage_out`` are **broker-local
        only**; anything cross-host has to go through the pilot's own
        staging plugin.  No record → unchanged legacy behaviour (the route
        has always staged into the pool scratch for any id).

        A **rhapsody-dialect** record is treated exactly like no record:
        its ``cwd`` lives inside the opaque task dict and its own ``cwd``
        field is always ``''``, so applying the "not yet placed" rule to it
        would turn every dialect task's stage_in into a 409 — a legacy
        behaviour change, not a safety gain.
        '''
        if rec is None or rec.task_dict is not None:
            return
        if not rec.cwd:
            raise HTTPException(status_code=409,
                                detail='task not yet placed')
        member = pool_state.member(rec.member_id)
        if member is not None and not member.shared_fs:
            pilot = pool_state.pilots.get(rec.pilot_id or '')
            child = (pilot.child_endpoint_name if pilot else None) or '?'
            raise HTTPException(
                status_code=409,
                detail=f'staging for this task is not broker-local; use '
                       f'the pilot\'s staging plugin at {child}')

    @staticmethod
    def _check_filename(name: str) -> None:
        '''Reject a staging filename with a slash, ``..``, or empty component.

        Files live at the top of the task scratch dir; relative subpaths are
        not supported (they complicate safety).
        '''
        if '/' in name or '\\' in name or name in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for staging: {name!r}')

    async def _route_stage_in(self, request: Request) -> dict:
        '''Upload one base64 file into a pool task's scratch dir.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']
        body    = await request.json()

        if task_id in self._endpoint_mode_tasks:
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        pool_name   = body.get('pool')
        filename    = body.get('filename')
        content_b64 = body.get('content_b64')
        overwrite   = bool(body.get('overwrite', False))

        if not pool_name or not filename or content_b64 is None:
            raise HTTPException(
                status_code=400,
                detail="stage_in requires 'pool', 'filename', 'content_b64'")

        pool_state = self._find_pool(sid, pool_name)
        if not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')

        self._check_filename(filename)

        try:
            content = base64.b64decode(content_b64)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f'invalid base64: {e}') from e

        # The dispatcher's own staging is broker-local.  Refusals are
        # conditional on a record existing: this route has always staged
        # into the pool scratch for *any* id, before any submit, and that
        # is relied upon.
        rec = pool_state.tasks.get(task_id)
        self._check_broker_local(pool_state, rec)

        # An empty ``cwd`` means the record cannot say where the task will
        # run (a rhapsody-dialect task, whose cwd is inside its opaque task
        # dict).  ``_check_broker_local`` has already 409'd every *other*
        # unplaced record, so falling back to the pool scratch here is the
        # unchanged legacy path -- and never ``Path('')``, which would
        # resolve to the broker's working directory.
        if rec is not None and rec.cwd:
            scratch = Path(rec.cwd)
            scratch.mkdir(parents=True, exist_ok=True)
        else:
            scratch = pool_state.task_scratch_dir(task_id)
        path = scratch / filename
        if path.exists() and not overwrite:
            raise HTTPException(
                status_code=409,
                detail=f'file exists (set overwrite=true): {path}')

        path.write_bytes(content)
        return {'cwd': str(scratch), 'size': len(content)}

    async def _route_stage_out(self, request: Request) -> dict:
        '''Download one file from a pool task's scratch dir.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id  = request.path_params['task_id']
        filename = request.path_params['filename']

        if task_id in self._endpoint_mode_tasks:
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        self._check_filename(filename)

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is None:
                continue
            self._check_broker_local(ps, rec)
            # Use the record's own cwd: recomputing ``scratch_base /
            # task_id`` is wrong for any task with an explicit or
            # dispatcher-assigned cwd.  An empty cwd (a rhapsody-dialect
            # task) falls back to it, as before -- never ``Path('')``.
            base = Path(rec.cwd) if rec.cwd else ps.scratch_base / task_id
            path = base / filename
            if not path.is_file():
                raise HTTPException(
                    status_code=404,
                    detail=f'output not found: {path}')
            content = path.read_bytes()
            return {
                'filename'   : filename,
                'size'       : len(content),
                'content_b64': base64.b64encode(content).decode('ascii'),
            }

        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def on_topology_change(self, participants: dict) -> None:
        '''Rich-topology hook: bind pilots + honour the owner-session reclaim.

        The broker delivers the rich topology
        (``name -> {role, plugins, liveness}``) on every change, synthesizing a
        ``liveness == 'lost'`` entry for a participant that vanishes after the
        grace.  Two concerns share this signal:

        - **Pilot child liveness.**  A child that becomes ``present`` activates
          a PENDING/STARTING pilot; ``suspect`` pauses scheduling to it (a blip
          must not tear it down); ``lost`` finalises the pilot — DONE if
          walltime elapsed, else FAILED — reclaiming its capacity and
          re-enqueuing unfinished tasks.
        - **Owner-session reclaim.**  ``super().on_topology_change`` arms the
          reclaim-drain for a *session owner* declared ``lost``; the drain then
          closes the session, which tears down its pools.

        Also refreshes the cached per-endpoint plugin set and the cached
        topology from the non-lost participants.
        '''
        participants = participants or {}

        # Refresh {endpoint_name: set(plugin_names)} from the non-lost,
        # non-self participants.
        self_name = getattr(self._app.state, 'endpoint_name', None)
        new:  dict[str, set[str]] = {}
        live: dict[str, dict]     = {}
        for name, info in participants.items():
            if name == self_name:
                continue
            if (info or {}).get('liveness') == 'lost':
                # Drop cached plugin-clients on a lost endpoint so a
                # reconnecting one re-registers fresh.
                for key in [k for k in self._child_clients if k[0] == name]:
                    self._child_clients.pop(key, None)
                continue
            plugins = (info or {}).get('plugins', {})
            if isinstance(plugins, dict):
                plugins = list(plugins.keys())
            new[name]  = set(plugins)
            live[name] = info
        self._connected_endpoints = new
        self._participants        = live

        for ps in self._all_pools():
            self._reconcile_pilots_for(ps, participants)

        # Owner-session reclaim-drain: arms on a *lost* owner of ephemeral
        # sessions, cancels on its return.
        await super().on_topology_change(participants)

    def _reconcile_pilots_for(self, ps: 'PoolState',
                              participants: dict) -> None:
        '''Apply child-endpoint liveness to one pool's pilots.'''
        for pilot in list(ps.pilots.values()):
            ce = pilot.child_endpoint_name
            if not ce or pilot.state not in PILOT_LIVE_STATES:
                continue
            info     = participants.get(ce)
            liveness = (info or {}).get('liveness') if info else None

            if liveness == 'present':
                # Un-pause a pilot paused on a suspect blip.
                if pilot.state == PILOT_ACTIVE and not pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = True
                    ps.persist()
                    self._drain_pending(ps)
                if pilot.state in (PILOT_PENDING, PILOT_STARTING):
                    self._activate_pilot(ps, pilot)

            elif liveness == 'suspect':
                # Pause scheduling to a suspect child (do NOT demote).
                if pilot.state == PILOT_ACTIVE and pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = False
                    ps.persist()

            elif liveness == 'lost':
                if pilot.adopted:
                    # An adopted endpoint that goes away has left, or its
                    # allocation ended -- neither is a pilot failure,
                    # whatever the deadline says.
                    self._mark_pilot_done(ps, pilot, 'adopted endpoint gone')
                elif time.time() >= pilot.walltime_deadline:
                    self._mark_pilot_done(ps, pilot, 'walltime reached')
                else:
                    self._mark_pilot_failed(
                        ps, pilot, 'child endpoint lost before walltime')
                self._drain_pending(ps)

    def _activate_pilot(self, ps: PoolState, pilot: PilotRecord) -> None:
        '''Transition a PENDING/STARTING pilot to ACTIVE on child handshake.'''
        capacity = pilot.nodes * pilot.cpus_per_node
        if capacity <= 0:
            log.warning('[%s] cannot bind pilot %s: pool size %r has zero '
                        'capacity', self.instance_name, pilot.pid,
                        pilot.size_key)
            if pilot.adopted:
                # Nothing will ever change that for an adopted endpoint;
                # left PENDING, the sweeper would later fail it with a
                # misleading "endpoint not connected".
                self._mark_pilot_failed(
                    ps, pilot, f'pilot size {pilot.size_key!r} has zero '
                               f'capacity')
            return
        if pilot.adopted and 'rhapsody' not in \
                self._connected_endpoints.get(pilot.child_endpoint_name, ()):
            # A submitted pilot's child is our own wrapper; an adopted
            # endpoint is whatever joined, and without rhapsody every task
            # dispatched to it would fail.
            self._mark_pilot_failed(
                ps, pilot, f'endpoint {pilot.child_endpoint_name} serves '
                           f'no rhapsody')
            return

        old_state = pilot.state
        pilot.capacity  = capacity
        pilot.state     = PILOT_ACTIVE
        pilot.active_at = time.time()
        ps.persist()

        if pilot.active_at and pilot.submitted_at:
            log.info('[%s] pilot %s registered as %s; lag=%.1fs',
                     self.instance_name, pilot.pid, pilot.child_endpoint_name,
                     pilot.active_at - pilot.submitted_at)

        self._dispatch_notify('pilot_status', {
            'pilot_id'      : pilot.pid,
            'pool'          : ps.config.name,
            'state'         : pilot.state,
            'child_endpoint': pilot.child_endpoint_name,
            'capacity'      : capacity,
        })

        try:
            ps.policy.on_pilot_state(pilot, old_state, PILOT_ACTIVE)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

        self._drain_pending(ps)

    # -- pilot submission path -----------------------------------------

    def _make_submit_pilot(self, pool_state: PoolState):
        '''Return the policy's ``submit_pilot`` callable, bound to a pool.

        Contract v2: ``submit_pilot(size_key=None, *, member_id=None)``.
        ``size_key`` keeps position 0 so every existing positional call
        keeps its meaning; ``member_id`` is keyword-only and ``None``
        selects the pool's primary member (the implicit one for a legacy
        pool).
        '''
        def _submit(size_key: str | None = None, *,
                    member_id: str | None = None) -> str:
            return self._submit_pilot(pool_state, size_key,
                                      member_id=member_id)
        return _submit

    def _submit_pilot(self, pool_state: PoolState,
                      size_key: str | None,
                      member_id: str | None = None) -> str:
        '''Register a pilot for one member and schedule its psij submission.

        Called from the policy's ``on_tick`` (on the housekeeping loop).
        Returns the dispatcher-local pilot id; the actual psij submission runs
        as a background task so the caller returns immediately.

        The member's ``attributes`` and the chosen size are **snapshotted**
        onto the record and never change afterwards, even if the member is
        re-declared or removed: a pilot's node really does have the
        software it had when it started, and a departed member's pilots
        still have to be sized for accounting.

        A ``pilot: endpoint`` member is **adopted**, not submitted (plan
        122): its endpoint already runs inside the allocation, so the
        record is created PENDING with the endpoint as its own child, no
        psij job is asked for, and the last topology is replayed through
        :meth:`_reconcile_pilots_for` — so it is activated at once when the
        endpoint is ``present``, and otherwise (``suspect`` or absent) by
        the next delivery, exactly as a submitted pilot's child is.
        '''
        cfg    = pool_state.config
        member = cfg.member(member_id) if member_id else None
        if member is None:
            if member_id:
                raise KeyError(
                    f"pool {cfg.name}: unknown member {member_id!r} "
                    f"(available: {sorted(cfg.members)})")
            member = cfg.primary_member()

        size_key = size_key or member.default_size
        if size_key not in member.pilot_sizes:
            raise KeyError(
                f"pool {cfg.name}: unknown pilot_size "
                f"{size_key!r} (available: "
                f"{sorted(member.pilot_sizes)})")

        adopt = member.pilot == PILOT_ENDPOINT
        if adopt:
            # There is exactly one endpoint to adopt, so a second call
            # while a record for it is live would create a second pilot
            # bound to the same child endpoint name.
            live = pool_state.live_pilots_for(member.member_id)
            if live:
                log.warning('[%s] pool %r: member %r already holds adopted '
                            'pilot %s; not adopting %r again',
                            self.instance_name, cfg.name, member.member_id,
                            live[0].pid, member.endpoint_name)
                return live[0].pid

        size   = member.pilot_sizes[size_key]
        pid    = f'p.{uuid.uuid4().hex[:10]}'
        record = PilotRecord(
            pid              = pid,
            pool             = cfg.name,
            owning_sid       = pool_state.owning_sid,
            size_key         = size_key,
            rhapsody_backend = size.rhapsody_backend,
            state            = PILOT_PENDING,
            submitted_at     = time.time(),
            walltime_deadline= self._pilot_deadline(member, size),
            member_id        = member.member_id,
            attributes       = dict(member.attributes),
            endpoint_name    = member.endpoint_name or '',
            nodes            = size.nodes,
            cpus_per_node    = size.cpus_per_node,
            gpus_per_node    = size.gpus_per_node,
            # the stamp every later rule reads (see ``PilotRecord.adopted``)
            adopted          = adopt,
        )
        if adopt:
            # Pre-bind the endpoint as its own child, exactly as
            # ``_do_pilot_submit`` does for a submitted pilot, so the
            # topology hook matches it.
            record.child_endpoint_name = member.endpoint_name

        pool_state.pilots[pid] = record
        pool_state.persist()

        if adopt:
            log.info('[%s] pool %r: adopting endpoint %r as pilot %s for '
                     'member %r', self.instance_name, cfg.name,
                     member.endpoint_name, pid, member.member_id)
            self._reconcile_pilots_for(pool_state, self._participants)
        else:
            asyncio.create_task(
                self._do_pilot_submit(pool_state, record, size, member))

        self._dispatch_notify('autoscale_decision', {
            'pool'     : cfg.name,
            'action'   : 'adopt_pilot' if adopt else 'submit_pilot',
            'pilot_id' : pid,
            'size_key' : size_key,
            'member_id': member.member_id,
        })
        return pid

    @staticmethod
    def _pilot_deadline(member: PoolMember, size: PilotSize) -> float:
        '''Return the walltime deadline for a new pilot of *member*.

        A submitted pilot: ``now + walltime_sec``, capped by the member's
        ``end_time`` when it knows one.

        An adopted endpoint (``pilot == PILOT_ENDPOINT``): the allocation's
        ``end_time`` only, ``0.0`` (unknown) without one.  Nothing ends an
        adopted endpoint at ``now + walltime_sec`` -- that figure is a size
        default (3600 when the allocation reports no end, or runs
        UNLIMITED) -- so a deadline made of it would drop the endpoint from
        dispatch and capacity near that time while it keeps holding the
        member's single pilot slot: the pool would stall with every task
        queued.  The allocation's end is also what keeps a **re-**adopted
        endpoint honest, since the member is re-declared with its join-time
        ``walltime_sec`` on every re-attach.
        '''
        if member.pilot == PILOT_ENDPOINT:
            return float(member.end_time or 0.0)
        deadline = time.time() + size.walltime_sec
        if member.end_time:
            return min(deadline, float(member.end_time))
        return deadline

    def _build_pilot_env(self, pool_state: PoolState,
                         record: PilotRecord,
                         member: PoolMember) -> dict[str, str]:
        '''Build bootstrap env vars for the pilot's child endpoint service.

        ``RADICAL_ORBIT_SCRATCH_BASE`` comes from **the member's** own
        ``scratch_base`` when it declares one — that is a path on the
        member's host, and this (plus the cwd assignment at dispatch) is
        the only place it is read.  Nothing on the broker host ever
        ``mkdir``s it.
        '''
        broker_url = getattr(self._app.state, 'broker_url', '') or ''
        scratch = str(member.scratch_base or pool_state.scratch_base)
        env: dict[str, str] = {
            'RADICAL_ORBIT_BROKER_URL'      : str(broker_url),
            'RADICAL_ORBIT_POOL'            : pool_state.config.name,
            'RADICAL_ORBIT_RHAPSODY_BACKEND': record.rhapsody_backend,
            'RADICAL_ORBIT_SCRATCH_BASE'    : scratch,
        }
        if pool_state.config.multi_member:
            env['RADICAL_ORBIT_MEMBER'] = record.member_id
        # The broker's cert path is a path on the BROKER host.  A member
        # that shares the filesystem sees the same file; a non-shared
        # member runs elsewhere, where that path (e.g. the broker user's
        # $HOME) need not exist -- shipping it made every remote pilot
        # fail TLS silently.  Such a pilot inherits its endpoint's own
        # RADICAL_ORBIT_BROKER_CERT, or falls back to the endpoint's
        # default ~/.radical/orbit/broker_cert.pem on its host.
        shared = member.shared_fs
        cert   = os.environ.get('RADICAL_ORBIT_BROKER_CERT')
        if cert and shared:
            env['RADICAL_ORBIT_BROKER_CERT'] = cert
        return env

    def _build_job_spec(self, pool_state: PoolState,
                        size: PilotSize,
                        child_endpoint: str,
                        env: dict[str, str],
                        member: PoolMember) -> dict:
        '''Build a psij-compatible JobSpec for the pilot.

        ``queue_name`` and ``project`` come from **the member**, which for
        a legacy pool is the implicit projection of the pool's own fields.
        '''
        resources: dict[str, Any] = {
            'node_count'        : size.nodes,
            'processes_per_node': size.cpus_per_node,
        }
        if size.gpus_per_node:
            resources['gpu_cores_per_process'] = size.gpus_per_node

        attributes: dict[str, Any] = {
            'queue_name': member.queue,
            'duration'  : size.walltime_sec,
        }
        if member.account:
            attributes['project'] = member.account

        return {
            'executable' : 'radical-orbit-endpoint-wrapper.sh',
            'arguments'  : ['-n', child_endpoint, '--plugins', 'default'],
            'environment': env,
            'resources'  : resources,
            'attributes' : attributes,
        }

    async def _do_pilot_submit(self, pool_state: PoolState,
                               record: PilotRecord,
                               size: PilotSize,
                               member: PoolMember) -> None:
        '''Call psij on the member's target endpoint to submit the pilot job.

        The pilot may be cancelled while ``submit_tunneled`` is in flight
        (member removal, session teardown): it then has no psij job id to
        cancel yet and is simply marked FAILED.  So a submit that returns
        onto a terminal record must cancel the job it just created rather
        than resurrect the pilot around an orphaned allocation.
        '''
        endpoint_name = record.endpoint_name
        if not endpoint_name:
            self._mark_pilot_failed(
                pool_state, record,
                f'pool {pool_state.config.name!r} has no endpoint_name set')
            return

        # Fail fast on the unconfigured default-pool queue sentinel rather than
        # submit a pilot to a batch queue literally named 'default'.
        if member.queue == 'default':
            self._mark_pilot_failed(
                pool_state, record,
                "pool queue is the 'default' sentinel; re-declare the pool "
                "with an explicit 'queue' before submitting pilots")
            return

        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(
                pool_state, record, 'psij client unavailable')
            return

        child_endpoint = child_endpoint_name(
            pool_state.config.name, record.member_id, record.pid)
        # Pre-bind so on_topology_change can match the registering child.
        record.child_endpoint_name = child_endpoint
        env      = self._build_pilot_env(pool_state, record, member)
        job_spec = self._build_job_spec(pool_state, size, child_endpoint,
                                        env, member)

        try:
            from .batch_system import detect_batch_system
            executor = detect_batch_system().psij_executor
            result   = await asyncio.to_thread(
                psij_c.submit_tunneled, job_spec, executor, 'none')
        except Exception as e:
            log.exception('[%s] psij submit_tunneled failed for %s: %s',
                          self.instance_name, record.pid, e)
            self._mark_pilot_failed(pool_state, record, f'psij error: {e}')
            return

        job_id = result.get('job_id')

        # The submission is the one long await on this path, and the record
        # can reach a terminal state under it: a member removed, the owning
        # session closed, the handshake sweep giving up.  Writing STARTING
        # over that would resurrect a pilot nobody is waiting for -- and its
        # batch job would keep the allocation.  So cancel what we just
        # started and leave the record as the other path left it.
        if record.is_terminal():
            log.warning('[%s] pilot %s reached %s while its psij submit was '
                        'in flight; cancelling job %s',
                        self.instance_name, record.pid, record.state, job_id)
            if job_id:
                try:
                    await asyncio.to_thread(psij_c.cancel_job, job_id)
                except Exception as e:
                    log.warning('[%s] psij cancel failed for %s: %s',
                                self.instance_name, record.pid, e)
            return

        record.psij_job_id = job_id
        record.state       = PILOT_STARTING
        pool_state.persist()
        self._dispatch_notify('pilot_status', {
            'pilot_id'   : record.pid,
            'pool'       : pool_state.config.name,
            'state'      : record.state,
            'psij_job_id': record.psij_job_id,
        })

        try:
            pool_state.policy.on_pilot_state(
                record, PILOT_PENDING, PILOT_STARTING)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    @staticmethod
    def _pilot_endpoint(pool_state: PoolState,
                        record: PilotRecord) -> str | None:
        '''Resolve the endpoint that runs a pilot's psij job.

        The **pilot's own snapshot** (backfilled at load for a pre-121
        record), never the member: removing a member drops it from the
        config *before* awaiting the cancels of its pilots, so a member
        lookup would return ``None`` exactly when the cancel matters most.
        '''
        return record.endpoint_name or None

    async def _do_pilot_cancel(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        '''Best-effort psij cancel + FAILED for one pilot.

        The second of the two exits an adopted pilot can take (the other is
        the topology ``lost`` branch): removing its member — a ``leave``, or
        an endpoint the federation detached — releases the endpoint rather
        than killing a job, so the record ends **DONE**.  Whichever of the
        two hooks fires first, the pilot's member is never charged a
        failure for it.
        '''
        if record.is_terminal():
            return
        if record.adopted:
            self._mark_pilot_done(pool_state, record, 'endpoint released')
            return
        endpoint_name = record.endpoint_name
        if not endpoint_name or not record.psij_job_id:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        try:
            await asyncio.to_thread(psij_c.cancel_job, record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij cancel failed for %s: %s',
                        self.instance_name, record.pid, e)
        self._mark_pilot_failed(pool_state, record, 'cancelled')

    async def _reconcile_pilot(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        '''Sweeper path: query psij state for an overdue pilot.

        An **adopted** pilot has no psij job to ask about: it is waiting for
        its own endpoint to appear in the topology.  Past the handshake
        timeout that endpoint is not coming, and the record is failed with
        that reason — left PENDING it would sit forever while counting
        against the strategy's in-flight guards and the pool ceiling.
        '''
        if record.is_terminal():
            return
        if record.adopted:
            self._mark_pilot_failed(
                pool_state, record,
                f'endpoint {record.child_endpoint_name} not connected')
            return
        endpoint_name = record.endpoint_name
        if not endpoint_name or not record.psij_job_id:
            return
        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            return
        try:
            status = await asyncio.to_thread(
                psij_c.get_job_status, record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij get_job_status failed for %s: %s',
                        self.instance_name, record.pid, e)
            return

        state = str(status.get('state', '')).upper()
        if state in ('COMPLETED', 'DONE', 'FAILED', 'CANCELED'):
            self._mark_pilot_failed(
                pool_state, record, f'handshake timeout; psij state {state}')

    def _mark_pilot_failed(self, pool_state: PoolState,
                           record: PilotRecord, reason: str) -> None:
        '''Mark a pilot FAILED, re-enqueue assigned tasks, notify the policy.'''
        log.warning('[%s] pilot %s → FAILED (%s)',
                    self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_FAILED, reason)

    def _mark_pilot_done(self, pool_state: PoolState,
                         record: PilotRecord, reason: str) -> None:
        '''Mark a pilot DONE (clean end, e.g. walltime expiry).'''
        log.info('[%s] pilot %s → DONE (%s)',
                 self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_DONE, reason)

    def _finalize_pilot(self, pool_state: PoolState, record: PilotRecord,
                        new_state: str, reason: str) -> None:
        '''Drive a pilot to a terminal state and reclaim its tasks.

        Persists the transition, notifies clients, re-enqueues any non-terminal
        tasks assigned to this pilot (clearing their stale rhapsody-uid mapping
        so a late terminal event from the dead pilot can't clobber the requeued
        task), and signals the policy.

        Stamps ``finished_at`` — the only place a pilot reaches a terminal
        state — so the pool's ``pilot_history`` keeps the interval this pilot
        held its allocation after it disappears from the live fleet.

        A re-queued task also drops its ``member_id`` — it is set at
        dispatch beside ``pilot_id`` and would otherwise report a placement
        it no longer has — and bumps ``requeues``.  Past the policy's
        ``max_requeues`` the task is **failed** instead of re-queued, which
        also protects the pre-existing pilot-loss path from an infinite
        bounce.  Its input spool deliberately survives: the task is about
        to be dispatched somewhere else.

        A FAILED pilot also keeps *why* on its record (``error``), so the
        reason travels with every ``pilot_history`` entry instead of living
        only in the broker log.  A DONE pilot's reason is not an error and
        is not stamped.
        '''
        old_state = record.state
        record.state = new_state
        if new_state == PILOT_FAILED and reason:
            record.error = str(reason)[:PILOT_ERROR_MAX]
        if record.finished_at is None:
            record.finished_at = time.time()
        self._dispatch_notify('pilot_status', {
            'pilot_id' : record.pid,
            'pool'     : pool_state.config.name,
            'state'    : new_state,
            'reason'   : reason,
            'member_id': record.member_id,
        })

        cap = pool_state.policy.max_requeues

        for t in list(pool_state.tasks.values()):
            if t.pilot_id == record.pid and \
                    t.state not in TASK_TERMINAL_STATES:
                if t.rhapsody_uid:
                    self._uid_to_task.pop(t.rhapsody_uid, None)
                    t.rhapsody_uid = None
                t.requeues += 1
                t.pilot_id  = None
                t.member_id = None
                if t.requeues > cap:
                    self._mark_task_failed(
                        pool_state, t, 'requeued too often (pilot lost)')
                else:
                    t.state = TASK_QUEUED
                    self._dispatch_notify('task_status', self._task_dict(t))
        pool_state.persist()

        try:
            pool_state.policy.on_pilot_state(record, old_state, new_state)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    # -- dispatch loop -------------------------------------------------

    def _drain_pending(self, pool_state: PoolState) -> None:
        '''Ask the policy for (task, pilot) pairs until it stops.

        Bounded by the pending-queue length: a single drain can dispatch at
        most as many tasks as are QUEUED.  A pick that repeats or returns a
        non-QUEUED (stale) task is logged and breaks the drain — a broken
        policy is visible rather than silently rate-limited.
        '''
        budget   = len(pool_state.pending_queue())
        assigned : dict[str, list[TaskRecord]] = {}
        pilots   : dict[str, PilotRecord]      = {}
        while budget > 0:
            budget -= 1
            try:
                pair = pool_state.policy.pick_dispatch(pool_state)
            except Exception as e:
                log.exception('[%s] pick_dispatch raised: %s',
                              self.instance_name, e)
                break
            if pair is None:
                break
            task, pilot = pair
            if task.state != TASK_QUEUED:
                log.warning('[%s] pool %r: pick_dispatch returned non-QUEUED '
                            'task %s (%s); stopping drain',
                            self.instance_name, pool_state.config.name,
                            task.task_id, task.state)
                break
            if not self._claim(pool_state, task, pilot):
                continue
            assigned.setdefault(pilot.pid, []).append(task)
            pilots[pilot.pid] = pilot

        if not assigned:
            return

        # one ledger write for the whole drain, however many tasks it claimed
        pool_state.persist()

        for pid, tasks in assigned.items():
            for task in tasks:
                self._notify_task(pool_state, task)
            asyncio.create_task(
                self._do_rhapsody_submit(pool_state, tasks, pilots[pid]))

    def _claim(self, pool_state: PoolState,
               task: TaskRecord, pilot: PilotRecord) -> bool:
        '''Claim the task for this pilot; return whether the claim stands.

        This is where a **dispatcher-assigned cwd** is resolved: in a class
        pool the member — and therefore the filesystem — is not known at
        submit time, so a task submitted without a ``cwd`` gets one here,
        under the placed member's ``scratch_base`` (falling back to the
        pool's broker-local scratch).  A **re-dispatch** after a pilot loss
        re-assigns it, because a task moving to a member with a different
        scratch root must not carry the old path; a client-supplied cwd
        (``cwd_assigned == False``) is left alone.

        The directory is created here only for a ``shared_fs`` member.  For
        a non-shared one the broker creates nothing locally — the staging
        ``put`` of the task's inputs creates it on the pilot instead.
        '''
        if task.cwd_assigned:
            member = pool_state.member(pilot.member_id)
            shared = member is None or member.shared_fs
            if member is not None and member.scratch_base:
                base = Path(member.scratch_base)
                # '~' means the broker's home only when the broker shares
                # the filesystem; for a remote member it must travel
                # untouched and be expanded on the member's own host.
                if shared:
                    base = base.expanduser()
            else:
                base = pool_state.scratch_base
            task.cwd = str(base / task.task_id)
            if shared:
                try:
                    Path(task.cwd).mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    self._mark_task_failed(
                        pool_state, task,
                        f'could not create task cwd {task.cwd}: {e}')
                    return False

        task.state      = TASK_RUNNING
        task.pilot_id   = pilot.pid
        task.member_id  = pilot.member_id
        task.started_at = time.time()
        pilot.in_flight     += 1
        pilot.started_tasks += 1
        return True

    async def _place_inputs(self, pool_state: PoolState,
                            tasks: list[TaskRecord],
                            pilot: PilotRecord) -> list[TaskRecord]:
        '''Put each task's spooled inputs where the pilot will find them.

        Returns the tasks that are still runnable; a task whose inputs
        could not be placed is FAILED here rather than run without them.

        - ``shared_fs`` member → copy from the broker-local spool into the
          task's cwd.  The dispatcher created that directory at ``_claim``
          when it assigned the cwd itself, but a **client-supplied** cwd is
          only a promise, so create it here too.
        - non-shared member → ``put`` each file to ``<cwd>/<name>`` over
          the **pilot's own** staging plugin.  That put is also what
          creates the directory remotely, so an input-less **exec-style**
          task on such a member gets one zero-byte marker put to the same
          effect — cheaper than adding a ``mkdir`` route to a staging
          plugin that only has put/get/list.  A **rhapsody-dialect** task
          gets no marker: its cwd rides inside the opaque task dict, is
          never rewritten by the dispatcher, and may name a directory the
          client already owns — writing into it would be a guess.
        '''
        member = pool_state.member(pilot.member_id)
        shared = member is None or member.shared_fs

        if shared and not any(t.spooled for t in tasks):
            return tasks

        stg = None
        if not shared:
            stg = await self._get_staging_client(pilot.child_endpoint_name)
            if stg is None:
                for task in tasks:
                    self._mark_task_failed(
                        pool_state, task,
                        'could not place inputs on the pilot: staging '
                        'client unavailable')
                return []

        ok: list[TaskRecord] = []
        for task in tasks:
            spool = pool_state.spool_dir(task.task_id)
            try:
                if shared:
                    if task.spooled:
                        Path(task.cwd).mkdir(parents=True, exist_ok=True)
                    for name in task.spooled:
                        shutil.copyfile(spool / name,
                                        Path(task.cwd) / name)
                elif task.spooled:
                    for name in task.spooled:
                        await asyncio.to_thread(
                            stg.put, str(spool / name),
                            str(Path(task.cwd) / name), True)
                elif task.task_dict is None:
                    marker = spool / _CWD_MARKER
                    marker.parent.mkdir(parents=True, exist_ok=True)
                    # 1 byte, not 0: older endpoints' staging 400s an empty put
                    marker.write_bytes(b'\n')
                    await asyncio.to_thread(
                        stg.put, str(marker),
                        str(Path(task.cwd) / _CWD_MARKER), True)
            except Exception as e:
                log.warning('[%s] input placement failed for task %s: %s',
                            self.instance_name, task.task_id, e)
                self._mark_task_failed(
                    pool_state, task,
                    f'could not place inputs on the pilot: {e}')
                continue
            ok.append(task)

        return ok

    async def _do_rhapsody_submit(self, pool_state: PoolState,
                                  tasks: list[TaskRecord],
                                  pilot: PilotRecord) -> None:
        '''Post claimed tasks to the pilot's rhapsody session, one call.'''
        if not pilot.child_endpoint_name:
            for task in tasks:
                self._mark_task_failed(pool_state, task,
                                       'child endpoint unavailable')
            return

        rh = await self._get_rhapsody_client(
            pilot.child_endpoint_name, pilot.rhapsody_backend)
        if rh is None:
            for task in tasks:
                self._mark_task_failed(pool_state, task,
                                       'rhapsody client unavailable')
            return

        # Place spooled inputs BEFORE the submit: a task that runs without
        # its inputs is worse than a task that fails with a reason.
        tasks = await self._place_inputs(pool_state, tasks, pilot)
        if not tasks:
            return

        fwds = []
        for task in tasks:
            if task.task_dict is not None:
                # rhapsody dialect: the client's dict, forwarded verbatim.
                # The uid is namespaced by the owning session, because the
                # pilot's rhapsody session is shared across sessions and
                # client-side uid counters (asyncflow's `task.NNNNNN`) are
                # only unique per client process.
                fwd = dict(task.task_dict)
                fwd['uid'] = f'{task.task_id}.{task.owning_sid}'
                # Per-key merge: the caller's own
                # task_backend_specific_kwargs override the keys derived
                # from `requirements` (the caller knows its backend), but
                # keys it did not set still come from the mapping.  Left
                # untouched when nothing is derived, so a task without
                # requirements forwards byte-identically.
                derived = backend_kwargs(task.requirements,
                                         pilot.rhapsody_backend)
                if derived:
                    fwd['task_backend_specific_kwargs'] = {
                        **derived,
                        # `or {}` -- the key may be present and null
                        **(task.task_dict.get(
                            'task_backend_specific_kwargs') or {}),
                    }
            else:
                fwd = {
                    'uid'       : task.task_id,
                    'executable': task.cmd[0] if task.cmd else '',
                    'arguments' : task.cmd[1:] if len(task.cmd) > 1 else [],
                    'cwd'       : task.cwd,
                    # rhapsody's concurrent backend reads cwd from
                    # task_backend_specific_kwargs (BaseTask's top-level cwd
                    # is ignored); mirror it here so the task runs in its
                    # scratch dir.  The requirements mapping merges onto
                    # that dict — it never replaces it, and it never emits
                    # a 'cwd' of its own.
                    'task_backend_specific_kwargs': {
                        'cwd': task.cwd,
                        **backend_kwargs(task.requirements,
                                         pilot.rhapsody_backend),
                    },
                }
            fwds.append((task, fwd))

        # Map uids before submitting: a sub-second task can report terminal
        # before the threaded submit call returns, and an unmapped terminal
        # event is dropped.
        for task, fwd in fwds:
            task.rhapsody_uid = fwd['uid']
            self._uid_to_task[fwd['uid']] = (pool_state.owning_sid,
                                             pool_state.config.name,
                                             task.task_id)

        try:
            await asyncio.to_thread(rh.submit_tasks, [f for _, f in fwds])
            self._mark_dirty(pool_state)
        except Exception as e:
            log.exception('[%s] rhapsody submit failed for %d task(s): %s',
                          self.instance_name, len(tasks), e)
            for task, fwd in fwds:
                self._uid_to_task.pop(fwd['uid'], None)
                # a task the pilot already finished keeps that outcome
                if task.state not in TASK_TERMINAL_STATES:
                    task.rhapsody_uid = None
                    self._mark_task_failed(pool_state, task,
                                           f'rhapsody submit error: {e}')

    def _on_event(self, event: dict) -> None:
        '''Broker raw-tap callback: a child rhapsody reported a transition.

        The tap fires on the plugin-host loop — the dispatcher's own loop — so
        terminal handling runs inline.  The tap is unfiltered, so filter here
        on plugin/topic; the rhapsody uid → pool mapping is ``_uid_to_task``.

        Rhapsody ships one completion as ``task_status`` and several as
        ``task_status_batch`` under ``tasks``
        (``plugin_rhapsody._flush_notifications``); both are handled, as in
        ``RhapsodyClient._on_task_done``.
        '''
        if event.get('plugin') != 'rhapsody':
            return

        data  = event.get('data') or {}
        topic = event.get('topic')
        if   topic == 'task_status'      : items = [data]
        elif topic == 'task_status_batch': items = data.get('tasks') or []
        else                             : return

        for item in items:
            self._on_task_status(item)

    def _on_task_status(self, data: dict) -> None:
        '''One rhapsody task-status payload from the tap.'''
        uid   = data.get('uid')
        state = str(data.get('state', '')).upper()
        if not uid or state not in ('DONE', 'FAILED', 'CANCELED', 'COMPLETED'):
            return

        target = {
            'DONE'     : TASK_DONE,
            'COMPLETED': TASK_DONE,
            'FAILED'   : TASK_FAILED,
            'CANCELED' : TASK_CANCELED,
        }[state]
        self._handle_task_terminal(uid, target, data)

    def _handle_task_terminal(self, uid: str, target_state: str,
                              data: dict) -> None:
        '''Host-loop handler for a child rhapsody task completion.'''
        # Endpoint-mode tasks: forget the mapping and re-emit the terminal
        # status under the dispatcher's plugin name so consumers filtering on
        # plugin='task_dispatcher' still see the event.
        if uid in self._endpoint_mode_tasks:
            endpoint_name = self._endpoint_mode_tasks.pop(uid)
            self._persist_endpoint_mode()
            self._dispatch_notify('task_status', {
                'task_id'  : uid,
                'endpoint' : endpoint_name,
                'state'    : target_state,
                'exit_code': data.get('exit_code'),
                'error'    : data.get('error'),
            })
            return

        mapping = self._uid_to_task.pop(uid, None)
        if not mapping:
            return
        sid, pool_name, task_id = mapping
        pool_state = self._find_pool(sid, pool_name)
        if not pool_state:
            return
        task = pool_state.tasks.get(task_id)
        if task is None or task.state in TASK_TERMINAL_STATES:
            return

        task.state       = target_state
        task.exit_code   = data.get('exit_code')
        task.error       = data.get('error')
        task.finished_at = time.time()
        pool_state.drop_spool(task_id)

        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)

        self._notify_task(pool_state, task, child_data=data)
        self._drain_pending(pool_state)

    # -- rhapsody-dialect batching: notifications + ledger persists -----
    #
    # Same trade as plugin_rhapsody's NOTIFY window: up to `_notify_window`
    # seconds of latency buys one WS frame per batch instead of one per
    # task, and one `state.json` fsync per dirty pool per flush instead of
    # one per completion -- the dominant latency cost of pool mode.  A
    # zero window (latency-sensitive broker) flushes inline, which is also
    # the exec-mode behavior of old.

    def _notify_task(self, pool_state: PoolState, task: TaskRecord,
                     child_data: dict | None = None) -> None:
        '''Emit one task's status: dialect-shaped and batched for a
        rhapsody-dialect task, the classic immediate frame for an
        exec-style one (its consumers -- the Explorer page among them --
        read the exec shape).'''
        if task.task_dict is None:
            self._dispatch_notify('task_status', self._task_dict(task))
            return

        # rhapsody consumer contract: `uid`, not `task_id`, plus whatever
        # result/error fields the pilot's rhapsody reported
        payload = {k: v for k, v in (child_data or {}).items()
                   if k in _RH_FORWARD_KEYS}
        payload['uid']   = task.task_id
        payload['state'] = task.state
        if task.error is not None:
            payload.setdefault('error', task.error)
        if task.exit_code is not None:
            payload.setdefault('exit_code', task.exit_code)
        self._queue_rh_notification(payload)

    def _queue_rh_notification(self, payload: dict) -> None:
        '''Buffer one dialect notification; flush by window, count or bytes.'''
        size = _payload_size(payload)
        with self._rh_notify_lock:
            self._rh_notify_buf.append(payload)
            self._rh_notify_bytes += size
            now      = self._notify_window <= 0 \
                       or len(self._rh_notify_buf) >= NOTIFY_BATCH_SIZE \
                       or self._rh_notify_bytes >= self._notify_batch_bytes
            schedule = not now and not self._rh_flush_scheduled
            if schedule:
                self._rh_flush_scheduled = True

        if now:
            self._flush_rh()
        elif schedule:
            self._schedule_rh_flush(self._notify_window)

    def _mark_dirty(self, pool_state: PoolState) -> None:
        '''Coalesce this pool's next ledger persist into the flush window.'''
        if self._notify_window <= 0:
            pool_state.persist()
            return
        with self._rh_notify_lock:
            self._dirty_pools.add((pool_state.owning_sid,
                                   pool_state.config.name))
            schedule = not self._rh_flush_scheduled
            if schedule:
                self._rh_flush_scheduled = True
        if schedule:
            self._schedule_rh_flush(self._notify_window)

    def _schedule_rh_flush(self, delay: float) -> None:
        '''Arm one delayed flush on the host loop.'''
        async def _do_flush():
            if delay > 0:
                await asyncio.sleep(delay)
            self._flush_rh()

        try:
            asyncio.get_running_loop().create_task(_do_flush())
        except RuntimeError:
            # no running loop (direct-call tests): flush inline
            self._flush_rh()

    def _flush_rh(self) -> None:
        '''Flush buffered notifications and persist every dirty pool.'''
        with self._rh_notify_lock:
            self._rh_flush_scheduled = False
            batch = list(self._rh_notify_buf)
            self._rh_notify_buf.clear()
            self._rh_notify_bytes = 0
            dirty = list(self._dirty_pools)
            self._dirty_pools.clear()

        for sid, name in dirty:
            ps = self._find_pool(sid, name)
            if ps is not None:            # a torn-down pool needs no persist
                ps.persist()

        if not batch:
            return

        # frame-budget splitting, same shape plugin_rhapsody emits
        frame:  list[dict] = []
        nbytes             = 0
        frames             = [frame]
        for payload in batch:
            size = _payload_size(payload)
            if frame and nbytes + size > self._notify_batch_bytes:
                frame  = []
                nbytes = 0
                frames.append(frame)
            frame.append(payload)
            nbytes += size

        for frame in frames:
            if not frame:
                continue
            if len(frame) == 1:
                self._dispatch_notify('task_status', frame[0])
            else:
                self._dispatch_notify('task_status_batch', {'tasks': frame})

    def _mark_task_failed(self, pool_state: PoolState,
                          task: TaskRecord, reason: str) -> None:
        '''Mark one task FAILED and free its pilot slot.'''
        task.state       = TASK_FAILED
        task.error       = reason
        task.finished_at = time.time()
        pool_state.drop_spool(task.task_id)
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)
        self._notify_task(pool_state, task)

    async def _cancel_task(self, pool_state: PoolState,
                           task: TaskRecord) -> dict:
        '''Cancel path: either remove from queue or cancel on the pilot.'''
        if task.state in TASK_TERMINAL_STATES:
            return self._task_dict(task)
        if task.state == TASK_QUEUED:
            task.state       = TASK_CANCELED
            task.finished_at = time.time()
            pool_state.drop_spool(task.task_id)
            self._mark_dirty(pool_state)
            self._notify_task(pool_state, task)
            return self._task_dict(task)

        # RUNNING — best-effort cancel on the pilot
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot and pilot.child_endpoint_name and task.rhapsody_uid:
            rh = await self._get_rhapsody_client(pilot.child_endpoint_name)
            if rh is not None:
                try:
                    await asyncio.to_thread(rh.cancel_task, task.rhapsody_uid)
                except Exception as e:
                    log.warning('[%s] rhapsody cancel_task failed: %s',
                                self.instance_name, e)
        task.state       = TASK_CANCELED
        task.finished_at = time.time()
        pool_state.drop_spool(task.task_id)
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)
        self._notify_task(pool_state, task)
        return self._task_dict(task)

    # -- helpers -------------------------------------------------------

    def _persist_endpoint_mode(self) -> None:
        '''Rewrite the endpoint-mode ledger (task_id → endpoint) atomically.'''
        try:
            write_json_atomic(self._endpoint_mode_path,
                              self._endpoint_mode_tasks)
        except OSError as e:
            log.warning('[%s] could not persist endpoint-mode ledger: %s',
                        self.instance_name, e)

    def _require_known_session(self, sid: str) -> None:
        '''Raise 404 unless *sid* is a known session.'''
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session: {sid}')

    def _task_dict(self, task: TaskRecord) -> dict:
        '''Return the plain-dict view of a task record.'''
        return asdict(task)

    def _pilot_dict(self, pilot: PilotRecord) -> dict:
        '''Return the plain-dict view of a pilot record.'''
        return asdict(pilot)

    @staticmethod
    def _sizes_dict(sizes: dict[str, PilotSize]) -> dict:
        '''Return the flat JSON view of a pilot-size menu.'''
        return {
            name: {
                'nodes'           : size.nodes,
                'cpus_per_node'   : size.cpus_per_node,
                'gpus_per_node'   : size.gpus_per_node,
                'walltime_sec'    : size.walltime_sec,
                'rhapsody_backend': size.rhapsody_backend,
            }
            for name, size in sizes.items()
        }

    def _summarize_pool(self, ps: PoolState, verbose: bool = False) -> dict:
        '''Return a summary dict for one pool (optionally verbose).

        Every pre-121 key stays exactly as it was — the flat
        ``pilot_sizes``, ``queue``, ``account``, ``endpoint_name`` and
        ``min``/``max_pilots`` are the primary-member projection — and the
        capability-class fields are additive.  The verbose ``members``
        block is **frozen contract** (plan 121 §9): the federation and the
        Explorer read it verbatim, so keys may be added but never renamed
        or removed.
        '''
        cfg     = ps.config
        now     = time.time()
        live    = ps.live_pilots()
        pending = [t for t in ps.tasks.values() if t.state == TASK_QUEUED]
        summary = {
            'name'        : cfg.name,
            'endpoint_name': cfg.endpoint_name,
            'queue'       : cfg.queue,
            'account'     : cfg.account,
            'default_size': cfg.default_size,
            'pilot_sizes' : self._sizes_dict(cfg.pilot_sizes),
            'live_pilots'  : len(live),
            'pending_tasks': len(pending),
            'min_pilots'   : cfg.min_pilots,
            'max_pilots'   : cfg.max_pilots,
            # -- capability class ------------------------------------
            'pool_class'      : cfg.pool_class,
            'multi_member'    : cfg.multi_member,
            # A list of *strings*, named apart from the verbose `members`
            # (a list of objects) so no consumer has to discover the type.
            # Empty for a legacy pool: its single implicit member is an
            # internal construct, never part of the wire contract.
            'member_ids'      : list(cfg.members) if cfg.multi_member else [],
            'max_pilots_total': sum(m.max_pilots for m in ps.members()),
        }
        if verbose:
            summary['pilots'] = [self._pilot_dict(p) for p in live]
            summary['recent_tasks'] = [
                self._task_dict(t)
                for t in sorted(ps.tasks.values(),
                                key=lambda t: t.arrival_ts,
                                reverse=True)[:50]
            ]
            # Pool-level history covers pilots whose member has since been
            # removed (they are in no member's list) -- which is exactly
            # why the pool total is reported rather than summed from the
            # member figures.
            # The wire needs dicts: ``asdict`` each record exactly once
            # here and let the member blocks share the views.
            history = ps.pilot_history()
            views   = {p.pid: asdict(p) for p in history}
            summary['pilot_history']  = list(views.values())
            summary['node_hours_used'] = node_hours(history, now=now)
            summary['members'] = [
                self._member_dict(ps, m, now, views) for m in ps.members()
            ] if cfg.multi_member else []
        return summary

    @staticmethod
    def _last_pilot_error(history: list[PilotRecord]) -> str | None:
        '''Return the ``error`` of the most recent FAILED pilot, or ``None``.

        *history* is oldest first, so the newest failure is found by walking
        it backwards.  The walk stops at the newest pilot that reached
        ACTIVE (``active_at`` set): a failure older than a healthy pilot is
        history, not the member's current problem -- otherwise a lost-and-
        re-added member would show "child endpoint lost" under an ``ok``
        row forever.  A FAILED pilot written before this field existed
        carries no error and is skipped rather than reported as a healthy
        member: an older failure that *does* say why is the better answer.
        '''
        for pilot in reversed(history or []):
            if pilot.active_at:
                return None
            if pilot.state == PILOT_FAILED and pilot.error:
                return pilot.error
        return None

    def _member_dict(self, ps: PoolState, m: PoolMember,
                     now: float, views: dict[str, dict]) -> dict:
        '''Return the verbose per-member block (frozen contract, §9).

        ``pilot`` / ``end_time`` are the member's declaration; the derived
        ``remaining_sec`` is the most walltime any of its **live** pilots
        still has (``None`` when it holds none) -- a pilot's deadline is
        dispatcher state.  A consumer that knows the member's ``end_time``
        prefers that and falls back to this live figure (see the
        federation's ``MemberRecord.remaining_sec``).  Never negative: a
        pilot past a mis-estimated deadline has no time left, it does not
        owe any.

        The failure count and pause come from the policy's
        ``member_health``; ``register_policy`` admits only
        :class:`DispatchPolicy` subclasses, whose base answers a healthy
        member.

        *views* maps pid to the ``asdict`` view :meth:`_summarize_pool`
        already built for the pool-level history.
        '''
        mine    = ps.live_pilots_for(m.member_id)
        history = ps.pilot_history(m.member_id)
        used    = node_hours(history, now=now)
        health  = ps.policy.member_health(m.member_id)
        left    = [max(0.0, p.walltime_deadline - now) for p in mine
                   if p.walltime_deadline]
        return {
            'member_id'           : m.member_id,
            'endpoint_name'       : m.endpoint_name,
            'queue'               : m.queue,
            'account'             : m.account,
            'pilot'               : m.pilot,
            'end_time'            : m.end_time,
            'remaining_sec'       : max(left) if left else None,
            'attributes'          : dict(m.attributes),
            'budget'              : dict(m.budget),
            'min_pilots'          : m.min_pilots,
            'max_pilots'          : m.max_pilots,
            'shared_fs'           : m.shared_fs,
            'pilot_sizes'         : self._sizes_dict(m.pilot_sizes),
            'default_size'        : m.default_size,
            'live_pilots'         : len(mine),
            'pilots_active'       : sum(1 for p in mine
                                        if p.state == PILOT_ACTIVE),
            'node_hours_used'     : used,
            'node_hours_remaining': ps.member_budget_left(m.member_id, now),
            'pilot_history'       : [views[p.pid] for p in history],
            # -- why this member is not producing pilots ------------------
            # The reason the most recent pilot of this member died, plus
            # what the policy holds against it.  Without these a member
            # whose every submit fails is indistinguishable from an idle
            # one: `live_pilots` is 0 either way.
            'last_pilot_error'          : self._last_pilot_error(history),
            'consecutive_pilot_failures':
                health['consecutive_pilot_failures'],
            'paused_until'              : health['paused_until'],
        }

    # -- session-close teardown (owner lost / ttl / cancel_all) ---------

    async def _teardown_session_pools(self, sid: str) -> int:
        '''Tear down every pool owned by *sid*: cancel pilots, drop the pools.

        The session-close hook that ties a pool's pilots to the owning
        session's lifetime.  Each live pilot is cancelled (best-effort psij
        cancel + FAILED); the pools are dropped.  Idempotent.
        '''
        pools = self._pool_states.pop(sid, None)
        if not pools:
            return 0
        for ps in pools.values():
            for pilot in list(ps.pilots.values()):
                if pilot.is_terminal():
                    continue
                try:
                    await self._do_pilot_cancel(ps, pilot)
                except Exception as e:
                    log.warning('[%s] pilot %s teardown-cancel failed: %s',
                                self.instance_name, pilot.pid, e)
            # Drop this pool's uid→task correlations.
            for uid, (usid, _pool, _tid) in list(self._uid_to_task.items()):
                if usid == sid:
                    self._uid_to_task.pop(uid, None)
            try:    ps.close()
            except Exception:
                pass
        log.info('[%s] tore down %d pool(s) for session %s',
                 self.instance_name, len(pools), sid)
        return len(pools)
