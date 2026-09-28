'''
Task dispatcher — state records and durable per-pool store.

Two record types survive dispatcher restarts:

- :class:`PilotRecord` — the dispatcher's view of one submitted batch
  job (the "pilot") plus its eventual child endpoint.
- :class:`TaskRecord`  — the dispatcher's view of one dispatched task,
  uniquely keyed by ``task_id``.

Persistence is one ``state.json`` per pool holding the pool config plus the
pilot and task maps.  It is rewritten atomically (tempfile + ``os.replace``)
on every mutation; the write is microseconds at this scale.  Recovery is a
single ``json.load`` — there is no append log, snapshot overlay, or
compaction.

State machines
--------------
Pilot:  ``PENDING → STARTING → ACTIVE → (DONE | FAILED)``
        (``ACTIVE`` may be entered from any earlier state on handshake,
        skipping ``STARTING`` if the pilot came up faster than expected.)
        Three timestamps bracket that walk — ``submitted_at``, ``active_at``
        and ``finished_at`` — so a terminal pilot still carries the interval
        it actually held the allocation (see :func:`node_hours`).

Task:   ``QUEUED → RUNNING → (DONE | FAILED | CANCELED)``
'''

from __future__ import annotations

import dataclasses
import json
import logging
import os
import tempfile
import time

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# State vocabularies
# ---------------------------------------------------------------------------

# Pilot state machine
PILOT_PENDING   = 'PENDING'
PILOT_STARTING  = 'STARTING'
PILOT_ACTIVE    = 'ACTIVE'
PILOT_DONE      = 'DONE'
PILOT_FAILED    = 'FAILED'

PILOT_STATES          = {PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE,
                         PILOT_DONE, PILOT_FAILED}
PILOT_TERMINAL_STATES = {PILOT_DONE, PILOT_FAILED}
PILOT_LIVE_STATES     = PILOT_STATES - PILOT_TERMINAL_STATES

# Task state machine
TASK_QUEUED   = 'QUEUED'
TASK_RUNNING  = 'RUNNING'
TASK_DONE     = 'DONE'
TASK_FAILED   = 'FAILED'
TASK_CANCELED = 'CANCELED'

TASK_STATES          = {TASK_QUEUED, TASK_RUNNING, TASK_DONE,
                        TASK_FAILED, TASK_CANCELED}
TASK_TERMINAL_STATES = {TASK_DONE, TASK_FAILED, TASK_CANCELED}

# How much of a pilot's failure reason is kept on its record.  A psij
# traceback can be kilobytes and the record rides in the ``pilot_history``
# of every verbose summary, so it is truncated to something a table row
# and a tooltip can carry.
PILOT_ERROR_MAX = 300


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@dataclass
class PilotRecord:
    '''Dispatcher's view of one pilot (= one SLURM/PBS batch job).'''
    pid                : str           # dispatcher-local id: "p.<uuid8>"
    pool               : str           # pool name
    size_key           : str           # key into pool.pilot_sizes
    rhapsody_backend   : str           # resolved from PilotSize
    owning_sid         : str         = ''      # session that owns this pool
    psij_job_id        : str | None  = None    # set after submit_tunneled
    child_endpoint_name    : str | None  = None    # set at handshake
    state              : str         = PILOT_PENDING
    submitted_at       : float       = 0.0
    active_at          : float | None = None   # PENDING → ACTIVE time
    capacity           : int         = 0       # concurrent tasks (from handshake)
    in_flight          : int         = 0
    started_tasks      : int         = 0       # monotonic counter
    walltime_deadline  : float       = 0.0
    accepting_new_tasks: bool        = True    # flipped False by drain
    finished_at        : float | None = None   # terminal-state timestamp
    # Why this pilot went FAILED, truncated to ``PILOT_ERROR_MAX``.  A
    # submit-side failure ('psij error: … Disk quota exceeded') is
    # otherwise visible only in the broker log, while every consumer of
    # this record sees a pilot that simply is not there.
    error              : str | None  = None
    # -- capability-class fields ------------------------------------------
    # ``member_id`` is the pool member this pilot was submitted for; ``''``
    # means the implicit member of a legacy pool.  ``attributes`` and the
    # size/endpoint snapshots are taken **at submit time** and never change:
    # a pilot outlives its member, and a removed member's pilots still have
    # to be cancelled (``endpoint_name``), sized (the node counts) and
    # matched (``attributes``) after the member is gone.
    member_id          : str         = ''
    attributes         : dict        = field(default_factory=dict)
    endpoint_name      : str         = ''      # who runs the psij job
    nodes              : int         = 0
    cpus_per_node      : int         = 0
    gpus_per_node      : int         = 0
    # This pilot **is** an endpoint the dispatcher adopted (a
    # ``pilot: endpoint`` member, plan 122), not a batch job it submitted.
    # Stamped once at creation and persisted, because the two end
    # differently -- an adopted pilot goes DONE, never FAILED, when its
    # endpoint disappears or its member is removed -- and the distinction
    # has to survive a restart.  Deliberately NOT inferred from a missing
    # ``psij_job_id``: a *submitted* pilot has none either, for the window
    # between its child endpoint name being pre-bound and psij answering.
    adopted            : bool        = False

    def lag(self) -> float | None:
        '''Return the PENDING→ACTIVE duration, or ``None`` if not yet active.'''
        if self.active_at is None:
            return None
        return self.active_at - self.submitted_at

    def is_terminal(self) -> bool:
        '''Return whether this pilot is in a terminal (DONE/FAILED) state.'''
        return self.state in PILOT_TERMINAL_STATES

    def free_capacity(self) -> int:
        '''Return open task slots, or 0 when draining/terminal.'''
        if self.state != PILOT_ACTIVE or not self.accepting_new_tasks:
            return 0
        return max(0, self.capacity - self.in_flight)


@dataclass
class TaskRecord:
    '''Dispatcher's view of one dispatched task, keyed by ``task_id``.'''
    task_id      : str
    pool         : str
    cmd          : list[str]
    cwd          : str                          # shared-FS scratch path
    owning_sid   : str         = ''             # session that owns the pool
    priority     : int         = 0
    inputs       : list[str]   = field(default_factory=list)
    outputs      : list[str]   = field(default_factory=list)
    state        : str         = TASK_QUEUED
    pilot_id     : str | None  = None           # set on assignment
    rhapsody_uid : str | None  = None           # rhapsody-side task uid
    submitted_at : float       = 0.0
    started_at   : float | None = None
    finished_at  : float | None = None
    exit_code    : int | None  = None
    arrival_ts   : float       = 0.0            # tie-break for queue ordering
    error        : str | None  = None
    # Per-task resource shape as submitted, validated at submit by
    # ``plugin_task_dispatcher.parse_requirements`` (shape -- which may
    # also derive an omitted ``cores`` from ``ranks``) and
    # ``check_requirements_against_pool`` (fit + backend gate).
    # ``{}`` means "no declaration" and forwards byte-identically to
    # pre-requirements behaviour.  An older ``state.json`` without the key
    # loads as ``{}`` because ``record_from_dict`` drops unknown keys and
    # this field defaults.  ``software``/``labels`` are persisted but not
    # acted on in this round (dispatcher-side placement attributes; plan
    # 121).
    requirements : dict        = field(default_factory=dict)
    # -- capability-class fields ------------------------------------------
    # The member this task is currently placed on -- set at dispatch beside
    # ``pilot_id`` and cleared with it when a pilot loss re-queues the task.
    # Redundant with ``pilots[pilot_id].member_id`` by construction; kept
    # because it is on the wire, so a consumer need not join on the pilot.
    member_id    : str | None  = None
    # Times a pilot loss re-queued this task; capped by the pool policy's
    # ``max_requeues``.
    requeues     : int         = 0
    # Files the dispatcher actually holds for this task under
    # ``<state_dir>/inputs/<task_id>/`` (submitted as ``inputs_b64``).  NOT
    # ``inputs``, which keeps its client-declared meaning -- the names a
    # client says the task consumes, which the dispatcher never acts on.
    spooled      : list[str]   = field(default_factory=list)
    # True when ``cwd`` was assigned by the dispatcher at dispatch rather
    # than supplied by the client, so a re-dispatch to another member may
    # re-assign it.
    cwd_assigned : bool        = False
    # Rhapsody-dialect tasks: the serialized task dict as submitted
    # (JSON-safe -- cloudpickled fields ride as base64 strings), forwarded
    # verbatim to the pilot's rhapsody session.  ``None`` marks an
    # exec-style task, which runs off ``cmd``/``cwd`` as before.
    task_dict    : dict | None = None

    def is_terminal(self) -> bool:
        '''Return whether this task is in a terminal state.'''
        return self.state in TASK_TERMINAL_STATES


# ---------------------------------------------------------------------------
# Node-hour accounting
# ---------------------------------------------------------------------------

def node_hours(history: list[dict] | None,
               now: float | None = None) -> float:
    '''Return the node-hours consumed by a list of pilot dicts.

    *history* is a list of ``asdict(PilotRecord)`` views (the pool- or
    member-level ``pilot_history`` of a verbose summary).  A pilot that has
    not finished yet is charged up to *now*.

    The node count is ``entry['nodes']`` — the size snapshot taken at
    submit time.  It is the only source that is correct for a
    **mixed-node-count** pool and the only one that still works once the
    pilot's member has been removed (its size menu is gone with it).  An
    entry without one (a pre-121 record) is skipped.

    An entry with no ``active_at`` is skipped entirely: a pilot that never
    reached ACTIVE consumed no allocation, and queue time is not charged.

    This lives here, not in the federation, because the dispatcher needs it
    for its own per-member summary and must not import a federation module.
    '''
    if not history:
        return 0.0
    if now is None:
        now = time.time()

    total = 0.0
    for entry in history:
        nodes = entry.get('nodes') or 0
        if not nodes:
            continue

        # Charge from ``active_at`` ONLY: a pilot's queue time is not
        # allocation time, and a pilot that never reached ACTIVE consumed
        # nothing.  (Falling back to ``submitted_at`` would both bill queue
        # time and charge a never-started record from the epoch to `now`.)
        # Both timestamps are tested against ``None``, not truthiness: a
        # ``0.0`` is the epoch, which is a legitimate (if odd) instant and
        # must not read as "absent".
        start = entry.get('active_at')
        if start is None:
            continue
        end = entry.get('finished_at')
        if end is None:
            end = now
        total += nodes * max(0.0, end - start) / 3600.0

    return total


# ---------------------------------------------------------------------------
# Atomic JSON I/O
# ---------------------------------------------------------------------------

def write_json_atomic(path: str | Path, payload: Any) -> None:
    '''Write *payload* as JSON to *path* atomically (tempfile + ``os.replace``).

    The tempfile is created in the destination directory so the rename stays
    on one filesystem; it is fsynced before the replace so a crash can only
    ever leave the old file or the new file, never a torn one.
    '''
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.state.', suffix='.tmp',
                               dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(payload, f, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_json(path: str | Path, default: Any = None) -> Any:
    '''Return the JSON at *path*, or *default* when absent/unreadable.'''
    path = Path(path)
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as e:
        log.warning('task_dispatcher: unreadable state %s: %s', path, e)
        return default


# ---------------------------------------------------------------------------
# Record (de)serialisation
# ---------------------------------------------------------------------------

def record_from_dict(record_cls: type, data: dict) -> Any:
    '''Reconstruct a record dataclass from *data*, dropping unknown keys.

    Unknown keys are ignored so an older ``state.json`` survives a schema
    addition.
    '''
    valid = {f.name for f in dataclasses.fields(record_cls)}
    return record_cls(**{k: v for k, v in data.items() if k in valid})


def records_from(data: dict | None, record_cls: type) -> dict[str, Any]:
    '''Reconstruct a ``{id: record}`` map from a persisted dict.'''
    return {rec_id: record_from_dict(record_cls, rec)
            for rec_id, rec in (data or {}).items()}


def records_to(records: dict[str, Any]) -> dict[str, dict]:
    '''Serialise a ``{id: record}`` map to plain dicts for persistence.'''
    return {rec_id: asdict(rec) for rec_id, rec in records.items()}


# ---------------------------------------------------------------------------
# Per-pool durable store
# ---------------------------------------------------------------------------

class PoolStore:
    '''One ``state.json`` for a pool: config + pilots + tasks.

    Rewritten atomically on every mutation; recovery is a single
    ``json.load``.  A single-owner discipline (all reads/writes happen on the
    plugin's event-loop thread) means no locking is needed.
    '''

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        '''Return the backing ``state.json`` path.'''
        return self._path

    def load(self) -> dict:
        '''Return the persisted payload, or ``{}`` when absent/unreadable.'''
        return read_json(self._path, default={}) or {}

    def save(self, owning_sid: str, config: dict,
             pilots: dict[str, Any], tasks: dict[str, Any]) -> None:
        '''Rewrite the pool's ``state.json`` atomically.'''
        write_json_atomic(self._path, {
            'owning_sid': owning_sid,
            'config'    : config,
            'pilots'    : records_to(pilots),
            'tasks'     : records_to(tasks),
        })
