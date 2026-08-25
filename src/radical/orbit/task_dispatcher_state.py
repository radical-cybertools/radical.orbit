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

Task:   ``QUEUED → RUNNING → (DONE | FAILED | CANCELED)``
'''

from __future__ import annotations

import dataclasses
import json
import logging
import os
import tempfile

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
    # Rhapsody-dialect tasks: the serialized task dict as submitted
    # (JSON-safe -- cloudpickled fields ride as base64 strings), forwarded
    # verbatim to the pilot's rhapsody session.  ``None`` marks an
    # exec-style task, which runs off ``cmd``/``cwd`` as before.
    task_dict    : dict | None = None

    def is_terminal(self) -> bool:
        '''Return whether this task is in a terminal state.'''
        return self.state in TASK_TERMINAL_STATES


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
