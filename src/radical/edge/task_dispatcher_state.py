'''
Task dispatcher — state records and append-only log.

Two record types survive dispatcher restarts:

- :class:`PilotRecord` — the dispatcher's view of one submitted batch
  job (the "pilot") plus its eventual child edge.
- :class:`TaskRecord`  — the dispatcher's view of one Makeflow rule
  dispatched via the wrapper, uniquely keyed by ``task_id``.

Persistence is an append-only JSONL log per pool
(``pilot.log``, ``task.log``) plus periodic ``snapshot.json``.  On plugin
startup the log is replayed and cross-referenced with the bridge's
topology to reconcile orphan pilots.

State machines
--------------
Pilot:  ``PENDING → STARTING → ACTIVE → (DONE | FAILED)``
        (``ACTIVE`` may be entered from any earlier state on handshake,
        skipping ``STARTING`` if the pilot came up faster than expected.)

Task:   ``QUEUED → RUNNING → (DONE | FAILED | CANCELED)``
'''

from __future__ import annotations

import json
import logging
import os
import tempfile
import time

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger('radical.edge')


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
    psij_job_id        : str | None  = None    # set after submit_tunneled
    child_edge_name    : str | None  = None    # set at handshake
    state              : str         = PILOT_PENDING
    submitted_at       : float       = 0.0
    active_at          : float | None = None   # PENDING → ACTIVE time
    capacity           : int         = 0       # concurrent tasks (from handshake)
    in_flight          : int         = 0
    started_tasks      : int         = 0       # monotonic counter
    walltime_deadline  : float       = 0.0
    accepting_new_tasks: bool        = True    # flipped False by drain_pilot

    def lag(self) -> float | None:
        '''PENDING→ACTIVE duration, or ``None`` if not yet active.'''
        if self.active_at is None:
            return None
        return self.active_at - self.submitted_at

    def is_terminal(self) -> bool:
        return self.state in PILOT_TERMINAL_STATES

    def free_capacity(self) -> int:
        '''Slots available for new task assignment, or 0 if draining/terminal.'''
        if self.state != PILOT_ACTIVE or not self.accepting_new_tasks:
            return 0
        return max(0, self.capacity - self.in_flight)


@dataclass
class TaskRecord:
    '''Dispatcher's view of one Makeflow rule dispatched via the wrapper.'''
    task_id      : str                          # sha1 prefix (stable per run)
    pool         : str
    cmd          : list[str]
    cwd          : str                          # shared-FS scratch path
    priority     : int         = 0              # passed through from Makeflow
    inputs       : list[str]   = field(default_factory=list)
    outputs      : list[str]   = field(default_factory=list)
    state        : str         = TASK_QUEUED
    pilot_id     : str | None  = None           # set on assignment
    rhapsody_uid : str | None  = None           # rhapsody-side task uid
    submitted_at : float       = 0.0
    started_at   : float | None = None
    finished_at  : float | None = None
    exit_code    : int | None  = None
    arrival_ts   : float       = 0.0            # for arrivals_window
    error        : str | None  = None

    def is_terminal(self) -> bool:
        return self.state in TASK_TERMINAL_STATES


@dataclass
class EdgeModeRecord:
    '''Ledger entry for one edge-mode task (transparent rhapsody proxy).

    Edge-mode tasks bypass pool state, so they need their own tiny
    persisted ledger to survive a bridge restart (C4): an in-flight
    entry lets get/cancel routes re-resolve the target edge after
    replay.  A terminal record is appended on completion and filtered
    out on replay, so the ledger self-prunes via the usual
    last-write-wins compaction.
    '''
    task_id: str
    edge   : str
    state  : str = TASK_RUNNING

    def is_terminal(self) -> bool:
        return self.state in TASK_TERMINAL_STATES


# ---------------------------------------------------------------------------
# Append-only JSONL log
# ---------------------------------------------------------------------------

class StateLog:
    '''Append-only JSONL log for one record type within one pool.

    The log is a sequence of "events" — each event is the dataclass
    ``asdict`` of the record at the moment of write.  Replay reduces
    events to a ``{id: record}`` map by keeping the last write per id.

    Snapshots compact the log: the entire current map is written to
    ``snapshot.json`` atomically (tempfile + rename); on next replay,
    the snapshot is loaded first and subsequent events overlay it.
    '''

    def __init__(self, path: str | Path, record_cls: type,
                 id_attr: str) -> None:
        '''Open (creating parents as needed) an append-only log.

        Args:
            path       : Path to the ``.log`` file.  Snapshot lives next
                         to it as ``<stem>.snapshot.json``.
            record_cls : Dataclass to reconstruct on replay.
            id_attr    : Name of the field that uniquely identifies a
                         record (e.g. ``'pid'`` for ``PilotRecord``).
        '''
        self._path         = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._record_cls   = record_cls
        self._id_attr      = id_attr
        self._snapshot_path = self._path.with_suffix('.snapshot.json')

        # One persistent append handle, held open for the life of the
        # log.  Opening in ``'a'`` creates the file and sets ``O_APPEND``,
        # so every write atomically targets EOF — which lets
        # :meth:`snapshot` truncate through this same handle (writes then
        # resume cleanly at the new, zero EOF) without reopening, and
        # avoids an open/close syscall pair on every :meth:`append`.
        # Single-owner discipline: append + snapshot run on one thread
        # (the plugin's event loop), so no locking is needed here.
        self._fh = self._path.open('a')

        # Compaction bookkeeping consumed by the plugin's state sweeper
        # via :meth:`needs_compaction`: how many appends have accrued
        # since the last snapshot, and when that snapshot happened.
        self._appends_since_snapshot = 0
        self._last_snapshot_ts       = time.time()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def snapshot_path(self) -> Path:
        return self._snapshot_path

    def append(self, record: Any) -> None:
        '''Append a record to the log.  Flushes after each write.

        Writes through the persistent ``O_APPEND`` handle and ``flush``es
        (to the OS page cache) — same durability as before: survives a
        process crash, not a power loss.  The log stays the source of
        truth between snapshots.
        '''
        line = json.dumps(asdict(record), default=str)
        self._fh.write(line + '\n')
        self._fh.flush()
        self._appends_since_snapshot += 1

    def replay(self) -> dict[str, Any]:
        '''Reduce snapshot + log to ``{id: record}``.

        Malformed JSON lines are logged and skipped rather than crashing
        the dispatcher — a partial write from a crash should not wedge
        the next startup.
        '''
        state: dict[str, Any] = {}

        if self._snapshot_path.is_file():
            try:
                snap = json.loads(self._snapshot_path.read_text())
                for rec_id, data in snap.items():
                    state[rec_id] = self._from_dict(data)
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                log.warning("task_dispatcher: snapshot %s unreadable: %s",
                            self._snapshot_path, e)

        if self._path.is_file():
            for line in self._iter_lines():
                try:
                    data = json.loads(line)
                    rec  = self._from_dict(data)
                    state[getattr(rec, self._id_attr)] = rec
                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    log.warning(
                        "task_dispatcher: skipping malformed log line "
                        "in %s: %s", self._path, e)

        return state

    def snapshot(self, state: dict[str, Any]) -> None:
        '''Write a compacted snapshot atomically, then truncate the log.

        Atomicity: write to tempfile in the same directory, fsync, rename.
        """Poor man's atomic replace""" — good enough for the dispatcher's
        crash-safety needs; the log is still the source of truth between
        snapshots.
        '''
        payload = {rec_id: asdict(rec) for rec_id, rec in state.items()}
        # Same directory as the snapshot path so rename is on the same FS
        fd, tmp = tempfile.mkstemp(
            prefix='.snapshot.', suffix='.tmp',
            dir=str(self._snapshot_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(payload, f, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._snapshot_path)
        except Exception:
            # Leave stale tempfile for inspection; don't corrupt anything.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

        # Truncate the log now that its contents are captured in the
        # snapshot, through the persistent handle.  With ``O_APPEND`` the
        # next ``append`` write still lands at EOF (now zero), so no
        # reopen is needed and no sparse hole is created.
        self._fh.flush()
        self._fh.seek(0)
        self._fh.truncate()
        self._appends_since_snapshot = 0
        self._last_snapshot_ts       = time.time()

    def needs_compaction(self, *, max_appends: int, max_age_sec: float,
                         now: float | None = None) -> bool:
        '''Whether this log is due for a snapshot.

        Compaction triggers when there are uncompacted appends AND
        either:
        - their count has reached *max_appends* (size trigger), or
        - the oldest uncompacted append has lingered past *max_age_sec*
          (age trigger) — so a low-traffic log whose tail never reaches
          the size threshold still gets compacted instead of growing
          slowly forever.

        Returns ``False`` when nothing has been appended since the last
        snapshot, so an idle log is never rewritten.
        '''
        if self._appends_since_snapshot <= 0:
            return False
        if self._appends_since_snapshot >= max_appends:
            return True
        now = now if now is not None else time.time()
        return (now - self._last_snapshot_ts) >= max_age_sec

    def close(self) -> None:
        '''Flush and close the persistent append handle.  Idempotent.'''
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass

    def _iter_lines(self) -> Iterator[str]:
        '''Yield non-empty stripped lines from the log.'''
        with self._path.open('r') as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line

    def _from_dict(self, data: dict) -> Any:
        '''Reconstruct a record dataclass from a dict read from disk.

        Unknown keys are dropped silently so old logs survive schema
        additions.
        '''
        import dataclasses as _dc
        valid = {f.name for f in _dc.fields(self._record_cls)}
        return self._record_cls(**{k: v for k, v in data.items()
                                   if k in valid})
