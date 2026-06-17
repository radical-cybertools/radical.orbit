"""Unit tests for task_dispatcher_state.

Covers: record dataclasses, append/replay, snapshot compaction, malformed
line recovery, schema-additive replay.
"""

import json
from pathlib import Path

from radical.edge.task_dispatcher_state import (
    PilotRecord, TaskRecord, StateLog,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE, PILOT_DONE, PILOT_FAILED,
    PILOT_TERMINAL_STATES, PILOT_LIVE_STATES,
    TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
    TASK_TERMINAL_STATES,
)


# ---------------------------------------------------------------------------
# Record helpers / properties
# ---------------------------------------------------------------------------

class TestPilotRecord:

    def test_lag_none_before_active(self):
        p = PilotRecord(pid='p.a', pool='x', size_key='s',
                        rhapsody_backend='concurrent',
                        submitted_at=100.0)
        assert p.lag() is None

    def test_lag_computed_after_active(self):
        p = PilotRecord(pid='p.a', pool='x', size_key='s',
                        rhapsody_backend='concurrent',
                        submitted_at=100.0, active_at=150.0)
        assert p.lag() == 50.0

    def test_is_terminal(self):
        assert PilotRecord(pid='p', pool='x', size_key='s',
                           rhapsody_backend='c',
                           state=PILOT_DONE).is_terminal()
        assert PilotRecord(pid='p', pool='x', size_key='s',
                           rhapsody_backend='c',
                           state=PILOT_FAILED).is_terminal()
        assert not PilotRecord(pid='p', pool='x', size_key='s',
                               rhapsody_backend='c',
                               state=PILOT_ACTIVE).is_terminal()

    def test_free_capacity_only_when_active(self):
        p = PilotRecord(pid='p', pool='x', size_key='s',
                        rhapsody_backend='c', state=PILOT_ACTIVE,
                        capacity=8, in_flight=3)
        assert p.free_capacity() == 5

        p.state = PILOT_PENDING
        assert p.free_capacity() == 0

        p.state = PILOT_ACTIVE
        p.accepting_new_tasks = False
        assert p.free_capacity() == 0

    def test_free_capacity_never_negative(self):
        p = PilotRecord(pid='p', pool='x', size_key='s',
                        rhapsody_backend='c', state=PILOT_ACTIVE,
                        capacity=4, in_flight=10)
        assert p.free_capacity() == 0


class TestTaskRecord:

    def test_is_terminal(self):
        assert TaskRecord(task_id='t', pool='x', cmd=['a'], cwd='/',
                          state=TASK_DONE).is_terminal()
        assert TaskRecord(task_id='t', pool='x', cmd=['a'], cwd='/',
                          state=TASK_FAILED).is_terminal()
        assert TaskRecord(task_id='t', pool='x', cmd=['a'], cwd='/',
                          state=TASK_CANCELED).is_terminal()
        assert not TaskRecord(task_id='t', pool='x', cmd=['a'], cwd='/',
                              state=TASK_RUNNING).is_terminal()

    def test_state_vocabulary_fully_covered(self):
        assert PILOT_TERMINAL_STATES <= {PILOT_DONE, PILOT_FAILED}
        assert PILOT_LIVE_STATES == {PILOT_PENDING, PILOT_STARTING,
                                     PILOT_ACTIVE}
        assert TASK_TERMINAL_STATES == {TASK_DONE, TASK_FAILED,
                                        TASK_CANCELED}


# ---------------------------------------------------------------------------
# StateLog append/replay
# ---------------------------------------------------------------------------

class TestStateLog:

    def test_append_and_replay_single_record(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        p = PilotRecord(pid='p.1', pool='cpu', size_key='s',
                        rhapsody_backend='concurrent')
        log.append(p)
        state = log.replay()
        assert set(state.keys()) == {'p.1'}
        assert state['p.1'].pool == 'cpu'

    def test_last_write_wins(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        p = PilotRecord(pid='p.1', pool='cpu', size_key='s',
                        rhapsody_backend='concurrent',
                        state=PILOT_PENDING)
        log.append(p)
        p.state = PILOT_ACTIVE
        p.capacity = 8
        log.append(p)
        state = log.replay()
        assert state['p.1'].state == PILOT_ACTIVE
        assert state['p.1'].capacity == 8

    def test_multiple_records(self, tmp_path: Path):
        log = StateLog(tmp_path / 'task.log', TaskRecord, 'task_id')
        for i in range(5):
            log.append(TaskRecord(task_id=f't.{i}', pool='x',
                                  cmd=[str(i)], cwd='/'))
        state = log.replay()
        assert set(state.keys()) == {f't.{i}' for i in range(5)}

    def test_malformed_line_skipped(self, tmp_path: Path):
        log = StateLog(tmp_path / 'task.log', TaskRecord, 'task_id')
        log.append(TaskRecord(task_id='t.good', pool='x', cmd=['a'], cwd='/'))
        # Inject a garbage line
        with log.path.open('a') as f:
            f.write('not valid json\n')
        log.append(TaskRecord(task_id='t.also_good', pool='x', cmd=['b'], cwd='/'))
        state = log.replay()
        assert set(state.keys()) == {'t.good', 't.also_good'}

    def test_unknown_fields_dropped_on_replay(self, tmp_path: Path):
        """Future schema extensions survive loading of older logs."""
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        # Manually craft a log line with an unknown field
        payload = {
            'pid': 'p.x', 'pool': 'cpu', 'size_key': 's',
            'rhapsody_backend': 'concurrent',
            'state': PILOT_ACTIVE, 'submitted_at': 1.0,
            'future_field': 'whatever',
        }
        with log.path.open('a') as f:
            f.write(json.dumps(payload) + '\n')
        state = log.replay()
        assert state['p.x'].pool == 'cpu'

    def test_empty_log_replay(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        assert log.replay() == {}


# ---------------------------------------------------------------------------
# Snapshot compaction
# ---------------------------------------------------------------------------

class TestSnapshot:

    def test_snapshot_truncates_log(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        log.append(PilotRecord(pid='p.1', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent'))
        log.append(PilotRecord(pid='p.2', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent'))
        state = log.replay()

        log.snapshot(state)

        assert log.snapshot_path.is_file()
        assert log.path.stat().st_size == 0  # truncated

        # Replay still gives the same state
        state2 = log.replay()
        assert set(state2.keys()) == {'p.1', 'p.2'}

    def test_snapshot_plus_new_appends(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        log.append(PilotRecord(pid='p.a', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent',
                               state=PILOT_ACTIVE))
        log.snapshot(log.replay())

        log.append(PilotRecord(pid='p.b', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent',
                               state=PILOT_PENDING))
        state = log.replay()
        assert set(state.keys()) == {'p.a', 'p.b'}
        assert state['p.a'].state == PILOT_ACTIVE

    def test_snapshot_atomic_against_crash(self, tmp_path: Path):
        """A corrupt snapshot file falls back to log-only replay."""
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        log.append(PilotRecord(pid='p.keep', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent'))
        # Corrupt the snapshot file directly
        log.snapshot_path.write_text("garbage")
        state = log.replay()
        # log-only replay still recovers
        assert 'p.keep' in state

    def test_snapshot_overwrite(self, tmp_path: Path):
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        log.append(PilotRecord(pid='p.1', pool='cpu', size_key='s',
                               rhapsody_backend='concurrent',
                               state=PILOT_PENDING))
        log.snapshot(log.replay())

        p = PilotRecord(pid='p.1', pool='cpu', size_key='s',
                        rhapsody_backend='concurrent', state=PILOT_ACTIVE,
                        capacity=4, active_at=5.0)
        log.append(p)
        log.snapshot(log.replay())

        state = log.replay()
        assert state['p.1'].state == PILOT_ACTIVE
        assert state['p.1'].capacity == 4

    def test_creates_parent_dirs(self, tmp_path: Path):
        nested = tmp_path / 'a' / 'b' / 'c' / 'pilot.log'
        log = StateLog(nested, PilotRecord, 'pid')
        assert nested.parent.is_dir()
        log.append(PilotRecord(pid='p', pool='x', size_key='s',
                               rhapsody_backend='c'))
        assert log.replay()['p'].pool == 'x'

    def test_append_resumes_at_eof_after_truncate(self, tmp_path: Path):
        """The held O_APPEND handle keeps working across a snapshot."""
        log = StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')
        for i in range(3):
            log.append(PilotRecord(pid=f'p.{i}', pool='x', size_key='s',
                                   rhapsody_backend='c'))
        log.snapshot(log.replay())          # truncates through the handle
        # Appends after truncation must land at the new (zero) EOF, not
        # leave a sparse gap; replay should see snapshot + new record.
        log.append(PilotRecord(pid='p.new', pool='x', size_key='s',
                               rhapsody_backend='c'))
        state = log.replay()
        assert set(state.keys()) == {'p.0', 'p.1', 'p.2', 'p.new'}


# ---------------------------------------------------------------------------
# Compaction triggers + handle lifecycle
# ---------------------------------------------------------------------------

class TestCompactionPolicy:

    def _log(self, tmp_path: Path) -> StateLog:
        return StateLog(tmp_path / 'pilot.log', PilotRecord, 'pid')

    def _rec(self, i: int) -> PilotRecord:
        return PilotRecord(pid=f'p.{i}', pool='x', size_key='s',
                           rhapsody_backend='c')

    def test_no_compaction_when_idle(self, tmp_path: Path):
        log = self._log(tmp_path)
        assert log.needs_compaction(max_appends=1, max_age_sec=0.0) is False

    def test_size_trigger(self, tmp_path: Path):
        log = self._log(tmp_path)
        for i in range(5):
            log.append(self._rec(i))
        assert log.needs_compaction(max_appends=5, max_age_sec=1e9) is True
        assert log.needs_compaction(max_appends=6, max_age_sec=1e9) is False

    def test_age_trigger(self, tmp_path: Path):
        log = self._log(tmp_path)
        log.append(self._rec(0))
        # Below the size threshold, but past the age window → due.
        future = log._last_snapshot_ts + 100.0
        assert log.needs_compaction(max_appends=1000, max_age_sec=10.0,
                                    now=future) is True

    def test_counters_reset_after_snapshot(self, tmp_path: Path):
        log = self._log(tmp_path)
        for i in range(3):
            log.append(self._rec(i))
        log.snapshot(log.replay())
        assert log.needs_compaction(max_appends=1, max_age_sec=1e9) is False

    def test_close_is_idempotent(self, tmp_path: Path):
        log = self._log(tmp_path)
        log.append(self._rec(0))
        log.close()
        log.close()   # must not raise
