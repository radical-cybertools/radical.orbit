"""Unit tests for task_dispatcher_state.

Covers: record dataclasses, atomic JSON I/O helpers (``write_json_atomic`` /
``read_json``), record (de)serialisation (``record_from_dict`` /
``records_from`` / ``records_to``), and the per-pool durable store
(``PoolStore``).  There is no append log, snapshot overlay, or compaction —
persistence is one ``state.json`` per pool, rewritten atomically on every
mutation; recovery is a single ``json.load``.
"""

from pathlib import Path

from radical.orbit.task_dispatcher_state import (
    PilotRecord, TaskRecord, PoolStore,
    read_json, write_json_atomic,
    record_from_dict, records_from, records_to,
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
# Atomic JSON I/O
# ---------------------------------------------------------------------------

class TestAtomicJsonIO:

    def test_write_then_read_round_trip(self, tmp_path: Path):
        path = tmp_path / 'x.json'
        write_json_atomic(path, {'a': 1, 'b': [1, 2, 3]})
        assert read_json(path) == {'a': 1, 'b': [1, 2, 3]}

    def test_read_missing_file_returns_default(self, tmp_path: Path):
        assert read_json(tmp_path / 'nope.json') is None
        assert read_json(tmp_path / 'nope.json', default={}) == {}

    def test_read_malformed_json_returns_default(self, tmp_path: Path):
        path = tmp_path / 'bad.json'
        path.write_text('{not valid json')
        assert read_json(path, default={}) == {}

    def test_write_creates_parent_dirs(self, tmp_path: Path):
        nested = tmp_path / 'a' / 'b' / 'c' / 'x.json'
        write_json_atomic(nested, {'ok': True})
        assert nested.is_file()
        assert read_json(nested) == {'ok': True}

    def test_write_leaves_no_tempfile_behind(self, tmp_path: Path):
        path = tmp_path / 'x.json'
        write_json_atomic(path, {'a': 1})
        leftovers = [p for p in tmp_path.iterdir() if p != path]
        assert leftovers == []

    def test_overwrite_replaces_content(self, tmp_path: Path):
        path = tmp_path / 'x.json'
        write_json_atomic(path, {'a': 1})
        write_json_atomic(path, {'a': 2})
        assert read_json(path) == {'a': 2}


# ---------------------------------------------------------------------------
# Record (de)serialisation
# ---------------------------------------------------------------------------

class TestRecordSerialisation:

    def test_record_from_dict_round_trip(self):
        p = record_from_dict(PilotRecord, {
            'pid': 'p.1', 'pool': 'cpu', 'size_key': 's',
            'rhapsody_backend': 'concurrent', 'state': PILOT_ACTIVE,
        })
        assert isinstance(p, PilotRecord)
        assert p.pid == 'p.1' and p.state == PILOT_ACTIVE

    def test_record_from_dict_drops_unknown_fields(self):
        """Future schema extensions survive loading of an older state.json."""
        p = record_from_dict(PilotRecord, {
            'pid': 'p.x', 'pool': 'cpu', 'size_key': 's',
            'rhapsody_backend': 'concurrent', 'submitted_at': 1.0,
            'future_field': 'whatever',
        })
        assert p.pool == 'cpu'
        assert not hasattr(p, 'future_field')

    def test_records_from_empty_or_none(self):
        assert records_from(None, PilotRecord) == {}
        assert records_from({}, PilotRecord) == {}

    def test_records_to_and_from_round_trip(self):
        pilots = {
            'p.1': PilotRecord(pid='p.1', pool='cpu', size_key='s',
                              rhapsody_backend='concurrent',
                              state=PILOT_ACTIVE, capacity=8),
            'p.2': PilotRecord(pid='p.2', pool='cpu', size_key='s',
                              rhapsody_backend='concurrent'),
        }
        plain = records_to(pilots)
        assert set(plain.keys()) == {'p.1', 'p.2'}
        assert plain['p.1']['state'] == PILOT_ACTIVE

        restored = records_from(plain, PilotRecord)
        assert restored['p.1'].capacity == 8
        assert restored['p.2'].state == PILOT_PENDING


# ---------------------------------------------------------------------------
# PoolStore — one atomic state.json per pool
# ---------------------------------------------------------------------------

class TestPoolStore:

    def test_load_missing_file_returns_empty_dict(self, tmp_path: Path):
        store = PoolStore(tmp_path / 'pool' / 'state.json')
        assert store.load() == {}

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        nested = tmp_path / 'a' / 'b' / 'state.json'
        store = PoolStore(nested)
        assert nested.parent.is_dir()

    def test_path_property(self, tmp_path: Path):
        path = tmp_path / 'state.json'
        store = PoolStore(path)
        assert store.path == path

    def test_save_and_load_round_trip(self, tmp_path: Path):
        store = PoolStore(tmp_path / 'state.json')
        pilots = {'p.1': PilotRecord(pid='p.1', pool='cpu', size_key='s',
                                     rhapsody_backend='concurrent',
                                     state=PILOT_ACTIVE, capacity=4)}
        tasks = {'t.1': TaskRecord(task_id='t.1', pool='cpu',
                                   cmd=['echo'], cwd='/tmp',
                                   state=TASK_RUNNING)}
        store.save('sid-1', {'name': 'cpu', 'queue': 'batch'}, pilots, tasks)

        payload = store.load()
        assert payload['owning_sid'] == 'sid-1'
        assert payload['config'] == {'name': 'cpu', 'queue': 'batch'}

        restored_pilots = records_from(payload['pilots'], PilotRecord)
        restored_tasks  = records_from(payload['tasks'],  TaskRecord)
        assert restored_pilots['p.1'].state == PILOT_ACTIVE
        assert restored_pilots['p.1'].capacity == 4
        assert restored_tasks['t.1'].state == TASK_RUNNING
        assert restored_tasks['t.1'].cmd == ['echo']

    def test_save_overwrites_previous_state(self, tmp_path: Path):
        store = PoolStore(tmp_path / 'state.json')
        store.save('sid', {}, {}, {})
        pilots = {'p.1': PilotRecord(pid='p.1', pool='cpu', size_key='s',
                                     rhapsody_backend='concurrent')}
        store.save('sid', {}, pilots, {})
        payload = store.load()
        assert set(payload['pilots'].keys()) == {'p.1'}

    def test_save_empty_pools_and_tasks(self, tmp_path: Path):
        store = PoolStore(tmp_path / 'state.json')
        store.save('sid', {'name': 'cpu'}, {}, {})
        payload = store.load()
        assert payload['pilots'] == {}
        assert payload['tasks']  == {}
