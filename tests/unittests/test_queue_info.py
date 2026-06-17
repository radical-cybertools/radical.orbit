#!/usr/bin/env python3

# pylint: disable=protected-access
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument

import json
import os
import pytest

from unittest.mock import patch, Mock, MagicMock

from radical.orbit.queue_info import (QueueInfo, QueueInfoSlurm,
                                     _unwrap, _parse_gpus,
                                     _UNAVAIL_STATES)
from radical.orbit.plugin_queue_info import _parse_slurm_time, PluginQueueInfo

FIXTURES = os.path.join(os.path.dirname(__file__), '..', 'fixtures', 'slurm')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return f.read()


def _load_json(name):
    return json.loads(_load(name))


# ---- helper tests -----------------------------------------------------------

class TestUnwrap:

    def test_plain_value(self):
        assert _unwrap(42) == 42

    def test_set_number(self):
        assert _unwrap({'set': True, 'infinite': False, 'number': 1440}) == 1440

    def test_number_without_infinite_key(self):
        assert _unwrap({'set': True, 'number': 42}) == 42

    def test_infinite(self):
        assert _unwrap({'set': True, 'infinite': True, 'number': 0}) is None

    def test_infinite_without_number_key(self):
        assert _unwrap({'set': True, 'infinite': True}) is None

    def test_unset(self):
        assert _unwrap({'set': False, 'infinite': False, 'number': 0}) is None

    def test_unset_minimal(self):
        assert _unwrap({'set': False}) is None

    def test_string(self):
        assert _unwrap('hello') == 'hello'

    def test_none(self):
        assert _unwrap(None) is None


class TestParseGpus:

    def test_simple(self):
        assert _parse_gpus('gpu:8(S:0-7)') == 8

    def test_typed(self):
        assert _parse_gpus('gpu:mi250:4(S:0-3)') == 4

    def test_typed_no_socket(self):
        assert _parse_gpus('gpu:a100:2') == 2

    def test_no_socket(self):
        assert _parse_gpus('gpu:8') == 8

    def test_empty(self):
        assert _parse_gpus('') == 0

    def test_null(self):
        assert _parse_gpus('(null)') == 0

    def test_none(self):
        assert _parse_gpus(None) == 0

    def test_multi_gres(self):
        assert _parse_gpus('gpu:4(S:0-3),mic:2') == 4

    def test_invalid(self):
        assert _parse_gpus('invalid') == 0
        assert _parse_gpus('gpu:') == 0


class TestUnavailStates:

    def test_all_expected(self):
        expected = {'DOWN', 'DRAIN', 'DRAINING', 'FAIL', 'FAILING', 'MAINT',
                    'FUTURE', 'POWER_DOWN', 'POWERED_DOWN',
                    'NOT_RESPONDING', 'REBOOT_ISSUED'}
        assert _UNAVAIL_STATES == expected


# ---- QueueInfo ABC tests ----------------------------------------------------

class TestQueueInfoBase:

    @staticmethod
    def _make_concrete():
        """Return a concrete QueueInfo subclass for testing."""

        class ConcreteQueueInfo(QueueInfo):
            def __init__(self):
                super().__init__()
                self.collect_count = 0

            def _collect_info(self):
                self.collect_count += 1
                return {'queues': {'test': {'name': 'test'}}}

            def _collect_jobs(self, queue, user):
                self.collect_count += 1
                return {'jobs': [{'id': 1, 'name': 'test'}]}

            def _collect_all_user_jobs(self, user):
                self.collect_count += 1
                return {'jobs': [{'id': 1, 'name': 'test'}]}

            def _collect_allocations(self, user):
                self.collect_count += 1
                return {'allocations': [{'account': 'test'}]}

        return ConcreteQueueInfo

    def test_initialization(self):
        cls = self._make_concrete()
        qi  = cls()
        assert qi._cache == {}
        assert qi._cache_lock is not None

    def test_caching_via_get_info(self):
        cls = self._make_concrete()
        qi  = cls()

        result1 = qi.get_info()
        assert qi.collect_count == 1
        assert result1 == {'queues': {'test': {'name': 'test'}}}

        result2 = qi.get_info()
        assert qi.collect_count == 1    # cache hit
        assert result2 == result1

        result3 = qi.get_info(force=True)
        assert qi.collect_count == 2    # forced refresh
        assert result3 == result1

    def test_list_jobs_cache_keyed_by_queue(self):
        cls = self._make_concrete()
        qi  = cls()

        qi.list_jobs('queue_a')
        assert qi.collect_count == 1

        qi.list_jobs('queue_a')
        assert qi.collect_count == 1    # same queue → cache hit

        qi.list_jobs('queue_b')
        assert qi.collect_count == 2    # different queue → miss

    def test_list_allocations_cache(self):
        cls = self._make_concrete()
        qi  = cls()

        qi.list_allocations()
        assert qi.collect_count == 1

        qi.list_allocations()
        assert qi.collect_count == 1    # cache hit

        qi.list_allocations(force=True)
        assert qi.collect_count == 2    # forced refresh


# ---- QueueInfoSlurm init tests ---------------------------------------------

class TestQueueInfoSlurmInit:

    def test_default_env(self):
        qi = QueueInfoSlurm()
        assert isinstance(qi._env, dict)
        assert qi._env == dict(os.environ)

    def test_slurm_conf_injected(self):
        qi = QueueInfoSlurm(slurm_conf='/custom/path/slurm.conf')
        assert qi._env['SLURM_CONF'] == '/custom/path/slurm.conf'
        assert len(qi._env) > 1

    @patch('subprocess.run')
    def test_run_returns_stdout(self, mock_run):
        mock_run.return_value = Mock(stdout='test output', returncode=0)
        qi     = QueueInfoSlurm()
        result = qi._run(['echo', 'test'])
        assert result == 'test output'
        mock_run.assert_called_once()


# ---- _collect_info tests ----------------------------------------------------

class TestCollectInfo:

    def _make_backend(self):
        backend      = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        return backend

    def _mock_run(self, sinfo_stdout, scontrol_stdout):
        """Return a side_effect function that dispatches by command."""

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            if cmd[0] == 'sinfo':
                m.stdout = sinfo_stdout
            elif cmd[0] == 'scontrol':
                m.stdout = scontrol_stdout
            else:
                raise ValueError(f'unexpected command: {cmd}')
            return m

        return side_effect

    def test_collect_info(self):
        sinfo_raw    = _load('sinfo_01.json')
        scontrol_raw = _load('scontrol_nodes_01.json')
        expected     = _load_json('sinfo_01.expected.json')

        backend = self._make_backend()

        with patch('subprocess.run',
                   side_effect=self._mock_run(sinfo_raw, scontrol_raw)):
            result = backend._collect_info()

        assert result == expected

    def test_collect_info_no_scontrol(self):
        """If scontrol fails, mem_per_node_mb should be 0."""

        sinfo_raw = _load('sinfo_01.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            if cmd[0] == 'sinfo':
                m.stdout = sinfo_raw
            else:
                raise OSError('scontrol not available')
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_info()

        for pname, pinfo in result['queues'].items():
            assert pinfo['mem_per_node_mb'] == 0

    @patch('subprocess.run')
    def test_collect_info_empty_sinfo(self, mock_run):
        """Empty sinfo output yields empty queues dict."""

        sinfo_output    = {'sinfo': []}
        scontrol_output = {'nodes': []}

        def side_effect(cmd, **kw):
            m = Mock()
            if cmd[0] == 'sinfo':
                m.stdout = json.dumps(sinfo_output)
            elif cmd[0] == 'scontrol':
                m.stdout = json.dumps(scontrol_output)
            else:
                m.stdout = '{}'
            return m

        mock_run.side_effect = side_effect

        backend = self._make_backend()
        result  = backend._collect_info()

        assert 'queues' in result
        assert isinstance(result['queues'], dict)
        assert result['queues'] == {}


# ---- _collect_jobs tests ----------------------------------------------------

class TestCollectJobs:

    def _make_backend(self):
        backend      = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        return backend

    def test_collect_jobs(self):
        squeue_raw = _load('squeue_01.json')
        expected   = _load_json('squeue_01.expected.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = squeue_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_jobs('compute', None)

        assert len(result['jobs']) == len(expected['jobs'])

        for got, exp in zip(result['jobs'], expected['jobs']):
            for key in exp:
                if key == 'time_used' and exp[key] == '__DYNAMIC__':
                    assert got[key] > 0, \
                        f'job {got["job_id"]}: time_used should be > 0'
                else:
                    assert got[key] == exp[key], \
                        f'job {got["job_id"]}: {key}: {got[key]} != {exp[key]}'

    def test_collect_jobs_with_user(self):
        """Verify user filter is passed as --user flag."""

        calls = []

        def side_effect(cmd, **kw):
            calls.append(cmd)
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = '{"jobs": []}'
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_jobs('compute', 'alice')

        assert result == {'jobs': []}
        assert '--user' in calls[0]
        assert 'alice'  in calls[0]

    @patch('subprocess.run')
    def test_collect_jobs_minimal(self, mock_run):
        """Minimal squeue output parses without error."""

        squeue_output = {
            'jobs': [{
                'job_id'   : 12345,
                'name'     : 'test_job',
                'user_name': 'testuser',
                'partition': 'test_partition',
                'job_state': ['RUNNING']
            }]
        }

        mock_run.return_value = Mock(
            stdout=json.dumps(squeue_output), returncode=0)

        backend = self._make_backend()
        result  = backend._collect_jobs('test_partition', None)

        assert 'jobs' in result
        assert isinstance(result['jobs'], list)
        assert len(result['jobs']) == 1
        assert result['jobs'][0]['job_id'] == '12345'


# ---- _collect_allocations tests ---------------------------------------------

class TestCollectAllocations:

    def _make_backend(self):
        backend      = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env = dict(os.environ)
        return backend

    def test_collect_allocations_json(self):
        sacctmgr_raw = _load('sacctmgr_01.json')
        expected     = _load_json('sacctmgr_01.expected.json')

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = sacctmgr_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_json(None)

        assert result == expected

    def test_collect_allocations_parsable(self):
        parsable_raw = _load('sacctmgr_01_parsable.txt')
        expected     = _load_json('sacctmgr_01_parsable.expected.json')

        backend = self._make_backend()

        def side_effect(cmd, **kw):
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = parsable_raw
            return m

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_parsable(None)

        assert result == expected

    def test_collect_allocations_fallback(self):
        """If --json fails, _collect_allocations should fall back to parsable."""

        parsable_raw = _load('sacctmgr_01_parsable.txt')
        expected     = _load_json('sacctmgr_01_parsable.expected.json')

        call_count = [0]

        def side_effect(cmd, **kw):
            call_count[0] += 1
            m = MagicMock()
            m.check_returncode = MagicMock()

            if '--json' in cmd:
                raise RuntimeError('json not supported')

            m.stdout = parsable_raw
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations(None)

        assert result == expected
        assert call_count[0] == 2   # one failed --json, one parsable

    def test_collect_allocations_user_filter(self):
        """Verify user filter is passed as Users= argument."""

        calls = []

        def side_effect(cmd, **kw):
            calls.append(cmd)
            m = MagicMock()
            m.check_returncode = MagicMock()
            m.stdout = '{"associations": []}'
            return m

        backend = self._make_backend()

        with patch('subprocess.run', side_effect=side_effect):
            result = backend._collect_allocations_json('alice')

        assert result == {'allocations': []}
        assert any('Users=alice' in c for c in calls[0])

    @patch('subprocess.run')
    def test_collect_allocations_json_minimal(self, mock_run):
        """Minimal sacctmgr output parses without error."""

        sacctmgr_output = {
            'associations': [{
                'account'  : 'test_account',
                'user'     : 'testuser',
                'partition': 'test_partition'
            }]
        }

        mock_run.return_value = Mock(
            stdout=json.dumps(sacctmgr_output), returncode=0)

        backend = self._make_backend()
        result  = backend._collect_allocations_json(None)

        assert 'allocations' in result
        assert isinstance(result['allocations'], list)
        assert len(result['allocations']) == 1


# ---- caching tests ----------------------------------------------------------

class TestCaching:

    def _make_backend(self):
        import threading

        backend             = QueueInfoSlurm.__new__(QueueInfoSlurm)
        backend._env        = dict(os.environ)
        backend._cache      = {}
        backend._cache_time = {}
        backend._cache_lock = threading.Lock()
        return backend

    def test_cache_returns_cached_value(self):
        """Second call with force=False should not call collector."""

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()

        result1 = backend._get_cached('info', False, collector)
        result2 = backend._get_cached('info', False, collector)

        assert result1 == result2
        assert call_count[0] == 1

    def test_cache_bypassed_with_force(self):
        """force=True should always call collector."""

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()

        backend._get_cached('info', False, collector)
        backend._get_cached('info', True,  collector)

        assert call_count[0] == 2

    def test_cache_ttl_expiry(self):
        """Expired cache entries should be refreshed."""

        import time as _time

        call_count = [0]

        def collector():
            call_count[0] += 1
            return {'queues': {}}

        backend = self._make_backend()
        backend._cache_ttl = 0   # immediate expiry

        backend._get_cached('info', False, collector)
        _time.sleep(0.01)   # ensure time passes
        backend._get_cached('info', False, collector)

        assert call_count[0] == 2


# ---- parsable fixture format tests ------------------------------------------

class TestParsableParser:

    def test_short_lines_skipped(self):
        """Lines with fewer than 18 pipe-delimited fields are skipped."""

        result = QueueInfoSlurm._parse_assocs_parsable('short|line\n')
        assert result == []

    def test_empty_input(self):
        result = QueueInfoSlurm._parse_assocs_parsable('')
        assert result == []



# ---- _parse_slurm_time tests ------------------------------------------------

class TestParseSlurmTime:

    def test_hms(self):
        assert _parse_slurm_time('01:00:00') == 3600

    def test_hms_no_leading_zero(self):
        assert _parse_slurm_time('2:30:00') == 9000

    def test_ms(self):
        assert _parse_slurm_time('30:00') == 1800

    def test_days(self):
        assert _parse_slurm_time('1-00:00:00') == 86400

    def test_days_plus_time(self):
        assert _parse_slurm_time('2-06:00:00') == 2 * 86400 + 6 * 3600

    def test_unlimited(self):
        assert _parse_slurm_time('UNLIMITED') is None

    def test_unlimited_lowercase(self):
        assert _parse_slurm_time('unlimited') is None

    def test_infinite(self):
        assert _parse_slurm_time('INFINITE') is None

    def test_not_set(self):
        assert _parse_slurm_time('NOT_SET') is None

    def test_empty(self):
        assert _parse_slurm_time('') is None

    def test_whitespace_stripped(self):
        assert _parse_slurm_time('  01:00:00  ') == 3600

    def test_invalid_raises(self):
        with pytest.raises(RuntimeError):
            _parse_slurm_time('garbage')

    def test_invalid_day_raises(self):
        with pytest.raises(RuntimeError):
            _parse_slurm_time('X-01:00:00')


# ---- get_job_allocation tests -----------------------------------------------

class TestGetJobAllocation:
    """Tests for PluginQueueInfo.get_job_allocation().

    We test the plain method directly — no FastAPI app needed.
    """

    def _make_plugin(self):
        """Construct a PluginQueueInfo without starting prefetch.

        ``get_job_allocation()`` delegates to the active BatchSystem; we
        force-detect SLURM so the underlying squeue/env-var path is
        exercised regardless of the host running the tests.
        """
        from radical.orbit import batch_system as _bs
        from radical.orbit.batch_system_slurm import SlurmBatchSystem
        _bs._DETECTED = SlurmBatchSystem()
        plugin = PluginQueueInfo.__new__(PluginQueueInfo)
        plugin._backend = Mock()
        return plugin

    def teardown_method(self, method):
        """Clear the cached batch system between tests."""
        from radical.orbit import batch_system as _bs
        _bs._DETECTED = None

    def test_no_job_id_returns_none(self):
        plugin = self._make_plugin()
        env    = {k: v for k, v in os.environ.items()
                  if k not in ('SLURM_JOB_ID', 'SLURM_NNODES', 'SLURM_JOB_NUM_NODES')}
        with patch.dict(os.environ, env, clear=True):
            assert plugin.get_job_allocation() is None

    def test_in_job_returns_dict(self):
        plugin = self._make_plugin()
        env    = {
            'SLURM_JOB_ID'       : '12345',
            'SLURM_NNODES'       : '4',
            'SLURM_JOB_PARTITION': 'gpu',
            'SLURM_JOB_ACCOUNT'  : 'myproject',
            'SLURM_JOB_NAME'     : 'myjob',
            'SLURM_JOB_NODELIST' : 'node[01-04]',
            'SLURM_CPUS_ON_NODE' : '32',
        }
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='01:00:00\n',
                                     stderr='')):
            result = plugin.get_job_allocation()

        assert result['job_id']        == '12345'
        assert result['partition']     == 'gpu'
        assert result['n_nodes']       == 4
        assert result['nodelist']      == 'node[01-04]'
        assert result['cpus_per_node'] == 32
        assert result['account']       == 'myproject'
        assert result['job_name']      == 'myjob'
        assert result['runtime']       == 3600

    def test_optional_env_vars_absent(self):
        """Fields are None when optional SLURM env vars are not set."""
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '4'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='01:00:00\n',
                                     stderr='')):
            result = plugin.get_job_allocation()

        assert result['job_id']        == '12345'
        assert result['n_nodes']       == 4
        assert result['partition']     is None
        assert result['account']       is None
        assert result['cpus_per_node'] is None
        assert result['gpus_per_node'] is None

    def test_gpus_per_node_plain(self):
        """SLURM_GPUS_ON_NODE plain integer."""
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '2',
                  'SLURM_GPUS_ON_NODE': '4'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='01:00:00\n',
                                     stderr='')):
            result = plugin.get_job_allocation()
        assert result['gpus_per_node'] == 4

    def test_gpus_per_node_typed(self):
        """SLURM_GPUS_PER_NODE with type prefix (e.g. a100:2)."""
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '2',
                  'SLURM_GPUS_PER_NODE': 'a100:2'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='01:00:00\n',
                                     stderr='')):
            result = plugin.get_job_allocation()
        assert result['gpus_per_node'] == 2

    def test_in_job_fallback_env_var(self):
        """SLURM_JOB_NUM_NODES used when SLURM_NNODES absent."""
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_JOB_NUM_NODES': '8'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='02:00:00\n',
                                     stderr='')):
            result = plugin.get_job_allocation()

        assert result['n_nodes'] == 8
        assert result['runtime'] == 7200

    def test_unlimited_runtime(self):
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '2'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=0, stdout='UNLIMITED\n',
                                     stderr='')):
            result = plugin.get_job_allocation()

        assert result['n_nodes'] == 2
        assert result['runtime'] is None

    def test_missing_nnodes_raises(self):
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345'}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(RuntimeError, match='SLURM_NNODES'):
                plugin.get_job_allocation()

    def test_squeue_failure_raises(self):
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '4'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=Mock(returncode=1, stdout='',
                                     stderr='invalid job id')):
            with pytest.raises(RuntimeError, match='squeue failed'):
                plugin.get_job_allocation()

    def test_squeue_oserror_raises(self):
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '4'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run', side_effect=OSError('squeue not found')):
            with pytest.raises(RuntimeError, match='Cannot query runtime'):
                plugin.get_job_allocation()

    def test_squeue_timeout_raises(self):
        import subprocess as _sp
        plugin = self._make_plugin()
        env    = {'SLURM_JOB_ID': '12345', 'SLURM_NNODES': '4'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   side_effect=_sp.TimeoutExpired(['squeue'], timeout=10)):
            with pytest.raises(RuntimeError, match='Cannot query runtime'):
                plugin.get_job_allocation()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
