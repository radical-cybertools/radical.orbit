"""Unit tests for the BatchSystem abstraction and its backends."""

import os
import subprocess
from unittest.mock import patch, MagicMock

import pytest

from radical.orbit import batch_system as bs
from radical.orbit.batch_system import (
    BatchSystem, NullBatchSystem, detect_batch_system, reset_detection,
    STATE_PENDING, STATE_RUNNING, STATE_DONE, STATE_FAILED,
    STATE_CANCELLED, STATE_HELD, STATE_UNKNOWN, TERMINAL_STATES)
from radical.orbit.batch_system_slurm import SlurmBatchSystem, _parse_slurm_time
from radical.orbit.batch_system_pbs import (
    PBSProBatchSystem, _parse_pbs_walltime, _parse_qstat_f, _parse_exec_host)


@pytest.fixture(autouse=True)
def _reset_detection_cache():
    """Each test starts with a fresh detection cache."""
    reset_detection()
    yield
    reset_detection()


# ---------------------------------------------------------------------------
# Detection / factory
# ---------------------------------------------------------------------------

class TestDetect:

    def test_no_scheduler_returns_null(self):
        with patch('shutil.which', return_value=None):
            b = detect_batch_system()
        assert isinstance(b, NullBatchSystem)
        assert b.name == 'none'
        assert b.psij_executor == 'local'

    def test_detects_slurm_when_squeue_present(self):
        def _which(cmd):
            return '/usr/bin/squeue' if cmd == 'squeue' else None
        with patch('shutil.which', side_effect=_which):
            b = detect_batch_system()
        assert isinstance(b, SlurmBatchSystem)
        assert b.name == 'slurm'
        assert b.psij_executor == 'slurm'

    def test_detects_pbs_when_qstat_present_and_no_squeue(self):
        def _which(cmd):
            return '/usr/bin/qstat' if cmd == 'qstat' else None
        # Pin the Aurora marker absent so the generic PBS backend wins;
        # without this, AuroraPBSBatchSystem (registered first) would
        # match on hosts where /opt/aurora actually exists.
        with patch('shutil.which', side_effect=_which), \
             patch('radical.orbit.batch_system_pbs.os.path.isdir',
                   return_value=False):
            b = detect_batch_system()
        assert isinstance(b, PBSProBatchSystem)
        assert b.name == 'pbs'
        assert b.psij_executor == 'pbs'

    def test_detects_aurora_pbs_when_marker_present(self):
        from radical.orbit.batch_system_pbs import AuroraPBSBatchSystem
        def _which(cmd):
            return '/usr/bin/qstat' if cmd == 'qstat' else None
        with patch('shutil.which', side_effect=_which), \
             patch('radical.orbit.batch_system_pbs.os.path.isdir',
                   return_value=True):
            b = detect_batch_system()
        assert isinstance(b, AuroraPBSBatchSystem)
        assert b.name == 'pbs-aurora'
        assert b.psij_executor == 'pbs'
        assert b.default_custom_attributes() == {
            'pbs.l': 'filesystems=home:flare'}

    def test_slurm_wins_when_both_present(self):
        with patch('shutil.which', return_value='/usr/bin/x'):
            b = detect_batch_system()
        # Registration order in batch_system.detect_batch_system loads slurm
        # first, so slurm wins when both are detectable.
        assert isinstance(b, SlurmBatchSystem)

    def test_detection_is_cached(self):
        with patch('shutil.which', return_value=None):
            b1 = detect_batch_system()
        with patch('shutil.which', return_value='/usr/bin/squeue'):
            b2 = detect_batch_system()
        # Cached → same instance even after which() changes
        assert b1 is b2

    def test_force_re_detects(self):
        with patch('shutil.which', return_value=None):
            b1 = detect_batch_system()
        with patch('shutil.which', return_value='/usr/bin/squeue'):
            b2 = detect_batch_system(force=True)
        assert b1 is not b2
        assert isinstance(b2, SlurmBatchSystem)


# ---------------------------------------------------------------------------
# NullBatchSystem
# ---------------------------------------------------------------------------

class TestNullBackend:

    def test_in_allocation_false(self):
        assert NullBatchSystem().in_allocation() is False

    def test_job_id_none(self):
        assert NullBatchSystem().job_id() is None

    def test_job_state_unknown(self):
        assert NullBatchSystem().job_state('1') == STATE_UNKNOWN

    def test_job_nodes_empty(self):
        assert NullBatchSystem().job_nodes('1') == []

    def test_cancel_raises(self):
        with pytest.raises(RuntimeError, match='no batch system'):
            NullBatchSystem().cancel('1')

    def test_job_allocation_none(self):
        assert NullBatchSystem().job_allocation() is None


# ---------------------------------------------------------------------------
# SlurmBatchSystem
# ---------------------------------------------------------------------------

class TestSlurmBackend:

    def test_in_allocation_from_env(self):
        with patch.dict(os.environ, {'SLURM_JOB_ID': '42'}, clear=True):
            assert SlurmBatchSystem().in_allocation() is True
        with patch.dict(os.environ, {}, clear=True):
            assert SlurmBatchSystem().in_allocation() is False

    def test_job_id_from_env(self):
        with patch.dict(os.environ, {'SLURM_JOB_ID': '99'}, clear=True):
            assert SlurmBatchSystem().job_id() == '99'

    def test_job_state_running(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='RUNNING\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_RUNNING

    def test_job_state_pending_maps(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='PENDING\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_PENDING

    def test_job_state_completed_maps_to_done(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='COMPLETED\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_DONE

    def test_job_state_node_fail_maps_to_failed(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='NODE_FAIL\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_FAILED

    def test_job_state_cancelled_maps(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='CANCELLED\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_CANCELLED

    def test_job_state_suspended_maps_to_held(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='SUSPENDED\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_HELD

    def test_job_state_unknown_when_squeue_fails(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stdout='', stderr='x')):
            assert SlurmBatchSystem().job_state('1') == STATE_UNKNOWN

    def test_job_state_unknown_on_oserror(self):
        with patch('subprocess.run', side_effect=OSError):
            assert SlurmBatchSystem().job_state('1') == STATE_UNKNOWN

    def test_job_state_unknown_on_unrecognised_string(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='WIBBLE\n')):
            assert SlurmBatchSystem().job_state('1') == STATE_UNKNOWN

    def test_job_nodes_via_squeue_and_scontrol(self):
        squeue = MagicMock(returncode=0, stdout='node[01-02]\n')
        scontrol = MagicMock(returncode=0, stdout='node01\nnode02\n')
        with patch('subprocess.run', side_effect=[squeue, scontrol]):
            nodes = SlurmBatchSystem().job_nodes('1')
        assert nodes == ['node01', 'node02']

    def test_job_nodes_empty_when_squeue_empty(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stdout='\n')):
            assert SlurmBatchSystem().job_nodes('1') == []

    def test_cancel_calls_scancel(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stderr='')) as m:
            SlurmBatchSystem().cancel('5')
        m.assert_called_once()
        assert m.call_args[0][0][:2] == ['scancel', '5']

    def test_cancel_raises_on_failure(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stderr='nope')):
            with pytest.raises(RuntimeError, match='scancel failed'):
                SlurmBatchSystem().cancel('5')


class TestParseSlurmTime:

    def test_hh_mm_ss(self):
        assert _parse_slurm_time('01:00:00') == 3600

    def test_mm_ss(self):
        assert _parse_slurm_time('05:30') == 5 * 60 + 30

    def test_days(self):
        assert _parse_slurm_time('2-00:00:00') == 2 * 86400

    def test_unlimited(self):
        assert _parse_slurm_time('UNLIMITED') is None

    def test_empty(self):
        assert _parse_slurm_time('') is None

    def test_garbage(self):
        with pytest.raises(RuntimeError):
            _parse_slurm_time('garbage')


# ---------------------------------------------------------------------------
# PBSProBatchSystem
# ---------------------------------------------------------------------------

class TestPBSBackend:

    def test_in_allocation_from_env(self):
        with patch.dict(os.environ, {'PBS_JOBID': '42.aurora'}, clear=True):
            assert PBSProBatchSystem().in_allocation() is True
        with patch.dict(os.environ, {}, clear=True):
            assert PBSProBatchSystem().in_allocation() is False

    def test_job_id_from_env(self):
        with patch.dict(os.environ, {'PBS_JOBID': '99'}, clear=True):
            assert PBSProBatchSystem().job_id() == '99'

    def _qstat_f_output(self, state='R'):
        return ("Job Id: 42.aurora\n"
                f"    job_state = {state}\n"
                "    queue = compute\n"
                "    Resource_List.walltime = 02:00:00\n"
                "    exec_host = nid001/0*64+nid002/0*64\n")

    def test_job_state_running(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout=self._qstat_f_output('R'))):
            assert PBSProBatchSystem().job_state('42') == STATE_RUNNING

    def test_job_state_pending(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout=self._qstat_f_output('Q'))):
            assert PBSProBatchSystem().job_state('42') == STATE_PENDING

    def test_job_state_finished(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout=self._qstat_f_output('F'))):
            assert PBSProBatchSystem().job_state('42') == STATE_DONE

    def test_job_state_held(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout=self._qstat_f_output('H'))):
            assert PBSProBatchSystem().job_state('42') == STATE_HELD

    def test_job_state_falls_back_to_x_for_finished(self):
        first = MagicMock(returncode=1, stdout='', stderr='unknown job')
        second = MagicMock(returncode=0, stdout=self._qstat_f_output('F'))
        with patch('subprocess.run', side_effect=[first, second]):
            assert PBSProBatchSystem().job_state('42') == STATE_DONE

    def test_job_state_unknown_when_both_fail(self):
        fail = MagicMock(returncode=1, stdout='', stderr='nope')
        with patch('subprocess.run', side_effect=[fail, fail]):
            assert PBSProBatchSystem().job_state('42') == STATE_UNKNOWN

    def test_job_nodes_parses_exec_host(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout=self._qstat_f_output('R'))):
            assert PBSProBatchSystem().job_nodes('42') == ['nid001', 'nid002']

    def test_cancel_calls_qdel(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=0, stderr='')) as m:
            PBSProBatchSystem().cancel('42')
        m.assert_called_once()
        assert m.call_args[0][0] == ['qdel', '42']

    def test_cancel_raises_on_failure(self):
        with patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stderr='nope')):
            with pytest.raises(RuntimeError, match='qdel failed'):
                PBSProBatchSystem().cancel('42')

    def test_job_allocation_none_outside_job(self):
        with patch.dict(os.environ, {}, clear=True):
            assert PBSProBatchSystem().job_allocation() is None

    def test_job_allocation_uses_pbs_nodefile(self, tmp_path):
        nf = tmp_path / 'nodefile'
        nf.write_text('nid001\nnid001\nnid002\n')
        env = {'PBS_JOBID': '42.aurora', 'PBS_NODEFILE': str(nf),
               'PBS_O_QUEUE': 'compute', 'PBS_ACCOUNT': 'proj1',
               'PBS_JOBNAME': 'demo'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=MagicMock(returncode=0,
                                          stdout='Job Id: 42.aurora\n'
                                                 '    job_state = R\n'
                                                 '    Resource_List.walltime = 01:00:00\n'
                                                 '    Resource_List.select = 2:ncpus=64:ngpus=4\n')):
            alloc = PBSProBatchSystem().job_allocation()
        assert alloc['job_id'] == '42.aurora'
        assert alloc['n_nodes'] == 2
        assert alloc['nodelist'] == 'nid001,nid002'
        assert alloc['runtime'] == 3600
        assert alloc['cpus_per_node'] == 64
        assert alloc['gpus_per_node'] == 4
        assert alloc['account'] == 'proj1'
        assert alloc['job_name'] == 'demo'

    def test_job_allocation_raises_when_unknown(self):
        env = {'PBS_JOBID': '42.aurora'}
        with patch.dict(os.environ, env, clear=True), \
             patch('subprocess.run',
                   return_value=MagicMock(returncode=1, stdout='',
                                          stderr='gone')):
            with pytest.raises(RuntimeError, match='node count'):
                PBSProBatchSystem().job_allocation()


class TestParsePBSWalltime:

    def test_hh_mm_ss(self):
        assert _parse_pbs_walltime('01:00:00') == 3600

    def test_mm_ss(self):
        assert _parse_pbs_walltime('30:15') == 30 * 60 + 15

    def test_seconds(self):
        assert _parse_pbs_walltime('45') == 45

    def test_empty(self):
        assert _parse_pbs_walltime('') is None
        assert _parse_pbs_walltime(None) is None

    def test_garbage(self):
        with pytest.raises(RuntimeError):
            _parse_pbs_walltime('garbage')


class TestParseQstatF:

    def test_simple(self):
        out = ("Job Id: 1.x\n"
               "    job_state = R\n"
               "    queue = compute\n")
        d = _parse_qstat_f(out)
        assert d['job_state'] == 'R'
        assert d['queue'] == 'compute'

    def test_continuation(self):
        out = ("Job Id: 1.x\n"
               "    Resource_List.select = 2:ncpus=64:\n"
               "        ngpus=4\n")
        d = _parse_qstat_f(out)
        assert d['Resource_List.select'] == '2:ncpus=64:ngpus=4'


class TestParseExecHost:

    def test_dedup(self):
        s = 'nid001/0*64+nid002/0*64+nid001/64*64'
        assert _parse_exec_host(s) == ['nid001', 'nid002']

    def test_strips_domain(self):
        assert _parse_exec_host('host01.fqdn/0*64') == ['host01']

    def test_empty(self):
        assert _parse_exec_host('') == []


# ---------------------------------------------------------------------------
# Top-level vocabulary
# ---------------------------------------------------------------------------

def test_terminal_states_constant():
    assert TERMINAL_STATES == frozenset({STATE_DONE, STATE_FAILED,
                                         STATE_CANCELLED})


def test_batch_system_is_abc():
    with pytest.raises(TypeError):
        BatchSystem()  # type: ignore[abstract]
