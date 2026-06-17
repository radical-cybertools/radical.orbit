"""SLURM implementation of BatchSystem."""

import os
import shutil
import subprocess

from .batch_system import (BatchSystem, register_backend,
                           STATE_PENDING, STATE_RUNNING, STATE_DONE,
                           STATE_FAILED, STATE_CANCELLED, STATE_HELD,
                           STATE_UNKNOWN)


# SLURM job state strings → normalized vocabulary.
_STATE_MAP = {
    'PENDING'    : STATE_PENDING,
    'CONFIGURING': STATE_PENDING,
    'RUNNING'    : STATE_RUNNING,
    'COMPLETING' : STATE_RUNNING,
    'COMPLETED'  : STATE_DONE,
    'FAILED'     : STATE_FAILED,
    'TIMEOUT'    : STATE_FAILED,
    'NODE_FAIL'  : STATE_FAILED,
    'PREEMPTED'  : STATE_FAILED,
    'BOOT_FAIL'  : STATE_FAILED,
    'OUT_OF_MEMORY': STATE_FAILED,
    'CANCELLED'  : STATE_CANCELLED,
    'DEADLINE'   : STATE_CANCELLED,
    'SUSPENDED'  : STATE_HELD,
    'STOPPED'    : STATE_HELD,
    'REVOKED'    : STATE_HELD,
}


def _parse_slurm_time(s: str) -> 'int | None':
    """Parse a SLURM time string to seconds. None for UNLIMITED."""
    if s is None:
        return None
    s = s.strip()
    if not s or s.upper() in ('UNLIMITED', 'INFINITE', 'NOT_SET', 'N/A'):
        return None

    days = 0
    if '-' in s:
        d, s = s.split('-', 1)
        try:
            days = int(d)
        except ValueError as e:
            raise RuntimeError(f"Cannot parse SLURM time: {s!r}") from e

    parts = s.split(':')
    try:
        if   len(parts) == 3: h, m, sec = (int(p) for p in parts)
        elif len(parts) == 2: h, m, sec = 0, int(parts[0]), int(parts[1])
        else:                 raise ValueError
    except ValueError as e:
        raise RuntimeError(f"Cannot parse SLURM time: {s!r}") from e

    return days * 86400 + h * 3600 + m * 60 + sec


class SlurmBatchSystem(BatchSystem):
    """SLURM scheduler interface."""

    name          = 'slurm'
    psij_executor = 'slurm'

    @classmethod
    def detect(cls) -> bool:
        return shutil.which('squeue') is not None

    def in_allocation(self) -> bool:
        return bool(os.environ.get('SLURM_JOB_ID'))

    def job_id(self) -> 'str | None':
        return os.environ.get('SLURM_JOB_ID')

    def job_state(self, native_id) -> str:
        try:
            r = subprocess.run(
                ['squeue', '--job', str(native_id),
                 '--noheader', '--format=%T'],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return STATE_UNKNOWN
        if r.returncode != 0:
            return STATE_UNKNOWN
        for line in r.stdout.splitlines():
            line = line.strip()
            if line:
                return _STATE_MAP.get(line, STATE_UNKNOWN)
        return STATE_UNKNOWN

    def job_nodes(self, native_id) -> list:
        try:
            r = subprocess.run(
                ['squeue', '--job', str(native_id),
                 '--noheader', '--format=%N'],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return []
        nodelist = r.stdout.strip()
        if r.returncode != 0 or not nodelist:
            return []
        try:
            r2 = subprocess.run(
                ['scontrol', 'show', 'hostnames', nodelist],
                capture_output=True, text=True, timeout=10)
            if r2.returncode == 0 and r2.stdout.strip():
                return [h.strip() for h in r2.stdout.splitlines() if h.strip()]
        except (OSError, subprocess.TimeoutExpired):
            pass
        return []

    def nodelist(self) -> list:
        # Expand SLURM_JOB_NODELIST (a range expression like "nid[001-016]")
        # via ``scontrol show hostnames`` -- same expansion used by
        # ``job_nodes(native_id)`` above, but for the *current* allocation.
        raw = os.environ.get('SLURM_JOB_NODELIST')
        if not raw:
            return []
        try:
            r = subprocess.run(
                ['scontrol', 'show', 'hostnames', raw],
                capture_output=True, text=True, timeout=10)
            if r.returncode == 0 and r.stdout.strip():
                return [h.strip() for h in r.stdout.splitlines() if h.strip()]
        except (OSError, subprocess.TimeoutExpired):
            pass
        return []

    def cancel(self, native_id) -> None:
        r = subprocess.run(['scancel', str(native_id)],
                           capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            raise RuntimeError(f"scancel failed: {r.stderr.strip()}")

    def job_allocation(self) -> 'dict | None':
        job_id = os.environ.get('SLURM_JOB_ID')
        if not job_id:
            return None

        n_nodes = (os.environ.get('SLURM_NNODES') or
                   os.environ.get('SLURM_JOB_NUM_NODES'))
        if not n_nodes:
            raise RuntimeError(
                f"SLURM_JOB_ID={job_id!r} is set but SLURM_NNODES is unavailable")

        # walltime: query squeue for the per-job time limit
        try:
            r = subprocess.run(
                ['squeue', '--job', job_id, '--noheader', '--format=%l'],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError(
                f"Cannot query runtime for job {job_id}: {exc}") from exc
        if r.returncode != 0:
            raise RuntimeError(
                f"squeue failed for job {job_id}: {r.stderr.strip()}")
        runtime = _parse_slurm_time(r.stdout.strip())

        def _intenv(key):
            v = os.environ.get(key)
            try:
                return int(v) if v else None
            except ValueError:
                return None

        gpus_raw = (os.environ.get('SLURM_GPUS_ON_NODE') or
                    os.environ.get('SLURM_GPUS_PER_NODE'))
        gpus_per_node = None
        if gpus_raw:
            try:
                gpus_per_node = int(gpus_raw)
            except ValueError:
                try:
                    gpus_per_node = int(gpus_raw.split(':')[-1]) or None
                except ValueError:
                    gpus_per_node = None

        return {
            'job_id'       : job_id,
            'partition'    : os.environ.get('SLURM_JOB_PARTITION'),
            'n_nodes'      : int(n_nodes),
            'nodelist'     : os.environ.get('SLURM_JOB_NODELIST'),
            'cpus_per_node': _intenv('SLURM_CPUS_ON_NODE'),
            'gpus_per_node': gpus_per_node if gpus_per_node else None,
            'account'      : os.environ.get('SLURM_JOB_ACCOUNT'),
            'job_name'     : os.environ.get('SLURM_JOB_NAME'),
            'runtime'      : runtime,
        }


register_backend(SlurmBatchSystem)
