"""PBSPro implementation of BatchSystem.

Targets PBS Professional (Altair) as found on Aurora and similar ALCF
systems. Uses qstat / qdel for job control. Aurora's PBSPro publishes
state info both as text (qstat -f) and partially as JSON (qstat -f -F json
on recent versions); this backend only relies on text output, which is
universal.
"""

import os
import shutil
import subprocess

from .batch_system import (BatchSystem, register_backend,
                           STATE_PENDING, STATE_RUNNING, STATE_DONE,
                           STATE_FAILED, STATE_CANCELLED,
                           STATE_HELD, STATE_UNKNOWN)


# PBS single-letter state → normalized vocabulary.
# Reference: PBSPro qstat manual (job_state column).
_STATE_MAP = {
    'Q': STATE_PENDING,    # queued
    'W': STATE_PENDING,    # waiting (begin time / dependency)
    'T': STATE_PENDING,    # being moved
    'R': STATE_RUNNING,    # running
    'B': STATE_RUNNING,    # array job: at least one subjob running
    'E': STATE_RUNNING,    # exiting (still cleaning up)
    'F': STATE_DONE,       # finished (PBSPro only with -x)
    'X': STATE_DONE,       # subjob completed (array)
    'H': STATE_HELD,       # held
    'S': STATE_HELD,       # suspended
    'M': STATE_HELD,       # moved to another server
    'U': STATE_HELD,       # cycle-harvesting suspension
}


def _parse_pbs_walltime(s: str) -> 'int | None':
    """Parse a PBS walltime string ([[HH:]MM:]SS) to seconds.

    Returns None on empty / unset values.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    parts = s.split(':')
    try:
        if   len(parts) == 3: h, m, sec = (int(p) for p in parts)
        elif len(parts) == 2: h, m, sec = 0, int(parts[0]), int(parts[1])
        elif len(parts) == 1: h, m, sec = 0, 0, int(parts[0])
        else: raise ValueError
    except ValueError as e:
        raise RuntimeError(f"Cannot parse PBS walltime: {s!r}") from e
    return h * 3600 + m * 60 + sec


def _parse_qstat_f(stdout: str) -> dict:
    """Parse ``qstat -f <jobid>`` text output into a {key: value} dict.

    PBSPro indents every attribute line (``    key = value``).  Continuation
    lines for long values are indented *more* than attribute lines and may
    themselves contain ``=`` characters (e.g. inside ``Resource_List.select``).
    The rule used here: the first indented attribute line sets the
    *attribute-indent* width; any line indented strictly deeper is a
    continuation of the prior key.
    Section headers like ``Job Id: 12345`` are ignored.
    """
    result = {}
    cur_key = None
    cur_val_parts = []
    attr_indent = None    # set by the first attribute line we see

    def _flush():
        if cur_key is not None:
            result[cur_key] = ''.join(cur_val_parts).strip()

    for raw in stdout.splitlines():
        if not raw or not raw.strip():
            continue
        stripped = raw.strip()
        if stripped.startswith('Job Id:'):
            continue
        indent = len(raw) - len(raw.lstrip())
        is_continuation = (attr_indent is not None
                           and indent > attr_indent
                           and cur_key is not None)
        if is_continuation:
            cur_val_parts.append(stripped)
            continue
        if attr_indent is None:
            attr_indent = indent
        # A new attribute line.
        if '=' in stripped:
            _flush()
            k, v = stripped.split('=', 1)
            cur_key = k.strip()
            cur_val_parts = [v.strip()]
        elif cur_key is not None:
            # No '=' and not deeper indented → treat as plain continuation.
            cur_val_parts.append(stripped)
    _flush()
    return result


def _parse_exec_host(s: str) -> list:
    """Parse PBS exec_host into a list of hostnames.

    Format: ``host1/0*64+host2/0*64`` (host/cpuset*ncpus pairs).
    Returns deduplicated host list preserving order.
    """
    if not s:
        return []
    seen = set()
    hosts = []
    for token in s.split('+'):
        host = token.split('/', 1)[0].split('.', 1)[0]
        if host and host not in seen:
            seen.add(host)
            hosts.append(host)
    return hosts


def _read_pbs_nodefile() -> list:
    """Return deduplicated host list from $PBS_NODEFILE, empty if missing."""
    path = os.environ.get('PBS_NODEFILE')
    if not path or not os.path.isfile(path):
        return []
    try:
        with open(path) as f:
            lines = [l.strip() for l in f if l.strip()]
    except OSError:
        return []
    seen = set()
    hosts = []
    for h in lines:
        h = h.split('.', 1)[0]
        if h not in seen:
            seen.add(h)
            hosts.append(h)
    return hosts


class PBSProBatchSystem(BatchSystem):
    """PBSPro scheduler interface."""

    name          = 'pbs'
    psij_executor = 'pbs'

    def __init__(self) -> None:
        super().__init__()
        # Native ids we've been asked to cancel.  PBSPro's qstat letter
        # codes have no dedicated 'cancelled' value — the job ends up in
        # 'F' (finished) just like a normal exit, so we remember the
        # intent here and map terminal states to STATE_CANCELLED in
        # job_state().
        self._cancelled: set = set()

    @classmethod
    def detect(cls) -> bool:
        return shutil.which('qstat') is not None

    def in_allocation(self) -> bool:
        return bool(os.environ.get('PBS_JOBID'))

    def job_id(self) -> 'str | None':
        return os.environ.get('PBS_JOBID')

    def job_state(self, native_id) -> str:
        try:
            r = subprocess.run(
                ['qstat', '-f', str(native_id)],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return STATE_UNKNOWN
        if r.returncode != 0:
            # try -x to look up finished jobs
            try:
                r = subprocess.run(
                    ['qstat', '-x', '-f', str(native_id)],
                    capture_output=True, text=True, timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                return STATE_UNKNOWN
            if r.returncode != 0:
                return STATE_UNKNOWN
        info = _parse_qstat_f(r.stdout)
        code = info.get('job_state', '').strip()
        if not code:
            return STATE_UNKNOWN
        state = _STATE_MAP.get(code[0].upper(), STATE_UNKNOWN)
        if str(native_id) in self._cancelled and state in (STATE_DONE, STATE_FAILED):
            return STATE_CANCELLED
        return state

    def job_nodes(self, native_id) -> list:
        try:
            r = subprocess.run(
                ['qstat', '-f', str(native_id)],
                capture_output=True, text=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return []
        if r.returncode != 0:
            return []
        info = _parse_qstat_f(r.stdout)
        return _parse_exec_host(info.get('exec_host', ''))

    def nodelist(self) -> list:
        # PBS_NODEFILE lists each host once per slot; ``_read_pbs_nodefile``
        # already dedupes and short-circuits on a missing / empty file.
        return _read_pbs_nodefile()

    def cancel(self, native_id) -> None:
        r = subprocess.run(['qdel', str(native_id)],
                           capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            raise RuntimeError(f"qdel failed: {r.stderr.strip()}")
        self._cancelled.add(str(native_id))

    def job_allocation(self) -> 'dict | None':
        job_id = os.environ.get('PBS_JOBID')
        if not job_id:
            return None

        # Node count from PBS_NODEFILE first (always present in jobs),
        # fall back to qstat -f Resource_List.nodect if needed.
        nodes = _read_pbs_nodefile()
        n_nodes = len(nodes) or None

        # Pull walltime / partition / account from qstat.
        runtime    = None
        partition  = os.environ.get('PBS_QUEUE') or os.environ.get('PBS_O_QUEUE')
        account    = os.environ.get('PBS_ACCOUNT')
        job_name   = os.environ.get('PBS_JOBNAME')
        nodelist   = ','.join(nodes) if nodes else None
        cpus_per_node = None
        gpus_per_node = None

        try:
            r = subprocess.run(
                ['qstat', '-f', job_id],
                capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                info = _parse_qstat_f(r.stdout)
                runtime = _parse_pbs_walltime(
                    info.get('Resource_List.walltime', ''))
                if not n_nodes:
                    nct = info.get('Resource_List.nodect', '')
                    try:
                        n_nodes = int(nct) if nct else None
                    except ValueError:
                        n_nodes = None
                if not partition:
                    partition = info.get('queue', '') or None
                if not account:
                    account = info.get('Account_Name', '') or None
                if not job_name:
                    job_name = info.get('Job_Name', '') or None
                if not nodelist:
                    eh = info.get('exec_host', '')
                    if eh:
                        nodelist = ','.join(_parse_exec_host(eh))

                # Extract per-node resources from select=... when possible.
                # Format: "1:ncpus=64:ngpus=4" or "2:ncpus=64".
                select = info.get('Resource_List.select', '')
                if select:
                    chunk = select.split('+', 1)[0]
                    tokens = chunk.split(':')
                    for tok in tokens:
                        if tok.startswith('ncpus='):
                            try: cpus_per_node = int(tok[6:])
                            except ValueError: pass
                        elif tok.startswith('ngpus='):
                            try: gpus_per_node = int(tok[6:])
                            except ValueError: pass
        except (OSError, subprocess.TimeoutExpired) as exc:
            if not n_nodes:
                raise RuntimeError(
                    f"PBS_JOBID={job_id!r} is set but cannot query qstat: {exc}"
                ) from exc

        if not n_nodes:
            raise RuntimeError(
                f"PBS_JOBID={job_id!r} is set but node count is unavailable")

        return {
            'job_id'       : job_id,
            'partition'    : partition,
            'n_nodes'      : int(n_nodes),
            'nodelist'     : nodelist,
            'cpus_per_node': cpus_per_node,
            'gpus_per_node': gpus_per_node,
            'account'      : account,
            'job_name'     : job_name,
            'runtime'      : runtime,
        }


class AuroraPBSBatchSystem(PBSProBatchSystem):
    """Aurora (ALCF) specialization of PBSPro.

    Aurora requires ``#PBS -l filesystems=<list>`` on every submission
    (qsub rejects jobs without it), and the expected user base of
    radical.orbit is not expected to know PBS-level resource names.  This
    class fills in the defaults so that the UI and the Python client API
    both succeed out of the box; user-supplied values still win on
    conflict.

    Detection: the vendor-installed ``/opt/aurora`` directory is present
    on both login and compute nodes and is unambiguous (no hostname
    regex, no subprocess calls).
    """

    name = 'pbs-aurora'

    @classmethod
    def detect(cls) -> bool:
        return (super().detect()
                and os.path.isdir('/opt/aurora'))

    def default_custom_attributes(self) -> dict:
        return {'pbs.l': 'filesystems=home:flare'}


# Register Aurora before the generic backend so detect_batch_system()
# picks the specialization on ALCF hosts.
register_backend(AuroraPBSBatchSystem)
register_backend(PBSProBatchSystem)
