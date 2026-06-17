"""
QueueInfo abstract base + shared helpers + factory.

Backend implementations live in queue_info_slurm.py and queue_info_pbs.py.
"""

import getpass
import re
import time
import threading

from abc import ABC, abstractmethod


# Node states considered unavailable for scheduling (SLURM vocabulary;
# kept here for legacy reasons but only used by the SLURM backend).
_UNAVAIL_STATES = {'DOWN',    'DRAIN',   'DRAINING',
                   'FAIL',    'FAILING', 'MAINT',
                   'FUTURE',  'POWER_DOWN', 'POWERED_DOWN',
                   'NOT_RESPONDING', 'REBOOT_ISSUED'}


def _resolve_user(user):
    """
    Normalise the user argument used throughout QueueInfo public methods.

    - ``None``  → current OS user (default: self)
    - ``'*'``   → ``None`` (no filter; admin / all-users view)
    - anything else → returned unchanged
    """
    if user is None:
        return getpass.getuser()
    if user == '*':
        return None
    return user


def _unwrap(obj):
    """
    Extract a value from SLURM's {set, infinite, number} wrapper.

    Returns:
      The numeric value, or None if the field is infinite or unset.
    """

    if not isinstance(obj, dict):
        return obj

    if obj.get('infinite'):
        return None
    if not obj.get('set', True):
        return None

    return obj.get('number')


def _parse_gpus(gres_str):
    """
    Parse GPU count from a SLURM GRES string.

    Handles formats like:
      "gpu:8(S:0-7)"
      "gpu:mi250:8(S:0-7)"
      "gpu:8"
      "(null)"
      ""

    Returns:
      int: number of GPUs, or 0 if none.
    """

    if not gres_str or gres_str == '(null)':
        return 0

    total = 0
    for entry in gres_str.split(','):
        entry = entry.strip()
        if not entry.startswith('gpu'):
            continue

        # strip socket binding like (S:0-7)
        entry = re.sub(r'\(.*?\)', '', entry)

        parts = entry.split(':')
        # gpu:N or gpu:TYPE:N
        for part in reversed(parts):
            try:
                total += int(part)
                break
            except ValueError:
                continue

    return total


class QueueInfo(ABC):
    """
    Abstract base class for batch system queue information backends.

    Subclasses implement _collect_info, _collect_jobs, _collect_allocations
    to gather data from a specific batch system.  Results are cached with a
    configurable TTL.
    """

    _cache_ttl = 60     # class attribute — 60-second default, tweakable

    # Backend identifier exposed to clients (overridden by subclasses).
    backend_name = 'none'

    def __init__(self):

        self._cache      : dict        = {}
        self._cache_time : dict        = {}
        self._cache_lock : threading.Lock = threading.Lock()

    def start_prefetch(self):
        """
        Start background threads to prefetch queue info and allocations in
        parallel so both caches are warm as quickly as possible.
        """
        user = getpass.getuser()

        def _fetch_info():
            try:
                self.get_info(user=user)
            except Exception:
                pass

        def _fetch_alloc():
            try:
                self.list_allocations(user=user)
            except Exception:
                pass

        threading.Thread(target=_fetch_info,  daemon=True).start()
        threading.Thread(target=_fetch_alloc, daemon=True).start()


    def _get_cached(self, key, force, collector, *args):
        """
        Thread-safe caching with non-blocking collector:
          1. Acquire lock, check cache → return if valid
          2. Release lock, run collector (may be slow)
          3. Re-acquire lock, store result
        """

        if not force:
            with self._cache_lock:
                if key in self._cache:
                    age = time.time() - self._cache_time.get(key, 0)
                    if age < self._cache_ttl:
                        return self._cache[key]

        # run collector outside of lock
        result = collector(*args)

        with self._cache_lock:
            self._cache[key]      = result
            self._cache_time[key] = time.time()

        return result


    def get_info(self, user=None, force=False):
        """
        Return queue/partition info. force=True bypasses cache.

        Args:
            user (str): User to filter partitions for. When None (default),
                defaults to the current user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: {"queues": {<partition_name>: {...}, ...}}
        """
        user = _resolve_user(user)
        key = f'info:{user}'
        return self._get_cached(key, force, self._collect_info_filtered, user)


    def list_jobs(self, queue, user=None, force=False):
        """
        List jobs in a queue.

        Args:
            queue (str): Partition name to list jobs for.
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs.
            force (bool): Bypass cache if True.

        Returns:
            dict: {"jobs": [<job_dict>, ...]}
        """
        user = _resolve_user(user)
        key = f'jobs:{queue}:{user}'
        return self._get_cached(key, force, self._collect_jobs, queue, user)


    def list_all_jobs(self, user=None, force=False):
        """
        List all jobs for a user across all partitions.

        Args:
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs.
            force (bool): Bypass cache if True.

        Returns:
            dict: {"jobs": [<job_dict>, ...]}
        """
        user = _resolve_user(user)
        key = f'all_jobs:{user}'
        return self._get_cached(key, force, self._collect_all_user_jobs, user)


    def list_allocations(self, user=None, force=False):
        """
        List allocations/projects.  If user is set, filter to that user.
        When user=None, defaults to the current user. To return all
        rows, pass user='*'.
        """
        user = _resolve_user(user)
        key = f'alloc:{user}'
        return self._get_cached(key, force, self._collect_allocations, user)


    def _collect_info_filtered(self, user):
        """
        Collect queue/partition info filtered by user access.

        Args:
            user (str): User to filter for. None means no filtering.

        Returns:
            dict: {"queues": {<partition_name>: {...}, ...}}
                  Queue names are sorted alphabetically for stable UI order.
        """
        info = self._collect_info()

        if user is None:
            allowed = None
        else:
            allowed = self._get_user_partitions(user)  # pylint: disable=E1128

        queues = info.get('queues', {})
        sorted_queues = {
            k: queues[k]
            for k in sorted(queues)
            if allowed is None or k in allowed
        }
        return {'queues': sorted_queues}

    @abstractmethod
    def _collect_info(self):
        raise NotImplementedError

    @abstractmethod
    def _collect_jobs(self, queue, user):
        raise NotImplementedError

    @abstractmethod
    def _collect_all_user_jobs(self, user):
        raise NotImplementedError

    @abstractmethod
    def _collect_allocations(self, user):
        raise NotImplementedError

    def _get_user_partitions(self, user):
        """
        Return the set of partition names the user has access to.

        Override in subclasses that support partition-level access control.
        Return None to indicate no filtering is supported.

        Args:
            user (str): Username to check access for.

        Returns:
            set | None: Set of allowed partition names, or None if not supported.
        """
        return None


def make_queue_info(batch=None, conf_path=None) -> 'QueueInfo':
    """Factory: return a QueueInfo subclass matching the active scheduler.

    Args:
        batch:     Optional pre-detected BatchSystem instance. If None, calls
                   :func:`batch_system.detect_batch_system`.
        conf_path: Optional path to the scheduler's configuration file
                   (forwarded to the backend; only SLURM uses it today).

    Returns:
        QueueInfo: a QueueInfoSlurm, QueueInfoPBSPro, or QueueInfoNone
                   instance depending on what the local system supports.
    """
    if batch is None:
        from .batch_system import detect_batch_system
        batch = detect_batch_system()

    # Key on psij_executor (scheduler family) rather than name, so that
    # site specializations like AuroraPBSBatchSystem (name='pbs-aurora',
    # psij_executor='pbs') still route to the PBS queue_info backend.
    if batch.psij_executor == 'slurm':
        from .queue_info_slurm import QueueInfoSlurm
        return QueueInfoSlurm(slurm_conf=conf_path)
    if batch.psij_executor == 'pbs':
        from .queue_info_pbs import QueueInfoPBSPro
        return QueueInfoPBSPro()

    from .queue_info_none import QueueInfoNone
    return QueueInfoNone()


# Backwards-compat re-exports. External code (and the test suite) imports
# QueueInfoSlurm from this module; keep that path live.
from .queue_info_slurm import QueueInfoSlurm   # noqa: E402, F401
