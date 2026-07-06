"""
QueueInfo abstract base + shared helpers + factory.

Backend implementations live in queue_info_slurm.py and queue_info_pbs.py.
"""

import getpass
import time
import threading

from abc import ABC, abstractmethod


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
