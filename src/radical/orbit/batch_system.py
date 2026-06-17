"""
Batch system abstraction.

Encapsulates everything about the local HPC scheduler that callers
outside of ``queue_info`` need to know: presence detection, in-allocation
detection, normalized job state, node lookup, cancel, allocation summary.

Backend implementations live in ``batch_system_slurm.py`` and
``batch_system_pbs.py`` and register themselves via ``_REGISTRY`` below.
``detect_batch_system()`` returns the first backend whose ``detect()`` is
true; otherwise ``NullBatchSystem``.

State vocabulary (used everywhere outside the backend modules):
    PENDING    — queued, not yet running
    RUNNING    — running on compute nodes
    DONE       — completed successfully
    FAILED     — completed with non-zero exit, NODE_FAIL, PREEMPTED, TIMEOUT, …
    CANCELLED  — user- or admin-cancelled
    HELD       — held / suspended
    UNKNOWN    — backend reported nothing (job gone, transient error, no scheduler)
"""

from abc import ABC, abstractmethod


# Normalized state vocabulary
STATE_PENDING   = 'PENDING'
STATE_RUNNING   = 'RUNNING'
STATE_DONE      = 'DONE'
STATE_FAILED    = 'FAILED'
STATE_CANCELLED = 'CANCELLED'
STATE_HELD      = 'HELD'
STATE_UNKNOWN   = 'UNKNOWN'

TERMINAL_STATES = frozenset({STATE_DONE, STATE_FAILED, STATE_CANCELLED})


class BatchSystem(ABC):
    """Per-process scheduler interface.

    Subclasses are stateless; one instance per backend is held in the
    module-level cache returned by :func:`detect_batch_system`.
    """

    name          : str = 'none'   # short identifier
    psij_executor : str = 'local'  # corresponding PsiJ executor name

    @classmethod
    @abstractmethod
    def detect(cls) -> bool:
        """Return True if this scheduler is installed locally."""

    @abstractmethod
    def in_allocation(self) -> bool:
        """True when this process runs inside a batch job."""

    @abstractmethod
    def job_id(self) -> 'str | None':
        """Native job id of the current allocation, or None on a login node."""

    @abstractmethod
    def job_state(self, native_id) -> str:
        """Return a normalized state string for *native_id*.

        Returns one of the STATE_* constants. Returns STATE_UNKNOWN on any
        error (job gone, command failure, timeout, parse error).
        """

    @abstractmethod
    def job_nodes(self, native_id) -> list:
        """Return the list of compute node hostnames allocated to *native_id*.

        Returns an empty list if the job is not running or the lookup fails.
        """

    @abstractmethod
    def nodelist(self) -> list:
        """Return the expanded list of hostnames in *this* endpoint's allocation.

        Returns an empty list when the endpoint is not running inside a job (i.e.
        on a login node) or when the scheduler doesn't expose the info.
        Hostnames are returned one per node, in scheduler-reported order.
        """

    @abstractmethod
    def cancel(self, native_id) -> None:
        """Cancel *native_id*. Raises RuntimeError on failure."""

    @abstractmethod
    def job_allocation(self) -> 'dict | None':
        """Return allocation info about the current batch job, or None.

        On a login node returns None. Inside a batch job returns a dict with
        keys: job_id, partition, n_nodes, nodelist, cpus_per_node,
        gpus_per_node, account, job_name, runtime (seconds, None for
        unlimited).

        Raises RuntimeError when in_allocation() is true but details cannot
        be collected.
        """

    def terminal_states(self) -> frozenset:
        """The set of normalized states that mean 'job is done'."""
        return TERMINAL_STATES

    def default_custom_attributes(self) -> dict:
        """Per-site PSIJ custom_attributes to merge into every submission.

        Returned when the caller submits via the PSIJ executor that
        corresponds to this backend (``psij_executor`` on the class).
        Caller-provided attributes take precedence on key conflicts.

        Default: no defaults.  Site-specific subclasses override to
        encode hard requirements (e.g. Aurora's ``filesystems=home:flare``
        resource is mandatory for qsub).
        """
        return {}


class NullBatchSystem(BatchSystem):
    """Fallback when no scheduler is installed (e.g. dev laptop)."""

    name          = 'none'
    psij_executor = 'local'

    @classmethod
    def detect(cls) -> bool:
        return True   # always last in the registry; matches everything

    def in_allocation(self) -> bool:
        return False

    def job_id(self) -> 'str | None':
        return None

    def job_state(self, native_id) -> str:
        return STATE_UNKNOWN

    def job_nodes(self, native_id) -> list:
        return []

    def nodelist(self) -> list:
        return []

    def cancel(self, native_id) -> None:
        raise RuntimeError(f"no batch system available to cancel job {native_id}")

    def job_allocation(self) -> 'dict | None':
        return None


# ---------------------------------------------------------------------------
# Registry + detection
# ---------------------------------------------------------------------------

_REGISTRY : list = []   # populated by backend modules at import time
_DETECTED : 'BatchSystem | None' = None


def register_backend(cls) -> None:
    """Register a BatchSystem subclass. Called by backend modules at import."""
    if cls not in _REGISTRY:
        _REGISTRY.append(cls)


def detect_batch_system(force: bool = False) -> BatchSystem:
    """Return the active batch system, probing the registry on first call.

    Result is cached. Pass ``force=True`` to re-probe (mainly for tests).
    """
    global _DETECTED
    if _DETECTED is not None and not force:
        return _DETECTED

    # Import backend modules so they register. Local import avoids circular
    # imports during package init.
    from . import batch_system_slurm   # noqa: F401
    from . import batch_system_pbs     # noqa: F401

    for cls in _REGISTRY:
        try:
            if cls.detect():
                _DETECTED = cls()
                return _DETECTED
        except Exception:
            continue

    _DETECTED = NullBatchSystem()
    return _DETECTED


def reset_detection() -> None:
    """Clear the cached detection (tests only)."""
    global _DETECTED
    _DETECTED = None
