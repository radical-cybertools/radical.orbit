'''
Task dispatcher — requirement matching.

One pure, stateless function, :func:`satisfies`, is the *single*
implementation of "can this shape run this task".  It is used twice with the
same vocabulary:

- with a **member**'s ``attributes`` and its default :class:`PilotSize` —
  the scale-up decision (which member to grow) and the submit-time
  "no member could ever run this" gate;
- with a **pilot**'s attribute snapshot and its size snapshot — the dispatch
  decision (which pilot may run this task).

It knows nothing about occupancy: free capacity stays the dispatcher's
existing task-count slot test (``PilotRecord.free_capacity()``).  There is
deliberately no core/GPU reservation in this round — see plan 121 §1.1.

The reason strings deliberately read like
``federation_policy.BudgetLoadPolicy.reject_reason`` so a federation and a
dispatcher rejection are recognisably the same kind of statement.
'''

from __future__ import annotations

from typing import Any


# Backends whose group launch needs a ``pmi`` value the dispatcher cannot
# infer (rhapsody dragon v1, ``TaskLauncherV1._launch_group_task``): they
# slot-queue rather than place ranks.  Kept here so the matcher and the
# submit-time gate in ``plugin_task_dispatcher`` share one list.
NO_MPI_BACKENDS = frozenset(['dragon_v1'])


def satisfies(requirements: dict | None, attributes: dict | None,
              size: Any | None) -> str | None:
    '''Return ``None`` when *requirements* fit this shape, else a reason.

    *attributes* is a member's declared attribute map (or a pilot's
    snapshot of one); *size* is a :class:`PilotSize`-shaped object (or
    ``None``, in which case the size-dependent rules are skipped).

    Rules, in order:

    - ``software`` — ``set(req) <= set(attributes['software'])``.
    - ``cores``    — per **node**: ``size.cpus_per_node >= cores``.
    - ``gpus``     — per **node**: ``size.gpus_per_node >= gpus``.
    - ``mem_gb``   — against ``attributes['mem_gb_per_node']`` *when
      declared*; a missing attribute never rejects.
    - ``labels``   — every ``k: v`` needs ``attributes[k] == v`` or
      ``v in attributes[k]`` (list-valued attribute).  An undeclared label
      key rejects.
    - ``mpi``      — the backend must not be one that cannot place ranks.

    **Every other key is ignored**, ``ranks`` and any key this module does
    not know included: the submit-time parser
    (``plugin_task_dispatcher.parse_requirements``) is the gate that
    rejects unknown keys with a 400, and a matcher that also owned that
    whitelist would break the moment a key is added there.

    A requirement value ``<= 0``, an empty list or an empty map is always
    satisfied.
    '''
    if not requirements:
        return None

    attributes = attributes or {}

    # -- software --------------------------------------------------------
    want = requirements.get('software') or []
    if want:
        have = attributes.get('software') or []
        if isinstance(have, str):
            have = [have]
        missing = [s for s in want if s not in set(have)]
        if missing:
            return 'software missing: %s' % ', '.join(sorted(missing))

    # -- cores / gpus: shape only, per node (see module docstring) --------
    if size is not None:
        cores = requirements.get('cores') or 0
        if cores > 0 and getattr(size, 'cpus_per_node', 0) < cores:
            return 'cores %s < %s' % (getattr(size, 'cpus_per_node', 0), cores)

        gpus = requirements.get('gpus') or 0
        if gpus > 0 and getattr(size, 'gpus_per_node', 0) < gpus:
            return 'gpus %s < %s' % (getattr(size, 'gpus_per_node', 0), gpus)

    # -- memory: only when the member declares it ------------------------
    mem = requirements.get('mem_gb') or 0
    if mem > 0 and 'mem_gb_per_node' in attributes:
        have_mem = attributes.get('mem_gb_per_node') or 0
        if have_mem < mem:
            return 'mem_gb %s < %s' % (have_mem, mem)

    # -- labels ----------------------------------------------------------
    for key, val in (requirements.get('labels') or {}).items():
        if key not in attributes:
            return 'label %s=%s not matched' % (key, val)
        have_val = attributes[key]
        if isinstance(have_val, list):
            if val not in have_val:
                return 'label %s=%s not matched' % (key, val)
        elif have_val != val:
            return 'label %s=%s not matched' % (key, val)

    # -- mpi -------------------------------------------------------------
    if requirements.get('mpi') and size is not None:
        backend = getattr(size, 'rhapsody_backend', '')
        if backend in NO_MPI_BACKENDS:
            return 'backend %s cannot run an mpi task' % backend

    return None

