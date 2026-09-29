"""Unit tests for ``task_dispatcher_match.satisfies`` (plan 121 §3.3).

``satisfies`` is the *single* implementation of "can this shape run this
task", used with a member's attributes + default size (scale-up, the
submit-time gate) and with a pilot's attribute/size snapshot (dispatch).
It is pure and knows nothing about occupancy.

The load-bearing negative here is the last class: **keys outside the table
are ignored, not rejected**.  The submit-time parser owns the unknown-key
whitelist; a matcher that also owned it would double-own the contract and
break the moment a key is added there.
"""

import pytest

from radical.orbit.task_dispatcher_config import PilotSize
from radical.orbit.task_dispatcher_match  import satisfies


def _size(**kw) -> PilotSize:
    defaults = dict(nodes=1, cpus_per_node=8, gpus_per_node=2,
                    rhapsody_backend='concurrent')
    defaults.update(kw)
    return PilotSize(**defaults)


_ATTRS = {'site': 'NERSC', 'software': ['lammps', 'pytorch'],
          'mem_gb_per_node': 256, 'tier': 2}


# ---------------------------------------------------------------------------
# Trivially satisfied
# ---------------------------------------------------------------------------

class TestEmpty:

    @pytest.mark.parametrize('req', [None, {}])
    def test_no_requirements(self, req):
        assert satisfies(req, {}, None) is None

    @pytest.mark.parametrize('req', [
        {'cores': 0}, {'gpus': 0}, {'mem_gb': 0}, {'software': []},
        {'labels': {}}, {'cores': -1},
    ])
    def test_non_positive_or_empty_always_satisfied(self, req):
        assert satisfies(req, {}, _size(cpus_per_node=1, gpus_per_node=0)) \
            is None


# ---------------------------------------------------------------------------
# software
# ---------------------------------------------------------------------------

class TestSoftware:

    def test_subset_matches(self):
        assert satisfies({'software': ['lammps']}, _ATTRS, _size()) is None

    def test_full_set_matches(self):
        assert satisfies({'software': ['lammps', 'pytorch']},
                         _ATTRS, _size()) is None

    def test_missing_rejects_with_reason(self):
        assert satisfies({'software': ['vasp']}, _ATTRS, _size()) \
            == 'software missing: vasp'

    def test_missing_reason_is_sorted(self):
        assert satisfies({'software': ['zzz', 'aaa']}, _ATTRS, _size()) \
            == 'software missing: aaa, zzz'

    def test_undeclared_attribute_rejects(self):
        assert satisfies({'software': ['lammps']}, {}, _size()) \
            == 'software missing: lammps'


# ---------------------------------------------------------------------------
# cores / gpus -- shape only, per node
# ---------------------------------------------------------------------------

class TestSize:

    def test_cores_fit(self):
        assert satisfies({'cores': 8}, _ATTRS, _size()) is None

    def test_cores_exceed(self):
        assert satisfies({'cores': 16}, _ATTRS, _size()) == 'cores 8 < 16'

    def test_gpus_fit(self):
        assert satisfies({'gpus': 2}, _ATTRS, _size()) is None

    def test_gpus_exceed(self):
        assert satisfies({'gpus': 4}, _ATTRS, _size()) == 'gpus 2 < 4'

    def test_no_size_skips_size_rules(self):
        """A member with no default size still matches on attributes."""
        assert satisfies({'cores': 999, 'gpus': 999}, _ATTRS, None) is None


# ---------------------------------------------------------------------------
# mem_gb -- only when the attribute is declared
# ---------------------------------------------------------------------------

class TestMemory:

    def test_fits_declared(self):
        assert satisfies({'mem_gb': 128}, _ATTRS, _size()) is None

    def test_exceeds_declared(self):
        assert satisfies({'mem_gb': 512}, _ATTRS, _size()) \
            == 'mem_gb 256 < 512'

    def test_undeclared_never_rejects(self):
        assert satisfies({'mem_gb': 512}, {'site': 'PSC'}, _size()) is None


# ---------------------------------------------------------------------------
# labels
# ---------------------------------------------------------------------------

class TestLabels:

    def test_equal_match(self):
        assert satisfies({'labels': {'site': 'NERSC'}}, _ATTRS, _size()) \
            is None

    def test_numeric_match(self):
        assert satisfies({'labels': {'tier': 2}}, _ATTRS, _size()) is None

    def test_list_membership(self):
        assert satisfies({'labels': {'software': 'lammps'}},
                         _ATTRS, _size()) is None

    def test_value_mismatch(self):
        assert satisfies({'labels': {'site': 'PSC'}}, _ATTRS, _size()) \
            == 'label site=PSC not matched'

    def test_undeclared_key_rejects(self):
        assert satisfies({'labels': {'zone': 'a'}}, _ATTRS, _size()) \
            == 'label zone=a not matched'

    def test_not_in_list_rejects(self):
        assert satisfies({'labels': {'software': 'vasp'}},
                         _ATTRS, _size()) == 'label software=vasp not matched'


# ---------------------------------------------------------------------------
# mpi
# ---------------------------------------------------------------------------

class TestMpi:

    def test_dragon_v1_rejects(self):
        assert satisfies({'mpi': True}, _ATTRS,
                         _size(rhapsody_backend='dragon_v1')) \
            == 'backend dragon_v1 cannot run an mpi task'

    def test_other_backend_accepts(self):
        assert satisfies({'mpi': True}, _ATTRS,
                         _size(rhapsody_backend='dragon_v3')) is None

    def test_mpi_false_on_dragon_v1_accepts(self):
        assert satisfies({'mpi': False}, _ATTRS,
                         _size(rhapsody_backend='dragon_v1')) is None


# ---------------------------------------------------------------------------
# Unknown keys are IGNORED, not rejected (the parser owns the whitelist)
# ---------------------------------------------------------------------------

class TestUnknownKeysIgnored:

    def test_ranks_is_ignored(self):
        assert satisfies({'ranks': 4}, {}, _size(cpus_per_node=1)) is None

    def test_wholly_unknown_key_is_ignored(self):
        assert satisfies({'quantumness': 11}, {}, _size()) is None

    def test_unknown_key_does_not_mask_a_real_rejection(self):
        assert satisfies({'ranks': 4, 'gpus': 99}, _ATTRS, _size()) \
            == 'gpus 2 < 99'


# ---------------------------------------------------------------------------
# Rule order (first failing rule wins, so reasons are deterministic)
# ---------------------------------------------------------------------------

def test_software_reported_before_size():
    assert satisfies({'software': ['vasp'], 'cores': 999}, _ATTRS, _size()) \
        == 'software missing: vasp'
