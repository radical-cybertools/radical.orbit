"""Unit tests for federation_policy.

Covers the requirement matcher (the 121 §3.3 rules the dispatcher and the
federation share), the default :class:`BudgetLoadPolicy` — member filtering,
budget exhaustion, liveness exclusion, the load term, the deterministic
tie-break, the **cheapest-class** rule and the ``explain`` reasons a 409
carries — plus the ``module:Class`` loader.

The pre-class-pool ``pick(requirements, resources)`` /
``explain(requirements, resources)`` pair is **gone**, not deprecated: there
is exactly one policy API and nothing to drift against.
"""

import pytest

from radical.orbit.federation_policy import (
    BudgetLoadPolicy, FederationPolicy, FederationPolicyError,
    DEFAULT_POLICY, make_policy, satisfies,
)
from radical.orbit.federation_state import (
    MemberRecord, LIVENESS_LOST, LIVENESS_SUSPECT,
)


def _mem(member_id, *, cls=None, nodes=1, cpus=8, gpus=0, software=None,
         attributes=None, node_hours=10.0, used=0.0, running=0,
         liveness='ok', backend='concurrent', max_pilots=1):
    resource, _, name = member_id.rpartition('.')
    m = MemberRecord(
        member=name, member_id=member_id, queue='q', nodes=nodes,
        cpus_per_node=cpus, gpus_per_node=gpus, max_pilots=max_pilots,
        rhapsody_backend=backend, software=list(software or []),
        attributes=dict(attributes or {}),
        budget={'node_hours': node_hours} if node_hours else {},
        liveness=liveness)
    m.cls       = cls or m.default_class()
    m.pool_name = f'fed-{m.cls}'
    m.usage.node_hours_used      = used
    m.usage.node_hours_remaining = max(0.0, node_hours - used)
    m.usage.tasks_running        = running
    assert resource                                  # member ids are qualified
    return m


def _classes(*members):
    out = {}
    for m in members:
        out.setdefault(m.cls, []).append(m)
    return out


@pytest.fixture
def policy():
    return BudgetLoadPolicy()


# ---------------------------------------------------------------------------
# The shared matcher
# ---------------------------------------------------------------------------

class _Size:
    def __init__(self, cpus=8, gpus=0, backend='concurrent'):
        self.nodes            = 1
        self.cpus_per_node    = cpus
        self.gpus_per_node    = gpus
        self.rhapsody_backend = backend


class TestSatisfies:

    def test_empty_requirements_always_fit(self):
        assert satisfies({}, {}, _Size()) is None
        assert satisfies(None, None, None) is None

    def test_software_must_be_a_subset(self):
        attrs = {'software': ['lammps']}
        assert satisfies({'software': ['lammps']}, attrs, _Size()) is None
        assert satisfies({'software': ['lammps', 'pytorch']}, attrs,
                         _Size()) == 'software missing: pytorch'

    def test_cores_and_gpus_are_compared_per_node(self):
        size = _Size(cpus=4, gpus=1)
        assert satisfies({'cores': 4}, {}, size) is None
        assert satisfies({'cores': 8}, {}, size) == 'cores 4 < 8'
        assert satisfies({'gpus': 1},  {}, size) is None
        assert satisfies({'gpus': 2},  {}, size) == 'gpus 1 < 2'

    def test_zero_or_negative_requirements_are_always_satisfied(self):
        size = _Size(cpus=1, gpus=0)
        assert satisfies({'gpus': 0, 'cores': 0, 'mem_gb': 0}, {}, size) \
            is None
        assert satisfies({'software': []}, {}, size) is None

    def test_mem_gb_uses_the_per_node_attribute_when_declared(self):
        assert satisfies({'mem_gb': 8}, {'mem_gb_per_node': 16}, _Size()) \
            is None
        assert satisfies({'mem_gb': 32}, {'mem_gb_per_node': 16},
                         _Size()) == 'mem_gb 16 < 32'

    def test_an_undeclared_mem_gb_never_rejects(self):
        assert satisfies({'mem_gb': 999}, {}, _Size()) is None

    def test_labels_match_a_scalar_or_a_list_attribute(self):
        attrs = {'site': 'NERSC', 'tags': ['fast', 'io']}
        assert satisfies({'labels': {'site': 'NERSC'}}, attrs, _Size()) is None
        assert satisfies({'labels': {'tags': 'io'}}, attrs, _Size()) is None
        assert satisfies({'labels': {'site': 'PSC'}}, attrs, _Size()) == \
            'label site=PSC not matched'

    def test_an_undeclared_label_key_rejects(self):
        assert satisfies({'labels': {'zone': 'a'}}, {}, _Size()) == \
            'label zone=a not matched'

    def test_mpi_cannot_run_on_dragon_v1(self):
        assert satisfies({'mpi': True}, {}, _Size(backend='dragon_v1')) == \
            'backend dragon_v1 cannot run an mpi task'
        assert satisfies({'mpi': True}, {}, _Size(backend='concurrent')) \
            is None
        assert satisfies({'mpi': False}, {}, _Size(backend='dragon_v1')) \
            is None

    # -- three details that are verbatim from task_dispatcher_match, and
    # -- that a hand-rolled re-implementation gets wrong

    def test_missing_software_is_reported_sorted(self):
        # deterministic message: the reason string is compared in tests and
        # read by humans, and the request order is not meaningful
        assert satisfies({'software': ['pytorch', 'lammps', 'ase']},
                         {'software': []}, _Size()) == \
            'software missing: ase, lammps, pytorch'

    def test_a_string_valued_software_attribute_is_wrapped(self):
        # a member that declared software as a bare string must not have it
        # matched character by character
        assert satisfies({'software': ['lammps']},
                         {'software': 'lammps'}, _Size()) is None
        assert satisfies({'software': ['l']},
                         {'software': 'lammps'}, _Size()) == \
            'software missing: l'

    def test_mem_gb_per_node_declared_as_none_rejects(self):
        # declared-but-unknown is not the same as undeclared: the key is
        # present, so the rule applies and 'unknown' loses
        assert satisfies({'mem_gb': 8}, {'mem_gb_per_node': None},
                         _Size()) == 'mem_gb 0 < 8'
        assert satisfies({'mem_gb': 8}, {}, _Size()) is None

    def test_unknown_keys_are_ignored(self):
        # the submit-time parser owns the whitelist; a matcher that also
        # rejected unknown keys would double-own it
        assert satisfies({'ranks': 4}, {}, _Size()) is None
        assert satisfies({'whatever': 'x', 'ranks': 8}, {}, _Size()) is None


# ---------------------------------------------------------------------------
# Member filtering
# ---------------------------------------------------------------------------

class TestMemberFilter:

    def test_the_only_fitting_member_is_chosen(self, policy):
        small = _mem('a.default', cpus=2)
        big   = _mem('b.default', cpus=16)
        ranked = policy.eligible({'cores': 8}, [small, big])
        assert [m.member_id for m, _ in ranked] == ['b.default']

    def test_cores_are_now_compared_per_node_not_resource_wide(self, policy):
        # semantic change from the pre-class-pool policy: a member with
        # 4 nodes x 2 cores no longer serves a 8-core task, because no
        # shipped backend spreads one task across nodes
        wide = _mem('a.default', nodes=4, cpus=2)
        assert policy.reject_reason({'cores': 8}, wide) == 'cores 2 < 8'

    def test_software_must_be_a_subset(self, policy):
        m = _mem('a.default', software=['lammps'])
        assert policy.reject_reason({'software': ['lammps']}, m) is None
        assert 'pytorch' in policy.reject_reason(
            {'software': ['pytorch']}, m)

    def test_labels_reach_the_matcher_through_attributes(self, policy):
        m = _mem('a.default', attributes={'site': 'NERSC'})
        assert policy.reject_reason({'labels': {'site': 'NERSC'}}, m) is None
        assert policy.reject_reason({'labels': {'site': 'PSC'}}, m) == \
            'label site=PSC not matched'

    def test_ranks_and_mpi_do_not_reject_a_sane_member(self, policy):
        m = _mem('a.default')
        assert policy.reject_reason({'cores': 1, 'gpus': 0, 'mem_gb': 0,
                                     'ranks': 4, 'mpi': False,
                                     'software': [], 'labels': {}}, m) is None


class TestBudgetFilter:

    def test_exhausted_budget_excludes_the_member(self, policy):
        m = _mem('a.default', node_hours=1.0, used=1.0)
        assert 'node_hours' in policy.reject_reason({'node_hours': 0.5}, m)

    def test_a_sibling_is_not_excluded_with_it(self, policy):
        poor = _mem('a.cpu', cls='cpu', node_hours=1.0, used=1.0)
        rich = _mem('b.cpu', cls='cpu', node_hours=10.0)
        ranked = policy.eligible({'node_hours': 0.5}, [poor, rich])
        assert [m.member_id for m, _ in ranked] == ['b.cpu']

    def test_no_node_hours_requested_ignores_the_budget(self, policy):
        m = _mem('a.default', node_hours=1.0, used=1.0)
        assert policy.reject_reason({}, m) is None

    def test_the_reason_reports_the_shortfall(self, policy):
        m = _mem('a.default', node_hours=2.0, used=1.5)
        assert policy.reject_reason({'node_hours': 1.0}, m) == \
            'node_hours 0.500 < 1.000'


class TestLivenessFilter:

    def test_lost_is_excluded(self, policy):
        m = _mem('a.default', liveness=LIVENESS_LOST)
        assert policy.reject_reason({}, m) == 'liveness is lost'

    def test_suspect_is_excluded_too(self, policy):
        m = _mem('a.default', liveness=LIVENESS_SUSPECT)
        assert policy.reject_reason({}, m) == 'liveness is suspect'

    def test_a_live_peer_wins_over_a_lost_one(self, policy):
        dead = _mem('a.cpu', cls='cpu', liveness=LIVENESS_LOST)
        live = _mem('b.cpu', cls='cpu')
        ranked = policy.eligible({}, [dead, live])
        assert [m.member_id for m, _ in ranked] == ['b.cpu']


# ---------------------------------------------------------------------------
# Scoring and the tie-break
# ---------------------------------------------------------------------------

class TestScoring:

    def test_load_orders_two_otherwise_equal_members(self, policy):
        busy = _mem('a.cpu', cls='cpu', cpus=8, running=4)
        idle = _mem('b.cpu', cls='cpu', cpus=8, running=0)
        ranked = policy.eligible({}, [busy, idle])
        assert [m.member_id for m, _ in ranked] == ['b.cpu', 'a.cpu']

    def test_load_uses_the_member_slot_count(self, policy):
        # nodes x cpus_per_node, not the resource's total cores
        m = _mem('a.default', nodes=2, cpus=4, running=4)
        assert policy.score({}, m) == pytest.approx(1.0 - 4 / 8)

    def test_more_remaining_budget_wins(self, policy):
        spent = _mem('a.cpu', cls='cpu', node_hours=10.0, used=9.0)
        fresh = _mem('b.cpu', cls='cpu', node_hours=10.0, used=0.0)
        ranked = policy.eligible({}, [spent, fresh])
        assert ranked[0][0].member_id == 'b.cpu'

    def test_undeclared_budget_scores_as_full(self, policy):
        m = _mem('a.default', node_hours=0.0)
        assert policy.score({}, m) == pytest.approx(1.0)

    def test_ties_break_deterministically_on_the_member_id(self, policy):
        one = _mem('b.cpu', cls='cpu')
        two = _mem('a.cpu', cls='cpu')
        for members in ([one, two], [two, one]):
            ranked = policy.eligible({}, members)
            assert [m.member_id for m, _ in ranked] == ['a.cpu', 'b.cpu']


# ---------------------------------------------------------------------------
# Class choice
# ---------------------------------------------------------------------------

class TestPickClass:

    def test_a_gpu_task_lands_in_the_gpu_class(self, policy):
        classes = _classes(_mem('a.cpu', cls='cpu'),
                           _mem('b.gpu', cls='gpu', gpus=4,
                                software=['pytorch']))
        assert policy.pick_class({'gpus': 1, 'software': ['pytorch']},
                                 classes)[0] == 'gpu'

    def test_a_cpu_task_that_also_fits_a_gpu_member_lands_in_fed_cpu(self,
                                                                    policy):
        # THE rule: the GPU member has the software and enough cores, so it
        # is genuinely eligible — but a CPU task must not burn a GPU
        # allocation while a CPU one is available.
        cpu = _mem('a.cpu', cls='cpu', cpus=8,  software=['pytorch'])
        gpu = _mem('b.gpu', cls='gpu', cpus=64, gpus=8,
                   software=['pytorch'], node_hours=100.0)
        classes = _classes(cpu, gpu)
        req     = {'cores': 4, 'software': ['pytorch']}
        # both classes really do have an eligible member ...
        assert policy.eligible(req, [gpu])
        # ... and the cheapest one wins anyway
        assert policy.pick_class(req, classes)[0] == 'cpu'

    def test_the_gpu_class_is_used_when_no_cpu_member_fits(self, policy):
        cpu = _mem('a.cpu', cls='cpu', cpus=2, software=[])
        gpu = _mem('b.gpu', cls='gpu', cpus=64, gpus=8,
                   software=['pytorch'])
        classes = _classes(cpu, gpu)
        assert policy.pick_class({'software': ['pytorch']}, classes)[0] == \
            'gpu'

    def test_the_score_is_the_best_member_of_the_chosen_class(self, policy):
        busy = _mem('a.cpu', cls='cpu', cpus=8, running=4)
        idle = _mem('b.cpu', cls='cpu', cpus=8, running=0)
        cls, score = policy.pick_class({}, _classes(busy, idle))
        assert cls == 'cpu'
        assert score == pytest.approx(policy.score({}, idle))

    def test_a_class_with_no_eligible_member_is_skipped(self, policy):
        classes = _classes(_mem('a.cpu', cls='cpu', liveness=LIVENESS_LOST),
                           _mem('b.gpu', cls='gpu', gpus=1))
        assert policy.pick_class({}, classes)[0] == 'gpu'

    def test_nothing_eligible_is_none(self, policy):
        classes = _classes(_mem('a.cpu', cls='cpu', liveness=LIVENESS_LOST))
        assert policy.pick_class({}, classes) is None
        assert policy.pick_class({}, {}) is None

    def test_equally_cheap_classes_break_on_the_class_name(self, policy):
        # two classes whose cheapest members are identically shaped: the
        # answer must still be the same on every call
        classes = _classes(_mem('a.zed', cls='zed', cpus=8),
                           _mem('b.alp', cls='alp', cpus=8))
        assert policy.pick_class({}, classes)[0] == 'alp'


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

class TestExplain:

    def test_reasons_are_keyed_by_member_id(self, policy):
        members = [_mem('a.cpu', cls='cpu', gpus=0),
                   _mem('b.gpu', cls='gpu', gpus=1, node_hours=1.0,
                        used=1.0)]
        reasons = policy.explain({'gpus': 2, 'node_hours': 0.5}, members)
        assert set(reasons) == {'a.cpu', 'b.gpu'}
        assert reasons['a.cpu'] == 'gpus 0 < 2'
        assert 'gpus 1 < 2' == reasons['b.gpu']

    def test_omits_members_that_would_have_been_picked(self, policy):
        good = _mem('a.cpu', cls='cpu', cpus=16)
        bad  = _mem('b.cpu', cls='cpu', cpus=1)
        reasons = policy.explain({'cores': 8}, [good, bad])
        assert set(reasons) == {'b.cpu'}

    def test_it_agrees_with_pick_class(self, policy):
        members = [_mem('a.cpu', cls='cpu', cpus=1),
                   _mem('b.gpu', cls='gpu', cpus=1, gpus=1)]
        req = {'cores': 64}
        assert policy.pick_class(req, _classes(*members)) is None
        assert set(policy.explain(req, members)) == {'a.cpu', 'b.gpu'}

    def test_an_empty_federation_explains_nothing(self, policy):
        assert policy.explain({'cores': 1}, []) == {}


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

class _CustomPolicy(FederationPolicy):
    def __init__(self, cfg=None):
        super().__init__(cfg)
        self.seen = cfg


class TestMakePolicy:

    def test_default_is_budget_load(self):
        assert isinstance(make_policy(), BudgetLoadPolicy)
        assert isinstance(make_policy(DEFAULT_POLICY), BudgetLoadPolicy)

    def test_loads_a_module_class_spec(self):
        p = make_policy(f'{__name__}:_CustomPolicy')
        assert isinstance(p, _CustomPolicy)

    def test_config_reaches_the_policy(self):
        p = make_policy(f'{__name__}:_CustomPolicy', {'k': 'v'})
        assert p.seen == {'k': 'v'}

    def test_spec_without_a_colon_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='module:Class'):
            make_policy('nocolon')

    def test_unknown_module_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='cannot resolve'):
            make_policy('no.such.module:Policy')

    def test_unknown_attribute_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='cannot resolve'):
            make_policy(f'{__name__}:NoSuchClass')

    def test_non_policy_class_is_rejected(self):
        with pytest.raises(FederationPolicyError, match='not a '
                                                       'FederationPolicy'):
            make_policy(f'{__name__}:_Size')


class TestBaseIsInert:

    def test_the_base_chooses_nothing(self):
        base = FederationPolicy()
        assert base.pick_class({}, {'cpu': [_mem('a.cpu', cls='cpu')]}) is None
        assert base.eligible({}, [_mem('a.cpu', cls='cpu')]) == []
        assert base.explain({}, [_mem('a.cpu', cls='cpu')]) == {}

    def test_the_old_pick_explain_pair_is_gone(self):
        # replaced, not kept beside the new API: two parallel entry points
        # would guarantee they drift
        assert not hasattr(FederationPolicy, 'pick')
        assert not hasattr(BudgetLoadPolicy, 'pick')
