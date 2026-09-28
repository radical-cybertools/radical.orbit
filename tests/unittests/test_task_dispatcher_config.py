"""Unit tests for task_dispatcher_config.

Covers: PilotSize/PoolConfig parsing, schema errors, multiple pools and
multiple sizes, the built-in default-pool factory.  There is no on-disk pool
manifest to load — pools arrive only through ``register_session``.
"""

import pytest

from radical.orbit.task_dispatcher_config import (
    DEFAULT_POOL_NAME, PilotSize, PoolConfigError,
    default_pool_config, parse_pools,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_pool_dict(**overrides):
    """Return a valid single-pool raw dict; overrides merged into pool."""
    pool = {
        'name'         : 'cpu',
        'queue'        : 'batch',
        'account'      : 'proj123',
        'default_size' : 's',
        'pilot_sizes'  : {
            's': {'nodes': 1, 'cpus_per_node': 64,
                  'rhapsody_backend': 'concurrent'}
        },
    }
    pool.update(overrides)
    return {'pools': [pool]}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestParsePools:

    def test_minimal_valid_config(self):
        pools = parse_pools(_minimal_pool_dict())
        assert set(pools.keys()) == {'cpu'}
        p = pools['cpu']
        assert p.name == 'cpu'
        assert p.queue == 'batch'
        assert p.account == 'proj123'
        assert p.default_size == 's'
        assert p.min_pilots == 0
        assert p.max_pilots == 4
        assert p.strategy == 'conservative'
        assert p.strategy_config == {}
        assert p.scratch_base is None

    def test_pilot_size_fields(self):
        pools = parse_pools(_minimal_pool_dict(pilot_sizes={
            'gpu_big': {
                'nodes': 4, 'cpus_per_node': 128, 'gpus_per_node': 8,
                'walltime_sec': 7200, 'rhapsody_backend': 'dragon_v3'
            }
        }, default_size='gpu_big'))
        size = pools['cpu'].pilot_sizes['gpu_big']
        assert size == PilotSize(
            nodes=4, cpus_per_node=128, gpus_per_node=8,
            walltime_sec=7200, rhapsody_backend='dragon_v3')

    def test_multiple_pilot_sizes_per_pool(self):
        pools = parse_pools(_minimal_pool_dict(
            pilot_sizes={
                's': {'nodes': 1, 'cpus_per_node': 16,
                      'rhapsody_backend': 'concurrent'},
                'm': {'nodes': 4, 'cpus_per_node': 64,
                      'rhapsody_backend': 'dragon_v3'},
                'l': {'nodes': 16, 'cpus_per_node': 128,
                      'rhapsody_backend': 'dragon_v3'},
            }))
        assert set(pools['cpu'].pilot_sizes.keys()) == {'s', 'm', 'l'}

    def test_multiple_pools(self):
        raw = {'pools': [
            _minimal_pool_dict()['pools'][0],
            _minimal_pool_dict(name='gpu')['pools'][0],
        ]}
        pools = parse_pools(raw)
        assert set(pools.keys()) == {'cpu', 'gpu'}

    def test_account_nullable(self):
        pools = parse_pools(_minimal_pool_dict(account=None))
        assert pools['cpu'].account is None

    def test_strategy_config_pass_through(self):
        pools = parse_pools(_minimal_pool_dict(
            strategy='conservative',
            strategy_config={'min_dwell_sec': 15, 'custom_key': 'x'}))
        assert pools['cpu'].strategy_config == {
            'min_dwell_sec': 15, 'custom_key': 'x'}

    def test_known_strategy_accepted(self):
        pools = parse_pools(_minimal_pool_dict(strategy='conservative'))
        assert pools['cpu'].strategy == 'conservative'

    def test_unknown_strategy_rejected(self):
        # 'strategy' must name a registered policy (task_dispatcher_policy);
        # arbitrary/dotted specs are rejected at parse time so a bad name
        # 400s at register_session instead of failing at materialisation.
        with pytest.raises(PoolConfigError, match="unknown 'strategy'"):
            parse_pools(_minimal_pool_dict(
                strategy='my_module.pkg:MyStrategy'))

    def test_invalid_strategy_config_rejected(self):
        # The parser trial-instantiates the policy, so a strategy_config the
        # policy constructor rejects fails the declaration (400) instead of
        # raising later at pool materialisation (500 + dangling session).
        with pytest.raises(PoolConfigError, match='router_preference'):
            parse_pools(_minimal_pool_dict(
                strategy_config={'router_preference': 'bogus'}))

    def test_endpoint_name_defaults_to_none(self):
        """Pools without explicit endpoint_name parse to endpoint_name=None."""
        pools = parse_pools(_minimal_pool_dict())
        assert pools['cpu'].endpoint_name is None

    def test_endpoint_name_explicit_string(self):
        pools = parse_pools(_minimal_pool_dict(endpoint_name='endpoint_perlmutter'))
        assert pools['cpu'].endpoint_name == 'endpoint_perlmutter'

    def test_endpoint_name_explicit_null(self):
        pools = parse_pools(_minimal_pool_dict(endpoint_name=None))
        assert pools['cpu'].endpoint_name is None


# ---------------------------------------------------------------------------
# Default pool factory
# ---------------------------------------------------------------------------

class TestDefaultPool:

    def test_default_pool_name_constant(self):
        assert DEFAULT_POOL_NAME == 'default'

    def test_default_pool_config_factory(self):
        p = default_pool_config()
        assert p.name == 'default'
        assert p.endpoint_name is None             # auto-resolved later
        assert p.account is None
        assert p.max_pilots == 1
        assert p.min_pilots == 0
        assert p.strategy == 'conservative'
        assert p.default_size in p.pilot_sizes
        size = p.pilot_sizes[p.default_size]
        assert isinstance(size, PilotSize)
        assert size.rhapsody_backend == 'concurrent'
        # The zero-config default's queue is the unconfigured 'default'
        # sentinel, not a real batch queue: the dispatcher refuses to submit
        # a pilot for it (see PluginTaskDispatcher._do_pilot_submit) rather
        # than silently submitting to a queue literally named 'default'.
        assert p.queue == 'default'

    def test_default_pool_queue_override(self):
        p = default_pool_config(queue='regular')
        assert p.queue == 'regular'


# ---------------------------------------------------------------------------
# Schema errors
# ---------------------------------------------------------------------------

class TestSchemaErrors:

    def test_missing_top_level_pools_key(self):
        with pytest.raises(PoolConfigError, match="'pools'"):
            parse_pools({})

    def test_non_dict_root(self):
        with pytest.raises(PoolConfigError, match="JSON object"):
            parse_pools([])

    def test_empty_pools_list(self):
        with pytest.raises(PoolConfigError, match="no pools"):
            parse_pools({'pools': []})

    def test_missing_required_pool_field(self):
        for field in ('name', 'queue', 'default_size', 'pilot_sizes'):
            raw = _minimal_pool_dict()
            del raw['pools'][0][field]
            with pytest.raises(PoolConfigError, match=field):
                parse_pools(raw)

    def test_duplicate_pool_names(self):
        raw = {'pools': [
            _minimal_pool_dict()['pools'][0],
            _minimal_pool_dict()['pools'][0],
        ]}
        with pytest.raises(PoolConfigError, match="duplicate pool name"):
            parse_pools(raw)

    def test_default_size_not_in_pilot_sizes(self):
        with pytest.raises(PoolConfigError, match="default_size"):
            parse_pools(_minimal_pool_dict(default_size='nope'))

    def test_pilot_size_missing_backend(self):
        with pytest.raises(PoolConfigError, match="rhapsody_backend"):
            parse_pools(_minimal_pool_dict(pilot_sizes={
                's': {'nodes': 1, 'cpus_per_node': 64}  # no backend
            }))

    def test_pilot_size_empty_backend(self):
        with pytest.raises(PoolConfigError, match="rhapsody_backend"):
            parse_pools(_minimal_pool_dict(pilot_sizes={
                's': {'nodes': 1, 'cpus_per_node': 64,
                      'rhapsody_backend': ''}
            }))

    def test_pilot_size_zero_nodes(self):
        with pytest.raises(PoolConfigError, match="nodes"):
            parse_pools(_minimal_pool_dict(pilot_sizes={
                's': {'nodes': 0, 'cpus_per_node': 64,
                      'rhapsody_backend': 'concurrent'}
            }))

    def test_pilot_size_bool_rejected_as_int(self):
        """bool is a subclass of int; reject explicitly."""
        with pytest.raises(PoolConfigError, match="nodes"):
            parse_pools(_minimal_pool_dict(pilot_sizes={
                's': {'nodes': True, 'cpus_per_node': 64,
                      'rhapsody_backend': 'concurrent'}
            }))

    def test_min_pilots_greater_than_max(self):
        with pytest.raises(PoolConfigError, match="min_pilots"):
            parse_pools(_minimal_pool_dict(min_pilots=5, max_pilots=2))

    def test_empty_pilot_sizes(self):
        with pytest.raises(PoolConfigError, match="pilot_sizes"):
            parse_pools(_minimal_pool_dict(pilot_sizes={}))

    def test_endpoint_name_empty_string_rejected(self):
        with pytest.raises(PoolConfigError, match="endpoint_name"):
            parse_pools(_minimal_pool_dict(endpoint_name=''))

    def test_endpoint_name_non_string_rejected(self):
        with pytest.raises(PoolConfigError, match="endpoint_name"):
            parse_pools(_minimal_pool_dict(endpoint_name=42))


# ---------------------------------------------------------------------------
# Capability-class pools: members (plan 121 §3)
# ---------------------------------------------------------------------------

def _member_dict(**overrides):
    """Return a valid member declaration; overrides merged in."""
    m = {
        'member_id'    : 'perlmutter',
        'endpoint_name': 'ep_pm',
        'queue'        : 'regular',
        'account'      : 'm1234',
        'default_size' : 'default',
        'pilot_sizes'  : {
            'default': {'nodes': 1, 'cpus_per_node': 128,
                        'gpus_per_node': 4, 'walltime_sec': 1800,
                        'rhapsody_backend': 'concurrent'}
        },
        'attributes'   : {'site': 'NERSC', 'software': ['lammps']},
        'budget'       : {'node_hours': 40.0},
    }
    m.update(overrides)
    return m


def _class_pool_dict(members=None, **overrides):
    pool = {
        'name'      : 'fed-gpu',
        'pool_class': 'gpu',
        'members'   : members if members is not None else [_member_dict()],
    }
    pool.update(overrides)
    return {'pools': [pool]}


class TestMemberParsing:

    def test_list_form(self):
        cfg = parse_pools(_class_pool_dict())['fed-gpu']
        assert cfg.multi_member is True
        assert list(cfg.members) == ['perlmutter']
        assert cfg.pool_class == 'gpu'

    def test_map_form_is_equivalent(self):
        as_map = parse_pools(_class_pool_dict(
            members={'perlmutter': {k: v for k, v in _member_dict().items()
                                    if k != 'member_id'}}))['fed-gpu']
        as_list = parse_pools(_class_pool_dict())['fed-gpu']
        assert as_map == as_list

    def test_declaration_order_is_preserved(self):
        cfg = parse_pools(_class_pool_dict(members=[
            _member_dict(member_id='zeta'),
            _member_dict(member_id='alpha'),
        ]))['fed-gpu']
        assert list(cfg.members) == ['zeta', 'alpha']
        assert cfg.primary_member().member_id == 'zeta'

    def test_duplicate_member_id(self):
        with pytest.raises(PoolConfigError, match='duplicate member_id'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(), _member_dict()]))

    @pytest.mark.parametrize('bad', ['Perlmutter', '_pm', '-pm', 'p m', ''])
    def test_bad_member_id_charset(self, bad):
        with pytest.raises(PoolConfigError, match='member_id'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(member_id=bad)]))

    def test_default_queue_sentinel_rejected(self):
        with pytest.raises(PoolConfigError, match='sentinel'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(queue='default')]))

    def test_max_pilots_zero_rejected(self):
        with pytest.raises(PoolConfigError, match='max_pilots'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(max_pilots=0)]))

    def test_endpoint_name_required(self):
        m = _member_dict()
        del m['endpoint_name']
        with pytest.raises(PoolConfigError, match='endpoint_name'):
            parse_pools(_class_pool_dict(members=[m]))

    def test_default_size_must_be_in_pilot_sizes(self):
        with pytest.raises(PoolConfigError, match='default_size'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(default_size='nope')]))

    def test_bad_attribute_value_rejected(self):
        with pytest.raises(PoolConfigError, match='attribute'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(attributes={'a': {'nested': 1}})]))

    def test_attribute_list_of_strings_accepted(self):
        cfg = parse_pools(_class_pool_dict(members=[
            _member_dict(attributes={'software': ['a', 'b'],
                                     'site': 'x', 'mem_gb_per_node': 256})
        ]))['fed-gpu']
        assert cfg.primary_member().attributes['software'] == ['a', 'b']

    def test_unknown_budget_key_rejected(self):
        with pytest.raises(PoolConfigError, match='budget'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(budget={'core_hours': 1})]))

    def test_non_positive_budget_rejected(self):
        with pytest.raises(PoolConfigError, match='node_hours'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(budget={'node_hours': 0})]))

    def test_shared_fs_must_be_bool(self):
        with pytest.raises(PoolConfigError, match='shared_fs'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(shared_fs='yes')]))

    def test_long_pool_plus_member_name_rejected(self):
        with pytest.raises(PoolConfigError, match='64 characters'):
            parse_pools(_class_pool_dict(
                members=[_member_dict(member_id='m' * 60)],
                name='p' * 10))


class TestShapeSwitch:

    def test_members_key_makes_it_a_class_pool(self):
        assert parse_pools(_class_pool_dict())['fed-gpu'].multi_member

    def test_legacy_declaration_has_one_implicit_member(self):
        cfg = parse_pools(_minimal_pool_dict())['cpu']
        assert cfg.multi_member is False
        assert list(cfg.members) == ['']
        assert cfg.members[''].queue == 'batch'

    def test_explicit_false_beside_members_parses_legacy(self):
        """Defence in depth for an old or hand-edited state file."""
        raw = _minimal_pool_dict()
        raw['pools'][0]['multi_member'] = False
        raw['pools'][0]['members'] = [_member_dict()]
        cfg = parse_pools(raw)['cpu']
        assert cfg.multi_member is False
        assert list(cfg.members) == ['']
        assert cfg.queue == 'batch'

    def test_empty_members_rejected_on_the_declaration_path(self):
        with pytest.raises(PoolConfigError, match='must not be empty'):
            parse_pools(_class_pool_dict(members=[]))

    def test_empty_members_accepted_on_the_replay_path(self):
        cfg = parse_pools(_class_pool_dict(members=[]),
                          allow_empty_members=True)['fed-gpu']
        assert cfg.members == {}
        assert cfg.multi_member is True

    def test_missing_members_key_when_flagged(self):
        with pytest.raises(PoolConfigError, match="'members'"):
            parse_pools({'pools': [{'name': 'x', 'multi_member': True}]})


class TestProjection:

    def test_scalars_project_the_primary_member(self):
        cfg = parse_pools(_class_pool_dict(members=[
            _member_dict(member_id='primary', queue='q1',
                         endpoint_name='ep1', account='a1',
                         min_pilots=1, max_pilots=3,
                         scratch_base='/scratch/a'),
            _member_dict(member_id='second', queue='q2',
                         endpoint_name='ep2'),
        ]))['fed-gpu']
        assert cfg.queue         == 'q1'
        assert cfg.endpoint_name == 'ep1'
        assert cfg.account       == 'a1'
        assert cfg.min_pilots    == 1
        assert cfg.max_pilots    == 3
        assert cfg.scratch_base  == '/scratch/a'
        assert cfg.default_size  == 'default'

    def test_reproject_after_primary_removal(self):
        cfg = parse_pools(_class_pool_dict(members=[
            _member_dict(member_id='a', queue='q1', endpoint_name='ep1'),
            _member_dict(member_id='b', queue='q2', endpoint_name='ep2'),
        ]))['fed-gpu']
        cfg.members.pop('a')
        cfg.reproject()
        assert cfg.queue == 'q2' and cfg.endpoint_name == 'ep2'

    def test_bind_endpoint_writes_through_to_implicit_member(self):
        cfg = parse_pools(_minimal_pool_dict(endpoint_name=None))['cpu']
        cfg.bind_endpoint('picked')
        assert cfg.endpoint_name              == 'picked'
        assert cfg.members[''].endpoint_name == 'picked'


class TestPoolClass:

    def test_default_is_empty(self):
        assert parse_pools(_minimal_pool_dict())['cpu'].pool_class == ''

    @pytest.mark.parametrize('bad', ['GPU', 'g pu', 'gpu!', 42])
    def test_bad_pool_class_rejected_not_coerced(self, bad):
        with pytest.raises(PoolConfigError, match='pool_class'):
            parse_pools(_minimal_pool_dict(pool_class=bad))


class TestRoundTrip:
    """``parse_pools({'pools': [cfg.to_dict()]})`` is exactly the replay path."""

    def test_legacy_to_dict_flags_the_shape(self):
        """The persisted implicit member is ignored on replay because the
        explicit ``multi_member: false`` wins."""
        cfg = parse_pools(_minimal_pool_dict())['cpu']
        d   = cfg.to_dict()
        assert d['multi_member'] is False
        assert list(d['members']) == ['']

    def test_legacy_round_trip(self):
        cfg = parse_pools(_minimal_pool_dict())['cpu']
        assert parse_pools({'pools': [cfg.to_dict()]})['cpu'] == cfg

    def test_legacy_round_trip_is_stable_twice(self):
        cfg  = parse_pools(_minimal_pool_dict())['cpu']
        once = parse_pools({'pools': [cfg.to_dict()]})['cpu']
        assert parse_pools({'pools': [once.to_dict()]})['cpu'] == cfg

    def test_class_pool_round_trip(self):
        cfg = parse_pools(_class_pool_dict(members=[
            _member_dict(member_id='a'),
            _member_dict(member_id='b', endpoint_name='ep_br',
                         shared_fs=False, budget={}),
        ]))['fed-gpu']
        assert parse_pools({'pools': [cfg.to_dict()]})['fed-gpu'] == cfg

    def test_emptied_class_pool_round_trip(self):
        cfg = parse_pools(_class_pool_dict())['fed-gpu']
        cfg.members.clear()
        cfg.reproject()
        back = parse_pools({'pools': [cfg.to_dict()]}, 'replay',
                           allow_empty_members=True)['fed-gpu']
        assert back.members == {} and back.multi_member is True


class TestDirectConstruction:
    """``__post_init__`` is the single construction site of the implicit
    member: PoolConfig is instantiated directly in several places that
    never touch the parser."""

    def test_direct_poolconfig_has_its_implicit_member(self):
        from radical.orbit.task_dispatcher_config import PoolConfig
        cfg = PoolConfig(
            name='x', queue='batch', account=None,
            pilot_sizes={'s': PilotSize(nodes=1, cpus_per_node=4,
                                        rhapsody_backend='concurrent')},
            default_size='s')
        assert list(cfg.members) == ['']
        assert cfg.members[''].default_size == 's'

    def test_default_pool_config_has_its_implicit_member(self):
        cfg = default_pool_config()
        assert list(cfg.members) == ['']
        assert cfg.members[''].queue == DEFAULT_POOL_NAME

    def test_pilot_sizes_are_shared_by_reference(self):
        """The legacy projection and the member can never drift."""
        cfg = default_pool_config()
        assert cfg.members[''].pilot_sizes is cfg.pilot_sizes
