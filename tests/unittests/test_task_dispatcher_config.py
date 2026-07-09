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
