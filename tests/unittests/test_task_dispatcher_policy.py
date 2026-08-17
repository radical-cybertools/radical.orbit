"""Unit tests for task_dispatcher_policy — base class + manual registry.

Covers: builtin resolution of 'conservative', register_policy round-trip
(including precedence over a same-named builtin and pickup by PoolState /
``_drain_pending``), unknown-name rejection, argument validation, and the
inert defaults of the :class:`DispatchPolicy` base class.
"""

import pytest

from fastapi import FastAPI

from radical.orbit.plugin_task_dispatcher import (
    PluginTaskDispatcher, PoolState,
)
from radical.orbit.task_dispatcher_config import (
    PilotSize, PoolConfig, PoolConfigError, parse_pools,
)
from radical.orbit.task_dispatcher_policy import (
    _REGISTRY, DispatchPolicy, known_policies, make_policy, register_policy,
)
from radical.orbit.task_dispatcher_state import TaskRecord, TASK_QUEUED
from radical.orbit.task_dispatcher_strategy_conservative import (
    ConservativePolicy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pool_cfg(**overrides) -> PoolConfig:
    defaults = dict(
        name='cpu', queue='batch', account=None,
        pilot_sizes={'s': PilotSize(nodes=1, cpus_per_node=4,
                                    rhapsody_backend='concurrent')},
        default_size='s',
    )
    defaults.update(overrides)
    return PoolConfig(**defaults)


class _DummyPolicy(DispatchPolicy):
    """Inert policy that counts how often the dispatcher consults it."""

    def __init__(self, pool, cfg, **kw):
        super().__init__(pool, cfg, **kw)
        self.picks = 0

    def pick_dispatch(self, pool_state):
        self.picks += 1
        return None


@pytest.fixture
def clean_registry():
    """Snapshot + restore the runtime policy registry around a test."""
    saved = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(saved)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:

    def test_conservative_is_known(self):
        assert 'conservative' in known_policies()

    def test_make_policy_resolves_conservative(self):
        cfg    = _pool_cfg(strategy_config={'min_dwell_sec': 5.0})
        policy = make_policy(cfg)
        assert isinstance(policy, ConservativePolicy)
        assert isinstance(policy, DispatchPolicy)
        assert policy._min_dwell_sec == 5.0

    def test_make_policy_unknown_name_raises(self):
        cfg = _pool_cfg(strategy='no_such_policy')
        with pytest.raises(PoolConfigError, match='unknown dispatch strategy'):
            make_policy(cfg)

    def test_register_policy_roundtrip(self, clean_registry):
        register_policy('dummy', _DummyPolicy)
        assert 'dummy' in known_policies()
        policy = make_policy(_pool_cfg(strategy='dummy'))
        assert isinstance(policy, _DummyPolicy)

    def test_register_policy_overrides_builtin(self, clean_registry):
        register_policy('conservative', _DummyPolicy)
        policy = make_policy(_pool_cfg())
        assert isinstance(policy, _DummyPolicy)
        # names are de-duplicated across builtin + runtime registries
        assert known_policies().count('conservative') == 1

    def test_register_policy_validates_arguments(self):
        with pytest.raises(ValueError):
            register_policy('', _DummyPolicy)
        with pytest.raises(ValueError):
            register_policy('dummy', _DummyPolicy(_pool_cfg(), {}))

        class NotAPolicy:
            pass

        with pytest.raises(ValueError,
                           match='must inherit from DispatchPolicy'):
            register_policy('dummy', NotAPolicy)

    def test_parse_pools_accepts_registered_strategy(self, clean_registry):
        register_policy('dummy', _DummyPolicy)
        raw = {'pools': [{
            'name'        : 'cpu',
            'queue'       : 'batch',
            'default_size': 's',
            'strategy'    : 'dummy',
            'pilot_sizes' : {
                's': {'nodes': 1, 'cpus_per_node': 4,
                      'rhapsody_backend': 'concurrent'}},
        }]}
        assert parse_pools(raw)['cpu'].strategy == 'dummy'


# ---------------------------------------------------------------------------
# Base-class defaults
# ---------------------------------------------------------------------------

class TestDispatchPolicyDefaults:

    def test_defaults_are_inert(self):
        policy    = DispatchPolicy(_pool_cfg(), {})
        submitted = []
        assert policy.pick_dispatch(pool_state=None) is None
        assert policy.on_tick(None, submitted.append) is None
        assert policy.on_pilot_state(None, 'PENDING', 'ACTIVE') is None
        assert submitted == []


# ---------------------------------------------------------------------------
# Dispatcher integration — PoolState + _drain_pending drive the policy
# ---------------------------------------------------------------------------

class TestDispatcherIntegration:

    def _make_plugin(self, tmp_path) -> PluginTaskDispatcher:
        app = FastAPI()
        app.state.endpoint_name = 'endpoint0'
        app.state.broker_url    = 'https://localhost:9999'
        app.state.broker_caller = None
        app.state.broker_tap    = None
        return PluginTaskDispatcher(
            app, state_root=tmp_path / 'state',
            scratch_root=tmp_path / 'scratch')

    def test_pool_state_uses_registered_policy(self, tmp_path,
                                               clean_registry):
        register_policy('dummy', _DummyPolicy)
        plugin = self._make_plugin(tmp_path)
        cfg    = _pool_cfg(strategy='dummy', endpoint_name='endpoint0')
        ps     = PoolState(cfg, tmp_path / 'state' / 'p',
                           tmp_path / 'scratch' / 'p', plugin,
                           owning_sid='s.1')
        assert isinstance(ps.policy, _DummyPolicy)

        # _drain_pending consults the registered policy for queued tasks.
        ps.tasks['t.1'] = TaskRecord(
            task_id='t.1', pool='cpu', cmd=['echo'], cwd='/tmp',
            state=TASK_QUEUED, arrival_ts=1.0)
        plugin._drain_pending(ps)
        assert ps.policy.picks == 1

    def test_replay_skips_pool_with_unavailable_policy(self, tmp_path,
                                                       clean_registry):
        # A pool persisted with a runtime-registered policy must not crash
        # broker startup when that policy is absent after a restart — replay
        # skips it with a warning; other pools still come back.
        register_policy('dummy', _DummyPolicy)
        plugin = self._make_plugin(tmp_path)
        plugin._materialise_pool(
            's.1', _pool_cfg(strategy='dummy', endpoint_name='endpoint0'))
        plugin._materialise_pool(
            's.1', _pool_cfg(name='keep', endpoint_name='endpoint0'))

        _REGISTRY.pop('dummy')
        replayed = self._make_plugin(tmp_path)
        assert set(replayed._pool_states.get('s.1', {})) == {'keep'}
