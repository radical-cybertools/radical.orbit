#!/usr/bin/env python

# pylint: disable=protected-access,unused-import

import pytest

from fastapi import FastAPI

from radical.edge.plugin_base      import Plugin
from radical.edge.plugin_host_base import (
    PluginHostBase,
    _resolve_plugin_names,
    _discover_entry_points,
    _expand_special_tokens,
    DEFAULT_PLUGINS_BY_ROLE,
)


# ---------------------------------------------------------------------------
# _resolve_plugin_names
# ---------------------------------------------------------------------------

class TestResolvePluginNames:

    def test_exact_match(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['psij'], available) == ['psij']

    def test_prefix_match(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['sys'], available) == ['sysinfo']

    def test_multiple(self):
        available = ['sysinfo', 'psij', 'queue_info']
        result = _resolve_plugin_names(['sys', 'psij'], available)
        assert result == ['sysinfo', 'psij']

    def test_no_match(self):
        available = ['sysinfo', 'psij']
        with pytest.raises(ValueError, match="No plugin matches 'foo'"):
            _resolve_plugin_names(['foo'], available)

    def test_ambiguous(self):
        available = ['iri_connect', 'iri_instance', 'psij']
        # Prefix 'ir' is ambiguous (matches iri, iri_info)
        with pytest.raises(ValueError, match="Ambiguous"):
            _resolve_plugin_names(['ir'], available)

    def test_exact_match_priority_over_prefix(self):
        """Exact match wins even when it is also a prefix of another name."""
        available = ['iri_connect', 'iri_connect_v2', 'psij']
        assert _resolve_plugin_names(['iri_connect'], available) == ['iri_connect']

    def test_wildcard(self):
        """fnmatch-style wildcard glob picks every match."""
        available = ['iri_connect', 'iri_info', 'psij', 'sysinfo']
        assert _resolve_plugin_names(['iri*'], available) \
            == ['iri_connect', 'iri_info']

    def test_wildcard_no_match(self):
        available = ['psij', 'sysinfo']
        with pytest.raises(ValueError, match="No plugin matches pattern 'iri\\*'"):
            _resolve_plugin_names(['iri*'], available)

    def test_dedupes_preserving_order(self):
        """Same plugin requested twice (e.g. via overlapping wildcards) -> once."""
        available = ['iri_connect', 'iri_info', 'psij']
        # 'iri*' and 'iri_connect' both pull in iri_connect
        result = _resolve_plugin_names(['iri*', 'iri_connect'], available)
        assert result == ['iri_connect', 'iri_info']


# ---------------------------------------------------------------------------
# _expand_special_tokens — 'all' / 'default' expansion
# ---------------------------------------------------------------------------

class TestExpandSpecialTokens:

    def _app(self, *, is_bridge=False):
        app = FastAPI()
        app.state.is_bridge = is_bridge
        return app

    def test_all_expands_to_full_registry(self):
        app = self._app()
        out = _expand_special_tokens(['all'], app, ['psij', 'sysinfo'])
        assert out == ['psij', 'sysinfo']

    def test_default_for_bridge(self, monkeypatch):
        app = self._app(is_bridge=True)
        # bridge default is ['iri*', 'staging', 'sysinfo']
        out = _expand_special_tokens(['default'], app,
                                     ['iri_connect', 'staging', 'sysinfo', 'psij'])
        assert out == ['iri_connect', 'staging', 'sysinfo']

    def test_default_skips_uninstalled_plugins(self, monkeypatch):
        """A plugin in the role default but not in available is silently skipped."""
        for v in ('SLURM_JOB_ID', 'PBS_JOBID'):
            monkeypatch.delenv(v, raising=False)
        from unittest.mock import patch
        app = self._app()
        # Force 'standalone' role: no scheduler.  Default = psij/staging/sysinfo/rhapsody.
        # If rhapsody isn't installed, the default expansion just drops it.
        with patch('shutil.which', return_value=None):
            out = _expand_special_tokens(['default'], app,
                                         ['psij', 'staging', 'sysinfo'])
        assert out == ['psij', 'staging', 'sysinfo']

    def test_default_plus_extra(self, monkeypatch):
        """``-p default,rose`` -> default set + 'rose'."""
        from unittest.mock import patch
        for v in ('SLURM_JOB_ID', 'PBS_JOBID'):
            monkeypatch.delenv(v, raising=False)
        app = self._app()
        with patch('shutil.which', return_value=None):
            out = _expand_special_tokens(['default', 'rose'], app,
                                         ['psij', 'staging', 'sysinfo',
                                          'rhapsody', 'rose'])
        assert out == ['psij', 'staging', 'sysinfo', 'rhapsody', 'rose']

    def test_passthrough_for_other_tokens(self):
        """Non-special tokens come back unchanged for later resolution."""
        app = self._app()
        out = _expand_special_tokens(['psij', 'sys'], app, ['psij', 'sysinfo'])
        assert out == ['psij', 'sys']

    def test_default_role_table_complete(self):
        """Sanity: every advertised role has a default set."""
        for role in ('bridge', 'login', 'compute', 'standalone'):
            assert role in DEFAULT_PLUGINS_BY_ROLE


# ---------------------------------------------------------------------------
# _discover_entry_points  (smoke test — no real entry points in test env)
# ---------------------------------------------------------------------------

def test_discover_entry_points_smoke():
    """Should not raise even when no entry points are installed."""
    _discover_entry_points()


# ---------------------------------------------------------------------------
# PluginHostBase — concrete test subclass
# ---------------------------------------------------------------------------

class _TestHost(PluginHostBase):
    """Concrete subclass for testing the mixin."""

    def __init__(self, app: FastAPI):
        self._app               = app
        self._plugins           = {}
        self._announce_called   = 0
        self._app.state.is_bridge = False

    async def _announce_topology(self):
        self._announce_called += 1


class _DummySession:
    """Minimal stand-in for PluginSession."""

    def __init__(self, sid):
        self._sid    = sid
        self.closed  = False

    async def close(self):
        self.closed = True
        return {}


class _DummyPlugin(Plugin):
    plugin_name   = '_test_dummy'
    session_class = None

    def __init__(self, app, instance_name='_test_dummy', **kwargs):
        super().__init__(app, instance_name)
        self._extra = kwargs


# Ensure _test_dummy is in the registry for each test, clean up after
@pytest.fixture(autouse=True)
def _cleanup_registry():
    Plugin._registry['_test_dummy'] = _DummyPlugin
    yield
    Plugin._registry.pop('_test_dummy', None)


# ---------------------------------------------------------------------------
# _load_plugins_from_filter
# ---------------------------------------------------------------------------

def test_load_plugins_from_filter():
    app  = FastAPI()
    host = _TestHost(app)
    host._load_plugins_from_filter(['_test_dummy'])
    assert '_test_dummy' in host._plugins
    assert isinstance(host._plugins['_test_dummy'], _DummyPlugin)


def test_load_plugins_from_filter_skip_disabled():
    app  = FastAPI()
    host = _TestHost(app)

    original = _DummyPlugin.is_enabled

    @classmethod
    def _disabled(cls, a):
        return False

    _DummyPlugin.is_enabled = _disabled
    try:
        host._load_plugins_from_filter(['_test_dummy'])
        assert '_test_dummy' not in host._plugins
    finally:
        _DummyPlugin.is_enabled = original


# ---------------------------------------------------------------------------
# register_dynamic_plugin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_dynamic_plugin():
    app  = FastAPI()
    host = _TestHost(app)

    plugin = await host.register_dynamic_plugin(
        _DummyPlugin, 'dummy.one', color='red')

    assert 'dummy.one'           in host._plugins
    assert host._plugins['dummy.one'] is plugin
    assert plugin.instance_name  == 'dummy.one'
    assert plugin._extra         == {'color': 'red'}
    assert host._announce_called == 1


@pytest.mark.asyncio
async def test_register_dynamic_plugin_duplicate_rejected():
    app  = FastAPI()
    host = _TestHost(app)

    await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')

    with pytest.raises(ValueError, match="already registered"):
        await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')

    assert host._announce_called == 1  # only the first succeeded


# ---------------------------------------------------------------------------
# deregister_dynamic_plugin
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deregister_dynamic_plugin():
    app  = FastAPI()
    host = _TestHost(app)

    plugin = await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')
    assert host._announce_called == 1

    # Inject a mock session to verify cleanup
    sess = _DummySession('s1')
    plugin._sessions['s1'] = sess

    await host.deregister_dynamic_plugin('dummy.one')

    assert 'dummy.one'          not in host._plugins
    assert sess.closed           is True
    assert host._announce_called == 2


@pytest.mark.asyncio
async def test_deregister_unknown_is_noop():
    app  = FastAPI()
    host = _TestHost(app)

    await host.deregister_dynamic_plugin('nonexistent')
    assert host._announce_called == 0


@pytest.mark.asyncio
async def test_deregister_strips_direct_routes():
    """deregister must remove the plugin's entries from app.state.direct_routes.

    Regression test: if stale routes from a deregistered instance are left
    in the table, a subsequent register_dynamic_plugin under the same name
    would leave the dead instance's routes ahead of the new ones in match
    order, and requests would dispatch onto an object whose ``_sessions``
    has been emptied.
    """
    app  = FastAPI()
    host = _TestHost(app)

    p1 = await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')
    p1.add_route_get('probe', lambda req: {'ok': 1})
    routes_after_register = list(app.state.direct_routes)
    assert any(entry[3].__self__ is p1 for entry in routes_after_register), \
        "test setup: expected at least one route bound to the new plugin"

    await host.deregister_dynamic_plugin('dummy.one')

    # No surviving route should be bound to the deregistered instance.
    assert not any(entry[3].__self__ is p1
                   for entry in app.state.direct_routes
                   if hasattr(entry[3], '__self__')), \
        "stale routes for deregistered plugin remained in direct_routes"


@pytest.mark.asyncio
async def test_reregister_replaces_routes_cleanly():
    """register → deregister → re-register: requests hit the NEW instance."""
    app  = FastAPI()
    host = _TestHost(app)

    p1 = await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')
    p1.add_route_get('probe', lambda req: {'ok': 1})
    await host.deregister_dynamic_plugin('dummy.one')

    p2 = await host.register_dynamic_plugin(_DummyPlugin, 'dummy.one')
    p2.add_route_get('probe', lambda req: {'ok': 2})

    # The first matching 'probe' route in direct_routes must belong to p2.
    for entry in app.state.direct_routes:
        method, pattern, _, handler = entry
        if method == 'GET' and pattern.match('/dummy.one/probe'):
            assert getattr(handler, '__self__', None) is not p1, \
                "stale route from p1 still ahead of p2 in match order"
            # First match wins; p2's must be it.
            assert handler({}) == {'ok': 2}
            break
    else:
        pytest.fail("no route matched /dummy.one/probe after re-register")


# ---------------------------------------------------------------------------
# _announce_topology is abstract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_announce_topology_abstract():
    """Base class raises NotImplementedError."""

    class _Bare(PluginHostBase):
        pass

    bare       = _Bare()
    bare._app     = FastAPI()
    bare._plugins = {}

    with pytest.raises(NotImplementedError):
        await bare._announce_topology()
