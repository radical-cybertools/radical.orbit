#!/usr/bin/env python

# pylint: disable=protected-access,unused-import

import inspect

import pytest

from fastapi import FastAPI

from radical.orbit.plugin_base      import Plugin
from radical.orbit.plugin_host_base import (
    PluginHostBase,
    _resolve_plugin_names,
    _discover_entry_points,
    _expand_special_tokens,
    default_plugins,
)


def _bundled(name):
    """Whether *name* is registered to its bundled ``radical.orbit`` class."""
    cls = Plugin.get_plugin_class(name)
    return cls is not None and cls.__module__ == f'radical.orbit.plugin_{name}'


# ---------------------------------------------------------------------------
# _resolve_plugin_names
# ---------------------------------------------------------------------------

class TestResolvePluginNames:

    def test_exact_match(self):
        available = ['sysinfo', 'psij', 'queue_info']
        assert _resolve_plugin_names(['psij'], available) == ['psij']

    def test_multiple(self):
        available = ['sysinfo', 'psij', 'queue_info']
        result = _resolve_plugin_names(['sysinfo', 'psij'], available)
        assert result == ['sysinfo', 'psij']

    def test_no_match(self):
        available = ['sysinfo', 'psij']
        with pytest.raises(ValueError, match="No plugin matches 'foo'"):
            _resolve_plugin_names(['foo'], available)

    def test_prefix_no_longer_matches(self):
        """The ambiguous prefix-match form was dropped: a partial name is an
        error, not a resolution."""
        available = ['sysinfo', 'psij', 'queue_info']
        with pytest.raises(ValueError, match="No plugin matches 'sys'"):
            _resolve_plugin_names(['sys'], available)

    def test_exact_match_only(self):
        """An exact name resolves even when it is a prefix of another name."""
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

    def _app(self, *, is_broker=False):
        app = FastAPI()
        app.state.is_broker = is_broker
        return app

    def test_all_expands_to_full_registry(self):
        app = self._app()
        out = _expand_special_tokens(['all'], app, ['psij', 'sysinfo'])
        assert out == ['psij', 'sysinfo']

    @staticmethod
    def _isolate(monkeypatch):
        """Restrict the registry to the bundled plugin classes, so a test
        does not depend on plugins other tests registered."""
        import radical.orbit  # noqa: F401  (registers the bundled plugins)
        bundled = {n: c for n, c in Plugin._registry.items() if _bundled(n)}
        monkeypatch.setattr(Plugin, '_registry', bundled)
        return bundled

    def _expect(self, names):
        """*names* minus optional plugins that are not registered (their
        dependency is absent)."""
        return [n for n in names
                if n not in self._OPTIONAL or _bundled(n)]

    def test_default_for_broker(self, monkeypatch):
        self._isolate(monkeypatch)
        app = self._app(is_broker=True)
        # psij does not declare 'broker'; the other three do
        out = _expand_special_tokens(['default'], app,
                                     ['iri_connect', 'staging', 'sysinfo', 'psij'])
        assert out == ['iri_connect', 'staging', 'sysinfo']

    def test_default_skips_uninstalled_plugins(self, monkeypatch):
        """A plugin in the role default but not in available is silently skipped."""
        self._isolate(monkeypatch)
        for v in ('SLURM_JOB_ID', 'PBS_JOBID'):
            monkeypatch.delenv(v, raising=False)
        from unittest.mock import patch
        app = self._app()
        # Force 'standalone' role: no scheduler.  rhapsody, queue_info and
        # globus declare it too but are not available, so they are dropped.
        with patch('shutil.which', return_value=None):
            out = _expand_special_tokens(['default'], app,
                                         ['psij', 'staging', 'sysinfo'])
        assert out == self._expect(['psij', 'staging', 'sysinfo'])

    def test_default_plus_extra(self, monkeypatch):
        """``-p default,rose`` -> default set + 'rose'."""
        self._isolate(monkeypatch)
        from unittest.mock import patch
        for v in ('SLURM_JOB_ID', 'PBS_JOBID'):
            monkeypatch.delenv(v, raising=False)
        app = self._app()
        with patch('shutil.which', return_value=None):
            out = _expand_special_tokens(['default', 'rose'], app,
                                         ['psij', 'staging', 'sysinfo',
                                          'rhapsody', 'rose'])
        assert out == self._expect(['psij', 'staging', 'sysinfo',
                                    'rhapsody']) + ['rose']

    def test_passthrough_for_other_tokens(self):
        """Non-special tokens come back unchanged for later resolution."""
        app = self._app()
        out = _expand_special_tokens(['psij', 'sys'], app, ['psij', 'sysinfo'])
        assert out == ['psij', 'sys']

    # The ``default`` set per role, pinned: plugins declare their roles via
    # ``Plugin.default_roles``; this guards against an accidental change.
    # Optional plugins (their dependency may be absent) are expected exactly
    # when they are registered.
    _OPTIONAL = {'psij'    : ('login', 'standalone'),
                 'rhapsody': ('compute', 'standalone'),
                 'globus'  : ('login', 'compute', 'standalone')}

    @pytest.mark.parametrize('role, core', [
        ('broker',     {'iri_connect', 'sfapi_connect', 'staging', 'sysinfo',
                        'task_dispatcher', 'federation'}),
        ('login',      {'staging', 'sysinfo', 'queue_info'}),
        ('compute',    {'staging', 'sysinfo', 'queue_info'}),
        ('standalone', {'staging', 'sysinfo', 'queue_info'}),
    ])
    def test_default_set_per_role(self, monkeypatch, role, core):
        """``default`` expands to exactly the bundled plugins declaring
        the host role."""
        from unittest.mock import patch
        bundled = self._isolate(monkeypatch)
        expected = core | {n for n, roles in self._OPTIONAL.items()
                           if role in roles and n in bundled}
        assert core <= set(bundled)
        with patch('radical.orbit.utils.host_role',
                   return_value={'role': role}):
            out = _expand_special_tokens(['default'], self._app(),
                                         list(bundled))
        assert set(out) == expected

    def test_default_roles_not_inherited(self, monkeypatch):
        """A subclass registered under a new name does not inherit its
        parent's default roles; it declares its own."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))

        class _Parent(Plugin):
            plugin_name   = '_test_parent'
            default_roles = ('compute',)

        class _Child(_Parent):
            plugin_name   = '_test_child'

        assert _Parent.default_roles == ('compute',)
        assert _Child.default_roles  == ()
        assert default_plugins('compute', ['_test_parent', '_test_child']) \
            == ['_test_parent']

    def test_default_roles_inherited_from_abstract_base(self, monkeypatch):
        """A plugin inherits roles declared on an abstract base that is
        not itself a registered plugin, and its own declaration wins."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))

        class _Base(Plugin):
            default_roles = ('compute',)

        class _Impl(_Base):
            plugin_name   = '_test_impl'

        class _Own(_Base):
            plugin_name   = '_test_own'
            default_roles = ('login',)

        assert '_Base' not in [c.__name__ for c in Plugin._registry.values()]
        assert _Impl.default_roles == ('compute',)
        assert _Own.default_roles  == ('login',)

    def test_default_roles_kept_under_parent_name(self, monkeypatch):
        """A subclass that keeps the parent's ``plugin_name`` (and so
        replaces it in the registry) keeps the parent's roles, unless it
        declares its own."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))

        class _Parent(Plugin):
            plugin_name   = '_test_same'
            default_roles = ('compute',)

        class _Keep(_Parent):
            pass

        assert _Keep.default_roles == ('compute',)
        assert default_plugins('compute', ['_test_same']) == ['_test_same']

        class _Restated(_Parent):
            plugin_name   = '_test_same'

        assert _Restated.default_roles == ('compute',)

        class _Own(_Parent):
            default_roles = ('login',)

        assert _Own.default_roles == ('login',)
        assert default_plugins('compute', ['_test_same']) == []

    def test_default_roles_chain_new_name(self, monkeypatch):
        """A <- B (A's name, inherited) <- C (new name): C gets no roles."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))

        class _A(Plugin):
            plugin_name   = '_test_a'
            default_roles = ('compute',)

        class _B(_A):
            pass

        class _C(_B):
            plugin_name   = '_test_c'

        assert _B.default_roles == ('compute',)
        assert _C.default_roles == ()

    @pytest.mark.parametrize('roles, exc', [
        ('compute',            TypeError),     # bare str would substring-match
        (('compute', 'gpu'),   ValueError),    # unknown role
        ((['compute'],),       TypeError),     # unhashable element
    ])
    def test_default_roles_validated(self, monkeypatch, roles, exc):
        """Invalid ``default_roles`` raise at class creation, unregistered."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))
        with pytest.raises(exc, match='_Bad.default_roles'):
            type('_Bad', (Plugin,), {'plugin_name'  : '_test_bad',
                                     'default_roles': roles})
        assert '_test_bad' not in Plugin._registry

    def test_default_roles_validated_when_inherited(self, monkeypatch):
        """Roles inherited from a non-Plugin mixin are validated too."""
        monkeypatch.setattr(Plugin, '_registry', dict(Plugin._registry))

        class _Mixin:
            default_roles = 'compute'

        with pytest.raises(TypeError, match='_BadMix.default_roles'):
            type('_BadMix', (_Mixin, Plugin),
                 {'plugin_name': '_test_badmix'})
        assert '_test_badmix' not in Plugin._registry

    def test_default_includes_declaring_external_plugin(self, monkeypatch):
        """An entry-point plugin joins a role's defaults by declaring it."""
        from unittest.mock import patch

        class _Ext:
            default_roles = ('compute',)

        monkeypatch.setitem(Plugin._registry, 'ext', _Ext)
        for role, expected in (('compute', ['ext']), ('login', [])):
            with patch('radical.orbit.utils.host_role',
                       return_value={'role': role}):
                assert _expand_special_tokens(['default'], self._app(),
                                              ['ext']) == expected


# ---------------------------------------------------------------------------
# _discover_entry_points  (smoke test — no real entry points in test env)
# ---------------------------------------------------------------------------

def test_discover_entry_points_smoke():
    """Should not raise even when no entry points are installed."""
    _discover_entry_points()


# ---------------------------------------------------------------------------
# PluginHostBase — concrete test subclass
# ---------------------------------------------------------------------------

def _bound(handler):
    """The plugin a (possibly session-wrapped) route handler is bound to."""
    return getattr(inspect.unwrap(handler), '__self__', None)


class _TestHost(PluginHostBase):
    """Concrete subclass for testing the mixin."""

    def __init__(self, app: FastAPI):
        self._app               = app
        self._plugins           = {}
        self._announce_called   = 0
        self._app.state.is_broker = False

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
    assert any(_bound(entry[3]) is p1 for entry in routes_after_register), \
        "test setup: expected at least one route bound to the new plugin"

    await host.deregister_dynamic_plugin('dummy.one')

    # No surviving route should be bound to the deregistered instance.
    assert not any(_bound(entry[3]) is p1
                   for entry in app.state.direct_routes), \
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
            assert _bound(handler) is not p1, \
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
