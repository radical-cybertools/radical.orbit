#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import radical.orbit
import radical.orbit
from radical.orbit.plugin_base import Plugin
from radical.orbit.plugin_session_base import PluginSession

from fastapi import FastAPI, HTTPException
from starlette.routing import Route
from starlette.requests import Request
from unittest.mock import Mock
import asyncio
import uuid
import time
import pytest


def test_plugin_init_subclass_collision(caplog):
    '''
    Test that duplicate plugin names emit a warning during subclassing.
    '''
    import logging
    # The 'radical.orbit' logger runs with propagate=False (see logging_config),
    # so caplog's root handler never sees its records.  Attach caplog's handler
    # to the logger directly to capture regardless of propagation.
    logger = logging.getLogger("radical.orbit")
    logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger="radical.orbit"):
            class CollidingPluginA(Plugin):
                plugin_name = "collide"
                session_class = PluginSession

            class CollidingPluginB(Plugin):
                plugin_name = "collide"
                session_class = PluginSession
    finally:
        logger.removeHandler(caplog.handler)

    assert "Duplicate plugin_name 'collide' - overwriting" in caplog.text


def test_plugin_initialization():
    '''
    Test that Plugin initializes correctly with app and name.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.instance_name == "test_plugin"
    assert isinstance(plugin._uid, str)
    # Verify it's a valid UUID
    assert uuid.UUID(plugin._uid)
    assert plugin._namespace == f"/{plugin.instance_name}"


def test_plugin_uid_property():
    '''
    Test that the uid property returns the correct UUID.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    assert plugin.uid == plugin._uid
    assert isinstance(plugin.uid, str)
    # Verify it's a valid UUID
    uuid.UUID(plugin.uid)


def test_plugin_namespace_property():
    '''
    Test that the namespace property returns the correct namespace.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    expected_namespace = "/test_plugin"
    assert plugin.namespace == expected_namespace


def test_plugin_add_route_post():
    '''
    Test adding a POST route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_count = len(app.state.direct_routes)
    plugin.add_route_post("/test", test_handler)

    # Verify a new direct-dispatch route was added
    assert len(app.state.direct_routes) == initial_count + 1

    # Check the last added route
    method, pattern, param_names, handler = app.state.direct_routes[-1]
    assert method == "POST"
    assert pattern.match(f"{plugin.namespace}/test")
    assert handler is test_handler


def test_plugin_add_route_get():
    '''
    Test adding a GET route to the plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    initial_count = len(app.state.direct_routes)
    plugin.add_route_get("/test", test_handler)

    # Verify a new direct-dispatch route was added
    assert len(app.state.direct_routes) == initial_count + 1

    # Check the last added route
    method, pattern, param_names, handler = app.state.direct_routes[-1]
    assert method == "GET"
    assert pattern.match(f"{plugin.namespace}/test")
    assert handler is test_handler


def test_plugin_route_path_normalization():
    '''
    Test that double slashes in paths are normalized.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def test_handler():
        return {"status": "ok"}

    # Add route with leading slash
    plugin.add_route_post("/test", test_handler)
    _, pattern1, _, _ = app.state.direct_routes[-1]

    # Verify no double slashes in the regex pattern source
    assert "//" not in pattern1.pattern

    # Add route without leading slash
    plugin.add_route_get("test2", test_handler)
    _, pattern2, _, _ = app.state.direct_routes[-1]

    # Verify no double slashes
    assert "//" not in pattern2.pattern


def test_plugin_multiple_routes():
    '''
    Test adding multiple routes to the same plugin.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    async def handler1():
        return {"endpoint": "1"}

    async def handler2():
        return {"endpoint": "2"}

    async def handler3():
        return {"endpoint": "3"}

    initial_count = len(app.state.direct_routes)

    plugin.add_route_post("/endpoint1", handler1)
    plugin.add_route_get("/endpoint2", handler2)
    plugin.add_route_post("/endpoint3", handler3)

    # Verify all routes were added
    assert len(app.state.direct_routes) == initial_count + 3

    # Verify all routes match the plugin namespace
    for _, pattern, _, _ in app.state.direct_routes[-3:]:
        assert plugin.namespace.lstrip('/') in pattern.pattern


@pytest.mark.asyncio
async def test_plugin_session_management():
    '''
    Test base plugin session management.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession  # required — no fallback

    # Mock request for registration
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']
    assert sid in plugin._sessions
    assert isinstance(plugin._sessions[sid], PluginSession)

    # Test unregister
    request.path_params = {"sid": sid}
    await plugin.unregister_session(request)
    assert sid not in plugin._sessions


def test_plugin_unique_uids():
    '''
    Test that each plugin instance gets a unique UID.
    '''
    app = FastAPI()
    plugin1 = Plugin(app, "test_plugin")
    plugin2 = Plugin(app, "test_plugin")
    plugin3 = Plugin(app, "another_plugin")

    # All UIDs should be different
    assert plugin1.uid != plugin2.uid
    assert plugin1.uid != plugin3.uid
    assert plugin2.uid != plugin3.uid

    # Namespaces will be the same if names are the same
    assert plugin1.namespace == "/test_plugin"
    assert plugin2.namespace == "/test_plugin"
    assert plugin3.namespace == "/another_plugin"


@pytest.mark.asyncio
async def test_plugin_health_check():
    '''
    Test the health check endpoint.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Register a session first
    request = Mock(spec=Request)
    await plugin.register_session(request)

    # Call health check
    data = await plugin.health_check(request)

    assert data['status'] == 'healthy'
    assert data['plugin'] == 'test_plugin'
    assert data['active_sessions'] == 1
    assert 'uptime_seconds' in data


@pytest.mark.asyncio
async def test_plugin_session_ttl_expiration():
    '''
    Test that sessions expire after TTL.
    '''
    import time
    from fastapi import HTTPException

    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 1  # 1 second TTL

    # Register session
    request = Mock(spec=Request)
    data = await plugin.register_session(request)
    sid = data['sid']

    # Session should be accessible immediately
    assert sid in plugin._sessions

    # Wait for TTL to expire
    time.sleep(1.5)

    # Session should be expired now — any forwarded call returns 410
    with pytest.raises(HTTPException) as exc_info:
        await plugin._forward(sid, PluginSession.close)
    assert exc_info.value.status_code == 410  # Gone


@pytest.mark.asyncio
async def test_plugin_session_cleanup():
    '''
    Test cleanup of expired sessions.
    '''
    import time

    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 1

    # Create some sessions manually
    plugin._sessions["old_session"] = PluginSession("old_session")
    plugin._session_last_access["old_session"] = time.time() - 100  # Expired

    plugin._sessions["new_session"] = PluginSession("new_session")
    plugin._session_last_access["new_session"] = time.time()  # Fresh

    # Run cleanup
    cleaned = await plugin._cleanup_expired_sessions()

    assert cleaned == 1
    assert "old_session" not in plugin._sessions
    assert "new_session" in plugin._sessions


@pytest.mark.asyncio
async def test_cleanup_loop_survives_sweep_error(monkeypatch):
    '''A raising sweep must not kill the cleanup loop — it logs and keeps
    running, so later expired sessions are still reclaimed.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    calls = {'n': 0}

    async def _sweep():
        calls['n'] += 1
        if calls['n'] == 1:
            raise RuntimeError("boom")     # first sweep blows up
        return 0

    monkeypatch.setattr(plugin, '_cleanup_expired_sessions', _sweep)

    # Collapse the loop's 5s cadence so the test does not sleep for real.
    real_sleep = asyncio.sleep

    async def _fast_sleep(_seconds):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, 'sleep', _fast_sleep)

    task = asyncio.ensure_future(plugin._cleanup_loop())
    for _ in range(50):
        await real_sleep(0)
        if calls['n'] >= 3:
            break
    task.cancel()
    try:    await task
    except asyncio.CancelledError:
        pass

    assert calls['n'] >= 3   # survived the first-sweep RuntimeError


# ---------------------------------------------------------------------------
# Session policy: create-or-reconnect, lifetime validation, expiry, default
# ---------------------------------------------------------------------------

def _body_request(payload):
    '''Build a minimal Request-like mock whose .json() returns `payload`.'''
    async def _json():
        return payload
    request = Mock(spec=Request)
    request.json = _json
    return request


@pytest.mark.asyncio
async def test_session_create_or_reconnect():
    '''A client-supplied sid that exists reconnects (same sid, last_access
    bumped); a fresh sid creates under exactly that id.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Explicit-sid create
    data = await plugin.register_session(_body_request({"sid": "s1"}))
    assert data['sid'] == "s1"
    assert "s1" in plugin._sessions
    obj = plugin._sessions["s1"]

    # Reconnect: same sid returned, last_access bumped, session not rebuilt
    plugin._session_last_access["s1"] = 0.0
    data = await plugin.register_session(_body_request({"sid": "s1"}))
    assert data['sid'] == "s1"
    assert plugin._sessions["s1"] is obj              # not rebuilt
    assert plugin._session_last_access["s1"] > 0.0    # bumped


@pytest.mark.asyncio
async def test_session_mint_when_sid_omitted():
    '''sid=None mints a fresh session id.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    data = await plugin.register_session(_body_request({}))
    assert data['sid'].startswith("session.")
    assert data['sid'] in plugin._sessions


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [
    {"lifetime": "ttl"},                       # ttl lifetime, no ttl
    {"lifetime": "ttl", "ttl": 0},             # ttl not > 0
    {"lifetime": "ttl", "ttl": -5},            # ttl not > 0
    {"lifetime": "ttl", "ttl": float('nan')},  # NaN ttl (JSON NaN) — not finite
    {"lifetime": "ttl", "ttl": float('inf')},  # non-finite ttl
    {"lifetime": "persistent", "ttl": 10},     # ttl with non-ttl lifetime
    {"lifetime": "ephemeral", "ttl": 10},      # ttl with non-ttl lifetime
    {"lifetime": "forever"},                   # unknown lifetime
])
async def test_session_incoherent_policy_409(payload):
    '''Incoherent lifetime/ttl pairs and unknown lifetimes map to HTTP 409.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    with pytest.raises(HTTPException) as exc_info:
        await plugin.register_session(_body_request(payload))
    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_session_reconnect_policy_conflict_409():
    '''Reconnecting to a sid with a different lifetime/ttl is a 409.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "ttl", "ttl": 10}))

    # Different lifetime
    with pytest.raises(HTTPException) as exc_info:
        await plugin.register_session(
            _body_request({"sid": "s1", "lifetime": "persistent"}))
    assert exc_info.value.status_code == 409

    # Different ttl
    with pytest.raises(HTTPException) as exc_info:
        await plugin.register_session(
            _body_request({"sid": "s1", "lifetime": "ttl", "ttl": 20}))
    assert exc_info.value.status_code == 409

    # Same policy reconnects cleanly
    data = await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "ttl", "ttl": 10}))
    assert data['sid'] == "s1"


@pytest.mark.asyncio
async def test_session_ttl_expiry_via_cleanup():
    '''A 'ttl' session expires once now-last_access > ttl (time-driven).'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 0  # ensure ephemeral idle-timeout does not interfere

    await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "ttl", "ttl": 0.5}))
    assert "s1" in plugin._sessions

    # Not yet expired
    assert await plugin._cleanup_expired_sessions() == 0
    assert "s1" in plugin._sessions

    # Backdate last_access past the ttl and clean up (no real sleep)
    plugin._session_last_access["s1"] = time.time() - 5
    cleaned = await plugin._cleanup_expired_sessions()
    assert cleaned == 1
    assert "s1" not in plugin._sessions
    assert "s1" not in plugin._records


@pytest.mark.asyncio
async def test_session_persistent_never_expires():
    '''A 'persistent' session is never reclaimed by the cleanup pass.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    plugin.session_ttl = 1

    await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "persistent"}))

    # Backdate far past any idle/ttl horizon — persistent stays put
    plugin._session_last_access["s1"] = time.time() - 10_000
    assert await plugin._cleanup_expired_sessions() == 0
    assert "s1" in plugin._sessions


@pytest.mark.asyncio
async def test_session_default_auto_create_under_concurrency():
    '''Two concurrent first requests for 'default' create exactly one session.'''
    import asyncio

    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    results = await asyncio.gather(
        plugin.register_session(_body_request({"sid": "default"})),
        plugin.register_session(_body_request({"sid": "default"})),
    )
    assert all(r['sid'] == "default" for r in results)
    assert list(plugin._sessions.keys()) == ["default"]


@pytest.mark.asyncio
async def test_session_default_forced_persistent():
    '''The reserved 'default' session is always persistent.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Bare register (lifetime defaults to 'ephemeral') resolves to persistent
    data = await plugin.register_session(_body_request({"sid": "default"}))
    assert data['sid'] == "default"
    assert plugin._records["default"].lifetime == "persistent"

    # An explicitly conflicting lifetime for 'default' is a 409
    with pytest.raises(HTTPException) as exc_info:
        await plugin.register_session(
            _body_request({"sid": "default", "lifetime": "ephemeral"}))
    assert exc_info.value.status_code == 409

    # Explicit 'persistent' is accepted (reconnect)
    data = await plugin.register_session(
        _body_request({"sid": "default", "lifetime": "persistent"}))
    assert data['sid'] == "default"


@pytest.mark.asyncio
async def test_session_default_auto_create_via_forward():
    '''An unregistered '_forward' to 'default' auto-creates it (persistent).'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    assert "default" not in plugin._sessions
    await plugin._forward("default", PluginSession.close)
    assert "default" in plugin._sessions
    assert plugin._records["default"].lifetime == "persistent"


def test_plugin_session_ttl_default():
    '''
    Test that session_ttl has a sensible default.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    # Default should be 3600 (1 hour)
    assert plugin.session_ttl == 3600


@pytest.mark.asyncio
async def test_plugin_ui_config_endpoint():
    '''
    Test the ui_config endpoint.
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    # Call ui_config endpoint
    request = Mock(spec=Request)
    data = await plugin.get_ui_config(request)

    # Verify response structure
    assert 'plugin_name' in data
    assert 'instance_name' in data
    assert 'version' in data
    assert 'ui' in data
    assert data['instance_name'] == 'test_plugin'


@pytest.mark.asyncio
async def test_plugin_ui_config_with_custom_config():
    '''
    Test ui_config endpoint with a custom ui_config.
    '''

    class CustomPlugin(Plugin):
        plugin_name = "custom"
        session_class = PluginSession
        ui_config = {
            "icon": "🔧",
            "title": "Custom Plugin",
            "description": "A custom plugin"
        }

    app = FastAPI()
    plugin = CustomPlugin(app, "custom")

    request = Mock(spec=Request)
    data = await plugin.get_ui_config(request)

    assert data['plugin_name'] == 'custom'
    assert data['ui']['icon'] == '🔧'
    assert data['ui']['title'] == 'Custom Plugin'


def test_plugin_ui_config_default():
    '''
    Test that ui_config has a sensible default (None).
    '''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")

    # Default should be None
    assert plugin.ui_config is None


# ---------------------------------------------------------------------------
# Session ownership + owner-checked reattach (M6)
# ---------------------------------------------------------------------------

def _owned_request(payload, owner=None):
    '''Request mock whose headers carry the trusted x-orbit-src owner
    (the header the serving runtime injects from the broker-stamped src).'''
    async def _json():
        return payload
    request = Mock(spec=Request)
    request.json    = _json
    request.headers = {} if owner is None else {'x-orbit-src': owner}
    return request


def _fresh_plugin():
    app    = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession
    return plugin


@pytest.mark.asyncio
async def test_session_owner_recorded_from_header():
    '''register_session records owner from the x-orbit-src header at create.'''
    plugin = _fresh_plugin()
    data = await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    assert data['sid'] == "s1"
    assert plugin._records["s1"].owner == "epB"


@pytest.mark.asyncio
async def test_session_owner_none_without_header():
    '''No x-orbit-src header (old stack / gateway) -> owner None.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}))
    assert plugin._records["s1"].owner is None


@pytest.mark.asyncio
async def test_session_same_owner_reattach_ok():
    '''Reattach by the same owner re-states policy and succeeds.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    data = await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    assert data['sid'] == "s1"


@pytest.mark.asyncio
async def test_session_different_owner_reattach_403():
    '''Reattach by a different owner (or none) to a bound session -> 403.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))

    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(_owned_request({"sid": "s1"}, "attacker"))
    assert exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(_owned_request({"sid": "s1"}))  # no owner
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_session_ownerless_reattach_unaffected():
    '''An owner-less session accepts any reattach and stays owner-less.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}))          # None
    data = await plugin.register_session(_owned_request({"sid": "s1"}, "x"))
    assert data['sid'] == "s1"
    assert plugin._records["s1"].owner is None


@pytest.mark.asyncio
async def test_default_session_owner_none_even_with_header():
    '''The shared reserved default session always records owner None.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "default"}, "epB"))
    assert plugin._records["default"].owner is None


# ---------------------------------------------------------------------------
# Owner-liveness driven ephemeral reclaim: suspect never arms the drain, lost
# stamps a drain deadline the sweep enforces, owner return clears it,
# ttl/persistent survive.
# ---------------------------------------------------------------------------

def _drain_deadline(plugin, sid):
    return plugin._records[sid].drain_deadline


@pytest.mark.asyncio
async def test_suspect_does_not_arm_drain():
    '''A blip reaching 'suspect' must NOT stamp a reclaim-drain deadline.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    await plugin.on_topology_change({"epB": {"liveness": "suspect"}})
    assert _drain_deadline(plugin, "s1") is None        # nothing armed
    assert await plugin._cleanup_expired_sessions() == 0
    assert "s1" in plugin._sessions                     # session survives


@pytest.mark.asyncio
async def test_lost_arms_drain_and_reclaims_ephemeral():
    '''An owner declared 'lost' stamps a drain deadline; once it passes, the
    sweep reclaims the ephemeral session; a persistent one survives.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    await plugin.register_session(
        _owned_request({"sid": "p1", "lifetime": "persistent"}, "epB"))

    await plugin.on_topology_change({"epB": {"liveness": "lost"}})
    assert _drain_deadline(plugin, "s1") is not None    # armed
    assert _drain_deadline(plugin, "p1") is None        # persistent untouched

    # Backdate the deadline into the past and sweep (no real wait).
    plugin._records["s1"].drain_deadline = time.time() - 1
    assert await plugin._cleanup_expired_sessions() == 1

    assert "s1" not in plugin._sessions
    assert "s1" not in plugin._records
    assert "p1" in plugin._sessions                     # persistent survives


@pytest.mark.asyncio
async def test_owner_return_cancels_drain():
    '''An owner seen 'present' again clears its drain deadline.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))

    await plugin.on_topology_change({"epB": {"liveness": "lost"}})
    assert _drain_deadline(plugin, "s1") is not None
    await plugin.on_topology_change({"epB": {"liveness": "present"}})
    assert _drain_deadline(plugin, "s1") is None

    assert await plugin._cleanup_expired_sessions() == 0
    assert "s1" in plugin._sessions                     # not reclaimed


@pytest.mark.asyncio
async def test_owner_bound_ephemeral_not_idle_expired():
    '''An owner-bound ephemeral session is governed by liveness, not the idle
    timeout; an owner-less one still idle-expires (old-stack behavior).'''
    plugin = _fresh_plugin()
    plugin.session_ttl = 1                             # tiny idle timeout

    await plugin.register_session(_owned_request({"sid": "bound"}, "epB"))
    await plugin.register_session(_owned_request({"sid": "free"}))   # owner None

    # Backdate both well past the idle timeout.
    plugin._records["bound"].last_access = time.time() - 100
    plugin._records["free"].last_access  = time.time() - 100

    cleaned = await plugin._cleanup_expired_sessions()
    assert cleaned == 1
    assert "bound" in plugin._sessions                 # liveness-governed
    assert "free" not in plugin._sessions              # idle-expired


@pytest.mark.asyncio
async def test_ttl_and_persistent_never_armed_by_owner_loss():
    '''An owner with only ttl/persistent sessions arms no drain on loss.'''
    plugin = _fresh_plugin()
    await plugin.register_session(
        _owned_request({"sid": "t1", "lifetime": "ttl", "ttl": 100}, "epB"))
    await plugin.register_session(
        _owned_request({"sid": "p1", "lifetime": "persistent"}, "epB"))

    await plugin.on_topology_change({"epB": {"liveness": "lost"}})
    assert _drain_deadline(plugin, "t1") is None
    assert _drain_deadline(plugin, "p1") is None
    assert await plugin._cleanup_expired_sessions() == 0
    assert "t1" in plugin._sessions
    assert "p1" in plugin._sessions


if __name__ == '__main__':

    test_plugin_initialization()
    test_plugin_uid_property()
    test_plugin_namespace_property()
    test_plugin_add_route_post()
    test_plugin_add_route_get()
    test_plugin_route_path_normalization()
    test_plugin_multiple_routes()
    test_plugin_unique_uids()

    print("All tests passed!")



