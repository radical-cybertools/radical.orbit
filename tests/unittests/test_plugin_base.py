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
import logging
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
    plugin._session_last_access["s1"] = time.time() - 10
    data = await plugin.register_session(_body_request({"sid": "s1"}))
    assert data['sid'] == "s1"
    assert plugin._sessions["s1"] is obj              # not rebuilt
    assert time.time() - plugin._session_last_access["s1"] < 5   # bumped


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
    {"lifetime": "ttl", "ttl": -5},            # negative ttl
    {"ttl": -1},                               # negative ttl
    {"lifetime": "ttl", "ttl": float('nan')},  # NaN ttl (JSON NaN) — not finite
    {"lifetime": "ttl", "ttl": float('inf')},  # non-finite ttl
    {"ttl": "60"},                             # not a number
    {"lifetime": "forever"},                   # unknown lifetime
])
async def test_session_incoherent_policy_409(payload):
    '''Invalid ttls and unknown lifetimes map to HTTP 409.'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    with pytest.raises(HTTPException) as exc_info:
        await plugin.register_session(_body_request(payload))
    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_session_reconnect_keeps_the_timeout_set_at_creation():
    '''A reconnect ignores ttl/lifetime: the timeout was fixed at creation
    (federation re-registers 'persistent', clients replay their body).'''
    app = FastAPI()
    plugin = Plugin(app, "test_plugin")
    plugin.session_class = PluginSession

    await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "ttl", "ttl": 10}))
    data = await plugin.register_session(
        _body_request({"sid": "s1", "lifetime": "persistent"}))
    assert data['sid'] == "s1"
    assert plugin._timeout(plugin._records["s1"]) == 10


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

    # Default should be 900 (15 min)
    assert plugin.session_ttl == 900


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
async def test_session_any_client_can_attach():
    '''Any client holding the sid can attach (a portal and a CLI share a
    session); attachment security is out of scope for now.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    for other in ("cli", None):
        data = await plugin.register_session(_owned_request({"sid": "s1"},
                                                            other))
        assert data['sid'] == "s1"
    assert plugin._records["s1"].owner == "epB"        # diagnostics only


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
# Session lifetime: one idle timeout per session, fixed at creation; owners
# do not matter; activity = requests (while running), notifications, and --
# at the deadline -- is_busy; ended sessions answer 410 with the reason.
# ---------------------------------------------------------------------------

def _sid_request(sid, headers=None):
    request = Mock(spec=Request)
    request.path_params = {'sid': sid}
    request.headers     = headers or {}
    return request


class _BusySession(PluginSession):
    busy = True

    def is_busy(self):
        return self.busy


@pytest.mark.asyncio
async def test_owner_loss_does_not_reclaim():
    '''A client may disconnect and reconnect later: owner liveness never
    ends a session.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}, "epB"))
    await plugin.on_topology_change({"epB": {"liveness": "lost"}})
    assert await plugin._cleanup_expired_sessions() == 0
    assert "s1" in plugin._sessions


@pytest.mark.asyncio
async def test_idle_timeout_applies_to_every_session():
    plugin = _fresh_plugin()
    plugin.session_ttl = 1
    await plugin.register_session(_owned_request({"sid": "bound"}, "epB"))
    await plugin.register_session(_owned_request({"sid": "free"}))
    await plugin.register_session(_owned_request({"sid": "long", "ttl": 3600}))
    await plugin.register_session(_owned_request({"sid": "never", "ttl": 0}))
    for sid in ("bound", "free", "long", "never"):
        plugin._records[sid].last_access = time.time() - 100
    assert await plugin._cleanup_expired_sessions() == 2
    assert set(plugin._sessions) == {"long", "never"}


@pytest.mark.asyncio
async def test_legacy_lifetime_maps_onto_the_timeout():
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "p",
                                                  "lifetime": "persistent"}))
    await plugin.register_session(_owned_request({"sid": "e",
                                                  "lifetime": "ephemeral"}))
    await plugin.register_session(_owned_request({"sid": "x", "ttl": 30,
                                                  "lifetime": "persistent"}))
    assert plugin._timeout(plugin._records["p"]) == 0
    assert plugin._timeout(plugin._records["e"]) == plugin.session_ttl
    assert plugin._timeout(plugin._records["x"]) == 30   # explicit ttl wins


@pytest.mark.asyncio
async def test_notification_is_activity():
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    await plugin.register_session(_owned_request({"sid": "s1"}))
    plugin._records["s1"].last_access = time.time() - 100
    plugin._sessions["s1"].notify("task_status", {"uid": "t1"})
    assert time.time() - plugin._records["s1"].last_access < 5
    assert await plugin._cleanup_expired_sessions() == 0


@pytest.mark.asyncio
async def test_busy_at_the_deadline_keeps_the_session():
    plugin = _fresh_plugin()
    plugin.session_class = _BusySession
    plugin.session_ttl   = 60
    await plugin.register_session(_owned_request({"sid": "b"}))
    plugin._records["b"].last_access = time.time() - 100
    assert await plugin._cleanup_expired_sessions() == 0     # busy
    plugin._sessions["b"].busy = False
    assert await plugin._cleanup_expired_sessions() == 0     # clock reset
    plugin._records["b"].last_access = time.time() - 100
    assert await plugin._cleanup_expired_sessions() == 1


@pytest.mark.asyncio
async def test_failing_is_busy_keeps_the_session():
    plugin = _fresh_plugin()

    class Broken(PluginSession):
        def is_busy(self):
            raise RuntimeError('probe broke')
    plugin.session_class = Broken
    plugin.session_ttl   = 1
    await plugin.register_session(_owned_request({"sid": "b"}))
    plugin._records["b"].last_access = time.time() - 100
    assert await plugin._cleanup_expired_sessions() == 0


@pytest.mark.asyncio
async def test_a_running_request_keeps_its_session():
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    await plugin.register_session(_owned_request({"sid": "s1"}))
    gate, started = asyncio.Event(), asyncio.Event()

    async def slow(request):
        started.set()
        await gate.wait()
        return {}
    call = asyncio.ensure_future(plugin._accessing(slow)(_sid_request("s1")))
    await started.wait()
    plugin._records["s1"].last_access = time.time() - 100
    assert await plugin._cleanup_expired_sessions() == 0
    gate.set()
    assert await call == {}
    assert plugin._records["s1"].inflight == 0


@pytest.mark.asyncio
async def test_expiry_is_explained_and_answered_with_410(caplog):
    from radical.orbit import plugin_base as pb
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    await plugin.register_session(_owned_request({"sid": "s1"}))
    plugin._records["s1"].last_access = time.time() - 100
    with caplog.at_level(logging.WARNING, logger="radical.orbit"):
        assert await plugin._cleanup_expired_sessions() == 1
    msg = caplog.records[-1].getMessage()
    assert caplog.records[-1].levelno == logging.WARNING
    assert "s1" in msg and "ttl=" in msg and pb.ISSUES_URL in msg

    for call in (lambda: plugin._forward("s1", PluginSession.ping),
                 lambda: plugin._accessing(Mock())(_sid_request("s1")),
                 lambda: plugin.register_session(_owned_request({"sid": "s1"}))):
        with pytest.raises(HTTPException) as exc:
            await call()
        assert exc.value.status_code == 410 and exc.value.detail == msg


@pytest.mark.asyncio
async def test_explorer_expiry_is_info(caplog):
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    handler = plugin._tagging_client(plugin.register_session)
    request = _owned_request({"sid": "e1"})
    request.headers = {"x-orbit-client": "explorer"}
    await handler(request)
    plugin._records["e1"].last_access = time.time() - 100
    with caplog.at_level(logging.INFO, logger="radical.orbit"):
        assert await plugin._cleanup_expired_sessions() == 1
    rec = caplog.records[-1]
    assert rec.levelno == logging.INFO and "Explorer" in rec.getMessage()



@pytest.mark.asyncio
async def test_session_shared_with_a_script_is_not_logged_as_explorer(caplog):
    '''The Explorer tag is case-insensitive and does not stick to a
    session a script also uses.'''
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    handler = plugin._tagging_client(plugin.register_session)
    request = _owned_request({"sid": "e1"})
    request.headers = {"X-Orbit-Client": "explorer"}
    await handler(request)
    assert plugin._records["e1"].client == "explorer"
    await handler(_owned_request({"sid": "e1"}))            # a script attaches
    assert plugin._records["e1"].client == "mixed"
    plugin._records["e1"].last_access = time.time() - 100
    with caplog.at_level(logging.INFO, logger="radical.orbit"):
        assert await plugin._cleanup_expired_sessions() == 1
    assert caplog.records[-1].levelno == logging.WARNING

@pytest.mark.asyncio
async def test_unregistered_session_answers_410_closed():
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "s1"}))
    assert await plugin.unregister_session(_sid_request("s1")) == {"ok": True}
    assert await plugin.unregister_session(_sid_request("s1")) == {"ok": True}
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(_owned_request({"sid": "s1"}))
    assert exc.value.status_code == 410 and "closed" in exc.value.detail


@pytest.mark.asyncio
async def test_persistent_sessions_are_not_remembered():
    '''Unregistering 'default' (persistent) forgets it: it is recreated on
    demand as before.'''
    plugin = _fresh_plugin()
    await plugin.register_session(_owned_request({"sid": "default"}))
    await plugin.unregister_session(_sid_request("default"))
    assert "default" not in plugin._ended
    assert await plugin._forward("default", PluginSession.ping) == {}


@pytest.mark.asyncio
async def test_ended_record_is_bounded(monkeypatch):
    from radical.orbit import plugin_base as pb
    monkeypatch.setattr(pb, 'ENDED_KEEP', 3)
    plugin = _fresh_plugin()
    for i in range(5):
        await plugin.register_session(_owned_request({"sid": f"s{i}"}))
        await plugin._end_session(f"s{i}", 'expired', time.time())
    assert list(plugin._ended) == ["s2", "s3", "s4"]


@pytest.mark.asyncio
async def test_touch_route_is_an_access_only():
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    await plugin.register_session(_owned_request({"sid": "s1"}))
    plugin._records["s1"].last_access = time.time() - 50
    assert await plugin.touch_session(_sid_request("s1")) == {}
    assert time.time() - plugin._records["s1"].last_access < 5
    with pytest.raises(HTTPException) as exc:
        await plugin.touch_session(_sid_request("nope"))
    assert exc.value.status_code == 404 and "nope" not in plugin._sessions


def test_last_access_shim_does_not_revive_an_ended_sid():
    plugin = _fresh_plugin()
    plugin._ended["gone"] = (time.time(), 'expired', 60, 100, None)
    plugin._session_last_access["gone"] = time.time()
    assert "gone" not in plugin._records


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





@pytest.mark.asyncio
async def test_reconnect_past_the_deadline_answers_410_before_the_sweep():
    '''A reconnect must not revive a session whose timeout has passed but
    which the sweep has not reached yet.'''
    plugin = _fresh_plugin()
    plugin.session_ttl = 60
    await plugin.register_session(_owned_request({"sid": "s1"}))
    plugin._records["s1"].last_access = time.time() - 100
    with pytest.raises(HTTPException) as exc:
        await plugin.register_session(_owned_request({"sid": "s1"}))
    assert exc.value.status_code == 410
    assert "s1" not in plugin._sessions
