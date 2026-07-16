
# pylint: disable=protected-access,unused-variable

import asyncio

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_math import PluginMath, MathSession


def _mk():
    app    = FastAPI()
    plugin = PluginMath(app)
    return app, plugin, TestClient(app)


# ── plugin construction ─────────────────────────────────────────────────────

def test_plugin_math_init():
    """Test plugin initialization and route registration."""
    app, plugin, _ = _mk()

    assert plugin.instance_name == 'math'
    assert plugin.namespace     == '/math'

    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    for op in ('add', 'sub', 'mul', 'div', 'history'):
        assert any(f'math/{op}/' in p for p in route_pats), op


# ── session behaviour (no HTTP involved) ────────────────────────────────────

def test_math_session_compute_and_history():
    """Compute records every operation in the session history."""
    session = MathSession('sid.test')

    r = asyncio.run(session.compute('add', 3.0, 4.0))
    assert r['result'] == 7.0

    r = asyncio.run(session.compute('div', 1.0, 4.0))
    assert r['result'] == 0.25

    h = asyncio.run(session.history())
    assert h['count'] == 2
    assert h['ops'][0]['op'] == 'add'
    assert h['ops'][1]['op'] == 'div'


def test_math_session_notifies():
    """Every compute emits a 'result' notification via the parent plugin."""
    session = MathSession('sid.test')
    sent    = []

    class _FakePlugin:
        def _dispatch_notify(self, topic, data):
            sent.append((topic, data))

    session._plugin = _FakePlugin()
    asyncio.run(session.compute('mul', 6.0, 7.0))

    assert sent == [('result', {'op': 'mul', 'a': 6.0, 'b': 7.0,
                                'result': 42.0, 'count': 1})]


def test_math_session_unknown_op_raises():
    session = MathSession('sid.test')
    with pytest.raises(ValueError):
        asyncio.run(session.compute('pow', 2.0, 3.0))


def test_math_session_closed_raises():
    session = MathSession('sid.test')
    asyncio.run(session.close())
    with pytest.raises(RuntimeError):
        asyncio.run(session.compute('add', 1.0, 2.0))


# ── HTTP round trips (TestClient over the ASGI routes) ──────────────────────

def test_math_http_roundtrip():
    """Register a session, compute, read history, unregister."""
    _, plugin, client = _mk()

    resp = client.post('/math/register_session')
    assert resp.status_code == 200
    sid = resp.json()['sid']

    resp = client.post(f'/math/add/{sid}', json={'a': 3, 'b': 4})
    assert resp.status_code == 200
    assert resp.json()['result'] == 7.0

    resp = client.post(f'/math/div/{sid}', json={'a': 1, 'b': 4})
    assert resp.status_code == 200
    assert resp.json()['result'] == 0.25

    resp = client.get(f'/math/history/{sid}')
    assert resp.status_code == 200
    assert resp.json()['count'] == 2

    resp = client.post(f'/math/unregister_session/{sid}')
    assert resp.status_code == 200


def test_math_http_div_by_zero_is_400():
    _, plugin, client = _mk()
    sid  = client.post('/math/register_session').json()['sid']

    resp = client.post(f'/math/div/{sid}', json={'a': 1, 'b': 0})
    assert resp.status_code == 400
    assert 'division by zero' in resp.json()['detail']


def test_math_http_bad_body_is_400():
    _, plugin, client = _mk()
    sid  = client.post('/math/register_session').json()['sid']

    for body in ({}, {'a': 1}, {'a': 'x', 'b': 2}, {'a': None, 'b': 2}):
        resp = client.post(f'/math/add/{sid}', json=body)
        assert resp.status_code == 400, body


def test_math_http_unknown_session_is_404():
    _, plugin, client = _mk()

    resp = client.post('/math/add/no-such-sid', json={'a': 1, 'b': 2})
    assert resp.status_code == 404
