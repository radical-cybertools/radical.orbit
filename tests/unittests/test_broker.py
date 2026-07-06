"""Tests for :class:`radical.orbit.broker.Broker`.

The suite drives the broker two ways, matching how the code is reached in
production:

* Through its app with the Starlette ``TestClient`` (register handshake +
  token gate over a real WS), and
* By direct method calls with a lightweight ``FakeWS`` where timing/teardown
  can't be expressed through the client (drop/grace/reap).

No test sleeps for more than ~1 s; liveness knobs are injected tiny.
"""

import asyncio
import threading
import time

import pytest

from starlette.websockets import WebSocketDisconnect
from fastapi.testclient   import TestClient

from radical.orbit import protocol


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------

import subprocess


def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def self_signed(tmp_path):
    if not _have_openssl():
        pytest.skip("openssl not available")
    import os
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    os.chmod(key, 0o600)
    return cert, key


@pytest.fixture
def make_broker(self_signed, tmp_path, monkeypatch):
    """Factory: build a Broker with tmp resolver paths and tiny timers.

    Returns the *unstarted* broker — direct-method tests call
    ``await broker.startup()`` / ``await broker.shutdown()`` themselves so
    they own the loop; TestClient tests let the app lifespan do it.
    """
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'broker.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')

    cert, key = self_signed

    _TUNING_KEYS = ('ping_interval', 'ping_timeout', 'grace', 'call_cap',
                    'call_timeout', 'reap_interval', 'event_queue', 'frame_cap')

    def _build(**kwargs):
        from radical.orbit.broker import Broker, BrokerTuning
        # Tiny liveness timers by default; tunable kwargs route to BrokerTuning.
        tuning = BrokerTuning(ping_timeout=0.05, grace=0.05)
        for _k in list(kwargs):
            if _k in _TUNING_KEYS:
                setattr(tuning, _k, kwargs.pop(_k))
        defaults = dict(cert=str(cert), key=str(key), no_auth=True, tuning=tuning)
        defaults.update(kwargs)
        return Broker(**defaults)

    return _build


class FakeWS:
    """A minimal transport stand-in: records sent frames, notes a close."""

    def __init__(self):
        self.sent   = []
        self.closed = None

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000) -> None:
        self.closed = code

    def msgs(self):
        return [protocol.parse_message(d) for d in self.sent]


def _register_msg(name, plugins=None, credential=None, resume_key=None,
                  role='endpoint'):
    return protocol.Register(
        src=name, role=role, plugins=plugins or [],
        identity=protocol.Identity(name=name, credential=credential,
                                   resume_key=resume_key))


async def _register(broker, name, ws, **kw):
    return await broker._register(ws, _register_msg(name, **kw))


def _event_bytes(src, plugin, topic, session=None, data=None):
    ev = protocol.Event(src=src, plugin=plugin, topic=topic, session=session,
                        ts=0.0, seq=0, data=data or {})
    return protocol.pack_message(ev)


def _subscribe_bytes(src, patterns):
    sub = protocol.Subscribe(
        src=src, patterns=[protocol.SubscribePattern(**p) for p in patterns])
    return protocol.pack_message(sub)


# ---------------------------------------------------------------------------
# Registration + topology (TestClient — real WS handshake)
# ---------------------------------------------------------------------------

def test_register_happy_path_ack_and_topology(make_broker):
    broker = make_broker()
    with TestClient(broker.app) as client:
        with client.websocket_connect("/register") as ws:
            ws.send_bytes(protocol.pack_message(
                _register_msg("e1", plugins=[{"name": "sysinfo"}])))

            ack = protocol.parse_message(ws.receive_bytes())
            assert ack.kind == 'register_ack'
            assert ack.ok is True
            assert ack.resume_key           # minted, non-empty

            topo = protocol.parse_message(ws.receive_bytes())
            assert topo.kind == 'topology'
            assert 'e1'     in topo.participants
            assert 'broker' in topo.participants           # broker is a participant
            assert topo.participants['e1'].liveness == 'present'
            assert 'sysinfo' in topo.participants['e1'].plugins


def test_first_frame_not_register_is_rejected(make_broker):
    broker = make_broker()
    with TestClient(broker.app) as client:
        with client.websocket_connect("/register") as ws:
            # An event as the first frame -> handler returns -> socket closes.
            ws.send_bytes(_event_bytes("e1", "p", "t"))
            with pytest.raises(WebSocketDisconnect):
                ws.receive_bytes()


# ---------------------------------------------------------------------------
# Token gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_gate_rejects_bad_and_missing(make_broker):
    broker = make_broker(no_auth=False, token='sekret')
    await broker.startup()
    try:
        ws_bad = FakeWS()
        ok, name = await _register(broker, 'e1', ws_bad, credential='wrong')
        assert ok is False and name is None
        assert ws_bad.msgs()[0].reason == 'invalid-credential'

        ws_missing = FakeWS()
        ok, _ = await _register(broker, 'e1', ws_missing, credential=None)
        assert ok is False

        ws_good = FakeWS()
        ok, name = await _register(broker, 'e1', ws_good, credential='sekret')
        assert ok is True and name == 'e1'
        assert broker.registry['e1'] is ws_good
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_auth_disabled_accepts_no_credential(make_broker):
    broker = make_broker(no_auth=True)
    await broker.startup()
    try:
        ws = FakeWS()
        ok, name = await _register(broker, 'e1', ws, credential=None)
        assert ok is True and name == 'e1'
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Resume key: replace vs name-in-use
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resume_key_replaces_and_old_socket_teardown_noops(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        ok, _ = await _register(broker, 'e1', ws1)
        key = protocol.parse_message(ws1.sent[0]).resume_key

        ws2 = FakeWS()
        ok, _ = await _register(broker, 'e1', ws2, resume_key=key)
        assert ok is True
        assert broker.registry['e1'] is ws2
        await asyncio.sleep(0)                 # let the old-socket close run
        assert ws1.closed == 1000

        # The replaced socket's teardown must no-op via the guard.
        await broker._on_socket_drop('e1', ws1, clean=False)
        assert broker.registry['e1'] is ws2
        assert broker._liveness['e1'] == 'present'
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_keyless_reregister_within_grace_is_name_in_use(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)

        ws2 = FakeWS()
        ok, name = await _register(broker, 'e1', ws2)     # no resume_key
        assert ok is False and name is None
        assert protocol.parse_message(ws2.sent[0]).reason == 'name-in-use'
        assert broker.registry['e1'] is ws1
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_name_frees_after_lost_and_new_key_minted(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)
        key1 = protocol.parse_message(ws1.sent[0]).resume_key

        await broker._on_socket_drop('e1', ws1, clean=False)   # -> suspect
        broker._fire_lost('e1', ws1)                           # -> lost
        await asyncio.sleep(0)
        assert 'e1' not in broker.registry

        ws2 = FakeWS()
        ok, _ = await _register(broker, 'e1', ws2)             # fresh, no key
        assert ok is True
        key2 = protocol.parse_message(ws2.sent[0]).resume_key
        assert key2 and key2 != key1
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Request / response routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_request_routing_overwrites_client_src(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)
        ws2.sent.clear()

        # Client lies about src; broker must overwrite it with 'e1'.
        req = protocol.make_request('LIAR', 'e2', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))

        fwd = ws2.msgs()[0]
        assert fwd.kind == 'request'
        assert fwd.src  == 'e1'                          # overwritten
        call = broker._calls[req.corr_id]
        assert (call.src, call.dst, call.future) == ('e1', 'e2', None)
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_forwarded_call_table_cap_fast_fails_503(make_broker):
    from radical.orbit.broker import _Call
    broker = make_broker()
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)
        ws1.sent.clear()
        ws2.sent.clear()

        # Fill the forwarded-call table to the ceiling.
        cap = broker._forwarded_call_cap
        for i in range(cap):
            broker._calls['fill-%d' % i] = _Call('e1', 'e2', None, 1e9)
        assert len(broker._calls) == cap

        req = protocol.make_request('e1', 'e2', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))

        # 503 back to the requester, dst never received the frame, no growth.
        resp = ws1.msgs()[0]
        assert resp.kind == 'response' and resp.status == 503
        assert resp.corr_id == req.corr_id
        assert not ws2.sent                             # not forwarded to dst
        assert len(broker._calls) == cap                # table did not grow
        assert req.corr_id not in broker._calls
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_request_to_unknown_dst_gets_502(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)
        ws1.sent.clear()

        req = protocol.make_request('e1', 'ghost', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))

        resp = ws1.msgs()[0]
        assert resp.kind == 'response' and resp.status == 502
        assert resp.corr_id == req.corr_id
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_response_forwards_to_peer(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)

        req = protocol.make_request('e1', 'e2', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))
        ws1.sent.clear()

        resp = protocol.Response(src='e2', dst='e1', status=200,
                                corr_id=req.corr_id, body=b'ok')
        await broker._route_frame('e2', protocol.pack_message(resp))

        got = ws1.msgs()[0]
        assert got.kind == 'response' and got.status == 200
        assert req.corr_id not in broker._calls           # in-flight entry popped
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_response_to_broker_resolves_pending(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws2 = FakeWS()
        await _register(broker, 'e2', ws2)
        ws2.sent.clear()

        task = asyncio.ensure_future(broker.caller.call('e2', 'GET', '/p'))
        await asyncio.sleep(0)                            # let call() send
        sent = ws2.msgs()[0]
        assert sent.kind == 'request' and sent.src == 'broker'

        resp = protocol.Response(src='e2', dst='broker', status=201,
                                corr_id=sent.corr_id, body=b'done')
        await broker._route_frame('e2', protocol.pack_message(resp))
        out = await asyncio.wait_for(task, timeout=1.0)
        assert out['status'] == 201
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Fast-fail on lost
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lost_fastfails_endpoint_and_broker_inflight(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)

        # Endpoint-owned inflight: e1 -> e2.
        req = protocol.make_request('e1', 'e2', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))
        ws1.sent.clear()

        # Broker-owned inflight: broker -> e2.
        task = asyncio.ensure_future(broker.caller.call('e2', 'GET', '/y'))
        await asyncio.sleep(0)

        broker._fire_lost('e2', ws2)                      # e2 declared lost
        await asyncio.sleep(0)

        got = ws1.msgs()[0]
        assert got.kind == 'response' and got.status == 504
        assert got.corr_id == req.corr_id

        out = await asyncio.wait_for(task, timeout=1.0)
        assert out['status'] == 504
        assert 'e2' not in broker.registry
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_call_cap_raises_synchronously(make_broker):
    from radical.orbit.broker import _Call
    broker = make_broker(call_cap=2)
    await broker.startup()
    try:
        ws2 = FakeWS()
        await _register(broker, 'e2', ws2)
        loop = asyncio.get_running_loop()
        # Fill the cap with broker-originated (future-bearing) calls.
        broker._calls['a'] = _Call('broker', 'e2', loop.create_future(), 1e9)
        broker._calls['b'] = _Call('broker', 'e2', loop.create_future(), 1e9)
        with pytest.raises(RuntimeError):
            await broker.caller.call('e2', 'GET', '/x')
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_call_threadsafe_roundtrips(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws2 = FakeWS()
        await _register(broker, 'e2', ws2)
        ws2.sent.clear()

        holder = {}

        def worker():
            cfut = broker.caller.call_threadsafe('e2', 'GET', '/p')
            holder['r'] = cfut.result(timeout=2.0)

        t = threading.Thread(target=worker)
        t.start()

        # Drive the routing loop until the call has been sent.
        for _ in range(200):
            await asyncio.sleep(0.005)
            if ws2.sent:
                break
        sent = ws2.msgs()[0]
        resp = protocol.Response(src='e2', dst='broker', status=200,
                                corr_id=sent.corr_id, body=b'k')
        await broker._route_frame('e2', protocol.pack_message(resp))

        for _ in range(200):
            await asyncio.sleep(0.005)
            if 'r' in holder:
                break
        t.join(timeout=2.0)
        assert holder['r']['status'] == 200
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Events: seq stamping, subscribe filtering, backpressure, tap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_event_seq_stamping_session_and_global(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        cons = FakeWS()
        await _register(broker, 'c1', cons)
        broker._events.update_subscription('c1', {'patterns': [{}]}, True)
        cons.sent.clear()

        await broker._route_frame('e1', _event_bytes('e1', 'p', 't', session='s'))
        await broker._route_frame('e1', _event_bytes('e1', 'p', 't', session='s'))
        await broker._route_frame('e1', _event_bytes('e1', 'p', 't'))  # session-less
        await broker._route_frame('e1', _event_bytes('e1', 'p', 't'))
        await asyncio.sleep(0.02)

        evs = cons.msgs()
        per_session = [e.seq for e in evs if e.session == 's']
        global_seq  = [e.seq for e in evs if e.session is None]
        assert per_session == [0, 1]
        assert global_seq  == [0, 1]
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_subscribe_filtering_exact_wildcard_unsubscribe(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        c_exact, c_wild, c_none = FakeWS(), FakeWS(), FakeWS()
        await _register(broker, 'c_exact', c_exact)
        await _register(broker, 'c_wild',  c_wild)
        await _register(broker, 'c_none',  c_none)
        broker._events.update_subscription(
            'c_exact', {'patterns': [{'plugin': 'psij'}]}, True)
        broker._events.update_subscription(
            'c_wild', {'patterns': [{}]}, True)
        for ws in (c_exact, c_wild, c_none):
            ws.sent.clear()

        await broker._route_frame('e1', _event_bytes('e1', 'psij', 't'))
        await broker._route_frame('e1', _event_bytes('e1', 'rhapsody', 't'))
        await asyncio.sleep(0.02)

        assert [e.plugin for e in c_exact.msgs()] == ['psij']
        assert [e.plugin for e in c_wild.msgs()]  == ['psij', 'rhapsody']
        assert c_none.sent == []                          # non-subscriber isolated

        # Unsubscribe the wildcard -> no further delivery.
        broker._events.update_subscription('c_wild', {'patterns': [{}]}, False)
        c_wild.sent.clear()
        await broker._route_frame('e1', _event_bytes('e1', 'psij', 't'))
        await asyncio.sleep(0.02)
        assert c_wild.sent == []
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_event_queue_drop_oldest_with_counter(make_broker):
    broker = make_broker(event_queue=2)
    await broker.startup()
    try:
        cons = FakeWS()
        await _register(broker, 'c1', cons)
        broker._events.update_subscription('c1', {'patterns': [{}]}, True)
        # Pause the sender so the bounded queue actually overflows.
        broker._events.pause_sender('c1')

        for _ in range(4):
            broker._events.ingest('e1', {
                'kind': 'event', 'version': protocol.PROTOCOL_VERSION,
                'src': 'e1', 'plugin': 'p', 'topic': 't',
                'session': None, 'ts': 0.0, 'seq': 0, 'data': {}})

        assert broker._events.dropped('c1') == 2
        buf = broker._events._out['c1'].buf
        seqs = [protocol.parse_message(d).seq for d in buf]
        assert seqs == [2, 3]                              # oldest two dropped
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_raw_tap_receives_every_event(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        seen = []
        lock = threading.Lock()

        def _tap(ev):
            with lock:
                seen.append(ev['seq'])

        broker.tap(_tap)
        for _ in range(3):
            await broker._route_frame('e1', _event_bytes('e1', 'p', 't'))

        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            with lock:
                if len(seen) == 3:
                    break
            await asyncio.sleep(0.01)
        with lock:
            assert sorted(seen) == [0, 1, 2]
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Liveness: clean close, suspect->lost, re-register cancels grace
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_clean_close_skips_suspect(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)
        await broker._on_socket_drop('e1', ws1, clean=True)
        assert 'e1' not in broker.registry
        assert 'e1' not in broker._liveness
        assert 'e1' not in broker._grace_timers
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_suspect_then_lost_cascade(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)

        await broker._on_socket_drop('e1', ws1, clean=False)
        assert broker._liveness['e1'] == 'suspect'
        assert 'e1' in broker.registry
        assert 'e1' in broker._grace_timers

        broker._fire_lost('e1', ws1)
        await asyncio.sleep(0)
        assert 'e1' not in broker.registry
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_reregister_cancels_grace(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)
        key = protocol.parse_message(ws1.sent[0]).resume_key

        await broker._on_socket_drop('e1', ws1, clean=False)  # suspect + grace
        assert 'e1' in broker._grace_timers

        ws2 = FakeWS()
        ok, _ = await _register(broker, 'e1', ws2, resume_key=key)
        assert ok is True
        assert 'e1' not in broker._grace_timers
        assert broker._liveness['e1'] == 'present'
        assert broker.registry['e1'] is ws2
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Call reaper: unanswered forwarded calls are bounded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reaper_fastfails_unanswered_forwarded_call(make_broker):
    broker = make_broker(call_timeout=0.02, reap_interval=0.01)
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)

        # e1 -> e2 forwarded call; e2 never answers.
        req = protocol.make_request('e1', 'e2', 'GET', '/x')
        await broker._route_frame('e1', protocol.pack_message(req))
        assert req.corr_id in broker._calls
        ws1.sent.clear()

        # The reaper evicts the stale entry and fast-fails the requester (504).
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if req.corr_id not in broker._calls and ws1.sent:
                break
            await asyncio.sleep(0.01)
        assert req.corr_id not in broker._calls
        resp = ws1.msgs()[0]
        assert resp.kind == 'response' and resp.status == 504
        assert resp.corr_id == req.corr_id
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# Control ops
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_control_terminate_self_sigterms(make_broker, monkeypatch):
    broker = make_broker()
    await broker.startup()
    try:
        import radical.orbit.broker as bmod
        killed = {}
        monkeypatch.setattr(bmod.os, 'kill',
                            lambda pid, sig: killed.setdefault('sig', sig))
        await broker._handle_control('e1', {'op': 'terminate'})
        await asyncio.sleep(0.6)
        assert killed.get('sig') == bmod.signal.SIGTERM
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_control_disconnect_removes_target(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1, ws2 = FakeWS(), FakeWS()
        await _register(broker, 'e1', ws1)
        await _register(broker, 'e2', ws2)

        await broker._handle_control('e2', {'op': 'disconnect',
                                            'data': {'name': 'e1'}})
        await asyncio.sleep(0)
        assert 'e1' not in broker.registry
        assert ws1.closed == 1000
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# dst=='broker' host round-trip + config wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dst_broker_routes_into_host(make_broker):
    from radical.orbit.plugin_base import Plugin

    class _EchoPlugin(Plugin):
        plugin_name = 'echo_test'
        version     = '0.0.1'

        def __init__(self, app, instance_name):
            super().__init__(app, instance_name)
            self.add_route_get('ping', self._ping)

        async def _ping(self, request):
            return {'pong': True}

    broker = make_broker()
    await broker.startup()
    try:
        # Register the test plugin onto the host (on the host loop).
        import asyncio as _a
        fut = _a.run_coroutine_threadsafe(
            broker._plugin_host.register_dynamic_plugin(_EchoPlugin, 'echo'),
            broker._host_loop)
        fut.result(timeout=5.0)

        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)
        ws1.sent.clear()

        req = protocol.make_request('e1', 'broker', 'GET', '/echo/ping')
        await broker._route_frame('e1', protocol.pack_message(req))

        # The host dispatch runs as a supervised background task; await it.
        # (Other frames — e.g. a topology broadcast — may also arrive, so
        # select the response rather than assuming it is first.)
        def _responses():
            return [m for m in ws1.msgs() if m.kind == 'response']
        for _ in range(200):
            await asyncio.sleep(0.005)
            if _responses():
                break
        resp = _responses()[0]
        assert resp.status == 200
        assert b'pong' in resp.body
        assert resp.corr_id == req.corr_id
    finally:
        await broker.shutdown()


def test_run_sets_ws_max_size_and_keepalive_from_config(make_broker,
                                                        monkeypatch):
    broker = make_broker(frame_cap=12345, ping_interval=2.5, ping_timeout=7.5)
    captured = {}

    import uvicorn
    monkeypatch.setattr(uvicorn, 'run',
                        lambda app, **kw: captured.update(kw))
    broker.run()

    assert captured['ws_max_size']      == 12345
    assert captured['ws_ping_interval'] == 2.5
    assert captured['ws_ping_timeout']  == 7.5


# ---------------------------------------------------------------------------
# Gateway seam
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gateway_seam_surface(make_broker):
    broker = make_broker()
    await broker.startup()
    try:
        ws1 = FakeWS()
        await _register(broker, 'e1', ws1)

        # The seam attributes/methods a later gateway is handed.
        assert broker.app is not None
        assert broker.caller is not None                 # caller handle
        assert callable(broker.tap)
        snap = broker.topology_snapshot()
        assert 'e1' in snap and 'broker' in snap
        assert snap['e1']['liveness'] == 'present'
        assert broker.auth_enabled is False              # no_auth=True
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# --host bind address survives startup (item 14 regression)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_host_bind_arg_reaches_uvicorn_after_startup(make_broker,
                                                           monkeypatch):
    """The constructor ``host`` must reach ``uvicorn.run`` even after startup
    built the hosted-plugin host — the plugin host lives on its own attribute
    (``_plugin_host``), so it never clobbers the bind address (``_host``)."""
    broker = make_broker(host='127.0.0.1')
    await broker.startup()
    try:
        # The hosted-plugin host is reachable at its own attribute; call_host
        # still routes into it (empty host → 404 on an unknown route).
        assert broker._plugin_host is not None
        resp = await broker.call_host('GET', '/no-such-route')
        assert resp['status'] in (404, 503)

        captured = {}
        import uvicorn
        monkeypatch.setattr(uvicorn, 'run',
                            lambda app, **kw: captured.update(kw))
        broker.run()
        assert captured['host'] == '127.0.0.1'
    finally:
        await broker.shutdown()


# ---------------------------------------------------------------------------
# clean hosted-plugin shutdown — no orphaned background tasks (item 19)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hosted_plugin_shutdown_cancels_background_tasks(make_broker):
    """Broker.shutdown() drives the hosted plugins' shutdown hook so their
    background tasks (e.g. the session-cleanup loop) are cancelled before the
    host loop is torn down — no 'Task was destroyed but it is pending'."""
    broker = make_broker(plugins='sysinfo', gateway=False)
    await broker.startup()
    plugin = broker._plugin_host.plugins['sysinfo']

    # Arm the plugin's background session-cleanup task on the host loop.
    def _arm():
        plugin._ensure_cleanup_task()
    asyncio.run_coroutine_threadsafe(
        _run_on_loop(_arm), broker._host_loop).result(timeout=5.0)
    assert plugin._cleanup_task is not None
    assert not plugin._cleanup_task.done()

    await broker.shutdown()

    # The cleanup task was cancelled cleanly during shutdown.
    assert plugin._cleanup_task.done()


async def _run_on_loop(fn):
    fn()


def test_strip_broker_prefix_handles_query(make_broker):
    """The broker-prefix stripper handles the bare prefix, subpaths, and the
    bare-prefix-plus-query form (`/broker?foo=bar` -> `/?foo=bar`)."""
    b = make_broker()
    assert b._strip_broker_prefix('/broker')         == '/'
    assert b._strip_broker_prefix('/broker/x/y')     == '/x/y'
    assert b._strip_broker_prefix('/broker?foo=bar') == '/?foo=bar'
    assert b._strip_broker_prefix('/broker/x?f=b')   == '/x?f=b'
    assert b._strip_broker_prefix('/other')          == '/other'


@pytest.mark.asyncio
async def test_restart_sender_delivers_buffered_events(make_broker):
    """A sender cancelled mid-drain (wake cleared, items still buffered) must
    not stall on restart — the fresh sender flushes the backlog immediately."""
    broker = make_broker()
    await broker.startup()
    try:
        er = broker._events
        ws = FakeWS()
        broker.registry['e1'] = ws
        er.add_endpoint('e1')
        oq = er._out['e1']

        # Emulate the race: a buffered frame with wake cleared (as a prior
        # sender would have left it after clearing wake, then being cancelled).
        er.pause_sender('e1')
        oq.push(b'frame1')
        oq.wake.clear()

        er.restart_sender('e1')
        for _ in range(50):
            await asyncio.sleep(0)
            if b'frame1' in ws.sent:
                break
        assert b'frame1' in ws.sent
    finally:
        broker.registry.pop('e1', None)
        await broker.shutdown()
