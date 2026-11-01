"""
Tests for the broker/endpoint wire protocol (``protocol.py``).
"""
import msgpack
import pytest

from radical.orbit.protocol import (
    PROTOCOL_VERSION, FRAME_CAP, ProtocolError,
    Identity, SubscribePattern, ParticipantInfo,
    Request, Response, Event, Register, RegisterAck,
    Subscribe, Unsubscribe, Topology, Control,
    mint_id, mint_corr_id, pack_message, parse_message,
    make_request, make_response,
)


NON_UTF8_BODY = b'\x00\xff\x80'


def _roundtrip(msg):
    """Pack + parse and return the reconstructed message."""
    data = pack_message(msg)
    return parse_message(data), data


class TestIdHelpers:
    """id / corr_id minting helpers."""

    def test_mint_id_is_uuid4_str(self):
        import uuid
        mid = mint_id()
        assert isinstance(mid, str)
        assert uuid.UUID(mid).version == 4

    def test_mint_id_unique(self):
        assert mint_id() != mint_id()

    def test_mint_corr_id_prefixed_by_src(self):
        cid = mint_corr_id('endpoint-1')
        assert cid.startswith('endpoint-1:')

    def test_mint_corr_id_unique(self):
        assert mint_corr_id('a') != mint_corr_id('a')


class TestRoundTrip:
    """Round-trip every kind through pack_message/parse_message."""

    def test_request_roundtrip(self):
        msg = Request(
            src='client.1', dst='endpoint.1', method='POST',
            path='/rhapsody/submit/session.abc',
            headers={'content-type': 'application/json'},
            body=NON_UTF8_BODY,
            corr_id=mint_corr_id('client.1'))
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Request)
        assert parsed.version == PROTOCOL_VERSION
        assert parsed.src == 'client.1'
        assert parsed.dst == 'endpoint.1'
        assert parsed.method == 'POST'
        assert parsed.path == '/rhapsody/submit/session.abc'
        assert parsed.headers == {'content-type': 'application/json'}
        assert parsed.body == NON_UTF8_BODY
        assert isinstance(parsed.body, bytes)
        assert parsed.corr_id == msg.corr_id

    def test_response_roundtrip(self):
        msg = Response(
            src='endpoint.1', dst='client.1', status=200,
            headers={'content-type': 'application/octet-stream'},
            body=NON_UTF8_BODY, corr_id='client.1:abc')
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Response)
        assert parsed.status == 200
        assert parsed.body == NON_UTF8_BODY
        assert isinstance(parsed.body, bytes)
        assert parsed.corr_id == 'client.1:abc'

    def test_event_roundtrip(self):
        msg = Event(
            src='broker', dst=None, plugin='rhapsody', topic='task_status',
            session='session.xyz', ts=1234.5, seq=42,
            data={'uid': 'task-1', 'state': 'DONE'})
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Event)
        assert parsed.plugin == 'rhapsody'
        assert parsed.topic == 'task_status'
        assert parsed.session == 'session.xyz'
        assert parsed.ts == 1234.5
        assert parsed.seq == 42
        assert parsed.data == {'uid': 'task-1', 'state': 'DONE'}

    def test_event_roundtrip_sessionless(self):
        msg = Event(
            src='broker', plugin='sysinfo', topic='metrics', session=None,
            ts=1.0, seq=1, data={})
        parsed, _ = _roundtrip(msg)
        assert parsed.session is None

    def test_event_ts_seq_default_none(self):
        # Senders leave ts/seq unset; the broker assigns them on ingest.
        msg = Event(src='e1', plugin='p', topic='t')
        parsed, _ = _roundtrip(msg)
        assert parsed.ts is None
        assert parsed.seq is None

    def test_register_roundtrip(self):
        msg = Register(
            src='endpoint.1',
            identity=Identity(name='endpoint.1', credential='tok',
                               resume_key=None),
            role='endpoint',
            plugins=[{'name': 'sysinfo', 'namespace': '/endpoint.1/sysinfo/x'}])
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Register)
        assert parsed.identity.name == 'endpoint.1'
        assert parsed.identity.credential == 'tok'
        assert parsed.identity.resume_key is None
        assert parsed.role == 'endpoint'
        assert parsed.plugins == [
            {'name': 'sysinfo', 'namespace': '/endpoint.1/sysinfo/x'}]

    def test_register_ack_roundtrip_ok(self):
        msg = RegisterAck(
            src='broker', dst='endpoint.1', ok=True, reason=None,
            resume_key='rk-123')
        parsed, _ = _roundtrip(msg)
        assert parsed.ok is True
        assert parsed.reason is None
        assert parsed.resume_key == 'rk-123'

    def test_register_ack_roundtrip_failure(self):
        msg = RegisterAck(
            src='broker', dst='endpoint.1', ok=False,
            reason='name in use', resume_key='')
        parsed, _ = _roundtrip(msg)
        assert parsed.ok is False
        assert parsed.reason == 'name in use'
        assert parsed.resume_key == ''

    def test_subscribe_roundtrip_with_wildcards(self):
        msg = Subscribe(
            src='endpoint.1', dst='broker',
            patterns=[
                SubscribePattern(endpoint=None, plugin='rhapsody', topic='task_status'),
                SubscribePattern(endpoint=None, plugin=None, topic=None),
            ])
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Subscribe)
        assert len(parsed.patterns) == 2
        assert parsed.patterns[0].endpoint is None
        assert parsed.patterns[0].plugin == 'rhapsody'
        assert parsed.patterns[0].topic == 'task_status'
        assert parsed.patterns[1].endpoint is None
        assert parsed.patterns[1].plugin is None
        assert parsed.patterns[1].topic is None

    def test_unsubscribe_roundtrip(self):
        msg = Unsubscribe(
            src='endpoint.1', dst='broker',
            patterns=[SubscribePattern(endpoint='e1', plugin='p', topic='t')])
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Unsubscribe)
        assert parsed.patterns[0].endpoint == 'e1'

    def test_topology_roundtrip(self):
        msg = Topology(
            src='broker', dst='endpoint.1',
            participants={
                'endpoint.1': ParticipantInfo(
                    role='endpoint',
                    plugins={'sysinfo': {'namespace': '/endpoint.1/sysinfo/x',
                                         'version': '1.0',
                                         'ui_config': {},
                                         'enabled': True}},
                    liveness='present'),
                'consumer.2': ParticipantInfo(
                    role='consumer', plugins={}, liveness='suspect'),
            })
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Topology)
        assert set(parsed.participants) == {'endpoint.1', 'consumer.2'}
        ep = parsed.participants['endpoint.1']
        assert ep.role == 'endpoint'
        assert ep.liveness == 'present'
        assert ep.plugins['sysinfo']['namespace'] == '/endpoint.1/sysinfo/x'
        assert parsed.participants['consumer.2'].liveness == 'suspect'

    def test_control_roundtrip(self):
        msg = Control(src='broker', dst='endpoint.1', op='terminate',
                      data={'reason': 'operator request'})
        parsed, _ = _roundtrip(msg)
        assert isinstance(parsed, Control)
        assert parsed.op == 'terminate'
        assert parsed.data == {'reason': 'operator request'}

    def test_control_default_data(self):
        msg = Control(src='broker', op='shutdown')
        parsed, _ = _roundtrip(msg)
        assert parsed.data == {}


class TestMakeRequestResponse:
    """make_request / make_response convenience constructors."""

    def test_make_request_mints_namespaced_corr_id(self):
        req = make_request('client.1', 'endpoint.1', 'GET', '/health')
        assert req.corr_id.startswith('client.1:')
        assert req.src == 'client.1'
        assert req.dst == 'endpoint.1'
        assert req.method == 'GET'
        assert req.path == '/health'
        assert req.body == b''
        assert req.headers == {}

    def test_make_request_explicit_corr_id(self):
        req = make_request('client.1', 'endpoint.1', 'GET', '/health',
                            corr_id='client.1:fixed')
        assert req.corr_id == 'client.1:fixed'

    def test_make_response_swaps_src_dst_and_copies_corr_id(self):
        req  = make_request('client.1', 'endpoint.1', 'POST', '/submit',
                             body=b'{"x": 1}')
        resp = make_response(req, 200, body=b'{"ok": true}')
        assert resp.src == 'endpoint.1'
        assert resp.dst == 'client.1'
        assert resp.corr_id == req.corr_id
        assert resp.status == 200
        assert resp.body == b'{"ok": true}'

    def test_make_response_roundtrips(self):
        req  = make_request('client.1', 'endpoint.1', 'GET', '/x')
        resp = make_response(req, 404, body=b'not found')
        parsed, _ = _roundtrip(resp)
        assert parsed.status == 404
        assert parsed.src == 'endpoint.1'
        assert parsed.dst == 'client.1'
        assert parsed.corr_id == req.corr_id


class TestFrameCap:
    """Oversized-frame enforcement, on both pack and parse."""

    def test_pack_message_raises_over_small_cap(self):
        msg = Request(src='a', dst='b', method='GET', path='/x',
                      body=b'0123456789')
        with pytest.raises(ProtocolError, match='exceeds cap'):
            pack_message(msg, cap=8)

    def test_pack_message_ok_under_cap(self):
        msg = Request(src='a', dst='b', method='GET', path='/x', body=b'x')
        data = pack_message(msg, cap=FRAME_CAP)
        assert isinstance(data, bytes)

    def test_parse_message_enforces_cap(self):
        msg  = Request(src='a', dst='b', method='GET', path='/x',
                       body=b'0123456789')
        data = pack_message(msg, cap=FRAME_CAP)
        with pytest.raises(ProtocolError, match='exceeds cap'):
            parse_message(data, cap=8)

    def test_default_frame_cap_is_four_mib(self):
        assert FRAME_CAP == 4 * 1024 * 1024


class TestErrors:
    """Malformed / invalid frames raise ProtocolError."""

    def test_unknown_kind(self):
        raw  = {'version': PROTOCOL_VERSION, 'id': mint_id(), 'corr_id': None,
                'src': 'a', 'dst': 'b', 'kind': 'bogus'}
        data = msgpack.packb(raw, use_bin_type=True)
        with pytest.raises(ProtocolError, match='unknown message kind'):
            parse_message(data)

    def test_missing_required_field(self):
        # request without 'method'
        raw  = {'version': PROTOCOL_VERSION, 'id': mint_id(), 'corr_id': None,
                'src': 'a', 'dst': 'b', 'kind': 'request',
                'path': '/x', 'headers': {}, 'body': b''}
        data = msgpack.packb(raw, use_bin_type=True)
        with pytest.raises(ProtocolError, match="invalid 'request' message"):
            parse_message(data)

    def test_wrong_version(self):
        req  = Request(src='a', dst='b', method='GET', path='/x')
        raw  = req.model_dump(mode='python')
        raw['version'] = 999
        data = msgpack.packb(raw, use_bin_type=True)
        with pytest.raises(ProtocolError) as exc_info:
            parse_message(data)
        msg = str(exc_info.value)
        assert '999' in msg
        assert str(PROTOCOL_VERSION) in msg

    def test_malformed_msgpack(self):
        with pytest.raises(ProtocolError, match='malformed msgpack'):
            parse_message(b'\xff\xff\xff\xff\xff\xff')

    def test_non_mapping_frame(self):
        data = msgpack.packb([1, 2, 3], use_bin_type=True)
        with pytest.raises(ProtocolError, match='expected a mapping'):
            parse_message(data)

    def test_control_invalid_op_rejected(self):
        raw  = {'version': PROTOCOL_VERSION, 'id': mint_id(), 'corr_id': None,
                'src': 'broker', 'dst': None,
                'kind': 'control', 'op': 'reboot', 'data': {}}
        data = msgpack.packb(raw, use_bin_type=True)
        with pytest.raises(ProtocolError, match="invalid 'control' message"):
            parse_message(data)

    def test_control_valid_ops_accepted(self):
        for op in ('shutdown', 'error', 'terminate', 'disconnect'):
            msg = Control(src='broker', op=op)
            parsed, _ = _roundtrip(msg)
            assert parsed.op == op
