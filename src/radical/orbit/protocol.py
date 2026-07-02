"""
Wire protocol for broker <-> endpoint (participant) communication.

One symmetric, **versioned** envelope; ``kind`` discriminates the payload.
Wire encoding is msgpack, always (``use_bin_type=True``, ``raw=False``) — one
packer, one parser, ``body`` is native bytes (no base64, no JSON/text path).
Validation is pydantic v2: ``m0_results.md`` measured slotted dataclasses
against pydantic v2 and found no performance edge (~2.5x baseline both), so
pydantic wins on clarity and consistency with the rest of the codebase. The
broker's pure-forwarding role is the documented exception — it never builds
a model, see ``peek_routing`` below.

There is **no ``ping``/``pong`` kind** in the envelope: liveness is
WS-protocol-level only (transport keepalive on both sides), never app-level.

``channel`` is reserved for the deferred blocking-served-call / in-band-
isolation cases: the field exists on every envelope, is always ``None``
today, and carries no behaviour yet.

``seq`` (``event`` only) is broker-assigned and frozen with the envelope:
monotone per session for session-scoped events, monotone per the global
session-less stream otherwise. Consumers detect drops as ``seq`` gaps; the
optional replay plugin (later milestone) uses ``ts``/``seq`` to splice
replay into live delivery with no wire change.

``corr_id`` is only meaningful on ``request``/``response`` and is namespaced
by the requester's ``src`` (see ``mint_corr_id``) so every pending-call table
has a single, unambiguous owner per entry.

Frame-size cap: ``FRAME_CAP`` bounds the packed frame by the design
inequality ``frame_cap <= heartbeat_timeout * min_bandwidth / 2`` (e.g. a 3 s
pong timeout and a 3 MB/s link floor a ~4 MB cap). The same cap doubles as a
bound on GIL-hold time: M0 measured a 4 MB ``msgpack.unpackb`` pinning the
GIL ~66 ms with no release, so capping the frame size also caps how long a
single inbound frame can stall any thread — including the transport thread
that answers keepalive pings.

Version mismatches are rejected outright at ``parse_message`` time.
Version *negotiation* (letting old and new peers agree on a shared
protocol version) is not implemented here — it rides ``register`` /
``register_ack`` ``capabilities`` in a later milestone; today a mismatch is
simply a hard error.
"""

import uuid

from typing import Any, Dict, List, Literal, Optional, Union

import msgpack

from pydantic import BaseModel, ConfigDict, Field, ValidationError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROTOCOL_VERSION = 1

# Default frame-size cap in bytes -- see the module docstring for the
# design inequality and the GIL-hold rationale.
FRAME_CAP = 4 * 1024 * 1024


class ProtocolError(Exception):
    """Raised on any envelope pack/parse failure.

    Covers: oversized frame, malformed msgpack, unknown ``kind``,
    missing/invalid fields, and protocol version mismatch.
    """


# ---------------------------------------------------------------------------
# id / corr_id helpers
# ---------------------------------------------------------------------------

def mint_id() -> str:
    """Mint a fresh envelope id (``uuid4``)."""
    return str(uuid.uuid4())


def mint_corr_id(src: str) -> str:
    """Mint a correlation id namespaced by *src*.

    corr_ids are namespaced by the requesting ``src`` so that every pending-
    call table (one per broker-side ``src`` bucket, one per endpoint) has a
    single, unambiguous owner per entry -- timeout/cleanup never has to guess
    which side's table an id belongs to.
    """
    return f"{src}:{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Nested models
# ---------------------------------------------------------------------------

class Identity(BaseModel):
    """Participant identity presented at ``register``."""
    model_config = ConfigDict(extra='forbid')

    name:       str
    credential: Optional[str] = None
    resume_key: Optional[str] = None


class SubscribePattern(BaseModel):
    """One ``subscribe``/``unsubscribe`` interest pattern.

    A ``None`` field is a wildcard -- mirrors the client callback-registry
    tuples (``endpoint_id``/``plugin_name``/``topic`` all optional).
    """
    model_config = ConfigDict(extra='forbid')

    endpoint: Optional[str] = None
    plugin:   Optional[str] = None
    topic:    Optional[str] = None


class ParticipantInfo(BaseModel):
    """One entry of a ``topology`` envelope's ``participants`` dict.

    ``plugins`` is a free-form dict keyed by plugin name -- each value
    carries whatever the hosting plugin publishes (``namespace``,
    ``version``, ``ui_config``, ``enabled``, ...); this module does not
    constrain its shape.
    """
    model_config = ConfigDict(extra='forbid')

    role:     str
    plugins:  Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    liveness: str


# ---------------------------------------------------------------------------
# Envelope base
# ---------------------------------------------------------------------------

class _Envelope(BaseModel):
    """Common envelope fields shared by every ``kind``."""
    model_config = ConfigDict(extra='forbid')

    version: int           = PROTOCOL_VERSION
    id:      str           = Field(default_factory=mint_id)
    corr_id: Optional[str] = None
    channel: Optional[str] = None   # reserved; always None today
    src:     str
    dst:     Optional[str] = None


# ---------------------------------------------------------------------------
# Per-kind envelopes
# ---------------------------------------------------------------------------

class Request(_Envelope):
    """Proxied HTTP-shaped request, routed by ``dst``."""

    kind:      Literal['request'] = 'request'
    method:    str
    path:      str
    headers:   Dict[str, str] = Field(default_factory=dict)
    body:      bytes          = b''
    is_binary: bool            = False


class Response(_Envelope):
    """Reply to a ``request``, correlated via ``corr_id``."""

    kind:      Literal['response'] = 'response'
    status:    int
    headers:   Dict[str, str] = Field(default_factory=dict)
    body:      bytes          = b''
    is_binary: bool            = False


class Event(_Envelope):
    """A plugin-originated notification.

    ``seq`` is broker-assigned: monotone per session for session-scoped
    events (``session`` set), monotone per the global session-less stream
    otherwise (``session`` is ``None``).
    """

    kind:    Literal['event'] = 'event'
    plugin:  str
    topic:   str
    session: Optional[str] = None
    ts:      float
    seq:     int
    data:    Dict[str, Any] = Field(default_factory=dict)


class Register(_Envelope):
    """Endpoint -> broker registration handshake."""

    kind:         Literal['register'] = 'register'
    identity:     Identity
    role:         str
    plugins:      List[Dict[str, Any]] = Field(default_factory=list)
    capabilities: Dict[str, Any]       = Field(default_factory=dict)


class RegisterAck(_Envelope):
    """Broker -> endpoint reply to ``register``.

    ``resume_key`` is minted on first registration of an identity (broker
    memory only); a reconnect presenting it replaces the stale socket.
    """

    kind:         Literal['register_ack'] = 'register_ack'
    ok:           bool
    reason:       Optional[str] = None
    capabilities: Dict[str, Any] = Field(default_factory=dict)
    resume_key:   str


class Subscribe(_Envelope):
    """Register live-event interest patterns."""

    kind:     Literal['subscribe'] = 'subscribe'
    patterns: List[SubscribePattern] = Field(default_factory=list)


class Unsubscribe(_Envelope):
    """Withdraw live-event interest patterns (same shape as ``subscribe``)."""

    kind:     Literal['unsubscribe'] = 'unsubscribe'
    patterns: List[SubscribePattern] = Field(default_factory=list)


class Topology(_Envelope):
    """Full or partial topology snapshot."""

    kind:         Literal['topology'] = 'topology'
    participants: Dict[str, ParticipantInfo] = Field(default_factory=dict)


class Control(_Envelope):
    """Administrative operation (shutdown/error/terminate/disconnect)."""

    kind: Literal['control'] = 'control'
    op:   Literal['shutdown', 'error', 'terminate', 'disconnect']
    data: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Union type for annotations + kind -> model dispatch
# ---------------------------------------------------------------------------

Message = Union[
    Request, Response, Event, Register, RegisterAck,
    Subscribe, Unsubscribe, Topology, Control,
]


_KIND_TO_MODEL = {
    'request':       Request,
    'response':      Response,
    'event':         Event,
    'register':      Register,
    'register_ack':  RegisterAck,
    'subscribe':     Subscribe,
    'unsubscribe':   Unsubscribe,
    'topology':      Topology,
    'control':       Control,
}


# ---------------------------------------------------------------------------
# pack / parse
# ---------------------------------------------------------------------------

def pack_message(msg: BaseModel, cap: int = FRAME_CAP) -> bytes:
    """Serialize an envelope model to a msgpack frame.

    Raises ``ProtocolError`` if the packed frame exceeds *cap* bytes.
    """
    data = msg.model_dump(mode='python')
    try:
        packed = msgpack.packb(data, use_bin_type=True)
    except Exception as e:
        raise ProtocolError(f"failed to pack {msg.kind!r} message: {e}") from e

    if len(packed) > cap:
        raise ProtocolError(
            f"frame size {len(packed)} bytes exceeds cap {cap} bytes")

    return packed


def parse_message(data: bytes, cap: int = FRAME_CAP) -> Message:
    """Parse + validate a msgpack frame into its ``kind``-specific model.

    Raises ``ProtocolError`` on: oversized frame, malformed msgpack, unknown
    ``kind``, missing/invalid fields, or a protocol version mismatch.
    """
    if len(data) > cap:
        raise ProtocolError(
            f"frame size {len(data)} bytes exceeds cap {cap} bytes")

    try:
        raw = msgpack.unpackb(data, raw=False)
    except Exception as e:
        raise ProtocolError(f"malformed msgpack frame: {e}") from e

    if not isinstance(raw, dict):
        raise ProtocolError(
            f"malformed frame: expected a mapping, got {type(raw).__name__}")

    version = raw.get('version')
    if version != PROTOCOL_VERSION:
        raise ProtocolError(
            f"protocol version mismatch: frame is v{version!r}, this "
            f"process speaks v{PROTOCOL_VERSION} (version negotiation "
            "rides register/register_ack capabilities, not yet implemented)")

    kind = raw.get('kind')
    cls  = _KIND_TO_MODEL.get(kind)
    if cls is None:
        raise ProtocolError(f"unknown message kind: {kind!r}")

    try:
        return cls(**raw)
    except ValidationError as e:
        raise ProtocolError(f"invalid {kind!r} message: {e}") from e


def peek_routing(data: bytes):
    """Extract routing fields from a msgpack frame **without** validation.

    This is the broker's pure-forwarding fast path: it routes by
    ``kind``/``src``/``dst``/``corr_id`` and never inspects or mutates
    bodies, so it has no reason to pay pydantic validation cost or to build
    a model at all -- a plain msgpack unpack is enough (and, unlike
    ``parse_message``, tolerates an otherwise-invalid frame as long as the
    routing fields are present).

    Returns ``(kind, src, dst, corr_id)``.
    """
    try:
        raw = msgpack.unpackb(data, raw=False)
    except Exception as e:
        raise ProtocolError(f"malformed msgpack frame: {e}") from e

    if not isinstance(raw, dict):
        raise ProtocolError(
            f"malformed frame: expected a mapping, got {type(raw).__name__}")

    return raw.get('kind'), raw.get('src'), raw.get('dst'), raw.get('corr_id')


# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------

def make_request(src: str, dst: str, method: str, path: str, *,
                  headers:   Optional[Dict[str, str]] = None,
                  body:      bytes                    = b'',
                  is_binary: bool                      = False,
                  corr_id:   Optional[str]              = None) -> Request:
    """Build a ``request`` envelope; mints a namespaced ``corr_id`` by default."""
    return Request(
        src=src, dst=dst, method=method, path=path,
        headers=headers or {}, body=body, is_binary=is_binary,
        corr_id=corr_id or mint_corr_id(src))


def make_response(request: Request, status: int, *,
                   headers:   Optional[Dict[str, str]] = None,
                   body:      bytes                    = b'',
                   is_binary: bool                      = False) -> Response:
    """Build the ``response`` envelope for *request*.

    ``src``/``dst`` are the request's swapped; ``corr_id`` is copied so the
    caller's pending-table lookup resolves.
    """
    return Response(
        src=request.dst, dst=request.src, status=status,
        headers=headers or {}, body=body, is_binary=is_binary,
        corr_id=request.corr_id)
