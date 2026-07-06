'''
Event-replay plugin — the optional retaining subscriber for radical.orbit.

The broker core is deliberately **stateless** about events: it stamps every
``event`` frame with an authoritative ``seq``/``ts`` (monotone per session for
session-scoped events, monotone per the global session-less stream otherwise),
fans it out to live subscribers, and forgets it.  ``PluginReplay`` is the single
component that *retains* the stream so a consumer that connected late — or
dropped and reconnected — can pull what it missed.

Because the wire already carries everything replay needs (``ts``/``seq`` are
frozen with the envelope), this plugin needs **no protocol change**: it is a
pure consumer of the broker's raw event tap (``app.state.broker_tap`` — see
:mod:`radical.orbit.broker_events`), which delivers *every* event dict on the
plugin-host loop, including events for endpoints that are not connected.  That
is why replay must be **broker-hosted**: a retaining subscriber has to see the
whole stream, not just its own endpoint's slice.

Replay is **not** in the broker's default plugin set — the core stays stateless
and this plugin is the one retaining subscriber, loaded per use case.  Operators
add ``replay`` to the broker ``--plugins`` spec when a use case needs durable
ephemeral-event delivery.

Retention
---------
* **Session-scoped events** (``event['session']`` set): one bounded buffer per
  session, capped by BOTH an age (``session_max_age``, default 600 s) and a
  byte size (``session_max_bytes``, default 4 MB, measured as the msgpack size
  of the event dict).  Per-session isolation means one chatty session cannot
  evict another's history.
* **Session-less events** (``event['session']`` is ``None``): one global ring
  capped by total bytes (``global_max_bytes``, default 16 MB).

Every eviction — age or size — increments the buffer's ``dropped`` counter, so
a gap is *observable*: the ``stats`` route exposes retained count, byte size,
dropped, and lowest/highest ``seq`` for the global ring and each per-session
buffer.

Delivery is **at-least-once on the wire with ``seq`` dedup at the consumer =
effectively-once**: a per-consumer cursor advances only on ACK, so a drop
mid-replay re-sends rather than loses, and the consumer discards duplicates by
``seq``.
'''

from __future__ import annotations

import asyncio
import logging
import time

from collections import deque
from typing      import Any, Callable, Dict, List, Optional

import msgpack

from fastapi import FastAPI, HTTPException, Request

from .client               import PluginClient
from .plugin_base          import Plugin
from .plugin_session_base  import PluginSession

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults (class attributes on the plugin; injectable per instance for tests)
# ---------------------------------------------------------------------------

_SESSION_MAX_AGE   = 600.0              # per-session buffer age bound  (s)
_SESSION_MAX_BYTES = 4  * 1024 * 1024   # per-session buffer size bound (bytes)
_GLOBAL_MAX_BYTES  = 16 * 1024 * 1024   # global session-less ring bound (bytes)
_CURSOR_TTL        = 3600.0             # cursor inactivity expiry (s)
_FETCH_MAX_EVENTS  = 256                # default per-fetch event cap
_FETCH_MAX_BYTES   = 1024 * 1024        # default per-fetch byte cap
_SWEEP_INTERVAL    = 30.0              # background age/cursor sweep cadence (s)


# ---------------------------------------------------------------------------
# _RingBuffer — one bounded, drop-oldest retention buffer
# ---------------------------------------------------------------------------

class _RingBuffer:
    '''A bounded, drop-oldest event buffer with a dropped-events counter.

    Entries are ``(seq, ts, size, event)`` tuples held oldest-first.  A
    per-session buffer sets ``max_age``; the global ring leaves it ``None``
    (bytes-only).  Both enforce ``max_bytes``.  Every eviction bumps
    :attr:`dropped` so a consumer can see the loss as a ``seq`` discontinuity.
    '''

    def __init__(self, max_bytes: int, max_age: Optional[float] = None) -> None:
        self.max_bytes = max_bytes
        self.max_age   = max_age
        self.events: deque = deque()   # (seq, ts, size, event)
        self.nbytes  = 0
        self.dropped = 0

    def append(self, seq: int, ts: float, size: int,
               event: Dict[str, Any], now: float) -> None:
        self.events.append((seq, ts, size, event))
        self.nbytes += size
        self._enforce(now)

    def _drop_left(self) -> None:
        _, _, size, _ = self.events.popleft()
        self.nbytes  -= size
        self.dropped += 1

    def _enforce(self, now: float) -> None:
        if self.max_age is not None:
            while self.events and (now - self.events[0][1]) > self.max_age:
                self._drop_left()
        while self.nbytes > self.max_bytes and self.events:
            self._drop_left()

    def evict_aged(self, now: float) -> None:
        '''Lazily drop age-expired entries (called on any access + by sweep).'''
        if self.max_age is None:
            return
        while self.events and (now - self.events[0][1]) > self.max_age:
            self._drop_left()

    @property
    def lowest_seq(self) -> Optional[int]:
        return self.events[0][0] if self.events else None

    @property
    def highest_seq(self) -> Optional[int]:
        return self.events[-1][0] if self.events else None

    def stats(self) -> Dict[str, Any]:
        return {
            'retained'   : len(self.events),
            'bytes'      : self.nbytes,
            'dropped'    : self.dropped,
            'lowest_seq' : self.lowest_seq,
            'highest_seq': self.highest_seq,
        }

    def select(self, after_exclusive: int, patterns: List[dict],
               max_events: int, max_bytes: int) -> List[Dict[str, Any]]:
        '''Return matching events with ``seq > after_exclusive``, oldest-first.

        Bounded by *max_events* / *max_bytes*, but always returns at least the
        first matching event so a single oversized event can never stall the
        cursor.
        '''
        out: List[Dict[str, Any]] = []
        nbytes = 0
        for seq, _ts, size, event in self.events:
            if seq <= after_exclusive:
                continue
            if not _event_matches(patterns, event):
                continue
            if out and (len(out) >= max_events or nbytes + size > max_bytes):
                break
            out.append(event)
            nbytes += size
        return out


# ---------------------------------------------------------------------------
# _Cursor — per-consumer replay position
# ---------------------------------------------------------------------------

class _Cursor:
    '''A consumer's replay position: the highest ``seq`` it has ACKed.

    Keyed by a client-chosen ``cursor_id``.  A *stable* consumer name makes
    reconnect-resume work — the same cursor_id picks up exactly where the
    consumer left off — mirroring the stable-endpoint-name reattach story.
    The position advances only on ACK; it starts at ``-1`` so a fresh cursor
    (no ``after_seq``) replays the whole retained buffer.
    '''

    __slots__ = ('position', 'last_access')

    def __init__(self, position: int, last_access: float) -> None:
        self.position    = position
        self.last_access = last_access


# ---------------------------------------------------------------------------
# pattern matching — same None-wildcard semantics as protocol.SubscribePattern
# ---------------------------------------------------------------------------

def _event_matches(patterns: List[dict], event: Dict[str, Any]) -> bool:
    '''True if *event* matches any of *patterns* (empty/None -> match all).

    A pattern is ``{endpoint?, plugin?, topic?}``; a ``None``/absent field is a
    wildcard.  ``endpoint`` matches the event's broker-stamped ``src``.  This is
    the same OR-of-patterns, AND-of-fields rule the broker's subscription
    registry and the client callback registry use.
    '''
    if not patterns:
        return True
    src   = event.get('src')
    plug  = event.get('plugin')
    topic = event.get('topic')
    for p in patterns:
        if p.get('endpoint') is not None and p['endpoint'] != src:
            continue
        if p.get('plugin')   is not None and p['plugin']   != plug:
            continue
        if p.get('topic')    is not None and p['topic']    != topic:
            continue
        return True
    return False


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ReplayClient(PluginClient):
    '''Consumer-side helper for the event-replay plugin (session-less).

    Replay→live splice
    ------------------
    To receive every event across a late join or reconnect **exactly once**,
    the consumer splices replay into live delivery:

    1. Register the live callback **first**
       (``runtime.register_callback(endpoint=..., plugin=..., topic=...)``), so
       no live event is missed while replay drains.
    2. Drain ``replay_iter(cursor_id, patterns=..., session=...)``, feeding each
       replayed event through the same handler as the live callback.
    3. **Dedup by ``seq``.**  Replayed events carry ``event['seq']``; keep a set
       of seen ``seq`` (per stream) and skip any already seen.  Live delivery
       now exposes the same broker-stamped ``seq`` too: register the live
       callback with ``runtime.register_callback(..., with_meta=True)`` and read
       ``meta['seq']`` (the identical authoritative value this plugin retains),
       so the dedup keys on the broker ``seq`` on both streams — no application
       key needed.

    Because live delivery starts *before* replay finishes, the two streams
    overlap around the join point; the ``seq`` dedup discards the duplicates, so
    the handler observes each event once — at-least-once on the wire + ``seq``
    dedup = effectively-once.  A ``fetch`` reporting ``gap=True`` means retained
    events were evicted before the cursor reached them: the consumer re-syncs
    state (topology + state-mirroring recover on reconnect regardless) instead
    of trusting continuity.

    A stable ``cursor_id`` (a consumer name) makes reconnect-resume work: the
    cursor picks up where the last ACK left off.
    '''

    def register_session(self, **_kwargs: Any) -> None:
        '''No-op: the replay routes are all session-less.'''
        return

    def fetch(self, cursor_id: str, patterns: Optional[list] = None,
              session: Optional[str] = None, after_seq: Optional[int] = None,
              ack_seq: Optional[int] = None,
              max_events: Optional[int] = None) -> dict:
        '''Pull one batch of retained events for *cursor_id*.

        *ack_seq*, when given, advances the stored cursor to that ``seq``
        **before** the next batch is selected (a batch fetched without an ACK is
        re-delivered on the next fetch — the at-least-once property).  *session*
        selects the per-session buffer (``None`` = the global ring).  *patterns*
        filters with the same None-wildcard semantics as a subscribe pattern.

        Returns ``{events, next_seq, gap}``.
        '''
        body: dict = {'cursor_id': cursor_id}
        if patterns   is not None: body['patterns']   = patterns
        if session    is not None: body['session']    = session
        if after_seq  is not None: body['after_seq']  = after_seq
        if ack_seq    is not None: body['ack_seq']    = ack_seq
        if max_events is not None: body['max_events'] = max_events
        resp = self._http.post(self._url('fetch'), json=body)
        self._raise(resp, 'fetch')
        return resp.json()

    def drop_cursor(self, cursor_id: str) -> dict:
        '''Forget a cursor's stored position (cleanup).'''
        resp = self._http.post(self._url('drop_cursor'),
                               json={'cursor_id': cursor_id})
        self._raise(resp, 'drop_cursor')
        return resp.json()

    def stats(self) -> dict:
        '''Buffer stats: global ring + per-session (session-less route).'''
        resp = self._http.get(self._url('stats'))
        self._raise(resp, 'stats')
        return resp.json()

    def replay_iter(self, cursor_id: str, patterns: Optional[list] = None,
                    session: Optional[str] = None,
                    after_seq: Optional[int] = None,
                    max_events: Optional[int] = None):
        '''Generator that drains replay: fetch, yield each event, ACK, repeat.

        Fetches a batch, yields its events, ACKs it (so the cursor advances),
        and stops when a batch comes back empty.  This is the drain step of the
        splice recipe (see the class docstring): register the live callback
        first, then drive this generator, deduping by ``seq``.
        '''
        ack: Optional[int] = None
        first = True
        while True:
            resp = self.fetch(cursor_id, patterns=patterns, session=session,
                              after_seq=(after_seq if first else None),
                              ack_seq=ack, max_events=max_events)
            first  = False
            events = resp.get('events') or []
            if not events:
                break
            for event in events:
                yield event
            ack = resp['next_seq']


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginReplay(Plugin):
    '''Broker-hosted event-replay plugin (the single retaining subscriber).

    Subscribes to the broker's raw event tap at load time (retention starts
    immediately, not on first request) and serves pull-based replay with
    per-consumer cursors.  Loaded per use case via the broker ``--plugins``
    spec — it is **not** in the default set.
    '''

    plugin_name   = 'replay'
    session_class = PluginSession
    client_class  = ReplayClient
    version       = '0.0.1'

    # Retention / cursor tunables — injectable per instance for tests (mirrors
    # the session_ttl / reclaim_drain precedent).
    session_max_age   = _SESSION_MAX_AGE
    session_max_bytes = _SESSION_MAX_BYTES
    global_max_bytes  = _GLOBAL_MAX_BYTES
    cursor_ttl        = _CURSOR_TTL
    default_max_events = _FETCH_MAX_EVENTS
    default_max_bytes  = _FETCH_MAX_BYTES
    sweep_interval    = _SWEEP_INTERVAL

    ui_config = {
        'icon'       : '🎞️',
        'title'      : 'Event Replay',
        'description': 'Retains the broker event stream for replay to '
                       'late/reconnecting consumers.',
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Broker hosts only, and only when the raw event tap is wired.

        A retaining subscriber must see the whole event stream, which only the
        broker's tap provides — so replay is disabled off the broker and
        disabled when the tap is absent (e.g. a host with no ``broker_tap``).
        '''
        from .utils import host_role
        if host_role(app)['role'] != 'broker':
            return False
        return getattr(app.state, 'broker_tap', None) is not None

    def __init__(self, app: FastAPI, instance_name: str = 'replay') -> None:
        super().__init__(app, instance_name)

        self._now: Callable[[], float] = time.time

        # Global ring (session-less broadcasts) + per-session buffers.
        self._global = _RingBuffer(self.global_max_bytes)
        self._session_buffers: Dict[str, _RingBuffer] = {}

        # Per-consumer cursors, keyed by client-chosen cursor_id.
        self._cursors: Dict[str, _Cursor] = {}

        # Subscribe to the raw event tap NOW so retention starts at load time
        # (a late consumer must find events already buffered).  Absent (None)
        # off the broker — is_enabled keeps us from loading there anyway.
        self._broker_tap = getattr(app.state, 'broker_tap', None)
        self._untap: Optional[Callable[[], None]] = None
        if self._broker_tap is not None:
            self._untap = self._broker_tap(self._on_event)

        # Background age/cursor sweeper — started only when a loop is already
        # running (the broker constructs hosted plugins on the running host
        # loop).  Tests without a loop drive _evict/_prune_cursors directly.
        self._sweeper: Optional[asyncio.Task] = None
        self._maybe_start_sweeper()

        self.add_route_post('fetch',       self._route_fetch)
        self.add_route_post('drop_cursor', self._route_drop_cursor)
        self.add_route_get ('stats',       self._route_stats)

    # -- retention (raw-tap consumer; runs on the plugin-host loop) --------

    def _on_event(self, event: Dict[str, Any]) -> None:
        '''Retain one broker event (fed by the raw tap on the host loop).

        Session-scoped events go to a per-session buffer (age+size bounded);
        session-less events go to the global ring (bytes bounded).  The size
        measure is the msgpack-encoded size of the event dict.
        '''
        if event.get('kind') != 'event':
            return
        seq = event.get('seq')
        if seq is None:
            return
        now = self._now()
        ts  = event.get('ts') or now
        try:
            size = len(msgpack.packb(event, use_bin_type=True))
        except Exception:
            size = 0

        session = event.get('session')
        if session:
            buf = self._session_buffers.get(session)
            if buf is None:
                buf = _RingBuffer(self.session_max_bytes, self.session_max_age)
                self._session_buffers[session] = buf
        else:
            buf = self._global
        buf.append(int(seq), float(ts), size, event, now)

    # -- housekeeping (injectable timers; sweep + lazy on access) ----------

    def _evict_aged(self, now: float) -> None:
        self._global.evict_aged(now)
        for buf in list(self._session_buffers.values()):
            buf.evict_aged(now)

    def _prune_cursors(self, now: float) -> int:
        '''Drop cursors idle longer than ``cursor_ttl``.  Returns the count.'''
        stale = [cid for cid, cur in self._cursors.items()
                 if now - cur.last_access > self.cursor_ttl]
        for cid in stale:
            self._cursors.pop(cid, None)
        return len(stale)

    def _maybe_start_sweeper(self) -> None:
        if self._sweeper is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._sweeper = loop.create_task(self._sweep_loop())

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.sweep_interval)
                now = self._now()
                self._evict_aged(now)
                self._prune_cursors(now)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] replay sweeper error: %s',
                              self.instance_name, e)

    async def shutdown(self) -> None:
        '''Cancel the background sweeper and drop the raw-event tap, then run
        the base teardown (cleanup task + sessions).  Keeps the host loop free
        of pending plugin tasks on broker shutdown.'''
        if self._sweeper is not None and not self._sweeper.done():
            self._sweeper.cancel()
            try:    await self._sweeper
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._untap is not None:
            try:    self._untap()
            except Exception:
                pass
            self._untap = None
        await super().shutdown()

    # -- routes ------------------------------------------------------------

    async def _route_fetch(self, request: Request) -> dict:
        '''Pull a batch for a cursor; ACK-advance first, then select.

        Body: ``{cursor_id, patterns?, session?, after_seq?, ack_seq?,
        max_events?, max_bytes?}``.  Returns ``{events, next_seq, gap}`` where
        ``gap`` is true when the cursor position has fallen below the buffer's
        lowest retained ``seq`` (events were evicted — re-sync, don't trust
        continuity).
        '''
        body = await request.json()
        cursor_id = body.get('cursor_id')
        if not cursor_id:
            raise HTTPException(status_code=400,
                                detail="fetch requires 'cursor_id'")

        patterns   = body.get('patterns') or []
        session    = body.get('session')
        after_seq  = body.get('after_seq')
        ack_seq    = body.get('ack_seq')
        max_events = int(body.get('max_events') or self.default_max_events)
        max_bytes  = int(body.get('max_bytes')  or self.default_max_bytes)

        now = self._now()
        cur = self._cursors.get(cursor_id)
        if cur is None:
            cur = _Cursor(position=-1, last_access=now)
            self._cursors[cursor_id] = cur
        cur.last_access = now

        # ACK advances the persistent cursor FIRST — a batch fetched without an
        # ACK is therefore re-delivered on the next fetch (at-least-once).
        if ack_seq is not None:
            cur.position = max(cur.position, int(ack_seq))

        lower = int(after_seq) if after_seq is not None else cur.position

        buf = (self._global if session is None
               else self._session_buffers.get(session))
        if buf is None:
            return {'events': [], 'next_seq': lower, 'gap': False}
        buf.evict_aged(now)

        events   = buf.select(lower, patterns, max_events, max_bytes)
        lowest   = buf.lowest_seq
        gap      = lowest is not None and lowest > lower + 1
        next_seq = events[-1]['seq'] if events else lower
        return {'events': events, 'next_seq': next_seq, 'gap': gap}

    async def _route_drop_cursor(self, request: Request) -> dict:
        body = await request.json()
        cursor_id = body.get('cursor_id')
        existed = self._cursors.pop(cursor_id, None) is not None
        return {'dropped': existed}

    async def _route_stats(self, request: Request) -> dict:
        now = self._now()
        self._evict_aged(now)
        sessions = {sid: buf.stats()
                    for sid, buf in self._session_buffers.items()}
        return {
            'global'  : self._global.stats(),
            'sessions': sessions,
            'cursors' : len(self._cursors),
        }
