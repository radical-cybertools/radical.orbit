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
add ``replay`` to the broker ``--plugins`` spec when a use case needs
replay-after-reconnect for ephemeral events.

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
effectively-once**: ``fetch`` is stateless — the consumer passes its own
``after_seq`` position each call — so a consumer that re-fetches the same
``after_seq`` simply re-reads the batch, and duplicates are discarded by
``seq``.  The server keeps no per-consumer cursor.
'''

from __future__ import annotations

import asyncio
import logging
import time

from collections import deque
from typing      import Any, Callable, Dict, List, Optional

import msgpack

from fastapi import FastAPI, Request

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
_FETCH_MAX_EVENTS  = 256                # default per-fetch event cap
_FETCH_MAX_BYTES   = 1024 * 1024        # default per-fetch byte cap
_SWEEP_INTERVAL    = 30.0              # background age-eviction sweep cadence (s)


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
    2. Drain ``replay_iter(patterns=..., session=...)``, feeding each replayed
       event through the same handler as the live callback.
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
    events were evicted before ``after_seq`` reached them: the consumer re-syncs
    state (topology + state-mirroring recover on reconnect regardless) instead
    of trusting continuity.

    ``fetch`` is stateless — the consumer tracks its own ``after_seq`` position
    (``replay_iter`` advances it via each batch's ``next_seq``), so a reconnect
    resumes simply by passing the last position it held.
    '''

    def register_session(self, **_kwargs: Any) -> None:
        '''No-op: the replay routes are all session-less.'''
        return

    def fetch(self, after_seq: int, patterns: Optional[list] = None,
              session: Optional[str] = None,
              max_events: Optional[int] = None) -> dict:
        '''Pull one batch of retained events with ``seq > after_seq``.

        Stateless: the server keeps no cursor — pass ``after_seq=-1`` to read
        the whole retained buffer, or the last ``next_seq`` to resume.  A fetch
        that repeats the same *after_seq* simply re-reads the batch (at-least-
        once).  *session* selects the per-session buffer (``None`` = the global
        ring); *patterns* filters with subscribe-pattern None-wildcard
        semantics.

        Returns ``{events, next_seq, gap}``.
        '''
        body: dict = {'after_seq': after_seq}
        if patterns   is not None: body['patterns']   = patterns
        if session    is not None: body['session']    = session
        if max_events is not None: body['max_events'] = max_events
        resp = self._http.post(self._url('fetch'), json=body)
        self._raise(resp, 'fetch')
        return resp.json()

    def stats(self) -> dict:
        '''Buffer stats: global ring + per-session (session-less route).'''
        resp = self._http.get(self._url('stats'))
        self._raise(resp, 'stats')
        return resp.json()

    def replay_iter(self, patterns: Optional[list] = None,
                    session: Optional[str] = None,
                    after_seq: int = -1,
                    max_events: Optional[int] = None):
        '''Generator that drains replay: fetch, yield events, advance, repeat.

        Fetches a batch with ``seq > after_seq``, yields its events, advances
        its own position to the batch's ``next_seq``, and stops when a batch
        comes back empty.  This is the drain step of the splice recipe (see the
        class docstring): register the live callback first, then drive this
        generator, deduping by ``seq``.  Defaults to ``after_seq=-1`` (whole
        retained buffer).
        '''
        pos = after_seq
        while True:
            resp   = self.fetch(pos, patterns=patterns, session=session,
                                max_events=max_events)
            events = resp.get('events') or []
            if not events:
                break
            for event in events:
                yield event
            pos = resp['next_seq']


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginReplay(Plugin):
    '''Broker-hosted event-replay plugin (the single retaining subscriber).

    Subscribes to the broker's raw event tap at load time (retention starts
    immediately, not on first request) and serves stateless pull-based replay:
    a consumer passes its own ``after_seq`` position each fetch.  Loaded per use
    case via the broker ``--plugins`` spec — it is **not** in the default set.
    '''

    plugin_name   = 'replay'
    session_class = PluginSession
    client_class  = ReplayClient
    version       = '0.0.1'

    # Retention tunables — injectable per instance for tests (mirrors the
    # session_ttl / reclaim_drain precedent).
    session_max_age   = _SESSION_MAX_AGE
    session_max_bytes = _SESSION_MAX_BYTES
    global_max_bytes  = _GLOBAL_MAX_BYTES
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

        # Subscribe to the raw event tap NOW so retention starts at load time
        # (a late consumer must find events already buffered).  Absent (None)
        # off the broker — is_enabled keeps us from loading there anyway.
        self._broker_tap = getattr(app.state, 'broker_tap', None)
        self._untap: Optional[Callable[[], None]] = None
        if self._broker_tap is not None:
            self._untap = self._broker_tap(self._on_event)

        # Background age sweeper — started only when a loop is already running
        # (the broker constructs hosted plugins on the running host loop).
        # Tests without a loop drive _evict_aged directly.
        self._sweeper: Optional[asyncio.Task] = None
        self._maybe_start_sweeper()

        self.add_route_post('fetch', self._route_fetch)
        self.add_route_get ('stats', self._route_stats)

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
                self._evict_aged(self._now())
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
        '''Pull a batch with ``seq > after_seq`` (stateless — no cursor).

        Body: ``{after_seq?, patterns?, session?, max_events?, max_bytes?}``.
        *after_seq* defaults to ``-1`` (whole retained buffer); the consumer
        passes its own position and advances it by the returned ``next_seq``.
        Returns ``{events, next_seq, gap}`` where ``gap`` is true when the
        buffer's lowest retained ``seq`` has risen above ``after_seq`` (events
        were evicted — re-sync, don't trust continuity).
        '''
        body = await request.json()

        patterns   = body.get('patterns') or []
        session    = body.get('session')
        after_seq  = body.get('after_seq')
        lower      = int(after_seq) if after_seq is not None else -1
        max_events = int(body.get('max_events') or self.default_max_events)
        max_bytes  = int(body.get('max_bytes')  or self.default_max_bytes)

        now = self._now()
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

    async def _route_stats(self, request: Request) -> dict:
        '''Return retention stats: the global ring + each per-session buffer.'''
        self._evict_aged(self._now())
        sessions = {sid: buf.stats()
                    for sid, buf in self._session_buffers.items()}
        return {
            'global'  : self._global.stats(),
            'sessions': sessions,
        }
