"""Broker live-event delivery + subscription machinery.

Factored out of :mod:`radical.orbit.broker` so the routing loop's core stays
lean and readable.  :class:`EventRouter` owns everything about *events*: the
per-endpoint subscription interest sets, the broker-assigned ``seq``/``ts``
stamping, the per-subscriber bounded (drop-oldest) delivery queues with their
dedicated sender tasks, and the unfiltered raw tap.

Guarantees:

* A slow subscriber can never backpressure the routing loop or other
  subscribers — enqueue is non-blocking; overflow drops the *oldest* frame and
  bumps a per-subscriber counter so the consumer sees the loss as a ``seq`` gap
  (see :class:`radical.orbit.queues.BoundedDropOldestQueue`).
* ``seq`` is broker-assigned and frozen with the envelope: monotone per session
  for session-scoped events, monotone per the global stream otherwise.
* The raw tap sees *every* event (stateless, unfiltered) and runs on the
  plugin-host loop — never the routing loop.
"""

import asyncio
import logging
import time

from typing import Any, Callable, Dict, List, Optional

import msgpack

from . import protocol

from .queues import BoundedDropOldestQueue


log = logging.getLogger("radical.orbit.broker")


class EventRouter:
    """Owns the broker's subscription registry + event delivery.

    Args:
      registry:   the broker's live ``name -> transport`` dict (shared ref).
      spawn:      the broker's supervised-task creator.
      prof:       the broker's profiler (``broker_event_*`` sites).
      event_queue: per-subscriber delivery queue depth.
      host_loop_getter: returns the plugin-host loop (taps run there).
    """

    def __init__(self, registry: Dict[str, Any], spawn: Callable,
                 prof, event_queue: int,
                 host_loop_getter: Callable[[], Optional[asyncio.AbstractEventLoop]]):
        self._registry    = registry
        self._spawn       = spawn
        self._prof        = prof
        self._event_queue = event_queue
        self._host_loop   = host_loop_getter

        self._subscriptions: Dict[str, List[protocol.SubscribePattern]] = {}
        self._out:           Dict[str, BoundedDropOldestQueue] = {}
        self._senders:       Dict[str, asyncio.Task] = {}
        # One monotone seq counter per session-id ever seen.  Intentionally
        # never pruned: sessions are re-registrable, and a stale counter keeps a
        # re-registered session from re-using seq numbers a consumer already saw.
        self._session_seq:   Dict[str, int]          = {}
        self._global_seq:    int                     = 0
        self._taps:          list                    = []

    # ── endpoint lifecycle ────────────────────────────────────────────

    def add_endpoint(self, name: str) -> None:
        self._subscriptions.setdefault(name, [])
        self._out[name] = BoundedDropOldestQueue(self._event_queue)
        self.restart_sender(name)

    def remove_endpoint(self, name: str) -> None:
        self._subscriptions.pop(name, None)
        self._out.pop(name, None)
        self.pause_sender(name)
        self._senders.pop(name, None)

    def pause_sender(self, name: str) -> None:
        s = self._senders.get(name)
        if s and not s.done():
            s.cancel()

    def restart_sender(self, name: str) -> None:
        self.pause_sender(name)
        self._senders[name] = self._spawn(self._sender(name),
                                          'broker_sender:%s' % name)

    async def _sender(self, name: str) -> None:
        oq = self._out.get(name)
        if oq is None:
            return
        async for data in oq.drain():
            ws = self._registry.get(name)
            if ws is None:
                return
            try:
                await ws.send_bytes(data)
            except Exception as e:
                log.debug("[Broker] event send to %s failed: %s", name, e)
                return

    def dropped(self, name: str) -> int:
        oq = self._out.get(name)
        return oq.dropped if oq else 0

    # ── subscriptions ─────────────────────────────────────────────────

    def update_subscription(self, name: str, raw: dict, add: bool) -> None:
        patterns = [protocol.SubscribePattern(**p)
                    for p in raw.get('patterns', [])]
        cur = self._subscriptions.setdefault(name, [])
        if add:
            cur.extend(patterns)
        else:
            for p in patterns:
                if p in cur:
                    cur.remove(p)

    @staticmethod
    def _matches(patterns: List[protocol.SubscribePattern],
                 endpoint: str, plugin: Optional[str],
                 topic: Optional[str]) -> bool:
        for p in patterns:
            if p.endpoint is not None and p.endpoint != endpoint:
                continue
            if p.plugin   is not None and p.plugin   != plugin:
                continue
            if p.topic    is not None and p.topic    != topic:
                continue
            return True
        return False

    # ── raw tap ───────────────────────────────────────────────────────

    def tap(self, callback: Callable[[Dict[str, Any]], Any]) -> Callable[[], None]:
        self._taps.append(callback)

        def _remove() -> None:
            try:    self._taps.remove(callback)
            except ValueError:
                pass
        return _remove

    def _dispatch_tap(self, cb: Callable, event: Dict[str, Any]) -> None:
        loop = self._host_loop()
        if loop is None:
            return

        async def _run() -> None:
            try:
                res = cb(event)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                log.error("[Broker] tap callback failed: %r", e, exc_info=e)

        asyncio.run_coroutine_threadsafe(_run(), loop)

    # ── ingest ────────────────────────────────────────────────────────

    def ingest(self, src_name: str, raw: dict) -> None:
        """Stamp ``seq``/``ts``, fan out to matching subscribers, feed the tap.

        Runs on the routing loop; every step is a dict op or a non-blocking
        enqueue, so a storm never stalls routing.
        """
        raw['src'] = src_name
        session    = raw.get('session')
        if session:
            seq = self._session_seq.get(session, 0)
            self._session_seq[session] = seq + 1
            raw['seq'] = seq
        else:
            raw['seq'] = self._global_seq
            self._global_seq += 1
        raw['ts'] = time.time()   # broker is the ts authority; overwrite always

        self._prof.prof('broker_event_in', uid=str(raw['seq']),
                        msg='%s/%s' % (raw.get('plugin'), raw.get('topic')))

        packed = msgpack.packb(raw, use_bin_type=True)
        plugin = raw.get('plugin')
        topic  = raw.get('topic')
        for sub_name, patterns in self._subscriptions.items():
            if self._matches(patterns, src_name, plugin, topic):
                oq = self._out.get(sub_name)
                if oq is not None:
                    oq.push(packed)
                    self._prof.prof('broker_event_out', uid=str(raw['seq']),
                                    msg=sub_name)

        for cb in list(self._taps):
            self._dispatch_tap(cb, dict(raw))
