"""A small async bounded, drop-oldest queue shared across the broker.

:class:`BoundedDropOldestQueue` is the one implementation of the "a slow
consumer must never backpressure the producer" primitive used by the broker's
per-subscriber event delivery, the gateway's per-SSE-client fan-out, and the
replay plugin.  A single async producer pushes; a single async consumer drains
on wake.
"""

import asyncio

from collections import deque
from typing      import Any, AsyncIterator


class BoundedDropOldestQueue:
    """A bounded FIFO that drops the *oldest* item on overflow.

    Backed by a ``deque(maxlen)`` plus a ``dropped`` counter and an
    ``asyncio.Event`` wake.  Overflow drops the oldest item and bumps
    :attr:`dropped`, so a stalled consumer is disciplined at its own queue and
    can never backpressure the producer or grow memory without bound.

    Contract ("consumer drains on wake"):

    * :meth:`push` appends and sets :attr:`wake` (never blocks).  When the
      deque is already full the append evicts the oldest item and increments
      :attr:`dropped` — the loss is observable as a jump in :attr:`dropped`
      (and, for sequenced payloads, a gap in the consumer's ``seq``).
    * A single consumer waits on :attr:`wake`, clears it, then pops
      :attr:`buf` until empty.  :meth:`drain` packages exactly that loop as
      ``async for item in q.drain(): ...``; a consumer that needs a timed wait
      (e.g. to emit periodic keepalives) can wait on :attr:`wake` and pop
      :attr:`buf` directly instead.

    Single producer / single consumer on one event loop is assumed.
    """

    __slots__ = ('buf', 'dropped', 'wake')

    def __init__(self, maxlen: int) -> None:
        self.buf     : deque         = deque(maxlen=maxlen)
        self.dropped : int           = 0
        self.wake    : asyncio.Event = asyncio.Event()

    def push(self, item: Any) -> None:
        """Append *item*, evicting the oldest (and bumping ``dropped``) when full."""
        if len(self.buf) == self.buf.maxlen:
            self.dropped += 1                  # deque evicts the oldest on append
        self.buf.append(item)
        self.wake.set()

    async def drain(self) -> AsyncIterator[Any]:
        """Yield queued items forever, waiting on :attr:`wake` when empty."""
        while True:
            await self.wake.wait()
            self.wake.clear()
            while self.buf:
                yield self.buf.popleft()
