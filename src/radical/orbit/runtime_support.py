"""Cross-loop plumbing for the endpoint participant runtime.

Two small, test-covered building blocks factored out of ``runtime.py`` so the
runtime module stays focused on protocol/dispatch logic:

* :class:`Handoff` — the M0-validated coalesced cross-loop queue.  A producer
  thread pushes items; **at most one** ``call_soon_threadsafe`` wakeup is
  outstanding per burst (re-armed only after a drain), which is what yields the
  ~2.2 M msgs/s handoff M0 measured.  The buffer is *soft*-bounded with an
  overflow counter and never blocks the producer — the transport loop must
  never block (M0 lesson 5); the strict request backpressure (503 fast-fail)
  is applied at dispatch time, not here.

* :class:`CallbackDispatcher` — runs user callbacks on a **dedicated** thread
  fed by a bounded, drop-oldest queue.  A slow user callback must never stall
  the work loop (it is user code and can never be "disciplined"), so it is
  structurally isolated the same way the transport is.  Drops bump a counter so
  loss is observable.
"""

import logging
import threading

from collections import deque
from typing      import Any, Callable, List, Optional


log = logging.getLogger("radical.orbit.runtime")


class Handoff:
    """Coalesced, thread-safe cross-loop queue (one direction).

    *drain* is a sync callable ``(items: list) -> None`` executed **on the
    consumer loop** with every item accumulated since the last drain.  The
    consumer loop is bound after its thread is up via :meth:`bind`.
    """

    def __init__(self, drain: Callable[[List[Any]], None],
                 soft_max: int = 100000):
        self._drain    = drain
        self._soft_max = soft_max
        self._buf      : deque = deque()
        self._lock     = threading.Lock()
        self._armed    = False
        self._loop     = None            # asyncio loop of the consumer thread
        self.overflow  = 0

    def bind(self, loop) -> None:
        """Bind the consumer event loop (called once its thread is running)."""
        with self._lock:
            self._loop = loop
            # A producer may already have pushed before the loop existed;
            # arm a wakeup now so nothing is stranded.
            if self._buf and not self._armed:
                self._armed = True
                loop.call_soon_threadsafe(self._run_drain)

    def push(self, item: Any) -> None:
        """Enqueue *item* from the producer thread; arm one wakeup per burst."""
        with self._lock:
            if len(self._buf) >= self._soft_max:
                # Soft bound: count the pressure but never block the producer.
                self.overflow += 1
            self._buf.append(item)
            if not self._armed and self._loop is not None:
                self._armed = True
                self._loop.call_soon_threadsafe(self._run_drain)

    def _run_drain(self) -> None:
        with self._lock:
            items = list(self._buf)
            self._buf.clear()
            self._armed = False
        if items:
            self._drain(items)


class CallbackDispatcher:
    """Dedicated-thread runner for user callbacks (bounded, drop-oldest).

    ``submit(fn, *args)`` is called from the work loop; ``fn`` runs later on
    the dispatcher thread.  When the queue is full the *oldest* pending entry
    is evicted and :attr:`dropped` is bumped, so a wedged callback can never
    stall the caller or grow memory without bound.
    """

    def __init__(self, maxlen: int = 1024, name: str = 'orbit-callbacks'):
        self._buf     : deque = deque(maxlen=maxlen)
        self._cv      = threading.Condition()
        self._stop    = False
        self.dropped  = 0
        self._thread  = threading.Thread(target=self._run, name=name,
                                         daemon=True)

    @property
    def thread(self) -> threading.Thread:
        """The dispatcher thread (tests assert callbacks run *here*)."""
        return self._thread

    @property
    def ident(self) -> Optional[int]:
        return self._thread.ident

    def start(self) -> None:
        self._thread.start()

    def submit(self, fn: Callable, *args: Any) -> None:
        with self._cv:
            if self._buf.maxlen is not None and len(self._buf) == self._buf.maxlen:
                # deque drops the oldest on append once full.
                self.dropped += 1
            self._buf.append((fn, args))
            self._cv.notify()

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._buf and not self._stop:
                    self._cv.wait()
                if self._stop and not self._buf:
                    return
                fn, args = self._buf.popleft()
            try:
                fn(*args)
            except Exception as e:
                log.error("[Runtime] user callback failed: %r", e, exc_info=e)

    def stop(self) -> None:
        with self._cv:
            self._stop = True
            self._cv.notify()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
