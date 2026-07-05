"""Callback isolation for the endpoint participant runtime.

:class:`CallbackDispatcher` runs user notification callbacks on a **dedicated**
daemon thread, fed by a bounded queue.  A user callback runs arbitrary code — it
may block, or even call back into the runtime — so it must never run on the
transport or work loop: a slow callback there would stall request handling or
liveness.  Structural isolation on its own thread is the guarantee.

The queue is bounded so a wedged callback cannot grow memory without bound; an
overflowing submission is dropped with a logged warning (and counted in
:attr:`CallbackDispatcher.dropped`) so the loss is visible rather than silent.
"""

import logging
import queue
import threading

from typing import Any, Callable, Optional


log = logging.getLogger("radical.orbit.runtime")


class CallbackDispatcher:
    """Dedicated-thread runner for user callbacks (bounded queue).

    ``submit(fn, *args)`` is called from the work loop; ``fn`` runs later on the
    dispatcher thread.  When the queue is full the submission is dropped, a
    warning is logged, and :attr:`dropped` is bumped — so a wedged callback can
    never stall the caller or grow memory without bound, and the loss is
    observable.
    """

    def __init__(self, maxlen: int = 1024, name: str = 'orbit-callbacks'):
        self._q      : queue.Queue = queue.Queue(maxsize=maxlen)
        self.dropped = 0
        self._thread = threading.Thread(target=self._run, name=name,
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
        try:
            self._q.put_nowait((fn, args))
        except queue.Full:
            self.dropped += 1
            log.warning("[Runtime] callback queue full; dropping callback %r",
                        getattr(fn, '__name__', fn))

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is None:                 # stop sentinel
                return
            fn, args = item
            try:
                fn(*args)
            except Exception as e:
                log.error("[Runtime] user callback failed: %r", e, exc_info=e)

    def stop(self) -> None:
        try:    self._q.put_nowait(None)
        except queue.Full:
            # Full queue: the worker will drain and exit once it sees the
            # process go down (daemon thread); best-effort wake is enough.
            pass
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
