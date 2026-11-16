"""Dragon worker processes for the ``dragon`` plugin.

One endpoint thread serializes every Dragon call (concurrent DDict calls
from several threads of one process hang in dragonhpc 0.15.0), which caps
an endpoint at about one DDict operation per 0.4 ms.  Natively, Dragon
scales by processes: each process attaches the DDict with its own handle
and writes on its own.  This module gives the endpoint that shape.

- :func:`worker_main` runs in a Dragon process (started by
  :func:`dragon_spawn`; the secret comes through the environment).
  It connects back to the endpoint, attaches DDicts by descriptor on first
  use, and serves one request at a time: it is a single writer.
- :class:`WorkerPool` (endpoint side, asyncio) listens for the workers,
  splits multi-key requests by key hash, runs the parts in parallel and
  merges the results.

Frames are a 4-byte big-endian length plus msgpack; values travel as
Python objects (JSON types and ``bytes``).  A worker authenticates with
the pool's per-endpoint secret.  The worker exits when the endpoint closes
the connection.
"""

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'


import asyncio
import collections
import hmac
import logging
import os
import secrets
import socket
import struct
import sys
import zlib

from typing import Any, Callable, Dict, List, Optional

import msgpack

log = logging.getLogger("radical.orbit")


ENV_WORKERS      = 'RADICAL_ORBIT_DRAGON_WORKERS'
ENV_WORKER_HOST  = 'RADICAL_ORBIT_DRAGON_WORKER_HOST'
ENV_SECRET       = 'RADICAL_ORBIT_DRAGON_WORKER_SECRET'
ENV_BATCH_PUTS   = 'RADICAL_ORBIT_DRAGON_BATCH_PUTS'
ENV_KEYS_PER_REQ = 'RADICAL_ORBIT_DRAGON_KEYS_PER_REQUEST'
KEYS_PER_REQ     = 8        # keys per worker request; 0: one fixed worker per key
WRITE_HIGH       = 1 << 20  # bytes queued to a worker before a send waits
MAX_WORKERS      = 256
HELLO_TIMEOUT    = 120                  # s, all workers connected
STOP_TIMEOUT     = 10                   # s, workers acknowledge exit
FRAME_MAX        = 1 << 30
_LEN             = struct.Struct('>I')


def worker_count() -> int:
    """Workers per endpoint from ``$RADICAL_ORBIT_DRAGON_WORKERS``;
    0 (default): no workers, the endpoint's own Dragon thread does it."""
    raw = os.environ.get(ENV_WORKERS, '').strip()
    if not raw:
        return 0
    try:
        n = int(raw)
    except ValueError:
        n = -1
    if not 0 <= n <= MAX_WORKERS:
        raise ValueError(f"${ENV_WORKERS}={raw!r}: expected an integer "
                         f"0..{MAX_WORKERS}")
    return n


def route(key: str, n: int) -> int:
    """The worker for *key* (stable across processes, unlike ``hash``)."""
    return zlib.crc32(key.encode()) % n


def _pack(msg: Any) -> bytes:
    body = msgpack.packb(msg, use_bin_type=True)
    return _LEN.pack(len(body)) + body


def _unpack(body: bytes) -> Any:
    return msgpack.unpackb(body, raw=False)


# ------------------------------------------------------------------------
# worker side (blocking, inside a Dragon process)
#
def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def _recv(sock: socket.socket) -> Optional[Any]:
    head = _recv_exact(sock, _LEN.size)
    if head is None:
        return None
    n, = _LEN.unpack(head)
    if n > FRAME_MAX:
        raise ValueError(f"frame of {n} bytes")
    body = _recv_exact(sock, n)
    return None if body is None else _unpack(body)


def keys_per_request() -> int:
    """Keys per worker request (``$RADICAL_ORBIT_DRAGON_KEYS_PER_REQUEST``,
    default 8): under load a call is cut into chunks of that many keys, each
    sent to the least busy worker; ``0`` always routes every key to a fixed
    worker (crc32), so a call fans out to up to one request per worker."""
    raw = os.environ.get(ENV_KEYS_PER_REQ, '').strip()
    if not raw:
        return KEYS_PER_REQ
    try:
        n = int(raw)
    except ValueError:
        n = -1
    if n < 0:
        raise ValueError(f"{ENV_KEYS_PER_REQ}={raw!r}: expected an integer "
                         f">= 0")
    return n


def batch_puts() -> bool:
    """Whether multi-key puts go to Dragon as one batch put
    (``$RADICAL_ORBIT_DRAGON_BATCH_PUTS``, default on; ``0`` puts key by
    key, each confirmed: slower, for comparisons)."""
    return os.environ.get(ENV_BATCH_PUTS, '1').strip().lower() \
        not in ('0', 'false', 'no', 'off')


def put_order(dd: Any, keys: List[str]) -> List[str]:
    """*keys* in an order that keeps a Dragon batch put in one batch.

    Inside a batch, Dragon (0.15) opens manager connections only in
    ascending manager id; a key whose manager is below the highest one
    already open ends the batch and starts a new one (a round trip to
    every open manager).  So the first key of each manager goes first, in
    ascending manager id, and the rest follow as given, which keeps the
    managers interleaved.  Uses Dragon's private key-to-manager mapping;
    without it (or if it fails) the order is unchanged."""
    choose = getattr(dd, '_choose_manager_pickle_key', None)
    if choose is None or len(keys) < 2:
        return keys
    try:
        first: Dict[int, str] = {}
        for k in keys:
            first.setdefault(choose(k)[0], k)
    except Exception:
        return keys
    if len(first) < 2:
        return keys
    heads = [first[m] for m in sorted(first)]
    taken = set(heads)
    return heads + [k for k in keys if k not in taken]


def batch_put(dd: Any, items: Dict[str, Any]) -> int:
    """Store *items*; several at once as one Dragon batch put (no per-put
    confirmation: errors show only at the end, so on failure an unknown
    subset may be stored), or key by key when batch puts are off."""
    if len(items) == 1 or not batch_puts():
        for n, (k, v) in enumerate(items.items()):
            try:
                dd[k] = v
            except Exception as e:
                if len(items) == 1:
                    raise
                raise RuntimeError(f"put stopped at key {k!r} after {n} of "
                                   f"{len(items)} items: {e}") from e
        return len(items)
    n = 0
    order = put_order(dd, list(items))
    dd.start_batch_put()
    try:
        for k in order:
            dd[k] = items[k]
            n += 1
    except BaseException as e:
        try:
            dd.end_batch_put()
        except Exception:
            pass
        raise RuntimeError(f"batch put of {len(items)} items failed after "
                           f"{n} were sent, some may be stored: {e}") from e
    try:
        dd.end_batch_put()
    except Exception as e:
        raise RuntimeError(f"batch put of {len(items)} items failed, some "
                           f"may be stored: {e}") from e
    return n


def handle(op: str, dd: Any, args: dict) -> Any:
    """One data operation on DDict handle *dd*: the worker's whole job, and
    the same code the endpoint's own Dragon thread runs without workers."""
    if op == 'put':
        return batch_put(dd, args['items'])
    if op == 'get':
        found, missing = {}, []
        for k in args['keys']:
            try:
                found[k] = dd[k]
            except KeyError:
                missing.append(k)
        return [found, missing]
    if op == 'contains':
        return [k for k in args['keys'] if k not in dd]
    if op == 'pop':
        missing = []
        for k in args['keys']:
            try:
                del dd[k]
            except KeyError:
                missing.append(k)
        return missing
    raise ValueError(f"unknown op {op!r}")


def serve(sock: socket.socket, attach: Callable[[str], Any]) -> None:
    """The worker loop: one request at a time until ``exit`` or EOF.
    *attach* maps a descriptor to a DDict handle (``DDict.attach``)."""
    handles: Dict[str, Any] = {}
    try:
        while True:
            msg = _recv(sock)
            if msg is None or msg.get('op') == 'exit':
                if msg is not None:
                    sock.sendall(_pack({'ok': True, 'result': None}))
                return
            try:
                op, did = msg['op'], msg.get('did')
                if op == 'detach':
                    dd = handles.pop(did, None)
                    if dd is not None:
                        dd.detach()
                    result = None
                else:
                    dd = handles.get(did)
                    if dd is None:
                        dd = handles[did] = attach(msg['desc'])
                    result = handle(op, dd, msg.get('args') or {})
                reply = {'ok': True, 'result': result}
            except Exception as e:      # pylint: disable=broad-except
                reply = {'ok': False, 'type': type(e).__name__,
                         'error': str(e)}
            sock.sendall(_pack(reply))
    finally:
        for dd in handles.values():
            try:
                dd.detach()
            except Exception:
                pass


def worker_main(host: str, port: int, wid: int, secret: str,
                attach: Optional[Callable[[str], Any]] = None) -> None:
    """Connect to the endpoint's pool, say hello, serve until told to stop."""
    if attach is None:
        from dragon.data.ddict import DDict
        attach = DDict.attach
    sock = socket.create_connection((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        sock.sendall(_pack({'hello': wid, 'secret': secret,
                            'host': socket.gethostname(),
                            'pid': os.getpid()}))
        serve(sock, attach)
    finally:
        sock.close()


def main(argv: List[str]) -> int:
    host, port, wid = argv[0], int(argv[1]), int(argv[2])
    worker_main(host, port, wid, os.environ[ENV_SECRET])
    return 0


# ------------------------------------------------------------------------
# endpoint side (asyncio)
#
class WorkerError(RuntimeError):
    """A worker reported an error, or went away."""


class _Conn:
    """
    The endpoint's connection to one worker.

    Requests are sent as they come, without waiting for earlier replies,
    so the next request is already in the worker's socket when it finishes
    one: a worker idles about 1 ms per request otherwise.  The worker
    serves them in order, so replies are matched to requests by order.
    Sending is synchronous on the event loop (no lock); it waits only when
    more than ``WRITE_HIGH`` bytes are queued.  Replies are read in chunks
    and every complete frame in a chunk is resolved at once.
    """

    def __init__(self, wid: int, reader, writer, info: dict):
        self.wid      = wid
        self.reader   = reader
        self.writer   = writer
        self.info     = info
        self._pending: collections.deque = collections.deque()
        self._dead: Optional[WorkerError] = None
        self._reader  = asyncio.ensure_future(self._read_replies())

    @property
    def load(self) -> int:
        """Requests sent and not yet answered."""
        return len(self._pending)

    async def _read_replies(self) -> None:
        buf, head = bytearray(), _LEN.size
        try:
            while True:
                chunk = await self.reader.read(1 << 18)
                if not chunk:
                    raise OSError("connection closed by the worker")
                buf += chunk
                pos = 0
                while len(buf) - pos >= head:
                    n, = _LEN.unpack_from(buf, pos)
                    if len(buf) - pos - head < n:
                        break
                    body = bytes(buf[pos + head:pos + head + n])
                    pos += head + n
                    fut = self._pending.popleft()
                    if not fut.done():      # its caller may have given up
                        fut.set_result(body)
                if pos:
                    del buf[:pos]
        except (OSError, IndexError) as e:
            self._fail(WorkerError(f"worker {self.wid} went away: {e}"))
        except asyncio.CancelledError:
            self._fail(WorkerError(f"worker {self.wid}: connection closed"))
            raise

    def _fail(self, err: WorkerError) -> None:
        self._dead = err
        while self._pending:
            fut = self._pending.popleft()
            if not fut.done():
                fut.set_exception(err)

    def submit(self, msg: dict) -> asyncio.Future:
        """Send *msg* now; the future resolves to the raw reply body
        (decode it with :meth:`result`).  Synchronous: requests submitted
        one after another reach the worker in that order."""
        fut = asyncio.get_running_loop().create_future()
        if self._dead is not None:
            fut.set_exception(self._dead)
            return fut
        self._pending.append(fut)
        try:
            self.writer.write(_pack(msg))
        except OSError as e:
            self._fail(WorkerError(f"worker {self.wid} went away: {e}"))
        return fut

    async def flush(self) -> None:
        """Wait while too much is queued to the worker (back pressure)."""
        transport = self.writer.transport
        if transport is not None and \
                transport.get_write_buffer_size() > WRITE_HIGH:
            try:
                await self.writer.drain()
            except OSError as e:
                self._fail(WorkerError(f"worker {self.wid} went away: {e}"))

    def result(self, body: bytes) -> Any:
        """The result of a reply body; raises what the worker raised."""
        reply = _unpack(body)
        if not reply.get('ok'):
            if reply.get('type') == 'KeyError':
                raise KeyError(reply.get('error'))
            raise WorkerError(f"worker {self.wid}: {reply.get('type')}: "
                              f"{reply.get('error')}")
        return reply.get('result')

    async def call(self, msg: dict) -> Any:
        fut = self.submit(msg)
        await self.flush()
        return self.result(await fut)


class WorkerPool:
    """
    The endpoint side of N worker processes.

    *spawn* starts the workers: ``spawn(host, port, n, secret)``, a blocking
    call that the backend runs on its Dragon thread (it starts Dragon
    processes).  The pool then waits for all N to connect.
    """

    def __init__(self, n: int, spawn: Callable[[str, int, int, str], Any]):
        self.n        = n
        self._spawn   = spawn
        self._secret  = secrets.token_urlsafe(32)
        self._conns: Dict[int, _Conn] = {}
        self._kpr     = keys_per_request()
        # (did, owner, key) -> [worker, writes in flight]: a key one session
        # is writing is served by that worker for that session until the
        # writes are answered, so its later write (or read) of the key
        # cannot overtake them; writes of different sessions race anyway
        self._pinned: Dict[tuple, list] = {}
        self._inflight = 0                  # requests sent, not answered
        self._rr      = 0
        self._server  = None
        self._all_in  = None
        self.procs    = None                # whatever spawn returned

    @property
    def secret(self) -> str:
        return self._secret

    async def start(self, run_spawn) -> None:
        """Listen, spawn the workers (*run_spawn* runs a blocking call on
        the Dragon thread), wait for every hello."""
        self._all_in = asyncio.get_running_loop().create_future()
        self._server = await asyncio.start_server(self._accept,
                                                  host='0.0.0.0', port=0)
        port = self._server.sockets[0].getsockname()[1]
        host = os.environ.get(ENV_WORKER_HOST) or socket.gethostname()
        self.procs = await run_spawn(self._spawn, host, port, self.n,
                                     self._secret)
        try:
            await asyncio.wait_for(asyncio.shield(self._all_in),
                                   timeout=HELLO_TIMEOUT)
        except asyncio.TimeoutError:
            raise WorkerError(f"{len(self._conns)} of {self.n} Dragon "
                              f"workers connected within "
                              f"{HELLO_TIMEOUT}s") from None
        hosts = sorted({c.info.get('host') for c in self._conns.values()})
        log.info("[dragon] %d workers on %s", self.n, ', '.join(map(str, hosts)))

    async def _accept(self, reader, writer) -> None:
        try:
            head  = await asyncio.wait_for(reader.readexactly(_LEN.size), 10)
            n,    = _LEN.unpack(head)
            if n > 4096:
                raise ValueError("oversized hello")
            hello = _unpack(await asyncio.wait_for(reader.readexactly(n), 10))
            wid   = hello.get('hello')
            if not hmac.compare_digest(str(hello.get('secret', '')),
                                       self._secret) \
                    or not isinstance(wid, int) or not 0 <= wid < self.n \
                    or wid in self._conns:
                raise PermissionError("bad worker hello")
        except Exception as e:          # pylint: disable=broad-except
            log.warning("[dragon] rejected a worker connection: %s", e)
            writer.close()
            return
        sock = writer.get_extra_info('socket')
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._conns[wid] = _Conn(wid, reader, writer, hello)
        if len(self._conns) == self.n and not self._all_in.done():
            self._all_in.set_result(None)

    # -- requests --------------------------------------------------------------

    def _split(self, did: str, keys, owner: Optional[str] = None
               ) -> Dict[int, list]:
        """Worker -> keys for one call.  Keys this *owner* has a write in
        flight for go to that worker.  While fewer requests are in flight
        than there are workers, every other key goes to its fixed worker
        (crc32: the parallelism and key locality of an idle pool); under
        load they go in chunks of ``keys_per_request()`` keys to the least
        busy workers.  With ``0``, always the fixed worker."""
        parts: Dict[int, list] = {}
        n = self.n
        if not self._kpr:
            for k in keys:
                parts.setdefault(route(k, n), []).append(k)
            return parts
        pinned, free = self._pinned, []
        for k in keys:
            p = pinned.get((did, owner, k)) if pinned else None
            if p is None:
                free.append(k)
            else:
                parts.setdefault(p[0], []).append(k)
        if self._inflight < n:
            for k in free:
                parts.setdefault(route(k, n), []).append(k)
            return parts
        kpr = self._kpr
        for i in range(0, len(free), kpr):
            wid = self._least_busy(parts)
            parts.setdefault(wid, []).extend(free[i:i + kpr])
        return parts

    def _least_busy(self, parts: Dict[int, list]) -> int:
        """The worker with the fewest requests in flight, counting those of
        this call; ties go round robin."""
        n, conns, start = self.n, self._conns, self._rr
        best, best_load = start % n, None
        for j in range(n):
            wid  = (start + j) % n
            load = conns[wid].load + (wid in parts)
            if best_load is None or load < best_load:
                best, best_load = wid, load
                if not load:
                    break
        self._rr = best + 1
        return best

    def _pin(self, did: str, owner: Optional[str],
             parts: Dict[int, list]) -> None:
        for wid, ks in parts.items():
            for k in ks:
                p = self._pinned.get((did, owner, k))
                if p is None:
                    self._pinned[(did, owner, k)] = [wid, 1]
                else:
                    p[1] += 1

    def _unpin(self, did: str, owner: Optional[str],
               parts: Dict[int, list]) -> None:
        for ks in parts.values():
            for k in ks:
                p = self._pinned.get((did, owner, k))
                if p is not None:
                    p[1] -= 1
                    if p[1] <= 0:
                        del self._pinned[(did, owner, k)]

    async def _fan(self, op: str, did: str, desc: str, keys, args_of,
                   write: bool, owner: Optional[str] = None) -> list:
        """Send one request per part, all before any await (so the pins
        and the send order are set together); return the results."""
        parts = self._split(did, keys, owner)
        if write and self._kpr:
            self._pin(did, owner, parts)
        conns = [self._conns[w] for w in parts]
        futs  = [c.submit({'op': op, 'did': did, 'desc': desc,
                           'args': args_of(ks)})
                 for c, ks in zip(conns, parts.values())]
        self._inflight += len(futs)
        try:
            for c in conns:
                await c.flush()
            bodies = await asyncio.gather(*futs, return_exceptions=True)
        finally:
            self._inflight -= len(futs)
            if write and self._kpr:
                self._unpin(did, owner, parts)
        out = []
        for c, b in zip(conns, bodies):
            if isinstance(b, BaseException):
                raise b
            out.append(c.result(b))
        return out

    async def _on(self, wid: int, op: str, did: str, desc: str,
                  args: dict) -> Any:
        return await self._conns[wid].call({'op': op, 'did': did,
                                            'desc': desc, 'args': args})

    async def put(self, did: str, desc: str, items: Dict[str, Any],
                  owner: Optional[str] = None) -> int:
        done = await self._fan('put', did, desc, items,
                               lambda ks: {'items': {k: items[k] for k in ks}},
                               write=True, owner=owner)
        return sum(done)

    async def get(self, did: str, desc: str, keys: List[str],
                  owner: Optional[str] = None) -> tuple:
        found, missing = {}, []
        for f, m in await self._fan('get', did, desc, keys,
                                    lambda ks: {'keys': ks}, write=False,
                                    owner=owner):
            found.update(f)
            missing += m
        gone = set(missing)
        return found, [k for k in keys if k in gone]

    async def _keyed(self, op: str, did: str, desc: str,
                     keys: List[str], owner: Optional[str] = None
                     ) -> List[str]:
        missing = set()
        for m in await self._fan(op, did, desc, keys,
                                 lambda ks: {'keys': ks},
                                 write=(op == 'pop'), owner=owner):
            missing.update(m)
        return [k for k in keys if k in missing]

    async def contains(self, did: str, desc: str, keys: List[str],
                       owner: Optional[str] = None) -> list:
        return await self._keyed('contains', did, desc, keys, owner)

    async def pop(self, did: str, desc: str, keys: List[str],
                  owner: Optional[str] = None) -> list:
        return await self._keyed('pop', did, desc, keys, owner)

    async def detach(self, did: str) -> None:
        """Every worker drops its handle on *did* (before a destroy)."""
        await asyncio.gather(*[c.call({'op': 'detach', 'did': did})
                               for c in self._conns.values()],
                             return_exceptions=True)

    async def stop(self) -> None:
        """Ask every worker to exit (bounded), then close the listener."""
        async def _bye(c):
            try:
                await asyncio.wait_for(c.call({'op': 'exit'}), STOP_TIMEOUT)
            except Exception:
                pass
            c.writer.close()
            c._reader.cancel()
        await asyncio.gather(*[_bye(c) for c in self._conns.values()])
        self._conns.clear()
        if self._server is not None:
            self._server.close()
            self._server = None


# ``python -c``, not ``-m``: the package imports this module before
# ``-m`` would run it, which runs it twice
_ENTRY = ('import sys; from radical.orbit.dragon_workers import main; '
          'sys.exit(main(sys.argv[1:]))')


# ------------------------------------------------------------------------
#
def dragon_spawn(host: str, port: int, n: int, secret: str) -> list:
    """Start *n* worker processes in the Dragon runtime this process is part
    of, round-robin over its nodes.  Runs on the backend's Dragon thread."""
    from dragon.native.machine import System
    from dragon.native.process import Process

    try:
        policies = System().hostname_policies()
    except Exception as e:              # pylint: disable=broad-except
        log.warning("[dragon] runtime nodes unknown (%s): default placement",
                    e)
        policies = []

    src = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))    # .../src (or site-packages)
    env = dict(os.environ)
    env[ENV_SECRET] = secret
    env['PYTHONPATH'] = src + (os.pathsep + env['PYTHONPATH']
                               if env.get('PYTHONPATH') else '')
    procs = []
    for wid in range(n):
        policy = policies[wid % len(policies)] if policies else None
        proc = Process(target=sys.executable,
                       args=['-c', _ENTRY, host, str(port), str(wid)],
                       env=env, policy=policy)
        proc.start()
        procs.append(proc)
    return procs


def dragon_reap(procs: list, timeout: float = STOP_TIMEOUT) -> None:
    """Join the worker processes, kill what is left.  Dragon thread."""
    for p in procs or []:
        try:
            p.join(timeout)
        except Exception:
            pass
        try:
            if p.is_alive:
                p.kill()
        except Exception:
            pass


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
