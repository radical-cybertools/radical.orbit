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


def batch_put(dd: Any, items: Dict[str, Any]) -> int:
    """Store *items*; several at once as one Dragon batch put (no per-put
    confirmation: errors show only at the end, so on failure an unknown
    subset may be stored)."""
    if len(items) == 1:
        (k, v), = items.items()
        dd[k] = v
        return 1
    n = 0
    dd.start_batch_put()
    try:
        for k, v in items.items():
            dd[k] = v
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

    def __init__(self, wid: int, reader, writer, info: dict):
        self.wid    = wid
        self.reader = reader
        self.writer = writer
        self.info   = info
        self.lock   = asyncio.Lock()        # one request at a time

    async def call(self, msg: dict) -> Any:
        async with self.lock:
            try:
                self.writer.write(_pack(msg))
                await self.writer.drain()
                head = await self.reader.readexactly(_LEN.size)
                n,   = _LEN.unpack(head)
                body = await self.reader.readexactly(n)
            except (OSError, asyncio.IncompleteReadError) as e:
                raise WorkerError(f"worker {self.wid} went away: "
                                  f"{e}") from e
        reply = _unpack(body)
        if not reply.get('ok'):
            if reply.get('type') == 'KeyError':
                raise KeyError(reply.get('error'))
            raise WorkerError(f"worker {self.wid}: {reply.get('type')}: "
                              f"{reply.get('error')}")
        return reply.get('result')


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

    async def _on(self, wid: int, op: str, did: str, desc: str,
                  args: dict) -> Any:
        return await self._conns[wid].call({'op': op, 'did': did,
                                            'desc': desc, 'args': args})

    def _split(self, keys) -> Dict[int, list]:
        parts: Dict[int, list] = {}
        for k in keys:
            parts.setdefault(route(k, self.n), []).append(k)
        return parts

    async def put(self, did: str, desc: str, items: Dict[str, Any]) -> int:
        parts = self._split(items)
        done  = await asyncio.gather(*[
            self._on(w, 'put', did, desc, {'items': {k: items[k] for k in ks}})
            for w, ks in parts.items()])
        return sum(done)

    async def get(self, did: str, desc: str, keys: List[str]) -> tuple:
        found, missing = {}, []
        for f, m in await asyncio.gather(*[
                self._on(w, 'get', did, desc, {'keys': ks})
                for w, ks in self._split(keys).items()]):
            found.update(f)
            missing += m
        gone = set(missing)
        return found, [k for k in keys if k in gone]

    async def _keyed(self, op: str, did: str, desc: str,
                     keys: List[str]) -> List[str]:
        missing = set()
        for m in await asyncio.gather(*[
                self._on(w, op, did, desc, {'keys': ks})
                for w, ks in self._split(keys).items()]):
            missing.update(m)
        return [k for k in keys if k in missing]

    async def contains(self, did: str, desc: str, keys: List[str]) -> list:
        return await self._keyed('contains', did, desc, keys)

    async def pop(self, did: str, desc: str, keys: List[str]) -> list:
        return await self._keyed('pop', did, desc, keys)

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
