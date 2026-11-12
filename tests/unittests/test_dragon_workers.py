# pylint: disable=protected-access,redefined-outer-name

import asyncio
import socket
import threading

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit import dragon_workers as dw
from radical.orbit.plugin_dragon import PluginDragon

from test_plugin_dragon import _FakeBackend, _FakeDDict


class _DDict(_FakeDDict):
    """Fake DDict handle as a worker sees it (attach/detach)."""

    def __init__(self, *args):
        super().__init__(*args)
        self.detached = 0

    def detach(self):
        self.detached += 1


# ── config, routing ─────────────────────────────────────────────────────────

def test_worker_count(monkeypatch):
    monkeypatch.delenv(dw.ENV_WORKERS, raising=False)
    assert dw.worker_count() == 0
    monkeypatch.setenv(dw.ENV_WORKERS, '4')
    assert dw.worker_count() == 4
    for bad in ('x', '-1', str(dw.MAX_WORKERS + 1)):
        monkeypatch.setenv(dw.ENV_WORKERS, bad)
        with pytest.raises(ValueError):
            dw.worker_count()


def test_route_is_stable_and_spreads():
    keys  = [f'k{i}' for i in range(1000)]
    first = [dw.route(k, 4) for k in keys]
    assert first == [dw.route(k, 4) for k in keys]
    counts = [first.count(w) for w in range(4)]
    assert min(counts) > 200


# ── worker operations ───────────────────────────────────────────────────────

def test_batch_put_uses_one_batch():
    dd = _DDict()
    assert dw.batch_put(dd, {'a': 1, 'b': 2}) == 2
    assert dd == {'a': 1, 'b': 2} and dd.batches == 1 and not dd.in_batch
    assert dw.batch_put(dd, {'c': 3}) == 1
    assert dd.batches == 1                     # a single put needs no batch


def test_batch_put_failure_ends_batch():
    class _Full(_DDict):
        def __setitem__(self, k, v):
            if len(self) >= 1:
                raise RuntimeError('full')
            super().__setitem__(k, v)
    dd = _Full()
    with pytest.raises(RuntimeError, match='after 1 were sent'):
        dw.batch_put(dd, {'a': 1, 'b': 2})
    assert not dd.in_batch


def test_batch_put_end_failure():
    class _Late(_DDict):
        def end_batch_put(self):
            super().end_batch_put()
            raise RuntimeError('manager full')
    with pytest.raises(RuntimeError, match='some may be stored'):
        dw.batch_put(_Late(), {'a': 1, 'b': 2})


def test_handle_ops():
    dd = _DDict()
    dw.handle('put', dd, {'items': {'a': 1, 'b': b'\x00'}})
    assert dw.handle('get', dd, {'keys': ['a', 'zz']}) == [{'a': 1}, ['zz']]
    assert dw.handle('contains', dd, {'keys': ['b', 'zz']}) == ['zz']
    assert dw.handle('pop', dd, {'keys': ['a', 'zz']}) == ['zz']
    assert dd == {'b': b'\x00'}
    with pytest.raises(ValueError):
        dw.handle('nope', dd, {})


# ── worker loop over a socket ───────────────────────────────────────────────

def _call(sock, msg):
    sock.sendall(dw._pack(msg))
    return dw._recv(sock)


def test_serve_attaches_once_and_detaches():
    a, b    = socket.socketpair()
    dds     = {'D1': _DDict()}
    attached = []

    def attach(desc):
        attached.append(desc)
        return dds[desc]
    t = threading.Thread(target=dw.serve, args=(b, attach), daemon=True)
    t.start()
    m = {'did': 'x', 'desc': 'D1'}
    assert _call(a, dict(m, op='put', args={'items': {'k': 1}}))['ok']
    r = _call(a, dict(m, op='get', args={'keys': ['k', 'z']}))
    assert r == {'ok': True, 'result': [{'k': 1}, ['z']]}
    assert attached == ['D1']                       # cached handle
    r = _call(a, dict(m, op='bogus'))
    assert not r['ok'] and r['type'] == 'ValueError'
    assert _call(a, {'op': 'detach', 'did': 'x'})['ok']
    assert dds['D1'].detached == 1
    assert _call(a, {'op': 'exit'}) == {'ok': True, 'result': None}
    t.join(2)
    assert not t.is_alive()


def test_serve_exits_on_eof_and_detaches():
    a, b = socket.socketpair()
    dd   = _DDict()
    t = threading.Thread(target=dw.serve, args=(b, lambda d: dd), daemon=True)
    t.start()
    _call(a, {'op': 'put', 'did': 'x', 'desc': 'D',
              'args': {'items': {'k': 1}}})
    a.close()
    t.join(2)
    assert not t.is_alive() and dd.detached == 1


# ── pool, workers on threads ────────────────────────────────────────────────

def _thread_spawn(dds, started=None):
    """A spawn that runs worker_main on threads, attaching from *dds*."""
    def spawn(host, port, n, secret):
        threads = []
        for wid in range(n):
            t = threading.Thread(target=dw.worker_main,
                                 args=('127.0.0.1', port, wid, secret),
                                 kwargs={'attach': dds.__getitem__},
                                 daemon=True)
            t.start()
            threads.append(t)
        if started is not None:
            started.extend(threads)
        return threads
    return spawn


async def _run_spawn(fn, *args):
    return fn(*args)


def test_pool_splits_and_merges():
    dds = {'D': _DDict()}

    async def _run():
        pool = dw.WorkerPool(3, _thread_spawn(dds))
        await pool.start(_run_spawn)
        items = {f'k{i}': i for i in range(30)}
        assert await pool.put('x', 'D', items) == 30
        keys = ['k0', 'zz', 'k29', 'k7']
        found, missing = await pool.get('x', 'D', keys)
        assert found == {'k0': 0, 'k29': 29, 'k7': 7} and missing == ['zz']
        assert await pool.contains('x', 'D', ['k1', 'no', 'k2']) == ['no']
        assert await pool.pop('x', 'D', ['k1', 'no']) == ['no']
        await pool.detach('x')
        await pool.stop()
        return pool
    pool = asyncio.run(_run())
    assert dds['D'] == {f'k{i}': i for i in range(30) if i != 1}
    assert dds['D'].batches == 3                # one batch per worker
    for t in pool.procs:
        t.join(2)
        assert not t.is_alive()


def test_pool_rejects_bad_secret(monkeypatch):
    monkeypatch.setattr(dw, 'HELLO_TIMEOUT', 1)

    def spawn(host, port, n, secret):
        t = threading.Thread(target=dw.worker_main,
                             args=('127.0.0.1', port, 0, 'wrong'),
                             kwargs={'attach': lambda d: _DDict()},
                             daemon=True)
        t.start()
        return [t]

    async def _run():
        pool = dw.WorkerPool(1, spawn)
        with pytest.raises(dw.WorkerError, match='0 of 1'):
            await pool.start(_run_spawn)
        await pool.stop()
    asyncio.run(_run())


def test_pool_worker_gone_is_worker_error():
    dds = {'D': _DDict()}

    async def _run():
        pool = dw.WorkerPool(1, _thread_spawn(dds))
        await pool.start(_run_spawn)
        await pool._conns[0].call({'op': 'exit'})     # worker leaves
        with pytest.raises(dw.WorkerError, match='went away'):
            await pool.get('x', 'D', ['k'])
        await pool.stop()
    asyncio.run(_run())


# ── backend and plugin with workers ─────────────────────────────────────────

class _WorkerBackend(_FakeBackend):
    """Fake Dragon plus thread workers attaching the backend's DDicts."""

    def __init__(self, workers=2):
        super().__init__(workers=workers)
        self.spawned = []
        self.reaped  = None

    def _new_ddict(self, managers_per_node, n_nodes, total_mem):
        return _DDict(managers_per_node, n_nodes, total_mem)

    def _spawn_workers(self, host, port, n, secret):
        by_desc = {dd.serialize(): dd for dd in self._ddicts.values()}

        class _Lookup(dict):
            def __missing__(lookup, desc):      # created after the spawn
                return {d.serialize(): d
                        for d in self._ddicts.values()}[desc]
        return _thread_spawn(_Lookup(by_desc), self.spawned)(
            host, port, n, secret)

    def _reap_workers(self, procs):
        self.reaped = procs
        for t in procs:
            t.join(2)


def _mkw(workers=2):
    app     = FastAPI()
    backend = _WorkerBackend(workers)
    plugin  = PluginDragon(app, backend=backend)
    tc      = TestClient(app)
    sid     = tc.post('/dragon/register_session').json()['sid']
    return plugin, backend, tc, sid


def test_plugin_data_ops_through_workers():
    plugin, backend, tc, sid = _mkw(2)
    with tc:
        did = tc.post(f'/dragon/ddict/create/{sid}', json={}).json()['id']
        u   = f'/dragon/ddict/{{}}/{sid}'
        items = {f'k{i}': {'i': i} for i in range(20)}
        r = tc.post(u.format('put'), json={'id': did, 'items': items})
        assert r.json() == {'n': 20}
        assert len(backend.spawned) == 2           # started on first use
        r = tc.post(u.format('get'), json={'id': did,
                                           'keys': ['k3', 'zz', 'k19']}).json()
        assert r['items'] == {'k3': {'i': 3}, 'k19': {'i': 19}}
        assert r['missing'] == ['zz']
        r = tc.post(u.format('get'), json={'id': did, 'keys': ['k1', 'q'],
                                           'values': False}).json()
        assert r['missing'] == ['q']
        r = tc.post(u.format('pop'), json={'id': did,
                                           'keys': ['k1', 'q']}).json()
        assert r == {'n': 1, 'missing': ['q']}
        dd = backend._ddicts[did]
        assert len(dd) == 19
        assert tc.post(f'/dragon/ddict/destroy/{sid}',
                       json={'id': did}).status_code == 200
        assert dd.destroyed and dd.detached == 2   # both workers let go


def test_backend_close_stops_workers():
    _, backend, tc, sid = _mkw(2)
    with tc:
        did = tc.post(f'/dragon/ddict/create/{sid}', json={}).json()['id']
        tc.post(f'/dragon/ddict/put/{sid}', json={'id': did,
                                                 'items': {'a': 1}})
        tc.portal.call(backend.close)         # on the loop the pool lives on
    assert backend.reaped == backend.spawned
    assert all(not t.is_alive() for t in backend.spawned)
    assert backend.down == 1


def test_backend_worker_start_failure_is_503(monkeypatch):
    monkeypatch.setattr(dw, 'HELLO_TIMEOUT', 0.5)
    _, backend, tc, sid = _mkw(2)
    backend._spawn_workers = lambda host, port, n, secret: []   # none come
    with tc:
        did = tc.post(f'/dragon/ddict/create/{sid}', json={}).json()['id']
        r = tc.post(f'/dragon/ddict/put/{sid}', json={'id': did,
                                                     'items': {'a': 1}})
    assert r.status_code == 503
    assert backend._workers is None
