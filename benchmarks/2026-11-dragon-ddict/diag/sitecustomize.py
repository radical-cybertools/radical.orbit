"""Diagnostic instrumentation for the put-collapse study (not an ORBIT change).

On PYTHONPATH with ``$ORBIT_BENCH_DIAG=<path prefix>``, every process that
imports ``radical.orbit.plugin_dragon`` (the endpoint) appends one JSON line
per Dragon call to ``<prefix>.<pid>.jsonl``:

    {"op": "_put", "n": 64, "q": queue wait s, "x": exec s on the Dragon
     thread, "k_sum": s in DDict set/get, "k_max": slowest key s,
     "h": route handler s (request parsed -> reply built), "t": end time}

Also chains ``nodeflate/sitecustomize.py`` behaviour when
``$ORBIT_BENCH_NO_DEFLATE=1``.
"""

import os

if os.environ.get('ORBIT_BENCH_NO_DEFLATE') == '1':
    import websockets
    _connect = websockets.connect

    def _nodeflate(*args, **kwargs):
        kwargs['compression'] = None
        return _connect(*args, **kwargs)

    websockets.connect = _nodeflate

PREFIX = os.environ.get('ORBIT_BENCH_DIAG')

if PREFIX:
    import functools
    import importlib.abc
    import importlib.machinery
    import json
    import sys
    import threading
    import time

    _out  = None
    _lock = threading.Lock()
    _tls  = threading.local()

    def _emit(rec):
        global _out
        with _lock:
            if _out is None:
                _out = open(f'{PREFIX}.{os.getpid()}.jsonl', 'a', buffering=1)
            _out.write(json.dumps(rec) + '\n')

    def _timed_item(orig):
        @functools.wraps(orig)
        def wrapper(self, *args):
            t0 = time.perf_counter()
            try:
                return orig(self, *args)
            finally:
                dt = time.perf_counter() - t0
                acc = getattr(_tls, 'acc', None)
                if acc is not None:
                    acc[0] += dt
                    acc[1] = max(acc[1], dt)
                    acc[2] += 1
        return wrapper

    def _patch(mod):
        from dragon.data.ddict import DDict
        DDict.__setitem__ = _timed_item(DDict.__setitem__)
        DDict.__getitem__ = _timed_item(DDict.__getitem__)

        B = mod.DDictBackend
        orig_call = B._call

        async def _call(self, fn, *args, **kwargs):
            t_sub = time.perf_counter()
            rec   = {'op': getattr(fn, '__name__', '?')}

            def run():
                rec['q'] = time.perf_counter() - t_sub
                _tls.acc = [0.0, 0.0, 0]
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    rec['x'] = time.perf_counter() - t0
                    rec['k_sum'], rec['k_max'], rec['n'] = _tls.acc
                    _tls.acc = None
            try:
                return await orig_call(self, run)
            finally:
                rec['t'] = time.time()
                _emit(rec)
        B._call = _call

        P = mod.PluginDragon
        for name in ('r_put', 'r_get'):
            orig = getattr(P, name)

            def mk(orig, name):
                @functools.wraps(orig)
                async def handler(self, request):
                    t0 = time.perf_counter()
                    try:
                        return await orig(self, request)
                    finally:
                        _emit({'op': name, 'h': time.perf_counter() - t0,
                               't': time.time()})
                return handler
            setattr(P, name, mk(orig, name))

    class _Loader(importlib.abc.Loader):
        def __init__(self, inner):
            self._inner = inner

        def create_module(self, spec):
            return self._inner.create_module(spec)

        def exec_module(self, module):
            self._inner.exec_module(module)
            _patch(module)

    class _Finder(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path, target=None):
            if name != 'radical.orbit.plugin_dragon':
                return None
            spec = importlib.machinery.PathFinder.find_spec(name, path)
            if spec is not None:
                spec.loader = _Loader(spec.loader)
            return spec

    sys.meta_path.insert(0, _Finder())
