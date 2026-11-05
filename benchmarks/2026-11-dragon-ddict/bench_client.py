#!/usr/bin/env python3
"""ORBIT client side of the Dragon DDict benchmarks (plain python, no Dragon).

Connects to the broker as a consumer (``$RADICAL_ORBIT_BROKER_URL``,
``_CERT``, ``_TOKEN``), opens a ``dragon`` plugin session on endpoint
``--ep`` and drives one DDict through ``DDictHandle``.  Every mode writes one
JSON document to ``--out`` (or stdout).

Modes::

    setup --n-nodes K [--mem 8G]      (re)create DDict 'bench' on the endpoint
    destroy                           destroy DDict 'bench' (if it exists)
    lat   [--sizes ..] [--samples N]  sequential update({k: v}) / get(k) per
                                      value size, plus two no-op round trips:
                                      'list' (ddict/list: ORBIT only, no Dragon
                                      call) and 'keys' (ddict/keys on a DDict
                                      with ~5 keys: ORBIT + the Dragon thread)
    batch [--batches ..] [--vsize B]  update / get_many with B keys per call
    conc  --start-at T --duration D   one of N concurrent clients (rank from
          [--batch B] [--shared]      $SLURM_PROCID): update phase [T, T+D],
                                      get phase [T+D+1, T+2D+1]; --shared:
                                      all ranks write the same keys

``--ep`` may list several endpoints (``a,b,c``): rank r uses the
(r % n)-th.  ``{rank}`` in ``--out`` is replaced by the rank.

Values are ``bytes`` (base64 on the wire, ~4/3 of the raw size).
"""

import argparse
import json
import logging
import os
import socket
import sys
import time

import numpy as np

from radical.orbit import EndpointRuntime

DDICT = 'bench'


def pct(xs):
    a = np.asarray(xs) * 1e6                      # s -> us
    return {'n': len(a), 'p50_us': float(np.percentile(a, 50)),
            'p90_us': float(np.percentile(a, 90)),
            'p99_us': float(np.percentile(a, 99)),
            'mean_us': float(a.mean())}


def timed(fn, n):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    return ts


def for_duration(fn, duration, min_calls=3):
    """Call *fn* repeatedly for *duration* s; returns (calls, elapsed, lats)."""
    lats, t_end = [], time.perf_counter() + duration
    while time.perf_counter() < t_end or len(lats) < min_calls:
        t0 = time.perf_counter()
        fn()
        lats.append(time.perf_counter() - t0)
    return len(lats), sum(lats), lats


def m_destroy(dragon):
    for e in dragon.ddict.list():
        if e['name'] == DDICT:
            dragon.ddict.attach(DDICT).destroy()
            return {'destroyed': e['id']}
    return {}


def m_setup(dragon, a):
    m_destroy(dragon)
    t0 = time.perf_counter()
    dd = dragon.ddict.create(name=DDICT, managers_per_node=a.mpn,
                             n_nodes=a.n_nodes, total_mem=a.mem)
    return {'id': dd.id, 'n_nodes': a.n_nodes, 'mpn': a.mpn, 'mem': a.mem,
            'create_s': time.perf_counter() - t0}


def m_lat(dragon, dd, a):
    res = {'sizes': {}}
    res['noop_list'] = pct(timed(dragon.ddict.list, a.samples))
    for size in a.sizes:
        key, val = f'lat-{size}', os.urandom(size)
        timed(lambda: dd.update({key: val}), 5)        # warm-up
        put = timed(lambda: dd.update({key: val}), a.samples)
        get = timed(lambda: dd[key], a.samples)
        assert dd[key] == val
        res['sizes'][str(size)] = {'put': pct(put), 'get': pct(get)}
    res['noop_keys'] = pct(timed(dd.keys, a.samples))
    return res


def m_batch(dd, a):
    res, val = {}, os.urandom(a.vsize)
    for b in a.batches:
        items = {f'b{i}': val for i in range(b)}
        keys  = list(items)
        dd.update(items)                                # warm-up
        n, el, _ = for_duration(lambda: dd.update(items), a.duration)
        put = {'calls': n, 'ops_s': n * b / el,
               'MB_s': n * b * a.vsize / el / 1e6}
        n, el, _ = for_duration(lambda: dd.get_many(keys), a.duration)
        get = {'calls': n, 'ops_s': n * b / el,
               'MB_s': n * b * a.vsize / el / 1e6}
        res[str(b)] = {'put': put, 'get': get}
    return res


def m_conc(dd, a):
    rank  = int(os.environ.get('SLURM_PROCID', 0))
    val   = os.urandom(a.vsize)
    pre   = 's' if a.shared else f'c{rank}'           # overlapping key sets?
    items = {f'{pre}-{i}': val for i in range(a.batch)}
    keys  = list(items)
    dd.update(items)                                    # warm-up
    res = {'rank': rank, 'host': socket.gethostname(), 'batch': a.batch,
           'vsize': a.vsize, 'shared': a.shared}
    for phase, fn, t0 in (('put', lambda: dd.update(items), a.start_at),
                          ('get', lambda: dd.get_many(keys),
                           a.start_at + a.duration + 1)):
        late = time.time() - t0
        if late > 0:
            res[f'{phase}_late_s'] = late           # missed the barrier
        time.sleep(max(0.0, t0 - time.time()))
        n, el, lats = for_duration(fn, a.duration)
        res[phase] = dict(pct(lats), calls=n, elapsed=el,
                          ops=n * a.batch, t_start=t0)
    return res


def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['setup', 'destroy', 'lat', 'batch', 'conc'])
    p.add_argument('--ep', required=True, help='endpoint name')
    p.add_argument('--out')
    p.add_argument('--n-nodes', type=int, default=1)
    p.add_argument('--mem', default='8G')
    p.add_argument('--mpn', type=int, default=1, help='managers per node')
    p.add_argument('--sizes', type=int, nargs='+',
                   default=[64, 1024, 16384, 262144, 1048576])
    p.add_argument('--samples', type=int, default=200)
    p.add_argument('--batches', type=int, nargs='+',
                   default=[1, 4, 16, 64, 256, 1024])
    p.add_argument('--vsize', type=int, default=1024)
    p.add_argument('--batch', type=int, default=1)
    p.add_argument('--duration', type=float, default=3.0)
    p.add_argument('--start-at', type=float, default=0.0)
    p.add_argument('--shared', action='store_true',
                   help='conc: all ranks use the same keys (default: own)')
    p.add_argument('--wait', type=float, default=60.0,
                   help='max s to wait for the endpoint to register')
    a = p.parse_args()
    rank = int(os.environ.get('SLURM_PROCID', 0))
    eps  = a.ep.split(',')
    a.ep = eps[rank % len(eps)]

    # $BENCH_CLIENT_LOG=INFO shows e.g. the runtime's ws_compression decision
    logging.basicConfig(level=os.environ.get('BENCH_CLIENT_LOG', 'WARNING'),
                        format='%(asctime)s %(levelname)s %(message)s')
    rt = EndpointRuntime()
    rt.start(wait=True)
    try:
        t0 = time.time()
        while a.ep not in rt.topology():
            if time.time() - t0 > a.wait:
                raise RuntimeError(f'endpoint {a.ep} not in topology')
            time.sleep(0.5)
        dragon = rt.get_plugin(a.ep, 'dragon')
        if a.mode == 'setup':
            res = m_setup(dragon, a)
        elif a.mode == 'destroy':
            res = m_destroy(dragon)
        else:
            dd = dragon.ddict.attach(DDICT)
            res = {'lat'  : lambda: m_lat(dragon, dd, a),
                   'batch': lambda: m_batch(dd, a),
                   'conc' : lambda: m_conc(dd, a)}[a.mode]()
        dragon.close()
    finally:
        rt.stop()

    res.update(mode=a.mode, ep=a.ep, client_host=socket.gethostname(),
               broker=os.environ.get('RADICAL_ORBIT_BROKER_URL'))
    out = json.dumps(res, indent=1)
    if a.out:
        with open(a.out.format(rank=rank), 'w') as f:
            f.write(out)
    else:
        print(out)


if __name__ == '__main__':
    sys.exit(main())
