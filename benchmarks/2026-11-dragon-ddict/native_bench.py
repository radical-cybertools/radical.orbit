#!/usr/bin/env python3
"""Native DDict baseline: the same operations inside the Dragon runtime, no
ORBIT.  Launch with the ``dragon`` launcher::

    dragon -s native_bench.py lat  --out native-lat.json
    dragon -m -N K native_bench.py conc --n-nodes K --procs P --out f.json

Modes::

    lat    one process: sequential put / get per value size (bytes values)
    conc   P Dragon processes spread round-robin over the runtime's nodes,
           each looping put (then get) on its own keys for --duration s;
           DDict over --n-nodes nodes, one manager per node
"""

import argparse
import json
import os
import socket
import sys
import time

import numpy as np

from dragon.data.ddict import DDict
from dragon.infrastructure.policy import Policy
from dragon.native.machine import System, Node
from dragon.native.process import Process
from dragon.native.queue import Queue


def pct(xs):
    a = np.asarray(xs) * 1e6
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


def lat(dd, a):
    res = {'sizes': {}}
    for size in a.sizes:
        key, val = f'lat-{size}', os.urandom(size)
        def put():
            dd[key] = val
        def get():
            return dd[key]
        timed(put, 5)
        res['sizes'][str(size)] = {'put': pct(timed(put, a.samples)),
                                   'get': pct(timed(get, a.samples))}
    return res


def worker(desc, rank, vsize, duration, t_put, q):
    """One native client: put phase from t_put, get phase 1 s after it."""
    dd  = DDict.attach(desc)
    val = os.urandom(vsize)
    keys = [f'n{rank}-{i}' for i in range(64)]
    for k in keys:
        dd[k] = val
    out = {'rank': rank, 'host': socket.gethostname()}
    for phase, t0 in (('put', t_put), ('get', t_put + duration + 1)):
        time.sleep(max(0.0, t0 - time.time()))
        n, t_end = 0, time.perf_counter() + duration
        tb = time.perf_counter()
        while time.perf_counter() < t_end:
            k = keys[n % 64]
            if phase == 'put':
                dd[k] = val
            else:
                dd[k]
            n += 1
        out[phase] = {'ops': n, 'elapsed': time.perf_counter() - tb}
    dd.detach()
    q.put(out)


def conc(dd, a):
    hosts = [Node(h).hostname for h in System().nodes]
    q, t_put = Queue(), time.time() + 10 + a.procs * 0.2
    procs = []
    for r in range(a.procs):
        pol = Policy(placement=Policy.Placement.HOST_NAME,
                     host_name=hosts[r % len(hosts)])
        p = Process(target=worker, policy=pol,
                    args=(dd.serialize(), r, a.vsize, a.duration, t_put, q))
        p.start()
        procs.append(p)
    ranks = [q.get(timeout=a.duration * 2 + 120) for _ in procs]
    for p in procs:
        p.join()
    return {'procs': a.procs, 'runtime_nodes': len(hosts),
            'ranks': ranks,
            'put_ops_s': sum(r['put']['ops'] / r['put']['elapsed']
                             for r in ranks),
            'get_ops_s': sum(r['get']['ops'] / r['get']['elapsed']
                             for r in ranks)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['lat', 'conc'])
    p.add_argument('--out')
    p.add_argument('--n-nodes', type=int, default=1)
    p.add_argument('--mem', type=int, default=8 * 2**30)
    p.add_argument('--sizes', type=int, nargs='+',
                   default=[64, 1024, 16384, 262144, 1048576])
    p.add_argument('--samples', type=int, default=200)
    p.add_argument('--procs', type=int, default=1)
    p.add_argument('--vsize', type=int, default=1024)
    p.add_argument('--duration', type=float, default=5.0)
    a = p.parse_args()

    dd = DDict(1, a.n_nodes, a.mem, timeout=120)
    try:
        res = lat(dd, a) if a.mode == 'lat' else conc(dd, a)
    finally:
        dd.destroy()
    res.update(mode=a.mode, n_nodes=a.n_nodes, host=socket.gethostname())
    out = json.dumps(res, indent=1)
    if a.out:
        with open(a.out, 'w') as f:
            f.write(out)
    else:
        print(out)


if __name__ == '__main__':
    sys.exit(main())
