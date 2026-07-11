#!/usr/bin/env python3
"""Throughput overhead: batch (unpinned) vs per-task ProcessGroup (pinned).

The Dragon batch worker pool ignores per-task ``Policy`` placement (see
``dragon/workflows/batch/batch.py:Manager.__setstate__``).  Tasks that
need an explicit ``HOST_NAME`` therefore bypass the pool and run as a
dedicated one-shot ``ProcessGroup``, paying ``pg.init() / pg.start() /
pg.close()`` per task.  This script measures how much that costs.

Submits ``/bin/true`` twice per task count, once unpinned (batch path)
and once pinned round-robin across nodes (bypass path), and prints a
plain-text table of p50 / p95 / max per-task time from
``submit_tasks()`` to the DONE callback firing.

Two task counts (override via env):

* ``TASKS_PER_NODE`` (default 128) × n_nodes — one task per core
* 10 ×  the above

Each row reports p50, p95, max, wall, throughput, and counts.

Run inside a Slurm allocation::

    salloc -A <acct> -C cpu -N <n> -t 30 ...
    cd <shared-fs-dir>
    dragon python test_placement_throughput.py
"""
import asyncio
import os
import subprocess
import time

from dragon.infrastructure.policy import Policy
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackendV3


TASKS_PER_NODE  = int(os.environ.get('TASKS_PER_NODE', '128'))
LARGE_MULTIPLIER = int(os.environ.get('LARGE_MULTIPLIER', '10'))


def _make_tasks(count, nodelist, *, pinned):
    tasks = []
    for i in range(count):
        kw = {
            'uid'        : f'thr.{i:06d}',
            'executable' : '/bin/true',
        }
        if pinned:
            kw['task_backend_specific_kwargs'] = {
                'process_template': {
                    'policy': Policy(
                        placement = Policy.Placement.HOST_NAME,
                        host_name = nodelist[i % len(nodelist)],
                    ),
                },
            }
        tasks.append(ComputeTask(**kw))
    return tasks


async def _wait_and_stamp(task):
    # Resolves when the task transitions to DONE / FAILED / CANCELED;
    # time.time() is captured after asyncio resumes the coroutine, so it
    # tracks the DONE callback to within one scheduler tick.
    await task
    return time.time()


async def _run(session, tasks, *, label):
    n  = len(tasks)
    t0 = time.time()
    await session.submit_tasks(tasks)
    t_sub = time.time() - t0
    print(f'    submit returned in {t_sub:.2f}s; waiting on {n} tasks',
          flush=True)

    # Live progress so a slow path (lots of pg.init/start) is
    # distinguishable from a true hang while the run is in flight.
    pending  = {asyncio.ensure_future(_wait_and_stamp(t)) for t in tasks}
    t_stamps = []
    last_log = time.time()
    while pending:
        done_set, pending = await asyncio.wait(
            pending, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
        for fut in done_set:
            t_stamps.append(fut.result())
        now = time.time()
        if done_set or (now - last_log) >= 5.0:
            print(f'    progress  done={len(t_stamps):>6} / {n}  '
                  f'pending={len(pending):>6}  '
                  f'rate={len(t_stamps) / max(now - t0, 1e-6):.1f}/s',
                  flush=True)
            last_log = now

    wall = time.time() - t0
    lat  = sorted(t - t0 for t in t_stamps)
    done = sum(1 for t in tasks if str(t.state) == 'DONE')
    return {
        'label' : label,
        'n'     : n,
        'wall'  : wall,
        'sub'   : t_sub,
        'p50'   : lat[n // 2],
        # p95: the (ceil(0.95 * n) - 1)-th element of a sorted ascending
        # sample is the conventional inclusive 95th percentile.
        'p95'   : lat[max(0, int(0.95 * n + 0.5) - 1)],
        'max'   : lat[-1],
        'thru'  : n / wall if wall else 0.0,
        'done'  : done,
        'fail'  : n - done,
    }


def _print_table(rows):
    cols = [
        ('mode',     '<8'),
        ('count',    '>7'),
        ('submit s', '>9'),
        ('p50 s',    '>8'),
        ('p95 s',    '>8'),
        ('max s',    '>8'),
        ('wall s',   '>8'),
        ('tasks/s',  '>9'),
        ('done',     '>6'),
        ('fail',     '>5'),
    ]
    hdr = '  '.join(f'{name:{spec}}' for name, spec in cols)
    sep = '  '.join('-' * len(name) + ' ' * (int(spec[1:]) - len(name))
                    if spec.startswith('>')
                    else '-' * int(spec[1:])
                    for name, spec in cols)
    print()
    print(hdr)
    print(sep)
    for r in rows:
        vals = (
            r['label'],
            r['n'],
            f"{r['sub']:.2f}",
            f"{r['p50']:.3f}",
            f"{r['p95']:.3f}",
            f"{r['max']:.3f}",
            f"{r['wall']:.2f}",
            f"{r['thru']:.1f}",
            r['done'],
            r['fail'],
        )
        print('  '.join(f'{v:{spec}}' for v, (_, spec) in zip(vals, cols)))
    print()


async def main():
    nodelist = subprocess.check_output(
        ['scontrol', 'show', 'hostnames', os.environ['SLURM_JOB_NODELIST']],
        text=True).split()
    n_nodes = len(nodelist)
    small   = TASKS_PER_NODE * n_nodes
    large   = LARGE_MULTIPLIER * small

    print(f'\nnodes: {n_nodes}    tasks_per_node: {TASKS_PER_NODE}')
    print(f'small: {small} tasks    large: {large} tasks    /bin/true\n')

    backend = await DragonExecutionBackendV3()
    session = Session(backends=[backend])

    rows = []
    async with session:
        for n in (small, large):
            for pinned in (False, True):
                label = 'bypass' if pinned else 'batch'
                tasks = _make_tasks(n, nodelist, pinned=pinned)
                print(f'  running  mode={label:<6}  n={n} ...', flush=True)
                rows.append(await _run(session, tasks, label=label))

    _print_table(rows)


asyncio.run(main())
