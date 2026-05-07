#!/usr/bin/env python3
"""
Example: Rhapsody Task Throughput Benchmark
===========================================

Measures task throughput for different batch sizes using the Rhapsody
Session/Task API with the Edge execution backend.  All bridge
interactions are handled by the backend — no direct BridgeClient usage.

Runs two passes: one with identical (homogeneous) tasks and one with
per-task arguments (heterogeneous) to compare template-compressed vs
regular batched submit paths.

Prerequisites:
  - A Radical Edge bridge is running (RADICAL_BRIDGE_URL set).
  - An edge service is connected with the Rhapsody plugin loaded.
  - The ``rhapsody`` package is installed on both client and edge.

Usage:
  python examples/example_rhapsody_throughput.py [batch_sizes...]

  Default batch sizes: 1 2 4 8 16 … 65536
"""

import asyncio
import sys
import time

import rhapsody


# ---- minimal task functions ------------------------------------------------

def _noop():
    """Minimal function task — runs in-process, no child process."""
    return True


def _noop_arg(x):
    """Minimal function task with one argument."""
    return x


# ---- output helper ---------------------------------------------------------

fout = open("rhapsody_throughput.out", "a")
fout.write("\n==============================================================\n")


def out(data=''):
    """Print to stdout and also to the output file."""
    print(data)
    print(data, file=fout)
    fout.flush()


# ---- benchmark core --------------------------------------------------------

async def run_batch(session, n: int, hetero: bool = False) -> dict:
    """Submit *n* tasks in one batch, wait, return timing."""

    if hetero:
        tasks = [rhapsody.ComputeTask(function=_noop_arg, args=(i,))
                 for i in range(n)]
    else:
        tasks = [rhapsody.ComputeTask(function=_noop)
                 for _ in range(n)]

    t0 = time.time()
    await session.submit_tasks(tasks)
    t_submit = time.time() - t0

    t1 = time.time()
    await session.wait_tasks(tasks)
    t_wait = time.time() - t1

    t_total = t_submit + t_wait

    return {
        "batch_size":    n,
        "submit_time":   t_submit,
        "wait_time":     t_wait,
        "total_time":    t_total,
        "tasks_per_sec": n / t_total if t_total > 0 else float('inf'),
    }


async def run_pass(session, batch_sizes, hetero=False):
    """Run one full benchmark pass, return list of result dicts."""

    label = "heterogeneous" if hetero else "homogeneous"
    out(f"\n--- {label} tasks "
        f"{'(per-task args)' if hetero else '(template)'} ---\n")

    hdr = (f"{'batch':>6}  {'submit':>8}  {'wait':>8}  "
           f"{'total':>8}  {'tasks/s':>9}")
    out(hdr)
    out("-" * len(hdr))

    # warmup
    await run_batch(session, 1, hetero=hetero)

    results = []
    for n in batch_sizes:
        r = await run_batch(session, n, hetero=hetero)
        results.append(r)
        out(f"{r['batch_size']:>6}  "
            f"{r['submit_time']:>7.3f}s  "
            f"{r['wait_time']:>7.3f}s  "
            f"{r['total_time']:>7.3f}s  "
            f"{r['tasks_per_sec']:>9.1f}")

    out()
    best = max(results, key=lambda r: r['tasks_per_sec'])
    out(f"Peak throughput: {best['tasks_per_sec']:.1f} tasks/s "
        f"(batch size {best['batch_size']})")

    return results


# ---- main ------------------------------------------------------------------

async def main():

    # Parse optional batch sizes from command line
    if len(sys.argv) > 1:
        batch_sizes = [int(x) for x in sys.argv[1:]]
    else:
        batch_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024,
                       2048, 4096, 8192,
                       16384, 32768, 65536
                       ]

    # ---- set up Rhapsody session with Edge backend ---
    # Edge auto-discovery: ``get_backend('edge')`` with no
    # ``bridge_url`` / ``edge_name`` resolves the bridge URL via
    # radical.edge.utils and selects the first connected edge
    # advertising the rhapsody plugin.  ``await backend`` raises
    # RuntimeError if no candidate is found.
    backend = rhapsody.get_backend('edge', backends=['noop'])
    backend = await backend       # async init (registers remote session)

    out(f"Bridge:  {backend._bridge_url}")
    out(f"Edge:    {backend._edge_name}")
    out(f"Batches: {batch_sizes}")

    session = rhapsody.Session(backends=[backend])

    await run_pass(session, batch_sizes, hetero=False)
    await run_pass(session, batch_sizes, hetero=True)
    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
