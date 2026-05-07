#!/usr/bin/env python3
"""
Example: Rhapsody Individual Task Submission
=============================================

Submits tasks one-by-one through the Edge Rhapsody backend.  The Edge
backend collects individually submitted tasks over a short time window
and flushes them as bulk HTTP requests, so single-task submit calls
still achieve high throughput.

Uses the noop backend so tasks complete instantly — pure infrastructure
overhead measurement.

Usage:
  python examples/example_rhapsody_individual.py [options]

  --tasks N                  Number of tasks (default 8192)
  --batch-window SEC         Client submit batch window (default 0.05)
  --batch-limit N            Client submit batch size limit (default 1024)
  --notify-window SEC        Edge notification batch window (default 0.05)
  --notify-limit N           Edge notification batch size (default 256)
"""

import argparse
import asyncio
import time

import rhapsody


def _noop():
    """Minimal function task."""
    return True

def _noop_arg(x):
    """Minimal function task with one argument."""
    return x


def parse_args():
    p = argparse.ArgumentParser(
        description='Individual task submission benchmark')
    p.add_argument('--tasks',         "-t", type=int,   default=8192)
    p.add_argument('--batch-window',  "-w", type=float, default=0.05)
    p.add_argument('--batch-limit',   "-l", type=int,   default=1024)
    p.add_argument('--notify-window', "-W", type=float, default=0.05)
    p.add_argument('--notify-limit',  "-L", type=int,   default=256)
    return p.parse_args()


async def main():

    args = parse_args()

    # ---- set up Rhapsody session with Edge backend ---
    # Edge auto-discovery: ``get_backend('edge')`` with no
    # ``bridge_url`` / ``edge_name`` resolves the bridge URL via
    # radical.edge.utils and selects the first connected edge
    # advertising the rhapsody plugin.  ``await backend`` raises
    # RuntimeError if no candidate is found.
    backend = rhapsody.get_backend(
        'edge',
        backends=['noop'],
        batch_window=args.batch_window,
        batch_limit=args.batch_limit,
        notify_batch_window=args.notify_window,
        notify_batch_size=args.notify_limit,
    )
    backend = await backend

    print(f"Bridge:         {backend._bridge_url}")
    print(f"Edge:           {backend._edge_name}")
    print(f"Tasks:          {args.tasks}")
    print(f"Batch window:   {args.batch_window}s")
    print(f"Batch limit:    {args.batch_limit}")
    print(f"Notify window:  {args.notify_window}s")
    print(f"Notify limit:   {args.notify_limit}")

    session = rhapsody.Session(backends=[backend])

    # ---- submit tasks individually ---
    n_tasks = args.tasks
    print(f"\nSubmitting {n_tasks} tasks one at a time ...")
    all_tasks = []
    t0 = time.time()

    for i in range(n_tasks):
        task = rhapsody.ComputeTask(function=_noop_arg, args=(i,))
        await session.submit_tasks([task])
        all_tasks.append(task)

    t_submit = time.time() - t0

    # ---- wait for all ---
    print(f"\nAll submitted in {t_submit:.2f}s  "
          f"({n_tasks / t_submit:.1f} tasks/s)")
    print("Waiting for completion ...")

    t1 = time.time()
    await session.wait_tasks(all_tasks)
    t_wait = time.time() - t1

    t_total = t_submit + t_wait
    print("\nResults:")
    print(f"  Submit:  {t_submit:.2f}s  ({n_tasks / t_submit:.1f} tasks/s)")
    print(f"  Wait:    {t_wait:.2f}s")
    print(f"  Total:   {t_total:.2f}s  ({n_tasks / t_total:.1f} tasks/s)")

    await session.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
