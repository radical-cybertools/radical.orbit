#!/usr/bin/env python3
"""
Example: Rhapsody Plugin via ORBIT
=========================================

Submits a batch of compute tasks through the ORBIT bridge,
waits for completion, and prints the results and session statistics.

Prerequisites:
  - A ORBIT bridge is running (RADICAL_ORBIT_BRIDGE_URL set).
  - An endpoint service is connected with the Rhapsody plugin loaded.
  - The ``rhapsody`` package is installed on the endpoint node.
"""

import json
import time

from radical.orbit import BridgeClient


def my_notification_cb(endpoint: str, plugin: str, topic: str, data: dict):
    print(f"[Notification]   {endpoint}/{plugin} topic={topic} data={data}")


def main():

    # ---- connect to the bridge ----
    bc  = BridgeClient()
    eids = bc.list_endpoints()

    if not eids:
        print("No endpoints found.")
        return

    eid = eids[1]
    print(f"Using endpoint: {eid}")

    ec = bc.get_endpoint_client(eid)
    rh = ec.get_plugin('rhapsody')

    # Register for asynchronous bridge notifications
    rh.register_notification_callback(my_notification_cb)

    # ---- define tasks ----
    tasks = [
        {
            "executable": "/bin/echo",
            "arguments" : ["hello from task 1"],
        },
        {
            "executable": "/bin/echo",
            "arguments" : ["hello from task 2"],
        },
        {
            "executable": "/bin/sleep",
            "arguments" : ["2"],
        },
    ]

    # ---- submit ----
    # To submit to a specific Rhapsody backend (e.g., dragon, flux), you can
    # pass the `backend` argument. E.g.:
    # submitted = rh.submit_tasks(tasks, backend="dragon_v3")
    print(f"Submitting {len(tasks)} tasks ...")
    submitted = rh.submit_tasks(tasks)

    uids = [t['uid'] for t in submitted]
    print(f"  Task UIDs: {uids}")

    # ---- wait for completion ----
    print("Waiting for tasks to complete ...")
    t0 = time.time()
    completed = rh.wait_tasks(uids)
    elapsed = time.time() - t0
    print(f"  All tasks finished in {elapsed:.1f}s")

    # ---- print results ----
    print("\n" + "=" * 60)
    print(" Results")
    print("=" * 60 + "\n")
    for task in completed:
        uid   = task.get('uid', '?')
        state = task.get('state', '?')
        out   = (task.get('stdout') or '').strip()
        err   = (task.get('stderr') or '').strip()
        rc    = task.get('exit_code')

        print(f"  [{uid}]   state={state}  exit_code={rc}")
        if out:
            print(f"    stdout: {out}")
        if err:
            print(f"    stderr: {err}")

    # ---- individual task query ----
    print("\n" + "-" * 60)
    print(f" Querying individual task: {uids[0]}")
    info = rh.get_task(uids[0])
    print(json.dumps(info, indent=2, default=str))

    # ---- cleanup ----
    rh.close()
    bc.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
