#!/usr/bin/env python3

from radical.orbit import BridgeClient


def main():

    bc = BridgeClient()
    eids = bc.list_endpoints()

    if not eids:
        print("No endpoints found.")
        return

    eid = eids[0]
    print(f"Using endpoint: {eid}")

    ec = bc.get_endpoint_client(eid)
    lucid = ec.get_plugin('lucid')

    print("Submitting pilot...")
    res = lucid.pilot_submit({
        'resource': 'local.localhost',
        'nodes': 1,
        'runtime': 10
    })
    pid = res['pid']
    print(f"Pilot ID: {pid}")

    print("Submitting tasks...")
    tids = []
    for _ in range(3):
        res = lucid.task_submit({'description': {'executable': 'date'}})
        tid = res['tid']
        tids.append(tid)
        print(f"Task ID: {tid}")

    for tid in tids:
        print(f"Waiting for task {tid}...")
        res = lucid.task_wait(tid)
        stdout = res['task']['stdout'].strip()
        print(f"Task {tid} result: {stdout}")

    bc.close()


if __name__ == "__main__":
    main()

