#!/usr/bin/env python3

from radical.orbit import EndpointRuntime


def main():

    rt = EndpointRuntime()
    rt.start(wait=True)
    eids = [n for n in rt.topology() if n != 'broker']

    if not eids:
        print("No endpoints found.")
        return

    eid = eids[0]
    print(f"Using endpoint: {eid}")

    lucid = rt.get_plugin(eid, 'lucid')

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

    rt.stop()


if __name__ == "__main__":
    main()

