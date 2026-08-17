#!/usr/bin/env python3

from radical.orbit import EndpointRuntime


def main():

    rt = EndpointRuntime()
    rt.start(wait=True)

    # topology() also lists the broker and this script's own consumer
    # participant (role='consumer'); pick a real endpoint hosting 'lucid'.
    eid = next((n for n, info in rt.topology().items()
                if info.get('role') == 'endpoint'
                and 'lucid' in (info.get('plugins') or {})), None)

    if not eid:
        print("No endpoint with the 'lucid' plugin found.")
        return

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

