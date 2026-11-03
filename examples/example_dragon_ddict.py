#!/usr/bin/env python3
"""Dragon DDict through ORBIT (Orbit-Dragon POC).

Needs an endpoint that serves the ``dragon`` plugin next to a Dragon
runtime (dragonhpc >= 0.15.0)::

    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,dragon

The client creates a DDict, writes to it, and (with ``--runtime-check``,
when the client runs on a host of the Dragon runtime) starts a Dragon
process that attaches to the same DDict by its descriptor and writes back.

Usage::

    python examples/example_dragon_ddict.py [endpoint] [--runtime-check]
"""

import sys

from radical.orbit import EndpointRuntime


def runtime_reader(descriptor):
    """Runs inside the Dragon runtime: read the client's data, answer."""
    from dragon.data.ddict import DDict
    dd = DDict.attach(descriptor)
    dd['from_runtime'] = {'seen': dd['from_client']}
    dd.detach()


def runtime_check(dd):
    """Start a Dragon process that attaches to *dd* by its descriptor."""
    from dragon.workflows.runtime import attach
    from dragon.native.process import Process

    rt = attach(timeout=60)
    try:
        proc = Process(target=runtime_reader, args=(dd.descriptor,))
        proc.start()
        proc.join()
        print(f"runtime process exit code: {proc.returncode}")
    finally:
        rt.detach()
    print(f"from_runtime: {dd['from_runtime']}")


def main():

    args  = [a for a in sys.argv[1:] if not a.startswith('--')]
    check = '--runtime-check' in sys.argv

    rt = EndpointRuntime()
    rt.start(wait=True)
    try:
        eids = [name for name, info in rt.topology().items()
                if info and 'dragon' in info.get('plugins', [])]
        if not eids:
            print("no endpoint serves the 'dragon' plugin")
            return 1
        eid = args[0] if args else eids[0]
        print(f"endpoint: {eid}")

        dragon = rt.get_plugin(eid, 'dragon')

        dd = dragon.ddict.create(name='demo', total_mem='64M')
        print(f"created:  {dd.id} ({dd.name})")
        try:
            dd['from_client'] = [1, 2, 3]
            dd['blob']        = b'\x00\x01\x02'
            dd.update({f'k{i}': {'i': i} for i in range(10)})

            print(f"len:      {len(dd)}")
            print(f"blob:     {dd['blob']!r}")
            print(f"k3:       {dd['k3']}")
            print(f"missing:  {dd.get('nope', '<none>')}")

            # a second client finds the same DDict by name
            print(f"attached: {dragon.ddict.attach('demo')}")
            print(f"listed:   {[e['name'] for e in dragon.ddict.list()]}")

            if check:
                runtime_check(dd)
        finally:
            dd.destroy()
            print(f"destroyed {dd.id}")
            dragon.close()
    finally:
        rt.stop()
    return 0


if __name__ == '__main__':
    sys.exit(main())
