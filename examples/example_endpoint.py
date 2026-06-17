#!/usr/bin/env python3
"""
example_endpoint.py — Submit a child Endpoint service as a batch job.

This example demonstrates how to use the psij ``submit_tunneled`` API to launch a
new ORBIT service on a compute node, optionally setting up a reverse
SSH tunnel so the compute node can reach the bridge through the login node.

Usage
-----
    python examples/example_endpoint.py \\
        --url    http://login09.frontier.olcf.ornl.gov:8000 \\
        --endpoint   frontier                    # parent endpoint that will submit the job
        --name   frontier.compute1           # name the child endpoint will register as
        --queue  batch                       # SLURM partition
        --account ABC123                     # SLURM account
        --nodes  1                           # number of nodes
        --tunnel                             # set up reverse SSH tunnel (required on
                                             # systems where compute nodes cannot reach
                                             # the bridge directly)

Prerequisites
-------------
- A running bridge (orbit-bridge.py)
- A running parent endpoint on the login node (orbit-endpoint-wrapper.sh --url <bridge>)
- The parent endpoint must have the 'psij' plugin loaded
"""

import argparse
import sys
import time

from radical.orbit import BridgeClient


def main():
    parser = argparse.ArgumentParser(
        description="Submit a child Endpoint service as a batch job via PsiJ.")
    parser.add_argument('--url',     required=True,
                        help="Bridge URL, e.g. http://login09.host:8000")
    parser.add_argument('--endpoint',    required=True,
                        help="Parent endpoint name (must have psij plugin)")
    parser.add_argument('--name',    required=True,
                        help="Name for the child endpoint service to register as")
    parser.add_argument('--queue',   default=None,
                        help="SLURM partition / PBS queue")
    parser.add_argument('--account', default=None,
                        help="Allocation account / project")
    parser.add_argument('--nodes',   type=int, default=1,
                        help="Number of compute nodes (default: 1)")
    parser.add_argument('--duration', type=int, default=600,
                        help="Job wall time in seconds (default: 600)")
    parser.add_argument('--executor', default='slurm',
                        choices=['slurm', 'pbs', 'lsf', 'local'],
                        help="PsiJ executor (default: slurm)")
    parser.add_argument('--tunnel', action='store_true',
                        help="Set up reverse SSH tunnel (login → compute)")
    parser.add_argument('--plugins', default=None,
                        help="Comma-separated list of plugins for the child endpoint "
                             "(default: same as parent)")
    args = parser.parse_args()

    # ── Connect to bridge ────────────────────────────────────────────────────
    print(f"Connecting to bridge at {args.url} …")
    client = BridgeClient(url=args.url)

    endpoints = client.list_endpoints()
    if args.endpoint not in endpoints:
        print(f"ERROR: endpoint '{args.endpoint}' not connected. Available: {list(endpoints)}")
        client.close()
        sys.exit(1)

    endpoint = client.get_endpoint_client(args.endpoint)
    psij = endpoint.get_plugin('psij')

    # ── Determine plugins to forward to child endpoint ───────────────────────────
    if args.plugins:
        plugins_arg = args.plugins
    else:
        parent_plugins = endpoints[args.endpoint].get('plugins', [])
        plugins_arg = ','.join(p for p in parent_plugins)

    # ── Build job spec ───────────────────────────────────────────────────────
    arguments = [
        '--url',  args.url,
        '--name', args.name,
    ]
    if plugins_arg:
        arguments += ['-p', plugins_arg]

    attributes = {'duration': str(args.duration), 'node_count': args.nodes}
    if args.queue:
        attributes['queue_name'] = args.queue
    if args.account:
        attributes['account'] = args.account

    # When tunnel=True the server-side watcher writes the port to the hardcoded
    # path ~/.radical/orbit/tunnels/{endpoint_name}.port.  We inject --tunnel into
    # the child endpoint's arguments so it knows to wait for that file.
    if args.tunnel:
        arguments.append('--tunnel')

    job_spec = {
        'executable': 'orbit-endpoint-wrapper.sh',
        'arguments':  arguments,
        'attributes': attributes,
    }

    # ── Submit the endpoint job ──────────────────────────────────────────────────
    print(f"Submitting endpoint job '{args.name}' via executor '{args.executor}' …")
    if args.tunnel:
        print("  Reverse SSH tunnel requested — watcher will spawn SSH once job starts.")
        import pathlib as _pl
        relay_file = _pl.Path.home() / '.radical' / 'orbit' / 'tunnels' / f'{args.name}.port'
        print(f"  Relay port file: {relay_file}")

    result = psij.submit_tunneled(job_spec, executor=args.executor, tunnel=args.tunnel)

    job_id    = result['job_id']
    native_id = result.get('native_id', '?')
    endpoint_name = result['endpoint_name']

    print(f"  job_id    : {job_id}")
    print(f"  native_id : {native_id}")
    print(f"  endpoint_name : {endpoint_name}")

    if not args.tunnel:
        print("\nNo tunnel requested.  Waiting for child endpoint to connect to bridge …")
        _wait_for_endpoint(client, endpoint_name, timeout=300)
        return

    # ── Poll tunnel status ───────────────────────────────────────────────────
    print("\nWaiting for tunnel to become active …")
    for attempt in range(120):          # up to 10 min (5s × 120)
        time.sleep(5)
        status = psij.tunnel_status(endpoint_name)
        st = status.get('status', '?')
        port = status.get('port')
        pid  = status.get('pid')

        if st == 'pending':
            print(f"  [{attempt * 5:>4}s] pending (job queued / starting) …")
        elif st == 'active':
            print(f"  tunnel active!  port={port}, pid={pid}")
            break
        elif st == 'failed':
            print("  ERROR: tunnel watcher failed.")
            client.close()
            sys.exit(1)
        elif st == 'done':
            print("  Watcher finished (tunnel completed).")
            break
        else:
            print(f"  [{attempt * 5:>4}s] status={st}")
    else:
        print("  Timed out waiting for tunnel.")
        client.close()
        sys.exit(1)

    # ── Wait for the child endpoint to appear ────────────────────────────────────
    print(f"\nWaiting for child endpoint '{endpoint_name}' to register with the bridge …")
    _wait_for_endpoint(client, endpoint_name, timeout=120)

    client.close()


def _wait_for_endpoint(client: BridgeClient, endpoint_name: str, timeout: int = 300) -> None:
    """Poll until *endpoint_name* appears in the bridge's endpoint list."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        endpoints = client.list_endpoints()
        if endpoint_name in endpoints:
            plugins = endpoints[endpoint_name].get('plugins', [])
            print(f"  Child endpoint '{endpoint_name}' connected!  plugins: {plugins}")
            return
        time.sleep(5)
        print("  … still waiting …")

    print(f"  Timed out after {timeout}s — '{endpoint_name}' did not connect.")
    client.close()
    sys.exit(1)


if __name__ == '__main__':
    main()
