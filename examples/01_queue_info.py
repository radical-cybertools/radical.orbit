#!/usr/bin/env python3

"""
Example: queue_info plugin
==========================

Demonstrates the queue_info plugin which exposes SLURM queue information,
job listings, and allocation data via REST.

Usage:
  # default — connects to the first registered endpoint
  ./examples/01_queue_info.py

  # multi-cluster — load a second plugin instance with a custom name
  ./examples/01_queue_info.py --name=frontier --slurm_conf=/etc/slurm/frontier.conf

Prerequisites:
  - Bridge running   (bin/radical-orbit-bridge.py)
  - Endpoint running     (bin/radical-orbit-endpoint.py)
  - SLURM commands available on the endpoint node (sinfo, squeue, sacctmgr)
"""

import argparse
import httpx
import pprint
import sys

BRIDGE_HTTP = "https://localhost:8000"


# ------------------------------------------------------------------------------
#
def main():

    parser = argparse.ArgumentParser(description='queue_info example client')
    parser.add_argument('--bridge', default=BRIDGE_HTTP,
                        help='Bridge URL (default: %(default)s)')
    parser.add_argument('--name',   default=None,
                        help='Plugin name (for multi-cluster setups)')
    parser.add_argument('--slurm_conf', default=None,
                        help='Path to slurm.conf on the endpoint node')
    parser.add_argument('--queue',  default=None,
                        help='Partition name for job listing')
    parser.add_argument('--user',   default=None,
                        help='User name to filter jobs/allocations')
    args = parser.parse_args()


    def check(r, label):
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            print(f"Error {r.status_code}: {r.text}")
            sys.exit(1)
        data = r.json()
        print(f"\n--- {label} ---")
        pprint.pprint(data)
        return data


    with httpx.Client(timeout=60.0, verify='cert.pem') as http:

        # --- load plugin ------------------------------------------------
        params = {}
        if args.name:
            params['name'] = args.name
        if args.slurm_conf:
            params['slurm_conf'] = args.slurm_conf

        r    = http.post(f"{args.bridge}/endpoint/load_plugin/radical.queue_info",
                         params=params)
        data = check(r, "load_plugin")
        ns   = data['namespace']
        base = f"{args.bridge}{ns}"
        print(f"namespace: {ns}")

        # --- register client --------------------------------------------
        r    = http.post(f"{base}/register_client")
        data = check(r, "register_client")
        cid  = data['cid']
        print(f"client id: {cid}")

        # --- echo -------------------------------------------------------
        r = http.get(f"{base}/echo/{cid}", params={'q': 'hello-queue-info'})
        check(r, "echo")

        # --- get_info ---------------------------------------------------
        r = http.get(f"{base}/get_info/{cid}")
        check(r, "get_info")

        # --- list_jobs (optional) ---------------------------------------
        if args.queue:
            params = {}
            if args.user:
                params['user'] = args.user
            r = http.get(f"{base}/list_jobs/{cid}/{args.queue}", params=params)
            check(r, f"list_jobs (queue={args.queue})")

        # --- list_allocations -------------------------------------------
        params = {}
        if args.user:
            params['user'] = args.user
        r = http.get(f"{base}/list_allocations/{cid}", params=params)
        check(r, "list_allocations")

        # --- unregister client ------------------------------------------
        r = http.post(f"{base}/unregister_client/{cid}")
        check(r, "unregister_client")

        print("\ndone.")


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    main()


# ------------------------------------------------------------------------------
