#!/usr/bin/env python3
"""
Clean up after ``lucid_e2e_demo.py``.

Without arguments: list the demo runs under LUCID ``/tmp_radical/``.
With run ids (or ``--all``): delete them there (Globus delete task), and with
``--nersc`` also remove the run dirs under ``$SCRATCH/lucid-e2e/runs/`` on the
endpoint (rhapsody task).
"""

import argparse
import json
import os
import sys
import time

from pathlib import Path

from radical.orbit            import EndpointRuntime
from radical.orbit.lucid_flow import RUN_ID_RE


LUCID_COLL = '54387d00-a525-11f0-8d5c-0affdd0cd947'
CLIENT_ID  = '8b84fc2d-49e9-49ea-b54d-b3a29a70cf31'
OUT_BASE   = '/tmp_radical'


def main():

    ap = argparse.ArgumentParser(description='clean up LUCID e2e demo runs')
    ap.add_argument('runs',       nargs='*', help='run ids (e2e-YYYYmmdd-HHMMSS)')
    ap.add_argument('--all',      action='store_true', help='all runs in ' + OUT_BASE)
    ap.add_argument('--nersc',    action='store_true', help='also remove NERSC run dirs')
    ap.add_argument('--broker',   default=os.environ.get('RADICAL_ORBIT_BROKER_URL'))
    ap.add_argument('--endpoint', help='endpoint name (default: auto-detect)')
    ap.add_argument('--token',    default=str(Path.home() / '.globus' / 'auth_tokens.json'))
    args = ap.parse_args()

    data    = json.load(open(args.token))
    refresh = next(t['refresh_token'] for t in data.get('other_tokens', [])
                   if t['resource_server'] == 'transfer.api.globus.org')

    rt = EndpointRuntime(broker_url=args.broker)
    rt.start(wait=True)
    try:
        eid = next((n for n, i in rt.topology().items()
                    if i.get('role') == 'endpoint'
                    and (not args.endpoint or n == args.endpoint)
                    and 'globus' in (i.get('plugins') or {})), None)
        if not eid:
            sys.exit('no endpoint serving globus found')
        globus = rt.get_plugin(eid, 'globus', refresh_token=refresh,
                               client_id=CLIENT_ID)

        # only names that look like demo runs: /tmp_radical is shared, and
        # the names end up in Globus deletes and an ``rm -rf``
        existing = sorted(e['name'] for e in
                          globus.ls(LUCID_COLL, OUT_BASE + '/').get('entries', [])
                          if e.get('type') == 'dir'
                          and RUN_ID_RE.match(e.get('name') or ''))
        runs = existing if args.all else args.runs
        if not runs:
            print(f'runs in lucid:{OUT_BASE}/:')
            for r in existing:
                print(f'  {r}')
            return

        for r in runs:
            if r not in existing:
                print(f'  {r}: not a demo run in lucid:{OUT_BASE}/ -- skipped')
        runs  = [r for r in runs if r in existing]
        paths = [f'{OUT_BASE}/{r}/' for r in runs]
        if paths:
            sub = globus.delete(LUCID_COLL, paths, recursive=True,
                                label='lucid e2e cleanup')
            while True:
                task = globus.get_task(sub['task_id'])
                if task.get('status') in ('SUCCEEDED', 'FAILED'):
                    break
                time.sleep(3)
            print(f'  lucid: deleted {len(paths)} run(s): {task.get("status")}')

        if args.nersc and runs:
            # run ids as positional args, never spliced into the script
            rh  = rt.get_plugin(eid, 'rhapsody')
            cmd = 'cd "$SCRATCH/lucid-e2e/runs" && rm -rf -- "$@"'
            uid = rh.submit_tasks([{'executable': '/bin/bash',
                                    'arguments' : ['-lc', cmd, 'rm'] + runs}])[0]['uid']
            rh.wait_tasks([uid], timeout=600)
            print(f'  nersc: removed {len(runs)} run dir(s): '
                  f'{rh.get_task(uid).get("state")}')
            rh.close()
        globus.close()
    finally:
        rt.stop()


if __name__ == '__main__':
    main()
