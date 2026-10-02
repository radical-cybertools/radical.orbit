#!/usr/bin/env python3
"""
LUCID end-to-end demo: CellProfiler on Perlmutter, driven through ORBIT.

  1. stage-in   Globus: pipeline + one field per well of the ANL week-1 P1
                plate, LUCID collection -> NERSC DTN (fresh run dir)
  2. compute    rhapsody: one CellProfiler task per well (docker image via
                shifter), Abe's ``cropped_cells.cppipe``
  3. package    tarball of the cropped cells, per-well counts, montage,
                preview crops
  4. stage-out  Globus: results -> LUCID ``/tmp_radical/<run-id>/``
  5. show       fetch montage + previews (staging plugin), open the montage

The flow lives in ``radical.orbit.lucid_flow`` (shared with the ``lucid``
portal plugin).  P1 carries only channels 4 and 5: ch4 is fed as DNA, ch5
fills the other four Cell Painting slots -- an infrastructure demo, not a
scientific result.

Prerequisites
  * broker reachable, ``RADICAL_ORBIT_BROKER_URL`` set (or ``--broker``)
  * an endpoint on a Perlmutter compute allocation serving ``globus``,
    ``rhapsody`` and ``staging`` (``-p default,globus``), started with
    ``RADICAL_ORBIT_SCRATCH_BASE=$SCRATCH``
  * a Globus Transfer token with NERSC DTN data_access consent in
    ``~/.globus/auth_tokens.json`` (``get_globus_token.py``)

Clean up the LUCID tmp target with ``lucid_e2e_cleanup.py``.
"""

import argparse
import os
import shutil
import subprocess
import sys

from radical.orbit            import EndpointRuntime
from radical.orbit.lucid_flow import (LucidRun, LucidError, WELLS, DEFAULTS,
                                      TOKEN_FILE, candidate_endpoints,
                                      transfer_refresh_token)


def gb(n):
    return f'{(n or 0) / 1e9:.2f} GB'


class Printer:
    '''Print phase changes and task-count changes only.'''

    def __init__(self):
        self._last = None

    def __call__(self, st):
        phase = st['phase']
        t     = st['tasks']
        sig   = (phase, st['phases'][phase]['state'] if phase else None,
                 t['executing'], t['done'], t['failed'])
        if sig == self._last:
            return
        self._last = sig
        if phase == 'compute' and sig[1] == 'running':
            print(f'    compute: {t["submitted"]} submitted, {t["executing"]} '
                  f'executing, {t["done"]} done, {t["failed"]} failed',
                  flush=True)
        elif phase:
            print(f'    {phase}: {sig[1]}', flush=True)


def main():

    ap = argparse.ArgumentParser(description='LUCID end-to-end demo via ORBIT')
    ap.add_argument('--broker',   default=os.environ.get('RADICAL_ORBIT_BROKER_URL'))
    ap.add_argument('--endpoint', help='endpoint name (default: auto-detect)')
    ap.add_argument('--wells',    type=int, default=96, help='wells to process (1-96)')
    ap.add_argument('--backend',  default=DEFAULTS['backend'], help='rhapsody backend')
    ap.add_argument('--token',    default=TOKEN_FILE)
    ap.add_argument('--outdir',   default='.', help='local dir for fetched images')
    ap.add_argument('--no-show',  action='store_true', help='do not open the montage')
    args = ap.parse_args()

    try:
        refresh = transfer_refresh_token(args.token)
    except LucidError as e:
        sys.exit(str(e))

    n_wells = max(1, min(args.wells, len(WELLS)))
    rt = EndpointRuntime(broker_url=args.broker)
    rt.start(wait=True)
    try:
        cands = candidate_endpoints(rt.topology())
        eid   = args.endpoint or (cands[0] if cands else None)
        if not eid or eid not in cands:
            sys.exit('no endpoint serving globus, rhapsody and staging found '
                     '(start it with -p default,globus)')
        run = LucidRun(rt, eid, WELLS[:n_wells], refresh_token=refresh,
                       local_dir=args.outdir, backend=args.backend,
                       on_update=Printer())
        print(f'run {run.run_id}: endpoint {eid}, {n_wells} wells, '
              f'backend {args.backend}')
        st = run.run()
    finally:
        rt.stop()

    if st['status'] != 'done':
        sys.exit(f'run failed in {st["phase"]}: {st["error"]}')

    s  = st['stats']
    si = s['stage_in']
    so = s['stage_out']
    c  = s['compute']
    ph = {p: v['secs'] or 0 for p, v in st['phases'].items()}
    print(f'''
== LUCID e2e demo {st["run_id"]} ==================================
  stage-in   {si["files"]:6d} files  {gb(si["bytes"]):>10s}   {ph["stage_in"]:6.0f} s
  compute    {c["done"]:6d} tasks  {c["nodes"]:4d} nodes   {ph["compute"]:6.0f} s   ({c["done"]}/{c["tasks"]} ok)
             {c["files"]:6d} files created
  stage-out  {so["files"]:6d} files  {gb(so["bytes"]):>10s}   {ph["stage_out"]:6.0f} s
  results    lucid:{st["target"]}
  images     {os.path.dirname(st["images"][0]) if st["images"] else "-"}
''')
    for w, err in st['failed_wells'].items():
        print(f'  failed {w}: {err}')

    if st['images'] and not args.no_show and shutil.which('xdg-open'):
        subprocess.Popen(['xdg-open', st['images'][0]],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
