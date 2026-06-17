#!/usr/bin/env python3
"""
Example: file staging via Globus Online (Transfer API).

Submits a transfer between two Globus collections, waits for it to finish,
and lists the destination directory.  Globus moves the data
collection-to-collection out of band — nothing flows through the client,
edge, or bridge.

Requirements
------------
* A running bridge with at least one connected edge that loads the
  ``globus`` plugin (needs ``globus-sdk`` installed on the edge).
* A Globus Transfer token.  Acquire one via Globus Auth (for example with the
  Globus CLI), and export it, e.g.::

      export GLOBUS_ACCESS_TOKEN="$(...)"        # access token, or
      export GLOBUS_REFRESH_TOKEN=... GLOBUS_CLIENT_ID=...

Collections
-----------
Defaults to the public **Globus Tutorial Collection 1 / 2** (GCSv5),
overridable via ``GLOBUS_SRC`` / ``GLOBUS_DST``.  The literal ``"local"``
resolves to the edge's configured local collection.
"""

import os
import time

from radical.edge import BridgeClient

# Public Globus tutorial collections (guest collections, world-readable demo
# data under /home/share/godata/).  Override via env for real endpoints.
TUTORIAL_SRC = '6c54cade-bde5-45c1-bdea-f4bd71dba2cc'
TUTORIAL_DST = '31ce9ba0-176d-45a5-add3-f37d233ba47d'

SRC  = os.environ.get('GLOBUS_SRC', TUTORIAL_SRC)
DST  = os.environ.get('GLOBUS_DST', TUTORIAL_DST)
SRC_PATH = os.environ.get('GLOBUS_SRC_PATH', '/home/share/godata/')
DST_PATH = os.environ.get('GLOBUS_DST_PATH', '/~/edge-globus-demo/')


def _auth_kwargs() -> dict:
    """Build register_session auth kwargs from the environment."""
    access  = os.environ.get('GLOBUS_ACCESS_TOKEN')
    refresh = os.environ.get('GLOBUS_REFRESH_TOKEN')
    cid     = os.environ.get('GLOBUS_CLIENT_ID')

    if access:
        return {'access_token': access}
    if refresh and cid:
        return {'refresh_token': refresh, 'client_id': cid}

    raise SystemExit(
        'No Globus credential found. Set GLOBUS_ACCESS_TOKEN, or '
        'GLOBUS_REFRESH_TOKEN + GLOBUS_CLIENT_ID '
        '(see get_globus_token.py).')


def main():

    bc   = BridgeClient()
    eids = bc.list_edges()

    if not eids:
        print('No edges connected - start an edge service first')
        bc.close()
        return

    ec     = bc.get_edge_client(eids[0])
    globus = ec.get_plugin('globus')

    # Register a session carrying the Globus token.
    globus.register_session(**_auth_kwargs())

    try:
        # Submit a recursive directory transfer.
        sub = globus.submit_transfer(
            source=SRC, destination=DST,
            items=[{'source': SRC_PATH, 'destination': DST_PATH,
                    'recursive': True}],
            label='radical-edge globus example')
        task_id = sub['task_id']
        print(f'Submitted transfer: {task_id}')

        # Wait for completion (polls Globus server-side).
        deadline = time.time() + 300
        while time.time() < deadline:
            res = globus.task_wait(task_id, timeout=30, polling_interval=10)
            if res['completed']:
                break
            print('  … still transferring')

        task = globus.get_task(task_id)
        print(f"Final status: {task.get('status')} "
              f"({task.get('files_transferred')} files, "
              f"{task.get('bytes_transferred')} bytes)")

        # List what landed on the destination.
        listing = globus.ls(DST, DST_PATH)
        names   = [e.get('name') for e in listing.get('entries', [])]
        print(f'Destination {DST_PATH} now has {len(names)} entries: {names}')

    finally:
        globus.close()
        bc.close()
        print('Done.')


if __name__ == '__main__':
    main()
