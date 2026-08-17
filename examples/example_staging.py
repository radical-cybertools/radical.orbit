#!/usr/bin/env python3
"""
Example: File staging between client and endpoint.

Demonstrates listing a remote directory, uploading a file, and downloading it.
"""

import os
import tempfile

from radical.orbit import EndpointRuntime


def main():

    rt   = EndpointRuntime()
    rt.start(wait=True)

    # topology() also lists the broker and this script's own consumer
    # participant (role='consumer'); pick a real endpoint hosting 'staging'.
    eid = next((n for n, info in rt.topology().items()
                if info.get('role') == 'endpoint'
                and 'staging' in (info.get('plugins') or {})), None)

    if not eid:
        print("No endpoint with the 'staging' plugin found - "
              "start an endpoint service first")
        rt.stop()
        return

    staging = rt.get_plugin(eid, 'staging')

    # Create a local test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Hello from the client!")
        local_src = f.name

    try:
        # List remote /tmp
        result = staging.list("/tmp")
        print(f"Remote /tmp has {len(result['entries'])} entries")

        # Upload
        remote_path = f"/tmp/endpoint_staging_test_{os.getpid()}.txt"
        staging.put(local_src, remote_path)
        print(f"Uploaded: {remote_path}")

        # Download back
        local_dst = local_src + ".downloaded"
        staging.get(remote_path, local_dst)
        print(f"Downloaded: {local_dst}")

    finally:
        for p in (local_src, local_src + ".downloaded"):
            if os.path.exists(p):
                os.unlink(p)

    staging.close()
    rt.stop()
    print("Done.")


if __name__ == "__main__":
    main()
