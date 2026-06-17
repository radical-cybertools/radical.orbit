#!/usr/bin/env python3
"""
Example: File staging between client and endpoint.

Demonstrates listing a remote directory, uploading a file, and downloading it.
"""

import os
import tempfile

from radical.orbit import BridgeClient


def main():

    bc   = BridgeClient()
    eids = bc.list_endpoints()

    if not eids:
        print("No endpoints connected - start an endpoint service first")
        bc.close()
        return

    ec      = bc.get_endpoint_client(eids[0])
    staging = ec.get_plugin('staging')

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
    bc.close()
    print("Done.")


if __name__ == "__main__":
    main()
