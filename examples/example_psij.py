#!/usr/bin/env python3

import argparse
import json
import sys
import time
from radical.orbit import BridgeClient


def my_notification_cb(topic: str, data: dict):
    print(f"\n[Notification] {topic}: {data}\n")


def get_config(config_path: str = None) -> dict:

    # Default configuration
    config = {
        "endpoint_id_match": None,
        "endpoint_id_prefix": None,
        "job_executor": None,
        "job_spec": {
            "executable": "/bin/sleep",
            "arguments": ["5"],
            "attributes": {
                # Standard attributes:
                "account": None, # Will auto-discover if None
                "queue_name": None, # Will auto-discover if None
                "duration": "100",
                # You can also pass custom/backend-specific attributes
                # e.g., "slurm.constraint": "cpu_gen_1"
            }
        }
    }

    if config_path:
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
        except Exception as e:
            print(f"Failed to load config file {config_path}: {e}")
            sys.exit(1)
        else:
            config.update(user_config)

    return config


def main():
    parser = argparse.ArgumentParser(description="PSI/J Job Submission Example")
    parser.add_argument('--config', type=str, default=None,
                        help="Path to JSON configuration file")
    args = parser.parse_args()

    config = get_config(args.config)

    bc = BridgeClient()
    eids = bc.list_endpoints()

    if not eids:
        print("No endpoints found.")
        return

    eid = None
    endpoint_id_match = config.get("endpoint_id_match")
    endpoint_id_prefix = config.get("endpoint_id_prefix")
    
    if endpoint_id_match:
        for _eid in eids:
            if endpoint_id_match in _eid:
                eid = _eid
                break
        if not eid:
            print(f"No endpoint found matching '{endpoint_id_match}'.")
            return
    elif endpoint_id_prefix:
        for _eid in eids:
            if _eid.startswith(endpoint_id_prefix):
                eid = _eid
                break
        if not eid:
            print(f"No endpoint found starting with prefix '{endpoint_id_prefix}'.")
            return
    else:
        eid = eids[0]

    print(f"Using endpoint: {eid}")

    ec = bc.get_endpoint_client(eid)
    pi = ec.get_plugin('psij')

    # Register for asynchronous bridge notifications
    pi.register_notification_callback(my_notification_cb)

    job_spec = config.get("job_spec")
    attrs = job_spec.get("attributes", {})
    
    # Optional: Automatically discover queues and accounts if they aren't provided
    if "queue_info" in ec.list_plugins() and (not attrs.get("queue_name") or not attrs.get("account")):
        print("Discovering queue information...")
        qi = ec.get_plugin("queue_info")
        
        if not attrs.get("queue_name"):
            info = qi.get_info()
            queues = info.get("queues", {})
            qlist = list(queues.values()) if isinstance(queues, dict) else info
            if qlist:
                # Simple priority selection
                for pref in ['debug', 'interactive']:
                    match = next((q for q in qlist if (q.get('name') or q.get('partition') or q) == pref), None)
                    if match:
                        attrs["queue_name"] = pref
                        break
                if not attrs.get("queue_name"):
                    first_q = qlist[0]
                    attrs["queue_name"] = first_q.get("name") or first_q.get("partition") or first_q

        if not attrs.get("account"):
            allocs = qi.list_allocations()
            alloc_list = allocs.get("allocations", [])
            accounts = set(a.get("account") for a in alloc_list if a.get("account"))
            if accounts:
                attrs["account"] = list(accounts)[0]

    job_executor = config.get("job_executor")

    print(f"Submitting Job (Queue: {attrs.get('queue_name')}, Account: {attrs.get('account')})...")
    if job_executor:
        res = pi.submit_job(job_spec, job_executor)
    else:
        res = pi.submit_job(job_spec)
        
    job_id = res['job_id']

    print(f"\nMonitoring Job {job_id}")
    print("-" * 30)

    try:
        while True:
            res = pi.get_job_status(job_id)
            state = res['state']
            print(f"Status: {state:<12} (at {time.strftime('%H:%M:%S')})")

            if state in ['COMPLETED', 'FAILED', 'CANCELED']:
                break

            time.sleep(1.0)

        print("\nJob Finished.")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

    bc.close()


if __name__ == "__main__":
    main()

