#!/usr/bin/env python3

from radical.orbit import BridgeClient


def main():

    bc   = BridgeClient()
    eids = bc.list_endpoints()
    print(f"Found {len(eids)} Endpoint(s): {eids}")

    for eid in eids:
        ec = bc.get_endpoint_client(eid)
        plugins = ec.list_plugins()
        if not plugins.get('queue_info', {}).get('enabled', False):
            print(f"\n[{eid}] No batch scheduler available — skipping")
            continue
        qi = ec.get_plugin('queue_info')

        info   = qi.get_info()
        queues = info.get('queues', {})

        render_queues(eid, queues)

        # Show jobs for the first available queue
        first_queue = next(iter(queues))
        jobs = qi.list_jobs(first_queue)
        render_jobs(eid, first_queue, jobs.get('jobs', []))

        allocs = qi.list_allocations()
        render_allocations(eid, allocs.get('allocations', []))

    bc.close()


def _fmt_time(minutes):
    if minutes is None:
        return 'unlimited'
    try:
        h, m = divmod(int(minutes), 60)
        return f"{h}:{m:02d}"
    except (ValueError, TypeError):
        return str(minutes).lower()


def _fmt_elapsed(seconds):
    if not seconds:
        return '-'
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}"


def render_queues(eid, queues):
    print(f"\n{'=' * 80}")
    print(f" Endpoint: {eid}  —  {len(queues)} partition(s)")
    print(f"{'=' * 80}")
    print(f"  {'Partition':<18} {'State':<8} {'Nodes(tot/avail/idle)':>22} "
          f"{'CPUs':>6} {'Mem(GB)':>8} {'GPUs':>5} {'TimeLimit':>10}")
    print("  " + "-" * 76)
    for q in queues.values():
        mem_gb = f"{q['mem_per_node_mb'] // 1024}" if q['mem_per_node_mb'] else '-'
        gpus   = str(q['gpus_per_node']) if q['gpus_per_node'] else '-'
        nodes  = f"{q['nodes_total']}/{q['nodes_available']}/{q['nodes_idle']}"
        print(f"  {q['name']:<18} {q['state']:<8} {nodes:>22} "
              f"{q['cpus_per_node']:>6} {mem_gb:>8} {gpus:>5} "
              f"{_fmt_time(q['time_limit']):>10}")


def render_jobs(eid, queue, jobs):
    print(f"\n  [ Jobs in '{queue}' — {len(jobs)} ]")
    if not jobs:
        print("    (none)")
        return
    print(f"  {'JobID':<10} {'Name':<20} {'User':<12} {'State':<10} "
          f"{'Nodes':>5} {'CPUs':>5} {'Elapsed':>10} {'Account':<12}")
    print("  " + "-" * 76)
    for j in jobs[:20]:                   # cap at 20 rows
        print(f"  {j['job_id']:<10} {j['job_name'][:20]:<20} {j['user'][:12]:<12} "
              f"{j['state']:<10} {j['nodes']:>5} {j['cpus']:>5} "
              f"{_fmt_elapsed(j['time_used']):>10} {j['account'][:12]:<12}")
    if len(jobs) > 20:
        print(f"  ... and {len(jobs) - 20} more")


def render_allocations(eid, allocs):
    print(f"\n  [ Allocations — {len(allocs)} ]")
    if not allocs:
        print("    (none)")
        return
    print(f"  {'Account':<20} {'User':<12} {'MaxJobs':>8} {'MaxSubmit':>10} "
          f"{'Fairshare':>10} {'QOS'}")
    print("  " + "-" * 70)
    for a in allocs:
        print(f"  {str(a.get('account',''))[:20]:<20} "
              f"{str(a.get('user',''))[:12]:<12} "
              f"{str(a.get('max_jobs')  or '-'):>8} "
              f"{str(a.get('max_submit') or '-'):>10} "
              f"{str(a.get('fairshare') or '-'):>10}  "
              f"{a.get('qos','')}")


if __name__ == "__main__":
    main()
