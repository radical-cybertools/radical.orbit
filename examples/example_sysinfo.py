#!/usr/bin/env python3

from radical.orbit import BridgeClient


def bytes2human(n):
    if n is None: return "N/A"
    n = int(n)
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n


def main():

    bc = BridgeClient()
    eids = bc.list_endpoints()
    print(f"Found {len(eids)} Endpoint(s): {eids}")

    for eid in eids:
        ec = bc.get_endpoint_client(eid)
        si = ec.get_plugin('sysinfo')

        metrics = si.get_metrics()
        render_metrics(eid, metrics)

    bc.close()


def render_metrics(eid: str, m: dict):
    # Header
    system = m.get('system', {})
    hostname = system.get('hostname', '?')
    kernel = system.get('kernel', '?')

    print("\n" + "=" * 62)
    print(f" Endpoint: {eid} | Host: {hostname} | OS: {kernel}")

    # CPU Table
    cpu = m.get('cpu', {})
    print("\n[ CPU ]")
    print(f"  Model:      {cpu.get('model')}")
    print(f"  Cores:      {cpu.get('cores_physical')} physical / {cpu.get('cores_logical')} logical")

    load_avg = cpu.get('load_avg', [0, 0, 0])
    cores = cpu.get('cores_logical', 1) or 1
    if load_avg:
        load_pct = [round((l / cores) * 100, 1) for l in load_avg]
        print(f"  Load:       {load_pct[0]}% / {load_pct[1]}% / {load_pct[2]}%")
    else:
        print("  Load:       N/A")
    print(f"  Usage:      {cpu.get('percent')}%")

    # GPU Table
    if m.get('gpus'):
        print("\n[ GPUs ]")
        print(f"  {'ID':<6} {'Name':<25} {'GPU %':>6} {'Mem %':>6} {'Total':>10}")
        print("  " + "-" * 60)
        for g in m['gpus']:
            mem_total = bytes2human(g.get('mem_total', 0) * 1024 * 1024)
            print(f"  {str(g.get('id')):<6} {g.get('name')[:25]:<25} {str(g.get('util_gpu')):>5}% {str(g.get('util_mem')):>5}% {mem_total:>10}")

    # Disks Table
    print("\n[ Disks ]")
    print(f"  {'Mount':<15} {'Device':<15} {'Type':<6} {'Total':>8} {'Used':>8} {'Use%':>5}")
    print("  " + "-" * 60)
    for d in m.get('disks', []):
        print(f"  {d.get('mount')[:15]:<15} {d.get('device')[:15]:<15} {d.get('type'):<6} "
              f"{bytes2human(d.get('total')):>8} {bytes2human(d.get('used')):>8} {d.get('percent'):>4}%")

    # Memory Table
    mem = m.get('memory', {})
    print("\n[ Memory ]")
    print(f"  Total:      {bytes2human(mem.get('total')):>10}")
    print(f"  Used:       {bytes2human(mem.get('used')):>10} ({mem.get('percent')}%)")
    print(f"  Available:  {bytes2human(mem.get('available')):>10}")
    print("\n" + "=" * 62)


if __name__ == "__main__":
    main()

