#!/usr/bin/env python3
"""
XGFabric Workflow Client

Connects to the bridge, starts an XGFabric workflow on the specified edge,
and watches execution to completion via SSE notifications.

Usage:
    python examples/xgfabric.py [options]

Examples:
    python examples/xgfabric.py --bridge-url https://bridge:8000 --edge thinkie
    python examples/xgfabric.py -w myworkflow -r myresource
    RADICAL_BRIDGE_URL=https://bridge:8000 python examples/xgfabric.py
"""

import argparse
import logging
import os
import sys
import threading
import time

from radical.edge import BridgeClient

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(name)s %(message)s')


# ─────────────────────────────────────────────────────────────────────────────
#  Display
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_clusters(clusters, label):
    if not clusters:
        return f"  {label}: (none)\n"
    lines = f"  {label}:\n"
    for c in clusters:
        online = "online" if c.get('online') else "offline"
        gpu    = " GPU"   if c.get('has_gpu') else ""
        pilot  = f"  pilot={c['pilot_job_id']}" if c.get('pilot_job_id') else ""
        lines += f"    {c['name']} [{online}{gpu}]{pilot}\n"
    return lines


def _print_status(status):
    st     = status.get('status', '?')
    phase  = status.get('phase', '')
    prog   = status.get('progress', 0)
    msg    = status.get('message', '')
    active = status.get('active_cluster') or '-'
    err    = status.get('error', '')
    batch  = status.get('current_batch', 0)
    tbatch = status.get('total_batches', 0)
    sims   = status.get('completed_simulations', 0)
    tsims  = status.get('total_simulations', 0)
    imm    = status.get('immediate_clusters', [])
    alloc  = status.get('allocate_clusters', [])

    bar_len = 30
    filled  = int(bar_len * prog / 100)
    bar     = '█' * filled + '░' * (bar_len - filled)

    ts = time.strftime('%H:%M:%S')
    print(f"\n[{ts}] {st.upper()}  phase={phase}")
    print(f"  [{bar}] {prog}%  {msg}")
    print(f"  active={active}  batch={batch}/{tbatch}  sims={sims}/{tsims}")
    print(_fmt_clusters(imm,   'immediate'), end='')
    print(_fmt_clusters(alloc, 'allocate '), end='')
    if err:
        print(f"  ERROR: {err}")
    sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run an XGFabric workflow end-to-end and stream progress.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # URL/cert default to None so BridgeClient self-resolves via
    # radical.edge.utils (CLI > env > file).
    parser.add_argument('-u', '--bridge-url',  default=None,
                        help='Bridge URL  (CLI > $RADICAL_BRIDGE_URL > '
                             '~/.radical/edge/bridge.url).')
    parser.add_argument('-c', '--bridge-cert', default=None,
                        help='Bridge CA cert path  (CLI > $RADICAL_BRIDGE_CERT '
                             '> ~/.radical/edge/bridge_cert.pem).')
    parser.add_argument('-e', '--edge',     default='local',
                        help='Edge name where xgfabric plugin is running')
    parser.add_argument('-w', '--workflow', default='__default__',
                        help='Workflow config name or path (__default__ = built-in)')
    parser.add_argument('-r', '--resource', default='__default__',
                        help='Resource config name or path (__default__ = built-in)')
    args = parser.parse_args()

    print(args)

    bc  = BridgeClient(url=args.bridge_url, cert=args.bridge_cert)
    ec  = bc.get_edge_client(args.edge)
    xgf = ec.get_plugin('xgfabric')


    done      = threading.Event()
    last_data = {}
    seen_logs = set()   # timestamps of log entries already printed

    def on_topology(edges):
        ts = time.strftime('%H:%M:%S')
        print(f"[{ts}] topology: {list(edges.keys())}", flush=True)

    def on_status(edge, plugin, topic, data):
        last_data.update(data)

        # Print any new error log entries immediately
        for entry in data.get('log', []):
            key = (entry.get('time'), entry.get('message'))
            if entry.get('level') == 'error' and key not in seen_logs:
                seen_logs.add(key)
                print(f"  [{entry['time']}] TASK FAILED: {entry['message']}",
                      flush=True)

        _print_status(data)

        st = data.get('status')
        if st in ('completed', 'failed'):
            done.set()

    try:
        # Show initial cluster/config state before starting
        print(f"Connecting to bridge: {args.bridge_url}")
        print(f"Edge: {args.edge}  Workflow: {args.workflow}  Resource: {args.resource}")
        _print_status(xgf.get_status())

        # Register for topology and workflow notifications
        bc.register_topology_callback(on_topology)
        xgf.register_notification_callback(on_status, topic='workflow_status')

        # Start the workflow
        print(f"\nStarting workflow (workflow={args.workflow}, resource={args.resource})...")
        result = xgf.start_workflow(args.workflow, args.resource)
        print(f"Started: {result}")

        # Wait for completion driven by SSE notifications
        print("Waiting for completion (Ctrl+C to stop)...\n")
        while not done.wait(timeout=5):
            pass  # SSE callbacks drive the display; timeout is just a safety net

        final_status = last_data.get('status')
        final_error  = last_data.get('error', '')
        if final_status == 'failed':
            print(f"\nWorkflow FAILED: {final_error}", file=sys.stderr)
            sys.exit(1)
        else:
            print("\nWorkflow completed successfully.")

    except KeyboardInterrupt:
        print("\nInterrupted — stopping workflow...")
        try:
            xgf.stop_workflow()
            print("Workflow stopped.")
        except Exception as e:
            print(f"Stop failed: {e}", file=sys.stderr)
        sys.exit(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        try:
            xgf.stop_workflow()
        except Exception:
            pass
        sys.exit(1)

    finally:
        xgf.close()
        bc.close()


if __name__ == '__main__':
    main()
