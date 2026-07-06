#!/usr/bin/env python3
"""Thin entry point for the ORBIT endpoint (participant) runtime.

All logic lives in :class:`radical.orbit.runtime.EndpointRuntime` (also exported
as ``radical.orbit.Endpoint``) — the single node abstraction that dials the
broker over one outbound WebSocket and serves (and/or consumes) plugins.  This
script keeps its broker-era filename and CLI surface; it handles argparse, log
configuration, and signal-driven shutdown, then hands off to the runtime.
"""

import argparse
import logging
import os
import signal
import socket
import sys
import threading

from radical.orbit.runtime import EndpointRuntime
import radical.orbit.logging_config as _lc


log = logging.getLogger("radical.orbit.endpoint")


def main():
    parser = argparse.ArgumentParser(description="ORBIT Service")
    parser.add_argument("--name",      "-n", nargs="?", help="Endpoint name")
    parser.add_argument("--url",       "-u", nargs="?",
                        help="Broker URL.  CLI > $RADICAL_ORBIT_BROKER_URL > "
                             "~/.radical/orbit/broker.url.")
    parser.add_argument("--cert",      "-c", nargs="?",
                        help="Broker TLS cert path.  CLI > "
                             "$RADICAL_ORBIT_BROKER_CERT > "
                             "~/.radical/orbit/broker_cert.pem.")
    parser.add_argument("--token",     "-t",
                        help="Shared broker auth token.  CLI > "
                             "$RADICAL_ORBIT_BROKER_TOKEN > "
                             "~/.radical/orbit/broker.token.")
    parser.add_argument("--plugins",   "-p", default="default",
                        help="Comma-separated plugins to load (default: "
                             "the role-specific default set).  Special "
                             "tokens: 'default' (role's default set), "
                             "'all' (every registered plugin).  Wildcards "
                             "allowed: 'iri*'.  Prefix matching supported: "
                             "'sys'->sysinfo.  Combine, e.g.: '-p default,rose'.")
    parser.add_argument("--log-level", "-l",
                        default=(os.environ.get("RADICAL_ORBIT_LOG_LVL")
                                 or os.environ.get("RADICAL_LOG_LVL") or "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level (default: INFO; env: "
                             "RADICAL_ORBIT_LOG_LVL / RADICAL_LOG_LVL)")
    parser.add_argument("--tunnel", default='none',
                        choices=['none', 'forward', 'reverse'],
                        help="SSH tunnel mode for the broker connection. "
                             "'none' connects directly; 'forward' opens "
                             "ssh -L from this (compute) node to the "
                             "login host (compute->login); 'reverse' "
                             "waits for the parent-side ssh -R and reads "
                             "~/.radical/orbit/tunnels/<name>.port from "
                             "the shared filesystem.")
    parser.add_argument("--tunnel-via", metavar="HOST", default=None,
                        help="Login host for --tunnel forward.  Falls "
                             "back to $PBS_O_HOST / $SLURM_SUBMIT_HOST. "
                             "Ignored for --tunnel none / reverse.")

    args = parser.parse_args()

    level = getattr(logging, args.log_level.upper(), logging.INFO)
    # A serving endpoint wants a stable, recoverable name (auto-``consumer.<uuid>``
    # naming is for fire-and-forget consumers only), so default to the hostname.
    endpoint_name = args.name or socket.gethostname()
    log_file = (os.environ.get('RADICAL_ORBIT_LOG_FILE')
                or os.path.expanduser(
                    f'~/.radical/orbit/logs/{endpoint_name}.log'))
    _lc.configure_logging(level, log_file=log_file)
    log.info("Log level: %s; log file: %s",
             args.log_level.upper(), log_file)

    plugins = [t.strip() for t in args.plugins.split(',') if t.strip()]

    # EndpointRuntime resolves URL + cert via radical.orbit.utils (CLI > env > file).
    runtime = EndpointRuntime(broker_url=args.url,
                              cert       =args.cert,
                              name       =endpoint_name,
                              plugins    =plugins,
                              tunnel     =args.tunnel,
                              tunnel_via =args.tunnel_via,
                              token      =args.token)

    stop = threading.Event()

    def signal_handler(signum, frame):
        log.info("Received shutdown signal")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal_handler)

    log.info("Starting ORBIT endpoint (%s)", runtime.broker_url)

    try:
        runtime.start(wait=True)
        stop.wait()
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("Endpoint crashed")
        sys.exit(1)
    finally:
        log.info("Endpoint stopping")
        runtime.stop()
        log.info("Endpoint stopped")


if __name__ == "__main__":
    main()
