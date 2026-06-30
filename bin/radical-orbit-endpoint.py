#!/usr/bin/env python3
"""Thin entry point for the ORBIT Service.

All logic lives in :class:`radical.orbit.service.EndpointService` (also
exported as ``radical.orbit.Endpoint``).  This script handles argparse,
log configuration, signal handlers, and forwards to the class.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys

from radical.orbit.service import EndpointService
import radical.orbit.logging_config as _lc


log = logging.getLogger("radical.orbit.endpoint")


async def main():
    parser = argparse.ArgumentParser(description="ORBIT Service")
    parser.add_argument("--name",      "-n", nargs="?", help="Endpoint name")
    parser.add_argument("--url",       "-u", nargs="?",
                        help="Bridge URL.  CLI > $RADICAL_ORBIT_BRIDGE_URL > "
                             "~/.radical/orbit/bridge.url.")
    parser.add_argument("--cert",      "-c", nargs="?",
                        help="Bridge TLS cert path.  CLI > "
                             "$RADICAL_ORBIT_BRIDGE_CERT > "
                             "~/.radical/orbit/bridge_cert.pem.")
    parser.add_argument("--token",     "-t",
                        help="Shared bridge auth token.  CLI > "
                             "$RADICAL_ORBIT_BRIDGE_TOKEN > "
                             "~/.radical/orbit/bridge.token.")
    parser.add_argument("--plugins",   "-p", default="default",
                        help="Comma-separated plugins to load (default: "
                             "the role-specific default set).  Special "
                             "tokens: 'default' (role's default set), "
                             "'all' (every registered plugin).  Wildcards "
                             "allowed: 'iri*'.  Prefix matching supported: "
                             "'sys'→sysinfo.  Combine, e.g.: '-p default,rose'.")
    parser.add_argument("--log-level", "-l",
                        default=(os.environ.get("RADICAL_ORBIT_LOG_LVL")
                                 or os.environ.get("RADICAL_LOG_LVL") or "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level (default: INFO; env: "
                             "RADICAL_ORBIT_LOG_LVL / RADICAL_LOG_LVL)")
    parser.add_argument("--tunnel", default='none',
                        choices=['none', 'forward', 'reverse'],
                        help="SSH tunnel mode for the bridge connection. "
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
    endpoint_name = args.name or 'endpoint'
    log_file = (os.environ.get('RADICAL_ORBIT_LOG_FILE')
                or os.path.expanduser(
                    f'~/.radical/orbit/logs/{endpoint_name}.log'))
    _lc.configure_logging(level, log_file=log_file)
    log.info("Log level: %s; log file: %s",
             args.log_level.upper(), log_file)

    plugins = [t.strip() for t in args.plugins.split(',') if t.strip()]

    # EndpointService resolves URL + cert via radical.orbit.utils (CLI > env > file).
    service = EndpointService(bridge_url=args.url,
                          cert       =args.cert,
                          name       =args.name,
                          plugins    =plugins,
                          tunnel     =args.tunnel,
                          tunnel_via =args.tunnel_via,
                          token      =args.token)

    loop = asyncio.get_running_loop()

    def signal_handler():
        log.info("Received shutdown signal")
        service.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    log.info("Starting ORBIT Service (%s)", service.bridge_url)

    try:
        await service.run()
    except asyncio.CancelledError:
        log.info("Service cancelled")
    except Exception:
        log.exception("Service crashed")
        sys.exit(1)
    finally:
        log.info("Service stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
