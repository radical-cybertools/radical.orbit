#!/usr/bin/env python3
"""Thin entry point for the ORBIT Broker.

All logic lives in :class:`radical.orbit.broker.Broker` — the active hub of the
participant star (routing loop + own-thread plugin host).  The HTTP/SSE/Explorer
compat tier is the broker's ``gateway`` module, on by default (``--no-gateway``
for a headless broker).
"""

import argparse
import logging
import os
import sys

import radical.orbit.logging_config as _lc
from radical.orbit.broker import Broker


_TLS_HOWTO = '''\
TLS setup
---------
The broker serves HTTPS/WSS and needs a certificate + private key.
Resolution order for each:

  cert:  --cert PATH  >  $RADICAL_ORBIT_BROKER_CERT  >  ~/.radical/orbit/broker_cert.pem
  key:   --key  PATH  >  $RADICAL_ORBIT_BROKER_KEY   >  ~/.radical/orbit/broker_key.pem

To create a self-signed pair at the default location:

  mkdir -p ~/.radical/orbit
  openssl req -x509 -newkey rsa:4096 -nodes \\
      -keyout ~/.radical/orbit/broker_key.pem \\
      -out    ~/.radical/orbit/broker_cert.pem \\
      -days 365 -subj "/CN=$(hostname -f)"
  chmod 600 ~/.radical/orbit/broker_key.pem

The key must be mode 0600 or stricter — the broker refuses to start
otherwise.  Endpoints and clients pin the *cert* (never the key): copy
broker_cert.pem to ~/.radical/orbit/ on each connecting host, or point
$RADICAL_ORBIT_BROKER_CERT at it there.  Hostname matching is disabled
for pinned certs, so the CN does not need to match the broker host.
'''


def main():
    parser = argparse.ArgumentParser(
        description='ORBIT Broker',
        epilog=_TLS_HOWTO,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cert', default=None,
                        help='TLS cert path.  CLI > $RADICAL_ORBIT_BROKER_CERT > '
                             '~/.radical/orbit/broker_cert.pem.')
    parser.add_argument('--key', default=None,
                        help='TLS key path.  CLI > $RADICAL_ORBIT_BROKER_KEY > '
                             '~/.radical/orbit/broker_key.pem.  Refuses to '
                             'start if the file is more permissive than '
                             '0o600.')
    parser.add_argument('--host', default='0.0.0.0',
                        help='Bind address (default: 0.0.0.0).')
    parser.add_argument('--port', type=int, default=8000,
                        help='Bind port (default: 8000).')
    parser.add_argument('--plugins', '-p', default='default',
                        help='Comma-separated plugins to host on the '
                             'broker (default: the broker role default '
                             'set — see plugin_host_base.'
                             'DEFAULT_PLUGINS_BY_ROLE).  Special tokens: '
                             '"default" (role default), "all" (every '
                             'registered plugin), "" (none).  Wildcards '
                             'allowed: "iri*". Prefix matching '
                             'supported. Combine, e.g.: "-p default,rose".')
    parser.add_argument('--token', default=None,
                        help='Shared ingress auth token.  CLI > '
                             '$RADICAL_ORBIT_BROKER_TOKEN > '
                             '~/.radical/orbit/broker.token.  If none is set, '
                             'one is generated and written to that file '
                             '(mode 0600) at startup.')
    parser.add_argument('--no-auth', action='store_true',
                        help='Disable ingress authentication (local dev only). '
                             'Also via $RADICAL_ORBIT_BROKER_NO_AUTH=1.')
    parser.add_argument('--no-gateway', action='store_true',
                        help='Run a headless broker: only the token-gated '
                             'WebSocket /register ingress, no HTTP/SSE/Explorer '
                             'compat tier.  The gateway is on by default.')
    args = parser.parse_args()

    log_level_name = (os.environ.get('RADICAL_ORBIT_LOG_LVL')
                      or os.environ.get('RADICAL_LOG_LVL') or 'INFO').upper()
    level = getattr(logging, log_level_name, logging.INFO)
    log_file = (os.environ.get('RADICAL_ORBIT_LOG_FILE')
                or os.path.expanduser('~/.radical/orbit/logs/broker.log'))
    _lc.configure_logging(level, log_file=log_file)
    logging.getLogger('radical.orbit').info(
        "Log level: %s; log file: %s", log_level_name, log_file)

    try:
        broker = Broker(cert=args.cert,
                        key=args.key,
                        host=args.host,
                        port=args.port,
                        plugins=args.plugins,
                        token=args.token,
                        no_auth=args.no_auth,
                        gateway=not args.no_gateway)
    except (ValueError, FileNotFoundError, PermissionError) as e:
        # Cert/key resolution failures (missing, unreadable, key too
        # permissive) — print the actionable how-to instead of a traceback.
        print(f'\nERROR: {e}\n\n{_TLS_HOWTO}', file=sys.stderr)
        sys.exit(1)

    broker.run()


if __name__ == "__main__":
    main()
