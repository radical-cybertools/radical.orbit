#!/usr/bin/env python3
'''
IRI Plugin Example
==================

Submit a one-shot test job to an HPC resource via the bridge's IRI plugin.

Usage::

    python examples/example_iri.py [TOKEN_FILE [ENDPOINT [RESOURCE [QUEUE]]]]

Defaults are filled from ``ENDPOINT_CONFIG`` below.  Environment variables
``IRI_ACCOUNT`` and ``IRI_WORKDIR`` override the per-endpoint account and
workdir values.

The token file holds only the Bearer token string (no ``Bearer`` prefix).
The token is read locally, sent to the bridge once at ``connect`` time,
and held in bridge process memory only — never written to bridge disk.

Per-endpoint defaults: edit ENDPOINT_CONFIG below to match your account
and (where required) workdir before first use.
'''

import os
import sys
import time

from radical.edge.client import BridgeClient


# ─────────────────────────────────────────────────────────────────────────────
#  Per-endpoint defaults.  CLI positionals (resource, queue) and env vars
#  (IRI_ACCOUNT, IRI_WORKDIR) override these per call.
# ─────────────────────────────────────────────────────────────────────────────

ENDPOINT_CONFIG = {
    'nersc': {
        'resource_id': 'perlmutter',
        'queue_name' : 'debug',
        'account'    : None,                  # set me
        'workdir'    : None,                  # optional on NERSC
        'constraint' : 'cpu',                 # required on Perlmutter
    },
    'olcf': {
        'resource_id': 'odo',
        'queue_name' : 'batch',
        'account'    : 'fus183',
        'workdir'    : '/gpfs/wolf2/olcf/fus183/proj-shared',
        'constraint' : None,
    },
}

TERMINAL_STATES = {'completed', 'failed', 'canceled'}


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_args(argv):
    '''Resolve the full per-endpoint config from CLI args + env + defaults.'''
    token_file = argv[1] if len(argv) > 1 else '~/.amsc/token_olcf'
    endpoint   = argv[2] if len(argv) > 2 else 'olcf'
    cfg        = dict(ENDPOINT_CONFIG.get(endpoint, {}))
    return {
        'token_path' : os.path.expanduser(token_file),
        'endpoint'   : endpoint,
        'resource_id': argv[3] if len(argv) > 3 else cfg.get('resource_id'),
        'queue_name' : argv[4] if len(argv) > 4 else cfg.get('queue_name'),
        'account'    : os.environ.get('IRI_ACCOUNT') or cfg.get('account'),
        'workdir'    : os.environ.get('IRI_WORKDIR') or cfg.get('workdir'),
        'constraint' : cfg.get('constraint'),
    }


def read_token(path):
    '''Read a single-line bearer token; bail out clearly on common mistakes.'''
    if not os.path.exists(path): sys.exit(f'token file not found: {path}')
    with open(path) as f: token = f.read().strip()
    if not token:                sys.exit(f'token file is empty: {path}')
    return token


def build_job_spec(cfg):
    '''Compose the IRI job spec from the resolved config dict.'''
    attrs = {'queue_name': cfg['queue_name'], 'duration': 300}
    if cfg['account']:    attrs['account']    = cfg['account']
    if cfg['constraint']: attrs['constraint'] = cfg['constraint']
    spec = {
        'executable' : '/bin/bash',
        'arguments'  : ['-lc', 'echo "IRI test: $(hostname) $(date)"'],
        'name'       : 'edge-iri-test',
        'resources'  : {'node_count': 1, 'process_count': 1},
        'attributes' : attrs,
    }
    if cfg['workdir']: spec['directory'] = cfg['workdir']
    return spec


def state_of(status):
    '''Pull the job state out of an IRI status response.

    Different IRI backends nest the state differently — some return
    ``{"state": "..."}`` at the top level, others wrap it in a ``status``
    sub-dict.  Try both.
    '''
    if isinstance(status.get('state'), str):
        return status['state']
    return (status.get('status') or {}).get('state', 'unknown')


def submit_and_wait(iri, cfg, poll=5.0):
    '''Submit the job, then poll until terminal state.

    Output is deduplicated: we only print when the state changes, so a
    job sitting queued for a minute is one line, not twelve.
    '''
    job    = iri.submit_job(cfg['resource_id'], build_job_spec(cfg))
    job_id = job['job_id']
    print(f'  Job submitted: {job_id}')

    print('\nPolling for completion…')
    last = None
    while True:
        state = state_of(iri.get_job_status(cfg['resource_id'], job_id))
        if state != last:
            print(f'  State: {state}')
            last = state
        if state.lower() in TERMINAL_STATES:
            return state
        time.sleep(poll)


def try_list_projects(iri, endpoint):
    '''Print the project list, or a one-line note when the endpoint won't.

    OLCF S3M tokens are compute-scoped (401 on /api/v1/account/projects);
    on some IRI deployments the account route is not deployed at all (404).
    Either way: print a note, don't raise.
    '''
    print('\nFetching projects…')
    try:
        plist = iri.list_projects().get('projects', [])
    except RuntimeError as exc:
        msg = str(exc)
        if   'HTTP 401' in msg: hint = 'token is compute-scoped'
        elif 'HTTP 404' in msg: hint = 'route not available'
        else:                   hint = msg
        print(f'  (project listing unavailable for {endpoint!r} — {hint})')
        return
    print(f'  {len(plist)} project(s) found')
    for p in plist[:3]:
        print(f'    {p.get("name", p.get("id", "-"))}')


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    cfg   = parse_args(sys.argv)
    token = read_token(cfg['token_path'])
    if not cfg['resource_id']:
        sys.exit(f'no resource_id for endpoint {cfg["endpoint"]!r} '
                 f'and none on the CLI — set ENDPOINT_CONFIG or pass argv[3]')

    print(f'Connecting to bridge (endpoint: {cfg["endpoint"]})…')
    bc     = BridgeClient()
    bridge = bc.get_edge_client('bridge')
    cx     = bridge.get_plugin('iri_connect')

    print('Available endpoints:')
    for key, ep in cx.list_endpoints().items():
        mark = ' (connected)' if ep.get('connected') else ''
        print(f'  {key}: {ep["label"]}  [{ep["auth"]}]{mark}')

    print(f'\nConnecting to {cfg["endpoint"]}…')
    iri = cx.connect(endpoint=cfg['endpoint'], token=token)
    print(f'  instance session: {iri.sid}')

    iri.register_notification_callback(
        lambda edge, plugin, topic, data:
            print(f'  [notification] job {data["job_id"]}: {data["state"]}'),
        topic='job_status')

    try:
        print('\nFetching resources…')
        rlist = iri.list_resources().get('resources', [])
        print(f'  {len(rlist)} found: '
              + ', '.join(r.get('name', '-') for r in rlist))

        print(f'\nUsing resource: {cfg["resource_id"]}  '
              f'(queue: {cfg["queue_name"]}'
              + (f', account: {cfg["account"]}' if cfg['account'] else '')
              + ')')

        print('\nSubmitting test job…')
        state = submit_and_wait(iri, cfg)
        print(f'\nJob finished with state: {state}')

        try_list_projects(iri, cfg['endpoint'])
    finally:
        cx.disconnect(cfg['endpoint'])
        bc.close()
        print('\nDone.')


if __name__ == '__main__':
    main()
