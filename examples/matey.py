#!/usr/bin/env python3
"""
MATEY active learning across heterogeneous edge endpoints — interactive demo.

This is the MATEY-driven counterpart of ``amsc.py``.  Same launch /
discovery / teardown machinery; the difference is in step 4: the ROSE
workflow runs the MATEY transformer-based fusion-seed inference as an
*executable task* via Rhapsody / Dragon, rather than the toy GP-fitting
workload that ``amsc.py`` ships with.

Architecture
============

  Client (this script, your laptop)
        │
        │  HTTPS / WebSocket
        ▼
   ╔══════════╗      ┌── pre-existing edge   (HPC compute node, ready to run)
   ║  Bridge  ║─────►├── pre-existing edge   (HPC login node, runs PsiJ)
   ╚══════════╝      ├── new edge ★          (spawned via IRI)
                     └── new edge ★          (spawned via PsiJ ↦ submit_tunneled)

What this script does
---------------------
1. Asks you which targets to use.  Targets come from two pools:
     - edges already connected to the bridge (login-node or compute-node);
     - IRI endpoints (NERSC, OLCF) you can reach with a bearer token.

2. For each target, asks you the per-target details (queue, account,
   walltime, …).  Defaults are best-guesses — correct them at the prompt.

3. Submits the launch jobs (or reuses ready edges).  Then waits for the
   *first* edge to come up.

4. On that first edge: stages one ``env_runner_<name>.sh`` per entry of
   the target's ``env_setup`` dict (each runner just activates an env
   then ``exec "$@"`` — the actual command comes from the caller), then
   runs a ROSE active-learning workflow whose simulation step is the
   MATEY ``basic_inference.py`` driver invoked through the runner.

5. Tears down anything this script created.  Pre-existing edges and any
   stragglers from our submission set are left alone — the demo keeps
   the cleanup logic simple by design.

Prerequisites on every target machine
-------------------------------------
- A radical.edge install with Rhapsody and Dragon at: ``~/.amsc/ve``
- The bridge's TLS certificate at:                    ``~/.amsc/radical.edge.cert``
- A login host reachable from the compute node (used for ``--tunnel``)

Tokens
------
IRI bearer tokens are read locally and live at::

    ~/.amsc/token_nersc
    ~/.amsc/token_olcf

The script reads them from disk and sends them to the bridge once at
``iri_connect.connect()`` time.  The bridge holds them in process memory
only — they are never written to disk on the bridge side.

Run::

    python examples/matey.py
"""

import asyncio
import logging
import os
import re
import sys
import time
import uuid

from pathlib import Path

# RADICAL Edge client + ROSE / Rhapsody bits
from radical.edge.client import BridgeClient

import rhapsody
from radical.asyncflow      import WorkflowEngine
from rose.al.active_learner import SequentialActiveLearner
from rose.metrics           import MEAN_SQUARED_ERROR_MSE

# Note: sklearn / mpi4py / dragon are NOT imported at module level.
# They are imported inside the task bodies below, so they only have to
# be installed on the HPC side (where the tasks actually execute), not
# on the client where this script is launched from.

# Quiet logging: this is a demo, the print() lines tell the story.
rhapsody.enable_logging(level=logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
#  Workflow knobs — edit to taste.
# ─────────────────────────────────────────────────────────────────────────────

# ROSE active-learning shape.  GPU placement / ensemble width are
# deliberately not knobs yet — the first cut of this script just runs
# one MATEY inference per AL iteration.
MSE_THRESHOLD = 0.01   # convergence target — compared against MATEY's nrmse
MAX_ITER      = 15     # hard cap on AL iterations

# How long we are willing to wait for the first edge to come up.
EDGE_WAIT_SECONDS  = 30 * 60


# ─────────────────────────────────────────────────────────────────────────────
#  Per-IRI-endpoint defaults.
#  Best-guesses — correct any field below to match your account / project.
#  Anything you set to ``None`` will be asked for at runtime.
# ─────────────────────────────────────────────────────────────────────────────

IRI_DEFAULTS = {
    'nersc': {
        'enabled'     : True,
        'iri_url'     : 'https://api.iri.nersc.gov',
        'resource_id' : 'perlmutter',
        'login_host'  : 'perlmutter.nersc.gov',
        'home_dir'    : '/global/u2/m/merzky',  # target's $HOME
        'amsc_dir'    : None,                   # default to <home>/.amsc
        # ``tunnel``: 'none' / 'forward' / 'reverse'.  Forward = child
        # ssh -L compute->login (Aurora, Perlmutter).  Reverse = parent
        # ssh -R login->compute (Odo).
        'tunnel'      : 'forward',
        'account'     : 'm5290',
        'workdir'     : None,
        'queue_name'  : 'debug',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : 'cpu',
        'reservation' : None,
        'environment' : {},
        'setup'       : [                       # setup for the edge service
            'module load openmpi',
        ],
        # ─── Application configuration ───────────────────────────────
        # ``env_setup``: named env-activation profiles for *task*
        # launchers (NOT the edge service).  Each non-empty entry
        # yields a ``~/.amsc/env_runner_<name>.sh`` whose body runs
        # the commands and then ``exec "$@"``.  ``None`` is treated
        # as no profiles.
        #
        # ``app``: per-target paths the workflow body needs (MATEY).
        # ``None`` is treated as no application paths configured.
        'env_setup'   : {
            'matey': [
                'module load conda',
                'conda activate /global/common/software/amsc007/matey_env',
            ],
        },
        'app'         : {
            'matey_dir'      : '/global/u2/m/merzky/MATEY/examples',
            'matey_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_Demo_March2026_Final'
                               '/demo_nbatchsloc100',
            'matey_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500',
        },
        # ─────────────────────────────────────────────────────────────
    },
    'olcf': {
        'enabled'     : True,
        'iri_url'     : 'https://amsc-open.s3m.olcf.ornl.gov',
        'resource_id' : 'odo',
        'login_host'  : 'login1.frontier.olcf.ornl.gov',
        'home_dir'    : '/autofs/nccsopen-svm1_home/merzky',
        'amsc_dir'    : None,
        'tunnel'      : 'reverse',
        'account'     : 'fus183',
        'workdir'     : '/gpfs/wolf2/olcf/fus183/proj-shared',
        'queue_name'  : 'batch',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : None,
        'reservation' : None,
        'environment' : {},
        'setup'       : None,
        'env_setup'   : None,
        'app'         : None,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  Per-machine defaults for PsiJ submission via existing edges, plus the
#  ``enabled`` flag for compute / standalone edges.
#
#  Keys are edge names (as reported by ``bc.list_edges()``).  Anything not
#  in this dict is treated as ``enabled=True`` with no per-host pre-fills
#  — the prompts use their hard-coded fallback values.
# ─────────────────────────────────────────────────────────────────────────────

MACHINE_DEFAULTS = {
    'aurora': {
        'enabled'     : True,
        'account'     : 'Fusion-FM',
        'queue_name'  : 'debug',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : None,
        'tunnel'      : 'forward',
        'amsc_dir'    : None,
        'setup'       : None,
        'env_setup'   : None,
        'app'         : None,
    },
    'perlmutter': {
        'enabled'     : True,
        'account'     : 'm5290',
        'queue_name'  : 'debug',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : 'cpu',
        'tunnel'      : 'forward',
        'amsc_dir'    : None,
        'setup'       : [
            'module load openmpi',
        ],
        'env_setup'   : {
            'matey': [
                'module load conda',
                'conda activate /global/common/software/amsc007/matey_env',
            ],
        },
        'app'         : {
            'matey_dir'      : '/global/u2/m/merzky/MATEY/examples',
            'matey_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_Demo_March2026_Final'
                               '/demo_nbatchsloc100',
            'matey_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500',
        },
    },
    'odo': {
        'enabled'     : True,
        'account'     : 'fus183',
        'queue_name'  : 'batch',
        'walltime_min': 30,
        'n_nodes'     : 1,
        'constraint'  : None,
        'tunnel'      : 'reverse',
        'amsc_dir'    : None,
        'setup'       : None,
        'env_setup'   : None,
        'app'         : None,
    },
    'thinkie': {
        'enabled'     : False,
        'amsc_dir'    : None,
        'setup'       : None,
        'env_setup'   : None,
        'app'         : None,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  File-system layout.
#
#  ``AMSC_DIR`` (client-side) is the directory where we read IRI bearer
#  tokens (``token_<endpoint>``).  Defaults to ``~/.amsc``; override by
#  setting ``$AMSC_DIR`` before launching.
#
#  Per-target ``amsc_dir`` (target-side) is a path component (relative
#  to the target's ``$HOME``) under which the install script laid down
#  ``ve/bin/radical-edge-wrapper.sh``.  Defaults to ``.amsc``; override
#  per target via the ``amsc_dir`` field in IRI_DEFAULTS / MACHINE_DEFAULTS.
#
#  Bridge cert is no longer plumbed by this script — child edges
#  resolve it via radical.edge's CLI > env > file precedence (default
#  file path: ``~/.radical/edge/bridge_cert.pem`` on each target).
#
#  Why not pass ``~/.amsc/...`` and let bash expand it?  PsiJ's
#  ``single_launch.sh`` quotes the executable arg, so the literal ``~``
#  reaches ``bash`` as a path component and never expands.  Tested,
#  doesn't work — keep paths absolute.
# ─────────────────────────────────────────────────────────────────────────────

AMSC_DIR = Path(os.environ.get('AMSC_DIR') or Path.home() / '.amsc').expanduser()


# ─────────────────────────────────────────────────────────────────────────────
#  Tiny prompt helpers.
#
#  All user interaction goes through these four functions.  They use plain
#  ``input()`` for now; swap them out for ``rich`` / ``questionary`` /
#  ``prompt_toolkit`` later without touching the rest of the script.
# ─────────────────────────────────────────────────────────────────────────────

def ask(prompt, default=None):
    """Ask for a string, returning ``default`` when the user just hits Enter."""
    suffix = f' [{default}]' if default is not None else ''
    answer = input(f'{prompt}{suffix}: ').strip()
    return answer or (default if default is not None else '')


def ask_int(prompt, default):
    """Ask for an integer, falling back to ``default`` on empty input."""
    while True:
        raw = ask(prompt, str(default))
        try:               return int(raw)
        except ValueError: print(f'  not an integer: {raw!r} — try again')


def confirm(prompt, default=True):
    """Yes/no confirmation; default applies on empty input."""
    suffix = ' [Y/n]' if default else ' [y/N]'
    while True:
        answer = input(f'{prompt}{suffix}: ').strip().lower()
        if not answer:           return default
        if answer in ('y', 'yes'): return True
        if answer in ('n', 'no'):  return False
        print('  please answer y or n')


_TUNNEL_MODES = ('none', 'forward', 'reverse')


def _ask_tunnel(default):
    """Prompt for an SSH tunnel mode, validating against the allowed values."""
    if default not in _TUNNEL_MODES:
        default = 'forward'
    while True:
        raw = ask('  ssh tunnel direction (none/forward/reverse)', default)
        if raw in _TUNNEL_MODES:
            return raw
        print(f'  invalid: {raw!r} — pick one of {_TUNNEL_MODES}')


def select_many(items, prompt):
    """Numbered multi-select.  Returns the selected items in input order.

    ``items`` is a list of (label, value) tuples.  Empty input selects
    nothing; ``all`` selects everything.
    """
    if not items:
        return []
    print(f'\n{prompt}')
    for i, (label, _) in enumerate(items, start=1):
        print(f'  {i:2d}) {label}')
    raw = ask('  enter numbers (e.g. "1 3 5"), "all", or empty for none', '')
    if raw.lower() == 'all':
        return [v for _, v in items]
    picks = []
    for tok in raw.split():
        try:
            idx = int(tok)
        except ValueError:
            print(f'  ignored non-numeric: {tok!r}')
            continue
        if 1 <= idx <= len(items): picks.append(items[idx - 1][1])
        else:                      print(f'  ignored out-of-range: {idx}')
    return picks


# ─────────────────────────────────────────────────────────────────────────────
#  Target discovery.
#
#  A "target" is a place we can either reuse or launch an edge service on.
#  Three flavours:
#
#    - 'compute' : pre-existing edge already on a compute node — ready to run
#    - 'login'   : pre-existing edge on a login node — we'll submit via PsiJ
#    - 'iri'     : an IRI endpoint we'll connect to and submit a job through
# ─────────────────────────────────────────────────────────────────────────────

def discover_targets(bc):
    """Return a list of ``(label, descriptor_dict)`` for every viable target.

    The bridge itself appears in ``bc.list_edges()`` whenever it hosts plugins
    (e.g. ``iri_connect``); we filter it out — the bridge is not a target.
    """
    targets = []

    # 1. Existing edges
    for name in bc.list_edges():
        if name == 'bridge':
            continue
        if not MACHINE_DEFAULTS.get(name, {}).get('enabled', True):
            print(f'  (skipped {name}: disabled in MACHINE_DEFAULTS)')
            continue
        edge    = bc.get_edge_client(name)
        plugins = edge.list_plugins()

        # We need rhapsody to run ROSE tasks (compute-node case) or psij
        # plus an actual scheduler to submit a child edge (login-node case).
        has_rhapsody = 'rhapsody' in plugins
        has_psij     = 'psij'     in plugins

        # Ask the edge what role / scheduler it thinks it has (added to
        # plugin_sysinfo for exactly this).  Tolerate sysinfo absence.
        try:
            info      = edge.get_plugin('sysinfo').host_role()
            role      = info.get('role',          'unknown')
            scheduler = info.get('scheduler',     'none')
            executor  = info.get('psij_executor', 'local')
        except Exception:
            role, scheduler, executor = 'unknown', 'none', 'local'

        # Compute-mode targets — the edge can run ROSE tasks directly.
        # That covers compute-node edges (inside an allocation) and
        # standalone hosts (laptops / workstations); both load Rhapsody by
        # default per the plugin matrix.
        #
        # Login-mode targets — the edge has a real batch scheduler and can
        # submit a child edge via PsiJ.  ``executor`` came straight from
        # sysinfo.host_role()['psij_executor'] so it matches what PsiJ
        # expects (slurm / pbs / …) regardless of subclass naming.
        if role in ('compute', 'standalone') and has_rhapsody:
            targets.append((
                f'[ready]    edge {name} ({role}, will run tasks here)',
                {'kind': 'compute', 'edge_name': name}))
        elif role == 'login' and has_psij:
            targets.append((
                f'[psij]     edge {name} (login node {scheduler}, '
                f'will submit a child via PsiJ)',
                {'kind'     : 'login',
                 'edge_name': name,
                 'executor' : executor}))
        # else: not a viable target for AMSC (missing plugins, unknown role, …).

    # 2. IRI endpoints.  iri_connect lives on the bridge.
    try:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep_key, ep_info in cx.list_endpoints().items():
            if not IRI_DEFAULTS.get(ep_key, {}).get('enabled', True):
                print(f'  (skipped iri:{ep_key}: disabled in IRI_DEFAULTS)')
                continue
            note = ' (already connected)' if ep_info.get('connected') \
                                          else ' (will submit a job)'
            label = (f'[iri]      {ep_key} — IRI endpoint at '
                     f'{ep_info["label"]}{note}')
            targets.append((label, {'kind': 'iri', 'endpoint': ep_key}))
    except Exception as exc:
        print(f'  (iri_connect unavailable: {exc})')

    return targets


# ─────────────────────────────────────────────────────────────────────────────
#  IRI launch path.
#
#  Steps:
#    1. Ask the user to confirm/override defaults for this endpoint.
#    2. Read the bearer token from ~/.amsc/token_<endpoint>.
#    3. iri_connect.connect(...) — creates a dynamic iri.<endpoint> plugin
#       on the bridge and returns an IRIInstanceClient bound to it.
#    4. Submit a job whose executable is radical-edge-wrapper.sh.  The job
#       will WS-connect back to the bridge; if --tunnel is set, the child
#       opens an outbound SSH tunnel to ``login_host`` first.
# ─────────────────────────────────────────────────────────────────────────────

def configure_iri(endpoint):
    """Walk the user through the per-endpoint settings."""
    d = dict(IRI_DEFAULTS[endpoint])  # local copy
    print(f'\n— Configure IRI endpoint: {endpoint} —')
    d['resource_id']  = ask     ('  resource id',          d['resource_id'])
    d['account']      = ask     ('  account / project',    d['account']) or None
    # OLCF rejects submissions without a top-level ``directory``; NERSC
    # accepts an empty value.  Empty input means "do not send the field".
    d['workdir']      = ask     ('  working directory (or empty)',
                                  d['workdir'] or '') or None
    d['home_dir']     = ask     ('  user $HOME on the target',
                                  d.get('home_dir') or '') or None
    d['queue_name']   = ask     ('  queue / partition',    d['queue_name'])
    d['walltime_min'] = ask_int ('  walltime (minutes)',   d['walltime_min'])
    d['n_nodes']      = ask_int ('  number of nodes',      d['n_nodes'])
    d['constraint']   = ask     ('  constraint (or empty)', d['constraint'] or '') or None
    d['reservation']  = ask     ('  reservation (or empty)', d['reservation'] or '') or None
    d['login_host']   = ask     ('  login host (for --tunnel)', d['login_host'])
    d['tunnel']       = _ask_tunnel(d.get('tunnel'))
    if not d['account']:
        raise RuntimeError(f'IRI {endpoint}: account/project is required')
    if not d['home_dir']:
        raise RuntimeError(f'IRI {endpoint}: home_dir on target is required '
                           f'(used to resolve <home>/{d.get("amsc_dir") or ".amsc"}'
                           f'/ve/bin/radical-edge-wrapper.sh)')
    return d


def read_token(endpoint):
    """Read ``$AMSC_DIR/token_<endpoint>``; raise with a clear message on error."""
    path = AMSC_DIR / f'token_{endpoint}'
    if not path.exists():
        raise RuntimeError(
            f'token file missing: {path}  (put your IRI bearer token '
            f'there, literal string only)')
    token = path.read_text().strip()
    if not token:
        raise RuntimeError(f'token file is empty: {path}')
    return token


def launch_iri(bc, endpoint, cfg, bridge_url):
    """Connect to the IRI endpoint and submit a job that starts an edge.

    Returns ``(iri_client, job_id, edge_name)`` so we can cancel later.
    """
    # Connect (idempotent — 409 returns the existing instance's client).
    cx    = bc.get_edge_client('bridge').get_plugin('iri_connect')
    token = read_token(endpoint)
    iri   = cx.connect(endpoint=endpoint, token=token)

    # Pick a unique edge name so we can spot it in topology updates.
    edge_name = f'amsc-{endpoint}-{uuid.uuid4().hex[:6]}'

    # Build the radical-edge-service.py CLI.  See bin/radical-edge-service.py.
    args = ['--name', edge_name, '--url', bridge_url]
    if cfg['tunnel']:
        args += ['--tunnel', '--tunnel-via', cfg['login_host']]

    # Per-endpoint custom attributes.  Anything beyond queue/duration
    # (constraint, reservation, …) goes through ``attributes`` so the
    # backend can pass it on to its native scheduler.
    attrs = {
        'queue_name': cfg['queue_name'],
        'duration'  : cfg['walltime_min'] * 60,   # seconds
        'account'   : cfg['account'],
    }
    if cfg['constraint']:  attrs['constraint']  = cfg['constraint']
    if cfg['reservation']: attrs['reservation'] = cfg['reservation']

    # Compose absolute paths against the target's $HOME (configured in
    # IRI_DEFAULTS).  We can't rely on bash to expand ``~`` — PsiJ's
    # launchers quote the executable arg, so the literal tilde reaches
    # bash as a path component and never expands.
    home    = cfg['home_dir'].rstrip('/')
    amsc    = (cfg.get('amsc_dir') or '.amsc').strip('/')
    wrapper = f'{home}/{amsc}/ve/bin/radical-edge-wrapper.sh'

    # Cert resolution is delegated to the child edge: it falls back to
    # ``~/.radical/edge/bridge_cert.pem`` (or $RADICAL_BRIDGE_CERT if
    # set on the target side).  We only inject the bridge URL — that
    # changes per bridge run and the file fallback would be stale.
    env = {'RADICAL_BRIDGE_URL': bridge_url}
    env.update(cfg['environment'])
    # Site-specific shell snippet — module loads, env exports, etc.
    # The wrapper ``eval``s this *before* exec-ing dragon / python.
    if cfg.get('setup'):
        env['RADICAL_EDGE_SETUP'] = '; '.join(cfg['setup'])

    job_spec = {
        'executable' : wrapper,
        'arguments'  : args,
        'name'       : edge_name,
        'resources'  : {'node_count': cfg['n_nodes'], 'process_count': 1},
        'attributes' : attrs,
        'environment': env,
    }
    # Top-level ``directory`` is required by Frontier-class SLURM (OLCF);
    # NERSC tolerates its absence.  Send only when set.
    if cfg.get('workdir'):
        job_spec['directory'] = cfg['workdir']

    print(f'  submitting IRI job ({endpoint} → {cfg["resource_id"]}, '
          f'edge name: {edge_name})…')
    job = iri.submit_job(cfg['resource_id'], job_spec)
    print(f'  IRI job_id: {job["job_id"]}')

    return {
        'kind'       : 'iri',
        'iri'        : iri,
        'endpoint'   : endpoint,
        'resource_id': cfg['resource_id'],
        'job_id'     : job['job_id'],
        'edge_name'  : edge_name,
        'cfg'        : cfg,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PsiJ launch path (existing login-node edges).
#
#  Steps:
#    1. Ask the parent edge's queue_info plugin for default account / queue
#       hints (when available) and let the user override.
#    2. Build a minimal job spec; submit_tunneled adds --tunnel --tunnel-via
#       automatically when ``tunnel=True``.
# ─────────────────────────────────────────────────────────────────────────────

def configure_psij(edge_name, executor):
    """Walk the user through the per-target settings for a login-node edge.

    The PsiJ ``executor`` (slurm/pbs) was already detected from the
    edge's sysinfo.host_role() during discovery and is passed in here.
    Per-host pre-fills come from ``MACHINE_DEFAULTS`` when present.
    """
    d = MACHINE_DEFAULTS.get(edge_name, {})
    print(f'\n— Configure PsiJ submission via edge: {edge_name} '
          f'(executor: {executor}) —')

    cfg = {
        'executor'    : executor,
        'queue_name'  : ask     ('  queue / partition',
                                  d.get('queue_name', 'debug')),
        'account'     : ask     ('  account / project',
                                  d.get('account', '') or '') or None,
        'walltime_min': ask_int ('  walltime (minutes)',
                                  d.get('walltime_min', 30)),
        'n_nodes'     : ask_int ('  number of nodes',
                                  d.get('n_nodes', 1)),
        'constraint'  : ask     ('  constraint (or empty)',
                                  d.get('constraint') or '') or None,
        'tunnel'      : _ask_tunnel(d.get('tunnel')),
        # Carried verbatim from MACHINE_DEFAULTS — not prompted.
        'amsc_dir'    : d.get('amsc_dir'),
        'setup'       : list(d.get('setup')     or []),
        'env_setup'   : dict(d.get('env_setup') or {}),
        'app'         : dict(d.get('app')       or {}),
    }
    if not cfg['account']:
        raise RuntimeError(f'edge {edge_name}: account/project is required')
    return cfg


def launch_psij(bc, edge_name, cfg, bridge_url):
    """Submit a child edge via the parent edge's PsiJ plugin."""
    edge = bc.get_edge_client(edge_name)
    psij = edge.get_plugin('psij')

    # Resolve $HOME on the target via the login-edge's sysinfo plugin.
    # Login and compute share $HOME via NFS/Lustre on every site we
    # care about, so the login-edge's home is also the compute job's.
    home    = edge.get_plugin('sysinfo').homedir().rstrip('/')
    amsc    = (cfg.get('amsc_dir') or '.amsc').strip('/')
    wrapper = f'{home}/{amsc}/ve/bin/radical-edge-wrapper.sh'

    # Unique name for the child edge.
    child_name = f'amsc-{edge_name}-{uuid.uuid4().hex[:6]}'

    attrs = {
        'queue_name': cfg['queue_name'],
        'duration'  : cfg['walltime_min'] * 60,
        'account'   : cfg['account'],
    }
    # PsiJ's ``JobAttributes`` schema has no ``constraint`` field; raw
    # ``attributes['constraint']`` would be silently dropped.  Backend-
    # specific flags ride in ``custom_attributes`` keyed by
    # ``<executor>.<flag>`` (e.g. ``slurm.constraint`` -> ``--constraint=…``,
    # ``pbs.l`` -> ``-l …``).  Site defaults from BatchSystem are merged in
    # bridge-side; this dict carries only what the caller explicitly set.
    custom_attrs = {}
    if cfg.get('constraint'):
        custom_attrs[f'{cfg["executor"]}.constraint'] = cfg['constraint']

    # Cert is left to the child edge to resolve from
    # ``~/.radical/edge/bridge_cert.pem`` on the target (or via
    # $RADICAL_BRIDGE_CERT if explicitly set there).  Only the bridge
    # URL — which changes per bridge run — is injected here.
    env = {'RADICAL_BRIDGE_URL': bridge_url}
    # Site-specific shell snippet — module loads, env exports, etc.
    # The wrapper ``eval``s this *before* exec-ing dragon / python.
    if cfg.get('setup'):
        env['RADICAL_EDGE_SETUP'] = '; '.join(cfg['setup'])

    job_spec = {
        'executable'        : wrapper,
        # ``--name`` is required by submit_tunneled; ``--tunnel`` and
        # ``--tunnel-via`` are appended for us when tunnel=True.
        'arguments'         : ['--name', child_name, '--url', bridge_url],
        'attributes'        : attrs,
        'custom_attributes' : custom_attrs,
        'resources'         : {'node_count': cfg['n_nodes'], 'process_count': 1},
        'environment'       : env,
    }

    print(f'  submitting PsiJ job via {edge_name} (executor: {cfg["executor"]}, '
          f'edge name: {child_name})…')
    res = psij.submit_tunneled(job_spec, executor=cfg['executor'],
                               tunnel=cfg['tunnel'])
    print(f'  PsiJ job_id: {res["job_id"]}')

    return {
        'kind'       : 'psij',
        'psij'       : psij,
        'parent_edge': edge_name,
        'job_id'     : res['job_id'],
        'edge_name'  : res.get('edge_name', child_name),
        'cfg'        : cfg,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Per-target env-runner staging.
#
#  ``env_setup`` is a dict mapping a profile name to a list of shell
#  commands.  For each non-empty entry we materialise an
#  ``env_runner_<name>.sh`` whose body runs the listed commands and then
#  ``exec "$@"`` — the actual command (executable + args) is supplied by
#  the caller, so a single runner can launch any application that needs
#  this environment.  We stage the runner via the ``staging`` plugin so
#  it lands in ``<home>/.amsc/`` on the edge's filesystem.
#
#  Returns ``{name: remote_path}`` for the runners we put in place.  An
#  empty dict means the target opted out (no env_setup configured).
# ─────────────────────────────────────────────────────────────────────────────

def stage_env_runners(edge_client, env_setup):
    """Generate + stage one ``env_runner_<name>.sh`` per ``env_setup`` entry."""
    if not env_setup:
        return {}

    # Resolve target $HOME via the edge's sysinfo plugin.
    home    = edge_client.get_plugin('sysinfo').homedir().rstrip('/')
    staging = edge_client.get_plugin('staging')
    AMSC_DIR.mkdir(parents=True, exist_ok=True)

    runners = {}
    for name, cmds in env_setup.items():
        if not cmds:
            continue   # empty profile -> caller can run commands directly

        body = ('#!/bin/bash\n'
                '# Auto-generated by examples/matey.py — do not edit.\n'
                + '\n'.join(cmds)
                + '\nexec "$@"\n')

        local  = AMSC_DIR / f'env_runner_{name}.sh'
        remote = f'{home}/.amsc/env_runner_{name}.sh'

        local.write_text(body)
        staging.put(str(local), remote, overwrite=True)
        runners[name] = remote
        print(f'  staged {remote}')
    return runners


def find_target_cfg(edge_name, created):
    """Look up the per-target cfg for a given edge name.

    Two cases:
      * we launched the edge (PsiJ or IRI) — pull the cfg from the
        ``created`` record we stashed at submission time;
      * the edge was already running — fall back to MACHINE_DEFAULTS.
    """
    for c in created:
        if c.get('edge_name') == edge_name:
            return c['cfg']
    return MACHINE_DEFAULTS.get(edge_name, {})


# ─────────────────────────────────────────────────────────────────────────────
#  Wait for the first edge to register.
#
#  We poll the bridge's edge list every few seconds and return the first
#  expected name we see.  Polling is dumb but readable; for a demo this is
#  better than wiring up an SSE callback bridge to asyncio.
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_first_edge(bc, expected_names, timeout=EDGE_WAIT_SECONDS,
                        poll=3.0, heartbeat=30.0):
    """Block until any name in *expected_names* appears in ``bc.list_edges()``.

    Returns the winning name, or raises TimeoutError after *timeout* seconds.
    Prints a heartbeat at most every ``heartbeat`` seconds so we don't
    spam the screen during long queue waits.
    """
    if not expected_names:
        raise RuntimeError('no expected edges — nothing to wait for')

    print(f'\n— Waiting for first edge to come up '
          f'(any of: {", ".join(expected_names)}) —')
    start_time = time.time()
    last_beat  = start_time
    while time.time() - start_time < timeout:
        live = set(bc.list_edges())
        for name in expected_names:
            if name in live:
                return name
        time.sleep(poll)
        if time.time() - last_beat >= heartbeat:
            elapsed = int(time.time() - start_time)
            print(f'  …{elapsed}s elapsed, {timeout - elapsed}s left')
            last_beat = time.time()
    raise TimeoutError(f'no edge appeared within {timeout}s; '
                       f'expected one of {expected_names}')


# ─────────────────────────────────────────────────────────────────────────────
#  ROSE workflow body.
#
#  Each AL iteration runs the MATEY ``basic_inference.py`` driver as an
#  *executable task*.  The command is composed by the simulation task
#  body and reads:
#
#      bash <env_runner_matey.sh> python <matey_dir>/basic_inference.py
#           --model_dir <model_dir> --newxgc_dir <xgc_dir>
#           --leadtime <K> --AR
#
#  The runner just activates the matey conda env and ``exec``s the
#  remainder, so the runner itself stays application-agnostic.
#
#  Wiring (verified by reading rose/asyncflow source)
#  --------------------------------------------------
#  asyncflow's executable-task future resolves to the captured stdout
#  string (workflow_manager.handle_task_success: as_executable → stdout).
#  ROSE's ``_register_task`` appends ``deps`` futures to the next task's
#  positional args, and asyncflow's ``_extract_dependency_values`` calls
#  ``.result()`` on each Future arg before invoking the function.  Net
#  effect: when ``training`` is registered with ``deps=sim_task``, the
#  simulation's full stdout reaches ``training`` as a positional ``*arg``.
#
#  MATEY's basic_inference prints a single metric line per run (see
#  matey/inference.py:343 and :487):
#      Prediction of XGC-D3D, rmse_loss:<value>; nrmse_loss <value>
#  where ``<value>`` is either a plain float or a ``tensor(...)`` repr.
#  The training task parses that line out of the sim's stdout, stores
#  ``rmse_iter_<K>`` / ``nrmse_iter_<K>`` plus the alias ``mse_iter_<K>``
#  (using nrmse — the normalised loss is the threshold-comparable one)
#  in DDict, and check_mse reads ``mse_iter_<K>`` back.
#
#  AL-driven leadtime
#  ------------------
#  ``active_learn_task`` looks at the (leadtime, nrmse) history in DDict
#  and writes ``next_leadtime_iter_<K+1>`` for the upcoming iteration.
#  The simulation reads ``next_leadtime_iter_<K>`` (falling back to 1
#  when missing — i.e. the pre-loop iteration) to pick its leadtime.
#  Strategy: round-robin sweep over 1..LEADTIME_MAX, biased toward
#  un-evaluated leadtimes, then toward the worst (highest-nrmse) one
#  among those already seen.  No GP needed for this first cut — the
#  closure stays cloudpickle-safe (no sklearn at module level) and the
#  behaviour is deterministic enough to debug.
# ─────────────────────────────────────────────────────────────────────────────

LEADTIME_MAX = 5    # AL sweeps leadtime in [1..LEADTIME_MAX]


def _extract_nrmse(stdout: str) -> tuple[float | None, float | None]:
    """Parse ``rmse_loss``/``nrmse_loss`` out of MATEY's stdout.

    Handles both plain-float and ``tensor(<num>, ...)`` formattings
    (basic_inference.py prints torch tensor repr).  Returns the *last*
    occurrence in stdout (the run actually executed in this task)
    rather than the first, in case the driver logs multiple cases.
    """
    # Allow optional ``tensor(`` wrapper, then capture the number.  The
    # number itself can be int / float / scientific notation.
    pat = re.compile(
        r'rmse_loss[:=]?\s*(?:tensor\()?\s*([\-+]?\d*\.?\d+(?:[eE][\-+]?\d+)?)'
        r'.*?nrmse_loss[:=]?\s*(?:tensor\()?\s*([\-+]?\d*\.?\d+(?:[eE][\-+]?\d+)?)'
    )
    matches = list(pat.finditer(stdout or ''))
    if not matches:
        return None, None
    rmse_s, nrmse_s = matches[-1].group(1), matches[-1].group(2)
    try:    return float(rmse_s), float(nrmse_s)
    except ValueError:
        return None, None
# ─────────────────────────────────────────────────────────────────────────────

async def run_rose_workflow(bridge_url, edge_name, app_cfg, runners):
    """Run the active-learning loop using the named edge as a Dragon backend.

    Args:
      bridge_url: URL the Rhapsody edge backend connects to.
      edge_name:  name of the (compute-side) edge that runs tasks.
      app_cfg:    ``app`` sub-dict from the target's per-target config —
                  must carry ``matey_dir`` / ``matey_model_dir`` /
                  ``matey_xgc_dir``.
      runners:    ``{name: remote_path}`` from ``stage_env_runners`` —
                  this script needs the entry under key ``'matey'``.
    """
    print(f'\n— Running ROSE on edge "{edge_name}" (bridge: {bridge_url}) —')

    # Refuse to start without the bits we need on the target.  Better to
    # die here with a clear message than to launch a futile job.
    matey_runner = runners.get('matey')
    if not matey_runner:
        raise RuntimeError(
            f'no env_runner_matey.sh staged on {edge_name}; '
            f'check the target\'s env_setup["matey"] is non-empty')
    for key in ('matey_dir', 'matey_model_dir', 'matey_xgc_dir'):
        if not app_cfg.get(key):
            raise RuntimeError(
                f'app config for {edge_name} is missing {key!r} — '
                f'fill it in MACHINE_DEFAULTS / IRI_DEFAULTS')

    matey_dir = app_cfg['matey_dir'].rstrip('/')
    model_dir = app_cfg['matey_model_dir']
    xgc_dir   = app_cfg['matey_xgc_dir']

    # 1. Engine + active learner
    backend   = rhapsody.get_backend('edge', bridge_url=bridge_url,
                                     edge_name=edge_name)
    engine    = await backend
    asyncflow = await WorkflowEngine.create(engine)
    acl       = SequentialActiveLearner(asyncflow)

    # 2. Shared DDict for cross-task state (rmse/nrmse history + the
    #    next leadtime the AL strategy wants the simulation to evaluate).
    @asyncflow.function_task
    async def create_ddict() -> str:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict(managers_per_node=1, n_nodes=1,
                      total_mem=512 * 1024 * 1024,
                      wait_for_keys=True,
                      working_set_size=MAX_ITER + 2)
        return ddict.serialize()

    ddict_descriptor = await create_ddict()
    print(f'  DDict ready (descriptor prefix: {ddict_descriptor[:32]}…)')

    # Bind the leadtime knob to the closure so it survives cloudpickle.
    leadtime_max = LEADTIME_MAX

    # 3. Tasks
    #
    # simulation: executable task — composes and returns the matey
    # command string.  Rhapsody / Dragon spawn a fresh process per
    # invocation; our env_runner activates the matey conda env first
    # and then exec's the python driver.
    #
    # The leadtime is read from DDict (key ``next_leadtime_iter_<K>``)
    # so the AL loop drives this knob.  Iteration index is derived the
    # same way every other task does it: count the existing
    # ``mse_iter_*`` sentinels and that's the next iteration index.
    # On the very first call (pre-loop, no entries yet) we default to
    # leadtime=1.
    @acl.simulation_task(as_executable=True)
    async def matey_inference(*args, **kwargs):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        try:
            iteration = 0
            while f"mse_iter_{iteration}" in ddict:
                iteration += 1
            key = f"next_leadtime_iter_{iteration}"
            leadtime = int(ddict[key]) if key in ddict else 1
            # Persist what we actually ran so the training task can pair
            # the resulting nrmse with the leadtime that produced it.
            ddict[f"leadtime_iter_{iteration}"] = leadtime
        finally:
            ddict.detach()

        return (f'bash {matey_runner}'
                f' python {matey_dir}/basic_inference.py'
                f' --model_dir {model_dir}'
                f' --newxgc_dir {xgc_dir}'
                f' --leadtime {leadtime}'
                f' --AR')

    # training: parses MATEY's stdout (passed in as the last positional
    # arg via ROSE's deps→args wiring; see module docstring above) and
    # records rmse / nrmse in DDict.  ``mse_iter_<K>`` is aliased to
    # nrmse — that's the normalised metric the threshold compares.
    @acl.training_task(as_executable=False)
    async def training(*args):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        try:
            iteration = 0
            while f"mse_iter_{iteration}" in ddict:
                iteration += 1

            sim_stdout = args[-1] if args else ''
            if not isinstance(sim_stdout, str):
                sim_stdout = str(sim_stdout) if sim_stdout is not None else ''

            rmse, nrmse = _extract_nrmse(sim_stdout)
            if nrmse is None:
                # Fail loudly — the AL loop has no signal to drive on
                # otherwise.  Stash the raw stdout for post-mortem.
                tail = sim_stdout[-2000:] if sim_stdout else '<empty>'
                ddict[f"sim_stdout_iter_{iteration}"] = tail
                raise RuntimeError(
                    f'iter={iteration}: could not parse rmse/nrmse from '
                    f'MATEY stdout — last 2KB stored at '
                    f'sim_stdout_iter_{iteration}.  Tail: {tail!r}')

            ddict[f"rmse_iter_{iteration}"]  = float(rmse) if rmse is not None else float('nan')
            ddict[f"nrmse_iter_{iteration}"] = float(nrmse)
            ddict[f"mse_iter_{iteration}"]   = float(nrmse)  # alias for criterion
            print(f'[train] iter={iteration} '
                  f'rmse={rmse} nrmse={nrmse:.6f}', flush=True)
        finally:
            ddict.detach()
        return {"rmse": rmse, "nrmse": nrmse}

    # active_learn: choose the leadtime to evaluate next.  Strategy:
    #   1. round-robin sweep — pick the lowest leadtime in 1..MAX that
    #      has not been evaluated yet;
    #   2. once every leadtime has at least one sample, re-pick the one
    #      with the highest current nrmse (most uncertain / worst fit).
    # Stores the choice under ``next_leadtime_iter_<K+1>`` so the next
    # iteration's simulation reads it.
    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        try:
            # iter index of the just-finished training round
            iteration = 0
            while f"mse_iter_{iteration}" in ddict:
                iteration += 1
            iteration -= 1   # latest completed

            # Build (leadtime → list[nrmse]) history.
            history: dict[int, list[float]] = {}
            for i in range(iteration + 1):
                lk = f"leadtime_iter_{i}"
                nk = f"nrmse_iter_{i}"
                if lk in ddict and nk in ddict:
                    history.setdefault(int(ddict[lk]), []).append(float(ddict[nk]))

            # Step 1: any leadtime in 1..MAX never tried?
            unseen = [lt for lt in range(1, leadtime_max + 1)
                      if lt not in history]
            if unseen:
                next_leadtime = unseen[0]
                strategy = 'round-robin (unseen)'
            else:
                # Step 2: pick the worst-performing (highest mean nrmse).
                next_leadtime = max(history,
                                    key=lambda lt: sum(history[lt]) / len(history[lt]))
                strategy = 'max-uncertainty (worst nrmse)'

            ddict[f"next_leadtime_iter_{iteration + 1}"] = next_leadtime

            # Cheap proxy for "uncertainty" so the IterationState has
            # something useful to surface.  Spread of seen nrmse values
            # is a reasonable stand-in until we wire a real surrogate.
            all_nrmse = [v for vs in history.values() for v in vs]
            mean_unc  = (sum(all_nrmse) / len(all_nrmse)) if all_nrmse else 0.0
            max_unc   = max(all_nrmse) if all_nrmse else 0.0

            print(f'[active] iter={iteration} chose leadtime='
                  f'{next_leadtime} ({strategy}) '
                  f'history={ {k: round(sum(v)/len(v), 4) for k, v in history.items()} }',
                  flush=True)
        finally:
            ddict.detach()
        return {"mean_uncertainty": mean_unc,
                "max_uncertainty" : max_unc,
                "next_leadtime"   : next_leadtime}

    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE,
                           threshold=MSE_THRESHOLD, as_executable=False)
    async def check_mse(*args) -> float:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        try:
            iteration = 0
            while f"mse_iter_{iteration}" in ddict:
                iteration += 1
            iteration -= 1
            mse: float = float(ddict[f"mse_iter_{iteration}"])
        finally:
            ddict.detach()
        return mse

    # 4. Run
    print('\nStarting ROSE active-learning loop (MATEY)\n' + '─' * 60)
    final_state = None
    async for state in acl.start(max_iter=MAX_ITER):
        final_state = state
        print(f'  ROSE iter={state.iteration:2d}  MSE={state.metric_value:.6f}  '
              f'stop={state.should_stop}')
        if state.should_stop:
            break

    # 5. Cleanup
    @asyncflow.function_task
    async def destroy_ddict(desc):
        from dragon.data.ddict.ddict import DDict
        DDict.attach(desc).destroy()

    await destroy_ddict(ddict_descriptor)
    await acl.shutdown()

    if final_state and final_state.metric_history:
        print('\n── Convergence ' + '─' * 50)
        for i, mse in enumerate(final_state.metric_history):
            print(f'  iter {i:2d} │ MSE = {mse:.6f}')


# ─────────────────────────────────────────────────────────────────────────────
#  Teardown — only touch resources THIS SCRIPT created.
# ─────────────────────────────────────────────────────────────────────────────

def teardown(bc, created):
    """Cancel jobs we submitted and disconnect IRI endpoints we connected."""
    print('\n— Tearing down resources we created —')

    # 1. Cancel IRI jobs
    for c in created:
        if c['kind'] != 'iri':
            continue
        try:
            c['iri'].cancel_job(c['resource_id'], c['job_id'])
            print(f'  cancelled IRI job {c["job_id"]}@{c["endpoint"]}')
        except Exception as exc:
            print(f'  could not cancel IRI job {c["job_id"]}: {exc}')

    # 2. Cancel PsiJ jobs
    for c in created:
        if c['kind'] != 'psij':
            continue
        try:
            c['psij'].cancel_job(c['job_id'])
            print(f'  cancelled PsiJ job {c["job_id"]} on {c["parent_edge"]}')
        except Exception as exc:
            print(f'  could not cancel PsiJ job {c["job_id"]}: {exc}')

    # 3. Disconnect IRI endpoints
    iri_eps = {c['endpoint'] for c in created if c['kind'] == 'iri'}
    if iri_eps:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep in iri_eps:
            try:
                cx.disconnect(ep)
                print(f'  disconnected IRI endpoint {ep}')
            except Exception as exc:
                print(f'  could not disconnect IRI {ep}: {exc}')


# ─────────────────────────────────────────────────────────────────────────────
#  Main — linear top-to-bottom flow.
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Top-level driver.  Synchronous on purpose: only the ROSE workflow
    body needs an event loop, and that's spun up explicitly with
    ``asyncio.run()`` further down."""
    # Single-instance guard.  Concurrent amsc.py runs interleave their log
    # output and step on each other's plugin sessions; refuse to start a
    # second one.  flock is held until this process exits (kernel auto-
    # releases on close).
    import fcntl
    _lock = open('/tmp/matey.lock', 'w')
    try:    fcntl.flock(_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit('another matey.py is already running; kill it first.')

    # 1. Connect to the bridge.  BridgeClient self-resolves URL + cert
    #    via radical.edge.utils (CLI > env > file).
    bc         = BridgeClient()
    bridge_url = bc.url
    print(f'Bridge: {bridge_url}')
    try:
        # 2. Discover targets and prompt for selection.
        targets = discover_targets(bc)
        if not targets:
            sys.exit('No usable targets discovered.  '
                     'Start at least one edge or expose iri_connect.')

        picks = select_many(targets, 'Pick targets to use:')
        if not picks:
            sys.exit('No targets selected.')

        # 3. Configure + launch each pick.  Pre-existing compute-node edges
        #    require no submission.  Each target's launch is wrapped in a
        #    try/except: a failed launch (bad project, queue rejected, …)
        #    logs a one-liner and the loop moves on to the next target.
        #    Whatever WAS launched successfully is still tracked in
        #    ``created`` so the teardown in the outer ``finally`` cleans
        #    it up even if a later target raises.
        created        = []          # things we will need to tear down
        expected_edges = []          # edge names we expect to come up

        try:
            for t in picks:
                try:
                    if t['kind'] == 'compute':
                        expected_edges.append(t['edge_name'])
                        print(f'\n— Reusing ready edge: {t["edge_name"]} —')

                    elif t['kind'] == 'iri':
                        cfg = configure_iri(t['endpoint'])
                        rec = launch_iri(bc, t['endpoint'], cfg, bridge_url)
                        created.append(rec)
                        expected_edges.append(rec['edge_name'])

                    elif t['kind'] == 'login':
                        cfg = configure_psij(t['edge_name'], t['executor'])
                        rec = launch_psij(bc, t['edge_name'], cfg, bridge_url)
                        created.append(rec)
                        expected_edges.append(rec['edge_name'])
                except Exception as exc:
                    label = t.get('edge_name') or t.get('endpoint') or repr(t)
                    print(f'\n— launch failed for {label}: {exc} —')
                    print('  (continuing with remaining targets)')

            # 4. Wait for the first edge to register, then prepare the
            #    application environment on it (env_runner_<name>.sh
            #    per non-empty env_setup entry) and run the workflow.
            if not expected_edges:
                sys.exit('No targets launched successfully — nothing to run.')
            first = wait_for_first_edge(bc, expected_edges)
            print(f'\n— First edge up: {first} —')

            cfg     = find_target_cfg(first, created)
            app_cfg = dict(cfg.get('app') or {})
            print(f'\n— Staging env runners on {first} —')
            runners = stage_env_runners(bc.get_edge_client(first),
                                        cfg.get('env_setup'))

            asyncio.run(run_rose_workflow(bridge_url, first,
                                          app_cfg, runners))

        finally:
            # 5. Tear down what we created — runs whether the workflow
            #    finished, raised, or we never got that far.  Stragglers
            #    from our submission set keep running idle until their
            #    walltime expires (simpler than racing cancels).
            teardown(bc, created)

    finally:
        bc.close()

    # Only printed when the run completed without an unhandled exception.
    # If the workflow or teardown raised, the traceback is the last word.
    print('\nDone.')


if __name__ == '__main__':
    main()
