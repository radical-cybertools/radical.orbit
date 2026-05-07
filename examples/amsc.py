#!/usr/bin/env python3
"""
ROSE active learning across heterogeneous edge endpoints — interactive demo.

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

4. Runs a small ROSE active-learning workflow on the first-up edge.

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

    python examples/amsc.py
"""

import asyncio
import logging
import os
import sys
import time
import uuid

from collections import defaultdict

from pathlib import Path

import numpy as np

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

# Which workload to run once the first edge is up.  Toggle freely:
#   'rose'     — active-learning loop (run_rose_workflow).
#   'rhapsody' — N matey-inference tasks via rhapsody (submit_rhapsody_workload).
WORKLOAD           = 'rhapsody'

# Demo mode: skip target discovery / configure prompts, auto-pick the
# perlmutter PsiJ path with MACHINE_DEFAULTS values, emit a coarse
# 7-step trace.  Set False to restore the original interactive flow.
DEMO_MODE          = True
N_NODES            = 16

# ROSE active-learning shape (mirrors examples/example_rose.py).
N_MPI_RANKS        = 4      # MPI ranks per simulation launch
N_SAMPLES_PER_RANK = 5      # sparse start; AL drives exploration
N_QUERY            = 8      # query points selected per AL step
MSE_THRESHOLD      = 0.01   # convergence target
MAX_ITER           = 15     # hard cap on AL iterations

# Rhapsody-direct workload shape (mirrors examples/run_matey.py).
N_MATEY_TASKS        = N_NODES * 10        # GPU-bound matey inference tasks
N_GKEYLL_TASKS       = N_NODES * 128 * 3   # CPU-bound gkeyll training tasks
MATEY_WRAPPER_NAME   = 'matey_wrapper.sh'
RHAPSODY_WORK_SUBDIR = 'rhapsody-runs'

# How long we are willing to wait for the first edge to come up.
EDGE_WAIT_SECONDS  = 30 * 60

COUNTERS = defaultdict(int)  # for unique edge names per submission endpoint


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
        'home_dir'    : '/global/u2/m/merzky',
        'amsc_dir'    : None,  # relative to $HOME on the target
        'tunnel'      : 'forward',
        'account'     : 'm5290',
        'workdir'     : None,
        'queue_name'  : 'debug',
        'qos'         : None,
        'walltime_min': 30,
        'n_nodes'     : N_NODES,
        'gpus_per_node': 4,
        'cores_per_node': 128,
        'constraint'  : 'gpu',
        'reservation' : None,
        'environment' : {},
        'setup'       : [
            'module load openmpi',
        ],
        # ``app`` carries workload-specific paths consumed by
        # submit_rhapsody_workload (matey + gkeyll).  ``None`` means
        # "this target does not support the rhapsody workload".
        'app'         : {
            'matey_dir'      : '/global/u2/m/merzky/MATEY',
            'matey_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_DemoMay_toytestonly'
                               '/demo_nbatchsloc100/',
            'matey_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500/',
            'gkeyll_dir'     : '/global/u2/m/merzky/gkeyll/amsc',
            'gkeyll_exe'     : 'rt_gk_d3d_iwl_2x2v_p1.sh',
        },
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
        'qos'         : None,
        'walltime_min': 30,
        'n_nodes'     : N_NODES,
        'gpus_per_node': None,
        'cores_per_node': None,
        'constraint'  : None,
        'reservation' : None,
        'environment' : {},
        'setup'       : None,
        'setup'       : ['module load cray-python/3.11.7',
                        ],
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
        'qos'         : None,
        'walltime_min': 30,
        'n_nodes'     : N_NODES,
        'gpus_per_node': None,
        'cores_per_node': None,
        'constraint'  : None,
        'tunnel'      : 'forward',
        'amsc_dir'    : None,
        'setup'       : None,
        'app'         : None,
    },
    'perlmutter': {
        'enabled'     : True,
        'account'     : 'amsc007_g',
        'queue_name'  : None,                    # 'gpu_ss11',
        'qos'         : 'express_amsc',
        'walltime_min': 30,
        'n_nodes'     : N_NODES,
        'gpus_per_node': 4,
        'cores_per_node': 128,
        'constraint'  : 'gpu',
        'tunnel'      : 'forward',
        'amsc_dir'    : None,
        'setup'       : [
            'module load openmpi',
        ],
        'app'         : {
            'matey_dir'      : '/global/u2/m/merzky/MATEY',
            'matey_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_DemoMay_toytestonly'
                               '/demo_nbatchsloc100/',
            'matey_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500/',
            'gkeyll_dir'     : '/global/u2/m/merzky/gkeyll/amsc',
            'gkeyll_exe'     : 'rt_gk_d3d_iwl_2x2v_p1.sh',
        },
    },
    'odo': {
        'enabled'     : True,
        'account'     : 'fus183',
        'queue_name'  : 'batch',
        'qos'         : None,
        'walltime_min': 30,
        'n_nodes'     : N_NODES,
        'gpus_per_node': None,
        'cores_per_node': None,
        'constraint'  : None,
        'tunnel'      : 'reverse',
        'amsc_dir'    : None,
        'setup'       : [
            'module reset',
            'module load cray-python/3.11.7 craype-network-ofi cray-mpich/8.1.32',
            'module list',
            'python3 -c "import mpi4py.MPI as M; print(M.Get_library_version())"',
                        ],
        'app'         : None,
    },
    'thinkie': {
        'enabled'     : False,
        'amsc_dir'    : None,
        'setup'       : None,
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
#  Demo-mode output helpers.
#
#  ``step()`` prints one aligned, coloured line per coarse phase via rich.
#  ``abort()`` prints a red ABORT line and exits non-zero — used at every
#  fail-fast boundary in the demo flow (no Python traceback noise).
#  ``say()`` is a print() proxy that no-ops in demo mode so chatty diagnostic
#  lines from the underlying functions don't dilute the step trace.  In
#  interactive mode it is plain print().
# ─────────────────────────────────────────────────────────────────────────────

try:
    from rich.console  import Console
    from rich.progress import (BarColumn, Progress, TaskProgressColumn,
                               TextColumn)
    _console = Console()
except ImportError:                              # pragma: no cover
    _console  = None
    Progress  = None
    BarColumn = TaskProgressColumn = TextColumn = None

_TOTAL_STEPS = 7   # connect / pick / configure / submit / await / run / teardown

def step(idx, label, detail=''):
    if _console:
        _console.print(
            f'[cyan]step {idx}/{_TOTAL_STEPS}[/cyan]  '
            f'[bold]{label:<20}[/bold]  '
            f'[bright_white]{detail}[/bright_white]')
    else:
        print(f'step {idx}/{_TOTAL_STEPS}  {label:<20}  {detail}')

def abort(msg):
    """Print a red ABORT line and exit with status 1.  No traceback."""
    if _console:
        _console.print(f'[bold red]ABORT[/bold red]               [red]{msg}[/red]')
    else:
        print(f'ABORT  {msg}')
    sys.exit(1)

def say(*args, **kwargs):
    """Chatty print, suppressed in demo mode."""
    if not DEMO_MODE:
        print(*args, **kwargs)


def _make_progress(n_matey, n_gkeyll):
    """Build the rhapsody workload's progress display.

    Returns ``(progress, tids)`` where *progress* is a ``rich.Progress``
    (or ``None`` when rich is unavailable) and *tids* maps each active
    kind to its task id.  Drive it with::

        progress.update(tids[kind], advance=1,
                        done=counts[kind]['done'],
                        failed=counts[kind]['failed'])
    """
    if Progress is None or _console is None:
        return None, {}

    progress = Progress(
        TextColumn("  [cyan]{task.fields[label]:<6s}[/cyan]"),
        BarColumn(bar_width=20),
        TaskProgressColumn(),
        TextColumn(
            "[bright_white]{task.fields[done]:>6d}[/bright_white] / "
            "[bright_white]{task.total:>6d}[/bright_white] done, "
            "[red]{task.fields[failed]:>6d}[/red] failed"),
        console=_console,
    )
    tids = {}
    for kind, n in (('matey', n_matey), ('gkeyll', n_gkeyll)):
        if n > 0:
            tids[kind] = progress.add_task('', total=n, label=kind,
                                           done=0, failed=0)
    return progress, tids


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
    d['login_host']   = ask     ('  login host (for --tunnel forward)',
                                  d['login_host'])
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
    edge_name = f'{endpoint}.{COUNTERS[endpoint]}'
    COUNTERS[endpoint] += 1

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
    # GPU allocation hint — best-effort: IRI may translate to the underlying
    # scheduler's flag (--gpus-per-node on SLURM) or silently drop it.
    if cfg.get('gpus_per_node'):
        attrs['gpus_per_node'] = cfg['gpus_per_node']
    if cfg.get('qos'):
        attrs['qos'] = cfg['qos']

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
        'resources'  : {'node_count': cfg['n_nodes']},
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
        'setup'       : list(d.get('setup') or []),
        'gpus_per_node': d.get('gpus_per_node'),
        'cores_per_node': d.get('cores_per_node'),
        'qos'         : d.get('qos'),
        'app'         : d.get('app'),
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
    COUNTERS[edge_name] += 1
    child_name = f'{edge_name}.{COUNTERS[edge_name]}'

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
    if cfg.get('gpus_per_node'):
        custom_attrs[f'{cfg["executor"]}.gpus-per-node'] = str(cfg['gpus_per_node'])
    if cfg.get('qos'):
        custom_attrs[f'{cfg["executor"]}.qos'] = cfg['qos']
    # Allocate ``n_nodes`` nodes but launch only ONE wrapper on the head
    # compute node -- Dragon spawns its own daemons on the rest.  PsiJ's
    # ResourceSpecV1 can't express this (its ``process_count = node_count
    # × ppn`` constraint forbids 1 task across N nodes), so we ride
    # through the slurm.mustache custom_attributes hook: it renders the
    # ``--nodes`` line *after* the default ResourceSpec block, so SLURM's
    # last-flag-wins gives us ``--nodes=N --ntasks=1 --ntasks-per-node=1``.
    if cfg.get('n_nodes'):
        custom_attrs[f'{cfg["executor"]}.nodes'] = str(cfg['n_nodes'])

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
        # No ``resources`` -- ``--nodes`` rides through ``slurm.nodes``
        # in custom_attributes so PsiJ's default ResourceSpec
        # (``--ntasks=1``) stays in place.  See the comment on
        # ``slurm.nodes`` above.
        'environment'       : env,
    }

    say(f'  submitting PsiJ job via {edge_name} (executor: {cfg["executor"]}, '
        f'edge name: {child_name})…')
    res = psij.submit_tunneled(job_spec, executor=cfg['executor'],
                               tunnel=cfg['tunnel'])
    say(f'  PsiJ job_id: {res["job_id"]}')

    return {
        'kind'       : 'psij',
        'psij'       : psij,
        'parent_edge': edge_name,
        'job_id'     : res['job_id'],
        'edge_name'  : res.get('edge_name', child_name),
        'cfg'        : cfg,
    }


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

    say(f'\n— Waiting for first edge to come up '
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
            say(f'  …{elapsed}s elapsed, {timeout - elapsed}s left')
            last_beat = time.time()
    raise TimeoutError(f'no edge appeared within {timeout}s; '
                       f'expected one of {expected_names}')


def _find_perlmutter_psij(bc):
    """Locate a connected perlmutter login-edge with the PsiJ plugin.

    Returns ``(edge_name, executor)`` or ``None``.  Used by demo mode to
    auto-pick the only target it cares about, without going through the
    interactive ``discover_targets`` / ``select_many`` flow.
    """
    if 'perlmutter' not in set(bc.list_edges()):
        return None
    edge    = bc.get_edge_client('perlmutter')
    plugins = edge.list_plugins()
    if 'psij' not in plugins:
        return None
    try:
        info     = edge.get_plugin('sysinfo').host_role()
        executor = info.get('psij_executor', 'slurm')
    except Exception:
        executor = 'slurm'
    return ('perlmutter', executor)


# ─────────────────────────────────────────────────────────────────────────────
#  ROSE workflow body.  Mirrors examples/example_rose.py — only difference:
#  it targets a specific edge_name passed in by the launcher.
# ─────────────────────────────────────────────────────────────────────────────

async def run_rose_workflow(bridge_url, edge_name, cfg=None):
    """Run the active-learning loop using the named edge as a Dragon backend.

    *cfg* (the matched per-target config dict) is accepted for signature
    parity with :func:`submit_rhapsody_workload` and currently unused.

    Closure discipline (from example_rose.py): every task captures only
    ``ddict_descriptor`` (a plain str) and re-derives the current iteration
    from sentinel keys in the DDict — no live object references.
    """
    print(f'\n— Running ROSE on edge "{edge_name}" (bridge: {bridge_url}) —')

    # 1. Engine + active learner
    backend   = rhapsody.get_backend('edge', bridge_url=bridge_url,
                                     edge_name=edge_name)
    engine    = await backend
    asyncflow = await WorkflowEngine.create(engine)
    acl       = SequentialActiveLearner(asyncflow)

    # 2. Shared DDict for cross-task state
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

    # 3. Tasks — captured closure: ddict_descriptor (str) only.
    @acl.simulation_task(as_executable=False)
    async def simulation(*args,
                         task_description={"process_templates": [(N_MPI_RANKS, {})]}):

        import sys, traceback
        try:
            from mpi4py import MPI
            from dragon.data.ddict.ddict import DDict
        except Exception:
            sys.stderr.write("=== SIM IMPORT FAILED ===\n")
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            raise

        from mpi4py import MPI
        from dragon.data.ddict.ddict import DDict

        comm  = MPI.COMM_WORLD
        rank  = comm.Get_rank()
        size  = comm.Get_size()
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1

        prev_iter = iteration - 1
        query_key = f"query_points_iter_{prev_iter}"

        if prev_iter >= 0 and query_key in ddict:
            all_query = ddict[query_key]
            X_local   = all_query[rank::size]
            rng       = np.random.default_rng(seed=rank + iteration * size)
            y_local   = (np.sin(X_local) * np.sin(5 * X_local)
                         + rng.normal(0.0, 0.1, X_local.shape))
        else:
            rng     = np.random.default_rng(seed=rank + iteration * size)
            X_local = rng.uniform(0.0, 2.0 * np.pi, (N_SAMPLES_PER_RANK, 1))
            y_local = (np.sin(X_local) * np.sin(5 * X_local)
                       + rng.normal(0.0, 0.1, X_local.shape))

        ddict[f"sim_rank_{rank}_iter_{iteration}"] = {"X": X_local, "y": y_local}
        comm.Barrier()
        if rank == 0:
            ddict[f"sim_meta_iter_{iteration}"] = {
                "n_ranks"           : size,
                "n_samples_per_rank": len(X_local),
            }
            print(f'[sim]   iter={iteration} ranks={size} '
                  f'pts={size * len(X_local)}', flush=True)
        ddict.detach()
        return {}

    @acl.training_task(as_executable=False)
    async def training(*args):
        # sklearn lives only on the HPC side; import inside the task.
        from sklearn.gaussian_process         import GaussianProcessRegressor
        from sklearn.gaussian_process.kernels import RBF, WhiteKernel
        from sklearn.metrics                  import mean_squared_error
        from dragon.data.ddict.ddict          import DDict
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        # Accumulate samples from ALL completed iterations — active learning
        # trains on the cumulative labeled set, not just the latest batch.
        X_parts, y_parts = [], []
        for it in range(iteration + 1):
            meta = ddict[f"sim_meta_iter_{it}"]
            for rank in range(meta["n_ranks"]):
                data = ddict[f"sim_rank_{rank}_iter_{it}"]
                X_parts.append(data["X"])
                y_parts.append(data["y"])
        X_train = np.vstack(X_parts)
        y_train = np.vstack(y_parts).ravel()

        kernel = (RBF(length_scale=0.3, length_scale_bounds=(0.01, 5.0))
                  + WhiteKernel(noise_level=1e-2))
        gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=10,
                                      normalize_y=True)
        gp.fit(X_train, y_train)

        X_test = np.linspace(0.0, 2.0 * np.pi, 300).reshape(-1, 1)
        y_pred = gp.predict(X_test)
        y_true = (np.sin(X_test) * np.sin(5 * X_test)).ravel()
        mse    = float(mean_squared_error(y_true, y_pred))

        ddict[f"model_iter_{iteration}"] = gp
        ddict[f"mse_iter_{iteration}"]   = mse
        print(f'[train] iter={iteration} n={len(X_train)} MSE={mse:.6f}',
              flush=True)
        ddict.detach()
        return {}

    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"model_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        gp = ddict[f"model_iter_{iteration}"]
        X_candidates  = np.linspace(0.0, 2.0 * np.pi, 500).reshape(-1, 1)
        _, std        = gp.predict(X_candidates, return_std=True)
        top_idx       = np.argsort(std)[-N_QUERY:]
        ddict[f"query_points_iter_{iteration}"] = X_candidates[top_idx]
        print(f'[active] iter={iteration} mean_unc={std.mean():.4f} '
              f'max_unc={std.max():.4f} n_query={N_QUERY}', flush=True)
        ddict.detach()
        return {"mean_uncertainty": float(std.mean()),
                "max_uncertainty" : float(std.max())}

    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE,
                           threshold=MSE_THRESHOLD, as_executable=False)
    async def check_mse(*args) -> float:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict.attach(ddict_descriptor)
        iteration = 0
        while f"mse_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1
        mse: float = ddict[f"mse_iter_{iteration}"]
        ddict.detach()
        return mse

    # 4. Run
    print('\nStarting ROSE active-learning loop\n' + '─' * 60)
    final_state = None
    async for state in acl.start(max_iter=MAX_ITER):
        final_state = state
        print(f'  ROSE iter={state.iteration:2d}  MSE={state.metric_value:.6f}  '
              f'mean_unc={state.mean_uncertainty}  stop={state.should_stop}')
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
#  Rhapsody-direct workload (alternative to ROSE).
#
#  Mirrors examples/run_matey.py: submits N matey-inference tasks against
#  the named edge as a rhapsody backend, with a semaphore-limited
#  concurrency cap and per-task GPU-affinity round-robin.  Goes through
#  the edge backend (bridge -> edge -> rhapsody plugin -> V3) — same
#  transport as run_rose_workflow.
#
#  Per-target requirements (see ``app`` field in IRI_DEFAULTS /
#  MACHINE_DEFAULTS):
#    * ``app.matey_dir``       — host directory containing
#                                ``matey_wrapper.sh`` and
#                                ``basic_inference.py``.
#    * ``app.matey_model_dir`` — passed as ``--model_dir``.
#    * ``app.matey_xgc_dir``   — passed as ``--newxgc_dir``.
#    * ``gpus_per_node``       — used to derive concurrency
#                                (``len(nodelist) * gpus_per_node``) and
#                                the per-task ``gpu_affinity`` index.
# ─────────────────────────────────────────────────────────────────────────────

async def submit_rhapsody_workload(bridge_url, edge_name, cfg, nodelist):
    """Submit matey-inference + gkeyll tasks via the named edge.

    Two task families share one Session and run concurrently, mixed by
    independent Semaphores: matey scales by GPU count (``n_hosts *
    gpus_per_node``), gkeyll by core count (``n_hosts * cores_per_node``),
    where ``n_hosts = len(nodelist)``.

    *nodelist* is the list of compute hostnames in the edge's allocation
    (from ``queue_info.nodelist()``).  Each task's policy pins it to a
    specific (host, gpu/core) slot via ``Policy.Placement.HOST_NAME``,
    so dragon's scheduler is forced onto the slot we picked rather than
    relying on its internal round-robin.

    Either family is skipped when its config block / resource count is
    absent or zero; bails if neither has anything to do.
    """
    app_cfg = (cfg or {}).get('app')
    if not app_cfg:
        raise RuntimeError(
            f"target {edge_name!r} has no 'app' config block — "
            "the rhapsody workload is not supported here.  Either "
            "set WORKLOAD='rose' or populate IRI_DEFAULTS / "
            "MACHINE_DEFAULTS['app'] for this target.")

    n_hosts        = len(nodelist) or 1
    gpus_per_node  = cfg.get('gpus_per_node')  or 0
    cores_per_node = cfg.get('cores_per_node') or 0
    n_gpus         = n_hosts * gpus_per_node
    n_cores        = n_hosts * cores_per_node

    has_matey = (n_gpus > 0 and N_MATEY_TASKS > 0
                 and all(app_cfg.get(k) for k in
                         ('matey_dir', 'matey_model_dir', 'matey_xgc_dir')))
    has_gkeyll = (n_cores > 0 and N_GKEYLL_TASKS > 0
                  and all(app_cfg.get(k) for k in ('gkeyll_dir', 'gkeyll_exe')))

    if not (has_matey or has_gkeyll):
        raise RuntimeError(
            f"target {edge_name!r}: nothing to run.  Need either "
            "(gpus_per_node > 0 + N_MATEY_TASKS > 0 + matey_* paths) "
            "or (cores_per_node > 0 + N_GKEYLL_TASKS > 0 + gkeyll_dir/exe).")

    # Lazy imports: dragon's Policy + cloudpickle are only needed when the
    # rhapsody workload actually runs; this lets amsc.py parse on client
    # machines without dragon installed.
    import base64
    import cloudpickle
    from dragon.infrastructure.policy import Policy
    from rhapsody.api import ComputeTask, Session

    def _pack_psk(cwd, policy):
        """Cloudpickle-encode task_backend_specific_kwargs for the wire.

        dragon ``Policy`` is a C-extension object that msgpack can't serialise.
        We ride through rhapsody's existing ``_pickled_fields`` escape hatch:
        encode the whole kwargs dict as ``cloudpickle::<b64>`` here and let
        the rhapsody plugin's ``_deserialize_task`` unpickle it on the edge.
        """
        raw = {'process_template': {'cwd': cwd, 'policy': policy}}
        return 'cloudpickle::' + base64.b64encode(cloudpickle.dumps(raw)).decode()

    matey_tasks  = []
    gkeyll_tasks = []

    if has_matey:
        matey_dir  = app_cfg['matey_dir'].rstrip('/')
        matey_wrap = f'{matey_dir}/{MATEY_WRAPPER_NAME}'
        matey_wd   = f'{matey_dir}/{RHAPSODY_WORK_SUBDIR}'
        matey_args = [
            'python', f'{matey_dir}/examples/basic_inference.py',
            '--model_dir',  app_cfg['matey_model_dir'],
            '--use_ddp',
            '--on_perlmutter',
            '--AR',
            '--leadtime',   '5',
            '--newxgc_dir', app_cfg['matey_xgc_dir'],
        ]
        matey_tasks = [
            ComputeTask(
                executable=matey_wrap,
                arguments=matey_args,
                capture_stdio=True,
                task_backend_specific_kwargs=_pack_psk(matey_wd, Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=nodelist[i % n_hosts],
                    gpu_affinity=[(i // n_hosts) % gpus_per_node])),
                _pickled_fields=['task_backend_specific_kwargs'],
            )
            for i in range(N_MATEY_TASKS)
        ]

    if has_gkeyll:
        gkeyll_dir = app_cfg['gkeyll_dir'].rstrip('/')
        gkeyll_exe = f'{gkeyll_dir}/{app_cfg["gkeyll_exe"]}'
        gkeyll_wd  = f'{gkeyll_dir}/{RHAPSODY_WORK_SUBDIR}'
        gkeyll_tasks = [
            ComputeTask(
                executable=gkeyll_exe,
                arguments=[],
                capture_stdio=True,
                task_backend_specific_kwargs=_pack_psk(gkeyll_wd, Policy(
                    placement=Policy.Placement.HOST_NAME,
                    host_name=nodelist[i % n_hosts],
                    cpu_affinity=[(i // n_hosts) % cores_per_node])),
                _pickled_fields=['task_backend_specific_kwargs'],
            )
            for i in range(N_GKEYLL_TASKS)
        ]

    say(f'\n— Running rhapsody workload on edge "{edge_name}" '
        f'(bridge: {bridge_url}) —')
    if has_matey:
        say(f'  matey  : {len(matey_tasks)} tasks, concurrency {n_gpus} '
            f'({n_hosts} host x {gpus_per_node} gpu)')
        say(f'           wrapper={matey_wrap}, cwd={matey_wd}')
    if has_gkeyll:
        say(f'  gkeyll : {len(gkeyll_tasks)} tasks, concurrency {n_cores} '
            f'({n_hosts} host x {cores_per_node} core)')
        say(f'           exe={gkeyll_exe}, cwd={gkeyll_wd}')

    backend = await rhapsody.get_backend(
        'edge', bridge_url=bridge_url, edge_name=edge_name)

    # Per-kind counters, inspected after gather for the summary line.
    counts = {'matey':  {'done': 0, 'failed': 0},
              'gkeyll': {'done': 0, 'failed': 0}}

    # ``Session(work_dir=…)`` is a client-side setting (rhapsody calls
    # ``os.makedirs(backend._work_dir)`` locally) — we can't pass the
    # remote matey/gkeyll paths there.  Per-task ``cwd`` rides through
    # the ``process_template`` instead.
    progress, tids = _make_progress(len(matey_tasks), len(gkeyll_tasks))

    async with Session(backends=[backend]) as session:
        # Use 1 as a no-op cap when a kind isn't running (its task list is
        # empty, so no coro will ever acquire that semaphore).
        sem_gpu  = asyncio.Semaphore(n_gpus  or 1)
        sem_core = asyncio.Semaphore(n_cores or 1)

        async def run_one(task, kind, sem):
            async with sem:
                await session.submit_tasks([task])
                try:
                    await task
                    counts[kind]['done'] += 1
                except BaseException:
                    counts[kind]['failed'] += 1
                if progress:
                    c = counts[kind]
                    progress.update(tids[kind], advance=1,
                                    done=c['done'], failed=c['failed'])

        coros  = [run_one(t, 'matey',  sem_gpu)  for t in matey_tasks]
        coros += [run_one(t, 'gkeyll', sem_core) for t in gkeyll_tasks]

        if progress:
            with progress:
                await asyncio.gather(*coros, return_exceptions=True)
        else:
            await asyncio.gather(*coros, return_exceptions=True)


# ─────────────────────────────────────────────────────────────────────────────
#  Teardown — only touch resources THIS SCRIPT created.
# ─────────────────────────────────────────────────────────────────────────────

def teardown(bc, created):
    """Cancel jobs we submitted and disconnect IRI endpoints we connected."""
    say('\n— Tearing down resources we created —')

    # 1. Cancel IRI jobs
    for c in created:
        if c['kind'] != 'iri':
            continue
        try:
            c['iri'].cancel_job(c['resource_id'], c['job_id'])
            say(f'  cancelled IRI job {c["job_id"]}@{c["endpoint"]}')
        except Exception as exc:
            say(f'  could not cancel IRI job {c["job_id"]}: {exc}')

    # 2. Cancel PsiJ jobs
    for c in created:
        if c['kind'] != 'psij':
            continue
        try:
            c['psij'].cancel_job(c['job_id'])
            say(f'  cancelled PsiJ job {c["job_id"]} on {c["parent_edge"]}')
        except Exception as exc:
            say(f'  could not cancel PsiJ job {c["job_id"]}: {exc}')

    # 3. Disconnect IRI endpoints
    iri_eps = {c['endpoint'] for c in created if c['kind'] == 'iri'}
    if iri_eps:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep in iri_eps:
            try:
                cx.disconnect(ep)
                say(f'  disconnected IRI endpoint {ep}')
            except Exception as exc:
                say(f'  could not disconnect IRI {ep}: {exc}')


# ─────────────────────────────────────────────────────────────────────────────
#  Demo-mode driver — auto-pick perlmutter PsiJ, no prompts, 7-step trace.
# ─────────────────────────────────────────────────────────────────────────────

def _main_demo(bc, bridge_url):
    """Auto-run the perlmutter PsiJ path with MACHINE_DEFAULTS values.

    Fail-fast at every boundary via ``abort()`` — no Python tracebacks.
    Mirrors the interactive flow's structure but skips ``select_many``
    and ``configure_psij`` (defaults are taken verbatim) and skips IRI
    target discovery entirely.
    """
    step(1, 'connect bridge', bridge_url)

    target = _find_perlmutter_psij(bc)
    if not target:
        abort("no 'perlmutter' login edge with PsiJ found in bridge "
              "topology.  Start the parent edge on Perlmutter first.")
    edge_name, executor = target
    step(2, 'pick target', f'{edge_name} (psij/{executor})')

    cfg = dict(MACHINE_DEFAULTS['perlmutter'])
    cfg['executor'] = executor
    qos_str = f', qos={cfg["qos"]}' if cfg.get('qos') else ''
    step(3, 'configure',
         f'{cfg["n_nodes"]} node x {cfg["gpus_per_node"]} gpu '
         f'x {cfg["cores_per_node"]} core, {cfg["walltime_min"]}m walltime, '
         f'queue={cfg["queue_name"]}{qos_str}')

    created = []
    try:
        try:
            rec = launch_psij(bc, edge_name, cfg, bridge_url)
        except Exception as exc:
            abort(f'launch_psij failed: {exc}')
        created.append(rec)
        step(4, 'submit child edge',
             f'job={rec["job_id"][:8]}…  edge={rec["edge_name"]}')

        t0 = time.time()
        try:
            first = wait_for_first_edge(bc, [rec['edge_name']])
        except Exception as exc:
            abort(f'wait_for_first_edge failed: {exc}')
        step(5, 'await child edge', f'up after {int(time.time() - t0)}s')

        # Fetch the child edge's allocated hostnames via queue_info.  These
        # become the ``host_name`` field of each task's Policy in step 6,
        # so dragon's scheduler is forced onto a (host, gpu/cpu) slot we
        # picked rather than relying on its internal round-robin.
        try:
            nodelist = bc.get_edge_client(first).get_plugin('queue_info').nodelist()
        except Exception as exc:
            abort(f'queue_info.nodelist failed: {exc}')
        if not nodelist:
            abort(f'edge {first!r} reported empty nodelist (queue_info not '
                  f'loaded, or edge not inside a batch allocation)')
        n_hosts = len(nodelist)
        n_gpus  = n_hosts * (cfg.get('gpus_per_node')  or 0)
        n_cores = n_hosts * (cfg.get('cores_per_node') or 0)
        step(6, 'run rhapsody',
             f'{n_hosts} hosts  '
             f'matey {N_MATEY_TASKS} (cap {n_gpus})  '
             f'gkeyll {N_GKEYLL_TASKS} (cap {n_cores})')

        try:
            asyncio.run(submit_rhapsody_workload(
                bridge_url, first, rec.get('cfg') or cfg, nodelist))
        except Exception as exc:
            abort(f'workload failed: {exc}')

    finally:
        step(7, 'teardown', f'cancelling {len(created)} psij job(s)')
        teardown(bc, created)


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
    _lock = open('/tmp/amsc.lock', 'w')
    try:    fcntl.flock(_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit('another amsc.py is already running; kill it first.')

    # 1. Connect to the bridge.  BridgeClient self-resolves URL + cert
    #    via radical.edge.utils (CLI > env > file).
    bc         = BridgeClient()
    bridge_url = bc.url

    if DEMO_MODE:
        try:
            _main_demo(bc, bridge_url)
        finally:
            bc.close()
        return

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

            # 4. Wait for the first edge to register, then run the workflow.
            if not expected_edges:
                sys.exit('No targets launched successfully — nothing to run.')
            first = wait_for_first_edge(bc, expected_edges)
            print(f'\n— First edge up: {first} —')
            matched = next((r for r in created if r.get('edge_name') == first),
                           None)
            matched_cfg = (matched or {}).get('cfg') or {}

            if WORKLOAD == 'rose':
                asyncio.run(run_rose_workflow(bridge_url, first, matched_cfg))
            elif WORKLOAD == 'rhapsody':
                # See _main_demo for why we fetch the nodelist here.
                try:
                    nodelist = bc.get_edge_client(first) \
                                 .get_plugin('queue_info').nodelist()
                except Exception as exc:
                    sys.exit(f'queue_info.nodelist failed for {first}: {exc}')
                if not nodelist:
                    sys.exit(f'edge {first!r} reported empty nodelist '
                             '(queue_info not loaded, or edge not in alloc)')
                asyncio.run(submit_rhapsody_workload(bridge_url, first,
                                                    matched_cfg, nodelist))
            else:
                sys.exit(f'unknown WORKLOAD={WORKLOAD!r} '
                         f"(expected 'rose' or 'rhapsody')")

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
