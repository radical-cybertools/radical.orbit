#!/usr/bin/env python3
"""
Rhapsody workload across heterogeneous edge endpoints — single-target runner.

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

Usage
-----
::

    python examples/amsc.py [<kind>:<name>] [horizontal|vertical] [<n_nodes>]

Where ``<kind>`` is one of ``psij``, ``iri``, or ``compute``, and ``<name>``
selects an entry in ``MACHINE_DEFAULTS`` (psij / compute) or
``IRI_DEFAULTS`` (iri).  The optional second / third arguments override
the slicing mode and the allocation size declared in the matching
``*_DEFAULTS`` entry; they can appear in any order (dispatched by
content).  Examples::

    python examples/amsc.py psij:perlmutter
    python examples/amsc.py psij:perlmutter horizontal 16
    python examples/amsc.py psij:perlmutter vertical 8
    python examples/amsc.py psij:thinkie
    python examples/amsc.py iri:olcf
    python examples/amsc.py compute:thinkie

With no arguments the script defaults to ``psij:perlmutter`` and the
slicing / node-count from ``MACHINE_DEFAULTS``.

The run is non-interactive: defaults are taken verbatim from the
matching ``*_DEFAULTS`` entry, and a 7-step coloured trace is emitted
(connect → pick → configure → submit → await → run → teardown).

Prerequisites on every target machine
-------------------------------------
- A radical.edge install with Rhapsody and Dragon at: ``~/.amsc/ve``
- A login host reachable from the compute node (used for ``--tunnel``)

Tokens
------
IRI bearer tokens are read locally and live at::

    ~/.amsc/token_nersc
    ~/.amsc/token_olcf

The script reads them from disk and sends them to the bridge once at
``iri_connect.connect()`` time.  The bridge holds them in process memory
only — they are never written to disk on the bridge side.
"""

import asyncio
import logging
import os
import sys
import time

from collections import defaultdict

from pathlib import Path

# RADICAL Edge client + Rhapsody bits
from radical.edge.client import BridgeClient

import rhapsody

# Note: dragon / cloudpickle are NOT imported at module level.  They
# are imported inside submit_rhapsody_workload so they only have to be
# installed on the HPC side (where the tasks actually execute), not on
# the client where this script is launched from.

# Quiet logging: this is a demo, the print() lines tell the story.
rhapsody.enable_logging(level=logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
#  Workflow knobs — edit to taste.
# ─────────────────────────────────────────────────────────────────────────────

N_NODES            = 16
N_GENERATIONS      = 1   # uniform scaling factor across all task kinds

# Rhapsody workload shape (mirrors examples/run_matey.py).
N_MATEY_TASKS          = N_NODES * 10      * N_GENERATIONS
N_INFER_TASKS      = N_NODES * 10      * N_GENERATIONS
N_GKEYLL_TASKS         = N_NODES * 128 * 3 * N_GENERATIONS
MATEY_WRAPPER_NAME     = 'matey_wrapper.sh'
INFER_WRAPPER_NAME = 'matey_wrapper.sh'
RHAPSODY_WORK_SUBDIR   = 'rhapsody-runs'

# Per-task-kind spec consumed by ``submit_rhapsody_workload``.  Each entry
# is a tuple ``(name, n_tasks, required_app_paths, default_template)``:
#
#   - name                 : kind label; also the prefix on ``app_cfg`` keys
#                            (``<name>_dir`` for cwd, ``<name>_executable`` /
#                            ``<name>_arguments`` for overrides).
#   - n_tasks              : how many tasks of this kind to submit.
#   - required_app_paths   : ``app_cfg`` keys that must exist when no
#                            ``<name>_executable`` override is set.
#   - default_template     : ``'matey'`` / ``'infer'`` (each builds its own
#                            basic_infer.py argv -- currently identical but
#                            kept separate so they can diverge) or ``'gkeyll'``
#                            (single exe, no args).
KINDS = [
    ('matey',  N_MATEY_TASKS,  ('matey_model_dir', 'matey_xgc_dir'), 'matey'),
    ('infer',  N_INFER_TASKS,  ('infer_model_dir', 'infer_xgc_dir'), 'infer'),
    ('gkeyll', N_GKEYLL_TASKS, ('gkeyll_exe',                     ), 'gkeyll'),
]

# How long we are willing to wait for the first edge to come up.
EDGE_WAIT_SECONDS  = 30 * 60

COUNTERS = defaultdict(int)  # for unique edge names per submission endpoint


# ─────────────────────────────────────────────────────────────────────────────
#  Resource slicing — machine-independent.
#
#  Carves the edge's allocation into per-kind pools.  Two shapes; switch
#  by commenting out one block and uncommenting the other.
#
#  - horizontal: every kind shares all nodes.  ``per_node`` is how many
#    devices (gpu or cpu) on each node that kind owns; affinity offsets
#    stack per device class so kinds don't collide.  Use ``'rest'`` to
#    claim whatever's left on a device after the other kinds.
#
#  - vertical: kinds get disjoint node subsets, sized by ``weight``.
#    Each kind uses its declared device class at full per-node capacity
#    on its own nodes.
#
#  ``device`` is the device class that kind binds to (``'gpu'`` →
#  ``gpu_affinity``, ``'cpu'`` → ``cpu_affinity``).  Different machines
#  can flip the mapping (matey on cpu, gkeyll on gpu, …) by editing
#  this block; per-machine MACHINE_DEFAULTS / IRI_DEFAULTS only carry
#  the device totals (gpus_per_node, cores_per_node).
# ─────────────────────────────────────────────────────────────────────────────

# --- shape A: horizontal (per-node device counts) ---------------------------
SLICING = {
    'mode': 'horizontal',
    'kinds': {
        'matey':  {'device': 'gpu', 'per_node': 2},
        'infer':  {'device': 'gpu', 'per_node': 2},
        'gkeyll': {'device': 'cpu', 'per_node': 'rest'},
    },
}

# --- shape B: vertical (whole-node weights) ---------------------------------
# SLICING = {
#     'mode': 'vertical',
#     'kinds': {
#         'matey':  {'device': 'gpu', 'weight': 1},
#         'infer':  {'device': 'gpu', 'weight': 1},
#         'gkeyll': {'device': 'cpu', 'weight': 2},
#     },
# }


# ─────────────────────────────────────────────────────────────────────────────
#  Per-IRI-endpoint defaults.
#  Best-guesses — edit any field below to match your account / project.
#  Selected by ``iri:<endpoint>`` on the command line; values are taken
#  verbatim, no prompts.
# ─────────────────────────────────────────────────────────────────────────────

IRI_DEFAULTS = {
    'nersc': {
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
        # submit_rhapsody_workload (matey + infer + gkeyll).  ``None``
        # means "this target does not support the rhapsody workload".
        'app'         : {
            'matey_dir'      : '/global/u2/m/merzky/MATEY',
            'matey_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_DemoMay_toytestonly'
                               '/demo_nbatchsloc100/',
            'matey_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500/',
            'infer_dir'      : '/global/u2/m/merzky/MATEY',
            'infer_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_DemoMay_toytestonly'
                               '/demo_nbatchsloc100/',
            'infer_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500/',
            'gkeyll_dir'     : '/global/u2/m/merzky/gkeyll/amsc',
            'gkeyll_exe'     : 'rt_gk_d3d_iwl_2x2v_p1.sh',
        },
    },
    'olcf': {
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
        'setup'       : ['module load cray-python/3.11.7',
                        ],
        'app'         : None,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  Per-machine defaults.  Keyed by edge name (as reported by
#  ``bc.list_edges()``).  Used by ``psij:<name>`` (queue / account /
#  walltime / tunnel for submitting a child edge) and by ``compute:<name>``
#  (only the ``app`` block + ``gpus_per_node`` / ``cores_per_node`` are
#  read in that path).  Values are taken verbatim — no prompts.
# ─────────────────────────────────────────────────────────────────────────────

MACHINE_DEFAULTS = {
    'aurora': {
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
            'infer_dir'      : '/global/u2/m/merzky/MATEY',
            'infer_model_dir': '/global/cfs/projectdirs/amsc007/zhan1668/MATEY'
                               '/models/Dev_Fusion_DemoMay_toytestonly'
                               '/demo_nbatchsloc100/',
            'infer_xgc_dir'  : '/global/cfs/cdirs/amsc007/data/xgc'
                               '/d3d_174310.03500/',
            'gkeyll_dir'     : '/global/u2/m/merzky/gkeyll/amsc',
            'gkeyll_exe'     : 'rt_gk_d3d_iwl_2x2v_p1.sh',
        },
    },
    'odo': {
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
        'account'      : None,
        'queue_name'   : None,
        'qos'          : None,
        'walltime_min' : 30,
        'n_nodes'      : 1,
        'gpus_per_node': 1,
        'cores_per_node': 20,
        'constraint'   : None,
        'tunnel'       : 'none',
        'amsc_dir'     : None,
        'setup'        : None,
        # Per-machine slicing override (consumed by submit_rhapsody_workload
        # in place of the module-level ``SLICING``).  thinkie has a single
        # GPU and can't host the default 4-GPU-per-node horizontal split,
        # so everything runs on CPU here: matey/infer each take 1 core,
        # gkeyll takes the remaining 18.
        'slicing'      : {
            'mode': 'horizontal',
            'kinds': {
                'matey':  {'device': 'cpu', 'per_node': 1},
                'infer':  {'device': 'cpu', 'per_node': 1},
                'gkeyll': {'device': 'cpu', 'per_node': 'rest'},
            },
        },
        # Stand-in workloads via /bin/sleep so thinkie can drive the
        # whole pipeline without the real matey / gkeyll binaries.  The
        # ``<kind>_dir`` is just the cwd; ``<kind>_executable`` /
        # ``<kind>_arguments`` override the kind-specific arg builder.
        'app'          : {
            'matey_dir'        : '/tmp',
            'matey_executable' : '/bin/sleep',
            'matey_arguments'  : ['0.1'],
            'infer_dir'        : '/tmp',
            'infer_executable' : '/bin/sleep',
            'infer_arguments'  : ['0.2'],
            'gkeyll_dir'       : '/tmp',
            'gkeyll_executable': '/bin/sleep',
            'gkeyll_arguments' : ['0.3'],
        },
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
#  Output helpers.
#
#  ``step()`` prints one aligned, coloured line per coarse phase via rich.
#  ``abort()`` prints a red ABORT line and exits non-zero — used at every
#  fail-fast boundary so the user sees a clean error instead of a traceback.
# ─────────────────────────────────────────────────────────────────────────────

try:
    from rich.console  import Console
    from rich.progress import Progress, ProgressColumn, TextColumn
    from rich.text     import Text
    _console = Console()

    class _TwoBars(ProgressColumn):
        """Two side-by-side full-cell bars (blue submitted, green done).

        Both bars are ``bar_width`` cells wide.  Uses ``█`` (FULL BLOCK)
        for filled and ``░`` (LIGHT SHADE) for empty so the bars visually
        have the height of an uppercase character.  Reads custom task
        fields ``submitted`` and ``done`` (both seeded via ``add_task``).
        """

        def __init__(self, bar_width=20):
            super().__init__()
            self.bar_width = bar_width

        def _bar(self, value, total, color):
            n = int(self.bar_width * value / total) if total else 0
            t = Text()
            t.append('█' * n,                    style=color)
            t.append('░' * (self.bar_width - n), style='bar.back')
            return t

        def render(self, task):
            total     = task.total or 0
            submitted = task.fields.get('submitted', 0)
            done      = task.fields.get('done',      0)
            text = self._bar(submitted, total, 'blue')
            text.append('  ')
            text.append_text(self._bar(done, total, 'green'))
            return text

except ImportError:                              # pragma: no cover
    _console   = None
    Progress   = None
    TextColumn = None
    _TwoBars   = None

_TOTAL_STEPS = 7   # connect / pick / configure / submit / await / run / teardown

def step(idx, label, detail='', newline=True):
    end = '\n' if newline else ''
    if _console:
        _console.print(
            f'[cyan]step {idx}/{_TOTAL_STEPS}[/cyan]  '
            f'[bold]{label:<20}[/bold]  '
            f'[bright_white]{detail}[/bright_white]',
            end=end)
    else:
        print(f'step {idx}/{_TOTAL_STEPS}  {label:<20}  {detail}', end=end)

def abort(msg):
    """Print a red ABORT line and exit with status 1.  No traceback."""
    if _console:
        _console.print(f'[bold red]ABORT[/bold red]               [red]{msg}[/red]')
    else:
        print(f'ABORT  {msg}')
    sys.exit(1)


def _make_progress(n_matey, n_infer, n_gkeyll):
    """Build the rhapsody workload's progress display.

    Returns ``(progress, tids)`` where *progress* is a ``rich.Progress``
    (or ``None`` when rich is unavailable) and *tids* maps each active
    kind to its task id.  Each line shows two bars (blue=submitted,
    green=done) plus a numeric trailer.  Drive it with::

        progress.update(tids[kind], advance=1,
                        submitted=counts[kind]['submitted'],
                        done     =counts[kind]['done'],
                        failed   =counts[kind]['failed'])
    """
    if Progress is None or _console is None:
        return None, {}

    progress = Progress(
        TextColumn("  [cyan]{task.fields[label]:<9s}[/cyan]"),
        _TwoBars(bar_width=20),
        TextColumn(
            "[blue]{task.fields[submitted]:>6d}[/blue] sub  "
            "[green]{task.fields[done]:>6d}[/green] done  "
            "[red]{task.fields[failed]:>6d}[/red] fail  / "
            "[bright_white]{task.total:>6d}[/bright_white]"),
        console=_console,
    )
    tids = {}
    for kind, n in (('gkeyll', n_gkeyll),
                    ('matey',  n_matey),
                    ('infer',  n_infer)):
        if n > 0:
            tids[kind] = progress.add_task('', total=n, label=kind,
                                           submitted=0, done=0, failed=0)
    return progress, tids



# ─────────────────────────────────────────────────────────────────────────────
#  IRI launch path.
#
#  Steps:
#    1. Read the bearer token from ~/.amsc/token_<endpoint>.
#    2. iri_connect.connect(...) — creates a dynamic iri.<endpoint> plugin
#       on the bridge and returns an IRIInstanceClient bound to it.
#    3. Submit a job whose executable is radical-edge-wrapper.sh.  The job
#       will WS-connect back to the bridge; if --tunnel is set, the child
#       opens an outbound SSH tunnel to ``login_host`` first.
# ─────────────────────────────────────────────────────────────────────────────

def _validate_iri_cfg(endpoint, cfg):
    """Sanity-check the IRI_DEFAULTS entry before we try to launch."""
    if not cfg.get('account'):
        raise RuntimeError(f'IRI {endpoint}: account/project is required')
    if not cfg.get('home_dir'):
        raise RuntimeError(f'IRI {endpoint}: home_dir on target is required '
                           f'(used to resolve <home>/'
                           f'{cfg.get("amsc_dir") or ".amsc"}'
                           f'/ve/bin/radical-edge-wrapper.sh)')


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
#  ``cfg`` is a copy of ``MACHINE_DEFAULTS[edge_name]`` with ``executor``
#  spliced in.  ``submit_tunneled`` adds --tunnel / --tunnel-via to the
#  child argv automatically per ``cfg['tunnel']``.
# ─────────────────────────────────────────────────────────────────────────────
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
        # ``exclusive_node_use=True`` forces SLURM to allocate whole
        # nodes regardless of the ``ntasks / ntasks-per-node`` arithmetic
        # PsiJ derives from ``node_count``.  Without it, the trio
        # (nodes=N, ntasks=N, ppn=1) is internally consistent but slurm
        # site policy / job_submit hooks can collapse the allocation
        # to the smallest node count that satisfies ntasks -- which is
        # 1.  ``--exclusive`` short-circuits that.
        'resources'         : {'node_count'        : cfg['n_nodes'],
                               'exclusive_node_use': True},
        'environment'       : env,
    }

    res = psij.submit_tunneled(job_spec, executor=cfg['executor'],
                               tunnel=cfg['tunnel'])

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
                        poll=3.0, heartbeat=10.0):
    """Block until any name in *expected_names* appears in ``bc.list_edges()``.

    Returns the winning name, or raises TimeoutError after *timeout* seconds.
    Prints a heartbeat at most every ``heartbeat`` seconds so we don't
    spam the screen during long queue waits.
    """
    if not expected_names:
        raise RuntimeError('no expected edges — nothing to wait for')

    start_time = time.time()
    last_beat  = start_time
    try:
        while time.time() - start_time < timeout:
            live = set(bc.list_edges())
            for name in expected_names:
                if name in live:
                    return name
            time.sleep(poll)
            if time.time() - last_beat >= heartbeat:
                sys.stdout.write('.')
                sys.stdout.flush()
                last_beat = time.time()
        raise TimeoutError(f'no edge appeared within {timeout}s; '
                           f'expected one of {expected_names}')
    finally:
        sys.stdout.write('\n')
        sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  Rhapsody workload.
#
#  Submits matey + infer + gkeyll task families against the named edge
#  as a rhapsody backend.  Resources are carved up per ``SLICING`` (see
#  top of file) into per-kind pools: each kind gets its own per-host
#  affinity range (horizontal) or its own disjoint node subset
#  (vertical), and its own semaphore sized to the resulting cap.
#
#  Per-target requirements (see ``app`` field in IRI_DEFAULTS /
#  MACHINE_DEFAULTS):
#    * ``app.matey_dir / matey_model_dir / matey_xgc_dir``  — matey paths
#    * ``app.infer_dir / infer_model_dir / infer_xgc_dir``  — infer paths
#    * ``app.gkeyll_dir / gkeyll_exe``                      — gkeyll paths
#    * ``gpus_per_node`` / ``cores_per_node``               — device totals
#                                                             carved by SLICING
# ─────────────────────────────────────────────────────────────────────────────

def _compute_slices(slicing, n_hosts, gpus_per_node, cores_per_node, nodelist):
    """Carve the allocation into per-kind pools according to *slicing*.

    Returns ``{kind: {hosts, device, affinity_start, affinity_count, cap}}``.
    ``cap`` is total concurrency for that kind; a kind with zero cap is
    skipped at workload time.
    """
    mode  = slicing.get('mode')
    kinds = slicing.get('kinds', {})

    if mode == 'horizontal':
        # Every kind shares all nodes; affinity offsets stack per device.
        per_device_total = {'gpu': gpus_per_node, 'cpu': cores_per_node}
        per_node  = {}                              # kind -> int slots/host
        rest_kind = {'gpu': None, 'cpu': None}      # at most one rest per device
        used      = {'gpu': 0, 'cpu': 0}
        for kind, spec in kinds.items():
            dev = spec['device']
            pn  = spec.get('per_node', 0)
            if pn == 'rest':
                if rest_kind[dev] is not None:
                    raise RuntimeError(
                        f"SLICING: multiple 'rest' on device {dev!r} "
                        f"({rest_kind[dev]!r} and {kind!r})")
                rest_kind[dev] = kind
                per_node[kind] = None
            else:
                per_node[kind] = int(pn)
                used[dev]     += int(pn)
        for dev, kind in rest_kind.items():
            if kind is not None:
                per_node[kind] = max(0, per_device_total[dev] - used[dev])

        cursor = {'gpu': 0, 'cpu': 0}
        slices = {}
        for kind, spec in kinds.items():
            dev = spec['device']
            pn  = per_node[kind]
            slices[kind] = {
                'hosts'         : list(nodelist),
                'device'        : dev,
                'affinity_start': cursor[dev],
                'affinity_count': pn,
                'cap'           : n_hosts * pn,
            }
            cursor[dev] += pn
        return slices

    if mode == 'vertical':
        # Disjoint node subsets by weight; each kind owns its nodes fully.
        weights = {k: int(kinds[k].get('weight', 0)) for k in kinds}
        total_w = sum(weights.values()) or 1
        n_each  = {k: (n_hosts * w) // total_w for k, w in weights.items()}
        remainder = n_hosts - sum(n_each.values())
        # Hand out leftover nodes in declaration order — deterministic.
        for k in list(weights):
            if remainder <= 0:
                break
            n_each[k] += 1
            remainder -= 1

        cursor = 0
        slices = {}
        for kind, spec in kinds.items():
            n            = n_each[kind]
            dev          = spec['device']
            cap_per_host = gpus_per_node if dev == 'gpu' else cores_per_node
            hosts        = list(nodelist[cursor:cursor + n])
            cursor      += n
            slices[kind] = {
                'hosts'         : hosts,
                'device'        : dev,
                'affinity_start': 0,
                'affinity_count': cap_per_host,
                'cap'           : len(hosts) * cap_per_host,
            }
        return slices

    raise RuntimeError(
        f"SLICING.mode={mode!r} (expected 'horizontal' or 'vertical')")


def _render_slicing(slicing_mode, slices, gpus_per_node, cores_per_node,
                    nodelist, active_kinds):
    """ASCII visualization of the resource carve-up, wrapped in a titled
    panel.

    Horizontal (uniform per-node): one template + ``×N`` for the node
    count.  Vertical (disjoint node subsets, CPU-only by current design):
    one line per kind with its node range.

    Yellow is reserved for structural / non-task text; each kind gets
    its own colour (matey=magenta, infer=cyan, gkeyll=green).
    """
    if not _console:
        return

    from rich.panel import Panel

    glyph = {'matey': 'M', 'infer': 'I', 'gkeyll': 'G'}
    color = {'matey': 'magenta', 'infer': 'cyan', 'gkeyll': 'green'}
    Y     = 'yellow'

    lines = []

    if slicing_mode == 'horizontal':
        n_hosts = len(nodelist)
        lines.append(f'[{Y}]each of {n_hosts} nodes:[/{Y}]')

        gpu_slots = [None] * (gpus_per_node  or 0)
        cpu_slots = [None] * (cores_per_node or 0)
        for name in active_kinds:
            sl       = slices[name]
            slot_arr = gpu_slots if sl['device'] == 'gpu' else cpu_slots
            for i in range(sl['affinity_start'],
                           sl['affinity_start'] + sl['affinity_count']):
                if i < len(slot_arr):
                    slot_arr[i] = name

        def _row(label, slots):
            if not slots or not any(slots):
                return
            # Short rows (≤ 8 slots) list each slot; longer ones run-length
            # compress contiguous same-kind runs (typical for cpu(128)).
            if len(slots) <= 8:
                cells = ' '.join(
                    f'[{color[s]}]{glyph[s]}[/{color[s]}]' if s
                    else f'[{Y}]·[/{Y}]'
                    for s in slots)
            else:
                parts = []
                i = 0
                while i < len(slots):
                    k, j = slots[i], i
                    while j < len(slots) and slots[j] == k:
                        j += 1
                    run = j - i
                    if k:
                        parts.append(
                            f'[{color[k]}]{glyph[k]}[/{color[k]}] '
                            f'[{Y}]×{run}[/{Y}]')
                    else:
                        parts.append(f'[{Y}]·×{run}[/{Y}]')
                    i = j
                cells = '  '.join(parts)
            pad = f'{label}:'.ljust(9)
            lines.append(f'  [{Y}]{pad}[/{Y}]  {cells}')

        if gpus_per_node:
            _row(f'gpu({gpus_per_node})',  gpu_slots)
        if cores_per_node:
            _row(f'cpu({cores_per_node})', cpu_slots)

        title = 'horizontal slicing'

    elif slicing_mode == 'vertical':
        # Cursor accumulates across ALL declared kinds so displayed
        # (start..end) indices match the underlying nodelist offsets
        # even when some kinds are inactive but still consume nodes.
        cursor = 0
        for name, sl in slices.items():
            n = len(sl['hosts'])
            if name in active_kinds and n > 0:
                start, end = cursor, cursor + n - 1
                lines.append(
                    f'  [{color[name]}]{name:<7s}[/{color[name]}] '
                    f'[{Y}]×{n} nodes ({start}..{end})[/{Y}]')
            cursor += n

        title = 'vertical slicing'

    else:
        return

    if not lines:
        return

    from rich.padding import Padding
    from rich.table   import Table
    from rich.text    import Text

    panel = Panel('\n'.join(lines),
                  title=f'[{Y}]{title}[/{Y}]',
                  title_align='left',
                  border_style=Y,
                  expand=False)

    _console.print()                                   # empty line before

    if slicing_mode == 'horizontal':
        # M / I / G are opaque without a legend.  Render box + legend
        # side-by-side via an invisible 2-column grid; vertical mode has
        # full kind names inline so it doesn't need this.
        legend_md = '\n'.join(
            f'[{color[k]}]{glyph[k]}[/{color[k]}] [{Y}]=[/{Y}] '
            f'[{color[k]}]{k}[/{color[k]}]'
            for k in ('matey', 'infer', 'gkeyll')
            if k in active_kinds)
        grid = Table.grid(padding=(0, 3))
        grid.add_column()
        grid.add_column()
        grid.add_row(panel, Text.from_markup(legend_md))
        _console.print(Padding(grid, (0, 0, 0, 2)))
    else:
        _console.print(Padding(panel, (0, 0, 0, 2)))


async def submit_rhapsody_workload(bridge_url, edge_name, cfg, nodelist):
    """Submit the active task kinds (per ``KINDS``) via the named edge.

    All active kinds share one Session and run concurrently, each behind
    its own semaphore sized to the per-kind cap from
    ``_compute_slices(SLICING, ...)``.  Each task's policy pins it to a
    (host, gpu/cpu) slot via ``Policy.Placement.HOST_NAME`` so dragon's
    scheduler is forced onto the slot we picked.

    *nodelist* is the list of compute hostnames in the edge's allocation
    (from ``queue_info.nodelist()``).  Kinds whose slice yields zero cap
    or whose ``app`` paths are absent are silently skipped; bails when
    nothing remains.
    """
    app_cfg = (cfg or {}).get('app')
    if not app_cfg:
        raise RuntimeError(
            f"target {edge_name!r} has no 'app' config block — "
            "the rhapsody workload is not supported here.  Populate "
            "IRI_DEFAULTS / MACHINE_DEFAULTS['app'] for this target.")

    n_hosts        = len(nodelist) or 1
    gpus_per_node  = cfg.get('gpus_per_node')  or 0
    cores_per_node = cfg.get('cores_per_node') or 0
    # Per-machine ``slicing`` overrides the module-level default — used
    # by thinkie (CPU-only on a 1-GPU laptop) and any future per-target
    # tweak.  Falls back to the global SLICING for everyone else.
    slicing = cfg.get('slicing') or SLICING
    slices  = _compute_slices(slicing, n_hosts,
                              gpus_per_node, cores_per_node, nodelist)

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

    def _make_tasks(kind, n_tasks, executable, arguments, wd):
        sl    = slices[kind]
        hosts = sl['hosts']
        nh    = len(hosts) or 1
        pc    = sl['affinity_count'] or 1
        affinity_field = ('gpu_affinity' if sl['device'] == 'gpu'
                          else 'cpu_affinity')
        tasks = []
        for i in range(n_tasks):
            host = hosts[i % nh]
            slot = sl['affinity_start'] + ((i // nh) % pc)
            policy_kwargs = {
                'placement': Policy.Placement.HOST_NAME,
                'host_name': host,
                affinity_field: [slot],
            }
            tasks.append(ComputeTask(
                uid=f'{kind}.{i:04d}',
                executable=executable,
                arguments=arguments,
                capture_stdio=True,
                task_backend_specific_kwargs=_pack_psk(wd,
                                                Policy(**policy_kwargs)),
                _pickled_fields=['task_backend_specific_kwargs'],
            ))
        return tasks

    # Build per-kind (executable, arguments) — one loop over KINDS replaces
    # three near-identical if-blocks.  A kind is skipped when its slice cap
    # is zero, its task count is zero, ``<name>_dir`` is missing, or
    # (without an explicit ``<name>_executable`` override) its default
    # paths aren't all populated.
    tasks_by_kind = {}
    for name, n_tasks, default_paths, template in KINDS:
        sl = slices.get(name, {})
        if sl.get('cap', 0) <= 0 or n_tasks <= 0:
            continue
        if not app_cfg.get(f'{name}_dir'):
            continue
        has_override = bool(app_cfg.get(f'{name}_executable'))
        if not has_override and not all(app_cfg.get(k) for k in default_paths):
            continue
        app_dir = app_cfg[f'{name}_dir'].rstrip('/')
        wd      = f'{app_dir}/{RHAPSODY_WORK_SUBDIR}'
        if has_override:
            exe  = app_cfg[f'{name}_executable']
            args = list(app_cfg.get(f'{name}_arguments') or [])
        elif template == 'matey':
            exe  = f'{app_dir}/{MATEY_WRAPPER_NAME}'
            args = [
                'python', f'{app_dir}/examples/basic_infer.py',
                '--model_dir',  app_cfg['matey_model_dir'],
                '--use_ddp',
                '--on_perlmutter',
                '--AR',
                '--leadtime',   '5',
                '--newxgc_dir', app_cfg['matey_xgc_dir'],
            ]
        elif template == 'infer':
            exe  = f'{app_dir}/{INFER_WRAPPER_NAME}'
            args = [
                'python', f'{app_dir}/examples/basic_infer.py',
                '--model_dir',  app_cfg['infer_model_dir'],
                '--use_ddp',
                '--on_perlmutter',
                '--AR',
                '--leadtime',   '5',
                '--newxgc_dir', app_cfg['infer_xgc_dir'],
            ]
        else:  # 'gkeyll'
            exe  = f'{app_dir}/{app_cfg["gkeyll_exe"]}'
            args = []
        tasks_by_kind[name] = _make_tasks(name, n_tasks, exe, args, wd)

    if not tasks_by_kind:
        raise RuntimeError(
            f"target {edge_name!r}: nothing to run.  Need a non-zero "
            "SLICING cap and matching app paths for at least one kind.")

    _render_slicing(slicing.get('mode'), slices,
                    gpus_per_node, cores_per_node, nodelist,
                    list(tasks_by_kind.keys()))

    backend = await rhapsody.get_backend(
        'edge', bridge_url=bridge_url, edge_name=edge_name)

    counts = {name: {'submitted': 0, 'done': 0, 'failed': 0}
              for name in tasks_by_kind}

    # ``Session(work_dir=…)`` is a client-side setting (rhapsody calls
    # ``os.makedirs(backend._work_dir)`` locally) — we can't pass the
    # remote per-kind paths there.  Per-task ``cwd`` rides through the
    # ``process_template`` instead.
    progress, tids = _make_progress(
        len(tasks_by_kind.get('matey',  ())),
        len(tasks_by_kind.get('infer',  ())),
        len(tasks_by_kind.get('gkeyll', ())))

    async with Session(backends=[backend]) as session:
        sems = {name: asyncio.Semaphore(slices[name]['cap'] or 1)
                for name in tasks_by_kind}

        async def run_one(task, kind):
            async with sems[kind]:
                await session.submit_tasks([task])
                counts[kind]['submitted'] += 1
                if progress and kind in tids:
                    progress.update(tids[kind],
                                    submitted=counts[kind]['submitted'])
                try:
                    await task
                    counts[kind]['done'] += 1
                except BaseException:
                    counts[kind]['failed'] += 1
                if progress and kind in tids:
                    c = counts[kind]
                    progress.update(tids[kind], advance=1,
                                    done=c['done'], failed=c['failed'])

        coros = [run_one(t, name)
                 for name, tasks in tasks_by_kind.items()
                 for t in tasks]

        if progress:
            _console.print()
            with progress:
                await asyncio.gather(*coros, return_exceptions=True)
            _console.print()
        else:
            await asyncio.gather(*coros, return_exceptions=True)


# ─────────────────────────────────────────────────────────────────────────────
#  Teardown — only touch resources THIS SCRIPT created.
# ─────────────────────────────────────────────────────────────────────────────

def teardown(bc, created):
    """Cancel jobs we submitted and disconnect IRI endpoints we connected.

    All output is detail under the step 7 header; the step trace itself is
    printed by the caller.  Per-item failures are non-fatal — we always
    push through to the next item.
    """
    # 1. Cancel IRI jobs
    for c in created:
        if c['kind'] != 'iri':
            continue
        try:
            c['iri'].cancel_job(c['resource_id'], c['job_id'])
        except Exception as exc:
            print(f'  could not cancel IRI job {c["job_id"]}: {exc}')

    # 2. Cancel PsiJ jobs
    for c in created:
        if c['kind'] != 'psij':
            continue
        try:
            c['psij'].cancel_job(c['job_id'])
        except Exception as exc:
            print(f'  could not cancel PsiJ job {c["job_id"]}: {exc}')

    # 3. Disconnect IRI endpoints
    iri_eps = {c['endpoint'] for c in created if c['kind'] == 'iri'}
    if iri_eps:
        cx = bc.get_edge_client('bridge').get_plugin('iri_connect')
        for ep in iri_eps:
            try:
                cx.disconnect(ep)
            except Exception as exc:
                print(f'  could not disconnect IRI {ep}: {exc}')


# ─────────────────────────────────────────────────────────────────────────────
#  Single-target driver — non-interactive, fail-fast, 7-step trace.
#
#  ``kind`` selects the launch path: ``psij`` (submit a child edge via an
#  existing login-edge), ``iri`` (submit via an IRI endpoint), or
#  ``compute`` (re-use an already-connected compute/standalone edge).
#  Every error boundary calls ``abort()`` so the user sees a one-line
#  reason, not a Python traceback.
# ─────────────────────────────────────────────────────────────────────────────

_VALID_KINDS = ('psij', 'iri', 'compute')


def _parse_target_arg(arg):
    """Return ``(kind, name)`` from a ``<kind>:<name>`` string.

    With ``arg is None`` the default ``('psij', 'perlmutter')`` is
    returned — matches the historical demo-mode default.
    """
    if not arg:
        return 'psij', 'perlmutter'
    if ':' not in arg:
        abort(f"target argument must be '<kind>:<name>': got {arg!r}")
    kind, _, name = arg.partition(':')
    if kind not in _VALID_KINDS:
        abort(f"target kind must be one of {_VALID_KINDS}: got {kind!r}")
    if not name:
        abort(f'empty name in target argument {arg!r}')
    return kind, name


def _flock_or_exit():
    """Single-instance guard via flock(/tmp/amsc.lock).

    Concurrent amsc.py runs interleave their log output and step on each
    other's plugin sessions; refuse to start a second one.  The lock is
    held until this process exits (kernel auto-releases on close).
    """
    import fcntl
    _lock = open('/tmp/amsc.lock', 'w')
    try:
        fcntl.flock(_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit('another amsc.py is already running; kill it first.')
    return _lock


def _resolve_executor(bc, edge_name):
    """Ask the edge's sysinfo plugin what PsiJ executor it expects."""
    try:
        info = bc.get_edge_client(edge_name).get_plugin('sysinfo').host_role()
        return info.get('psij_executor', 'local')
    except Exception:
        return 'local'


def _step_configure(cfg):
    """Step 3: one-line ``configure`` summary derived from cfg."""
    qos_str = f', qos={cfg["qos"]}' if cfg.get('qos') else ''
    step(3, 'configure',
         f'{cfg.get("n_nodes", "?")} node x '
         f'{cfg.get("gpus_per_node", "?")} gpu x '
         f'{cfg.get("cores_per_node", "?")} core, '
         f'{cfg.get("walltime_min", "?")}m walltime, '
         f'queue={cfg.get("queue_name", "?")}{qos_str}')


def _step_run(bc, bridge_url, edge_name, cfg):
    """Steps 6 + the actual workload run.

    Resolves the nodelist via the edge's queue_info plugin, prints the
    step 6 summary, then calls ``submit_rhapsody_workload``.  When
    queue_info is absent or reports an empty allocation (workstation /
    no batch system case), falls back to a single-node ``['localhost']``
    nodelist so dragon's HOST_NAME placement still has a target to bind.
    """
    try:
        nodelist = bc.get_edge_client(edge_name) \
                     .get_plugin('queue_info').nodelist()
    except Exception:
        nodelist = []
    if not nodelist:
        nodelist = ['localhost']
    n_hosts = len(nodelist)
    slicing = cfg.get('slicing') or SLICING
    slices  = _compute_slices(slicing, n_hosts,
                              cfg.get('gpus_per_node')  or 0,
                              cfg.get('cores_per_node') or 0,
                              nodelist)
    step(6, 'run rhapsody',
         f'{n_hosts} hosts  '
         f'matey {N_MATEY_TASKS} (cap {slices["matey"]["cap"]})  '
         f'infer {N_INFER_TASKS} (cap {slices["infer"]["cap"]})  '
         f'gkeyll {N_GKEYLL_TASKS} (cap {slices["gkeyll"]["cap"]})')
    try:
        asyncio.run(submit_rhapsody_workload(
            bridge_url, edge_name, cfg, nodelist))
    except Exception as exc:
        abort(f'workload failed: {exc}')


def _apply_cli_overrides(cfg, mode, n_nodes):
    """Apply optional CLI overrides onto a freshly-loaded target ``cfg``.

    ``mode`` (``'horizontal'`` / ``'vertical'`` / None) replaces the
    slicing mode in whichever slicing dict the target uses (per-cfg
    override if present, else module-level ``SLICING``).  ``n_nodes``
    (int / None) replaces ``cfg['n_nodes']``.  Either may be None to
    leave the corresponding cfg field untouched.
    """
    if n_nodes is not None:
        cfg['n_nodes'] = n_nodes
    if mode:
        base = cfg.get('slicing') or SLICING
        cfg['slicing'] = {**base, 'mode': mode}


def _main_target(bc, bridge_url, kind, name,
                 slicing_mode=None, n_nodes=None):
    """Run the workload against ``<kind>:<name>``.

    All three branches share the step 3/6/7 helpers; only the launch
    (steps 4-5) differs:

      - ``psij``    : submit a child edge via the login-edge's PsiJ plugin
      - ``iri``     : submit via the named IRI endpoint
      - ``compute`` : re-use the named edge directly (no submission)
    """
    step(1, 'connect bridge', bridge_url)

    live_edges = set(bc.list_edges())
    created    = []

    if kind == 'psij':
        if name not in live_edges:
            abort(f"no edge {name!r} connected to bridge.  "
                  f"Start the parent edge first.")
        if name not in MACHINE_DEFAULTS:
            abort(f"no MACHINE_DEFAULTS entry for {name!r}")
        if 'psij' not in bc.get_edge_client(name).list_plugins():
            abort(f"edge {name!r} has no psij plugin")

        executor = _resolve_executor(bc, name)
        step(2, 'pick target', f'{name} (psij/{executor})')

        cfg = dict(MACHINE_DEFAULTS[name])
        cfg['executor'] = executor
        _apply_cli_overrides(cfg, slicing_mode, n_nodes)
        _step_configure(cfg)

        try:
            try:
                rec = launch_psij(bc, name, cfg, bridge_url)
            except Exception as exc:
                abort(f'launch_psij failed: {exc}')
            created.append(rec)
            step(4, 'submit child edge',
                 f'job={rec["job_id"][:8]}…  edge={rec["edge_name"]}',
                 newline=False)

            t0 = time.time()
            try:
                first = wait_for_first_edge(bc, [rec['edge_name']])
            except Exception as exc:
                abort(f'wait_for_first_edge failed: {exc}')
            step(5, 'await child edge', f'up after {int(time.time() - t0)}s')

            _step_run(bc, bridge_url, first, rec.get('cfg') or cfg)
        finally:
            step(7, 'teardown', f'cancelling {len(created)} psij job(s)')
            teardown(bc, created)

    elif kind == 'iri':
        if name not in IRI_DEFAULTS:
            abort(f"no IRI_DEFAULTS entry for {name!r}")
        step(2, 'pick target', f'iri:{name}')

        cfg = dict(IRI_DEFAULTS[name])
        try:
            _validate_iri_cfg(name, cfg)
        except Exception as exc:
            abort(str(exc))
        _apply_cli_overrides(cfg, slicing_mode, n_nodes)
        _step_configure(cfg)

        try:
            try:
                rec = launch_iri(bc, name, cfg, bridge_url)
            except Exception as exc:
                abort(f'launch_iri failed: {exc}')
            created.append(rec)
            step(4, 'submit child edge',
                 f'job={rec["job_id"][:8]}…  edge={rec["edge_name"]}',
                 newline=False)

            t0 = time.time()
            try:
                first = wait_for_first_edge(bc, [rec['edge_name']])
            except Exception as exc:
                abort(f'wait_for_first_edge failed: {exc}')
            step(5, 'await child edge', f'up after {int(time.time() - t0)}s')

            _step_run(bc, bridge_url, first, rec.get('cfg') or cfg)
        finally:
            step(7, 'teardown', f'cancelling {len(created)} iri job(s)')
            teardown(bc, created)

    elif kind == 'compute':
        if name not in live_edges:
            abort(f"no edge {name!r} connected to bridge")
        if 'rhapsody' not in bc.get_edge_client(name).list_plugins():
            abort(f"edge {name!r} has no rhapsody plugin")
        if name not in MACHINE_DEFAULTS:
            abort(f"no MACHINE_DEFAULTS entry for {name!r} (need 'app' block)")
        step(2, 'pick target', f'{name} (compute)')

        cfg = dict(MACHINE_DEFAULTS[name])
        _apply_cli_overrides(cfg, slicing_mode, n_nodes)
        _step_configure(cfg)

        step(4, 'submit child edge', 'reusing existing edge')
        step(5, 'await child edge',  'already up')
        _step_run(bc, bridge_url, name, cfg)
        step(7, 'teardown',          'nothing to cancel')


# ─────────────────────────────────────────────────────────────────────────────
#  Main — parse arg, dispatch.
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Top-level driver.  Synchronous on purpose: only the rhapsody
    workload body needs an event loop, and that's spun up explicitly
    with ``asyncio.run()`` inside ``_step_run``."""
    _lock = _flock_or_exit()  # noqa: F841 — held for the process lifetime

    if len(sys.argv) > 4:
        abort(f'expected at most 3 arguments (got {len(sys.argv) - 1})')

    # Positional args dispatched by content (any order):
    #   target  : ``<kind>:<name>``       (e.g. ``psij:perlmutter``)
    #   mode    : ``horizontal`` / ``vertical``
    #   n_nodes : positive integer
    target_arg = slicing_mode = n_nodes = None
    for a in sys.argv[1:]:
        if ':' in a and target_arg is None:
            target_arg = a
        elif a in ('horizontal', 'vertical') and slicing_mode is None:
            slicing_mode = a
        elif a.isdigit() and n_nodes is None:
            n_nodes = int(a)
            if n_nodes <= 0:
                abort(f'n_nodes must be positive: got {n_nodes!r}')
        else:
            abort(f'unrecognized or duplicate argument: {a!r}')

    kind, name = _parse_target_arg(target_arg)

    # BridgeClient self-resolves URL + cert via radical.edge.utils
    # (CLI > env > file).
    bc         = BridgeClient()
    bridge_url = bc.url
    try:
        _main_target(bc, bridge_url, kind, name,
                     slicing_mode=slicing_mode, n_nodes=n_nodes)
    finally:
        bc.close()
        print()


if __name__ == '__main__':
    main()
