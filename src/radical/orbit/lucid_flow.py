
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'

'''
LUCID end-to-end flow: Cell Painting analysis on an HPC endpoint via ORBIT.

One run stages image data from the LUCID Globus collection to the target
endpoint's site (NERSC DTN), runs one CellProfiler task per selected well
through the endpoint's ``rhapsody`` plugin, packages the results (tarball,
per-well counts, a contrast-stretched montage and preview crops), stages them
back to the LUCID collection under ``/tmp_radical/<run-id>/`` and fetches the
previews to the caller via the ``staging`` plugin.

The flow is a client: it drives the ``globus``, ``rhapsody`` and ``staging``
plugins of the target endpoint through an :class:`EndpointRuntime`
(``get_plugin`` / ``topology``).  Both ``examples/lucid_e2e_demo.py`` and the
``lucid`` portal plugin run it.

Only the sample ANL week-1 P1 plate is wired up: it carries channels 4 and 5,
so ch4 is fed as DNA and ch5 fills the other four Cell Painting slots -- an
infrastructure demo, not a scientific result.
'''

import json
import logging
import os
import re
import secrets
import threading
import time

from pathlib import Path
from typing  import Any, Callable, Dict, List, Optional

log = logging.getLogger('radical.orbit')


LUCID_COLLECTION = '54387d00-a525-11f0-8d5c-0affdd0cd947'  # LUCID-Experimental-Data
NERSC_DTN        = '9d6d994a-6d04-11e5-ba46-22000b92c6ec'  # NERSC DTN
GLOBUS_CLIENT_ID = '8b84fc2d-49e9-49ea-b54d-b3a29a70cf31'  # get_globus_token.py
TOKEN_FILE       = '~/.globus/auth_tokens.json'
CP_IMAGE         = 'docker:cellprofiler/cellprofiler:4.2.6'

DEFAULTS = {
    'collection'   : LUCID_COLLECTION,
    'pipeline_path': '/example_pipeline/',
    'images_path'  : '/ANL_Week1_3DosePlates/staged/P1/Images',
    'out_base'     : '/tmp_radical',
    'field'        : 'f01p05',                       # field 1, plane 5
    'analysis'     : 'cropped_cells',
    'backend'      : 'dragon_v3',
}

ROWS  = 8
COLS  = 12
WELLS = [f'r{r:02d}c{c:02d}' for r in range(1, ROWS + 1)
                             for c in range(1, COLS + 1)]

# Analysis codes offered to the user.  Only ``cropped_cells`` is wired up;
# the others are placeholders for the portal (shown greyed out).
ANALYSES = [
    {'id'      : 'cropped_cells',
     'label'   : 'Cell Painting: cropped cells (CellProfiler 4.2.6)',
     'pipeline': 'cropped_cells.cppipe',
     'enabled' : True},
    {'id'      : 'h2ax_foci',
     'label'   : 'gamma-H2AX foci per nucleus (pending code)',
     'enabled' : False},
    {'id'      : 'nuclei_count',
     'label'   : 'Nuclei count and confluency (planned)',
     'enabled' : False},
    {'id'      : 'cp_embedding',
     'label'   : 'Cell Painting image embeddings (planned)',
     'enabled' : False},
]

NEEDED_PLUGINS = ('globus', 'rhapsody', 'staging')
GLOBUS_TIMEOUT = 2 * 3600       # give up (and cancel) a transfer after this
PHASES         = ('preflight', 'stage_in', 'compute', 'package', 'stage_out',
                  'fetch')
N_PREVIEW      = 8


class LucidError(RuntimeError):
    pass


# ------------------------------------------------------------------------------
#
def transfer_refresh_token(path: str = TOKEN_FILE) -> str:
    '''Globus Transfer refresh token written by ``get_globus_token.py``.'''
    path = os.path.expanduser(path)
    try:
        with open(path) as fin:
            data = json.load(fin)
        return next(t['refresh_token'] for t in data.get('other_tokens', [])
                    if t.get('resource_server') == 'transfer.api.globus.org')
    except (OSError, ValueError, StopIteration, KeyError) as e:
        raise LucidError(f'no Globus Transfer refresh token in {path} -- '
                         'run get_globus_token.py first') from e


def candidate_endpoints(topology: Dict[str, Any]) -> List[str]:
    '''Endpoints serving every plugin the flow needs.'''
    return sorted(name for name, info in (topology or {}).items()
                  if (info or {}).get('role') == 'endpoint'
                  and set(NEEDED_PLUGINS) <= set((info or {}).get('plugins') or {}))


def get_analysis(analysis_id: str) -> Dict[str, Any]:
    for a in ANALYSES:
        if a['id'] == analysis_id:
            if not a['enabled']:
                raise LucidError(f'analysis {analysis_id!r} is not available')
            return a
    raise LucidError(f'unknown analysis {analysis_id!r}')


# Caller-supplied strings end up in Globus requests and (via the run README)
# in a remote shell script: accept plain absolute paths and UUIDs only.
_PATH_RE = re.compile(r'^/[A-Za-z0-9._/-]*$')
_UUID_RE = re.compile(r'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')


def validate_path(path: str, what: str) -> str:
    if not isinstance(path, str) or not _PATH_RE.match(path) \
            or '..' in path.split('/'):
        raise LucidError(f'invalid {what}: {path!r} (absolute path of '
                         '[A-Za-z0-9._/-] expected)')
    return path


def validate_collection(uuid: str) -> str:
    if not isinstance(uuid, str) or not _UUID_RE.match(uuid):
        raise LucidError(f'invalid collection id: {uuid!r}')
    return uuid


def new_run_id() -> str:
    return time.strftime('e2e-%Y%m%d-%H%M%S-') + secrets.token_hex(2)


RUN_ID_RE = re.compile(r'^e2e-\d{8}-\d{6}(-[0-9a-f]{4})?$')


def validate_wells(wells: List[str]) -> List[str]:
    bad = [w for w in wells if w not in WELLS]
    if bad:
        raise LucidError(f'invalid wells: {bad[:5]}')
    if not wells:
        raise LucidError('no wells selected')
    return [w for w in WELLS if w in set(wells)]          # plate order


def _kv(text: str) -> Dict[str, str]:
    '''``key=value`` tokens of a task's stdout.'''
    out: Dict[str, str] = {}
    for tok in text.split():
        if '=' in tok:
            k, v = tok.split('=', 1)
            out[k] = v
    return out


# ------------------------------------------------------------------------------
# Remote scripts (bash on the endpoint; python inside the CellProfiler image)
#
CP_TASK = '''
set -e
in={run}/work/{s}; out={run}/out/{s}
mkdir -p $in $out {run}/logs
cp {run}/images/{s}-ch4sk1fk1fl1.tiff $in/dna.tiff
for n in rna agp er mito; do cp {run}/images/{s}-ch5sk1fk1fl1.tiff $in/$n.tiff; done
OMP_NUM_THREADS=2 shifter --image={image} cellprofiler -c -r \\
    -p {run}/pipeline/{pipeline} -i $in -o $out \\
    > {run}/logs/{s}.log 2>&1
rm -rf $in
echo "host=$(hostname) files=$(ls $out | wc -l)"
'''

PREVIEW = '''
import glob, os, sys
import numpy as np
import imageio.v2 as iio
out, res, n_prev = sys.argv[1], sys.argv[2], int(sys.argv[3])
wells = sorted(glob.glob(out + '/*/'), key=lambda d: -len(glob.glob(d + '*.png')))
pngs  = sorted(glob.glob(wells[0] + '*.png'), key=os.path.getsize, reverse=True)[:16]
T     = 160
def stretch(p):
    a = iio.imread(p).astype(float)
    a = np.stack([a] * 3, -1) if a.ndim == 2 else a[..., :3]
    lo, hi = np.percentile(a, 1), np.percentile(a, 99.5)
    return (np.clip((a - lo) / max(hi - lo, 1e-6), 0, 1) * 255).astype(np.uint8)
grid = np.zeros((4 * T, 4 * T, 3), np.uint8)
os.makedirs(res + '/preview', exist_ok=True)
for i, p in enumerate(pngs):
    a = stretch(p)
    if i < n_prev:
        iio.imwrite('%s/preview/cell_%02d.png' % (res, i), a)
    y, x = max(0, (a.shape[0] - T) // 2), max(0, (a.shape[1] - T) // 2)
    a = a[y:y + T, x:x + T]
    r, c = divmod(i, 4)
    grid[r * T:r * T + a.shape[0], c * T:c * T + a.shape[1]] = a
iio.imwrite(res + '/montage.png', grid)
print('montage_well=' + os.path.basename(wells[0].rstrip('/')))
'''

PACKAGE = '''
set -e
R={run}/results; mkdir -p $R; cd {run}/out
echo well,cropped_cells > $R/cells_per_well.csv
for d in */; do echo "${{d%/}},$(ls $d | wc -l)" >> $R/cells_per_well.csv; done
tar czf $R/cropped_cells.tgz */
cp {run}/pipeline/{pipeline} $R/
shifter --image={image} python - {run}/out $R {n_prev} <<'PY'
{preview}
PY
cat > $R/README.txt <<'TXT'
LUCID ORBIT end-to-end run {run_id}
Data:      {images_path}, field/plane {field}, {n} wells
           (ch4 fed as DNA, ch5 into the other four Cell Painting slots)
Pipeline:  {pipeline}, CellProfiler 4.2.6 (shifter)
Execution: ORBIT rhapsody plugin, endpoint {endpoint}
Staging:   ORBIT globus plugin, LUCID collection <-> NERSC DTN
NOTE: channel substitution makes this an infrastructure demo, not a result.
TXT
'''


# ------------------------------------------------------------------------------
#
class LucidRun:
    '''One end-to-end run.  :meth:`run` blocks; progress goes to ``on_update``
    as a snapshot of :attr:`state` after every change.'''

    def __init__(self, runtime, endpoint: str, wells: List[str],
                 refresh_token: str,
                 local_dir    : str,
                 collection   : str = DEFAULTS['collection'],
                 pipeline_path: str = DEFAULTS['pipeline_path'],
                 images_path  : str = DEFAULTS['images_path'],
                 out_base     : str = DEFAULTS['out_base'],
                 field        : str = DEFAULTS['field'],
                 analysis     : str = DEFAULTS['analysis'],
                 backend      : str = DEFAULTS['backend'],
                 run_id       : Optional[str] = None,
                 on_update    : Optional[Callable[[dict], None]] = None,
                 poll         : float = 3.0):

        self._rt        = runtime
        self._endpoint  = endpoint
        self._wells     = validate_wells(list(wells))
        self._analysis  = get_analysis(analysis)
        self._refresh   = refresh_token
        self._local     = Path(local_dir)
        self._coll      = validate_collection(collection)
        self._pipe_path = validate_path(pipeline_path, 'pipeline path').rstrip('/') + '/'
        self._img_path  = validate_path(images_path, 'images path').rstrip('/')
        self._out_base  = validate_path(out_base, 'output base').rstrip('/')
        self._field     = field
        self._backend   = backend
        self._on_update = on_update
        self._poll      = poll
        self._lock      = threading.Lock()
        self._cancel    = threading.Event()

        self.run_id = run_id or new_run_id()
        self.state  = {
            'run_id'  : self.run_id,
            'endpoint': endpoint,
            'analysis': self._analysis['id'],
            'wells'   : self._wells,
            'status'  : 'new',          # new | running | done | failed
            'phase'   : None,
            'phases'  : {p: {'state': 'pending', 'secs': None} for p in PHASES},
            'tasks'   : {'total': len(self._wells), 'submitted': 0,
                         'executing': 0, 'done': 0, 'failed': 0},
            'stats'   : {},
            'target'  : f'{self._out_base}/{self.run_id}/',
            'images'  : [],             # local paths: montage first
            'failed_wells': {},
            'error'   : None,
            'started' : None,
            'finished': None,
        }

    # --------------------------------------------------------------------------
    #
    def cancel(self) -> None:
        '''Ask a running :meth:`run` to stop at its next poll.'''
        self._cancel.set()

    def _sleep(self):
        if self._cancel.wait(self._poll):
            raise LucidError('run cancelled')

    def snapshot(self) -> dict:
        with self._lock:
            return json.loads(json.dumps(self.state))

    def _update(self, **kw):
        with self._lock:
            self.state.update(kw)
        if self._on_update:
            try:
                self._on_update(self.snapshot())
            except Exception:
                log.exception('[lucid] on_update callback failed')

    def _phase(self, name, st, secs=None):
        with self._lock:
            self.state['phases'][name] = {'state': st, 'secs': secs}
            if st == 'running':
                self.state['phase'] = name
        self._update()

    # --------------------------------------------------------------------------
    #
    def run(self) -> dict:
        self._update(status='running', started=time.time())
        sessions = []
        phase    = None
        t0       = time.time()
        try:
            globus = self._rt.get_plugin(self._endpoint, 'globus',
                                         refresh_token=self._refresh,
                                         client_id=GLOBUS_CLIENT_ID)
            sessions.append(globus)
            rh     = self._rt.get_plugin(self._endpoint, 'rhapsody',
                                         backends=[self._backend])
            sessions.append(rh)
            stage  = self._rt.get_plugin(self._endpoint, 'staging')
            sessions.append(stage)

            for phase, func in (('preflight', self._preflight),
                                ('stage_in',  self._stage_in),
                                ('compute',   self._compute),
                                ('package',   self._package),
                                ('stage_out', self._stage_out),
                                ('fetch',     self._fetch)):
                t0 = time.time()
                self._phase(phase, 'running')
                func(globus, rh, stage)
                self._phase(phase, 'done', round(time.time() - t0, 1))
            phase = None
            self._update(status='done')

        except Exception as e:
            log.exception('[lucid] run %s failed', self.run_id)
            if phase:
                self._phase(phase, 'failed', round(time.time() - t0, 1))
            self._update(status='failed', error=str(e))

        finally:
            # an open rhapsody session keeps its backend alive on the endpoint
            for s in sessions:
                try:
                    s.close()
                except Exception as e:
                    log.warning('[lucid] session close failed: %s', e)
            self._update(finished=time.time())

        return self.snapshot()

    # --------------------------------------------------------------------------
    #
    def _shell(self, rh, cmds: Dict[str, str], timeout: float,
               progress: bool = False) -> Dict[str, dict]:
        '''Run ``{name: bash}`` as rhapsody tasks; return ``{name: task}``.'''
        tasks = [{'executable': '/bin/bash',
                  'arguments' : ['-lc', 'unset PYTHONPATH\n' + cmd]}
                 for cmd in cmds.values()]
        uids  = [t['uid'] for t in rh.submit_tasks(tasks)]
        names = dict(zip(uids, cmds))
        final = {'DONE', 'FAILED', 'CANCELED', 'CANCELLED'}
        end   = time.time() + timeout
        while True:
            # one call returns every task with state and stdout
            listed = {t['uid']: t for t in rh.list_tasks().get('tasks', [])
                      if t.get('uid') in names}
            states = [listed.get(u, {}).get('state') for u in uids]
            if progress:
                n_run  = sum(1 for s in states if s == 'RUNNING')
                n_done = sum(1 for s in states if s == 'DONE')
                n_fail = sum(1 for s in states if s in final - {'DONE'})
                self._update(tasks={'total'    : len(uids),
                                    'submitted': len(uids) - n_run - n_done - n_fail,
                                    'executing': n_run,
                                    'done'     : n_done,
                                    'failed'   : n_fail})
            if all(s in final for s in states):
                return {names[u]: listed[u] for u in uids}
            if time.time() > end:
                raise LucidError(f'tasks did not finish within {timeout} s')
            self._sleep()

    @staticmethod
    def _check(task: dict, what: str) -> str:
        if task.get('state') != 'DONE':
            raise LucidError(f'{what} failed ({task.get("state")}): '
                             f'{(task.get("stderr") or task.get("error") or "")[-500:]}')
        return task.get('stdout') or ''

    def _globus_wait(self, globus, task_id: str, key: str,
                     timeout: float = GLOBUS_TIMEOUT) -> dict:
        # poll: the plugin's task_wait outlives the routed-RPC timeout.
        # Globus keeps a task ACTIVE while it retries recoverable errors and
        # turns it INACTIVE when the credential expires -- neither ends on
        # its own, so give up on INACTIVE, on timeout, or on cancel.
        end = time.time() + timeout
        try:
            while True:
                task = globus.get_task(task_id)
                with self._lock:
                    self.state['stats'][key] = {
                        'files': task.get('files_transferred') or 0,
                        'bytes': task.get('bytes_transferred') or 0,
                        'total': task.get('files')}
                self._update()
                status = task.get('status')
                detail = task.get('nice_status_details') or task.get('nice_status')
                if status == 'SUCCEEDED':
                    return task
                if status == 'FAILED':
                    raise LucidError(f'{key} transfer failed: {detail}')
                if status == 'INACTIVE':
                    raise LucidError(f'{key} transfer inactive (expired '
                                     f'credential?): {detail}')
                if time.time() > end:
                    raise LucidError(f'{key} transfer did not finish within '
                                     f'{timeout} s (last: {detail})')
                self._sleep()
        except Exception:
            try:
                globus.cancel_task(task_id)
            except Exception as e:
                log.warning('[lucid] cancel of transfer %s failed: %s',
                            task_id, e)
            raise

    # --------------------------------------------------------------------------
    #
    def _preflight(self, globus, rh, stage):
        pre = self._shell(rh, {'pre': f'echo scratch=$SCRATCH\n'
                                      f'shifterimg lookup {CP_IMAGE} >/dev/null'
                                      f' || shifterimg pull {CP_IMAGE} >/dev/null'},
                          900)['pre']
        scratch = _kv(self._check(pre, 'preflight')).get('scratch')
        if not scratch:
            raise LucidError('endpoint reports no $SCRATCH')
        # previews come back through staging at the very end: fail now, not
        # after an hour of compute, if it may not read scratch
        try:
            stage.list(scratch)
        except Exception as e:
            raise LucidError(f'staging plugin cannot access {scratch} -- start '
                             'the endpoint with RADICAL_ORBIT_SCRATCH_BASE='
                             f'$SCRATCH ({e})') from e
        self._run_dir = f'{scratch}/lucid-e2e/runs/{self.run_id}'
        self._update(run_dir=self._run_dir)

    def _stage_in(self, globus, rh, stage):
        items = [{'source'   : self._pipe_path,
                  'destination': f'{self._run_dir}/pipeline/',
                  'recursive': True}]
        for w in self._wells:
            for ch in ('ch4', 'ch5'):
                name = f'{w}{self._field}-{ch}sk1fk1fl1.tiff'
                items.append({'source'     : f'{self._img_path}/{name}',
                              'destination': f'{self._run_dir}/images/{name}'})
        sub = globus.submit_transfer(self._coll, NERSC_DTN, items,
                                     label=f'lucid {self.run_id} stage-in')
        self._globus_wait(globus, sub['task_id'], 'stage_in')

    def _compute(self, globus, rh, stage):
        cmds = {w: CP_TASK.format(run=self._run_dir, s=w + self._field,
                                  image=CP_IMAGE,
                                  pipeline=self._analysis['pipeline'])
                for w in self._wells}
        res    = self._shell(rh, cmds, 3600, progress=True)
        nodes  = set()
        files  = 0
        failed = {}
        for w, t in res.items():
            if t.get('state') != 'DONE':
                err = (t.get('stderr') or t.get('error') or '').strip()
                failed[w] = f'{t.get("state")}: {err[-200:]}'
                continue
            kv = _kv(t.get('stdout') or '')
            nodes.add(kv.get('host'))
            files += int(kv.get('files', 0))
        with self._lock:
            self.state['stats']['compute'] = {
                'tasks': len(res), 'done': len(res) - len(failed),
                'failed': len(failed), 'nodes': len(nodes - {None}),
                'files': files}
            self.state['failed_wells'] = failed
        self._update()
        if len(failed) == len(res):
            raise LucidError(f'all {len(res)} tasks failed -- see '
                             f'{self._run_dir}/logs/')

    def _package(self, globus, rh, stage):
        pkg = self._shell(rh, {'pkg': PACKAGE.format(
                    run=self._run_dir, run_id=self.run_id, image=CP_IMAGE,
                    pipeline=self._analysis['pipeline'],
                    images_path=self._img_path, field=self._field,
                    n=self.state['stats']['compute']['done'],
                    endpoint=self._endpoint, n_prev=N_PREVIEW,
                    preview=PREVIEW)}, 900)['pkg']
        self._check(pkg, 'package')

    def _stage_out(self, globus, rh, stage):
        sub = globus.submit_transfer(
                    NERSC_DTN, self._coll,
                    [{'source'     : f'{self._run_dir}/results/',
                      'destination': self.state['target'],
                      'recursive'  : True}],
                    label=f'lucid {self.run_id} stage-out')
        self._globus_wait(globus, sub['task_id'], 'stage_out')

    def _fetch(self, globus, rh, stage):
        # Globus guest collections offer no HTTPS download: previews come
        # back through the endpoint's staging plugin
        local = self._local / self.run_id
        local.mkdir(parents=True, exist_ok=True)
        remote = f'{self._run_dir}/results'
        names  = ['montage.png'] + sorted(
                    e['name'] for e in stage.list(f'{remote}/preview')['entries']
                    if e['name'].endswith('.png'))
        images = []
        for name in names:
            src = f'{remote}/{name}' if name == 'montage.png' \
                                     else f'{remote}/preview/{name}'
            dst = local / name
            stage.get(src, str(dst))
            images.append(str(dst))
        self._update(images=images)
