#!/usr/bin/env python3
'''
End-to-end IRI test: launch an ORBIT endpoint on OLCF Odo via the S3M
IRI facility API, then drive a minimal ROSE active-learning loop on it.

Flow: connect to the running broker → ``iri_connect`` (bearer token, read
from a local file) → batch job runs the endpoint wrapper → wait for the
endpoint to register → ROSE loop of pure-stdlib function tasks → teardown.

Usage::

    python examples/test_iri.py [<max_iter>]

Prerequisites (see DEPLOYMENT.md):

- A broker is running; its startup banner prints the bootstrap one-liners.
- The broker TLS cert is staged on the target
  (``~/.radical/orbit/broker_cert.pem``).
- The orbit ve exists on the target at ``ORBIT_VE`` below (with rhapsody
  and dragon; the test tasks need only the python stdlib).
- The IRI bearer token sits next to this script as ``token_olcf``,
  literal string only.  It is read locally, sent to the broker once at
  ``connect()`` time, and held there in process memory only.

Edit the configuration block below to match your account / paths.
'''

import asyncio
import base64
import logging
import os
import sys
import time

from pathlib import Path

import rhapsody

from radical.asyncflow      import WorkflowEngine
from radical.orbit          import EndpointRuntime
from radical.orbit          import utils as orbit_utils
from rose.al.active_learner import SequentialActiveLearner

rhapsody.enable_logging(level=logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration — edit to match your account / paths.
# ─────────────────────────────────────────────────────────────────────────────

ENDPOINT     = 'olcf'                        # iri_connect endpoint key
RESOURCE     = 'odo'                         # IRI resource id
LOGIN_HOST   = 'login1.frontier.olcf.ornl.gov'
ACCOUNT      = 'fus183'
QUEUE        = 'batch'
WORKDIR      = '/gpfs/wolf2/olcf/fus183/proj-shared'   # required by OLCF SLURM
ORBIT_VE     = '/autofs/nccsopen-svm1_home/merzky/radical/radical.orbit/ve3'

N_NODES      = 1
WALLTIME_MIN = 30
MAX_ITER     = 2                             # default; CLI-overridable

# Site-specific shell snippet, eval'd by the wrapper before dragon starts.
SETUP        = ['module load cray-python/3.11.7']

ENDPOINT_WAIT_SECONDS = 30 * 60              # max queue wait for the job


def abort(msg):
    print(f'ABORT  {msg}')
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
#  Launch.
# ─────────────────────────────────────────────────────────────────────────────

def read_token():
    '''Read ``token_olcf`` from the script's own directory.'''
    path = Path(__file__).resolve().parent / f'token_{ENDPOINT}'
    if not path.exists():
        raise RuntimeError(f'token file missing: {path}  (put your IRI '
                           f'bearer token there, literal string only)')
    token = path.read_text().strip()
    if not token:
        raise RuntimeError(f'token file is empty: {path}')
    return token


def job_environment(broker_url):
    '''Broker URL + current token for the child endpoint's job env.

    The TLS cert is NOT injected — it is staged manually on the target
    (see DEPLOYMENT.md).  The setup snippet travels base64-encoded: some
    job APIs compose batch scripts with unquoted ``export`` lines that
    truncate multi-word values.
    '''
    env = {'RADICAL_ORBIT_BROKER_URL': broker_url}
    token, _ = orbit_utils.resolve_broker_token()
    if token:
        env['RADICAL_ORBIT_BROKER_TOKEN'] = token
    if SETUP:
        env['RADICAL_ORBIT_SETUP_B64'] = base64.b64encode(
            '; '.join(SETUP).encode()).decode('ascii')
    return env


def launch(bc, broker_url):
    '''Connect to the IRI facility and submit the endpoint job.'''
    cx  = bc.get_plugin('broker', 'iri_connect')
    api = cx.connect(endpoint=ENDPOINT, token=read_token())

    name    = f'iri-test.{os.getpid()}'
    wrapper = f'{ORBIT_VE}/bin/radical-orbit-endpoint-wrapper.sh'

    job = api.submit_job(RESOURCE, {
        'executable' : wrapper,
        'arguments'  : ['--name', name, '--url', broker_url,
                        '--tunnel', 'reverse'],
        'name'       : name,
        'directory'  : WORKDIR,
        'resources'  : {'node_count': N_NODES},
        'attributes' : {'queue_name': QUEUE,
                        'duration'  : WALLTIME_MIN * 60,
                        'account'   : ACCOUNT},
        'environment': job_environment(broker_url),
    })
    print(f'job id   : {job["job_id"]}')
    return {'api': api, 'job_id': job['job_id'], 'endpoint_name': name}


# ─────────────────────────────────────────────────────────────────────────────
#  Wait for the endpoint to register.
# ─────────────────────────────────────────────────────────────────────────────

class JobFailureWatch:
    '''Trip on the first terminal-failure ``job_status`` notification for
    one job, so a dead job aborts the wait instead of blocking it.'''

    _FAILED = {'failed', 'cancelled', 'canceled', 'error', 'node_fail',
               'timeout', 'out_of_memory', 'preempted', 'deadline'}

    def __init__(self, bc, job_id):
        self._bc     = bc
        self._job_id = job_id
        self.failed  = False
        self.reason  = None
        self._cb     = self._on_status
        bc.register_callback(topic='job_status', callback=self._cb)

    def _on_status(self, endpoint, plugin, topic, data):
        if self.failed or not isinstance(data, dict):
            return
        if data.get('job_id') != self._job_id:
            return
        state = str(data.get('state', '')).lower()
        if state in self._FAILED:
            self.reason = data.get('error') or f'job entered state {state!r}'
            self.failed = True

    def close(self):
        try:
            self._bc.unregister_callback(topic='job_status',
                                         callback=self._cb)
        except Exception:
            pass


def wait_for_endpoint(bc, name, failure):
    '''Poll the topology until *name* registers (dots while waiting).'''
    start   = time.time()
    last_hb = start
    try:
        while time.time() - start < ENDPOINT_WAIT_SECONDS:
            if name in bc.topology():
                return name
            if failure.failed:
                raise RuntimeError(f'endpoint {name!r} will not appear — '
                                   f'its job failed: {failure.reason}')
            time.sleep(3.0)
            if time.time() - last_hb >= 10.0:
                sys.stdout.write('.')
                sys.stdout.flush()
                last_hb = time.time()
        raise TimeoutError(f'endpoint {name!r} did not appear within '
                           f'{ENDPOINT_WAIT_SECONDS}s')
    finally:
        sys.stdout.write('\n')
        sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  ROSE workload — pure-stdlib function tasks (imports inside the bodies,
#  no captured variables: they get cloudpickled to the endpoint).
# ─────────────────────────────────────────────────────────────────────────────

async def run_rose_workload(broker_url, endpoint_name, max_iter):

    backend = rhapsody.get_backend('orbit', broker_url=broker_url,
                                   endpoint_name=endpoint_name)
    engine  = await backend
    flow    = await WorkflowEngine.create(engine)
    acl     = SequentialActiveLearner(flow)

    @acl.simulation_task(as_executable=False)
    async def simulation(*args):
        import math
        import os
        import random
        import socket
        xs = [random.uniform(0.0, 2.0 * math.pi) for _ in range(16)]
        ys = [math.sin(x) + random.gauss(0.0, 0.1) for x in xs]
        return {'sim_host'  : socket.gethostname(),
                'sim_pid'   : os.getpid(),
                'sim_y_mean': sum(ys) / len(ys)}

    @acl.training_task(as_executable=False)
    async def training(*args):
        import os
        import random
        import socket
        import statistics
        losses = [abs(random.gauss(0.0, 1.0)) for _ in range(8)]
        return {'train_host': socket.gethostname(),
                'train_pid' : os.getpid(),
                'train_loss': statistics.mean(losses)}

    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        import math
        import os
        import random
        import socket
        queries = [random.uniform(0.0, 2.0 * math.pi) for _ in range(4)]
        return {'al_host'   : socket.gethostname(),
                'al_pid'    : os.getpid(),
                'al_queries': len(queries)}

    try:
        async for state in acl.start(max_iter=max_iter):
            print(f'  iter {state.iteration}: '
                  f'sim [{state.sim_host}/{state.sim_pid}] '
                  f'y_mean={state.sim_y_mean:+.3f}  '
                  f'train [{state.train_host}/{state.train_pid}] '
                  f'loss={state.train_loss:.3f}  '
                  f'al [{state.al_host}/{state.al_pid}] '
                  f'queries={state.al_queries}')
    finally:
        await acl.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
#  Main.
# ─────────────────────────────────────────────────────────────────────────────

def teardown(bc, rec):
    if not rec:
        return
    try:
        rec['api'].cancel_job(RESOURCE, rec['job_id'])
    except Exception as exc:
        print(f'  could not cancel job {rec["job_id"]}: {exc}')
    try:
        bc.get_plugin('broker', 'iri_connect').disconnect(ENDPOINT)
    except Exception as exc:
        print(f'  could not disconnect {ENDPOINT}: {exc}')


def main():
    max_iter = MAX_ITER
    if len(sys.argv) > 1:
        if not sys.argv[1].isdigit() or int(sys.argv[1]) <= 0:
            abort(f'usage: {sys.argv[0]} [<max_iter>]')
        max_iter = int(sys.argv[1])

    bc = EndpointRuntime()
    bc.start(wait=True)
    broker_url = bc.broker_url
    print(f'broker   : {broker_url}')

    rec = None
    try:
        try:
            rec = launch(bc, broker_url)
        except Exception as exc:
            abort(f'IRI launch failed: {exc}')

        t0    = time.time()
        watch = JobFailureWatch(bc, rec['job_id'])
        try:
            wait_for_endpoint(bc, rec['endpoint_name'], failure=watch)
        except Exception as exc:
            abort(f'wait for endpoint failed: {exc}')
        finally:
            watch.close()
        print(f'endpoint : {rec["endpoint_name"]} '
              f'up after {int(time.time() - t0)}s')

        print(f'rose     : {max_iter} iteration(s)')
        try:
            asyncio.run(run_rose_workload(
                broker_url, rec['endpoint_name'], max_iter))
        except Exception as exc:
            abort(f'rose workload failed: {exc}')
        print('OK')
    finally:
        print('teardown : cancelling job, disconnecting')
        teardown(bc, rec)
        bc.stop()


if __name__ == '__main__':
    main()
