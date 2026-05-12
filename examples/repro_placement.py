#!/usr/bin/env python3
"""Minimal reproducer for HOST_NAME / gpu_affinity placement oversubscription.

Submits ``GPUS_PER_NODE * n_hosts`` tasks, each pinned to a specific
``(host, gpu)`` via ``Policy(HOST_NAME, gpu_affinity)``.  Each task does
``sleep 10; hostname > ./repro_placement.out.$$`` — one file per task,
named with the task shell's PID, so the post-run
``cat repro_placement.out.* | sort | uniq -c`` shows exactly how many
ran on each host.

A single shared output file with ``>>`` looks tempting but is unreliable
on parallel filesystems: POSIX ``O_APPEND`` atomicity is guaranteed only
within one host's kernel, so concurrent appends from many compute nodes
on Lustre / GPFS / DVS / NFS can clobber each other.  Per-task files
sidestep that entirely.

Expected: each host appears exactly ``GPUS_PER_NODE`` times.
Observed bug (in amsc.py): per-host count varies 1..7 on a 16-node
allocation with GPUS_PER_NODE=4.

Usage
-----
    salloc -A <acct> -C gpu -N 16 -t 30 --ntasks-per-node=1 ...
    cd <some-dir-on-shared-fs>
    dragon python repro_placement.py
    cat repro_placement.out.* | sort | uniq -c
"""
import asyncio
import glob
import os
import subprocess
import time

from dragon.infrastructure.policy import Policy
from rhapsody.api import ComputeTask, Session
from rhapsody.backends import DragonExecutionBackendV3


# === adjust ==================================================================
GPUS_PER_NODE = 4
CPUS_PER_NODE = 128
OUT_FILE      = './repro_placement.out'
GENERATIONS   = 1
# =============================================================================


# Remove prior per-task outputs so this run's counts are unambiguous.
for stale in glob.glob(f'{OUT_FILE}.*'):
    os.unlink(stale)

nodelist = subprocess.check_output(
    ['scontrol', 'show', 'hostnames', os.environ['SLURM_JOB_NODELIST']],
    text=True).split()
n_hosts = len(nodelist)
n_tasks_gpu = n_hosts * GPUS_PER_NODE * GENERATIONS

# reserve CPU cores for GPU tasks
if n_tasks_gpu:
    CPUS_PER_NODE -= GPUS_PER_NODE

n_tasks_cpu = n_hosts * CPUS_PER_NODE * GENERATIONS 

print(f'\nnodelist ({n_hosts} hosts):')
for h in nodelist:
    print(f'  {h}')
print(f'\nbuilding {n_tasks_gpu} tasks ({GPUS_PER_NODE}/node)')
print(f'  output file: {OUT_FILE}\n')

shell_cmd = f'sleep 10; hostname > {OUT_FILE}.$$'

tasks = []
for i in range(n_tasks_gpu):
    host = nodelist[i % n_hosts]
    gpu  = (i // n_hosts) % GPUS_PER_NODE
    print(f'  task t.{i:02d}  host={host:>10s}  gpu={gpu}')
    tasks.append(ComputeTask(
        uid           = f't.{i:02d}',
        executable    = '/bin/bash',
        arguments     = ['-c', shell_cmd],
        capture_stdio = True,
        task_backend_specific_kwargs = {
            'process_template': {
                'cwd'   : os.path.dirname(OUT_FILE) or '.',
                'policy': Policy(
                    placement    = Policy.Placement.HOST_NAME,
                    host_name    = host,
                    gpu_affinity = [gpu],
                ),
            },
        },
    ))


async def main():
    backend = await DragonExecutionBackendV3()
    session = Session(backends=[backend])
    async with session:
        t0 = time.time()
        await session.submit_tasks(tasks)
        print(f'\nsubmitted {n_tasks_gpu} tasks in {time.time()-t0:.2f}s')

        await asyncio.gather(*tasks)
        elapsed = time.time() - t0
        done   = sum(1 for t in tasks if str(t.state) == 'DONE')
        failed = n_tasks_gpu - done
        print(f'\nall {n_tasks_gpu} tasks finished in {elapsed:.1f}s'
              f'   (done={done}  failed={failed})')
        print(f'\ncat {OUT_FILE}.* | sort | uniq -c\n')


asyncio.run(main())

