#!/bin/bash
# Run "$@" in a 1-node srun step so that an inner srun (the dragon
# launcher's) sees the whole allocation: inside a step Slurm sets
# SLURM_JOB_NODELIST to the step's node and the step vars confine srun.
export SLURM_JOB_NODELIST=$SLURM_NODELIST SLURM_JOB_NUM_NODES=$(scontrol show hostnames $SLURM_NODELIST | wc -l)
unset SLURM_NNODES SLURM_NTASKS SLURM_NPROCS SLURM_TASKS_PER_NODE SLURM_DISTRIBUTION \
      SLURM_STEP_ID SLURM_STEPID SLURM_STEP_NODELIST SLURM_STEP_NUM_NODES SLURM_STEP_NUM_TASKS \
      SLURM_STEP_TASKS_PER_NODE SLURM_STEP_LAUNCHER_PORT SLURM_STEP_RESV_PORTS SLURM_NODEID SLURM_PROCID SLURM_LOCALID SLURM_GTIDS
export SLURM_OVERLAP=1
exec "$@"
