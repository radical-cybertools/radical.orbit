# Environment for the Dragon DDict benchmarks (source it; edit the paths).
# Never writes under ~/.radical/orbit: cert, key, token and logs live here.
module load cray-python/3.12.12 >/dev/null 2>&1
OV=/pscratch/sd/m/merzky/overnight-2026-10-29
source $OV/ve/bin/activate                     # dragonhpc 0.15.0, matplotlib
export PATH=$OV/ve/bin:$PATH                   # dragon helpers resolve by name
export PYTHONPATH=$OV/wt-156/src               # radical.orbit from the PR tree
export PYTHONNOUSERSITE=1
export SLURM_EXPORT_ENV=ALL
export BENCH=$OV/wt-156/benchmarks/2026-11-dragon-ddict
export RUNS=${RUNS:-$OV/runs/dragon-bench}
export RADICAL_ORBIT_BROKER_CERT=$OV/tls/broker_cert.pem
export RADICAL_ORBIT_BROKER_KEY=$OV/tls/broker_key.pem
export RADICAL_ORBIT_BROKER_TOKEN=$(cat $RUNS/token)   # chmod 600, not committed
export RADICAL_ORBIT_LOG_LVL=${RADICAL_ORBIT_LOG_LVL:-WARNING}
