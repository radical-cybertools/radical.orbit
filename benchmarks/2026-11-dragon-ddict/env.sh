# Environment for the Dragon DDict benchmarks (source it).
# Never writes under ~/.radical/orbit: cert, key, token and logs live here.
# $DRAGON_BENCH_ROOT (default $PSCRATCH/dragon-bench) holds the venv (ve/,
# also ve310/ with py-spy) and the broker cert/key (tls/); radical.orbit
# comes from the tree this script is in.
module load cray-python/3.12.12 >/dev/null 2>&1
OV=${DRAGON_BENCH_ROOT:-${PSCRATCH:?set DRAGON_BENCH_ROOT}/dragon-bench}
export BENCH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export ORBIT_SRC=$(cd "$BENCH/../.." && pwd)
source $OV/ve/bin/activate                     # dragonhpc 0.15.0, matplotlib
export PATH=$OV/ve/bin:$PATH                   # dragon helpers resolve by name
export PYTHONPATH=$ORBIT_SRC/src               # radical.orbit from this tree
export PYTHONNOUSERSITE=1
export SLURM_EXPORT_ENV=ALL
export PYSPY=${PYSPY:-$OV/ve310/bin/py-spy}
export RUNS=${RUNS:-$OV/runs/dragon-bench}
export RADICAL_ORBIT_BROKER_CERT=$OV/tls/broker_cert.pem
export RADICAL_ORBIT_BROKER_KEY=$OV/tls/broker_key.pem
export RADICAL_ORBIT_BROKER_TOKEN=$(cat $RUNS/token)   # chmod 600, not committed
export RADICAL_ORBIT_LOG_LVL=${RADICAL_ORBIT_LOG_LVL:-WARNING}
