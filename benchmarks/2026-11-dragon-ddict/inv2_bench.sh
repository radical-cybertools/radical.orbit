#!/bin/bash
# Put-collapse follow-up (README: "Put collapse"): is the slowdown overwrite
# history under the multi-node runtime, with no concurrency at all?
#
#   export JOB=<jobid>; ./inv2_bench.sh broker-compute T-native Q-m Q-s stop
#
# T-native  native single writer, put rate per second (native_bench trend)
#           under dragon -m -N 1 and dragon -s
#           and native_bench replay (one writer, the A-m1 key/size sequence)
# Q-m, Q-s  fresh ORBIT endpoint (-m -N 1 / -s): the A-m1 sequence with
#           py-spy samples of the DDict processes in every c=1 put phase
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source $HERE/run_bench.sh                      # functions only
DUR=${DUR:-4}
export DIAG=1
PYSPY=/pscratch/sd/m/merzky/overnight-2026-10-29/ve310/bin/py-spy
EPAT='^[^ ]*python3 [^ ]*radical-orbit-endpoint.py'
ep_node() {
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} bash -c \
        "pgrep -u \$USER -f '$EPAT' > /dev/null && hostname" 2>/dev/null | head -1
}

sample() {                  # $1 node  $2 out prefix: py-spy (native frames)
    # of the DDict's Dragon processes during the next conc put phase
    ( sleep 16; $SRUN -N1 -n1 -w $1 bash -c \
        "for p in \$(pgrep -u \$USER -f _dragon_native); do \
           $PYSPY record --native --nonblocking -r 200 -d 3 --format raw \
             -o $2-\$p.txt -p \$p > /dev/null 2>&1 & done; wait" ) &
    SPID=$!
}

seq_runs() {                # $1 ep  $2 stage: c=1 runs that replay A-m1
    local node; node=$(ep_node); mkdir -p $RUNS/inv-$2-spy
    run() { sample $node $RUNS/inv-$st_-spy/$1; conc $ep_ 1 64 \
            inv-$st_-$1 "${@:2}"; wait $SPID; }
    ep_=$1 st_=$2
    for i in 1 2 3; do
        run seq$i;  conc $1 2 64 inv-$2-two$i;  conc $1 4 64 inv-$2-four$i
    done
    run seqafter
    run newkeys --shared
    run v64 --vsize 64
    run final
}

for stage in "$@"; do
    log "=== $stage"
    case $stage in
    broker-compute) start_broker compute ;;
    stop)           stop_broker ;;
    T-native)
        mkdir -p $RUNS/inv-native && cd $RUNS/inv-native
        for fl in "-m -N 1" "-s"; do
            for mode in replay trend; do
                tag=$mode-$(echo $fl | tr -d ' -')
                args="--duration 4"
                [ $mode = trend ] && args="--duration 40 --phase-s 8"
                $SRUN -N1 -n1 -w $N0 $HERE/step_env.sh dragon $fl \
                    $HERE/native_bench.py $mode $args \
                    --out $DATA/inv-native-$tag.json > $tag.log 2>&1
                log "native $tag rc=$?"
                $SRUN -N 4 -n 4 dragon-cleanup > /dev/null 2>&1
            done
        done
        cd - > /dev/null ;;
    Q-m) start_endpoint ep-aqm 1; setup ep-aqm 1 aqm
         seq_runs ep-aqm qm; stop_endpoints ep-aqm ;;
    Q-s) start_endpoint ep-aqs 0; setup ep-aqs 1 aqs
         seq_runs ep-aqs qs; stop_endpoints ep-aqs ;;
    *) log "unknown stage $stage" ;;
    esac
done
