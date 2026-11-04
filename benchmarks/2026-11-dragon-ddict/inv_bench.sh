#!/bin/bash
# Put-collapse and teardown investigation (README: "Put collapse",
# "Teardown").  Login node, existing allocation, compute-node broker:
#
#   export JOB=<jobid>; ./inv_bench.sh broker-compute A-m1 A-s A-mpn2 A-n2 \
#                                     A-native B-m B-s stop
#
# A-*  ORBIT put/get with the diag instrumentation (diag/sitecustomize.py)
#      on the endpoint; conc tags  inv-<stage>-<variant>[-rep]
# B-*  teardown probes: SIGTERM to the endpoint only ('ep'), or to the srun
#      step, which forwards it to the dragon launcher ('step'); timelines,
#      py-spy dumps, leftovers per node under $RUNS/B-*
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source $HERE/run_bench.sh                      # functions only
DUR=${DUR:-4}
export DIAG=1
EPAT='^[^ ]*python3 [^ ]*radical-orbit-endpoint.py'

leftovers() {               # $1 file: shm files and Dragon procs per node
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} bash -c \
        'echo "$(hostname) shm=$(ls /dev/shm | wc -l) dragon_procs=$(pgrep -u $USER -f _dragon_native -c)"' \
        > $1 2>&1
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} dragon-cleanup > /dev/null 2>&1
}

ep_node() {                 # node that runs the endpoint process
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} bash -c \
        "pgrep -u \$USER -f '$EPAT' > /dev/null && hostname" 2>/dev/null | head -1
}

sweep() {                   # $1 ep  $2 stage: the standard variant sweep
    local ep=$1 st=$2
    for rep in 1 2 3; do
        for c in 1 2 4; do conc $ep $c 64 inv-$st-base-$rep; done
    done
    for c in 1 2; do
        conc $ep $c 64  inv-$st-shared  --shared
        conc $ep $c 64  inv-$st-v64     --vsize 64
        conc $ep $c 1   inv-$st-b1
        conc $ep $c 16  inv-$st-b16
        conc $ep $c 256 inv-$st-b256
    done
}

short() {                   # $1 ep  $2 stage: 1 vs 2 clients, 2 reps
    for rep in 1 2; do
        for c in 1 2; do conc $1 $c 64 inv-$2-base-$rep; done
    done
}

teardown() {                # $1 ep  $2 launch K (0: -s)  $3 how (ep|step)
    local ep=$1 out=$RUNS/B-$1-$3; mkdir -p $out
    start_endpoint $ep $2
    setup $ep 1 $ep --mem 1G
    client batch --ep $ep --batches 64 --duration 1 > /dev/null
    local node; node=$(ep_node); log "endpoint $ep on $node"
    # probe on the endpoint node: timeline + py-spy dumps of the endpoint
    $SRUN -N1 -n1 -w $node bash -c \
        "$HERE/teardown_probe.sh \$(pgrep -u \$USER -f '$EPAT') $out none" \
        > $out/probe.out 2>&1 &
    local probe=$!
    sleep 3
    if [ $3 = ep ]; then    # the endpoint only
        $SRUN -N1 -n1 -w $node pkill -TERM -u $USER -f "$EPAT"
    else                    # scancel-like: srun forwards to the launcher
        kill -TERM $(cat $RUNS/ep-$ep/srun.pid)
    fi
    date +%s.%N > $out/t_signal
    wait $probe
    local t=0
    while kill -0 $(cat $RUNS/ep-$ep/srun.pid) 2>/dev/null && [ $t -lt 60 ]
    do sleep 1; t=$((t + 1)); done
    echo "step gone ${t}s after the probe ended" >> $out/events.txt
    kill -9 $(cat $RUNS/ep-$ep/srun.pid) 2>/dev/null
    leftovers $out/leftovers.txt
    log "teardown $ep/$3: $(tr '\n' ' ' < $out/events.txt) | $(tr '\n' ' ' < $out/leftovers.txt)"
}

for stage in "$@"; do
    log "=== $stage"
    case $stage in
    broker-compute) start_broker compute ;;
    stop)           stop_broker ;;
    A-m1)   start_endpoint ep-am1 1;  setup ep-am1 1 am1
            sweep ep-am1 m1;  stop_endpoints ep-am1
            leftovers $RUNS/ep-ep-am1/leftovers.txt ;;
    A-s)    start_endpoint ep-as 0;   setup ep-as 1 as
            short ep-as s;    stop_endpoints ep-as
            leftovers $RUNS/ep-ep-as/leftovers.txt ;;
    A-mpn2) start_endpoint ep-ampn2 1; setup ep-ampn2 1 ampn2 --mpn 2
            short ep-ampn2 mpn2; stop_endpoints ep-ampn2
            leftovers $RUNS/ep-ep-ampn2/leftovers.txt ;;
    A-n2)   start_endpoint ep-an2 2;  setup ep-an2 2 an2
            short ep-an2 n2;  stop_endpoints ep-an2
            leftovers $RUNS/ep-ep-an2/leftovers.txt ;;
    A-native)
        mkdir -p $RUNS/inv-native && cd $RUNS/inv-native
        nat() {             # $1 tag  $2 launcher flags  native args..
            local tag=$1 fl=$2; shift 2
            $SRUN -N1 -n1 -w $N0 $HERE/step_env.sh dragon $fl \
                $HERE/native_bench.py conc --duration $DUR \
                --out $DATA/inv-native-$tag.json "$@" > $tag.log 2>&1
            log "native $tag: rc=$? $(python -c "import json; d=json.load(open('$DATA/inv-native-$tag.json')); print(int(d['put_ops_s']), int(d['get_ops_s']))" 2>/dev/null)"
            $SRUN -N 4 -n 4 dragon-cleanup > /dev/null 2>&1
        }
        for p in 1 2 4 16; do nat m1-p$p "-m -N 1" --procs $p; done
        nat m1-p2-shared "-m -N 1" --procs 2 --shared
        nat m1-p2-v64    "-m -N 1" --procs 2 --vsize 64
        nat m1-p2-mpn2   "-m -N 1" --procs 2 --mpn 2
        nat m1-p16-mpn2  "-m -N 1" --procs 16 --mpn 2
        for p in 1 2 16; do nat s-p$p "-s" --procs $p; done
        cd - > /dev/null ;;
    B-m) teardown ep-bm-ep 1 ep;  teardown ep-bm-step 1 step ;;
    B-s) teardown ep-bs-ep 0 ep;  teardown ep-bs-step 0 step ;;
    *) log "unknown stage $stage" ;;
    esac
done
