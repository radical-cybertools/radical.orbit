#!/bin/bash
# Dragon DDict through ORBIT: launch script, run from the login node against
# an existing allocation (salloc --no-shell):
#
#   salloc -N 4 -C cpu -q interactive -t 02:00:00 -A amsc007 --no-shell
#   export JOB=<jobid>
#   ./run_bench.sh probe native broker-compute k1 k2 k4 varb broker-login
#
# Stages (each writes JSON under $RUNS/data, logs under $RUNS/<stage>):
#   probe           outbound TCP from a compute node (curl github.com)
#   native          native DDict latency (dragon -s, one node)
#   broker-compute  start the broker on node 0       (BROKER_URL -> node 0)
#   broker-login    start the broker on this login node, then repeat
#                   latency + the 1- and 4-node throughput points
#   k1 | k2 | k4    endpoint on a K-node Dragon runtime, DDict n_nodes=K;
#                   k1 also runs latency, batch and concurrency sweeps
#   k1nd            diagnostic: k1 latency/batch/16 clients without
#                   WebSocket permessage-deflate (nodeflate/sitecustomize.py)
#   varb            4 endpoints, one per node, each its own 1-node DDict
#   nativeK         native concurrent clients on K nodes (reference)
#   stop            stop the broker
#
# Node 0 hosts the broker and the Dragon frontend.  Clients are plain
# python processes started with srun, spread over the allocation's nodes.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source $HERE/env.sh
: ${JOB:?set JOB to the allocation id}
PORT=${PORT:-18432}
DATA=$RUNS/data; mkdir -p $DATA
NODES=($(scontrol show hostnames $(squeue -h -j $JOB -o %N)))
N0=${NODES[0]}
DUR=${DUR:-5}                                    # s per conc phase
SRUN="srun --jobid=$JOB --overlap"

log() { echo "[$(date +%T)] $*"; }

broker_url() { cat $RUNS/broker.url; }

start_broker() {            # $1: compute | login
    stop_broker
    mkdir -p $RUNS/broker-$1
    if [ "$1" = compute ]; then
        RADICAL_ORBIT_LOG_FILE=$RUNS/broker-$1/broker.log \
        $SRUN -N1 -n1 -w $N0 python $ORBIT_SRC/bin/radical-orbit-broker.py \
            --port $PORT -p '' > $RUNS/broker-$1/broker.out 2>&1 &
        echo "https://$N0:$PORT" > $RUNS/broker.url
    else
        RADICAL_ORBIT_LOG_FILE=$RUNS/broker-$1/broker.log \
        python $ORBIT_SRC/bin/radical-orbit-broker.py \
            --port $PORT -p '' > $RUNS/broker-$1/broker.out 2>&1 &
        echo "https://$(hostname):$PORT" > $RUNS/broker.url
    fi
    echo $! > $RUNS/broker.pid
    sleep 8
    log "broker ($1) at $(broker_url)"
}

stop_broker() {
    [ -f $RUNS/broker.pid ] || return 0
    kill $(cat $RUNS/broker.pid) 2>/dev/null; sleep 3
    rm -f $RUNS/broker.pid
}

client() {                  # one client on node 1: client <mode> <args..>
    RADICAL_ORBIT_BROKER_URL=$(broker_url) \
    $SRUN -N1 -n1 -w ${NODES[1]} python $HERE/bench_client.py "$@"
}

start_endpoint() {          # $1: name  $2: K nodes (0: dragon -s on $3)
    local name=$1 k=$2 node=${3:-$N0} dir=$RUNS/ep-$1
    mkdir -p $dir
    if [ "$k" = 0 ]; then
        launch="dragon -s"
    else
        launch="$HERE/step_env.sh dragon -m -N $k"
    fi
    local pp=$PYTHONPATH diag=
    if [ -n "${DIAG:-}" ]; then                 # diag/sitecustomize.py
        pp=$HERE/diag:$PYTHONPATH; diag=$dir/diag
    fi
    # SLURM_OVERLAP: the launcher's inner sruns share the allocation
    PYTHONPATH=$pp ORBIT_BENCH_DIAG=$diag \
    RADICAL_ORBIT_BROKER_URL=$(broker_url) SLURM_OVERLAP=1 \
    RADICAL_ORBIT_LOG_FILE=$dir/endpoint.log \
    $SRUN -N1 -n1 -w $node --chdir $dir $launch \
        $ORBIT_SRC/bin/radical-orbit-endpoint.py --name $name -p dragon \
        > $dir/endpoint.out 2>&1 &
    echo $! > $dir/srun.pid
    log "endpoint $name ($launch on $node)"
}

stop_endpoints() {          # destroy the DDicts first, then TERM, then kill
    for ep in "$@"; do
        client destroy --ep $ep --wait 5 >> $RUNS/ep-$ep/destroy.log 2>&1 || true
    done
    # SIGTERM to the endpoint process only, never to the dragon launcher
    # (whose command line also names the endpoint script): a signalled
    # launcher tears the runtime down under the endpoint (README, Teardown)
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} \
        pkill -TERM -u $USER -f '^[^ ]*python3 [^ ]*radical-orbit-endpoint.py' \
        2>/dev/null
    local t=0
    for ep in "$@"; do
        while kill -0 $(cat $RUNS/ep-$ep/srun.pid) 2>/dev/null \
                && [ $t -lt 40 ]; do sleep 1; t=$((t + 1)); done
        echo "$ep: step gone after ${t}s" >> $RUNS/ep-$ep/destroy.log
        kill $(cat $RUNS/ep-$ep/srun.pid) 2>/dev/null
    done
    sleep 3
    # leftovers (DDict managers, runtime services) and their /dev/shm
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} bash -c \
        'pgrep -u $USER -f "dragon|radical-orbit-endpoint" -c' \
        > $RUNS/leftover-$(date +%s).txt 2>&1
    $SRUN -N ${#NODES[@]} -n ${#NODES[@]} dragon-cleanup > /dev/null 2>&1
    log "endpoints stopped: $*"
}

setup() {                   # $1 ep  $2 n_nodes  $3 tag  [args]
    local ep=$1 nn=$2 tag=$3; shift 3
    client setup --ep $ep --n-nodes $nn --wait 300 "$@" \
        --out $DATA/setup-$tag.json && log "ddict on $ep: $(tr -d '\n ' < $DATA/setup-$tag.json)"
}

conc() {                    # $1 ep(s)  $2 clients  $3 batch  $4 tag  [args]
    local ep=$1 c=$2 b=$3 tag=$4 nn=$2 wl; shift 4
    if [ $c -lt 4 ]; then
        wl=$(IFS=,; echo "${NODES[*]:1:$c}")    # off node 0 (broker)
    else
        nn=4; wl=$(IFS=,; echo "${NODES[*]}")
    fi
    local t=$(python -c "import time; print(time.time() + 15 + $c * 0.5)")
    mkdir -p $DATA/conc-$tag
    RADICAL_ORBIT_BROKER_URL=$(broker_url) \
    $SRUN -N $nn -n $c -w $wl python $HERE/bench_client.py conc --ep $ep \
        --batch $b --duration $DUR --start-at $t \
        --out "$DATA/conc-$tag/c$c-b$b-r{rank}.json" "$@" \
        > $RUNS/conc-$tag-c$c-b$b.log 2>&1
    log "conc $tag c=$c b=$b $*: rc=$? ($(ls $DATA/conc-$tag/c$c-b$b-r*.json 2>/dev/null | wc -l) ranks)"
}

[ "${BASH_SOURCE[0]}" = "$0" ] || return 0      # sourced: functions only

for stage in "$@"; do
    log "=== $stage (nodes ${NODES[*]})"
    case $stage in
    probe)
        $SRUN -N1 -n1 -w $N0 curl -sS -m 5 -o /dev/null \
            -w '%{http_code}\n' https://github.com > $DATA/probe.txt 2>&1
        $SRUN -N1 -n1 -w $N0 curl -sS -m 5 -o /dev/null \
            -w '%{http_code}\n' https://pypi.org >> $DATA/probe.txt 2>&1
        log "probe: $(cat $DATA/probe.txt | tr '\n' ' ')" ;;
    native)
        mkdir -p $RUNS/native && cd $RUNS/native
        $SRUN -N1 -n1 -w ${NODES[1]} dragon -s $HERE/native_bench.py lat \
            --out $DATA/native-lat.json > native-lat.log 2>&1
        log "native lat rc=$?"; cd - >/dev/null ;;
    native1|native2|native4)
        k=${stage#native}; mkdir -p $RUNS/$stage && cd $RUNS/$stage
        for p in 4 16; do
            $SRUN -N1 -n1 -w $N0 $HERE/step_env.sh dragon -m -N $k \
                $HERE/native_bench.py conc --n-nodes $k --procs $p \
                --duration $DUR --out $DATA/native-conc-k$k-p$p.json \
                > native-conc-p$p.log 2>&1
            log "native conc k=$k p=$p rc=$?"
            $SRUN -N 4 -n 4 dragon-cleanup > /dev/null 2>&1
        done
        cd - >/dev/null ;;
    broker-compute) start_broker compute ;;
    broker-login)
        start_broker login
        for k in 1 4; do
            start_endpoint ep-L$k $k
            setup ep-L$k $k L$k
            [ $k = 1 ] && client lat --ep ep-L$k --out $DATA/lat-login.json
            for b in 1 64; do conc ep-L$k 16 $b login-k$k; done
            stop_endpoints ep-L$k
        done
        stop_broker ;;
    k1|k2|k4)
        k=${stage#k}
        start_endpoint ep-k$k $k
        setup ep-k$k $k k$k || { stop_endpoints ep-k$k; continue; }
        if [ $k = 1 ]; then
            client lat   --ep ep-k1 --out $DATA/lat-compute.json
            log "lat rc=$?"
            client batch --ep ep-k1 --out $DATA/batch-compute.json
            log "batch rc=$?"
            for b in 1 64; do
                for c in 1 2 4 8 16; do conc ep-k1 $c $b k1; done
            done
        else
            for b in 1 64; do
                for c in $((4 * k)) 16; do conc ep-k$k $c $b k$k; done
            done
        fi
        stop_endpoints ep-k$k ;;
    k1nd)               # diagnostic: k1 without WebSocket deflate
        export ORBIT_BENCH_NO_DEFLATE=1 PYTHONPATH=$HERE/nodeflate:$PYTHONPATH
        start_endpoint ep-nd 1
        setup ep-nd 1 nd
        client lat   --ep ep-nd --out $DATA/lat-nodeflate.json
        client batch --ep ep-nd --out $DATA/batch-nodeflate.json
        for b in 1 64; do conc ep-nd 16 $b nodeflate; done
        stop_endpoints ep-nd
        unset ORBIT_BENCH_NO_DEFLATE ;;
    varb)
        eps=()
        for i in 0 1 2 3; do
            start_endpoint ep-b$i 0 ${NODES[$i]}; eps+=(ep-b$i)
        done
        for i in 0 1 2 3; do setup ep-b$i 1 b$i; done
        for b in 1 64; do conc "$(IFS=,; echo "${eps[*]}")" 16 $b varb; done
        stop_endpoints "${eps[@]}" ;;
    stop) stop_broker ;;
    *) log "unknown stage $stage" ;;
    esac
done
