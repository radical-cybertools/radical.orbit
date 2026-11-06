#!/bin/bash
# F1b: latency vs value size with WebSocket permessage-deflate off / on / auto
# (radical.orbit PR #159 switch, trial-merged onto #156).  3 nodes: broker on
# node 0, latency client on node 1, endpoint (dragon -s) on node 2 -- the
# client -> broker -> endpoint hops of F1, each on its own node.
#
#   export JOB=<jobid> TREE=<src dir of the trial merge>
#   ./wscomp_bench.sh broker-compute off on auto nodeflate stop
#
# Per config the endpoint and the client run with
# RADICAL_ORBIT_WS_COMPRESSION=<cfg> (nodeflate: 'on' plus the old
# nodeflate/sitecustomize.py patch, a cross-check that should match 'off').
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
source $HERE/run_bench.sh                      # functions only
export PYTHONPATH=${TREE:?}:$PYTHONPATH        # the switch, ahead of wt-156
export RADICAL_ORBIT_LOG_LVL=INFO BENCH_CLIENT_LOG=INFO
mkdir -p $RUNS/wscomp

for stage in "$@"; do
    log "=== $stage"
    case $stage in
    broker-compute) start_broker compute ;;
    stop)           stop_broker ;;
    off|on|auto|nodeflate)
        ep=ep-wc-$stage
        if [ $stage = nodeflate ]; then
            export RADICAL_ORBIT_WS_COMPRESSION=on ORBIT_BENCH_NO_DEFLATE=1
            export PYTHONPATH=$HERE/nodeflate:$PYTHONPATH
        else
            export RADICAL_ORBIT_WS_COMPRESSION=$stage
        fi
        start_endpoint $ep 0 ${NODES[2]}
        setup $ep 1 wc-$stage --mem 8G
        client lat --ep $ep --out $DATA/lat-wscomp-$stage.json \
            > $RUNS/wscomp/client-$stage.log 2>&1
        log "lat $stage rc=$?"
        grep -h 'ws_compression' $RUNS/wscomp/client-$stage.log \
            $RUNS/ep-$ep/endpoint.log 2>/dev/null | sed 's/^/    /'
        stop_endpoints $ep ;;
    *) log "unknown stage $stage" ;;
    esac
done
