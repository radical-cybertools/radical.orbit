#!/bin/bash
# teardown_probe.sh <endpoint pid> <outdir> [signal|none]: signal the endpoint
# (none: someone else does) and record, for up to 60 s, py-spy dumps (with native frames) of the endpoint
# at +2/+8/+16/+24/+40 s and the surviving process list every second.
PS=${PYSPY:-$(command -v py-spy)}
[ -x "$PS" ] || { echo "teardown_probe.sh: py-spy not found, set PYSPY" >&2; exit 1; }
pid=$1 out=$2 sig=${3:-TERM}; mkdir -p $out
t0=$(date +%s.%N); [ $sig = none ] || kill -$sig $pid
echo "probe start $t0, sent ${sig} to $pid" > $out/events.txt
for s in $(seq 1 60); do
    sleep 1
    ps -u $USER -o pid,ppid,etimes,cmd | grep -E 'dragon|radical-orbit|_dragon_native' \
        | grep -v -E 'grep|teardown_probe' > $out/ps-$s.txt
    case $s in 2|8|16|24|40)
        timeout 20 $PS dump --native --pid $pid > $out/pyspy-$s.txt 2>&1 ;;
    esac
    if ! kill -0 $pid 2>/dev/null; then
        echo "endpoint $pid gone after ${s}s" >> $out/events.txt; break
    fi
done
kill -0 $pid 2>/dev/null && echo "endpoint $pid still alive after 60s" >> $out/events.txt
ls /dev/shm | grep -c . | sed 's/^/dev_shm_files: /' >> $out/events.txt
cat $out/events.txt
