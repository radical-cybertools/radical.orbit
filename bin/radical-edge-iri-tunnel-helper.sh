#!/usr/bin/env bash
#
# IRI tunnel helper — login-node companion for IRI-launched child edges
# that need a reverse tunnel back to the bridge.
#
# Background.  The PsiJ launch path runs a parent edge on the login node;
# its plugin_psij watcher discovers the compute hostname and spawns
# ``ssh -R``.  IRI launches skip the login node entirely (the job goes
# straight to the IRI API), so there is no parent edge to do that work.
# This script fills the gap: run it once on the login node, and any
# IRI-launched child edge configured with ``tunnel='reverse'`` will have
# its tunnel created on demand.
#
# Protocol (filesystem-mediated):
#
#   ~/.radical/edge/tunnels/<edge_name>.req    -- written by the child
#       JSON: {edge_name, hostname, bridge_host, bridge_port}
#       (atomic via .req.tmp + rename)
#
#   ~/.radical/edge/tunnels/<edge_name>.port   -- written by THIS script
#       Plain text: the remote port sshd allocated.
#       The child polls for this file, then connects to localhost:<port>.
#
# Usage:
#   ./bin/radical-edge-iri-tunnel-helper.sh
#
# Quick & dirty: polls every second, processes each .req exactly once,
# kills its SSH when the SSH dies (e.g. compute node disappears at job
# end) and removes the corresponding .port file.

set -u

RELAY_DIR="${HOME}/.radical/edge/tunnels"
mkdir -p "$RELAY_DIR"

# edge_name -> ssh pid
declare -A SSH_PIDS

log() {
    printf '[iri-tunnel-helper] %s %s\n' "$(date +'%Y-%m-%dT%H:%M:%S')" "$*" >&2
}

# Spawn ssh -R for a single .req file, parse the allocated port, write
# the .port file atomically.  On any failure log and bail out for this
# edge — caller will retry on the next sweep if the .req is still there.
handle_request() {
    local req_file="$1"
    local edge_name="$2"
    local payload hostname bridge_host bridge_port
    local stderr_log ssh_pid port port_file tmp

    payload=$(cat "$req_file" 2>/dev/null) || {
        log "could not read $req_file"
        return 1
    }
    hostname=$(printf '%s' "$payload" | python3 -c \
        'import json,sys;print(json.load(sys.stdin)["hostname"])' 2>/dev/null) || {
        log "malformed payload in $req_file"
        return 1
    }
    bridge_host=$(printf '%s' "$payload" | python3 -c \
        'import json,sys;print(json.load(sys.stdin)["bridge_host"])')
    bridge_port=$(printf '%s' "$payload" | python3 -c \
        'import json,sys;print(json.load(sys.stdin)["bridge_port"])')

    log "request: edge=$edge_name compute=$hostname bridge=$bridge_host:$bridge_port"

    stderr_log=$(mktemp -t iri-tunnel-helper-XXXXXX.log)
    ssh -N \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o BatchMode=yes \
        -o ServerAliveInterval=10 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -v \
        -R "0:${bridge_host}:${bridge_port}" \
        "$hostname" \
        2>"$stderr_log" &
    ssh_pid=$!

    # Wait up to 30s for the "Allocated port N" line to appear.
    port=
    for _ in $(seq 1 60); do
        if ! kill -0 "$ssh_pid" 2>/dev/null; then
            log "ssh exited before allocating a port (edge=$edge_name); stderr:"
            sed -e 's/^/    /' "$stderr_log" >&2
            rm -f "$stderr_log"
            return 1
        fi
        port=$(grep -m1 -oE 'Allocated port [0-9]+' "$stderr_log" \
               | awk '{print $3}')
        [ -n "$port" ] && break
        sleep 0.5
    done

    if [ -z "$port" ]; then
        log "timed out waiting for Allocated-port line (edge=$edge_name)"
        kill "$ssh_pid" 2>/dev/null
        rm -f "$stderr_log"
        return 1
    fi

    port_file="$RELAY_DIR/${edge_name}.port"
    tmp="${port_file}.tmp"
    printf '%s' "$port" > "$tmp" && mv "$tmp" "$port_file"
    log "edge=$edge_name allocated port=$port pid=$ssh_pid"

    SSH_PIDS["$edge_name"]=$ssh_pid

    # Detach the stderr log to a per-edge file so we can inspect it later.
    mv "$stderr_log" "$RELAY_DIR/${edge_name}.ssh.log"
}

# Reap any tracked SSHes that have died; clean their .port files so a
# fresh request for the same edge name doesn't see a stale port.
reap_dead() {
    local edge_name pid
    for edge_name in "${!SSH_PIDS[@]}"; do
        pid="${SSH_PIDS[$edge_name]}"
        if ! kill -0 "$pid" 2>/dev/null; then
            log "ssh for edge=$edge_name (pid=$pid) is gone; cleaning up"
            rm -f "$RELAY_DIR/${edge_name}.port" \
                  "$RELAY_DIR/${edge_name}.req"
            unset 'SSH_PIDS[$edge_name]'
        fi
    done
}

cleanup_all() {
    local edge_name pid
    log "shutting down; killing ${#SSH_PIDS[@]} ssh process(es)"
    for edge_name in "${!SSH_PIDS[@]}"; do
        pid="${SSH_PIDS[$edge_name]}"
        kill "$pid" 2>/dev/null || true
    done
    exit 0
}

trap cleanup_all INT TERM

log "watching $RELAY_DIR for *.req"

while true; do
    reap_dead

    for req_file in "$RELAY_DIR"/*.req; do
        [ -e "$req_file" ] || continue
        edge_name=$(basename "$req_file" .req)
        # Already handled (ssh still alive)?
        if [ -n "${SSH_PIDS[$edge_name]:-}" ]; then
            continue
        fi
        handle_request "$req_file" "$edge_name" || {
            # Move the .req aside so we don't loop on a broken payload.
            mv "$req_file" "${req_file}.failed.$(date +%s)" 2>/dev/null || true
        }
    done

    sleep 1
done
