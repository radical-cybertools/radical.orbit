#!/usr/bin/env bash
#
# IRI tunnel helper — login-node companion for IRI-launched child endpoints
# that need a reverse tunnel back to the bridge.
#
# Background.  The PsiJ launch path runs a parent endpoint on the login node;
# its plugin_psij watcher discovers the compute hostname and spawns
# ``ssh -R``.  IRI launches skip the login node entirely (the job goes
# straight to the IRI API), so there is no parent endpoint to do that work.
# This script fills the gap: run it once on the login node, and any
# IRI-launched child endpoint configured with ``tunnel='reverse'`` will have
# its tunnel created on demand.
#
# Protocol (filesystem-mediated):
#
#   ~/.radical/orbit/tunnels/<endpoint_name>.req    -- written by the child
#       JSON: {endpoint_name, hostname, bridge_host, bridge_port}
#       (atomic via .req.tmp + rename)
#
#   ~/.radical/orbit/tunnels/<endpoint_name>.port   -- written by THIS script
#       Plain text: the remote port sshd allocated.
#       The child polls for this file, then connects to localhost:<port>.
#
# Usage:
#   ./bin/orbit-iri-tunnel-helper.sh
#
# Quick & dirty: polls every second, processes each .req exactly once,
# kills its SSH when the SSH dies (e.g. compute node disappears at job
# end) and removes the corresponding .port file.

set -u

RELAY_DIR="${HOME}/.radical/orbit/tunnels"
mkdir -p "$RELAY_DIR"

# endpoint_name -> ssh pid
declare -A SSH_PIDS

log() {
    printf '[iri-tunnel-helper] %s %s\n' "$(date +'%Y-%m-%dT%H:%M:%S')" "$*" >&2
}

# Spawn ssh -R for a single .req file, parse the allocated port, write
# the .port file atomically.  On any failure log and bail out for this
# endpoint — caller will retry on the next sweep if the .req is still there.
handle_request() {
    local req_file="$1"
    local endpoint_name="$2"
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

    log "request: endpoint=$endpoint_name compute=$hostname bridge=$bridge_host:$bridge_port"

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
            log "ssh exited before allocating a port (endpoint=$endpoint_name); stderr:"
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
        log "timed out waiting for Allocated-port line (endpoint=$endpoint_name)"
        kill "$ssh_pid" 2>/dev/null
        rm -f "$stderr_log"
        return 1
    fi

    port_file="$RELAY_DIR/${endpoint_name}.port"
    tmp="${port_file}.tmp"
    printf '%s' "$port" > "$tmp" && mv "$tmp" "$port_file"
    log "endpoint=$endpoint_name allocated port=$port pid=$ssh_pid"

    SSH_PIDS["$endpoint_name"]=$ssh_pid

    # Detach the stderr log to a per-endpoint file so we can inspect it later.
    mv "$stderr_log" "$RELAY_DIR/${endpoint_name}.ssh.log"
}

# Reap any tracked SSHes that have died; clean their .port files so a
# fresh request for the same endpoint name doesn't see a stale port.
reap_dead() {
    local endpoint_name pid
    local to_remove=()
    for endpoint_name in "${!SSH_PIDS[@]}"; do
        pid="${SSH_PIDS[$endpoint_name]}"
        if ! kill -0 "$pid" 2>/dev/null; then
            log "ssh for endpoint=$endpoint_name (pid=$pid) is gone; cleaning up"
            rm -f "$RELAY_DIR/${endpoint_name}.port" \
                  "$RELAY_DIR/${endpoint_name}.req"
            to_remove+=("$endpoint_name")
        fi
    done
    for endpoint_name in "${to_remove[@]}"; do
        unset "SSH_PIDS[$endpoint_name]"
    done
}

cleanup_all() {
    local endpoint_name pid
    log "shutting down; killing ${#SSH_PIDS[@]} ssh process(es)"
    for endpoint_name in "${!SSH_PIDS[@]}"; do
        pid="${SSH_PIDS[$endpoint_name]}"
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
        endpoint_name=$(basename "$req_file" .req)
        # Already handled (ssh still alive)?
        if [ -n "${SSH_PIDS[$endpoint_name]:-}" ]; then
            continue
        fi
        handle_request "$req_file" "$endpoint_name" || {
            # Move the .req aside so we don't loop on a broken payload.
            mv "$req_file" "${req_file}.failed.$(date +%s)" 2>/dev/null || true
        }
    done

    sleep 1
done
