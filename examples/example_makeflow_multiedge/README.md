# Example: multi-edge Makeflow via the task dispatcher

This example walks through a 4-rule DAG that spans two radical.edge
edges and two pools, with file dependencies that cross the edge
boundary in both directions.

## What the workflow does

```
    gen       (edge_a, cpu) ─ produces raw.txt
       │
       ▼
    clean     (edge_a, cpu) ─ raw.txt → clean.txt
       │
       ▼   ← cross-edge file transfer (clean.txt fetched to client, pushed to edge_b)
    analyze  (edge_b, gpu) ─ clean.txt → counts.txt
       │
       ▼   ← cross-edge file transfer (counts.txt back to client, then to edge_a)
    summarize (edge_a, cpu) ─ clean.txt + counts.txt → summary.txt
```

The commands are deliberately trivial (`tr`, `wc`, `cat`) so the DAG
runs anywhere.  In a real workflow the `analyze` rule would be a GPU
kernel; its placement on the `gpu` pool is what matters here, not the
command itself.

## Prerequisites

1. A radical.edge **bridge** running somewhere reachable
   (`RADICAL_BRIDGE_URL` set).
2. Two radical.edge **edges** connected to that bridge:
   - one named `edge_a` with the `task_dispatcher`, `rhapsody`, `staging`,
     and `psij` plugins loaded
   - one named `edge_b` with the same plugins loaded
3. Each edge has a `~/.radical/edge/task_dispatcher/pools.json` file.
   The one in this directory declares both pools on both edges so you
   can copy it verbatim; in production the two edges might have
   different pool menus.
4. The `makeflow` binary on the client host's `PATH`
   (part of [cctools](https://cctools.readthedocs.io)).

## Running the example

```sh
# Copy the pool config into place on each edge's host:
scp pools.json edge_a_host:~/.radical/edge/task_dispatcher/pools.json
scp pools.json edge_b_host:~/.radical/edge/task_dispatcher/pools.json

# Start the bridge on the client host
radical-edge-bridge.py &

# Start edge_a on its login node
RADICAL_BRIDGE_URL=... radical-edge-wrapper.sh -n edge_a \
    --plugins task_dispatcher,rhapsody,staging,psij &

# Start edge_b on its login node
RADICAL_BRIDGE_URL=... radical-edge-wrapper.sh -n edge_b \
    --plugins task_dispatcher,rhapsody,staging,psij &

# On the client host, run the workflow
cd examples/example_makeflow_multiedge
radical-edge-makeflow workflow.makeflow
```

The `radical-edge-makeflow` convenience script preprocesses
`workflow.makeflow` into a temporary file with every rule's command
wrapped by `radical-edge-run`, then invokes `makeflow` on it.

After the run completes, `summary.txt` appears in the current
directory.

## What's happening under the hood

For each rule, Makeflow spawns one `radical-edge-run` subprocess.  The
wrapper:

1. Uploads declared inputs (`--in`) via `task_dispatcher.stage_in` to
   the target pool's shared-FS scratch directory on the target edge.
2. Calls `task_dispatcher.submit_task` with a stable `task_id`
   computed from the command plus its inputs, outputs, and the
   preprocessor-generated `--run-id`.
3. Subscribes to the bridge's SSE stream and blocks on a
   `task_status` notification for that `task_id`.
4. On success, downloads declared outputs (`--out`) via
   `task_dispatcher.stage_out`.
5. Exits with the task's `exit_code`.

Inside the edge, the task dispatcher's strategy decides when to
submit a pilot (a SLURM/PBS/… batch job that runs a child edge with
`rhapsody` + `staging` on compute nodes).  Once the pilot handshakes
back with a reported capacity, queued tasks are routed to it.

For cross-edge file transitions — `clean.txt` flowing from `edge_a`
to `edge_b` and `counts.txt` flowing back — the file simply bounces
through the client's Makeflow working directory, because Makeflow's
own DAG logic tracks declared inputs and outputs of each rule.  No
direct edge-to-edge transport is needed.

## Observability

The bridge's `/events` SSE channel broadcasts three topics from the
dispatcher:

- `task_status`         — rule-level state transitions
- `pilot_status`        — pilot lifecycle events
- `autoscale_decision`  — logged every time the strategy submits a pilot

The bridge's Explorer UI (at the bridge's root URL) shows all of these
live.

## Known limitations in v1

- Live stdout/stderr streaming during task execution is not yet
  implemented; only the terminal snapshot arrives in the task record.
  For in-progress inspection, tail the files at
  `~/.radical/edge/task_dispatcher/scratch/<pool>/<task_id>/` on the
  target edge.
- Cross-edge file transfer goes through the client.  For large
  intermediate artifacts this is slow; direct edge↔edge transfer is
  future work.
- The `gpu` pool in `pools.json` uses the `concurrent` rhapsody
  backend so the example runs without Dragon installed.  For real
  GPU workflows, change `rhapsody_backend` to `dragon_v3`.
