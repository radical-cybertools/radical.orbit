# Example: multi-endpoint Makeflow via the task dispatcher

This example walks through a 4-rule DAG that spans two radical.orbit
endpoints and two pools, with file dependencies that cross the endpoint
boundary in both directions.

## What the workflow does

```
    gen       (endpoint_a, cpu) ─ produces raw.txt
       │
       ▼
    clean     (endpoint_a, cpu) ─ raw.txt → clean.txt
       │
       ▼   ← cross-endpoint file transfer (clean.txt fetched to client, pushed to endpoint_b)
    analyze  (endpoint_b, gpu) ─ clean.txt → counts.txt
       │
       ▼   ← cross-endpoint file transfer (counts.txt back to client, then to endpoint_a)
    summarize (endpoint_a, cpu) ─ clean.txt + counts.txt → summary.txt
```

The commands are deliberately trivial (`tr`, `wc`, `cat`) so the DAG
runs anywhere.  In a real workflow the `analyze` rule would be a GPU
kernel; its placement on the `gpu` pool is what matters here, not the
command itself.

## Prerequisites

1. A radical.orbit **bridge** running somewhere reachable
   (`RADICAL_ORBIT_BRIDGE_URL` set).
2. Two radical.orbit **endpoints** connected to that bridge:
   - one named `endpoint_a` with the `task_dispatcher`, `rhapsody`, `staging`,
     and `psij` plugins loaded
   - one named `endpoint_b` with the same plugins loaded
3. Each endpoint has a `~/.radical/orbit/task_dispatcher/pools.json` file.
   The one in this directory declares both pools on both endpoints so you
   can copy it verbatim; in production the two endpoints might have
   different pool menus.
4. The `makeflow` binary on the client host's `PATH`
   (part of [cctools](https://cctools.readthedocs.io)).

## Running the example

```sh
# Copy the pool config into place on each endpoint's host:
scp pools.json endpoint_a_host:~/.radical/orbit/task_dispatcher/pools.json
scp pools.json endpoint_b_host:~/.radical/orbit/task_dispatcher/pools.json

# Start the bridge on the client host
radical-orbit-bridge.py &

# Start endpoint_a on its login node
RADICAL_ORBIT_BRIDGE_URL=... radical-orbit-endpoint-wrapper.sh -n endpoint_a \
    --plugins task_dispatcher,rhapsody,staging,psij &

# Start endpoint_b on its login node
RADICAL_ORBIT_BRIDGE_URL=... radical-orbit-endpoint-wrapper.sh -n endpoint_b \
    --plugins task_dispatcher,rhapsody,staging,psij &

# On the client host, run the workflow
cd examples/example_makeflow_multiendpoint
radical-orbit-makeflow workflow.makeflow
```

The `radical-orbit-makeflow` convenience script preprocesses
`workflow.makeflow` into a temporary file with every rule's command
wrapped by `radical-orbit-run`, then invokes `makeflow` on it.

After the run completes, `summary.txt` appears in the current
directory.

## What's happening under the hood

For each rule, Makeflow spawns one `radical-orbit-run` subprocess.  The
wrapper:

1. Uploads declared inputs (`--in`) via `task_dispatcher.stage_in` to
   the target pool's shared-FS scratch directory on the target endpoint.
2. Calls `task_dispatcher.submit_task` with a stable `task_id`
   computed from the command plus its inputs, outputs, and the
   preprocessor-generated `--run-id`.
3. Subscribes to the bridge's SSE stream and blocks on a
   `task_status` notification for that `task_id`.
4. On success, downloads declared outputs (`--out`) via
   `task_dispatcher.stage_out`.
5. Exits with the task's `exit_code`.

Inside the endpoint, the task dispatcher's strategy decides when to
submit a pilot (a SLURM/PBS/… batch job that runs a child endpoint with
`rhapsody` + `staging` on compute nodes).  Once the pilot handshakes
back with a reported capacity, queued tasks are routed to it.

For cross-endpoint file transitions — `clean.txt` flowing from `endpoint_a`
to `endpoint_b` and `counts.txt` flowing back — the file simply bounces
through the client's Makeflow working directory, because Makeflow's
own DAG logic tracks declared inputs and outputs of each rule.  No
direct endpoint-to-endpoint transport is needed.

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
  `~/.radical/orbit/task_dispatcher/scratch/<pool>/<task_id>/` on the
  target endpoint.
- Cross-endpoint file transfer goes through the client.  For large
  intermediate artifacts this is slow; direct endpoint↔endpoint transfer is
  future work.
- The `gpu` pool in `pools.json` uses the `concurrent` rhapsody
  backend so the example runs without Dragon installed.  For real
  GPU workflows, change `rhapsody_backend` to `dragon_v3`.
