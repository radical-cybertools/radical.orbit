# REST API Reference

The broker's **gateway** module serves an HTTP/SSE compatibility surface for
non-participant callers — browsers, `curl`, the Explorer UI. It is the compat
tier: broker-native participants speak the WebSocket envelope protocol instead
(see [Architecture & Wire Protocol](architecture.md)). The gateway is on by default; a broker
started with `--no-gateway` exposes only the WebSocket `/register` ingress.

All gateway routes are reachable at `http(s)://<broker_host>:<port>/`. Plugin
routes are reached through the catch-all proxy under
`/<endpoint_name>/<plugin_name>/…`: the gateway maps that URL onto the star
model `(dst=<endpoint_name>, path=<remainder>)` and routes the request over
the broker's WebSocket to that participant's plugins.

## Authentication

Every route except the UI shell (`GET /`) and the static plugin assets
(`GET /plugins/*`) requires the shared broker token, sent either as an
`Authorization: Bearer <token>` header or — for the browser — as the HttpOnly
cookie minted by `POST /auth`. A missing or invalid token yields **401**.
Authentication can be disabled for local development (`--no-auth`), in which
case the gate is inert.

## Gateway Endpoints

| Method | Path | Description |
|----|----|----|
| `GET` | `/` | Explorer UI (HTML). Ungated so the browser can prompt for the token. |
| `POST` | `/auth` | Validate the bearer token and set the HttpOnly auth cookie (used by the
Explorer / SSE). Reached only with a valid bearer header. |
| `POST` | `/endpoint/list` | Discovery. Returns `{"data": {"broker": {"url": …}, "endpoints": {name: {"endpoint": {role, liveness}, "plugins": {pname: {namespace, …}}}}}}`. Namespaces are the full `/{endpoint}/{plugin}` form. |
| `GET` | `/endpoints` | Flat listing. Returns `{"endpoints": [{name, plugins, connected, plugin_count}], "total": N}`. |
| `GET` | `/events` | SSE stream for real-time notifications and topology changes (see below). |
| `POST` | `/endpoint/disconnect/{endpoint_name}` | Gracefully disconnect and terminate an endpoint. **404** if not
connected; **400** for the reserved `broker` name. |
| `POST` | `/broker/terminate` | Terminate the broker process (endpoints keep running). |
| `GET` | `/plugins/{filename}` | Serve a JS plugin module file (used by the Explorer). Ungated. |
| *any* | `/{endpoint_name}/{path}` | Catch-all proxy to a plugin on the named participant. Methods: `GET`,
`POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `HEAD`. |

### Proxy semantics

The catch-all proxy forwards the request over the broker's routing table and
waits up to a **long deadline** (600 s — a large submit batch whose backend
setup takes seconds per task genuinely runs this long) for the participant's
response. Status mapping:

- **404** — the target participant (`endpoint_name`) is unknown / not
  connected.
- **503** — the broker's pending-call table is at capacity (too many in-flight
  calls).
- **504** — the participant did not respond within the deadline.

The shared token and hop-by-hop headers are stripped before a request is
forwarded, so the broker credential never rides on to plugins. A request whose
`endpoint_name` is `broker` is routed into the broker's own hosted-plugin
host instead of the routing-loop registry.

## SSE Event Format

The `/events` stream sends JSON-encoded frames. The broker sends the current
topology as the first frame on connect. A per-client queue is bounded and
drop-oldest, so a stalled reader can never backpressure the broker.

Notification frame:

    data: {"topic": "notification", "data": {
        "endpoint": "my_endpoint",
        "plugin":   "psij",
        "topic":    "job_status",
        "data":     { ... plugin-specific ... }
    }}

Topology frame (the same `{broker, endpoints}` shape `/endpoint/list`
returns):

    data: {"topic": "topology", "data": {
        "broker":    {"url": "https://broker:8000"},
        "endpoints": {"my_endpoint": {
            "endpoint": {"role": "endpoint", "liveness": "present"},
            "plugins":  {"sysinfo": {"namespace": "/my_endpoint/sysinfo", ...}}
        }}
    }}

## Plugin Base Routes

Every plugin automatically registers these routes under its namespace
(`/<endpoint_name>/<plugin_name>/` through the proxy):

| Method | Path | Description |
|----|----|----|
| `POST` | `register_session` | Create or reconnect to a session. Body (optional):
`{sid, lifetime, ttl}`. Returns `{"sid": "<session_id>"}`. |
| `POST` | `unregister_session/{sid}` | Close and remove a session. Returns `{"ok": true}` |
| `GET` | `version` | Plugin version. Returns `{"version": "x.y.z"}` |
| `GET` | `list_sessions` | Active session IDs. Returns `{"sessions": [...]}` |
| `GET` | `health` | Health check. Returns status, uptime, active session count |
| `GET` | `ui_config` | UI configuration for the Explorer. Returns plugin name, version, `ui` |

## PsiJ Plugin

Namespace: `psij`

| Method | Path | Description |
|----|----|----|
| `POST` | `submit/{sid}` | Submit a job. Body: `{"job_spec": {...}, "executor": "slurm"}` |
| `GET` | `status/{sid}/{job_id}` | Job status and output. Query params: `stdout_offset`, `stderr_offset` for streaming |
| `GET` | `list_jobs/{sid}` | All jobs in the session. Returns `{"jobs": [...]}` |
| `POST` | `cancel/{sid}/{job_id}` | Cancel a job. Returns `{"ok": true}` |

`submit` request body:

    {
        "executor": "slurm",
        "job_spec": {
            "executable": "/path/to/bin",
            "arguments":  ["--arg", "val"],
            "attributes": {
                "queue_name": "debug",
                "account":    "myproject",
                "duration":   600,
                "node_count": 2
            }
        }
    }

`status` response:

    {
        "job_id":      "job.abc123",
        "native_id":   "12345",
        "state":       "COMPLETED",
        "exit_code":   0,
        "executable":  "/path/to/bin",
        "arguments":   ["--arg", "val"],
        "executor":    "slurm",
        "stdout":      "...",
        "stderr":      "...",
        "stdout_offset": 1024,
        "stderr_offset": 0
    }

## Rhapsody Plugin

Namespace: `rhapsody`

`register_session` accepts an optional body: `{"backends": ["local", "dragon_v3"]}`

| Method | Path | Description |
|----|----|----|
| `POST` | `submit/{sid}` | Submit tasks. Body: `{"tasks": [{...}, ...]}` |
| `POST` | `wait/{sid}` | Wait for tasks. Body: `{"uids": [...], "timeout": 60}` |
| `GET` | `list_tasks/{sid}` | All tasks in session |
| `GET` | `task/{sid}/{uid}` | Task details including stdout, stderr, exception |
| `POST` | `cancel/{sid}/{uid}` | Cancel a task |
| `GET` | `statistics/{sid}` | Backend execution statistics |

## Queue Info Plugin

Namespace: `queue_info`

`is_enabled` and `job_allocation` are session-less and return immediately
without requiring a session.

| Method | Path | Description |
|----|----|----|
| `GET` | `is_enabled` | Returns `{"available": true/false}` — whether SLURM is present |
| `GET` | `job_allocation` | Returns current job allocation of the **endpoint** process (see below) |
| `GET` | `get_info/{sid}` | Partition and allocation info |
| `GET` | `list_jobs/{sid}/{queue}` | Jobs in a specific queue/partition |
| `GET` | `list_all_jobs/{sid}` | All jobs visible to the current user |
| `GET` | `list_allocations/{sid}` | Active allocations |
| `POST` | `cancel/{sid}/{job_id}` | Cancel a queued or running job |

`job_allocation` response:

    # Endpoint running on a login node (no SLURM job):
    {"allocation": null}

    # Endpoint running inside a SLURM job allocation:
    {"allocation": {"n_nodes": 4, "runtime": 3600}}

    # Endpoint running inside a SLURM job with unlimited walltime:
    {"allocation": {"n_nodes": 4, "runtime": null}}

`n_nodes` is the number of nodes in the allocation; `runtime` is the
walltime limit in seconds (`null` for UNLIMITED). A 500 response is
returned if `SLURM_JOB_ID` is set but allocation details cannot be
determined (missing env vars, `squeue` failure or timeout).

## Sysinfo Plugin

Namespace: `sysinfo`

| Method | Path | Description |
|----|----|----|
| `GET` | `homedir` | Home directory path. Returns `{"homedir": "/home/user"}` |
| `GET` | `metrics/{sid}` | System metrics (CPU, memory, disk, GPUs, network, filesystems) |

## Staging Plugin

Namespace: `staging`

| Method | Path | Description |
|----|----|----|
| `POST` | `put/{sid}` | Upload a file to the endpoint. Body: `{"src": "/local/path", "tgt": "/remote/path"}` |
| `POST` | `get/{sid}` | Download a file from the endpoint. Body: `{"src": "/remote/path", "tgt": "/local/path"}` |
| `GET` | `list/{sid}` | List files in the session staging area |

## XGFabric Plugin

Namespace: `xgfabric`

| Method | Path | Description |
|----|----|----|
| `GET` | `workdir/{sid}` | Get current config directory |
| `POST` | `workdir/{sid}` | Set config directory. Body: `{"path": "/path/to/configs"}` |
| `GET` | `configs/{sid}` | List saved configurations |
| `GET` | `config/{sid}/default` | Load the built-in default workflow config |
| `GET` | `config/{sid}/test` | Load the built-in test workflow config (stub tasks) |
| `GET` | `config/{sid}/{name}` | Load a named config from disk |
| `POST` | `config/{sid}` | Save a configuration. Body: workflow config dict with `"name"` field |
| `POST` | `config/{sid}/{name}/delete` | Delete a saved configuration |
| `GET` | `status/{sid}` | Current workflow state (status, phase, cluster lists, progress) |
| `POST` | `start/{sid}` | Start workflow. Body: `{"workflow": "default", "resource": "default"}` |
| `POST` | `stop/{sid}` | Cancel a running workflow |

## Task Dispatcher Plugin

Namespace: `task_dispatcher`. Hosted on the **broker**, so its routes are
reached under `/broker/task_dispatcher/…`.

`register_session` accepts an optional `{"pools": [...]}` declaration; a
session that declares none gets the built-in `default` pool (one node, one
CPU, `concurrent` backend).

| Method | Path | Description |
|----|----|----|
| `GET`  | `pools` | All pools visible to the caller |
| `GET`  | `pool/{sid}/{name}` | Detailed state for one of this session's pools |
| `GET`  | `fleet/{sid}` | Fleet snapshot across this session's pools |
| `POST` | `submit/{sid}` | Submit one task (exec dialect). Body below |
| `POST` | `submit_rh/{sid}` | Bulk submit in the rhapsody dialect. Body: `{"tasks": [{...}, ...]}`, each task dict carrying a `pool` key |
| `GET`  | `task/{sid}/{task_id}` | One task's record (pool mode) or the target's rhapsody info (endpoint mode) |
| `POST` | `cancel/{sid}/{task_id}` | Cancel one task |
| `POST` | `cancel_all/{sid}` | Cancel every task in the session and tear its pools down |
| `POST` | `stage_in/{sid}/{task_id}` | Upload an input file into the task's scratch dir (**broker-local only**) |
| `GET`  | `stage_out/{sid}/{task_id}/{filename}` | Download an output file (**broker-local only**) |
| `POST` | `pool/{sid}/{name}/members` | Add one member to a class pool |
| `DELETE` | `pool/{sid}/{name}/members/{member_id}` | Remove one member, draining its pilots |

There is deliberately **no add-pool route**: `register_session` already
adds pools to a live session — it reconnects the sid and then materialises
every declared pool, returning the existing `PoolState` for one this
session already owns.  Note the parser rejects an empty `pools` list, so a
re-registration must carry the **full** list of the session's pools.
Re-declaring a pool does **not** merge members; use the member routes.

### Pools are capability classes

A pool is a capability class, not a site.  A **legacy** declaration names
one endpoint/queue/account/pilot-size menu, exactly as before.  A **class
pool** instead carries `members`, each one an
`(endpoint, queue, account, pilot-size menu, attributes, budget)` tuple,
so a GPU pilot on two different clusters can live in the same pool:

```json
{"name": "fed-gpu", "pool_class": "gpu",
 "members": [
   {"member_id": "perlmutter", "endpoint_name": "ep_pm",
    "queue": "regular", "account": "m1234",
    "pilot_sizes": {"default": {"nodes": 1, "cpus_per_node": 128,
                                "gpus_per_node": 4, "walltime_sec": 1800,
                                "rhapsody_backend": "concurrent"}},
    "default_size": "default", "min_pilots": 0, "max_pilots": 2,
    "scratch_base": "/pscratch/…/atomic", "shared_fs": false,
    "attributes": {"site": "NERSC", "software": ["lammps", "pytorch"],
                   "mem_gb_per_node": 256},
    "budget": {"node_hours": 40.0}}]}
```

- `member_id` matches `^[a-z0-9][a-z0-9_.-]*$`; the pool name plus the
  member id must be at most 64 characters, because a pilot's child
  endpoint is named `<pool>_<member_id>_<pid>` (a legacy pool keeps the
  pre-existing `<pool>_<pid>`).
- `queue` must be a real queue, not the `default` sentinel.
- `attributes` are free-form (`str`, number, or list of `str`);
  `site: str` and `software: [str]` are conventions, not schema.  They are
  **declared, not self-reported**: a pilot snapshots its member's
  attributes at submit time and keeps them for its life, so re-declaring a
  member does not retro-fit running pilots.
- `budget` is `{}` or `{"node_hours": <positive number>}` — allocations are
  per site, so budgets are per member.
- `shared_fs` says whether the broker host and the member see the same
  `scratch_base`.  When false the dispatcher never touches `scratch_base`
  locally; a task's inputs travel to the pilot through its own `staging`
  plugin instead.  A non-shared member should set its own `scratch_base`:
  without one it inherits the pool's, which by default is a path on the
  broker host and usually does not exist on the member's machine.
- `pool_class` is an explicit string matching `^[a-z0-9_.-]*$`; `""` (the
  default, and every legacy pool) means "unclassified".  It is never
  derived inside the dispatcher.

A single-member pool is **never promoted** in place: promotion would have
to rename its implicit member and rewrite every live pilot's already
registered child endpoint name.  Declare a class pool as a class pool from
the start.

### `POST pool/{sid}/{name}/members`

Body is one member declaration (the shape above).  Returns
`200 {"pool": name, "member": {…}, "members": [<ids>], "created": true|false}`.

An **identical** re-POST is a `created: false` no-op — that is what makes a
restart replay idempotent.  There is no partial update: a member is
replaced by removing and re-adding it.  No pilot is submitted here; the
next housekeeping tick applies the new member's `min_pilots` floor.

| status | when |
|---|---|
| `400` | schema violation (detail carries the exact message) |
| `404` | unknown session or unknown pool |
| `409` | `pool <name> is not a class pool; declare it with 'members'` |
| `409` | `member exists with a different declaration` |

### `DELETE pool/{sid}/{name}/members/{member_id}`

Flags travel in the **JSON body**, not the query string:

```json
{"cancel_tasks": false, "force": false, "fail_unsatisfiable": true}
```

An empty body means the defaults above.  Returns
`200 {"pool": name, "member_id": mid, "pilots_cancelled": n,
"tasks_requeued": n, "tasks_failed": n}`.

The member is dropped from the pool **before** its pilots are cancelled
(and its pilots are paused first), so a housekeeping tick landing in that
window cannot submit a replacement pilot for the member being removed.
Each cancelled pilot's non-terminal tasks are re-queued — tasks are
pool-level and bind to a pilot only at dispatch, so removing a member
never orphans a queued task — and the pool is drained immediately rather
than waiting a tick.

- `fail_unsatisfiable` (default `true`) fails a QUEUED task no *remaining*
  member could ever serve, with
  `no member satisfies the task requirements: <reason>`, instead of leaving it
  QUEUED forever.  Pass `false` when the member may come back (an endpoint
  blip) and those tasks should wait for it.
- `cancel_tasks` fails every task the member was running, satisfiable or
  not.
- `force` is required to remove the **last** member; otherwise `409`.
  A member-less class pool persists and replays; it just cannot run
  anything until a member is added back.
- `404` for an unknown session, pool or member; `409` for a non-class pool.

### Pool summaries

`GET pools` (non-verbose) gains `pool_class`, `multi_member`,
`member_ids` (a list of **strings**; empty for a legacy pool, whose single
implicit member is an internal construct and never part of the wire) and
`max_pilots_total` (`sum(member.max_pilots)` — the Explorer header needs
it and this route must stay cheap).

`GET pool/{sid}/{name}` (verbose) additionally gains `pilot_history` (the
pool's pilots, oldest first, each carrying `member_id`, `attributes` and
its size snapshot), a pool-total `node_hours_used`, and `members`: one
object per member with

```
member_id, endpoint_name, queue, account, attributes, budget,
min_pilots, max_pilots, shared_fs, pilot_sizes, default_size,
live_pilots, pilots_active, node_hours_used,
node_hours_remaining (null when no budget), pilot_history
```

`node_hours_used` is computed server-side from each pilot's own size
snapshot, so a mixed-node-count pool is correct and a **departed** member's
pilots still size themselves.  Node-hours are charged from a pilot's
`active_at` only: queue time is not allocation time, and a pilot that never
reached ACTIVE is charged nothing.  The pool total therefore covers pilots
whose member has since been removed — which is why it is reported
separately rather than summed from the member figures.

`pilot_history` is **unbounded** (pre-existing behaviour: the pilot ledger
itself is never pruned within a pool's lifetime, only whole pool state
directories are, after 30 idle days).  A long-lived pool's verbose summary
therefore grows with its pilot count; `recent_tasks` stays capped at 50.

### `submit/{sid}` body

```json
{"pool": "cpu", "task_id": "t.abc", "cmd": ["/bin/echo", "hi"],
 "cwd": "/scratch/t.abc", "priority": 0, "inputs": [], "outputs": [],
 "inputs_b64": {"md.json": "<base64>"},
 "requirements": {"cores": 1, "gpus": 0, "mem_gb": 0, "ranks": 1,
                  "mpi": false, "software": [], "labels": {}}}
```

Exactly one of `pool` or `endpoint` is required — `pool` routes through a
dispatcher-managed pilot fleet, `endpoint` is a transparent proxy to that
endpoint's rhapsody plugin (no pool, no staging).

### `cwd`: required, except on a class pool

`cwd` is required, **except** for an exec-style task submitted to a class
pool: there the member — and therefore the filesystem — is not known until
the task is placed, so omitting `cwd` lets the dispatcher assign
`<member.scratch_base or pool scratch>/<task_id>` **at dispatch**, create
it when the member is `shared_fs`, and report it back on the task record
(clients that poll `task/{sid}/{task_id}` see it appear).  A re-dispatch
after a pilot loss re-assigns it; a client-supplied `cwd` is never
rewritten.

- An explicit `cwd` is a `400 explicit cwd is not valid for a pool with
  non-shared members` as soon as any member declares `shared_fs: false`.
- Rhapsody-dialect tasks are excluded: their `cwd` rides inside the opaque
  task dict, which is forwarded verbatim and never rewritten, so a class
  pool answers `400 rhapsody-dialect tasks must carry an explicit 'cwd';
  the dispatcher assigns one only for exec-style tasks`.

### `inputs_b64`: task inputs that travel with the submit

`{"<filename>": "<base64>"}` on a **pool-mode exec-style** submit only.
The files are spooled broker-locally under the pool's state directory and
placed in the task's cwd immediately before it runs — copied for a
`shared_fs` member, `put` over the pilot's own staging plugin for a
non-shared one (which is also what creates the directory there; an
input-less task on such a member gets one zero-byte `.orbit-cwd` marker
instead).  A task whose inputs cannot be placed is FAILED with
`could not place inputs on the pilot: …` rather than run without them.

This is what lets a client submit before it knows *where* the task will
run.  The names land in the task record's `spooled` field — **not**
`inputs`, which keeps its client-declared meaning.  The spool is dropped on
every terminal state and deliberately **survives a re-queue** (the task is
about to be dispatched somewhere else).

The spool is written **synchronously**, on the submit path, before the
response returns.  That is deliberate at this size: the caps above bound
one submit to 8 MiB, and the alternative — acknowledging a task whose
inputs are not yet on disk — would make a broker crash between the two
silently produce a task that runs with missing files.

The pilot serving a non-shared member must allow writes under that
member's `scratch_base`.  The `staging` plugin's allow-list is `$HOME` plus
`/tmp`, extended at session start with `$RADICAL_ORBIT_SCRATCH_BASE` —
which the dispatcher sets on every pilot it launches, to that pilot's
member `scratch_base`.  A pilot started outside the dispatcher needs the
variable set by hand, or its puts are refused with *Path escapes allowed
directories*.

| status | when |
|---|---|
| `400` | `invalid base64: …`, a filename with a slash or `..`, or a non-mapping block |
| `400` | `inputs_b64 is only supported for pool-mode exec tasks` (endpoint mode, or a rhapsody-dialect task) |
| `413` | a file above 2 MiB decoded, or a block above 8 MiB decoded |

The 2 MiB per-file cap comes from the 4 MiB protocol frame cap: the
base64 form is 4/3 of the decoded one and shares the frame with the rest
of the submit body.

### Dispatcher staging is broker-local

`stage_in` / `stage_out` read and write on the **broker host** only.  With
no `TaskRecord` for the id they behave exactly as they always have
(staging into the pool scratch before any submit); with a record they use
its own `cwd` and refuse what the broker cannot reach:

| status | when |
|---|---|
| `409` | `task not yet placed` (a class-pool task with no cwd assigned yet) |
| `409` | `staging for this task is not broker-local; use the pilot's staging plugin at <child_endpoint>` |

Result collection from a non-shared member therefore goes through the
pilot's own `staging` plugin; the dispatcher does not proxy it.

### The `requirements` object

Optional on both `submit/{sid}` and each task dict of `submit_rh/{sid}`.
Every key is optional; absent or `null` means "no declaration" and behaves
byte-for-byte as before the field existed.

| key | type | rule |
|---|---|---|
| `cores` | int | ≥ 1; **total** CPU cores for the task. Omitted ⇒ `max(1, ranks)` |
| `gpus` | int | ≥ 0; **total** GPUs for the task |
| `mem_gb` | int **or** float | ≥ 0 |
| `ranks` | int | ≥ 1; **process replicas** — MPI ranks when `mpi` is true |
| `mpi` | bool | selects the backend's MPI launch path |
| `software` | list[str] | placement attribute; never reaches rhapsody |
| `labels` | dict[str, str \| int \| float] | placement attribute; never reaches rhapsody |

`bool` is never accepted where an integer is expected (`"cores": true` is a
400). Anything not in the table is a **400** — a typo that silently drops a
field is worse than a refused request. Beyond the per-key types the
dispatcher enforces `cores >= ranks` (so `cores_per_rank` is never 0) and
`gpus % ranks == 0` (so `gpus_per_rank` is an exact integer), then rejects a
task no `pilot_size` in the target pool could ever host — compared **per
node**, since none of the shipped backends spreads one task across nodes.
A mixed pool is judged on its best member; `mem_gb` has no such check
because `PilotSize` carries no memory field. Note the built-in `default`
pool has `cpus_per_node = 1`, so any `cores >= 2` is a 400 there.

**`ranks` without `cores`.** When `cores` is omitted the dispatcher derives
`cores = max(1, ranks)`, so `{"ranks": 4}` alone means "four processes on
four cores" rather than a `cores` (1) `>= ranks` (4) rejection. The derived
value is what gets persisted and forwarded. An *explicit* `cores` below
`ranks` is a contradiction and stays a 400.

Example detail strings:

```
requirements: unknown key 'gpu'
requirements: 'cores' must be a positive integer, got 0
requirements: 'gpus' must be a non-negative integer, got 'two'
requirements: 'mem_gb' must be a non-negative number, got 'x'
requirements: 'mpi' must be a boolean
requirements: 'software' must be a list of strings
requirements: 'labels' must be a mapping of string to string|number
requirements: 'cores' (2) must be >= 'ranks' (4)
requirements: 'gpus' (3) must be divisible by 'ranks' (2)
requirements: 8 cores exceed every pilot_size (largest: 's', 4 cpus/node)
requirements: 2 gpus exceed every pilot_size (largest: 's', 0 gpus/node)
requirements: 'mpi' is unsupported on dragon_v1 (pool 'x', size 's')
```

**On a class pool** the gate is the dispatch policy's own test: a task is
accepted only if some member's declared attributes **and default pilot
size** — the only size the policy ever grows or dispatches to — satisfy
it.  A task that fits nothing but a larger, non-default size would queue
forever, so it is a 400 too.  The detail names the first member's reason:

```
no member satisfies the task requirements: software missing: lammps
no member satisfies the task requirements: cores 4 < 32
no member satisfies the task requirements: backend dragon_v1 cannot run an mpi task
```

A task no *declared* member could ever run is a client error; queueing it
forever is the worse answer.  The runtime counterpart (the only capable
member *left* after the submit) is `DELETE …/members`'
`fail_unsatisfiable` sweep, which asks exactly the same question — so
removing an unrelated member never fails a task the gate accepted.

`software` and `labels` never reach rhapsody: they are dispatcher-side
placement attributes, matched against a member's (and then a pilot's)
declared `attributes`.  On a **legacy** pool the implicit member declares
no attributes, so they are carried and persisted but match nothing.

The validated block is stored on the task record, so `requirements` appears
in every task-shaped response — `submit/{sid}`, `task/{sid}/{task_id}`, pool
summaries and `task_status` notifications — carrying `{}` for a task that
declared none.

### Forwarding: what each backend does with it

Requirements are **forwarded, not enforced**. The dispatcher maps them onto
the pilot's rhapsody backend (fixed per pilot by
`PilotSize.rhapsody_backend`) and merges the result into the task's
`task_backend_specific_kwargs` — the only place rhapsody reads resource
keys from; a top-level `cores`/`gpus`/`ranks` would be kept verbatim and
read by nobody. Oversubscription control stays with rhapsody.

| backend | forwarded | effect |
|---|---|---|
| `dragon_v2` | `ranks`, `gpus_per_rank` | honoured natively; spawns `ranks` replicas |
| `radical_pilot` | `ranks`, `cores_per_rank`, `gpus_per_rank`, `mem_per_rank` (MB) | honoured natively; note the exec-mode `cwd` also rides into the same `TaskDescription` (pre-existing) |
| `dragon_v3` | `type: "mpi"` + `ranks`, **only** when `mpi` is true | `ranks` is read only under `type: "mpi"` |
| `dragon_v1` | `ranks` | spawns `ranks` replicas and queues on a global slot counter; `mpi` is refused at submit |
| `dask` | `resources: {"GPU": gpus}` when `gpus > 0` | pre-checked; the task fails if unsatisfiable |
| `concurrent` | *(nothing)* | the backend reads only `shell`/`cwd`/`env` |

Any value equal to the backend's own default is omitted, so a task without
`requirements` forwards byte-identically. `ranks` means "process replicas"
and only incidentally "MPI ranks": `dragon_v1` and `dragon_v2` spawn
`ranks` replicas either way, while `dragon_v3` spawns them only under
`type: "mpi"` — a caller who sets `ranks: 4` expecting four processes gets
one on a `dragon_v3` pool. On `concurrent` and non-MPI `dragon_v3` — the
two defaults — the declaration is a persisted record and nothing more.

In the rhapsody dialect a caller-supplied `task_backend_specific_kwargs`
**wins per key** over the derived mapping: the caller knows its backend.
Keys it did not set still come from `requirements`.

### Endpoint mode: advisory only

An endpoint-mode submit (`"endpoint": "..."` instead of `"pool"`) accepts
`requirements`, applies the **same shape validation** (so a typo is still a
400), and then drops it: there is no pool, hence no fit check and no
backend gate, and the dispatcher never learns which backend the target
endpoint chose. Nothing is stored and nothing is forwarded: the task dict
sent on to the target's rhapsody plugin is exactly what it would have been
without the key, and the endpoint-mode response carries no `requirements`
of its own — there is no task record behind it. One advisory line is
logged per submit, and only when the block is non-empty.

### Resubmit ignores changed requirements

Both submit routes return the **cached record** for a task_id already in
`DONE`, `RUNNING` or `QUEUED` state — crash recovery, wrapper reconnect.
A resubmit with *changed* `requirements` is therefore silently ignored,
exactly as a changed `priority` is; only `FAILED`/`CANCELED` re-executes.
There is deliberately no mutation path. Validation runs **before** that
cache ladder, so a malformed `requirements` is a 400 even on a resubmit of
a cached `DONE` task.

## Error Responses

All plugin endpoints return standard HTTP status codes:

- `200` — Success
- `400` — Bad request (missing/invalid parameters)
- `401` — Missing or invalid broker token (ingress auth)
- `403` — Session owned by another participant (cross-owner reattach)
- `404` — Session, resource, or endpoint not found
- `409` — Conflict (e.g. incoherent/conflicting session lifetime policy)
- `410` — Session expired (TTL exceeded)
- `500` — Internal server error
- `502` — Bad gateway (upstream participant returned an invalid response)
- `503` — Broker/endpoint at concurrency cap (too many in-flight calls)
- `504` — Upstream (participant) timeout

### Error body format

Every synthesized error carries one canonical envelope:

    {"error": true, "status_code": <int>, "detail": "human-readable message"}

This is a superset of the older `{"detail": ...}` body: `detail` is always
present (so existing consumers keep working), and `error` + `status_code`
are additive. `status_code` mirrors the HTTP status on the wire, which is
useful for clients that read the body without the transport status (e.g. the
endpoint runtime tunnels an HTTP response over the WebSocket).

The one exception is request-validation (`422`): FastAPI's structured
validation body is preserved as-is and is *not* rewrapped in this envelope.

### Error status matrix

| Status | Meaning                                                            |
|--------|--------------------------------------------------------------------|
| `400`  | Bad request — missing or invalid parameters                        |
| `401`  | Missing or invalid broker token (ingress auth gate)                |
| `403`  | Forbidden — permission denied / cross-owner session reattach       |
| `404`  | Not found — session, resource, route, or endpoint                  |
| `409`  | Conflict — e.g. target already exists, incoherent lifetime         |
| `500`  | Internal server error — unhandled handler exception                |
| `502`  | Bad gateway — upstream participant returned an invalid response    |
| `503`  | At concurrency cap — too many in-flight calls (with `Retry-After`) |
| `504`  | Upstream timeout — participant handler exceeded the call deadline  |
