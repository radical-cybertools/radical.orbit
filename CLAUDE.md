# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

**IMPORTANT: Always plan first, then wait for the user's literal "go" before implementing anything.** Do not write code, edit files, or make changes until explicitly told to proceed.

## Project Overview

RADICAL Edge is a bridge-based distributed framework that connects external RCT (RADICAL-Cybertools) applications with HPC resources. It uses a three-tier architecture: **Client → Bridge → Edge**, communicating over HTTPS and WebSockets.

## Build & Install

```sh
pip install .
```

## Running Locally

Requires two terminals (optionally three for testing):

```sh
# Terminal 1 – Bridge (reverse proxy, public-facing)
./bin/radical-edge-bridge.py

# Terminal 2 – Edge service (HPC side, connects to bridge via WebSocket)
./bin/radical-edge-wrapper.sh  # preferred: sets up PATH and PYTHONPATH
# or: ./bin/radical-edge-service.py

# Terminal 3 – Test client (optional)
python examples/example_sysinfo.py   # System info
python examples/example_psij.py      # PsiJ job submission
python examples/example_rhapsody.py  # Rhapsody tasks
python examples/example_edge.py      # Submit a child edge service as a batch job
```

The bridge includes a web-based **Explorer UI** at the root URL (e.g., `http://localhost:8000/`).

For HTTPS, generate a self-signed cert first:
```sh
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=localhost"
```

## Testing

```sh
pytest tests/unittests/      # unit tests (231 tests)
pytest tests/integration/    # integration tests (require running services)
```

## Linting

```sh
flake8 src/ bin/             # config in .flake8
pylint src/radical/edge/     # config in .pylintrc
```

The flake8 config ignores many whitespace/formatting rules to match the project's alignment-heavy coding style.

## Architecture

### Three-tier request flow

1. **Bridge** (`bin/radical-edge-bridge.py`) – FastAPI server acting as reverse proxy. Clients send HTTP requests; the bridge forwards them to the appropriate edge over a persistent WebSocket, then returns the response. Correlates requests via UUID. Provides SSE endpoint (`/events`) for real-time notifications.

2. **Edge** (`bin/radical-edge-service.py`, wrapper: `bin/radical-edge-wrapper.sh`) – FastAPI service on HPC nodes. Initiates an outbound WebSocket connection to the bridge (firewall-friendly). Receives forwarded requests from the bridge, dispatches them to locally-mounted plugin routes via HTTP loopback, and returns results.

3. **Plugins** – extend the edge with domain-specific functionality. Each plugin gets a unique namespace (`/{plugin_name}/{uuid}/`) to avoid route collisions.

### Bridge REST API

Key endpoints:
- `POST /edge/list` – List connected edges and their plugins
- `POST /edge/disconnect/{edge_name}` – Disconnect and terminate an edge
- `POST /bridge/terminate` – Terminate the bridge process
- `GET /events` – SSE stream for real-time notifications
- `/{edge_name}/{plugin_namespace}/...` – Proxied requests to edge plugins

### Plugin system

- **Base class**: `src/radical/edge/plugin_base.py` – provides namespace isolation, session management, route-registration helpers, and notification support.
- **Session base**: `src/radical/edge/plugin_session_base.py` – per-client session state management.
- **Client API**: `src/radical/edge/client.py` – Python client for bridge/edge interaction with notification callback support.

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup. Client API: `SysInfoClient.homedir()` (session-less, returns edge home dir), `get_metrics()` (requires session).
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-edge-wrapper.sh`. Stores job metadata at submit time. Client API: `submit_job(job_spec, executor)`, `get_job_status(job_id, stdout_offset, stderr_offset)` (streams stdout/stderr with byte offsets), `list_jobs()`, `cancel_job(job_id)`, `submit_tunneled(job_spec, executor, tunnel='none'|'forward'|'reverse')` (spawns child edge via batch job; see tunnel section below), `tunnel_status(edge_name)` (session-less, returns `{status, port, pid}`). Notification topic: `job_status` → `{job_id, state, exit_code, stdout, stderr}`.
- **Tunnel implementation** — three runtime modes selected per-target via the `tunnel` field on `submit_tunneled` (and on the IRI/PsiJ entries in `examples/{amsc,matey}.py`):
  - `'none'` — child connects directly to the bridge.  No SSH spawn.
  - `'forward'` (compute→login) — `submit_tunneled` injects `--tunnel forward` and `--tunnel-via <login_hostname>` into the child's argv.  The child opens `ssh -L <port>:<bridge_host>:<bridge_port> <login_host> -N` itself, writes `~/.radical/edge/tunnels/<edge_name>.port` on the shared filesystem, then rewrites its bridge URL to `https://localhost:<port>`.  Used on Aurora / Perlmutter (compute→login SSH allowed; reverse direction blocked).  Failure surfaces naturally as the job's `FAILED` state — no parent-side cancel needed.
  - `'reverse'` (login→compute) — child gets `--tunnel reverse` only; the parent-side `_tunnel_watcher` (running inside the login-node `plugin_psij`) waits for the batch job to reach `RUNNING`, asks `BatchSystem.job_nodes(native_id)` for the compute hostname, and spawns `ssh -R 0:<bridge_host>:<bridge_port> <compute_host> -N` itself.  The remote port is parsed from sshd's `"Allocated port N"` stderr line and written to the rendezvous file; the child reads the same file path and connects to `https://localhost:<port>`.  On any spawn / lifetime failure the watcher records the reason in `_failure_reasons[job_id]` and cancels the now-useless allocation; `get_job_status` then synthesises `state='FAILED'` with the recorded `error` so a client poll bails early — operator-initiated cancels (no entry in `_failure_reasons`) keep their natural `CANCELLED` state.  Used on Odo (compute→login blocked; login→compute allowed).
  Spawn + port-parsing logic for both directions lives in `src/radical/edge/tunnel.py` (`spawn_tunnel`, `spawn_reverse_tunnel`) and is test-covered.  Login host resolution for forward mode: `--tunnel-via` CLI arg → `$PBS_O_HOST` → `$SLURM_SUBMIT_HOST`.  The boolean `tunnel=True/False` form is no longer accepted — must be one of the three string values.
- **Node discovery**: `BatchSystem.job_nodes(native_id)` returns allocated node hostnames; SLURM uses `squeue`/`scontrol show hostnames`, PBSPro parses `qstat -f exec_host`. Used by the tunnel watcher.
- **BatchSystem abstraction** (`batch_system.py`, `batch_system_slurm.py`, `batch_system_pbs.py`) – isolates scheduler-specific behaviour. `detect_batch_system()` returns the active backend (`SlurmBatchSystem`, `PBSProBatchSystem`, or `NullBatchSystem`). All schedulers expose a normalized state vocabulary (`PENDING`/`RUNNING`/`DONE`/`FAILED`/`CANCELLED`/`HELD`/`UNKNOWN`); callers compare against constants from `batch_system`, never raw scheduler strings. To add a new backend (e.g. LSF, Cobalt): subclass `BatchSystem`, implement the abstract methods, and call `register_backend(YourBackend)` at module load.
- **queue_info** (`plugin_queue_info.py`) – Batch queue/partition info, job listings, and allocations. Backend selected automatically via `make_queue_info()` factory: SLURM (`queue_info_slurm.py`, sinfo/squeue/sacctmgr), PBSPro (`queue_info_pbs.py`, qstat/pbsnodes; allocations not available — PBSPro has no native sacctmgr equivalent), or no-op (`queue_info_none.py`). Shared backend with caching. Background prefetch on startup. Client API: `backend()` (session-less, returns `'slurm'`/`'pbs'`/`'none'`), `job_allocation()` (session-less, returns `{job_id, partition, n_nodes, nodelist, cpus_per_node, gpus_per_node, account, job_name, runtime}` or None), `get_info(user, force)`, `list_jobs(queue, user, force)`, `list_all_jobs(user, force)`, `cancel_job(job_id)`, `list_allocations(user, force)`.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (default: Dragon V3). Registers backend callbacks for intermediate state notifications (e.g. RUNNING). Client API: `submit_tasks(tasks)`, `wait_tasks(uids, timeout)`, `list_tasks()`, `get_task(uid)`, `cancel_task(uid)`, `cancel_all_tasks()`. Function tasks supported via cloudpickle (``"function": "cloudpickle::<base64>"``, ``"_pickled_fields": [...]``) or import path (``"function": "module:func"``). Resource specs via ``task_backend_specific_kwargs`` (timeout, ranks, type, process_template). Session accepts optional `backends` list. Notification topics: `session_status` → `{sid, status}` on session init ready/failed; `task_status` → `{uid, state}` on RUNNING, `{uid, state, exit_code, return_value, error, exception}` on terminal states; `task_status_batch` → `{tasks: [...]}` for bulk terminal notifications. Client-side optimizations: template compression for homogeneous batches, size-aware pipelined submission, SSE-based wait with event wakeup.
- **lucid** (`plugin_lucid.py`) – RADICAL Pilot integration. Client API: `pilot_submit(description)`, `task_submit(description)`, `task_wait(tid)`.
- **xgfabric** (`plugin_xgfabric.py`) – ExaGraph fabric operations. Classifies connected edges as `immediate_clusters` (direct execution) or `allocate_clusters` (batch submission via SLURM). An edge is classified as `allocate` only if it has the `queue_info` plugin **and** `is_enabled` returns `true`; otherwise it is `immediate`. Cluster lists updated in real-time via `on_topology_change`. Client API: `get_workdir()`, `set_workdir(path)`, `list_configs()`, `load_config(name)` (also accepts `'default'`/`'test'` builtins), `save_config(cfg)`, `delete_config(name)`, `get_status()`, `start_workflow(workflow, resource)`, `stop_workflow()`. Notification topic: `workflow_status` → full workflow state dict.
- **staging** (`plugin_staging.py`) – File transfer between client and edge. Paths must be absolute (or use `~/...`) and within `$HOME` or `/tmp`. Never overwrites existing files. Client API: `put(local_src, remote_dst, overwrite=False)`, `get(remote_src, local_dst)`, `list(remote_path)` → `{path, entries: [{name, type, size}]}`.
- **iri_connect** (`plugin_iri_connect.py`) – IRI endpoint configurator (bridge-only). Lists available IRI endpoints and, on `connect(endpoint, token)`, dynamically registers a `PluginIRIInstance` under the instance name `iri.<endpoint>` (e.g. `iri.nersc`). Hardcoded endpoints: NERSC (`https://api.iri.nersc.gov`, Globus auth), OLCF (`https://amsc-open.s3m.olcf.ornl.gov`, S3M auth). Endpoint constants in `iri_endpoints.py`; shares the `iri_tokens` localStorage key with the Explorer UI. Client API: `list_endpoints()`, `connect(endpoint, token)` → returns an `IRIInstanceClient` bound to the new `iri.<endpoint>` instance (idempotent: on 409 returns a client for the existing instance), `disconnect(endpoint)`, `get_status()`.
- **iri.&lt;endpoint&gt;** (`plugin_iri_instance.py`, class `PluginIRIInstance`, not auto-registered) – per-endpoint IRI integration dynamically created by `iri_connect`. Combines job submission and resource info on a single pre-created session (no `{sid}` in routes; `register_session` always returns the fixed session ID). The bearer token lives in bridge process memory (inside the httpx client) for the lifetime of the instance and is **never** written to disk. Background job poller every 10 s. Client API (`IRIInstanceClient`): `list_resources(resource_type='compute')`, `get_resource(resource_id)`, `submit_job(resource_id, job_spec)`, `get_job_status(resource_id, job_id)`, `list_jobs(resource_id)`, `cancel_job(resource_id, job_id)`, `list_incidents()`, `list_projects()`, `list_allocations(project_id)`. Notification topic: `job_status` → `{job_id, state, resource_id, name, details}`.

### WebSocket protocol

Bridge ↔ Edge messages are JSON with `type` field (defined in `models.py`):
- **Edge → Bridge**: `register`, `response`, `notification`, `pong`
- **Bridge → Edge**: `request`, `ping`, `error`, `shutdown`, `topology`

Binary payloads use base64 encoding (`is_binary` flag). Heartbeat via WebSocket ping/pong.

### Notifications

Plugins can send real-time notifications to clients via Server-Sent Events (SSE).
The notification flow is: **Session → Plugin → EdgeService → Bridge → SSE clients**.

#### Sending notifications from a plugin session

```python
# In your PluginSession subclass:
class MySession(PluginSession):
    def do_work(self):
        # ... do some work ...

        # Send notification (works from sync/async contexts and threads)
        if self._notify:
            self._notify("work_status", {
                "status": "completed",
                "result": {"key": "value"}
            })
```

#### Sending notifications from a plugin

```python
# In your Plugin subclass (async context):
await self.send_notification("my_topic", {"key": "value"})
```

#### Subscribing to notifications (JavaScript/Browser)

```javascript
const eventSource = new EventSource('http://bridge:8000/events');
eventSource.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.topic === 'notification') {
        const {edge, plugin, topic, data} = msg.data;
        console.log(`${edge}/${plugin}: ${topic}`, data);
    } else if (msg.topic === 'topology') {
        // Edge connect/disconnect event
        console.log('Topology changed:', msg.data.edges);
    }
};
```

#### Subscribing to notifications (Python client API)

The `BridgeClient` and `PluginClient` classes provide callback-based notification support:

```python
from radical.edge.client import BridgeClient

# Connect to bridge
client = BridgeClient(url="http://localhost:8000")

# Option 1: Global callback (all notifications)
def on_any_notification(edge, plugin, topic, data):
    print(f"{edge}/{plugin}: {topic} -> {data}")

client.register_callback(callback=on_any_notification)

# Option 2: Plugin-specific callback
def on_psij_notification(edge, plugin, topic, data):
    print(f"PsiJ: {topic} -> {data}")

client.register_callback(edge_id="hpc1", plugin_name="psij", callback=on_psij_notification)

# Option 3: Topic-specific callback
def on_job_status(edge, plugin, topic, data):
    print(f"Job {data['job_id']}: {data['status']}")

client.register_callback(edge_id="hpc1", plugin_name="psij",
                         topic="job_status", callback=on_job_status)

# Option 4: Via PluginClient (most common)
edge = client.get_edge_client("hpc1")
psij = edge.get_plugin("psij")
psij.register_notification_callback(on_job_status, topic="job_status")

# Topology changes (edge connect/disconnect)
def on_topology(edges):
    print(f"Connected edges: {list(edges.keys())}")

client.register_topology_callback(on_topology)

# Cleanup
client.close()
```

#### Subscribing to notifications (raw SSE)

For non-Python clients or custom implementations:

```python
import json
import sseclient
import requests

response = requests.get('http://bridge:8000/events', stream=True)
client = sseclient.SSEClient(response)
for event in client.events():
    msg = json.loads(event.data)
    if msg['topic'] == 'notification':
        edge = msg['data']['edge']
        plugin = msg['data']['plugin']
        topic = msg['data']['topic']
        data = msg['data']['data']
        print(f"{edge}/{plugin}: {topic} -> {data}")
```

#### Topology updates (edge connect/disconnect)

Plugins can react to edge connect/disconnect events by overriding `on_topology_change`:

```python
class MyPlugin(Plugin):
    async def on_topology_change(self, edges: dict):
        """Called when edges connect or disconnect.

        Args:
            edges: Dict mapping edge names to plugin info.
                   Example: {"edge1": {"plugins": ["sysinfo", "psij"]}}
        """
        for edge_name, info in edges.items():
            print(f"Edge {edge_name} has plugins: {info.get('plugins', [])}")
```

### Explorer UI

The bridge serves a web-based explorer (`src/radical/edge/data/edge_explorer.html`) that provides:
- Real-time view of connected edges and plugins
- Interactive plugin interfaces (job submission, task management, system metrics)
- Edge and bridge termination controls
- SSE-based live updates

## Code Conventions

- Package uses `find_namespace_packages` under `src/radical/edge/`.
- Scripts in `bin/` are installed as console entry points.
- The codebase uses alignment-style formatting (extra spaces for visual column alignment) – this is intentional and should be preserved.
- Version is derived from `VERSION` file + git tags at build time (see `setup.py:get_version`).
- Pydantic models for message validation in `models.py`.
- UI configuration via `ui_schema.py` for dynamic plugin interfaces.
