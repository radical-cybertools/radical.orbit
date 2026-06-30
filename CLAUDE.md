# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

**IMPORTANT: Always plan first, then wait for the user's literal "go" before implementing anything.** Do not write code, edit files, or make changes until explicitly told to proceed.

## Project Overview

ORBIT is a bridge-based distributed framework that connects external RCT (RADICAL-Cybertools) applications with HPC resources. It uses a three-tier architecture: **Client → Bridge → Endpoint**, communicating over HTTPS and WebSockets.

## Build & Install

```sh
pip install .
```

## Running Locally

Requires two terminals (optionally three for testing):

```sh
# Terminal 1 – Bridge (reverse proxy, public-facing)
./bin/radical-orbit-bridge.py

# Terminal 2 – Endpoint service (HPC side, connects to bridge via WebSocket)
./bin/radical-orbit-endpoint-wrapper.sh  # preferred: sets up PATH and PYTHONPATH
# or: ./bin/radical-orbit-endpoint.py

# Terminal 3 – Test client (optional)
python examples/example_sysinfo.py   # System info
python examples/example_psij.py      # PsiJ job submission
python examples/example_rhapsody.py  # Rhapsody tasks
python examples/example_endpoint.py      # Submit a child endpoint service as a batch job
```

The bridge includes a web-based **Explorer UI** at the root URL (e.g., `http://localhost:8000/`).

For HTTPS, generate a self-signed cert first:
```sh
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=localhost"
```

### Ingress auth token

The bridge gates its HTTP ingress and the endpoint `/register` handshake with a
**shared bearer token**. On first start it generates one and writes it to
`~/.radical/orbit/bridge.token` (0600); the token value is **never** printed to
stdout (only its source/path), so it can't leak into captured logs (CWE-532) —
read it from the file. Resolution
(client/endpoint side): `--token` > `$RADICAL_ORBIT_BRIDGE_TOKEN` >
`~/.radical/orbit/bridge.token` — so same-host clients/endpoints pick it up with
no config; for a remote bridge, copy the token or set the env var. The Explorer
prompts for it and then rides an HttpOnly cookie minted by `POST /auth`. Disable
the gate for local dev with `--no-auth` (or `RADICAL_ORBIT_BRIDGE_NO_AUTH=1`).
Helpers live in `utils.py` (`resolve_bridge_token`, `ensure_bridge_token`,
`auth_disabled`, `tokens_match`); the gate is the `Bridge._auth_dispatch`
middleware. This is the interim credential that the planned broker rework
generalizes into a per-participant identity (mTLS).

## Testing

```sh
pytest tests/unittests/      # unit tests (231 tests)
pytest tests/integration/    # integration tests (require running services)
```

## Linting

```sh
flake8 src/ bin/             # config in .flake8
pylint src/radical/orbit/     # config in .pylintrc
```

The flake8 config ignores many whitespace/formatting rules to match the project's alignment-heavy coding style.

## Architecture

### Three-tier request flow

1. **Bridge** (`bin/radical-orbit-bridge.py`) – FastAPI server acting as reverse proxy. Clients send HTTP requests; the bridge forwards them to the appropriate endpoint over a persistent WebSocket, then returns the response. Correlates requests via UUID. Provides SSE endpoint (`/events`) for real-time notifications.

2. **Endpoint** (`bin/radical-orbit-endpoint.py`, wrapper: `bin/radical-orbit-endpoint-wrapper.sh`) – FastAPI service on HPC nodes. Initiates an outbound WebSocket connection to the bridge (firewall-friendly). Receives forwarded requests from the bridge, dispatches them to locally-mounted plugin routes via HTTP loopback, and returns results.

3. **Plugins** – extend the endpoint with domain-specific functionality. Each plugin gets a unique namespace (`/{plugin_name}/{uuid}/`) to avoid route collisions.

### Bridge REST API

Key endpoints:
- `POST /endpoint/list` – List connected endpoints and their plugins
- `POST /endpoint/disconnect/{endpoint_name}` – Disconnect and terminate an endpoint
- `POST /bridge/terminate` – Terminate the bridge process
- `GET /events` – SSE stream for real-time notifications
- `/{endpoint_name}/{plugin_namespace}/...` – Proxied requests to endpoint plugins

### Plugin system

- **Base class**: `src/radical/orbit/plugin_base.py` – provides namespace isolation, session management, route-registration helpers, and notification support.
- **Session base**: `src/radical/orbit/plugin_session_base.py` – per-client session state management.
- **Client API**: `src/radical/orbit/client.py` – Python client for bridge/endpoint interaction with notification callback support.

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup. Client API: `SysInfoClient.homedir()` (session-less, returns endpoint home dir), `get_metrics()` (requires session).
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-orbit-endpoint-wrapper.sh`. Stores job metadata at submit time. Client API: `submit_job(job_spec, executor)`, `get_job_status(job_id, stdout_offset, stderr_offset)` (streams stdout/stderr with byte offsets), `list_jobs()`, `cancel_job(job_id)`, `submit_tunneled(job_spec, executor, tunnel='none'|'forward'|'reverse')` (spawns child endpoint via batch job; see tunnel section below), `tunnel_status(endpoint_name)` (session-less, returns `{status, port, pid}`). Notification topic: `job_status` → `{job_id, state, exit_code, stdout, stderr}`.
- **Tunnel implementation** — three runtime modes selected per-target via the `tunnel` field on `submit_tunneled` (and on the IRI/PsiJ entries in `examples/{amsc,matey}.py`):
  - `'none'` — child connects directly to the bridge.  No SSH spawn.
  - `'forward'` (compute→login) — `submit_tunneled` injects `--tunnel forward` and `--tunnel-via <login_hostname>` into the child's argv.  The child opens `ssh -L <port>:<bridge_host>:<bridge_port> <login_host> -N` itself, writes `~/.radical/orbit/tunnels/<endpoint_name>.port` on the shared filesystem, then rewrites its bridge URL to `https://localhost:<port>`.  Used on Aurora / Perlmutter (compute→login SSH allowed; reverse direction blocked).  Failure surfaces naturally as the job's `FAILED` state — no parent-side cancel needed.
  - `'reverse'` (login→compute) — child gets `--tunnel reverse` only; the parent-side `_tunnel_watcher` (running inside the login-node `plugin_psij`) waits for the batch job to reach `RUNNING`, asks `BatchSystem.job_nodes(native_id)` for the compute hostname, and spawns `ssh -R 0:<bridge_host>:<bridge_port> <compute_host> -N` itself.  The remote port is parsed from sshd's `"Allocated port N"` stderr line and written to the rendezvous file; the child reads the same file path and connects to `https://localhost:<port>`.  On any spawn / lifetime failure the watcher records the reason in `_failure_reasons[job_id]` and cancels the now-useless allocation; `get_job_status` then synthesises `state='FAILED'` with the recorded `error` so a client poll bails early — operator-initiated cancels (no entry in `_failure_reasons`) keep their natural `CANCELLED` state.  Used on Odo (compute→login blocked; login→compute allowed).
  Spawn + port-parsing logic for both directions lives in `src/radical/orbit/tunnel.py` (`spawn_tunnel`, `spawn_reverse_tunnel`) and is test-covered.  Login host resolution for forward mode: `--tunnel-via` CLI arg → `$PBS_O_HOST` → `$SLURM_SUBMIT_HOST`.  The boolean `tunnel=True/False` form is no longer accepted — must be one of the three string values.
- **Node discovery**: `BatchSystem.job_nodes(native_id)` returns allocated node hostnames; SLURM uses `squeue`/`scontrol show hostnames`, PBSPro parses `qstat -f exec_host`. Used by the tunnel watcher.
- **BatchSystem abstraction** (`batch_system.py`, `batch_system_slurm.py`, `batch_system_pbs.py`) – isolates scheduler-specific behaviour. `detect_batch_system()` returns the active backend (`SlurmBatchSystem`, `PBSProBatchSystem`, or `NullBatchSystem`). All schedulers expose a normalized state vocabulary (`PENDING`/`RUNNING`/`DONE`/`FAILED`/`CANCELLED`/`HELD`/`UNKNOWN`); callers compare against constants from `batch_system`, never raw scheduler strings. To add a new backend (e.g. LSF, Cobalt): subclass `BatchSystem`, implement the abstract methods, and call `register_backend(YourBackend)` at module load.
- **queue_info** (`plugin_queue_info.py`) – Batch queue/partition info, job listings, and allocations. Backend selected automatically via `make_queue_info()` factory: SLURM (`queue_info_slurm.py`, sinfo/squeue/sacctmgr), PBSPro (`queue_info_pbs.py`, qstat/pbsnodes; allocations not available — PBSPro has no native sacctmgr equivalent), or no-op (`queue_info_none.py`). Shared backend with caching. Background prefetch on startup. Client API: `backend()` (session-less, returns `'slurm'`/`'pbs'`/`'none'`), `job_allocation()` (session-less, returns `{job_id, partition, n_nodes, nodelist, cpus_per_node, gpus_per_node, account, job_name, runtime}` or None), `get_info(user, force)`, `list_jobs(queue, user, force)`, `list_all_jobs(user, force)`, `cancel_job(job_id)`, `list_allocations(user, force)`.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (default: Dragon V3). Registers backend callbacks for intermediate state notifications (e.g. RUNNING). Client API: `submit_tasks(tasks)`, `wait_tasks(uids, timeout)`, `list_tasks()`, `get_task(uid)`, `cancel_task(uid)`, `cancel_all_tasks()`. Function tasks supported via cloudpickle (``"function": "cloudpickle::<base64>"``, ``"_pickled_fields": [...]``) or import path (``"function": "module:func"``). Resource specs via ``task_backend_specific_kwargs`` (timeout, ranks, type, process_template). Session accepts optional `backends` list. Notification topics: `session_status` → `{sid, status}` on session init ready/failed; `task_status` → `{uid, state}` on RUNNING, `{uid, state, exit_code, return_value, error, exception}` on terminal states; `task_status_batch` → `{tasks: [...]}` for bulk terminal notifications. Client-side optimizations: template compression for homogeneous batches, size-aware pipelined submission, SSE-based wait with event wakeup.
- **lucid** (`plugin_lucid.py`) – RADICAL Pilot integration. Client API: `pilot_submit(description)`, `task_submit(description)`, `task_wait(tid)`.
- **xgfabric** (`plugin_xgfabric.py`) – ExaGraph fabric operations. Classifies connected endpoints as `immediate_clusters` (direct execution) or `allocate_clusters` (batch submission via SLURM). An endpoint is classified as `allocate` only if it has the `queue_info` plugin **and** `is_enabled` returns `true`; otherwise it is `immediate`. Cluster lists updated in real-time via `on_topology_change`. Client API: `get_workdir()`, `set_workdir(path)`, `list_configs()`, `load_config(name)` (also accepts `'default'`/`'test'` builtins), `save_config(cfg)`, `delete_config(name)`, `get_status()`, `start_workflow(workflow, resource)`, `stop_workflow()`. Notification topic: `workflow_status` → full workflow state dict.
- **staging** (`plugin_staging.py`) – File transfer between client and endpoint. Paths must be absolute (or use `~/...`) and within `$HOME` or `/tmp`. Never overwrites existing files. Client API: `put(local_src, remote_dst, overwrite=False)`, `get(remote_src, local_dst)`, `list(remote_path)` → `{path, entries: [{name, type, size}]}`.
- **globus** (`plugin_globus.py`) – File staging via Globus Online (Transfer API). Endpoint-only (gated off the bridge; also disabled when `globus-sdk` is absent). Orchestrator only: Globus moves data **collection-to-collection** out of band, so no bytes flow through endpoint or bridge — distinct from the byte-streaming `staging` plugin. Synchronous `globus-sdk` calls are offloaded with `asyncio.to_thread`. **Auth** is supplied at `register_session`: either `access_token` (wrapped in `AccessTokenAuthorizer`; not renewed — re-register on expiry) **or** `refresh_token`+`client_id` (wrapped in `RefreshTokenAuthorizer`; auto-renews, survives long transfers). The credential lives in endpoint process memory, **never** on disk. **Collections** are UUIDs passed explicitly; the literal "local" resolves to the endpoint's configured collection (`RADICAL_ORBIT_GLOBUS_COLLECTION` env var, or a `local_collection` override at `register_session`). Client API: `submit_transfer(source, destination, items, label, sync_level)` (items = `[{source, destination, recursive}]`) → `{task_id, submission_id, status}`, `get_task(task_id)`, `task_wait(task_id, timeout, polling_interval)`, `cancel_task(task_id)`, `list_tasks(limit)`, `ls(collection, path)`, `mkdir(collection, path)`, `rename(collection, oldpath, newpath)`, `delete(collection, paths, recursive, label)` (Globus delete task), `endpoint_search(filter_text, limit)`, `get_endpoint(endpoint_id)`. Background poller (~10 s) emits notification topic `transfer_status` → `{task_id, status, label, bytes_transferred, files_transferred, nice_status}` on task state change. `ConsentRequired` (mapped-collection `data_access`) is surfaced as a clear 401 telling the caller to re-acquire a token with the collection's `data_access` scope. Explorer UI: `src/radical/orbit/data/plugins/globus.js`.
- **iri_connect** (`plugin_iri_connect.py`) – IRI endpoint configurator (bridge-only). Lists available IRI endpoints and, on `connect(endpoint, token)`, dynamically registers a `PluginIRIInstance` under the instance name `iri.<endpoint>` (e.g. `iri.nersc`). Hardcoded endpoints: NERSC (`https://api.iri.nersc.gov`, Globus auth), OLCF (`https://amsc-open.s3m.olcf.ornl.gov`, S3M auth). Endpoint constants in `iri_endpoints.py`; shares the `iri_tokens` localStorage key with the Explorer UI. Client API: `list_endpoints()`, `connect(endpoint, token)` → returns an `IRIInstanceClient` bound to the new `iri.<endpoint>` instance (idempotent: on 409 returns a client for the existing instance), `disconnect(endpoint)`, `get_status()`.
- **iri.&lt;endpoint&gt;** (`plugin_iri_instance.py`, class `PluginIRIInstance`, not auto-registered) – per-endpoint IRI integration dynamically created by `iri_connect`. Combines job submission and resource info on a single pre-created session (no `{sid}` in routes; `register_session` always returns the fixed session ID). The bearer token lives in bridge process memory (inside the httpx client) for the lifetime of the instance and is **never** written to disk. Background job poller every 10 s. Client API (`IRIInstanceClient`): `list_resources(resource_type='compute')`, `get_resource(resource_id)`, `submit_job(resource_id, job_spec)`, `get_job_status(resource_id, job_id)`, `list_jobs(resource_id)`, `cancel_job(resource_id, job_id)`, `list_incidents()`, `list_projects()`, `list_allocations(project_id)`. Notification topic: `job_status` → `{job_id, state, resource_id, name, details}`.

### WebSocket protocol

Bridge ↔ Endpoint messages are JSON with `type` field (defined in `models.py`):
- **Endpoint → Bridge**: `register`, `response`, `notification`, `pong`
- **Bridge → Endpoint**: `request`, `ping`, `error`, `shutdown`, `topology`

Binary payloads use base64 encoding (`is_binary` flag). Heartbeat via WebSocket ping/pong.

### Notifications

Plugins can send real-time notifications to clients via Server-Sent Events (SSE).
The notification flow is: **Session → Plugin → EndpointService → Bridge → SSE clients**.

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
        const {endpoint, plugin, topic, data} = msg.data;
        console.log(`${endpoint}/${plugin}: ${topic}`, data);
    } else if (msg.topic === 'topology') {
        // Endpoint connect/disconnect event
        console.log('Topology changed:', msg.data.endpoints);
    }
};
```

#### Subscribing to notifications (Python client API)

The `BridgeClient` and `PluginClient` classes provide callback-based notification support:

```python
from radical.orbit.client import BridgeClient

# Connect to bridge
client = BridgeClient(url="http://localhost:8000")

# Option 1: Global callback (all notifications)
def on_any_notification(endpoint, plugin, topic, data):
    print(f"{endpoint}/{plugin}: {topic} -> {data}")

client.register_callback(callback=on_any_notification)

# Option 2: Plugin-specific callback
def on_psij_notification(endpoint, plugin, topic, data):
    print(f"PsiJ: {topic} -> {data}")

client.register_callback(endpoint_id="hpc1", plugin_name="psij", callback=on_psij_notification)

# Option 3: Topic-specific callback
def on_job_status(endpoint, plugin, topic, data):
    print(f"Job {data['job_id']}: {data['status']}")

client.register_callback(endpoint_id="hpc1", plugin_name="psij",
                         topic="job_status", callback=on_job_status)

# Option 4: Via PluginClient (most common)
endpoint = client.get_endpoint_client("hpc1")
psij = endpoint.get_plugin("psij")
psij.register_notification_callback(on_job_status, topic="job_status")

# Topology changes (endpoint connect/disconnect)
def on_topology(endpoints):
    print(f"Connected endpoints: {list(endpoints.keys())}")

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
        endpoint = msg['data']['endpoint']
        plugin = msg['data']['plugin']
        topic = msg['data']['topic']
        data = msg['data']['data']
        print(f"{endpoint}/{plugin}: {topic} -> {data}")
```

#### Topology updates (endpoint connect/disconnect)

Plugins can react to endpoint connect/disconnect events by overriding `on_topology_change`:

```python
class MyPlugin(Plugin):
    async def on_topology_change(self, endpoints: dict):
        """Called when endpoints connect or disconnect.

        Args:
            endpoints: Dict mapping endpoint names to plugin info.
                   Example: {"endpoint1": {"plugins": ["sysinfo", "psij"]}}
        """
        for endpoint_name, info in endpoints.items():
            print(f"Endpoint {endpoint_name} has plugins: {info.get('plugins', [])}")
```

### Explorer UI

The bridge serves a web-based explorer (`src/radical/orbit/data/orbit_explorer.html`) that provides:
- Real-time view of connected endpoints and plugins
- Interactive plugin interfaces (job submission, task management, system metrics)
- Endpoint and bridge termination controls
- SSE-based live updates

## Code Conventions

- Package uses `find_namespace_packages` under `src/radical/orbit/`.
- Scripts in `bin/` are installed as console entry points.
- The codebase uses alignment-style formatting (extra spaces for visual column alignment) – this is intentional and should be preserved.
- Version is derived from `VERSION` file + git tags at build time (see `setup.py:get_version`).
- Pydantic models for message validation in `models.py`.
- UI configuration via `ui_schema.py` for dynamic plugin interfaces.
