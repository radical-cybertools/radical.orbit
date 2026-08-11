# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

**IMPORTANT: Always plan first, then wait for the user's literal "go" before implementing anything.** Do not write code, edit files, or make changes until explicitly told to proceed.

## Project Overview

ORBIT is a broker-based distributed framework that connects external RCT (RADICAL-Cybertools) applications with HPC resources. It is a **star**: one active **broker** hub routes between **endpoint** participants, each of which dials the hub over a single outbound WebSocket and may serve plugins, consume them, or both. A broker-hosted **gateway** module serves an HTTP/SSE/Explorer compatibility surface for non-participant callers.

## Build & Install

```sh
pip install .
```

## Running Locally

Requires two terminals (optionally three for testing):

```sh
# Terminal 1 – Broker (the active broker + gateway, public-facing)
./bin/radical-orbit-broker.py            # add --no-gateway for a headless broker

# Terminal 2 – Endpoint (HPC side, dials the broker over an outbound WebSocket)
./bin/radical-orbit-endpoint-wrapper.sh  # preferred: sets up PATH and PYTHONPATH
# or: ./bin/radical-orbit-endpoint.py

# Terminal 3 – Test client (optional)
python examples/example_sysinfo.py   # System info
python examples/example_psij.py      # PsiJ job submission
python examples/example_rhapsody.py  # Rhapsody tasks
python examples/example_endpoint.py      # Submit a child endpoint service as a batch job
```

The `radical-orbit-broker.py` filename runs the **active broker** — a WebSocket
`/register` hub that routes between participants — with the **gateway** compat
tier (HTTP REST, SSE `/events`, and the Explorer UI at the root URL, e.g.
`http://localhost:8000/`) attached on the same port by default; `--no-gateway`
disables it. The endpoint runs the participant runtime, which dials the broker
over one outbound WebSocket and may serve and/or consume plugins.

For HTTPS, generate a self-signed cert first:
```sh
openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 -subj "/CN=localhost"
```

### Embedded broker

A consumer client can host the full broker (routing + hosted plugins +
gateway) inside its own process instead of connecting out:
`EndpointRuntime(embedded=True)` (implementation: `embedded.py`,
`EmbeddedBroker`). The embedded broker binds port 8000 (falls back to an
ephemeral port with a warning when taken), uses the same operator-placed
cert/key/token as a standalone broker, and never writes under
`~/.radical/orbit`. Remote endpoints connect to `rt.broker_url` exactly as
they would to a standalone broker; `submit_tunneled` children inherit that
URL automatically. `embedded=True` skips URL resolution entirely (an ambient
`$RADICAL_ORBIT_BROKER_URL` is ignored); combining it with an explicit
`broker_url` raises. For host/port/auth/plugin control, construct
`EmbeddedBroker` directly and pass `broker_url=eb.url`. The standalone
`radical-orbit-broker.py` remains the deployment for shared, multi-client
brokers.

### Ingress auth token

The broker gates its HTTP ingress and the endpoint `/register` handshake with a
**shared bearer token**. Like the cert/key pair, the token is operator-placed —
the software never generates or writes it (`~/.radical/orbit` config is
read-only for the code; `utils.TOKEN_RECIPE` / `radical-orbit-broker.py --help`
carry the one-time creation recipe). The token value is **never** printed to
stdout (only its source/path), so it can't leak into captured logs (CWE-532).
Resolution (both sides): `--token` > `$RADICAL_ORBIT_BROKER_TOKEN` >
`~/.radical/orbit/broker.token` — so same-host clients/endpoints pick it up with
no config; for a remote broker, copy the token or set the env var. The broker
refuses to start without a token unless auth is disabled explicitly:
`auth=False` on the `Broker` ctor / `--no-auth` on the bin (local dev only; no
env-var escape hatch). The Explorer prompts for the token and then rides an
HttpOnly cookie minted by `POST /auth`. Helpers live in `utils.py`
(`resolve_broker_token`, `tokens_match`, `TOKEN_RECIPE`, `TLS_RECIPE`);
the broker core gates the WS `/register` handshake (`Broker._auth_dispatch`) and
the gateway gates HTTP ingress (`Gateway._auth_dispatch`, minting the `/auth`
cookie). This is the interim credential; a future per-participant identity
(mTLS) generalizes it.

The broker URL similarly has **no file fallback**: clients/endpoints resolve it
from the API/CLI arg > `$RADICAL_ORBIT_BROKER_URL` only (a stale on-disk URL
was a recurring source of confusion).

## Testing

```sh
pytest tests/unittests/      # unit tests (867 tests)
pytest tests/integration/    # integration tests (require running services)
```

## Linting

```sh
flake8 src/ bin/             # config in .flake8
pylint src/radical/orbit/     # config in .pylintrc
```

The flake8 config ignores many whitespace/formatting rules to match the project's alignment-heavy coding style.

## Architecture

### Star routing model

1. **Broker** (`bin/radical-orbit-broker.py`, `broker.py`) – the active hub. A lean routing loop switches messages between participants by `src`/`dst`, correlates request/response both ways via `corr_id`, tracks topology + two-stage (`suspect` → `lost`) liveness, and fans out events to subscribers. The broker is itself a participant (`src='broker'`) and hosts plugins (e.g. the task dispatcher) on their own loop/thread. Transport-level WebSocket keepalive drives liveness, structurally isolated from plugin behaviour.

2. **Endpoint** (`bin/radical-orbit-endpoint.py`, wrapper: `bin/radical-orbit-endpoint-wrapper.sh`, `runtime.py`) – a participant on an HPC login/compute node. It dials the broker over a single outbound WebSocket (firewall-friendly) and may **serve** plugins, **consume** other participants' plugins, or both. A dedicated transport thread owns the socket + keepalive; parse and plugin work run on a separate work loop; user callbacks fire on a third dispatcher thread — so a slow or blocking plugin never affects liveness.

3. **Gateway** (`gateway.py`) – a broker-hosted module (on by default) that serves the HTTP/SSE/Explorer compatibility surface for non-participant callers: the catch-all HTTP proxy over the broker's routing table, the `/events` SSE fan-out, the Explorer UI, CORS, and the ingress token gate.

4. **Plugins** – extend a participant (endpoint or broker) with domain-specific functionality, each under a unique namespace to avoid route collisions.

### Gateway HTTP surface

The gateway preserves these HTTP paths for the Explorer and other HTTP clients:
- `POST /endpoint/list` – List connected endpoints and their plugins
- `POST /endpoint/disconnect/{endpoint_name}` – Disconnect and terminate an endpoint
- `POST /broker/terminate` – Terminate the broker process
- `GET /events` – SSE stream for real-time notifications
- `/{endpoint_name}/{plugin_namespace}/...` – HTTP requests mapped onto `(dst, path)` and routed to the participant's plugins

### Plugin system

- **Base class**: `src/radical/orbit/plugin_base.py` – provides namespace isolation, session management, route-registration helpers, and notification support.
- **Session base**: `src/radical/orbit/plugin_session_base.py` – per-client session state management.
- **Client API**: `src/radical/orbit/client.py` – Python client for broker/endpoint interaction with notification callback support.

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup. Client API: `SysInfoClient.homedir()` (session-less, returns endpoint home dir), `get_metrics()` (requires session).
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-orbit-endpoint-wrapper.sh`. Stores job metadata at submit time. Client API: `submit_job(job_spec, executor)`, `get_job_status(job_id, stdout_offset, stderr_offset)` (streams stdout/stderr with byte offsets), `list_jobs()`, `cancel_job(job_id)`, `submit_tunneled(job_spec, executor, tunnel='none'|'forward'|'reverse')` (spawns child endpoint via batch job; see tunnel section below), `tunnel_status(endpoint_name)` (session-less, returns `{status, port, pid}`). Notification topic: `job_status` → `{job_id, state, exit_code, stdout, stderr}`.
- **Tunnel implementation** — three runtime modes selected per-target via the `tunnel` field on `submit_tunneled` (and on the IRI/PsiJ entries in `examples/{amsc,matey}.py`):
  - `'none'` — child connects directly to the broker.  No SSH spawn.
  - `'forward'` (compute→login) — `submit_tunneled` injects `--tunnel forward` and `--tunnel-via <login_hostname>` into the child's argv.  The child opens `ssh -L <port>:<broker_host>:<broker_port> <login_host> -N` itself, writes `~/.radical/orbit/tunnels/<endpoint_name>.port` on the shared filesystem, then rewrites its broker URL to `https://localhost:<port>`.  Used on Aurora / Perlmutter (compute→login SSH allowed; reverse direction blocked).  Failure surfaces naturally as the job's `FAILED` state — no parent-side cancel needed.
  - `'reverse'` (login→compute) — child gets `--tunnel reverse` only; the parent-side `_tunnel_watcher` (running inside the login-node `plugin_psij`) waits for the batch job to reach `RUNNING`, asks `BatchSystem.job_nodes(native_id)` for the compute hostname, and spawns `ssh -R 0:<broker_host>:<broker_port> <compute_host> -N` itself.  The remote port is parsed from sshd's `"Allocated port N"` stderr line and written to the rendezvous file; the child reads the same file path and connects to `https://localhost:<port>`.  On any spawn / lifetime failure the watcher records the reason in `_failure_reasons[job_id]` and cancels the now-useless allocation; `get_job_status` then synthesises `state='FAILED'` with the recorded `error` so a client poll bails early — operator-initiated cancels (no entry in `_failure_reasons`) keep their natural `CANCELLED` state.  Used on Odo (compute→login blocked; login→compute allowed).
  Spawn + port-parsing logic for both directions lives in `src/radical/orbit/tunnel.py` (`spawn_tunnel`, `spawn_reverse_tunnel`) and is test-covered.  Login host resolution for forward mode: `--tunnel-via` CLI arg → `$PBS_O_HOST` → `$SLURM_SUBMIT_HOST`.  The boolean `tunnel=True/False` form is no longer accepted — must be one of the three string values.
- **Node discovery**: `BatchSystem.job_nodes(native_id)` returns allocated node hostnames; SLURM uses `squeue`/`scontrol show hostnames`, PBSPro parses `qstat -f exec_host`. Used by the tunnel watcher.
- **BatchSystem abstraction** (`batch_system.py`, `batch_system_slurm.py`, `batch_system_pbs.py`) – isolates scheduler-specific behaviour. `detect_batch_system()` returns the active backend (`SlurmBatchSystem`, `PBSProBatchSystem`, or `NullBatchSystem`). All schedulers expose a normalized state vocabulary (`PENDING`/`RUNNING`/`DONE`/`FAILED`/`CANCELLED`/`HELD`/`UNKNOWN`); callers compare against constants from `batch_system`, never raw scheduler strings. To add a new backend (e.g. LSF, Cobalt): subclass `BatchSystem`, implement the abstract methods, and call `register_backend(YourBackend)` at module load.
- **queue_info** (`plugin_queue_info.py`) – Batch queue/partition info, job listings, and allocations. Backend selected automatically via `make_queue_info()` factory: SLURM (`queue_info_slurm.py`, sinfo/squeue/sacctmgr), PBSPro (`queue_info_pbs.py`, qstat/pbsnodes; allocations not available — PBSPro has no native sacctmgr equivalent), or no-op (`queue_info_none.py`). Shared backend with caching. Background prefetch on startup. Client API: `backend()` (session-less, returns `'slurm'`/`'pbs'`/`'none'`), `job_allocation()` (session-less, returns `{job_id, partition, n_nodes, nodelist, cpus_per_node, gpus_per_node, account, job_name, runtime}` or None), `get_info(user, force)`, `list_jobs(queue, user, force)`, `list_all_jobs(user, force)`, `cancel_job(job_id)`, `list_allocations(user, force)`.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (default: Dragon V3). Registers backend callbacks for intermediate state notifications (e.g. RUNNING). Client API: `submit_tasks(tasks)`, `wait_tasks(uids, timeout)`, `list_tasks()`, `get_task(uid)`, `cancel_task(uid)`, `cancel_all_tasks()`. Function tasks supported via cloudpickle (``"function": "cloudpickle::<base64>"``, ``"_pickled_fields": [...]``) or import path (``"function": "module:func"``). Resource specs via ``task_backend_specific_kwargs`` (timeout, ranks, type, process_template). Session accepts optional `backends` list. Notification topics: `session_status` → `{sid, status}` on session init ready/failed; `task_status` → `{uid, state}` on RUNNING, `{uid, state, exit_code, return_value, error, exception}` on terminal states; `task_status_batch` → `{tasks: [...]}` for bulk terminal notifications. Client-side optimizations: template compression for homogeneous batches, size-aware pipelined submission, SSE-based wait with event wakeup.
- **lucid** (`plugin_lucid.py`) – RADICAL Pilot integration. Client API: `pilot_submit(description)`, `task_submit(description)`, `task_wait(tid)`.
- **xgfabric** (`plugin_xgfabric.py`) – ExaGraph fabric operations. Classifies connected endpoints as `immediate_clusters` (direct execution) or `allocate_clusters` (batch submission via SLURM). An endpoint is classified as `allocate` only if it has the `queue_info` plugin **and** `is_enabled` returns `true`; otherwise it is `immediate`. Cluster lists updated in real-time via `on_topology_change`. Client API: `get_workdir()`, `set_workdir(path)`, `list_configs()`, `load_config(name)` (also accepts `'default'`/`'test'` builtins), `save_config(cfg)`, `delete_config(name)`, `get_status()`, `start_workflow(workflow, resource)`, `stop_workflow()`. Notification topic: `workflow_status` → full workflow state dict.
- **staging** (`plugin_staging.py`) – File transfer between client and endpoint. Paths must be absolute (or use `~/...`) and within `$HOME` or `/tmp`. Never overwrites existing files. Client API: `put(local_src, remote_dst, overwrite=False)`, `get(remote_src, local_dst)`, `list(remote_path)` → `{path, entries: [{name, type, size}]}`.
- **globus** (`plugin_globus.py`) – File staging via Globus Online (Transfer API). Endpoint-only (gated off the broker; also disabled when `globus-sdk` is absent). Orchestrator only: Globus moves data **collection-to-collection** out of band, so no bytes flow through endpoint or broker — distinct from the byte-streaming `staging` plugin. Synchronous `globus-sdk` calls are offloaded with `asyncio.to_thread`. **Auth** is supplied at `register_session`: either `access_token` (wrapped in `AccessTokenAuthorizer`; not renewed — re-register on expiry) **or** `refresh_token`+`client_id` (wrapped in `RefreshTokenAuthorizer`; auto-renews, survives long transfers). The credential lives in endpoint process memory, **never** on disk. **Collections** are UUIDs passed explicitly; the literal "local" resolves to the endpoint's configured collection (`RADICAL_ORBIT_GLOBUS_COLLECTION` env var, or a `local_collection` override at `register_session`). Client API: `submit_transfer(source, destination, items, label, sync_level)` (items = `[{source, destination, recursive}]`) → `{task_id, submission_id, status}`, `get_task(task_id)`, `task_wait(task_id, timeout, polling_interval)`, `cancel_task(task_id)`, `list_tasks(limit)`, `ls(collection, path)`, `mkdir(collection, path)`, `rename(collection, oldpath, newpath)`, `delete(collection, paths, recursive, label)` (Globus delete task), `endpoint_search(filter_text, limit)`, `get_endpoint(endpoint_id)`. Background poller (~10 s) emits notification topic `transfer_status` → `{task_id, status, label, bytes_transferred, files_transferred, nice_status}` on task state change. `ConsentRequired` (mapped-collection `data_access`) is surfaced as a clear 401 telling the caller to re-acquire a token with the collection's `data_access` scope. Explorer UI: `src/radical/orbit/data/plugins/globus.js`.
- **iri_connect** (`plugin_iri_connect.py`) – IRI endpoint configurator (broker-only). Lists available IRI endpoints and, on `connect(endpoint, token)`, dynamically registers a `PluginIRIInstance` under the instance name `iri.<endpoint>` (e.g. `iri.nersc`). Hardcoded endpoints: NERSC (`https://api.iri.nersc.gov`, Globus auth), OLCF (`https://amsc-open.s3m.olcf.ornl.gov`, S3M auth). Endpoint constants in `iri_endpoints.py`; shares the `iri_tokens` localStorage key with the Explorer UI. Client API: `list_endpoints()`, `connect(endpoint, token)` → returns an `IRIInstanceClient` bound to the new `iri.<endpoint>` instance (idempotent: on 409 returns a client for the existing instance), `disconnect(endpoint)`, `get_status()`.
- **iri.&lt;endpoint&gt;** (`plugin_iri_instance.py`, class `PluginIRIInstance`, not auto-registered) – per-endpoint IRI integration dynamically created by `iri_connect`. Combines job submission and resource info on a single pre-created session (no `{sid}` in routes; `register_session` always returns the fixed session ID). The bearer token lives in broker process memory (inside the httpx client) for the lifetime of the instance and is **never** written to disk. Background job poller every 10 s. Client API (`IRIInstanceClient`): `list_resources(resource_type='compute')`, `get_resource(resource_id)`, `submit_job(resource_id, job_spec)`, `get_job_status(resource_id, job_id)`, `list_jobs(resource_id)`, `cancel_job(resource_id, job_id)`, `list_incidents()`, `list_projects()`, `list_allocations(project_id)`. Notification topic: `job_status` → `{job_id, state, resource_id, name, details}`.

### WebSocket protocol

Broker ↔ endpoint messages share **one symmetric, versioned envelope** (defined in `protocol.py`), encoded **msgpack-only** on the wire — `body` is native bytes (no base64, no JSON path). Common fields: `version, id, corr_id?, channel?, kind, src, dst?`; `kind` discriminates:
- `request` / `response` – correlated RPC, routable in either direction by `dst`.
- `event` – push notification (`plugin, topic, session?, ts, seq, data`).
- `register` / `register_ack` – the identity + resume-key handshake.
- `subscribe` / `unsubscribe` – interest patterns for event delivery.
- `topology` – rich participant/plugin/liveness snapshot.
- `control` – `shutdown` / `error` / `terminate` / `disconnect`.

Liveness is **transport-level only** (WS protocol ping/pong; no app-level heartbeat in the envelope). A protocol-level frame-size cap bounds each frame; oversized payloads are the sender's problem (rhapsody batches size-aware).

### Notifications

Plugins can send real-time notifications to consumers as `event` frames (and, for HTTP clients, over Server-Sent Events via the gateway).
The notification flow is: **Session → Plugin → participant runtime → Broker → subscribed consumers (and the gateway's SSE fan-out)**.

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
const eventSource = new EventSource('http://broker:8000/events');
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

The Python SDK is `EndpointRuntime` (a zero-plugin consumer participant, exported as `Endpoint`); it and the `PluginClient` helpers provide callback-based notification support:

```python
from radical.orbit import EndpointRuntime

# Join the star as a consumer (broker URL: --arg > env > file)
rt = EndpointRuntime(broker_url="https://localhost:8000")

# Option 1: Global callback (all notifications)
def on_any_notification(endpoint, plugin, topic, data):
    print(f"{endpoint}/{plugin}: {topic} -> {data}")

rt.register_callback(callback=on_any_notification)

# Option 2: Plugin-specific callback
def on_psij_notification(endpoint, plugin, topic, data):
    print(f"PsiJ: {topic} -> {data}")

rt.register_callback(endpoint_id="hpc1", plugin_name="psij", callback=on_psij_notification)

# Option 3: Topic-specific callback
def on_job_status(endpoint, plugin, topic, data):
    print(f"Job {data['job_id']}: {data['status']}")

rt.register_callback(endpoint_id="hpc1", plugin_name="psij",
                     topic="job_status", callback=on_job_status)

# Option 4: Via the PluginClient helper (most common)
psij = rt.get_plugin("hpc1", "psij")
psij.register_notification_callback(on_job_status, topic="job_status")

# Topology changes (endpoint connect/disconnect)
def on_topology(endpoints):
    print(f"Connected endpoints: {list(endpoints.keys())}")

rt.register_topology_callback(on_topology)

# Cleanup
rt.close()
```

#### Subscribing to notifications (raw SSE)

For non-Python clients or custom implementations:

```python
import json
import sseclient
import requests

response = requests.get('http://broker:8000/events', stream=True)
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

The broker serves a web-based explorer (`src/radical/orbit/data/orbit_explorer.html`) that provides:
- Real-time view of connected endpoints and plugins
- Interactive plugin interfaces (job submission, task management, system metrics)
- Endpoint and broker termination controls
- SSE-based live updates

## Code Conventions

- Package uses `find_namespace_packages` under `src/radical/orbit/`.
- Scripts in `bin/` are installed as console entry points.
- The codebase uses alignment-style formatting (extra spaces for visual column alignment) – this is intentional and should be preserved.
- Version is derived from `VERSION` file + git tags at build time (see `setup.py:get_version`).
- The symmetric, versioned, msgpack-only wire envelope lives in `protocol.py` (Pydantic v2 validation; `parse_message`).
- UI configuration via `ui_schema.py` for dynamic plugin interfaces.
