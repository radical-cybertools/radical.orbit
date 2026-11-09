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

The verify loop — run this after every code change, before declaring work done:

```sh
PYTHONPATH=src ve3/bin/python -m pytest tests/unittests/ -q
```

`ve3/` is the project venv; `PYTHONPATH=src` runs from the source tree so no
reinstall is needed between edits (never use editable installs). Plain
`pytest` works too if the environment is already active:

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

2. **Endpoint** (`bin/radical-orbit-endpoint.py`, wrapper: `bin/radical-orbit-endpoint-wrapper.sh`, `runtime.py`) – a participant on an HPC login/compute node. It dials the broker over a single outbound WebSocket (firewall-friendly) and may **serve** plugins, **consume** other participants' plugins, or both. A dedicated transport thread owns the socket + keepalive; parse and plugin work run on a separate work loop; user callbacks fire on a third dispatcher thread — so a slow or blocking plugin never affects liveness. `EndpointRuntime.stop()` calls every served plugin's `on_host_stop()` hook concurrently (each bounded by `_PLUGIN_SHUTDOWN` = 15 s; default no-op; for external resources only: it does not close sessions, which may outlive an endpoint restart, e.g. psij's output of running jobs); a broker `shutdown`/`terminate` control sets `runtime.terminated`, and the endpoint bin then stops and exits. Stop signals: under `dragon` the wrapper stays the launcher's parent and forwards TERM/INT (HUP as TERM) to the endpoint python process only, via the PID the endpoint writes to `$RADICAL_ORBIT_ENDPOINT_PIDFILE`, then exits with the launcher's exit code — signalling `dragon` itself breaks teardown (`dragon -s` has no SIGTERM handler: orphaned DDict managers, leaked /dev/shm; killing an srun step under `dragon -m` orphans its backend step). Without dragon the wrapper `exec`s python. Batch jobs: `#SBATCH --signal=B:TERM@<lead>` (signal the batch shell only, with lead time before walltime; plain `--signal=TERM@...` hits every job step, i.e. dragon's sruns), with the wrapper run as the job's process.

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
- **Session lifetime**: one idle timeout per session, fixed at `register_session(ttl=<s>)` (`0` = never; default: the plugin's `session_ttl` class attribute, 900 s; task dispatcher and xgfabric 86400 s). Activity: `{sid}` requests (while in flight), session notifications (`PluginSession.notify`), `touch/{sid}` (`PluginClient.touch()`), and `PluginSession.is_busy()` probed at the deadline. Any client holding a sid can attach (sharing; attachment security deferred to SwarmSlicer); owner/participant loss does not end sessions. Ended sids (expired or unregistered) are remembered for the process lifetime (capped 100k) and answer 410 with the reason; an idle expiry logs WARNING (likely missing `close()`), or INFO for Explorer sessions (`X-Orbit-Client: explorer`). The Explorer touches its sessions every 5 min.
- **Session base**: `src/radical/orbit/plugin_session_base.py` – per-client session state management.
- **Client API**: `src/radical/orbit/client.py` – Python client for broker/endpoint interaction with notification callback support.

**Available plugins:**
- **sysinfo** (`plugin_sysinfo.py`) – System info (hostname, OS, CPU, memory, disk, network, GPUs). Detects shared filesystems (Lustre, GPFS, NFS, DVS, etc.). Background prefetch on startup. Client API: `SysInfoClient.homedir()` (session-less, returns endpoint home dir), `get_metrics()` (requires session).
- **psij** (`plugin_psij.py`) – HPC job submission via PsiJ (supports local, SLURM, PBS, LSF). Background job state polling. Default executable: `radical-orbit-endpoint-wrapper.sh`. Stores job metadata at submit time. Client API: `submit_job(job_spec, executor)`, `get_job_status(job_id, stdout_offset, stderr_offset)` (streams stdout/stderr with byte offsets), `list_jobs()`, `cancel_job(job_id)`, `submit_tunneled(job_spec, executor, tunnel='none'|'forward'|'reverse')` (spawns child endpoint via batch job; see tunnel section below), `tunnel_status(endpoint_name)` (session-less, returns `{status, port, pid}`). Notification topic: `job_status` → `{job_id, state, exit_code, stdout, stderr}`. **Endpoint jobs** (`submit_tunneled`) are cancelled gracefully: `cancel_job` asks the broker to `disconnect` the child endpoint (an endpoint-hosted psij sends the control frame via `EndpointRuntime.disconnect_endpoint`; a broker-hosted psij uses `BrokerCaller.disconnect_threadsafe`), the endpoint unregisters, runs `on_host_stop` and exits, and only a confirmed end within `$RADICAL_ORBIT_PSIJ_CANCEL_GRACE` s (default 30; `0` = off; a terminal state from psij or, for batch executors, `BatchSystem.job_state`) skips `job.cancel()` (reply `graceful: true`); on timeout, or when the job only left the queue (`UNKNOWN` `UNKNOWN_TOLERANCE` times after a known state: Perlmutter's `squeue` drops a finished job at once, but also answers `UNKNOWN` when it fails), `job.cancel()` runs anyway (reply `graceful: false, endpoint_stopped: true`; a failing cancel is a 500 as for any job — cancelling an ended job is a no-op for psij and `scancel -Q`) (`scancel` signals every process of the job at once: no unregister, broker drops the endpoint only via suspect/lost, unclean Dragon teardown). Skipped (plain cancel) unless the job is running (psij `ACTIVE`, or batch `RUNNING`: a queued job cannot own the name), and when the endpoint is not connected, the name was already connected at submit (not ours), or is the host's own name. Ownership is name-based (recorded per job at submit); binding it to the batch job id is a follow-up — endpoints do not advertise their `SLURM_JOB_ID`/`PBS_JOBID` (no registration/topology field), so two *running* jobs with the same `-n` are not told apart. The broker itself does not authorise `disconnect` controls (any participant may send one). Job stdout/stderr live in `$RADICAL_ORBIT_PSIJ_DIR/output/<sid>/`; session `close()` removes them except those of endpoint jobs (path logged; aged out after 7 days).
- **Tunnel implementation** — three runtime modes selected per-target via the `tunnel` field on `submit_tunneled` (and on the IRI/PsiJ entries in `examples/{amsc,matey}.py`):
  - `'none'` — child connects directly to the broker.  No SSH spawn.
  - `'forward'` (compute→login) — `submit_tunneled` injects `--tunnel forward` and `--tunnel-via <login_hostname>` into the child's argv.  The child opens `ssh -L <port>:<broker_host>:<broker_port> <login_host> -N` itself, writes `~/.radical/orbit/tunnels/<endpoint_name>.port` on the shared filesystem, then rewrites its broker URL to `https://localhost:<port>`.  Used on Aurora / Perlmutter (compute→login SSH allowed; reverse direction blocked).  Failure surfaces naturally as the job's `FAILED` state — no parent-side cancel needed.
  - `'reverse'` (login→compute) — child gets `--tunnel reverse` only; the parent-side `_tunnel_watcher` (running inside the login-node `plugin_psij`) waits for the batch job to reach `RUNNING`, asks `BatchSystem.job_nodes(native_id)` for the compute hostname, and spawns `ssh -R 0:<broker_host>:<broker_port> <compute_host> -N` itself.  The remote port is parsed from sshd's `"Allocated port N"` stderr line and written to the rendezvous file; the child reads the same file path and connects to `https://localhost:<port>`.  On any spawn / lifetime failure the watcher records the reason in `_failure_reasons[job_id]` and cancels the now-useless allocation; `get_job_status` then synthesises `state='FAILED'` with the recorded `error` so a client poll bails early — operator-initiated cancels (no entry in `_failure_reasons`) keep their natural `CANCELLED` state.  Used on Odo (compute→login blocked; login→compute allowed).
  Spawn + port-parsing logic for both directions lives in `src/radical/orbit/tunnel.py` (`spawn_tunnel`, `spawn_reverse_tunnel`) and is test-covered.  Login host resolution for forward mode: `--tunnel-via` CLI arg → `$PBS_O_HOST` → `$SLURM_SUBMIT_HOST`.  The boolean `tunnel=True/False` form is no longer accepted — must be one of the three string values.
- **Node discovery**: `BatchSystem.job_nodes(native_id)` returns allocated node hostnames; SLURM uses `squeue`/`scontrol show hostnames`, PBSPro parses `qstat -f exec_host`. Used by the tunnel watcher.
- **BatchSystem abstraction** (`batch_system.py`, `batch_system_slurm.py`, `batch_system_pbs.py`) – isolates scheduler-specific behaviour. `detect_batch_system()` returns the active backend (`SlurmBatchSystem`, `PBSProBatchSystem`, or `NullBatchSystem`). All schedulers expose a normalized state vocabulary (`PENDING`/`RUNNING`/`DONE`/`FAILED`/`CANCELLED`/`HELD`/`UNKNOWN`); callers compare against constants from `batch_system`, never raw scheduler strings. To add a new backend (e.g. LSF, Cobalt): subclass `BatchSystem`, implement the abstract methods, and call `register_backend(YourBackend)` at module load.
- **queue_info** (`plugin_queue_info.py`) – Batch queue/partition info, job listings, and allocations. Backend selected automatically via `make_queue_info()` factory: SLURM (`queue_info_slurm.py`, sinfo/squeue/sacctmgr), PBSPro (`queue_info_pbs.py`, qstat/pbsnodes; allocations not available — PBSPro has no native sacctmgr equivalent), or no-op (`queue_info_none.py`). Shared backend with caching. Background prefetch on startup. Client API: `backend()` (session-less, returns `'slurm'`/`'pbs'`/`'none'`), `job_allocation()` (session-less, returns `{job_id, partition, n_nodes, nodelist, cpus_per_node, gpus_per_node, account, job_name, runtime}` or None), `get_info(user, force)`, `list_jobs(queue, user, force)`, `list_all_jobs(user, force)`, `cancel_job(job_id)`, `list_allocations(user, force)`.
- **rhapsody** (`plugin_rhapsody.py`) – Task execution via Rhapsody backends (default: Dragon V3). Registers backend callbacks for intermediate state notifications (e.g. RUNNING). Client API: `submit_tasks(tasks)`, `wait_tasks(uids, timeout)`, `list_tasks()`, `get_task(uid)`, `cancel_task(uid)`, `cancel_all_tasks()`. Function tasks supported via cloudpickle (``"function": "cloudpickle::<base64>"``, ``"_pickled_fields": [...]``) or import path (``"function": "module:func"``). **Return values** keep the same fidelity in the other direction: a non-JSON-safe result travels as ``"return_value": "cloudpickle::<base64>"`` with ``"_return_value_encoding": "cloudpickle"`` (``bytes`` keep the plain-base64 form, marker ``"base64"``) and is decoded back into the original object by `RhapsodyClient` on both wait paths (SSE and poll); JSON-safe results are unchanged. Finished tasks stay queryable on the endpoint for `FINISHED_RETAIN_S` (1 h; then `get_task` answers 404 and they leave `list_tasks` / `wait_tasks`), and `RhapsodyClient` drops completions older than that unless a `wait_tasks` call is waiting on them (a later wait on one raises `ValueError`), so a long-lived session stays bounded. `get_task()` / `list_tasks()` deliberately return the wire form (they are proxied verbatim as JSON, e.g. by the task dispatcher). A result above the per-session return-value limit is reported as a task `error` rather than truncated or dropped as an oversized frame — the JSON-safe wire form is size-checked too — and terminal notifications flush in frames bounded by a notification byte budget, so a batch of large results cannot pack one frame past the cap. Both budgets are half of the **effective** transport frame cap, resolved once at plugin init from the hosting runtime (`app.state.endpoint_service._frame_cap`, which is tunable) and injected into every session; `RETURN_VALUE_LIMIT` / `NOTIFY_BATCH_BYTES` (half `protocol.FRAME_CAP`) are only the fallback for a host without that handle (broker plugin host, tests). Known limitation: only the local cap is discoverable — the broker's `frame_cap` is configurable too and is not advertised in the handshake (`RegisterAck` carries no tuning), so a broker configured below its endpoints can still reject a frame these budgets allow. Note the trust direction this adds: the **client unpickles endpoint-produced results**, where previously only the endpoint unpickled client-produced arguments (gated there by `allow_pickled_tasks`); there is no client-side gate, so a consumer must trust the endpoints it submits to. Resource specs via ``task_backend_specific_kwargs`` (timeout, ranks, type, process_template). Session telemetry (rhapsody's `[telemetry]` extra) is on by default; `RADICAL_ORBIT_RHAPSODY_TELEMETRY=0|false|no|off` on the endpoint switches it off. Session accepts optional `backends` list. Notification topics: `session_status` → `{sid, status}` on session init ready/failed; `task_status` → `{uid, state, sid}` on RUNNING, `{uid, state, sid, exit_code, stdout, stderr, return_value, error, exception}` on terminal states (stdout/stderr keep their last frame-cap/8 bytes each behind a truncation marker, flagged `stdout_truncated`/`stderr_truncated`; the session's `get_task` has the full text while the task is retained; the task dispatcher forwards the tails to its pool-mode (rhapsody-dialect) consumers but keeps no output of its own); `task_status_batch` → `{sid, tasks: [...]}` for bulk terminal notifications. Completions are coalesced over a window (`RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW`, default 0.25 s), or flushed at once when the buffer is full by count or bytes; a window of `0` flushes on the next loop iteration (not per completion), so the completions of one loop pass share a frame. Known limitation: a notification dropped on the way (a full client callback queue, the broker's per-subscriber queue) is not recovered, and its waiters wait forever (#149). Task and session notifications are broadcast to every client of the plugin and task uids are only unique per client, so consumers must ignore a `sid` other than their own (`RhapsodyClient` does; rhapsody's `OrbitExecutionBackend` does with rhapsody#112; the Explorer panel `data/plugins/rhapsody.js` does not yet; a missing `sid` means an older endpoint). The batch `sid` is on the frame, not on the items. `RhapsodyClient` also scopes its own runtime callbacks (`_on_task_done` for `task_status`/`task_status_batch`, and its `session_status` waiter) to its sid with `register_callback(session=sid)`, so other sessions' events never take room in its callback queue; `register_session` moves them to the new sid together with the sid switch (`rescope_callback`, one `_cond` critical section), and `close()` removes them. Callbacks other code registers through `register_notification_callback` (e.g. rhapsody's `OrbitExecutionBackend`) stay unscoped and rely on the sid filter. Client-side optimizations: template compression for homogeneous batches, size-aware pipelined submission, SSE-based wait with event wakeup. Submit batches (also the task dispatcher's) are sized by each task's packed msgpack bytes against `WS_PAYLOAD_LIMIT` = `FRAME_CAP // BUDGET_RATIO` (half the protocol frame cap). Known limitations: the budget follows the protocol default only — the broker's and endpoints' tuned caps are not advertised (#152); and when a batch fails part-way through a submit, the outcome of that batch and the uids of later batches are not reported back (#148). 
- **lucid** (`plugin_lucid.py`, flow in `lucid_flow.py`, UI `data/plugins/lucid.js`) – LUCID Cell Painting mock-up portal. Runs on a *local* endpoint (`-p lucid`, not on the broker) and is itself a client: `submit` drives `lucid_flow.LucidRun` against a remote endpoint's `globus` + `rhapsody` + `staging` plugins — Globus stage-in of selected plate wells from the LUCID collection to the NERSC DTN, one CellProfiler task per well (docker image via `shifter`), packaging (tarball, per-well counts, montage + preview crops), Globus stage-out to `/tmp_radical/<run-id>/` on the collection, previews fetched back via `staging` for inline display. One run at a time, state in memory (a reloaded page re-attaches). Globus Transfer refresh token read from `~/.globus/auth_tokens.json` (`get_globus_token.py`) on the portal host, never sent to the browser; it is handed to the target endpoint's `globus` session (in memory there, like any `globus` client credential). Remote endpoints are reached through the host runtime, or through `$RADICAL_ORBIT_LUCID_BROKER` when the UI is served by a different broker. Client API (`LucidClient`): `config()`, `submit(endpoint, wells, collection=, images_path=, pipeline_path=, analysis=)`, `status()`, `image(run_id, name)`. Notification topic: `run_status` → run state. CLI equivalent: `examples/lucid_e2e_demo.py`; cleanup: `examples/lucid_e2e_cleanup.py`.
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
- `subscribe` / `unsubscribe` – interest patterns for event delivery (`endpoint`, `plugin`, `topic`, optional `session`; `None` is a wildcard; `session` is on the wire only when set, so a session-less pattern packs as in v1).
- `topology` – rich participant/plugin/liveness snapshot.
- `control` – `shutdown` / `error` / `terminate` / `disconnect`.

Liveness is **transport-level only** (WS protocol ping/pong; no app-level heartbeat in the envelope). A protocol-level frame-size cap bounds each frame; oversized payloads are the sender's problem (rhapsody batches size-aware), and fail fast: `EndpointRuntime.call` packs a request before waiting for its answer — an over-cap request returns a 413 at once (so does `Broker._broker_call`, e.g. for a gateway body that fits the read cap but not a frame), one that cannot be queued (transport not up) raises a `ConnectionError` at once, and one that cannot be packed raises at once — and an over-cap response is replaced by a 500 for the same `corr_id` (`protocol.pack_or_error`, in the serving runtime and `Broker._dispatch_to_host`). Other frames (events, subscriptions) are still logged and dropped.

WebSocket **permessage-deflate is decided per hop by the dialling side**. It dominated large payloads on fast links (Dragon DDict through ORBIT on Perlmutter, #156: 1 MiB put/get p50 ~100 ms with deflate vs 16–19 ms without, 256 KiB 28 → 6 ms; small values unchanged). The **broker accepts** deflate by default (uvicorn `ws_per_message_deflate`, also for the embedded broker; accepting costs nothing unless offered); `BrokerTuning.ws_compression=False`/`'off'` or `RADICAL_ORBIT_WS_COMPRESSION=off` turns acceptance off. The **runtime offers** it per mode `off | on | auto` — `EndpointRuntime` tuning keyword `ws_compression` (bools accepted: `True`=on, `False`=off) > `RADICAL_ORBIT_WS_COMPRESSION` (`on/off/auto`, also `1/true/yes`, `0/false/no`; anything else raises) > default `auto` (`utils.resolve_ws_compression`). `auto` times a plain TCP connect to the broker host:port before each (re)connect (2 s deadline; a failed probe means off and the real connect reports the error) and offers deflate iff it took ≥ `ws_compression_rtt_ms` (`RADICAL_ORBIT_WS_COMPRESSION_RTT_MS`, default 10 ms); the decision is logged at info per connect. The probe measures the dialled hop only — through a forward tunnel that is the local tunnel end, so set `on` explicitly there; if websockets would dial through an environment proxy (`websockets.proxy.get_proxy`, i.e. `wss_proxy`/`ws_proxy`/`socks_proxy`/`https_proxy`/`http_proxy` minus `no_proxy`; `all_proxy` is not consulted), `auto` skips the probe and offers deflate (a direct probe would time the wrong path, or stall 2 s on a firewalled direct route). When offered, the runtime asks for zlib level 1 (`ClientPerMessageDeflateFactory(compress_settings={'level': 1})`); the broker compresses its direction with uvicorn's default settings. Negotiated per connection, so mixed settings interoperate (compressed only when offered and accepted); no protocol change.

### Notifications

Plugins can send real-time notifications to consumers as `event` frames (and, for HTTP clients, over Server-Sent Events via the gateway).
The notification flow is: **Session → Plugin → participant runtime → Broker → subscribed consumers (and the gateway's SSE fan-out)**.

**Session events.** A session's `notify` stamps its sid as the event `session` (`Plugin._dispatch_notify` / `send_notification` take `session=`; a plugin-level send is session-less unless it passes one — rhapsody's `session_status` does; xgfabric's `workflow_status` goes through its session's `notify`). Still session-less: the task dispatcher's plugin-level topics (`pool_members`, `pilot_status`, `task_status`, and the per-pool `task_status_batch`, which mix sessions) and lucid's `run_status`. The broker numbers session events per `(src, plugin, session)` — sids are not unique across endpoints and plugins (`'default'` everywhere) — in an LRU-bounded table (`broker_events.SESSION_SEQ_MAX`); a new or pruned stream starts at the broker-wide high-water mark of session seqs, so a key's seq never goes backwards while the broker runs (and need not start at 0). Session-less events use one global seq. A consumer scopes a callback with `rt.register_callback(..., session=sid)`: the runtime drops other sessions' events in `_on_event` before the callback queue, still delivers session-less ones (plugin-level notifications; older endpoints send everything session-less). **Broker session routing** is capability-gated: the broker advertises its own `session_events` under the reserved key `protocol.BROKER_CAPS` (`'_broker'`) of its topology entry's plugins — with or without hosted plugins, shaped as a disabled plugin (`protocol.broker_caps_entry()`) so older consumers listing it see the usual fields. Keys starting with `_` (`protocol.is_reserved`) are wire-only: `protocol.public_topology` drops them from `Broker.topology_snapshot` (hence gateway, Explorer, hosted plugins) and from the runtime's `topology()`, topology callbacks and served plugins; `Plugin.__init__` rejects such an instance name and the broker drops such a registered plugin with a warning. Only where the broker advertises it, and for a concrete endpoint and plugin, does a session registration subscribe with a session pattern (`SubscribePattern.session`, matched together with endpoint/plugin/topic; it narrows session-stamped events only — session-less events (plugin-level notifications, everything from an endpoint that does not stamp sessions) always pass, so no per-plugin flag is needed; the broker indexes a subscriber's session patterns that name endpoint and plugin by that stream), so the broker sends that registration only its own session's events. Elsewhere (older broker without the `_broker` entry, or a wildcard endpoint/plugin) the subscription stays the v1 `(endpoint, plugin, topic)` pattern and filtering stays local. Unscoped registrations (e.g. rhapsody's `OrbitExecutionBackend` callbacks via `register_notification_callback`) keep the plain pattern, so their runtime still receives all sessions' events from the broker: routing narrows traffic for scoped registrations only. The runtime counts registrations per wire pattern (`_wire_count`): `subscribe` on a pattern's first registration, `unsubscribe` when its last one goes. `EndpointRuntime._rescope_sessions` follows the broker capability as topology changes, moving the session registrations when it flips (new pattern subscribed first, then the old one withdrawn); `rescope_callback` moves the session pattern with the registration — events of the new session the broker ingests before it processes the new `subscribe` are not delivered, so a caller moving to a session that may already be emitting holds the plain pattern across the move (`hold_subscription` … `release_subscription`), as `RhapsodyClient` does around `register_session`. A resync after reconnect forgets the capability and sends the v1 forms (the broker may be another, older one, which would drop the socket on an unknown pattern field); the new connection's topology re-scopes, and a topology frame of an earlier connection cannot (`_conn_gen`). While the transport is down, registry changes only update the counts (no subscribe/unsubscribe is queued); the resync sends the full set. `hold_subscription` / `release_subscription` hold a plain pattern without a callback: `RhapsodyClient` holds the plain patterns of its own scoped callbacks across the `register_session` POST until they have moved to the new sid, so the broker does not filter out the new session's events in between (wire order: Sub(plain), Sub(new), Unsub(old), Unsub(plain)). The broker skips (logs) a malformed subscribe pattern instead of letting it close the subscriber's socket, and treats an empty session as none (numbering and routing agree); a defense only — the runtime validates the pattern in `register_callback` / `hold_subscription` before any state changes, so it never sends one. `unregister_callback(..., session=)` removes only that scope (without `session`, all registrations of the callback; callbacks match by equality, so bound methods unregister); `rescope_callback` moves a registration to another session atomically for the local filter (the broker side: see above). The `replay` plugin keeps session events per `(src, plugin, session)` (`fetch(session=, endpoint=, plugin=)`; `endpoint`/`plugin` make it exact; a bare sid of several streams reads them merged in ts order, an approximate position only; a read from a stream's start reports a gap when anything before its lowest retained seq was evicted, also after its buffer aged out) and its global ring holds only session-less events, so a global-ring reader no longer sees rhapsody/psij session events (a cross-session view is #151).

#### Sending notifications from a plugin session

```python
# In your PluginSession subclass:
class MySession(PluginSession):
    def do_work(self):
        # ... do some work ...

        # Send notification (works from sync/async contexts and threads)
        self.notify("work_status", {
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
rt.stop()
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
