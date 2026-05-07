# Radical Edge

Radical Edge provides a decentralized architectural framework for seamlessly interacting with high-performance computing (HPC) nodes and executing remote computations across edge services.

## Architecture

Radical Edge consists of three primary layers:
1. **Bridge (`radical-edge-bridge`)**: The centralized entry hub. It maintains WebSocket connections to external Edge services, manages edge discovery, and serves as an HTTP-to-WebSocket reverse proxy forwarding REST API calls to the respective Edges.
2. **Edge Service (`radical-edge-service`)**: Deployed directly on the compute nodes/HPC resources. It connects upstream to the Bridge via WebSocket, loading local Plugins to execute tasks natively within the remote network boundary.
3. **Clients / Portal (`client.py` & `edge_explorer.html`)**: Developer and end-user interfaces. The Python Client SDK seamlessly orchestrates dynamic REST interactions with Plugins, while the Web Portal demonstrates direct native JavaScript browser integration with the Bridge API over HTTP.

## Usage (Command Line)

### 1. Generating Certificates (Dev)

Write the cert + key directly into the default config dir
(`~/.radical/edge/`) — that way the bridge, edges, and clients all
find them with **no env vars set**.  Replace `95.217.193.116` with
your bridge's public IP.

```sh
mkdir -p ~/.radical/edge
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
        -keyout ~/.radical/edge/bridge_key.pem \
        -out    ~/.radical/edge/bridge_cert.pem \
        -subj   "/CN=95.217.193.116" \
        -addext "subjectAltName = IP:95.217.193.116,DNS:localhost,IP:127.0.0.1"
chmod 0600 ~/.radical/edge/bridge_key.pem
```

`chmod 0600` is mandatory: the bridge refuses to start if the key
file is more permissive.

To override the defaults (different paths, remote bridge URL, etc.),
set any of:

```sh
export RADICAL_BRIDGE_URL='https://my-bridge:8000/'
export RADICAL_BRIDGE_CERT="/path/to/bridge_cert.pem"
export RADICAL_BRIDGE_KEY="/path/to/bridge_key.pem"
```

See the **Bridge configuration** section below for the full
precedence rules (CLI > env > file).

### 2. Starting the Bridge
The Bridge server exposes a REST API and a WebSocket endpoint (`/register`):
```sh
./bin/radical-edge-bridge.py
```

### 3. Starting the Edge Service
Start the edge service (ideally on your target HPC node) pointing to the running Bridge:
```sh
./bin/radical-edge-service.py --name my-edge --url wss://localhost:8000
```

#### Using the Wrapper Script
For launching edge services via batch job schedulers (e.g., SLURM), use the wrapper script which properly sets up the environment:
```sh
./bin/radical-edge-wrapper.sh --url wss://bridge.example.org:8000 --name my-hpc-edge
```

The wrapper script automatically detects and exports the correct `PYTHONPATH` for the installed modules.

### 4. Running a Test Client
```sh
./examples/example_sysinfo.py
```

## REST API

The Bridge serves as an HTTP proxy with the following management endpoints:

### Management Endpoints
- `GET /` - Fetches the interactive Edge Explorer UI.
- `POST /edge/list` - Returns a JSON structure describing all currently connected Edges and their loaded Plugins namespaces.
- `POST /edge/disconnect/{edge_name}` - Disconnect a specific edge service from the bridge.
- `POST /bridge/terminate` - Terminate the bridge process (edges remain running).
- `GET /events` - Server-Sent Events (SSE) endpoint for real-time notifications.

### Proxy Routes
- `/*` - All other routes are parsed by the Bridge to extract the targeted `{edge_name}` and `{namespace}` path. Requests are tunneled via WebSocket directly to that Edge's registered internal FastAPI app.

## Plugin Structure

Plugins dynamically extend an Edge's capabilities. A Plugin implementation combines three core components:

### 1. The Plugin Class (REST API)
Inherits from `Plugin`. It binds directly to the Edge's internal `FastAPI` application to register routes. Routes must be stateless or manage state by instantiating discrete Sessions (e.g. `POST /register_session`).

### 2. The Session Class
Inherits from `PluginSession`. Represents a stateful context for a specific plugin client execution instance. Handles backend resources, concurrent job futures, and scoped operational contexts required across subsequent API calls by the same user.

### 3. Client API Shim (`client.py`)
Inherits from `PluginClient`. An abstraction layer enabling local Python developers to effortlessly instantiate new sessions and seamlessly invoke the REST API operations behind native Python instance methods (without manually unpacking JSON responses).

## Programming with Radical Edge

You can interact with Edge services pragmatically using the Python `BridgeClient` SDK. Example scripts reside in the `examples/` directory.

### Submitting PsiJ Jobs
The `psij` plugin exposes a normalized interface for interacting with different HPC batch system schedulers via PSI/J.

```python
job_spec = {
    "executable": "/bin/sleep",
    "arguments": ["5"],
    "attributes": {
        "queue_name": "debug",    # Batch queue
        "account": "my_account",  # Target allocation
        "duration": "100",        # Walltime in seconds
        # You can also pass custom scheduling constraints directly:
        "slurm.constraint": "V100"
    }
}
pi = ec.get_plugin('psij')
pi.submit_job(job_spec)
```

### Accessing Queue Info
You can query batch scheduling resources programmatically to auto-discover appropriate queues and limits before job submission.

```python
qi = ec.get_plugin('queue_info')
info = qi.get_info()           # Returns cluster hardware topologies and queue states
allocs = qi.list_allocations() # Returns active account allocations for the user
jobs = qi.list_jobs('debug')   # Returns jobs in the specified queue (filtered to current user by default)
```

## Built-in Plugins

### sysinfo
System information plugin providing hardware and environment details:
- CPU topology (cores, threads, model)
- Memory and storage information
- GPU detection (NVIDIA, AMD, Intel)
- Shared filesystem detection (Lustre, GPFS, NFS, BeeGFS, DVS, etc.)
- Network interface information
- Background prefetch for faster initial queries

### queue_info
SLURM queue information plugin:
- Queue/partition details and limits
- Job listing (filtered by user)
- Allocation information
- Background cache prefetch on plugin load

### psij
PSI/J job submission plugin:
- Submit jobs via various batch schedulers (SLURM, PBS, LSF, local)
- Real-time job status notifications via SSE
- Job cancellation support
- Custom attributes for scheduler-specific options

### lucid
RADICAL Pilot integration plugin for task-based workflows.

### rhapsody
RADICAL Rhapsody integration for workflow composition.

## Portal Integration

The interactive Edge Explorer interface (`src/radical/edge/data/edge_explorer.html`) provides a comprehensive browser-based client for interacting with the Bridge HTTP interface.

- Served dynamically via `GET /` on the Bridge.
- Discovers the endpoint hierarchy leveraging the `POST /edge/list` API.
- Implements purely client-side routing to interact with REST bindings of different edge plugins (e.g., querying `queue_info`, or submitting jobs dynamically via `psij` or `rhapsody` plugins).
- Supports real-time updates via Server-Sent Events (SSE) from the `/events` endpoint.
- Allows launching new edge services on remote resources via SSH and PSI/J job submission.
- Provides bridge and edge termination controls.

## Configuration

### Bridge configuration: URL, cert, key

The bridge URL, TLS cert, and TLS key are resolved with this
precedence:

> **CLI flag > environment variable > file under `~/.radical/edge/`**

| Item | Env var               | Default file                      |
|------|-----------------------|-----------------------------------|
| URL  | `RADICAL_BRIDGE_URL`  | `~/.radical/edge/bridge.url`      |
| Cert | `RADICAL_BRIDGE_CERT` | `~/.radical/edge/bridge_cert.pem` |
| Key  | `RADICAL_BRIDGE_KEY`  | `~/.radical/edge/bridge_key.pem`  |


Behaviour notes:

- **URL** (consumer side only): the bridge derives its own advertised
  URL from `(host, port)` — wildcard binds use the local FQDN
  (printing both FQDN and outbound-IPv4 forms on stdout); specific
  binds advertise that literal address.  The bridge writes
  `bridge.url` only when the file does not already exist, so a stale
  file the operator placed for a different bridge is never clobbered.
  Edges / clients raise `ValueError` if no URL resolves.
- **Cert / key**: never auto-written; the operator places them.
  Required for `https://` / `wss://` URLs; ignored entirely for
  `http://` / `ws://`.
- **Key**: The key is only needed by the bridge.  The bridge refuses
  to start if `bridge_key.pem` is more permissive than `0o600`.

### Bridge CLI Args

```
radical-edge-bridge.py [options]
  --cert CERT    TLS cert path                  (CLI > env > file)
  --key  KEY     TLS key path; mode 0o600       (CLI > env > file)
  --host HOST    Bind address (default: 0.0.0.0)
  --port PORT    Bind port    (default: 8000)
  -p PLUGINS     Bridge-hosted plugins (default: role default set)
```

### Edge Service CLI Args

```
radical-edge-service.py [options]
  --name NAME         Edge name (shown in Explorer and /edge/list)
  --url  URL          Bridge URL                 (CLI > env > file)
  --cert CERT         TLS cert path              (CLI > env > file)
  -p PLUGINS          Comma-separated plugins to load
  --tunnel            Open ssh -L outbound to login host before connect
  --tunnel-via HOST   Explicit login host for --tunnel (defaults to
                      $PBS_O_HOST / $SLURM_SUBMIT_HOST)
  --log-level LEVEL   DEBUG | INFO | WARNING | ERROR
```

### Log Level

Set the standard Python logging level via environment or launcher:

```sh
RADICAL_LOG_LVL=DEBUG ./bin/radical-edge-bridge.py
```

Or in code: `logging.getLogger("radical.edge").setLevel(logging.DEBUG)`.


## Troubleshooting

**Edge connects but no plugins appear in the Explorer**
: The plugin failed to import. Check the edge service log for `ImportError` or missing dependencies. Plugins with missing optional dependencies (e.g. PsiJ not installed) are silently skipped.

**Notifications not arriving (job/task table stops updating)**
: The SSE connection dropped. Refresh the page to reconnect. The Explorer reconnects automatically on topology changes but not on SSE stream errors.

**Job stuck in SUBMITTED state indefinitely**
: The PsiJ executor may be misconfigured. Check the edge log for PsiJ errors. For SLURM, verify the account and queue names are valid with `sinfo` and `sacctmgr`.

**SSL verification error when connecting**
: For `https://` / `wss://` URLs the cert is required — `BridgeClient` and edge services raise `ValueError` if no cert is resolved (CLI > env > file).  Either set `RADICAL_BRIDGE_CERT` to the `.pem` from setup, drop the file at `~/.radical/edge/bridge_cert.pem`, or use a plain `http://` / `ws://` URL (cert resolution is then skipped entirely — dev mode only).
