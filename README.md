
# Radical Edge

Radical Edge provides a decentralized architectural framework for seamlessly interacting with high-performance computing (HPC) nodes and executing remote computations across edge services.

## Architecture

Radical Edge consists of three primary layers:
1. **Bridge (`radical-edge-bridge`)**: The centralized entry hub. It maintains WebSocket connections to external Edge services, manages edge discovery, and serves as an HTTP-to-WebSocket reverse proxy forwarding REST API calls to the respective Edges.
2. **Edge Service (`radical-edge-service`)**: Deployed directly on the compute nodes/HPC resources. It connects upstream to the Bridge via WebSocket, loading local Plugins to execute tasks natively within the remote network boundary.
3. **Clients / Portal (`client.py` & `edge_explorer.html`)**: Developer and end-user interfaces. The Python Client SDK orchestrates dynamic REST interactions with Plugins, while the Web Portal demonstrates direct native JavaScript browser integration with the Bridge API over HTTP.

## Deployment

Create a virtualenv, conda env, or other isolated python environment of your
choice, and `pip install radical.edge`.

However, some plugins require dependencies, otherwise they won't load:
  - psyj: `pip install psij/python`
  - rhapsody: `pip install rhapsody-py`
  - rose: `pip install rose`

In fact, the ROSE plugin is only installed with ROSE - so that's also an example
how 3rd party module can install `radical.edge` plugins.  Note that plugin
dependencies are only needed on those machines on which the edge plugins are
actually used - the bridge host and the client hosts usually don't need those.


## Usage (Command Line)

### 1. Generating Certificates (Dev)
For the bridge to securely operate on HTTPs/WSS:
```sh
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
  -keyout bridge_key.pem -out bridge_cert.pem \
  -subj "/CN=RADICAL" \
  -addext "subjectAltName = IP:127.0.0.1,DNS:localhost"
```

Set the appropriate environment variables:
```sh
export RADICAL_BRIDGE_URL='https://localhost:8000/'
export RADICAL_BRIDGE_CERT="`pwd`/bridge_cert.pem"
export RADICAL_BRIDGE_KEY="`pwd`/bridge_key.pem"  # only needed for the bridge
```

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


## Portal Integration

The interactive Edge Explorer interface (`src/radical/edge/data/edge_explorer.html`) provides a comprehensive browser-based client for interacting with the Bridge HTTP interface.

- Served dynamically via `GET /` on the Bridge.
- Discovers the endpoint hierarchy leveraging the `POST /edge/list` API.
- Implements purely client-side routing to interact with REST bindings of different edge plugins (e.g., querying `queue_info`, or submitting jobs dynamically via `psij` or `rhapsody` plugins).
- Supports real-time updates via Server-Sent Events (SSE) from the `/events` endpoint.
- Allows launching new edge services on HPC resources via PSI/J job submission.
- Provides bridge and edge termination controls.

## Configuration

### Environment Variables

| Variable               | Description                                              | Default         |
|------------------------|----------------------------------------------------------|-----------------|
| `RADICAL_BRIDGE_URL`   | Bridge URL used by edge services and Python clients      | *(required)*    |
| `RADICAL_BRIDGE_CERT`  | Path to CA certificate for SSL verification              | *(none — HTTP)* |
| `RADICAL_BRIDGE_KEY`   | Path to private key (bridge startup only, HTTPS mode)    | *(none — HTTP)* |

### Edge Service CLI Args

```
radical-edge-service.py [options]
  --name NAME    Edge name (shown in Explorer and /edge/list)
  --url  URL     Bridge WebSocket URL (e.g. wss://bridge:8000)
  -p PLUGINS     Comma-separated list of plugin names to load
  --cert CERT    CA certificate path for SSL
```

### Bridge Startup

The bridge listens on `0.0.0.0:8000` by default. To change host/port, subclass `Bridge` or edit `bin/radical-edge-bridge.py` and pass `host=` / `port=` to `uvicorn.run()`.

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
: Set `RADICAL_BRIDGE_CERT` to the path of the CA certificate (the `.pem` file generated during setup). Without it, Python clients default to `verify=False` (dev mode only).
