
# ORBIT

ORBIT stands for 'Orchestrated Resource Brokerage & Integration Toolkit'.  It provides a decentralized architectural framework for seamlessly interacting with high-performance computing (HPC) nodes and executing remote computations across endpoint services.

## Architecture

ORBIT is a **star**: one active hub and many participants.
1. **Broker (`radical-orbit-broker`)**: The active hub. It routes messages between participants by `src`/`dst`, correlates request/response both ways, tracks topology and liveness, and is itself a participant that can host plugins (e.g. the task dispatcher). It also runs the `gateway` module (on by default), which serves the HTTP/SSE/Explorer compatibility surface for non-participant callers.
2. **Endpoint (`radical-orbit-endpoint`)**: A participant, typically on an HPC login or compute node. It dials the broker over a single outbound WebSocket (firewall-friendly) and may **serve** plugins, **consume** other participants' plugins, or both. A dedicated transport thread owns the socket and its keepalive so liveness reflects process/host/network health rather than plugin behaviour.
3. **Clients / Portal (`client.py` & `orbit_explorer.html`)**: Developer and end-user interfaces. The Python SDK is itself a participant (a zero-plugin consumer over the same runtime); the Web Portal is a non-participant that speaks HTTP/SSE to the broker's gateway.

Control flows through the star; bulk data moves out of band (Globus, shared filesystem, SSH tunnels).

## Deployment

Create a virtualenv, conda env, or other isolated python environment of your
choice, and `pip install radical.orbit`.

However, some plugins require dependencies, otherwise they won't load:
  - psyj: `pip install psij/python`
  - rhapsody: `pip install rhapsody-py`
  - rose: `pip install rose`

In fact, the ROSE plugin is only installed with ROSE - so that's also an example
how 3rd party module can install `radical.orbit` plugins.  Note that plugin
dependencies are only needed on those machines on which the endpoint plugins are
actually used - the broker host and the client hosts usually don't need those.


## Usage (Command Line)

### 1. Generating Certificates (Dev)

Write the cert + key directly into the default config dir
(`~/.radical/orbit/`) — that way the broker, endpoints, and clients all
find them with **no env vars set**.  Replace `95.217.193.116` with
your broker's public IP.

```sh
mkdir -p ~/.radical/orbit
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
        -keyout ~/.radical/orbit/broker_key.pem \
        -out    ~/.radical/orbit/broker_cert.pem \
        -subj   "/CN=95.217.193.116" \
        -addext "subjectAltName = IP:95.217.193.116,DNS:localhost,IP:127.0.0.1"
chmod 0600 ~/.radical/orbit/broker_key.pem
```

`chmod 0600` is mandatory: the broker refuses to start if the key
file is more permissive.

**Distribute the cert to every connecting host.**  Endpoints and
clients *pin* the broker cert — they trust exactly that certificate,
never the system CA store — so each host that connects needs a copy
(the cert only; the private key never leaves the broker host):

```sh
ssh <host> "mkdir -p ~/.radical/orbit"
scp ~/.radical/orbit/broker_cert.pem <host>:.radical/orbit/
```

This manual staging step is the one cert-distribution mechanism for
*every* endpoint startup channel (by hand, ssh, PsiJ, IRI job
submission).  Alternatively point `$RADICAL_ORBIT_BROKER_CERT` at the
copy on the connecting host.

To override the defaults (different paths, remote broker URL, etc.),
set any of:

```sh
export RADICAL_ORBIT_BROKER_URL='https://my-broker:8000/'
export RADICAL_ORBIT_BROKER_CERT="/path/to/broker_cert.pem"
export RADICAL_ORBIT_BROKER_KEY="/path/to/broker_key.pem"    # only needed for the broker
export RADICAL_ORBIT_BROKER_TOKEN="<shared ingress token>"  # see "Ingress authentication" below
```

See the **Broker configuration** section below for the full
precedence rules (CLI > env > file).

### 1b. Ingress authentication (token)

The broker requires a **shared bearer token** on its HTTP ingress and on the
endpoint `/register` handshake — without it, anyone who can reach the broker
could drive plugins (submit jobs, stage files). On first start the broker
**generates a token** and writes it to `~/.radical/orbit/broker.token` (mode
`0600`); the token itself is never printed — only its source/path is.  Same-host
endpoints and clients pick that file up automatically; for a remote broker,
copy the token (like the cert) and set:

```sh
export RADICAL_ORBIT_BROKER_TOKEN='<the token>'
```

Precedence is CLI (`--token`) > `$RADICAL_ORBIT_BROKER_TOKEN` >
`~/.radical/orbit/broker.token`. The Python client/SDK and the endpoint resolve
it automatically; the **Explorer** prompts for it once and rides an HttpOnly
cookie thereafter.

For pure local development you can disable the gate (loud warning):

```sh
./bin/radical-orbit-broker.py --no-auth   # or RADICAL_ORBIT_BROKER_NO_AUTH=1
```

### 2. Starting the Broker
The Broker process runs the active broker: a WebSocket `/register` hub that
routes between participants, plus the on-by-default gateway compat tier (HTTP
REST API, SSE `/events`, and the Explorer). Pass `--no-gateway` for a headless
broker (WebSocket ingress only).
```sh
./bin/radical-orbit-broker.py        # prints the auth token source + URL on startup
```

### 3. Starting the Endpoint Service
Start the endpoint service (ideally on your target HPC node) pointing to the running Broker:
```sh
./bin/radical-orbit-endpoint.py --name my-endpoint --url wss://localhost:8000
```

#### Using the Wrapper Script
For launching endpoint services via batch job schedulers (e.g., SLURM), use the wrapper script which properly sets up the environment:
```sh
./bin/radical-orbit-endpoint-wrapper.sh --url wss://broker.example.org:8000 --name my-hpc-endpoint
```

The wrapper script automatically detects and exports the correct `PYTHONPATH` for the installed modules.

### 4. Running a Test Client
```sh
./examples/example_sysinfo.py
```

## REST API

The broker's `gateway` module serves the HTTP compatibility surface, with the following management endpoints:

All routes except the UI shell (`GET /`) and the static plugin assets
(`/plugins/*`) require the broker token — sent as `Authorization: Bearer <token>`
or, for the browser, the cookie minted by `POST /auth`.

### Management Endpoints
- `GET /` - Fetches the interactive ORBIT Explorer UI. (ungated)
- `POST /auth` - Validates the bearer token and sets the HttpOnly auth cookie (used by the Explorer / SSE).
- `POST /endpoint/list` - Returns a JSON structure describing all currently connected Endpoints and their loaded Plugins namespaces.
- `POST /endpoint/disconnect/{endpoint_name}` - Disconnect a specific endpoint service from the broker.
- `POST /broker/terminate` - Terminate the broker process (endpoints remain running).
- `GET /events` - Server-Sent Events (SSE) endpoint for real-time notifications.

### Proxy Routes
- `/*` - All other routes are parsed by the gateway to extract the targeted `{endpoint_name}` and `{namespace}` path; the request is mapped onto `(dst, path)` and routed over the broker's WebSocket to that Endpoint's served plugins.

## Plugin Structure

Plugins dynamically extend an Endpoint's capabilities. A Plugin implementation combines three core components:

### 1. The Plugin Class (REST API)
Inherits from `Plugin`. It binds directly to the Endpoint's internal `FastAPI` application to register routes. Routes must be stateless or manage state by instantiating discrete Sessions (e.g. `POST /register_session`).

### 2. The Session Class
Inherits from `PluginSession`. Represents a stateful context for a specific plugin client execution instance. Handles backend resources, concurrent job futures, and scoped operational contexts required across subsequent API calls by the same user.

### 3. Client API Shim (`client.py`)
Inherits from `PluginClient`. An abstraction layer enabling local Python developers to effortlessly instantiate new sessions and seamlessly invoke the REST API operations behind native Python instance methods (without manually unpacking JSON responses).

## Programming with ORBIT

You can interact with other participants' plugins programmatically using the Python `EndpointRuntime` SDK — a zero-plugin runtime is a pure consumer that dials the broker over one outbound WebSocket and reaches plugins with `get_plugin(endpoint, plugin)`. Example scripts reside in the `examples/` directory.

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
pi = rt.get_plugin('my-endpoint', 'psij')   # rt = EndpointRuntime().start()
pi.submit_job(job_spec)
```

### Accessing Queue Info
You can query batch scheduling resources programmatically to auto-discover appropriate queues and limits before job submission.

```python
qi = rt.get_plugin('my-endpoint', 'queue_info')
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

The interactive ORBIT Explorer interface (`src/radical/orbit/data/orbit_explorer.html`) provides a comprehensive browser-based client for interacting with the Broker HTTP interface.

- Served dynamically via `GET /` on the Broker.
- Discovers the endpoint hierarchy leveraging the `POST /endpoint/list` API.
- Implements purely client-side routing to interact with REST bindings of different endpoint plugins (e.g., querying `queue_info`, or submitting jobs dynamically via `psij` or `rhapsody` plugins).
- Supports real-time updates via Server-Sent Events (SSE) from the `/events` endpoint.
- Allows launching new endpoint services on HPC resources via PSI/J job submission.
- Provides broker and endpoint termination controls.

## Configuration

### Broker configuration: URL, cert, key

The broker URL, TLS cert, and TLS key are resolved with this
precedence:

> **CLI flag > environment variable > file under `~/.radical/orbit/`**

| Item | Env var               | Default file                      |
|------|-----------------------|-----------------------------------|
| URL  | `RADICAL_ORBIT_BROKER_URL`  | `~/.radical/orbit/broker.url`      |
| Cert | `RADICAL_ORBIT_BROKER_CERT` | `~/.radical/orbit/broker_cert.pem` |
| Key  | `RADICAL_ORBIT_BROKER_KEY`  | `~/.radical/orbit/broker_key.pem`  |


Behaviour notes:

- **URL** (consumer side only): the broker derives its own advertised
  URL from `(host, port)` — wildcard binds use the local FQDN
  (printing both FQDN and outbound-IPv4 forms on stdout); specific
  binds advertise that literal address.  The broker writes
  `broker.url` only when the file does not already exist, so a stale
  file the operator placed for a different broker is never clobbered.
  Endpoints / clients raise `ValueError` if no URL resolves.
- **Cert / key**: never auto-written; the operator places them.
  Required for `https://` / `wss://` URLs; ignored entirely for
  `http://` / `ws://`.
- **Key**: The key is only needed by the broker.  The broker refuses
  to start if `broker_key.pem` is more permissive than `0o600`.

### Broker CLI Args

```
radical-orbit-broker.py [options]
  --cert CERT    TLS cert path                  (CLI > env > file)
  --key  KEY     TLS key path; mode 0o600       (CLI > env > file)
  --host HOST    Bind address (default: 0.0.0.0)
  --port PORT    Bind port    (default: 8000)
  -p PLUGINS     Broker-hosted plugins (default: role default set)
  --no-auth      Disable ingress auth (local dev only)
  --no-gateway   Headless broker: only the token-gated WebSocket
                 /register ingress, no HTTP/SSE/Explorer compat tier
                 (the gateway is on by default)
```

### Endpoint Service CLI Args

```
radical-orbit-endpoint.py [options]
  --name NAME         Endpoint name (shown in Explorer and /endpoint/list)
  --url  URL          Broker URL                 (CLI > env > file)
  --cert CERT         TLS cert path              (CLI > env > file)
  -p PLUGINS          Comma-separated plugins to load
  --tunnel MODE       Tunnel mode: none | forward | reverse
  --tunnel-via HOST   Login host for --tunnel forward (defaults to
                      $PBS_O_HOST / $SLURM_SUBMIT_HOST)
  --log-level LEVEL   DEBUG | INFO | WARNING | ERROR
```

### Log Level

Set the logging level via `RADICAL_ORBIT_LOG_LVL` (or the generic
`RADICAL_LOG_LVL`):

```sh
RADICAL_ORBIT_LOG_LVL=DEBUG ./bin/radical-orbit-broker.py
```

Or in code: `logging.getLogger("radical.orbit").setLevel(logging.DEBUG)`.


## Troubleshooting

**Endpoint connects but no plugins appear in the Explorer**
: The plugin failed to import. Check the endpoint service log for `ImportError` or missing dependencies. Plugins with missing optional dependencies (e.g. PsiJ not installed) are silently skipped.

**Notifications not arriving (job/task table stops updating)**
: The SSE connection dropped. Refresh the page to reconnect. The Explorer reconnects automatically on topology changes but not on SSE stream errors.

**Job stuck in SUBMITTED state indefinitely**
: The PsiJ executor may be misconfigured. Check the endpoint log for PsiJ errors. For SLURM, verify the account and queue names are valid with `sinfo` and `sacctmgr`.

**SSL verification error when connecting**
: For `https://` / `wss://` URLs the cert is required — the `EndpointRuntime` (consumers and endpoints) raises `ValueError` if no cert is resolved (CLI > env > file).  Either set `RADICAL_ORBIT_BROKER_CERT` to the `.pem` from setup, drop the file at `~/.radical/orbit/broker_cert.pem`, or use a plain `http://` / `ws://` URL (cert resolution is then skipped entirely — dev mode only).
