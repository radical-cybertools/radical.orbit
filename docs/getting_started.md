# Getting Started with ORBIT

## Outline

1. [Installation](#1-installation)
2. [Configuration](#2-configuration)
3. [Run a demo](#3-run-a-demo)
4. [First consumer](#4-first-consumer)
5. [Notifications](#5-notifications)
6. [Explorer UI](#6-explorer-ui)

ORBIT is a **star**: one active **broker** hub routes between **endpoint**
participants.  A participant dials the broker over a single outbound WebSocket
and may *serve* plugins, *consume* other participants' plugins, or both.  The
broker also runs a **gateway** module (on by default) that serves an
HTTP/SSE/Explorer compatibility surface for non-participant callers.

A local demo uses three processes, each in its own terminal: the broker, one
endpoint, and a consumer script.

## 1. Installation

Prepare an isolated Python environment (virtualenv, conda, or similar) for each
role.  For a local demo the broker, endpoint, and consumer can share one
environment; in production each machine has its own.

!!! note

    Python requirement: **3.10** or newer.

### 1.1. Create a virtual environment

```shell
export PYTHONNOUSERSITE=True
python3 -m venv ve_orbit
source ve_orbit/bin/activate
```

### 1.2. Install packages

!!! note

    This demo tracks the `devel` branch, which carries the latest changes and
    should be treated as an unstable release.

```shell
pip install git+https://github.com/radical-cybertools/radical.orbit.git@devel
```

Plugin dependencies are only needed on the machine that actually **serves** the
plugin — the broker host and consumer hosts usually do not need them:

- `psij`: `pip install psij-python`
- `rhapsody`: `pip install rhapsody-py`
- `rose`: `pip install rose`

### 1.3. Get the repository (optional)

Clone the repository to run the bundled example scripts:

```shell
git clone https://github.com/radical-cybertools/radical.orbit.git
```

## 2. Configuration

The broker URL, TLS cert, and TLS key each resolve with the precedence
**CLI flag > environment variable > file under `~/.radical/orbit/`**:

| Item | Env var                       | Default file                       |
|------|-------------------------------|------------------------------------|
| URL  | `RADICAL_ORBIT_BROKER_URL`  | `~/.radical/orbit/broker.url`      |
| Cert | `RADICAL_ORBIT_BROKER_CERT` | `~/.radical/orbit/broker_cert.pem` |
| Key  | `RADICAL_ORBIT_BROKER_KEY`  | `~/.radical/orbit/broker_key.pem`  |

### 2.1. Generate a certificate (development)

For `https://` / `wss://` URLs the broker needs a TLS cert and key.  Writing
them into the default config dir lets the broker, endpoints, and consumers find
them with **no env vars set**.  Run this on the machine that will host the
broker, and replace `95.217.193.116` with the broker's public IP.

!!! warning

    A self-signed certificate is for **development** only.

```shell
mkdir -p ~/.radical/orbit
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
        -keyout ~/.radical/orbit/broker_key.pem \
        -out    ~/.radical/orbit/broker_cert.pem \
        -subj   "/CN=95.217.193.116" \
        -addext "subjectAltName = IP:95.217.193.116,DNS:localhost,IP:127.0.0.1"
chmod 0600 ~/.radical/orbit/broker_key.pem
```

`chmod 0600` is mandatory: the broker refuses to start if the key file is more
permissive than `0o600`.  Only the broker needs the key; endpoints and
consumers need only the cert.  For a plain `http://` / `ws://` URL, cert
resolution is skipped entirely (dev mode only).

Add every client and endpoint address to the `subjectAltName` list, for
example:

```shell
 ... -addext "subjectAltName =
IP:95.217.193.116,
IP:10.0.0.5,
IP:127.0.0.1,
DNS:endpoint.example.org,
DNS:localhost"
```

### 2.2. Ingress authentication (token)

The broker gates its HTTP ingress **and** the endpoint `/register` handshake
with a **shared bearer token** — without it, anyone who can reach the broker
could drive plugins.  On first start the broker **generates** a token and writes
it to `~/.radical/orbit/broker.token` (mode `0600`).  The token value is
**never** printed to stdout (only its source/path is), so it cannot leak into
captured logs — read it from the file.

Resolution on the client/endpoint side is `--token` >
`$RADICAL_ORBIT_BROKER_TOKEN` > `~/.radical/orbit/broker.token`.  Same-host
endpoints and consumers therefore pick it up with no configuration; for a remote
broker, copy the token (like the cert) or set:

```shell
export RADICAL_ORBIT_BROKER_TOKEN='<the token>'
```

The Python SDK and the endpoint resolve the token automatically; the Explorer
prompts for it once and then rides an HttpOnly cookie minted by `POST /auth`.
For pure local development the gate can be disabled (with a loud warning):

```shell
./bin/radical-orbit-broker.py --no-auth   # or RADICAL_ORBIT_BROKER_NO_AUTH=1
```

## 3. Run a demo

All three roles run on the same host; use a separate terminal for each.

### 3.1. Terminal 1 — broker

The broker runs the active hub (the token-gated WebSocket `/register` route)
plus the on-by-default gateway compat tier (HTTP REST, SSE `/events`, and the
Explorer UI).  Pass `--no-gateway` for a headless broker.

```shell
# with RADICAL_ORBIT_BROKER_CERT / RADICAL_ORBIT_BROKER_KEY set (or the default
# files present), and the venv active:
radical-orbit-broker.py            # prints the auth-token source + URL on startup
```

Example output:

```text
[Broker] URL: https://broker.example.org:8000
[Broker] auth token generated and written to: ~/.radical/orbit/broker.token
INFO:     Uvicorn running on https://0.0.0.0:8000 (Press CTRL+C to quit)
```

### 3.2. Terminal 2 — endpoint

The endpoint runs the participant runtime, dialing the broker over one outbound
WebSocket and serving its plugins.

```shell
radical-orbit-endpoint.py --name my-endpoint --url wss://localhost:8000
```

For launching under a batch scheduler (SLURM, PBS), use the wrapper script,
which sets up `PATH` and `PYTHONPATH` for the installed package:

```shell
radical-orbit-endpoint-wrapper.sh --url wss://broker.example.org:8000 --name my-hpc-endpoint
```

The endpoint logs `registered as 'my-endpoint'` once the handshake completes,
and the plugins it loaded appear in the broker's topology.

### 3.3. Terminal 3 — consumer

Run a bundled example (from the cloned repository):

```shell
cd radical.orbit/examples
python3 example_sysinfo.py    # system info
python3 example_psij.py       # PsiJ job submission
python3 example_rhapsody.py   # Rhapsody tasks
```

### 3.4. Containerized

The `examples/docker` directory runs the broker, an endpoint, and a consumer in
separate containers.  It generates a self-signed cert during the image build.

```shell
export RADICAL_ORBIT_BROKER_HOSTNAME=broker

cd radical.orbit/examples/docker
docker compose up -d

# run an example inside the client container
docker exec -it radical-orbit-client bash
cd /app/radical.orbit/examples
python3 example_sysinfo.py

# tear down
docker compose down
```

### 3.5. Remote run

For a distributed deployment the broker, endpoints, and consumers run on
different machines:

- **Broker** on a public-facing host — generate the cert + key, place them
  (or set `RADICAL_ORBIT_BROKER_CERT` / `RADICAL_ORBIT_BROKER_KEY`), start the
  broker.  Distribute the cert and the token to endpoints and consumers.
- **Endpoint** on an HPC login or compute node — obtain the cert, set
  `RADICAL_ORBIT_BROKER_URL` (and, for a remote broker, the token), start the
  endpoint.  A firewalled site may need the broker IP in `no_proxy`
  (`export no_proxy="<broker_ip>,$no_proxy"`).
- **Consumer** on the local machine — obtain the cert and token, set the broker
  URL, run the consumer script.

## 4. First consumer

The Python SDK is `EndpointRuntime` (also exported as `Endpoint`).  A runtime
with no served plugins is a **pure consumer**: it dials
the broker over one outbound WebSocket, waits for the topology, and reaches other
participants' plugins with ``get_plugin(endpoint, plugin)``.

```python
from radical.orbit import EndpointRuntime

# broker URL/cert/token resolve via CLI-arg > env > file
rt = EndpointRuntime()
rt.start(wait=True)                      # blocks until registered + first topology

# every participant except the broker's own hosted-plugin entry
eids = [n for n in rt.topology() if n != 'broker']
print('endpoints:', eids)

si = rt.get_plugin(eids[0], 'sysinfo')   # discovers the namespace, registers a session
print(si.get_metrics())

rt.stop()
```

`start(wait=True)` guarantees that, when it returns, `topology()` already
reflects the broker's snapshot at registration time — so an immediate
`get_plugin` never races a not-yet-populated topology.

### Session lifetimes

`get_plugin(endpoint, plugin, **session_kwargs)` calls `register_session` for
you; the plugin helper's `register_session` accepts a lifetime policy:

- `lifetime='ephemeral'` (default) — an owner-bound session, reclaimed a grace
  period after its owning participant is declared `lost`; an owner-less one
  (gateway/HTTP caller) falls back to the plugin idle timeout.
- `lifetime='ttl', ttl=<seconds>` — expires `ttl` seconds after the last access
  (time-driven; requires a positive `ttl`).
- `lifetime='persistent'` — never expires; reclaimed only by explicit operator
  action.
- `sid='default'` — the reserved, shared, persistent session (one per plugin
  instance).

```python
psij = rt.get_plugin('my-endpoint', 'psij', lifetime='ttl', ttl=3600)
```

Reattach is **owner-checked**: reconnecting to an existing session whose recorded
owner differs from the caller's identity is rejected (HTTP 403).  Recovery goes
through re-registering the **same** participant name — so a serving endpoint (or
any consumer that needs its sessions back) should pass a stable ``name=`` rather
than accept the auto-generated ``consumer.<uuid8>``.

## 5. Notifications

Plugins push real-time notifications as broker ``event`` frames.  A consumer
registers callbacks on the runtime; the broker auto-subscribes it for the
matching pattern.  Every callback receives ``(endpoint, plugin, topic, data)``.

```python
rt = EndpointRuntime()
rt.start(wait=True)

# topic-specific callback
def on_job_status(endpoint, plugin, topic, data):
    print(f"Job {data['job_id']}: {data['state']}")

rt.register_callback(endpoint_id='my-endpoint', plugin_name='psij',
                     topic='job_status', callback=on_job_status)

# or via the plugin helper (most common)
psij = rt.get_plugin('my-endpoint', 'psij')
psij.register_notification_callback(on_job_status, topic='job_status')
```

Pass ``with_meta=True`` to also receive the broker-stamped envelope metadata —
the callback is then invoked as ``(endpoint, plugin, topic, data, meta)`` where
``meta = {'seq': int, 'ts': float, 'session': str | None}``.  The ``seq`` is the
authoritative broker sequence number, which lets live delivery deduplicate
against the ``replay`` plugin's retained stream:

```python
def on_job_status(endpoint, plugin, topic, data, meta):
    print(meta['seq'], data['state'])

rt.register_callback(endpoint_id='my-endpoint', plugin_name='psij',
                     topic='job_status', callback=on_job_status, with_meta=True)
```

Topology changes (participant connect / disconnect / liveness) are delivered
separately:

```python
def on_topology(participants):
    print('connected:', list(participants.keys()))

rt.register_topology_callback(on_topology)
```

## 6. Explorer UI

The broker's gateway serves a browser-based **Explorer** at the root URL (e.g.
`https://localhost:8000/`).  It discovers connected endpoints and their plugins,
offers interactive plugin interfaces (job submission, task management, system
metrics), streams live updates over SSE, and provides endpoint/broker
termination controls.  On first load it prompts for the ingress token and then
rides the HttpOnly `/auth` cookie.
