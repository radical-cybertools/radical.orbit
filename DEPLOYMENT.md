# Deployment Guide

## Recommended Network Topology

```
  Internet / User Network
        |
        |  HTTPS (8000)
        v
  ┌─────────────┐
  │   Broker    │  ← public-facing, DMZ or bastion
  └─────────────┘
        |
        |  WSS (outbound from HPC)
        v
  ┌─────────────┐   ┌─────────────┐
  │  Endpoint (HPC) │   │  Endpoint (HPC) │  ← one per cluster or login node
  └─────────────┘   └─────────────┘
```

**Key point**: endpoints initiate the outbound WebSocket connection to the broker.
No inbound ports need to be opened on the HPC firewall.

## Broker Setup

The broker is a single FastAPI/uvicorn process running the active broker: a
WebSocket `/register` hub that routes between participants, with the gateway
compat tier (HTTP REST, SSE, Explorer) attached on the same port by default. It
holds no job state — all session state lives in the endpoint processes.

```sh
# HTTP (development only)
./bin/radical-orbit-broker.py

# HTTPS (production)
export RADICAL_ORBIT_BROKER_CERT=/path/to/cert.pem
export RADICAL_ORBIT_BROKER_KEY=/path/to/key.pem
./bin/radical-orbit-broker.py

# Headless broker (WebSocket ingress only, no HTTP/SSE/Explorer)
./bin/radical-orbit-broker.py --no-gateway
```

Set the bind address/port with `--host` / `--port` (defaults `0.0.0.0:8000`).

The cert/key pair is created by the operator (see the README's
"Generating Certificates" section, or `radical-orbit-broker.py --help`
for a copy-pasteable openssl recipe); the broker never generates them.
The key file must be mode `0600` or stricter — the broker refuses to
start otherwise — and never leaves the broker host.

### Ingress token

The broker gates its HTTP ingress *and* the endpoint `/register`
handshake with a shared bearer token.  Like the cert/key pair, the
token is created by the operator — the broker never generates or
writes it (`~/.radical/orbit` config is read-only for the software;
`radical-orbit-broker.py --help` carries a copy-pasteable recipe):

```sh
mkdir -p ~/.radical/orbit
python3 -c "import secrets; print(secrets.token_urlsafe(32))" \
    > ~/.radical/orbit/broker.token
chmod 600 ~/.radical/orbit/broker.token
```

Precedence everywhere is `--token` > `$RADICAL_ORBIT_BROKER_TOKEN` >
`~/.radical/orbit/broker.token`; the broker refuses to start without a
token.  For local development only, `--no-auth` (API: `auth=False`)
disables the gate.

### Credential staging to endpoint/client hosts

The broker's startup banner prints ready-to-paste *pull*-style
one-liners for this section's steps (run on the connecting host,
fetching from the broker host over ssh) — that is the canonical quick
path.  The *push*-style commands below are the equivalent from the
broker host, for when ssh only works in that direction.

The **cert** is staged manually to every host that connects —
endpoints *pin* it (the system CA store is never consulted).  This is
the one cert-distribution mechanism for every startup channel (by
hand, ssh, PsiJ, IRI job submission):

```sh
ssh <endpoint-host> "mkdir -p ~/.radical/orbit"
scp ~/.radical/orbit/broker_cert.pem <endpoint-host>:.radical/orbit/
```

The **token** reaches the endpoint in one of two ways:

- manually-started endpoints (by hand, ssh, cron) resolve the token
  from `$RADICAL_ORBIT_BROKER_TOKEN` or
  `~/.radical/orbit/broker.token`.  Stage whichever matches the
  broker's configuration: when the broker uses its token *file*
  (the default), copy it, and re-copy it whenever the operator
  rotates it:

  ```sh
  ssh <endpoint-host> "mkdir -p ~/.radical/orbit"
  scp ~/.radical/orbit/broker.token <endpoint-host>:.radical/orbit/
  ```

  When the broker was started with `--token` or
  `$RADICAL_ORBIT_BROKER_TOKEN` (higher precedence; the file may be
  absent or stale), install *that* value on the endpoint host instead
  — export it, or write it to the token file (mode `0600`).

- launcher-submitted endpoints (e.g. `examples/amsc.py` via PsiJ or
  IRI) receive the broker's *current* token via
  `RADICAL_ORBIT_BROKER_TOKEN` in the job environment — no token file
  is needed on the target, and it can never go stale.

The private key is **not** copied — it stays on the broker host.

### systemd Unit File (Broker)

```ini
[Unit]
Description=ORBIT Broker
After=network.target

[Service]
Type=simple
User=radical
WorkingDirectory=/opt/orbit
Environment=RADICAL_ORBIT_BROKER_CERT=/opt/orbit/certs/broker_cert.pem
Environment=RADICAL_ORBIT_BROKER_KEY=/opt/orbit/certs/broker_key.pem
# Token: resolved from ~radical/.radical/orbit/broker.token (operator-
# placed — see "Ingress token" above), or pin one explicitly:
# Environment=RADICAL_ORBIT_BROKER_TOKEN=<shared ingress token>
ExecStart=/opt/orbit/bin/radical-orbit-broker.py
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

## Endpoint Service Setup

Endpoints are typically launched as batch jobs via the host scheduler (SLURM, PBS)
or as long-running daemon processes on login nodes.

```sh
# Direct launch (login node daemon)
./bin/radical-orbit-endpoint.py \
  --name my-hpc-endpoint \
  --url  wss://broker.example.org:8000 \
  -p     sysinfo,psij,queue_info,staging

# Via SLURM batch script
sbatch endpoint_job.sh
```

### SLURM Batch Script Example

```sh
#!/bin/bash
#SBATCH --job-name=orbit
#SBATCH --partition=service
#SBATCH --nodes=1
#SBATCH --time=24:00:00

# Both staged from the broker host (see "Credential staging" above).
# With the files at ~/.radical/orbit/{broker_cert.pem,broker.token}
# these two exports are unnecessary — shown here for explicitness.
export RADICAL_ORBIT_BROKER_CERT=/path/to/broker_cert.pem
export RADICAL_ORBIT_BROKER_TOKEN="$(cat /path/to/broker.token)"

./bin/radical-orbit-endpoint-wrapper.sh \
  --name "$SLURM_CLUSTER_NAME-endpoint" \
  --url  wss://broker.example.org:8000 \
  -p     sysinfo,psij,queue_info,staging,rhapsody
```

The wrapper script (`radical-orbit-endpoint-wrapper.sh`) sets up `PYTHONPATH` and
`PATH` for the installed package before starting the endpoint service.

## Session Persistence

**Sessions are not persisted.** When an endpoint disconnects and reconnects:

- All active sessions are lost
- The Explorer automatically refreshes its plugin list via SSE topology event
- Python clients will receive a `404` on next call; they must call
  `register_session()` again
- The Explorer re-registers sessions transparently on next API call

Plan for endpoint restarts by wrapping your client loop with a reconnection
strategy.

## SFAPI (NERSC) launch path

NERSC's IRI facility API is currently broken server-side for the `/compute/*`
routes, so ORBIT reaches Perlmutter through NERSC's **Superfacility API
(SFAPI)** directly. This is the `sfapi_connect` broker plugin (a standalone
sibling of `iri_connect`); on `connect` it registers a dynamic
`sfapi.<endpoint>` instance that submits and polls jobs via `api.nersc.gov`.

### Creating the SFAPI client

Create a client at <https://iris.nersc.gov> (Profile → *Superfacility API
Clients*):

- **IP ranges are fixed at client creation** and cannot be edited afterwards —
  to change them you delete and recreate the client. Each range is **/24 max
  width**. When the credential is held by the broker-side plugin, use the
  **"Spin" preset** (NERSC's Spin/Rancher egress ranges the broker runs behind);
  add a **`/32`** for any individual host you also use for direct testing.
- **Clients expire by security level**: the higher the privilege, the shorter
  the lifetime — **2 / 30 / 60 days**. The `red`/high level needed to submit
  jobs is the short-lived one. **Expiry manifests as authentication failures**
  (the connect call or a later poll returns `401`), not as an explicit
  "expired" signal — if a previously-working endpoint suddenly 401s, check the
  client's expiry date at iris.nersc.gov first.

The client yields an **OAuth2 client id** and an **RSA private key (PEM)**.

### Credential handling

The client id and PEM are passed to the broker **at connect time**
(`sfapi_connect.connect(endpoint='nersc', client_id=…, private_key=…)`) and
held in **broker process memory only** — inside the `SFAPITokenManager`, which
mints/refreshes short-lived access tokens via `authlib`
(client-credentials + `private_key_jwt`). **Nothing is written to disk.** The
plugin verifies the credentials by minting one token *before* registering the
instance, so bad/expired credentials fail fast with `401` and leave no
half-registered instance behind. A reconnect with fresh credentials rotates
them in place. Because a private key must never be pasted into a browser, the
Explorer page for this plugin is read-only (status + disconnect only) — connect
programmatically through the client API.

### Deterministic job environment

SFAPI-submitted jobs otherwise inherit the submitter's full server-side-captured
login environment (~300 vars), which silently wedges dragon's backend bring-up
after `BEIsUp`. The launch contract is therefore deterministic by construction:

- The sbatch renderer emits **`#SBATCH --export=NONE`** — the job starts from a
  clean environment. Everything the endpoint needs travels as explicit
  `export KEY=VALUE` lines in the script body.
- The **endpoint wrapper** (`radical-orbit-endpoint-wrapper.sh`) owns the rest:
  it prepends the venv's `@BINDIR@` to **`PATH`** (dragon resolves its
  WLM helpers — `dragon-network-config-launch-helper`, `dragon-backend` — BY
  NAME via `srun`, so they must be findable) and, under Slurm, forces
  **`SLURM_EXPORT_ENV=ALL`** so that `--export=NONE` does not scrub the env from
  the inner job steps.

### Dragon-logging deadlock caveat

Do **not** enable dragon's channel-based logging
(`ARGS="-l dragon_file=DEBUG -l stderr=DEBUG"` in the wrapper) on a path you
expect to succeed. Verified live on Perlmutter: dragon 0.14 routes those logs
through its own communication channels, which can **deadlock backend bring-up
right after `BEIsUp`/`FENodeIdxBE`** — the endpoint never comes up. Use it only
for post-mortem debugging of an already-broken startup.

## Health Checks

Every plugin exposes a health endpoint at `GET /{plugin}/health`:

```
GET /my-endpoint/psij/health
→ {"status": "healthy", "plugin": "psij", "version": "...",
   "uptime_seconds": 3600.0, "active_sessions": 2}
```

The broker itself does not yet have a dedicated `/health` endpoint, but
`GET /endpoint/list` returning 200 is a reliable liveness check.

For load-balancer health probes:

```sh
curl -sk https://broker:8000/endpoint/list -X POST | jq .
```

## Observability

Log level is controlled via the `RADICAL_ORBIT_LOG_LVL` environment
variable (falling back to the generic `RADICAL_LOG_LVL`):

```sh
# DEBUG logging
RADICAL_ORBIT_LOG_LVL=DEBUG ./bin/radical-orbit-broker.py
```

Key log namespaces:

| Namespace          | Content                                      |
|--------------------|----------------------------------------------|
| `radical.orbit`     | Broker, endpoint service, plugin base             |
| `radical.orbit.client` | Python client, SSE listener               |

Structured logging is not yet enabled; logs go to stderr by default.
