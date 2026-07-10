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
handshake with a shared bearer token.  On first start it generates one
and writes it to `~/.radical/orbit/broker.token` (mode `0600`; the
token itself is never printed, only its path).  Precedence everywhere
is `--token` > `$RADICAL_ORBIT_BROKER_TOKEN` >
`~/.radical/orbit/broker.token`.  For local development only,
`--no-auth` (or `$RADICAL_ORBIT_BROKER_NO_AUTH=1`) disables the gate.

### Credential staging to endpoint/client hosts

The **cert** is staged manually to every host that connects —
endpoints *pin* it (the system CA store is never consulted).  This is
the one cert-distribution mechanism for every startup channel (by
hand, ssh, PsiJ, IRI job submission):

```sh
ssh <endpoint-host> "mkdir -p ~/.radical/orbit"
scp ~/.radical/orbit/broker_cert.pem <endpoint-host>:.radical/orbit/
```

The **token** reaches the endpoint in one of two ways:

- manually-started endpoints (by hand, ssh, cron) read
  `~/.radical/orbit/broker.token` — copy it alongside the cert, and
  re-copy it whenever the broker's token is regenerated:

  ```sh
  ssh <endpoint-host> "mkdir -p ~/.radical/orbit"
  scp ~/.radical/orbit/broker.token <endpoint-host>:.radical/orbit/
  ```

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
# Token: resolved from ~radical/.radical/orbit/broker.token (generated
# on first start), or pin one explicitly:
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
