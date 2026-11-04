# Dragon DDict through ORBIT: first latency, throughput and scaling runs

The first performance and scaling numbers for the `dragon` plugin (PR #156) on
Perlmutter CPU nodes, measured 2026-11-04. A client reaches a Dragon DDict
through the ORBIT broker and an endpoint running inside a Dragon runtime. The
numbers are compared with native DDict access from inside the runtime.

Everything here comes from a single run per point. Treat the numbers as
first estimates, not a characterization (see [Caveats](#caveats)).

## Setup

```
client (plain python, EndpointRuntime)        broker (compute node 0, or login node)
   │  WSS, msgpack envelope, JSON body              │
   └──────────────► broker ◄────────────────────────┘
                      │ WSS
                      ▼
          endpoint (radical-orbit-endpoint.py, -p dragon)
          launched by `dragon`, runs in the Dragon runtime
                      │ one dedicated Dragon thread
                      ▼
          DDict (managers_per_node=1, n_nodes=K, 8 GiB total)
```

| | |
|---|---|
| Machine | Perlmutter CPU nodes: 2× AMD EPYC 7763 (128 cores, 256 HW threads), 512 GB, Slingshot `hsn0` |
| Allocation | `salloc -N 4 -C cpu -q interactive -A amsc007`, job 60792061, nodes `nid[200032-200035]` |
| ORBIT | PR #156 head `7da4db9` (devel merged), from the source tree (`PYTHONPATH=src`) |
| Dragon | `dragonhpc` 0.15.0, TCP transport |
| Python | cray-python 3.12.12, websockets 17.3, uvicorn 0.54.0, fastapi 0.162.0, msgpack 1.2.4 |
| Slurm | 25.11.8 |

- **Broker:** `bin/radical-orbit-broker.py -p ''` (no hosted plugins; the
  gateway stays on), TLS with the demo pair, a token generated for the run.
  It runs on node 0 (primary) or on the login node `login20` (F5).
- **Endpoint:** `dragon -m -N K bin/radical-orbit-endpoint.py -p dragon`
  started from a one-node step on node 0. The plugin uses the runtime it runs
  in (`dragon.in_dragon()`). Variant B uses `dragon -s` instead, with one
  endpoint per node.
- **DDict:** created through ORBIT, `managers_per_node=1`, `n_nodes=K`,
  `total_mem=8G`.
- **Clients:** `bench_client.py`, one process per client, each with its own
  `EndpointRuntime` (and so its own WebSocket). Clients are started with
  `srun` and spread evenly over the allocation's 4 nodes (block
  distribution). Runs with fewer than 4 clients use nodes 1..3, off the broker
  node. The latency and batch clients run on node 1.
- **Values:** random `bytes`, which travel base64-encoded in the JSON body
  (about 4/3 of the raw size). Throughput runs use 1 KiB values.
- **No-op baselines:** `ddict/list` is a full ORBIT round trip
  (client → broker → endpoint plugin → back) that makes no Dragon call.
  `keys` on the DDict (about 5 keys) adds the hop to the Dragon thread and
  one Dragon call.
- **Native baseline:** `native_bench.py` under `dragon -s` (latency) or
  `dragon -m -N K` (16 Dragon processes, round-robin over the runtime's
  nodes, each looping on its own 64 keys).

## Results

### F1: latency vs value size

![F1](figs/F1_latency.png)

| value | ORBIT put p50 / p99 (ms) | ORBIT get p50 / p99 (ms) | native put p50 (ms) | native get p50 (ms) | ORBIT without deflate, put / get p50 (ms) |
|---|---|---|---|---|---|
| 64 B    | 1.63 / 1.75   | 1.56 / 1.80   | 0.34 | 0.26 | 1.57 / 1.47 |
| 1 KiB   | 1.64 / 1.77   | 1.55 / 1.73   | 0.34 | 0.25 | 1.58 / 1.49 |
| 16 KiB  | 4.04 / 4.57   | 4.00 / 4.41   | 0.34 | 0.26 | 1.82 / 1.77 |
| 256 KiB | 28.3 / 31.0   | 29.4 / 33.9   | 0.40 | 0.30 | 5.95 / 6.54 |
| 1 MiB   | 99.8 / 102.9  | 102.7 / 107.3 | 0.45 | 0.41 | 16.3 / 18.7 |

The no-op round trips (p50): `ddict/list` takes 1.05 ms, and `keys` takes
3.57 ms. The figures use 200 samples per point.

### F2: throughput vs batch size (1 client, 1 KiB values)

![F2](figs/F2_batch.png)

| keys per call | 1 | 4 | 16 | 64 | 256 | 1024 |
|---|---|---|---|---|---|---|
| `update` keys/s  | 601 | 1,144 | 1,677 | 2,217 | 2,433 | 1,746 |
| `get_many` keys/s | 646 | 1,336 | 2,031 | 2,780 | 3,158 | 3,282 |
| `get_many` MB/s  | 0.66 | 1.37 | 2.08 | 2.85 | 3.23 | 3.36 |

### F3: throughput vs concurrent clients (one endpoint, 1-node DDict)

![F3](figs/F3_clients.png)

Aggregate keys/s, put / get:

| clients | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| 1 key per call   | 553 / 666   | 778 / 1,264 | 736 / 1,588 | 1,223 / 1,561 | 1,235 / 1,667 |
| 64 keys per call | 1,599 / 2,725 | 483 / 3,487 | 822 / 3,438 | 1,784 / 3,516 | 1,941 / 3,526 |

Native reference, 1-node DDict: 4 processes reach 4,321 / 6,302 and
16 processes reach 503 / 5,951.

### F4: scaling vs nodes (main question)

![F4](figs/F4_scaling.png)

Aggregate keys/s, put / get, 1 KiB values:

| setup | 1 node | 2 nodes | 4 nodes |
|---|---|---|---|
| one endpoint, 16 clients, 1 key/call    | 1,235 / 1,667 | 1,514 / 1,860 | 1,264 / 1,468 |
| one endpoint, 16 clients, 64 keys/call  | 1,941 / 3,526 | 2,284 / 3,427 | 1,803 / 2,986 |
| one endpoint, 4 clients/node, 1 key/call   | 736 / 1,588 | 1,459 / 1,749 | 1,264 / 1,468 |
| one endpoint, 4 clients/node, 64 keys/call | 822 / 3,438 | 1,255 / 3,334 | 1,803 / 2,986 |
| **variant B**: 4 endpoints × 1-node DDict, 16 clients, 1 key/call |  |  | 3,186 / 3,491 |
| **variant B**: 4 endpoints × 1-node DDict, 16 clients, 64 keys/call |  |  | 3,498 / **15,599** |
| native, 16 processes, 1 key/op | 503 / 5,951 | 1,830 / 10,200 | 9,342 / 15,677 |

### F5: broker placement (compute node vs login node)

![F5](figs/F5_broker_placement.png)

| | broker on compute node 0 | broker on login20 |
|---|---|---|
| latency p50, 1 KiB put / get (ms) | 1.64 / 1.55 | 1.61 / 1.52 |
| latency p50, 1 MiB put / get (ms) | 99.8 / 102.7 | 105.6 / 111.2 |
| 1 node, 16 clients, 64 keys/call (keys/s, put / get) | 1,941 / 3,526 | 1,883 / 3,571 |
| 4 nodes, 16 clients, 64 keys/call (keys/s, put / get) | 1,803 / 2,986 | 1,966 / 3,141 |

### Outbound connectivity from compute nodes

`srun ... curl -sS -m 5 -o /dev/null -w '%{http_code}' https://github.com`
returned `200` from `nid200032`, and so did `https://pypi.org`. Compute nodes
can open outbound HTTPS to the internet, so an external broker (e.g.
radical.three) is reachable directly with `--tunnel none`, at least on 443.
Other ports were not tested.

## Findings

1. **Per-call floor of about 1.0–1.6 ms.** The ORBIT round trip through the
   broker costs about 1 ms (`ddict/list`). A small put or get adds 0.5 ms
   for the Dragon thread hop and the native DDict op (0.25–0.34 ms
   natively). That makes ORBIT about 5× native for small values. `keys` is
   slower (3.6 ms) than a get even on a near-empty DDict.
2. **Large values are dominated by WebSocket permessage-deflate.** ORBIT
   hard-wires `compression='deflate'` on the runtime side
   (`runtime.py`, `websockets.connect(..., compression='deflate')`) and the
   broker accepts it (`broker.py`, `ws_per_message_deflate=True`). Every
   request and reply is deflated and inflated on both legs. Base64 of random
   bytes compresses poorly, so this is pure CPU. 1 MiB took about 100 ms
   (about 10 MB/s). With deflate turned off on the connecting side (stage
   `k1nd`, `nodeflate/sitecustomize.py`, a diagnostic monkeypatch, not an
   ORBIT change), 1 MiB took 16–19 ms (5.9×) and 256 KiB took 6 ms
   instead of 28 ms. Small-value latency and throughput did not change.
   Recommendation: make compression configurable, or default it off for
   this traffic. Base64 inside JSON inside msgpack adds a further 4/3 on
   bytes values.
3. **Throughput ceiling: the single Dragon thread.** Batching helps up to
   about 256 keys per call. Beyond that, `get_many` levels off at about
   3.3–3.5k keys/s and `update` at about 2.2–2.5k keys/s, which is about
   1/(native per-key latency). All Dragon calls of an endpoint are
   serialized on one thread, by design (dragonhpc 0.15.0 hangs on concurrent
   DDict calls from several threads), and the backend issues one DDict
   operation per key. More clients do not raise that ceiling: one endpoint
   saturates at about 3.5k gets/s with 2 or more clients.
4. **Scaling with nodes is flat for one endpoint.** Spreading the DDict over
   2 or 4 nodes does not raise ORBIT throughput (2,986–3,526 gets/s at
   1/2/4 nodes, 64 keys/call): the serial Dragon thread, not the DDict, is
   the bottleneck. Natively, the same DDict scales (16 processes: 5.9k →
   10.2k → 15.7k gets/s). **Variant B (one endpoint per node) scales.** Four
   endpoints with their own 1-node DDicts reach 15.6k gets/s (4.4× one
   endpoint, matching native at 4 nodes) and 3.5k puts/s. Per-node endpoints,
   or several Dragon threads or processes per endpoint, are the way to scale.
   Batched Dragon calls in the backend (if Dragon offers them) would lower
   the per-key cost.
5. **Put throughput collapses with concurrent writers (open).** At 64 keys
   per call, one client puts 1.6k keys/s (40 ms per call). Two clients
   together get only 483 keys/s (268 ms per call), and throughput recovers to
   about 1.9k at 16 clients. Gets do not show this. The native run shows a
   similar collapse on a single manager (4 writer processes: 4.3k puts/s,
   16 processes: 503 puts/s). Through ORBIT, however, all puts come from the
   one Dragon thread, so this is not explained yet. This is a single run; the
   endpoint's ORBIT profile is kept with the raw logs. It needs a repeat
   before anyone acts on it.
6. **Broker placement does not matter inside NERSC.** A broker on the login
   node gives the same latency and throughput as one on a compute node,
   within about 5%. The 1 MiB latency is about 6–8% higher, which may come
   from deflate CPU on the shared login node.
7. **Endpoint teardown is not clean** (no plugin change made). With a
   DDict still present, SIGTERM to the endpoint ran into
   `dragon.on_host_stop exceeded 15s` and
   `Dragon thread busy after 20s at exit: runtime and DDicts may be left
   behind` (seen on the login node with `dragon -s`). The DDict manager
   processes and their `/dev/shm` pools stayed behind and had to be removed by
   hand. In the allocation the benchmark destroys the DDict through ORBIT
   first (that always succeeded), but the endpoint still did not exit within
   25 s after SIGTERM, under either launcher. With `dragon -m` the log shows
   "Detected an abnormal exit", then a paramiko ssh cleanup attempt failing
   on an encrypted key. With `dragon -s` the step had to be terminated. The script then kills the step and runs
   `dragon-cleanup` on every node. After the step kill, 2–6 Dragon processes
   were left on one node per teardown.
8. **Launch pitfall: `dragon -m` from inside an srun step.** Inside a step,
   Slurm sets `SLURM_JOB_NODELIST` to the step's node and leaves out
   `SLURM_JOB_NUM_NODES`, so the launcher fails ("SLURM_JOB_NUM_NODES
   environment variable is not set", then "Unable to acquire backend network
   configuration"). Its inner `srun` is also confined to one node.
   `step_env.sh` restores the job-level variables and drops the step
   variables, which matters for an endpoint started as a step of a
   `--no-shell` allocation. A batch-script launch is not affected.
9. **Nothing broke under load.** There were no hangs, errors or 413s at
   up to 16 concurrent clients, 1024-key batches (about 1.4 MB request bodies)
   and 1 MiB values. Endpoint start under `dragon -m -N 4` took about 15 s,
   and a DDict create took about 2 s.

## Caveats

- Single run per point and short phases (5 s per concurrent phase, 3 s per
  batch point), so the run-to-run variance is unknown. The put-concurrency
  anomaly (finding 5) needs a repeat.
- The broker, the Dragon frontend and the endpoint share node 0 with up to 4
  clients. The login-node broker shares `login20` with other users.
- `dragon -m -N K` picks the K nodes itself (e.g. `nid200035` for K = 1), so
  the endpoint is not always on node 0.
- At K = 4 the "16 clients" and "4 clients/node" points are the same
  configuration, measured twice; the second run is kept.
- A first `k1nd` attempt was discarded: an earlier copy of `run_bench.sh`
  that was edited while it ran kept going and ran a parallel load against
  the same endpoint. Only the clean rerun is in `data/`.
- Endpoint and broker logging ran at WARNING. ORBIT's own profiler output
  (`endpoint.prof`, about 10 MB per endpoint) is written to the endpoint's
  working directory and kept with the raw logs, not committed.

## Files

| file | what |
|---|---|
| `env.sh` | environment: venv, PATH, PYTHONPATH, cert/key, token (from `$RUNS/token`), log level |
| `run_bench.sh` | launch script (login node, against an existing allocation): stages `probe native broker-compute k1 k1nd k2 k4 varb native1 native2 native4 broker-login stop` |
| `step_env.sh` | lets `dragon -m` run from a one-node step (finding 8) |
| `bench_client.py` | ORBIT client: modes `setup destroy lat batch conc` |
| `native_bench.py` | native DDict baseline (a Dragon program): modes `lat conc` |
| `nodeflate/sitecustomize.py` | diagnostic for `k1nd`: no permessage-deflate on ORBIT's `websockets.connect` |
| `analyze.py` | reduces `data/` to `data/summary.json` and plots F1–F5 into `figs/` |
| `data/` | raw per-run JSON (per-rank JSON for the concurrent runs), `probe.txt` |
| `figs/` | F1–F5 as PNG and SVG |

## Reproduce

```sh
cd benchmarks/2026-11-dragon-ddict
# edit env.sh (OV, venv, cert/key paths); then a token, never ~/.radical/orbit:
export RUNS=/pscratch/sd/$USER/dragon-bench; mkdir -p $RUNS
(umask 077; python3 -c 'import secrets; print(secrets.token_urlsafe(32))' > $RUNS/token)

salloc -N 4 -C cpu -q interactive -t 02:00:00 -A <account> --no-shell
export JOB=<jobid>
./run_bench.sh probe native broker-compute k1 k1nd k2 k4 varb \
               native1 native2 native4 broker-login stop
scancel $JOB

python analyze.py $RUNS/data figs
```

The whole sequence ran in 42 minutes on 4 nodes (about 2.8 node-hours),
including one failed attempt. Do not edit `run_bench.sh` while it runs:
bash reads the script as it goes.
