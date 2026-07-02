# M0 spike — results

Campaign of `plans/broker_architecture_plan.md` §M0, run 2026-07-02 in a 2-node
Perlmutter allocation (`nid[001049,001052]`, job 55382432). Spike code (throwaway,
uncommitted) lives in `~/radical/m0_spike/`; this document records the
measurements and the design decisions they validate for the M2 freeze.

Environment: python 3.11 (`ve3`), websockets 16.0, msgpack, pydantic 2.13.3,
rhapsody 0.1.2 + Dragon. Link emulation: userspace TCP delay proxy (50 ms each
way + 3 MB/s token bucket per direction) — no root on HPC, no `tc netem`.

## Verdicts against the M0 gates

| Gate | Verdict |
|---|---|
| Correlation table survives reconnect-mid-flight (no leaks, no double-resolution, resume-key replace/reject) | **PASS** — 10/10 checks, localhost and over the emulated WAN link |
| Caller handle composes with strict `(owning_sid, pool_name)` ownership across the two-loop boundary | **PASS** — plugin-host thread → `run_coroutine_threadsafe` → routing loop; pool goes `creating→ready` with the probe result |
| Transport isolation: pings answered through a 60 s work-loop block | **PASS** — endpoint stays `present` the full 60 s; no `suspect` ever broadcast; both localhost and over the link |
| Dragon off-thread viability + max GIL hold | **PASS** — `rh.Session` constructs off the main thread (29.7 s wall, task runs, clean close); max GIL hold during init only 2.8 ms; see below |
| Throughput gate (rhapsody-shaped e2e vs current-stack baseline) | **DEFERRED** (user decision; `rhapsody_throughput.py` baseline not locatable). Handoff microbenchmark half was run and passed. |

No kill criteria were hit.

## Measurements

### Cross-thread handoff (transport loop ↔ work loop)

Coalesced-wakeup pattern: bounded deque + at most one outstanding
`call_soon_threadsafe` per burst, swap-drain under lock.

- Sustained: **~2.2 M msgs/s round-trip (0.46 µs/msg amortized, 2 crossings/msg)**, zero overflow.
- Idle wakeup (parked loop): **p50 ≈ 75–97 µs, p99 ≈ 105–135 µs**, max ≈ 440 µs — dominated by the epoll wake, not the queue.

Implication: the two-loop split costs nothing at storm rates; don't expect
sub-10 µs latency for sparse single messages.

### Envelope validation cost (decides pydantic vs slotted dataclasses)

200k messages/variant, 1 KB-body `request` and small `event`:

| variant | request ns/msg | request msgs/s | event ns/msg | event msgs/s |
|---|---|---|---|---|
| raw dict msgpack (baseline) | 2 580 | 388 k | 2 971 | 337 k |
| + pydantic v2 validate | 6 324 | 158 k | 7 325 | 137 k |
| + slots dataclass w/ checks | 6 510 | 154 k | 7 498 | 133 k |

**Decision input:** slotted dataclasses have *no* performance edge over pydantic
v2 (~2.5× baseline both). Recommendation: **pydantic v2** for the envelope
(clarity, consistency with the codebase), with the broker's pure-forwarding path
allowed to stay raw-dict (it only reads `kind`/`dst`/`corr_id` and never mutates
bodies). Even fully validated, ~137–158 k msgs/s sits ~5× above the ~30 k
events/s storm estimate (rationale §4).

### Two-node run over the emulated WAN link

Broker + proxy on nid001052, endpoints on nid001049; 15/15 checks pass.

- Echo RTT: p50 **203.6 ms** (min 202.9, max 205.7) — exactly 4 crossings × 50 ms;
  the real inter-node network adds ≪ 1 ms.
- Storm: 2000 pipelined requests (window 64), 0 failures, 304 req/s —
  latency-bound (64 ÷ 0.204 s ≈ 314/s theoretical), not a stack limit.
- **Pong latency during the storm: max 102.8 ms** — precisely the 2 × 50 ms the
  ping itself pays on the link; request traffic did not perturb keepalive at all.
- Kill mid-flight over the link: all 50 in-flight fast-failed with synthesized
  504s at exactly the 10 s grace, zero leaked/double-resolved entries; resume-key
  recovery then restores `present` and new calls complete.

### Dragon gates

Measured with a GIL-observer thread (0.5 ms sampling; gaps ≫ sleep = GIL held
or scheduler noise). `sys.getswitchinterval()` = 5 ms (default, unchanged).
Raw numbers: `~/radical/m0_spike/dragon_gate_results.txt`.

**(a) Off-thread `rh.Session` construction: YES.** Mirroring
`plugin_rhapsody.py` (`get_backend('dragon_v3')` → `rh.Session(backends=[b])`)
inside a non-main `threading.Thread` with its own asyncio loop: constructs in
29.7 s, runs a trivial `ComputeTask` to `DONE`, closes cleanly. **Constraint:
the process must be launched via the `dragon` launcher** — plain `python`
cannot bring up the Dragon runtime. (The dragon launcher placed its primary
process on the second allocation node; multi-node bring-up is part of init.)

**(b) Max GIL hold:**

| workload | max gap | >5 ms | >50 ms | >500 ms |
|---|---|---|---|---|
| idle baseline (10 s) | 0.58 ms | 0 | 0 | 0 |
| Dragon/rhapsody session init (off-thread) | **2.8 ms** | 0 | 0 | 0 |
| `msgpack.unpackb`, ~4 MB frame | **65.8 ms** | 30 | 26 | 0 |
| `cloudpickle.loads`, ~10 MB blob | **57.8 ms** | 60 | 30 | 0 |

Key finding: session init's 30 s wall is runtime bring-up with the GIL
*released* — it never threatens the heartbeat. The real GIL threats are the C
deserialisers: a 4 MB `unpackb` pins the GIL ~66 ms, a 10 MB `cloudpickle.loads`
~58 ms; neither releases the GIL, so every thread — including the transport
thread — freezes for that span. Scaling ~linearly: an 8 MB frame ⇒ ~130 ms; an
unpack+unpickle chain ⇒ ~120 ms+.

**Heartbeat floor (the number M0 owed):** pong-timeout floor **≥ 500 ms,
prefer 1 s**. The plan's proposed default (ping 1 s / pong timeout 3 s) clears
the worst observed hold by >20×, so it **stands**. The aggressive LAN profile
(`suspect` ≈ 1 s) is feasible only *with* the M4 offload-hygiene item (large
unpack/unpickle via `asyncio.to_thread` keeps the holds bounded per-call, and
the frame cap bounds them by construction: a 4 MB cap ⇒ ≤ ~66 ms hold).

## Design lessons for M2–M4 (from building the spike)

1. **Fast-fail needs a broker-side forwarding table.** Keep a lightweight
   `inflight: corr_id → (src, dst)` map besides the requester-owned pending
   entry. On `lost`, synthesizing 504s from it converts client-timeout waits
   into grace-bounded failures with zero leaks — and it is what lets "single
   owner per pending entry" coexist with fast failure.
2. **The teardown guard is load-bearing on *both* paths.** `registry.get(name)
   is ws` must guard `_on_socket_drop` *and* `_on_lost`: a replaced socket's
   handler fires its `finally` after resume has installed the new socket and
   would otherwise re-suspect a live participant.
3. **websockets 16 keepalive is genuinely transport-independent.** With
   `ping_interval=1 / ping_timeout=3` on a dedicated transport thread, a work
   loop pinned in `time.sleep(60)` never trips the timeout. The structural
   isolation claim holds with the stock library — no custom ping machinery.
4. **Coalesced wakeups are the whole handoff game.** One `call_soon_threadsafe`
   per burst (re-armed only after a drain) is what yields ~2.2 M msgs/s;
   naive one-wakeup-per-message would be ~10× worse at the p50 wakeup cost.
5. **Inbound handoff must never block the transport loop.** A hard-bounded
   inbound queue would stall keepalive — the exact failure isolation exists to
   prevent. Spike used soft-bound + overflow counter; the real implementation
   answers overflow with the 503 fast-fail contract instead (the per-src pending
   cap stays strict and synchronous at the caller).
6. **`ClientConnection.latency` (websockets) gives pong RTT for free** — usable
   for the liveness tie-in and ops visibility; no envelope ping needed,
   confirming the "no app-level heartbeat" decision.
7. **An endpoint hosting the Dragon-backed rhapsody plugin must be launched
   via the `dragon` launcher** — session construction is thread-agnostic (M0
   verdict) but runtime bring-up is launcher-dependent. This lands on the
   endpoint wrapper script / M4 runtime docs, not on the plugin.
8. **Non-interactive SSH on Perlmutter drops `PYTHONUSERBASE`** (NERSC sets it
   to `~/.local/perlmutter/python-3.11`, where user-site packages live) — remote
   process launch must carry the env explicitly; this is exactly the job of the
   endpoint wrapper script.

## Caveats

- 2 nodes only: Dragon GIL-hold numbers are a **lower bound**
  (`Batch(num_nodes=…)` cost grows with allocation size); the heartbeat floor
  carries a safety margin and must be re-validated at target scale before an
  aggressive profile ships (plan §M0).
- The delay proxy shapes bandwidth per direction with a token bucket
  (100 ms-worth initial burst); `--bw-mbps` is decimal megabytes/s.
- The spike broker enforces its pending cap globally, not per-src (single-`src`
  table in the spike); the real broker implements the per-src cap as specified.
- The e2e rhapsody-shaped throughput comparison against the current stack is
  deferred with the throughput-gate baseline (see verdict table).
