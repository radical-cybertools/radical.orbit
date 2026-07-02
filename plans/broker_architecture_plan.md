# ORBIT: broker + endpoint (participant) architecture — implementation plan

## Context

ORBIT today is **Client → Bridge → Endpoint**. The shape we need it to take is
driven by a use case the current tiering cannot express cleanly: **a node is
normally both a server and a consumer at once.** A workflow manager submitted into
a job allocation spins up an endpoint, delegates part of its work to ad-hoc
endpoints on other resources, and *simultaneously* serves task-execution requests
on its own. Today "serving plugins" (endpoint) and "calling other endpoints"
(client) are separate tiers on separate transports, so such a node must be an
endpoint that builds a client internally — exactly what the task dispatcher does.

Two further facts compound this:

1. **Client↔bridge is stateless HTTP+SSE; bridge↔endpoint is a heartbeated
   WebSocket.** The bridge can't observe client liveness (sessions expire only on
   idle TTL), notifications can only be broadcast (no per-recipient routing), and a
   node behind the HTTP tier cannot be *called*.
2. **"Bridge" no longer fits.** It routes, correlates, tracks topology, and hosts
   active plugins. It is an active hub.

**Outcome:** one node type — an **endpoint** (a participant that dials the hub over
an outbound WebSocket and may *serve* plugins, *consume* them, or both) — and one
hub, the **broker** (an *active broker*: routes between endpoints, correlates
request/response, tracks topology + liveness, hosts plugins). Control is a **clean
star**; bulk data goes out-of-band. **Liveness is structurally isolated from
plugin behaviour** (dedicated transport thread on the endpoint; lean routing loop
on the broker) so failure detection can be aggressive — seconds, not tens of
seconds. Capabilities compose: the dispatcher and event replay are optional
plugins loaded per use case; the **gateway is a broker module** (the sole
non-participant ingress, on by default).

**No external users/plugins/endpoints exist yet → no backward compatibility.**
Real external users are imminent, which is why disruptive structural work happens
now (cheap pre-users; expensive after). This plan **starts from a token-authenticated
bridge** — the pre-broker security step (`security_token_mitigation.md`) already gates
the HTTP ingress and `/register` with a shared bearer token (cookie for the browser).

**Docs rule throughout:** present-tense rationale only — *why the thing is what it
is*. Never reference prior names or "what changed".

**Anchor rule:** references into the current tree use **symbol names**
(`function`/`method` + file), not line numbers — line anchors in this plan have
already drifted once across an unrelated PR; symbols are stable.

> Rationale, alternatives considered, the concurrency/correctness model, and the
> sequencing reasoning live in `broker_architecture_rationale.md`.

## Settled decisions + motivation

| Decision | Motivation |
|---|---|
| One `endpoint` node that may serve and/or consume; one `broker` hub; clean star | A node is normally both server and consumer (workflow-manager-in-allocation case). One abstraction expresses it directly; the star gives every future use case one routing model. |
| Rename `bridge`→`broker`; "active broker" in prose | The node routes/correlates/hosts plugins. "Broker" is CS/HPC-legible and reinforces ORBIT's "Brokerage". |
| Redesign the wire protocol: one symmetric, **versioned** envelope (`src`/`dst`/`corr_id`, request/response both ways), **msgpack-only on the wire** | We rewrite broker + runtime anyway; no compat consumers → a clean symmetric, versioned contract. One encoder/parser, native binary bodies (no base64, no dual JSON/msgpack paths, no hand-built fast-path serialization). The browser never speaks this protocol — it talks to the gateway. |
| Strictly star control; **bulk data out-of-band** (Globus, shared FS, SSH tunnels) | Direct endpoint↔endpoint is impossible vs. HPC firewalls. Globus already moves bytes collection-to-collection. |
| Serve plugins + push now; **correlated reverse RPC designed-for, not built** | Push (rhapsody task-state → callbacks) is the driver and needs no reply; the only correlated-reverse consumer (GUI prompt) is deferred. |
| **Transport isolation** — endpoint: dedicated transport thread with its own event loop owning the WS + heartbeat; broker: lean routing loop, hosted plugins on their own loop/thread | Liveness reflects process/host/network health, not plugin behaviour. This is a *structural* guarantee where the offload discipline is only a convention — and it is what makes fast (seconds-scale) failure detection possible. Single socket retained. |
| **Liveness is transport-level only** (WS protocol ping/pong; no app-level heartbeat in the envelope); **two-stage `suspect` → `lost`**; a **separate, longer, per-pool reclaim-drain timer** for pilots | The current app-level ping's pongs are already ignored (`register` handler in `bridge.py`: pong branch is a no-op) — dead protocol surface. A missed transport ping ⇒ socket close ⇒ `suspect` (fast signal); grace ⇒ `lost` (actionable). Reclaiming a 1000-node allocation must not fire on a blip — distinct timer. |
| **Offload discipline demoted to work-loop hygiene** | With transport isolation, a blocking plugin call stalls request handling, not liveness. Offloading (`asyncio.to_thread`) remains the documented pattern for keeping the work loop responsive — it is no longer load-bearing for correctness. |
| **Loop-lag watchdog kept as a cheap safety net** (broker) | The lean routing loop should never stall; if it somehow does, suppressing the `lost` declarations that would fire on resumption converts a bug into a logged hiccup instead of a fleet-wide false reclaim. ~20 lines; keep. |
| **Live event delivery in core: explicit `subscribe` registry** (exact/wildcard `(plugin, topic)` patterns per endpoint) + filter-at-edge; **buffering/replay an optional plugin** | The runtime auto-sends `subscribe` when a callback is registered (mirrors the client callback registry in `BridgeClient.register_callback`). Deterministic, self-describing, and the natural seed of full selective routing. Durability stays composable. |
| Topology always recovers on (re)connect; **ephemeral broadcasts durable only when replay is loaded** (documented) | Core stays stateless; durable delivery is a feature a use case opts into, not a core obligation. |
| **Single socket in v1; no frame chunking.** A protocol-level **frame-size cap** bound by the design inequality `frame_cap ≤ heartbeat_timeout × min_bandwidth / 2`; inbound traffic counts as proof-of-life | No msgpack streaming exists in the tree, and chunking would be new reassembly machinery for a problem the arithmetic doesn't show at v1 timeouts. Size-aware batching already exists client-side (rhapsody `WS_PAYLOAD_LIMIT`) and absorbs a smaller cap naturally. See rationale §4 for the fast-heartbeat regime. |
| Session: client-supplied `sid` (create-or-reconnect) + lifetime policy; reserved `default` is persistent | The two feature requests: restart-within-session + on-demand persistent default. |
| **Strict per-session pool isolation** — pools keyed `(owning_sid, pool_name)`; pool names are session-local; no cross-session attach; **data model decided before M0** | Closes feature request 1 exactly, isolates concurrent clients, and *simplifies* the dispatcher: config-compatibility checks and attach-vs-409 logic (`_pool_configs_compatible`, the reuse path in `_materialise_pool`) are deleted rather than ported. *(User decision.)* |
| **Sessions record their owner** — the broker stamps `src` on every forwarded request (client-supplied `src` is overwritten, never trusted); `register_session` binds `sid → owner identity`; **reattach to an existing `sid` from a different owner is rejected (403)** | Liveness-driven `ephemeral` expiry needs a trustworthy owner binding; improvising it later is a spoofing hole. Owner-checked reattach forces session recovery through the same endpoint name — deterministic, and it closes sid-guessing within the tenant (strong under mTLS; name-based under the shared token). |
| Identity = `(name, credential)` tuple; **`register_ack` mints a per-identity `resume_key`**; reconnect must present it to replace a live socket | With a *shared* token, bare same-identity-replace is a takeover primitive among token holders. The resume key makes replace safe now (~20 lines) and keeps the mTLS upgrade a value change, not a logic change. |
| **User callbacks fire on a dedicated dispatcher thread** in the runtime — never on the transport or work loop | A slow user callback must not stall the consumer's own request handling (and could never be "disciplined" — it is user code). Structural, like transport isolation. |
| **Backpressure bounded everywhere:** request path fails fast (503 + `Retry-After` at the endpoint concurrency cap; per-`src` pending cap at the broker), event path drops-oldest per subscriber with a dropped counter + `seq` gap detection | Unbounded queues (today: SSE queues, per-request `create_task`) turn a slow consumer into broker memory growth. Requests are never silently dropped; events are observably lossy. See rationale §4. |
| Admin (`terminate`/`disconnect`) = `control` ops via the gateway (primary) + a **process floor: uvicorn's default SIGTERM handling + the self-`SIGTERM` terminate route** (`terminate_bridge` in `bridge.py`) | Inherits the pre-step token gate; the floor keeps the broker stoppable without the gateway. (There is no custom signal handler in `bridge.py` to port — the floor is uvicorn's own.) |
| HTTP/SSE + Explorer = **broker-hosted `gateway` module** (not a `Plugin`); participant protocol mandatory otherwise | The gateway needs the pending table, the raw event tap, the uvicorn app, CORS, and the auth gate — an interface far wider than `Plugin`'s, and core must gate `/register` regardless. An explicit `gateway.py` module with a constructor-declared interface is honest modularity. On by default; disable with a flag. |
| Dead-simple consumer = zero-plugin endpoint, SDK sugar, documented separately from the gateway | Python apps get the real model without boilerplate; the gateway is for non-participants. |
| Dispatcher stays a broker-hosted plugin; harden the host with **supervised task creation** (a `spawn()` helper that logs/reports background-task exceptions) | Handler *raises* are already isolated on both hosts (`BridgePluginHost.handle_request`, `EndpointService._handle_request`); the real gap is `create_task`'d background coroutines whose exceptions die silently. Externalize only at the untrusted-plugin trigger. |
| Public API may reshape (`BridgeClient`/`EndpointClient`/`PluginClient` collapse); keep `get_plugin(target,…).method()`, `register_session`, `serve` shapes | rhapsody is rewritten right after; keep call-site shape for examples + future users. |

**Out of scope (deferred):** correlated server-initiated RPC; a **dedicated
control socket** (transport isolation covers its v1 motivation; revisit only if
measurement shows wire-level starvation the frame cap can't fix); selective live
routing beyond the exact/wildcard `subscribe` patterns (per-tenant isolation is
later); mTLS / per-participant auth and tenant authz (shared token + resume key
now; mTLS is the upgrade); direct endpoint↔endpoint data channels; migrating the
Explorer to a full participant; externalizing plugin hosts into managed
processes. Event **replay** is *not* deferred — it is an optional plugin (loaded
per use case).

## Protocol (frozen at M2, informed by the real M0 spike) — new module `protocol.py`

One envelope; `kind` discriminates. Common fields: `version, id, corr_id?,
channel?, kind, src, dst?`. **Wire encoding is msgpack, always** — one packer,
one parser, `body` is native bytes (no base64, no JSON/text path, no hand-built
response string; the fast-path serialization in `EndpointService._handle_request`
is deleted, not ported). Requests/responses retain `method`/`path`/`headers`/
`body` so direct-dispatch (`_match_route`, `RequestShim`) is reused unchanged.

- `request`  : `+ method, path, headers, body, is_binary`
- `response` : `+ status, headers, body, is_binary`
- `event`    : `+ plugin, topic, session?, ts, seq, data`
- `register` : `+ identity:{name, credential?, resume_key?}, role, plugins, capabilities`
- `register_ack` : `+ ok, reason?, capabilities, resume_key`
- `subscribe` / `unsubscribe` : `+ patterns:[{endpoint?, plugin?, topic?}]`  (None = wildcard, mirroring the client callback-registry tuples)
- `topology` : `+ participants:{name:{role, plugins, liveness}}`  (rich plugins dict — see below)
- `control`  : `+ op∈{shutdown,error,terminate,disconnect}, dst?`

**No `ping`/`pong` in the envelope** — liveness is WS-protocol-level only.
`version` + `capabilities` from day one. `channel` reserved for the deferred
blocking-served-call / in-band-isolation cases. **`seq` is broker-assigned and
frozen with the envelope:** monotone per session for session-scoped events,
monotone per the global session-less stream otherwise; consumers detect drops as
`seq` gaps. `ts`/`seq` let the optional replay plugin work with no wire change.
The envelope lives in a **new `protocol.py`** (not an in-place rewrite of
`models.py`) so the old stack keeps compiling during build-alongside; `models.py`
is deleted at the final flip. `parse_message(data)` replaces
`parse_endpoint_message`/`parse_bridge_message`. Whether messages validate via
pydantic or slotted dataclasses is decided in M0 by measurement (per-message
validation cost at task-storm rates).

### Design resolutions (settled before coding)

- **Correlation: one broker-side pending table + one per endpoint.** Broker is a
  switchboard *and* a participant (`src='broker'`). Response routing fork:
  `dst=='broker'` → broker pending table; else → `endpoint_ws[dst]`. `corr_id`s
  **namespaced by `src`**; single owner per entry for timeout/cleanup. The
  **reconnect-mid-flight timeout/cleanup race** is nailed in M0 against a real table.
- **Register-ack + resume key + grace invariant (liveness keyed on identity).**
  The runtime waits for `register_ack` before sending. First registration of an
  identity mints a `resume_key` (broker memory only); a reconnect presenting the
  key **replaces** the stale socket; a same-name register *without* it is
  rejected while the old registration is within grace. Invariant:
  a valid re-register cancels any pending `lost` timer for that identity and
  no-ops the old socket's topology-removal (keep the guard
  `endpoint_ws.get(name) == ws` in the `register` teardown path, `bridge.py`).
  Broker restart clears resume keys → first-come re-registration, same as today.
- **Canonical topology — `plugins` is a RICH DICT.** `{name: {role, plugins:
  {pname: {namespace, version, ui_config, enabled}}, liveness}}`. `get_plugin`
  needs the plugin **`namespace`**, which today travels on HTTP `/endpoint/list`
  but **not** the endpoint-facing WS topology
  (`Bridge._broadcast_topology_to_endpoints` strips it); the rich dict fixes
  that. `/endpoint/list` becomes gateway sugar.
- **TRANSPORT ISOLATION (load-bearing — document prominently).**
  *Endpoint:* a dedicated transport thread runs its own asyncio loop owning the
  single outbound WS: connect/reconnect/backoff, register handshake, WS
  keepalive, and raw frame send/recv. Envelope pack/unpack and all plugin/handler
  work run on the **work loop** (the main loop); handoff is a pair of
  thread-safe queues (`call_soon_threadsafe` wakeups, batched under load).
  *Broker:* the inverse — the uvicorn/server loop does **transport + routing
  only** (tiny dict operations); the hosted-plugin host runs on its own loop in
  its own thread, reached through the same handoff pattern (the M0 caller handle
  is thread-aware from the start).
  *Consequence:* the **offload discipline** (`asyncio.to_thread` for blocking
  calls, the established rhapsody `_prepare_batch` pattern) remains the
  documented hygiene rule for work-loop responsiveness, but liveness no longer
  depends on every plugin author obeying it.
  *Known bound:* the GIL. A C extension holding the GIL without release (large
  `msgpack.unpackb`, cloudpickle, possibly Dragon-native init) delays the
  transport thread by up to the longest GIL hold. **M0 measures the max GIL hold
  during `rh.Session(...)` init and large-frame unpack; the heartbeat timeout
  floor is set above the observed max.**
- **Liveness: transport-level, two-stage, fast.** WS protocol keepalive on both
  sides (broker: server ping config; endpoint: `websockets` client
  `ping_interval`/`ping_timeout`). A missed pong closes the socket ⇒ the
  identity enters **`suspect`** (topology signal, immediate); the grace timer
  then runs ⇒ **`lost`** (actionable: session cascade per policy). A valid
  re-register at any point cancels both. **Proposed defaults (pending the M0
  GIL measurement): ping 1 s, pong timeout 3 s ⇒ `suspect` ≈ 3–4 s; grace 10 s ⇒
  `lost` ≈ 13 s.** Profiles are deployment-tunable: on-cluster LAN can drop
  toward `suspect` ≈ 1 s; WAN/oversubscribed-login-node links get slower
  profiles (scheduling jitter of seconds is real there — `suspect`, not `lost`,
  absorbs it). Inbound traffic counts as proof-of-life (reset the idle timer;
  ping only an idle link). **Reclaiming a session-owned pool's pilots is a
  *separate*, longer, per-pool/policy-configurable drain timer started at
  `lost`.** A clean close skips `suspect` and is immediate.
  *Verify in M2/M3:* uvicorn exposes adequate server-side ping cadence
  (`ws_ping_interval`/`ws_ping_timeout` are server-wide — acceptable, the
  heartbeat is uniform by design); if it can't surface per-connection timing,
  the broker accepts `/register` via the `websockets` server library directly.
- **Frame-size cap, no chunking.** `frame_cap ≤ heartbeat_timeout × min_bandwidth / 2`
  (e.g. 3 s × 3 MB/s ⇒ ~4 MB cap; today's cap is 10 MB). Oversized application
  payloads are the sender's problem — rhapsody's client already does size-aware
  batching (`WS_PAYLOAD_LIMIT`), which is the pattern. If a future fast-heartbeat
  profile squeezes the cap too far, the escalation path is WS-level
  fragmentation with interleaved control frames (RFC 6455 permits; verify
  library behaviour) — **never** envelope-level chunk/reassembly.
- **Loop-lag watchdog (broker, safety net).** The routing loop measures its own
  progress; after a detected stall longer than the heartbeat budget it
  **suppresses** the `lost` declarations that would fire on resumption ("we were
  blind", not "they left"). With the lean loop this should never trigger; it
  bounds the blast radius if it does.
- **Event delivery: live in core, replay optional.** An endpoint's runtime sends
  `subscribe` frames as callbacks are registered; the broker keeps a per-endpoint
  interest set and forwards an event only to endpoints with a matching pattern
  (+ filter-at-edge in the client callback registry,
  `BridgeClient._dispatch_notification` semantics, as today). Core holds no
  event buffers and re-sends current topology on (re)connect. Core also exposes
  one **unfiltered raw-stream tap** to broker-hosted subscribers — the single
  (stateless) event hook — because a retaining subscriber must see events for
  endpoints that are *not* connected. The **optional replay plugin** subscribes
  to that tap, retains the stream (global ring for *session-less* broadcasts +
  per-session buffers, `ts`/`seq`, per-subscriber cursors), and replays what a
  reconnecting subscriber missed. **Guarantee:** topology + state-mirroring data
  recover on reconnect regardless (re-sync + topology re-send); **ephemeral
  events are durable only when replay is loaded** (intended, documented).
- **Replay→live splice (replay plugin).** Delivery is **at-least-once on the wire
  with `seq` dedup at the client = effectively-once**: the cursor advances on **ack**,
  so a drop mid-replay resends rather than loses, and the client discards duplicates
  by `seq`.
- **Backpressure (core).** *Request path:* per-endpoint work-loop concurrency cap
  with a bounded accept queue — overflow answers **503 + `Retry-After`
  immediately** (never silently dropped; the SDK retries with backoff; the 504
  deadline stays the backstop). Broker: per-`src` pending-table cap — overflow
  raises synchronously at the caller ("too many in-flight"). *Event path:*
  per-subscriber bounded queue, **drop-oldest** + dropped counter; consumers see
  the gap as a `seq` discontinuity and re-sync (or lean on the replay plugin,
  whose buffers are themselves bounded with their own counter). A slow
  subscriber can never backpressure the routing loop or other subscribers.
- **Broker-crash semantics:** **pool ownership survives** (durable plugin-level
  store); **pending entries are lost** → in-flight calls fail, callers retry;
  **resume keys are lost** → first-come re-registration; **replay
  buffers/cursors are lost** → replay degrades to re-sync on restart (topology +
  state-mirroring recover; ephemeral events around the restart are not).
- **Profiling ported, not dropped.** Re-label `_prof.prof(...)` sites with component
  prefixes (`broker_*`, `gateway_*`, `endpoint_*`) keyed by the new `id`/`corr_id`.

## Milestones

**Pre-M0:** decide the **session-scoped pool-ownership data model on paper** —
**strict per-session isolation** (user decision): pools keyed
`(owning_sid, pool_name)`, pool names session-local, no cross-session attach;
the sharing machinery in the dispatcher (`_pool_configs_compatible`, the
attach-or-409 path in `_materialise_pool`) is deleted, not ported. The model
**must answer pilot-lifetime × ownership × crash-recovery** — no spike exercises
it. Required answers: a session-owned pool's pilots **follow the session
lifetime policy**, drained via the **separate reclaim-drain timer** (not the
liveness grace) — `ephemeral` → drain after the reclaim timer past `lost`;
`ttl` → until expiry; `persistent` → to walltime. On **broker restart**,
ownership + pilots replay from the durable logs (`task_dispatcher_state.py`
JSONL + snapshot; note the store must gain an `owning_sid` field and a
sid-scoped state-dir layout — today recovery is *lazy*, triggered only by a
client re-declaring the pool, so restart-time replay is **built here, not
preserved**), owner reconnection reconciled under the grace, pools whose owner
never returns drained.

**Sequencing principle:** milestones run **sequentially**; every milestone
**merges to `devel` green as it completes** — build-alongside keeps `models.py`
+ the old stack as the `bin/` default throughout. Branch convention:
**`feature/broker_<n>`, one per milestone, PR into `devel`**. The **final flip
is its own small PR**: switch `bin/` defaults to the new stack, delete
`models.py` + the superseded modules + the old suite. (Activation must be
atomic; merging needn't be.)

### M0 — real routing + caller-handle spike (drives the M2 freeze)
- A *minimal but real* broker that routes by `src`/`dst` with a real correlation
  table, plus the in-process async caller handle the dispatcher will use — built
  against the **target** (strict-isolation) pool-ownership model and the
  **two-loop topology** (the handle is thread-aware: plugin-host loop → routing
  loop). **Exercise reconnect-mid-flight**, including the resume-key
  replace/reject paths.
- **Transport-isolation prototype:** endpoint transport thread + work loop; keep
  pings answered during a synthetic 60 s blocking plugin call.
- **Dragon gates (blockers for the heartbeat defaults):** (a) can
  `rh.Session(...)` be constructed off the main thread at all? (b) measure the
  **max GIL hold** during Dragon init and during large-frame
  `msgpack.unpackb`/cloudpickle loads — the pong-timeout floor sits above it.
- **Environment (decided): the whole M0 campaign runs in a 2-node Perlmutter
  allocation** — broker spike on the login node, endpoint + Dragon gates on
  compute (the forward-tunnel mode covers compute→login if arbitrary-port TCP
  is restricted, and gets exercised for free). Link emulation is the
  **userspace delay proxy** (~50 ms each way + token-bucket bandwidth cap) —
  no root on Perlmutter, so no `tc netem`. Caveat recorded with the results:
  GIL-hold numbers at 2 nodes are a **lower bound** (`Batch(num_nodes=…)` cost
  grows with allocation size) — the heartbeat floor carries a safety margin
  and is re-validated at target scale before any aggressive profile ships.
- **Throughput gate (rhapsody-shaped, remote link).** Two measurements on the
  prototype, over an emulated 100 ms-RTT / bandwidth-limited link (`tc netem`
  or a delay proxy): (a) the **cross-thread handoff microbenchmark** —
  amortized and worst-case (idle-loop wakeup) cost of the bounded-queue +
  `call_soon_threadsafe` crossing, sustained messages/s; (b) an **end-to-end
  rhapsody-shaped run** mirroring `rhapsody_throughput.py`'s batch ladder:
  pipelined batched submission (template + non-template) *and* a sustained
  state-notification storm (per-event and `task_status_batch`-batched),
  recording tasks/s, events/s, per-loop CPU, and **pong latency during the
  storm** (the liveness tie-in). **Pass bar: ≥ the current stack's measured
  baseline on the same link** (`rhapsody_throughput.out`: ~6.5 k tasks/s
  localhost, ~1.3 k tasks/s LAN today) and an unbatched event ceiling
  consistent with the ~30 k events/s estimate (rationale §4).
- Output: validated envelope + concurrency + liveness decisions; M2 freezes
  them. Kill criteria: caller handle can't compose with strict pool ownership;
  Dragon init can't run off-thread *and* its GIL holds exceed any acceptable
  heartbeat budget (that would force a per-role heartbeat profile — decide it
  here, not in M6); the throughput gate regresses the current-stack baseline.

### M1 — sessions, transport-agnostic slice (lands on the *current* stack)
- In `plugin_base.py`/`plugin_session_base.py` only — this code survives the
  rewrite untouched, so it ships **feature request 2 and most of feature
  request 1 months before the cutover**:
  `register_session(sid=None, lifetime='ephemeral'|'ttl'|'persistent', ttl=<n>)`
  with create-or-reconnect; `ParameterError`/409 on conflicting lifetime /
  incoherent pairs; per-session `{lifetime, ttl, last_access, owner?}` record
  (owner filled in by M6); time-driven expiry for `ttl`; `persistent` never
  expires; reserved `default` = persistent, on-demand, auto-created only in the
  base default path under a lock. Refactor `_forward`/`_cleanup_expired_sessions`.
- **Operator reclaim path for `default`-owned pools (decided): explicit
  `cancel_all` only** — persistent default pools have no liveness reclaim and
  live until the operator acts (predictability over cleverness; a per-pool idle
  policy can arrive later as an opt-in). Document `default` scope explicitly:
  the session registry is per plugin instance, so `default` is per-plugin (a
  `rhapsody` default and a `psij` default are distinct sessions).
- `ephemeral` (liveness-driven) is a stub until M6 — documented as such.
- **Two non-conforming `register_session` overrides** (not three): `rhapsody`
  (`PluginRhapsody.register_session` builds the session directly to avoid a race
  on shared plugin state and returns an `initializing` status) and
  `iri_instance` (fixed `_auto_sid`, **exempt**). The task dispatcher already
  delegates sid minting to the base (`super().register_session`) and needs no
  handling. Adapt rhapsody's override to the new signature here.

### M2 — Protocol foundation (`protocol.py`, alongside `models.py`)
- The msgpack-only envelope + `parse_message` + `subscribe`/`resume_key`/`seq`
  semantics as frozen above; its tests. Old stack untouched.

### M3 — Broker core (`broker.py`, alongside `bridge.py`)
- `Broker` = lean routing loop (switchboard + self-participant): registry, route
  by `dst` with the response fork, one broker-side pending table (per-`src`
  cap), **live delivery via the subscribe registry + raw-stream tap**,
  `topology`/liveness (`present`/`suspect`/`lost`, transport keepalive, grace,
  resume-key reconnect invariant), **loop-lag watchdog (safety net)**,
  `register_ack`.
- **Hosted-plugin host on its own loop/thread**, hardened with **supervised task
  creation** (a `spawn()` helper wrapping `create_task` that logs/reports
  background-task exceptions — handler raises are already isolated in
  `BridgePluginHost.handle_request`; the gap is background coroutines).
- **Not in the hub:** HTTP catch-all proxy, `/events` SSE, `/`+`/plugins` UI,
  CORS (→ gateway module, M5). The proxy `pending`/504 logic moves there. Core
  keeps WS `/register` + the token gate on it, `_prof` (re-labelled), and the
  process floor (uvicorn default SIGTERM handling + a terminate `control` op).

### M4 — Endpoint (participant) runtime — converge `service.py` + `client.py`
- One runtime, **transport thread + work loop** as specified (single outbound
  WS; keep tunnel modes, TLS handling, reconnect/backoff under the resume-key
  invariant), `register` with identity tuple + role + served plugins (**zero =
  pure consumer**), wait for `register_ack`, serve via
  `_handle_request`/`_match_route`/`RequestShim`; bounded request concurrency
  (503 fast-fail at the cap).
- **Offload hygiene applied** where it matters for work-loop responsiveness (the
  rhapsody `rh.Session` init `to_thread` fix — *contingent on the M0 Dragon
  gate* — following the existing `_prepare_batch` pattern).
- **Consumer side:** `request` `dst=target`; per-endpoint pending table by
  `corr_id`; `get_plugin(target,…).method()` + `register_session(...)` keep
  shape (sync facade over the runtime via `run_coroutine_threadsafe`).
  Discovery from the rich topology. Reconnect **re-syncs state**.
- **Events over WS:** reuse the callback-registry semantics; registering a
  callback emits `subscribe`. **User callbacks are dispatched on a dedicated
  callback thread** (bounded queue) — never on the transport or work loop.
- **Callback plugins:** `serve(PluginClass)` (push/event direction only in v1).
- **Zero-plugin consumer auto-naming:** `consumer.<uuid>` — for fire-and-forget
  consumers only. **Session reattach requires a stable, client-supplied endpoint
  name** (SDK parameter): reattach is owner-checked (M6), so an auto-named
  restart cannot recover its sessions by design. The runtime **retries a
  name-in-use registration with backoff** until the stale registration passes
  `lost` (a crashed client holds no resume key; under the fast keepalive the
  name frees in seconds) — so "restart with the same name → sessions come back"
  is automatic from the application's view.
- Move `RequestShim`/`_match_route` to `dispatch.py` (`bridge_plugin_host.py`
  imports it). **Keep `Plugin.add_route_*` dual registration** so plugin-level
  `TestClient` tests stay transport-agnostic.
- **Port every plugin `client_class` helper off `httpx`** (11 plugins carry one).

### M5 — Gateway module (compat tier)
- `gateway.py`, a broker module with an explicit constructor interface
  (pending table, event tap, topology, credential store) — on by default,
  `--no-gateway` to disable: HTTP catch-all → `request` over the shared broker
  table (its 504 deadline stays **long** — a thousands-of-task submit genuinely
  takes time; consider async/streaming), `/events` SSE fan-out (a subscriber of
  live delivery, bounded per-client queues with drop-oldest + counter),
  `/`+`/plugins/*` Explorer serving, **CORS allowlist**
  (`Bridge._setup_middleware`), the carried-over **token gate + `/auth` cookie**
  for HTTP ingress (core keeps the `/register` gate), and the discovery/admin
  HTTP surface: `/endpoint/list`, `/endpoints`, `/endpoint/disconnect/{name}`,
  `/broker/terminate`. **Preserve exact HTTP paths** (Explorer JS unchanged).
  **Delete** the dead 501 `/endpoint/*` stubs. TTL/persistent sessions only.

### M6 — Session liveness integration
- Wire the M1 session model to the new runtime: broker-stamped `src` →
  `register_session` records the **owner identity**; liveness-`lost` → the
  **separate reclaim-drain timer** for `ephemeral`-owned resources.
  **Reattach is owner-checked:** create-or-reconnect on an existing `sid` from
  a different owner identity is **rejected (403)** — session recovery goes
  through re-registering the same endpoint name (see the M4 retry note). With
  the shared token, "same owner" is effectively same-name; under mTLS it
  becomes cryptographically strong with no logic change. Gateway-originated
  sessions carry no participant identity and stay capability-style within the
  token trust domain (documented). Enriched
  `on_topology_change(participants)` (rich schema, role + `suspect`/`lost`
  liveness); the framework emits the post-grace `lost`.

### M7 — task_dispatcher port onto strict per-session pools (de-risked by M0)
- Replace the loopback-HTTP `BridgeClient` (`_start_bridge_client`) with the M0
  in-process caller handle (no `bridge_url`, no loopback, no SSE thread, no
  `_wait_for_bc` init barrier — that is a 10 s spin-wait, not marshalling; the
  real cross-thread paths to replace are the `call_soon_threadsafe` /
  `run_coroutine_threadsafe` sites). **All 16 `to_thread` wrappers go** — every
  one wraps a BridgeClient-backed HTTP call; there are no genuine
  CPU/deser offloads in this plugin. Consume the rich topology; the framework
  `suspect`/`lost` + grace replaces `_seen_child_endpoints`.
- **Port pools onto `(owning_sid, pool_name)`** (pre-M0 model): lifetime tied to
  the session, durable JSONL store re-keyed with `owning_sid` (state-dir layout
  change), session-close teardown added (currently absent), pilots reclaimed via
  the separate drain timer. Delete the sharing machinery. Single rewrite.

### Final flip — small PR
- Switch `bin/` entry points to broker + runtime; delete `models.py`,
  `bridge.py`, old `service.py`/`client.py` paths and the superseded tests.

### M8 — Event-replay plugin (optional; deferrable past users)
- Broker-hosted, loaded per use case: subscribes to the raw-stream tap, retains
  (global ring for session-less broadcasts + per-session buffers, `ts`/`seq`,
  per-subscriber cursors), replays with at-least-once + `seq` dedup. Buffers bounded
  by **age and size — per-session (so one chatty session can't evict another) and the
  global ring by total bytes — with a dropped-events counter** so gaps are
  observable. Needs no wire change, so it can land after users.

### Rename — separate mechanical PR (dead last)
- `bridge`→`broker` residue after M3 already names the new modules:
  `bin/radical-orbit-broker.py`, `RADICAL_ORBIT_BRIDGE_*`→`RADICAL_ORBIT_BROKER_*`,
  URL/cert/key names + resolvers in `utils.py`, logger
  `radical.orbit.bridge`→`radical.orbit.broker`. **Checklist item: grep for
  rename collateral** — the `lendpointr` ("ledger") and `wendpoint` ("wedge")
  manglings in `task_dispatcher_state.py`/`plugin_task_dispatcher.py` prose are
  prior evidence that mechanical renames here need their own isolated, reviewed
  pass. Explorer JS: vocabulary in comments/labels only — endpoint paths unchanged.

### Cross-repo coordination (explicit step)
- **rhapsody** (sibling `feature/orbit`): backend onto the runtime + a **served
  callback plugin** (task-state → state callbacks) + the Dragon-init offload fix
  (as gated by M0). **Keep rhapsody testable during its rewrite** — its backend
  builds against the new runtime behind a flag rather than a two-repo flag-day
  swap — then the repos land together.

### Downstream
- **Examples:** rewrite to the participant API + simple-consumer sugar.
- **Tests:** rewrite `test_bridge.py`→broker, `test_client.py` +
  `test_embedded_service.py` + `test_direct_dispatch.py` → runtime,
  `test_models.py` → `protocol.py`, `test_resolve_bridge.py`,
  `test_service_cert.py`, `test_explorer_html.py`, `test_plugin_host_base.py`;
  add session reconnect/lifetime, liveness-suspect/lost-vs-reclaim-drain,
  transport-isolation (ping under blocked work loop), resume-key
  replace/reject, subscribe filtering, seq-gap detection, 503 fast-fail,
  loop-lag-watchdog, symmetric-forward-call, gateway, pool-ownership, and (with
  the replay plugin) event-replay tests.
- **Docs:** broker/endpoint/participant vocabulary; **transport-isolation model
  prominently** (with the offload hygiene guide for work-loop responsiveness);
  the event-delivery-vs-optional-replay model + durability guarantee; the
  backpressure contract (request 503 fast-fail; event drop-oldest + seq gaps);
  separate **simple-consumer** and **gateway (compat)** sections; "active broker".

## Verification

- `PYTHONPATH=$PWD/src python -m pytest tests/unittests/ -q` (green before every
  milestone merge); `python -m flake8 src/`. Build-alongside keeps `models.py` +
  the old suite green until the final flip PR. Plugin-level `TestClient` tests
  stay green iff dual route registration is kept (M4).
- M0 gates: real correlation table survives a **reconnect-mid-flight** with no
  leaked or double-resolved entries (incl. resume-key replace/reject); caller
  handle composes with strict pool ownership across the two-loop boundary;
  transport prototype answers pings through a synthetic 60 s work-loop block;
  Dragon off-thread viability + max-GIL-hold numbers recorded; **throughput
  gate passed on the emulated 100 ms link** (submission + notification storm ≥
  current-stack baseline; handoff microbenchmark numbers recorded).
- E2E smoke: broker up; an endpoint serving `sysinfo`+`rhapsody`; an example that
  (a) calls a plugin, (b) serves a callback plugin and receives a pushed task-state
  event (dispatcher → M7), (c) reconnects to the same `sid` and **re-syncs state**,
  and — with the replay plugin loaded — **replays a missed log line /
  `workflow_status` transition exactly once**, (d) crashes and is declared
  `suspect` within seconds and `lost` after the grace **without reclaiming its
  pilots until the longer drain timer** (a brief blip reaches `suspect` at most
  and must not reclaim).
- Transport isolation: an endpoint stays `present` while a plugin handler blocks
  its work loop for 60 s.
- Loop-lag watchdog: a simulated broker routing-loop stall does **not**
  mass-declare endpoints `lost` on resumption.
- Subscribe filtering: a zero-interest compute endpoint does **not** receive
  another endpoint's task-event storm.
- Backpressure: overflowing the endpoint request cap yields immediate 503s (no
  silent drops, no memory growth); a stalled SSE consumer produces a bounded
  queue + dropped counter + client-observable `seq` gap.
- Gateway smoke: `curl`/browser the Explorer; HTTP ingress + SSE via the gateway.
- Pool-ownership: two sessions declare same-named pools and get **distinct,
  isolated pools**; a restart re-attaches each session to its own pools.

## Remaining open choices (recommendations baked in; confirm or override)
- **Heartbeat profile:** proposed default ping 1 s / pong timeout 3 s (`suspect`
  ≈ 3–4 s) / grace 10 s (`lost` ≈ 13 s); LAN-aggressive profile toward `suspect`
  ≈ 1 s; WAN/login-node profile slower. **Floor pending the M0 GIL
  measurement.** Separate reclaim-drain: minutes, per-pool/policy configurable.
- **Frame cap:** derive from the inequality (≈ 4 MB at the default profile);
  today's 10 MB is the ceiling.
- **Replay buffer bound:** per-session (age+size) + global ring by total bytes +
  dropped-events counter.
- **Session policy spelling:** two-field `lifetime=` + `ttl=`; reject incoherent pairs.
- **Admin:** `control` ops via the gateway (primary) + the uvicorn-SIGTERM /
  terminate-route floor.
- ~~`default`-session pool reclamation~~ **decided: explicit `cancel_all` only**
  (see M1).
