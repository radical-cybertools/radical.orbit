# ORBIT: broker + endpoint architecture — rationale & discussion trail

This document records the *why* behind the implementation plan in
`broker_architecture_plan.md`: the problem framing, the conceptual model, the
alternatives weighed and rejected, and the correctness/concurrency reasoning. The
plan answers *what to build*; this answers *why it is shaped this way*.

It assumes familiarity with the current ORBIT codebase (Client → Bridge → Endpoint
over HTTPS/WebSockets, a plugin system, an SSE notification path).

Docs convention: present-tense rationale only — *why the thing is what it is*. No
references to prior names or "what changed". Code anchors are symbol names, not
line numbers (line anchors drift; symbols don't).

---

## 1. The problem

Two concrete feature requests started this:

1. A client interacting with the `task_dispatcher` plugin must be **restartable
   within the same session**, reconnecting to the same pools and resource
   endpoints.
2. More generally, across all plugins, it should be possible to **reconnect to a
   `default` session** created on demand that persists for the lifetime of the
   managing endpoint/broker.

The reshaping below is driven by a larger need (§2) that the current tiering cannot
express cleanly; the two requests are closed along the way (sessions in §3.6,
same-pools via strict per-session pool ownership in §3.6) — and their
transport-agnostic core ships *early*, on the current stack, before the reframe
(§7).

Three structural facts about the current system frame that need:

- **A node is normally both a server and a consumer at once.** The motivating use
  case: a workflow manager submitted into a job allocation **spins up an
  endpoint**, **delegates part of its work to ad-hoc endpoints on other
  resources**, and **simultaneously serves task-execution requests on its own**.
  Today "serving plugins" and "calling other endpoints" are separate tiers with
  separate transports, so a node that must do both has to be an endpoint *and*
  construct a client internally — which the task dispatcher already does (loopback
  `BridgeClient` in `_start_bridge_client`, sixteen `to_thread` wrappers — every
  one of them wrapping a BridgeClient-backed HTTP call — and cross-thread
  marshalling via `call_soon_threadsafe`/`run_coroutine_threadsafe`).
- **The client↔bridge channel is stateless HTTP + SSE, while bridge↔endpoint is a
  heartbeated WebSocket.** So the bridge cannot observe client liveness, notifications
  can only be broadcast, and a node behind the HTTP tier cannot itself be *called*.
- **"Bridge" no longer fits the central component.** It routes, correlates, tracks
  topology, and hosts active plugins. It is a hub with agency.

Real external users are imminent. Disruptive structural work is cheap now (no
backward compatibility) and expensive once users exist, which sets the timing.

---

## 2. The conceptual model

**One node type, one hub, a clean star.**

- An **endpoint** is any participant that dials the hub over an *outbound*
  WebSocket and may **serve** plugins, **consume** them, or **both**. A pure consumer
  serves zero plugins. "Client" is not a separate tier.
- The **broker** is the hub: it routes between endpoints, correlates
  request/response, tracks topology + liveness, and hosts plugins. In prose it is an
  **active broker**.

What endpoints share is **transport + identity + connection-lifecycle**; what
differs is **role** (serve vs. consume), modelled as a capability. That single
abstraction is what lets the workflow-manager case (serve and delegate at once) be
expressed directly instead of by embedding a client inside an endpoint.

The topology is a **clean star for control**: every endpoint talks only through the
broker. This removes any need for direct endpoint↔endpoint connectivity (impossible
against HPC firewalls anyway; §5) and gives every future use case one routing model.
Bulk data does **not** transit the broker (§5).

Capabilities compose: the broker hosts plugins and an endpoint serves the plugins
its use case needs. The dispatcher (xgfabric, not AmSC) and event replay
(restart-continuity, not fire-and-forget) are **genuinely optional** — loaded per
use case. The **gateway** is a broker *module* (§3.3), on by default: it is the
sole non-participant ingress (browser, curl, non-Python), so any real deployment
runs it. Composing features per use case — rather than baking every capability
into the core — is a design goal in its own right.

Two further benefits follow from making the client a real WS participant,
**secondary** to the serve+consume unification:

- **Observable liveness** — the broker sees an endpoint connect/disconnect within
  seconds (§3.7), so plugins can react (e.g. spin down idle pilots when a driving
  app goes away).
- **Push to a participant** — notifications route to a specific endpoint over its
  own socket rather than being broadcast to everyone.

---

## 3. Decision-by-decision rationale (with alternatives rejected)

### 3.1 Naming

- **Hub → "broker".** Considered *station* (too non-descriptive for a CS/HPC
  audience), *hub* (a touch passive), *exchange* (messaging-loaded), *service broker*
  (rejected — SOA discovery/registry connotation misleads). "Broker" is CS/HPC-legible
  and reinforces ORBIT's "Brokerage". The plugin-hosting nuance is "active broker" in
  prose, kept out of the identifier.
- **Node → "endpoint" (not "participant").** Agency-neutral, CS-adopted, in-domain
  precedent for an *active* node (Globus endpoints). Yields a complete partition
  (broker vs. endpoints) and matches today's URL namespace. "Endpoint" means any node.
- **Explicit addressing triple `(endpoint, plugin, session)`** — *where* / *what* /
  *which context*, directly addressable; exactly today's `/{endpoint}/{plugin}/…/{sid}`.

### 3.2 Protocol: a single symmetric, versioned envelope — msgpack-only

One envelope discriminated by `kind`, carrying explicit `src`/`dst`/`corr_id` so
request/response work in both directions. We design it from scratch rather than
extending the asymmetric message set: the broker and runtime are rewritten anyway,
there are no external protocol consumers, and threading a legacy model through new
symmetric machinery is more friction. The envelope is frozen at M2, after the real M0
spike (§7) validates its concurrency and liveness decisions, and lives in a **new
`protocol.py` module** so the old stack keeps compiling during build-alongside (§7).

**The wire encoding is msgpack, always.** The alternative — a JSON-text path
alongside msgpack — was rejected: it carries a second parser, a base64 fallback for
binary bodies, and (to dodge double-encoding JSON bodies) a hand-built fast-path
response string in `EndpointService._handle_request` that is fragile by its own
admission. With msgpack-only, `body` is native bytes embedded verbatim: one packer,
one parser, no base64, and the fast-path hack is *deleted* rather than ported. The
usual counterargument — human-readable frames for debugging — doesn't apply: the
only JSON-native consumer is the browser, and the browser talks to the gateway
(HTTP/JSON/SSE), never the participant protocol. Debugging is a log line.

Fields present from day one for capabilities loaded later:

- **`version`** + a **capability** list on `register`/`register_ack`, so a later
  change is a value bump, not a format break.
- **`channel`** (a stream id) reserved for the deferred blocking-served-call and
  in-band-isolation cases. It is *not* used for heartbeat isolation (§3.7 solves
  that structurally).
- Events carry **`session?`, `topic`, `ts`, `seq`.** `seq` is **broker-assigned
  and frozen with the envelope** — monotone per session for session-scoped
  events, monotone per the global session-less stream otherwise — because
  client-side dedup and gap detection are meaningless without a defined scope,
  and the replay plugin (§3.5) and the backpressure contract (§4) both key on it.
- **No `ping`/`pong`.** Liveness is transport-level (§3.7). The app-level
  heartbeat it replaces was already vestigial: the current `register` handler's
  pong branch is a no-op — real liveness was always the WS-library keepalive.

Requests/responses retain `method`/`path`/`headers`/`body` so the existing
direct-dispatch machinery (`_match_route`, `RequestShim`) is reused unchanged.

### 3.3 Transport unification + the gateway

The participant (WS) protocol is mandatory; the stateless HTTP + SSE + Explorer-UI
surface is a broker-hosted **`gateway` module** — deliberately *not* a `Plugin`.
A parallel HTTP architecture was rejected (it re-creates the dual-surface
asymmetry); packaging the gateway through the plugin interface was **also**
rejected: the gateway needs the shared pending table, the raw event tap, the
topology, CORS, and the HTTP credential gate — an interface far wider than
`Plugin`'s — and the broker core needs an HTTP server for the WS `/register`
handshake regardless, so the "plugin" would share the core's app anyway. Forcing
it through `Plugin` would buy modularity vocabulary, not modularity. Instead
`gateway.py` declares its dependencies explicitly in its constructor and is on by
default (disable with a flag). Auth splits honestly: **core owns the credential
store and gates `/register`; the gateway applies the same credential to the HTTP
ingress** (bearer header / `POST /auth` cookie).

The gateway translates HTTP calls into participant requests for non-participant
callers and fans events out as SSE. Its sessions are TTL/persistent only (no
liveness channel). Its request/504 deadline stays **long** and independent of the
heartbeat — a thousands-of-task submit genuinely takes time (today's
`REQUEST_TIMEOUT=600`), so it does not inherit the short heartbeat timeout and may
move to async/streaming.

Distinct from the gateway is the **dead-simple consumer**: a *real* participant that
serves zero plugins, exposed as SDK sugar so a Python script stays a two-liner while
still getting liveness and the option to add served plugins. The two are documented
separately so users reach for the participant SDK, not the compat tier.

### 3.4 Callback plugins, push, and the deferred reverse path

A symmetric protocol lets an endpoint — including a driving application — **serve** a
plugin that others call, and lets the broker/other endpoints push to it. In scope now:

- **Push (the driver):** rhapsody hosts a callback plugin that turns task-state
  *events* into rhapsody state callbacks. One-way; fully covered by the event plane.
- **File-push:** a remote actor calls a client-side `staging`/`globus` plugin to push
  results; the control message is small (bulk bytes go out-of-band, §5).

The one use needing *correlated* server→client request/response — prompting a GUI for
input — is **deferred** (§6). The envelope supports it; the reverse correlated path is
not built until a concrete consumer lands. This keeps blocking-RPC generality out of
v1 and sidesteps the hardest part of the concurrency model (§4).

One consequence of consuming events over the participant socket: **user callbacks
run on a dedicated dispatcher thread** in the runtime, never on the transport or
work loop. User code cannot be held to any discipline; a slow callback on the work
loop would stall the consumer's own request handling (and, without §3.7, its
liveness). The dispatcher thread is the structural fix, same in kind as transport
isolation. Its queue is bounded (§4).

### 3.5 Event delivery and optional replay

Event handling splits into two layers, so durability is a composable feature rather
than a universal tax:

- **Live delivery — core, always on, stateless (modulo the interest sets).** An
  endpoint's runtime sends **`subscribe`/`unsubscribe`** frames as callbacks are
  registered/unregistered — the same `(endpoint?, plugin?, topic?)` wildcard
  tuples the client callback registry already uses
  (`BridgeClient.register_callback`), so the wire model mirrors an API users
  already have. The broker keeps a per-endpoint interest set and forwards an
  event only to endpoints with a matching pattern; the edge still
  filter-dispatches to individual callbacks. *Alternative rejected:* deriving
  interest implicitly from session registration — the broker sees
  `register_session` only as an opaque proxied request, so "deriving" means
  sniffing plugin-specific URL shapes (fragile coupling), and consumer-side
  interest lives in the consumer process where the broker can't see it at all.
  An explicit subscribe op is smaller, deterministic, and is the natural seed of
  the deferred full selective-routing model. The core holds no event buffers;
  current topology is **re-sent on (re)connect**, so it is re-derivable. Core
  also exposes one **unfiltered raw-stream tap** to broker-hosted subscribers —
  the single hook on the event path — because a retaining subscriber must see
  events for endpoints that are *not* currently connected and so cannot sit
  behind any interest filter.
- **Buffering / replay — an optional broker plugin.** Use cases that need event
  continuity across a reconnect load it; fire-and-forget use cases do not. The plugin
  subscribes to the raw-stream tap and retains the stream — a global ring for
  **session-less** broadcast events and per-session buffers for session-scoped ones —
  with `ts`/`seq` and per-subscriber cursors, and on reconnect replays what a
  subscriber missed. Topology is *not* in the ring; it recovers via the core re-send.
  The replay→live splice is this plugin's internal contract (§4), kept out of the
  frozen envelope.

**Guarantee.** Topology and state-mirroring data are recovered on reconnect regardless
of replay (the client re-syncs `get_task`/`fleet`; the core re-sends topology).
**Ephemeral events** — one-shot log lines, transient `workflow_status` transitions —
are retained and replayed **only when the replay plugin is loaded**; without it, an
ephemeral event emitted while a subscriber is disconnected is not retained. This is
intended and documented: durable delivery is a feature a use case opts into.

### 3.6 Sessions: identity, lifetime, reconnect, resource ownership

- **Client-supplied `sid` with create-or-reconnect.** Reattach if it exists, else
  create. Direct answer to feature request 1.
- **Lifetime policy** per session: `ephemeral`, `ttl=<n>`, `persistent`. Reserved
  **`default`** is persistent, on-demand (feature request 2). `ephemeral` is
  liveness-driven; `ttl` is time-driven; `persistent` never expires.
- **Two-layer identity, with a trustworthy owner binding.** Endpoint identity is
  the reconnect anchor; the session stays per-plugin. The **broker stamps `src`**
  on every forwarded request — a client-supplied `src` is overwritten, never
  trusted — and `register_session` records that identity as the session's
  **owner**. Liveness-driven expiry keys on the owner. Without the stamp,
  ownership would be spoofable and `ephemeral` expiry meaningless.
- **Reattach is owner-checked; session recovery goes through the name.** A
  create-or-reconnect on an existing `sid` from a *different* owner identity is
  rejected (403). *Alternative rejected:* letting a reattach re-bind the owner
  (so an auto-named restart could recover a session by `sid` alone) — that
  makes every `sid` a bearer capability within the tenant and recovery
  semantics ambiguous. Instead, clients that need session recovery use a
  **stable, client-supplied endpoint name** and simply re-register it after a
  crash; the runtime retries a name-in-use registration with backoff until the
  stale registration passes `lost` (a crashed client holds no resume key, and
  under the fast keepalive the name frees in seconds — §3.7). Consequences,
  stated plainly: auto-named `consumer.<uuid>` endpoints are fire-and-forget by
  design; with the shared token "same owner" is effectively same-*name* (it
  becomes cryptographically strong under mTLS with no logic change, §3.8);
  gateway-originated sessions carry no participant identity and remain
  capability-style within the token trust domain.
- **Reconnect re-syncs state; replay adds event continuity when loaded** (§3.5).
- **Strict per-session pool isolation.** The dispatcher's pools are keyed
  `(owning_sid, pool_name)`; **pool names are session-local namespaces** — two
  sessions declaring the same name get two distinct, isolated pools. The durable
  per-resource store stays plugin-level (with an `owning_sid` field and a
  sid-scoped state-dir layout) so restart recovery is preserved. *Alternative
  rejected:* keeping cross-session pool sharing (today a second session declaring
  a compatible same-name pool attaches to the existing pilot fleet). Isolation
  was chosen deliberately *(user decision)*: it makes "restart into the same
  pools" exact, it isolates concurrent clients and future tenants, and it
  *simplifies* the dispatcher — the config-compatibility check
  (`_pool_configs_compatible`), the attach-or-409 path in `_materialise_pool`,
  and the session-less fleet-wide read routes all disappear rather than being
  ported. Shared fleets, if ever needed, come back later as an explicit
  ownership+attachment feature, not as name collision.
  Pilot lifetime under ownership: a session-owned pool's **pilots are reclaimed
  by a *separate*, longer, per-pool/policy-configurable drain timer started at
  `lost`** — *not* the short liveness grace (§3.7): `ephemeral` → drain after the
  reclaim timer, `ttl` → until expiry, `persistent` → to walltime. On broker
  restart, ownership + pilots replay from the durable logs and owner reconnection
  is reconciled under the grace, draining pools whose owner never returns. Note
  this restart-time replay is **new capability**: today pool state reloads only
  lazily, when a client happens to re-declare the pool — the paper model (§7)
  defines the eager replay.

### 3.7 Liveness: transport isolation and fast failure detection

A **clean** disconnect sends a close frame and is detected immediately. A **crash**
(partition, OOM-kill, panic) sends nothing and is detectable only by a heartbeat
that stops arriving — so a heartbeat timeout is irreducible for the crash case.
The design goal is failure detection in **seconds** (connection problems and node
failures alike), which raises the bar on what may ever delay a heartbeat.

Three ways to keep heartbeats honest were weighed:

1. **Offload discipline alone** — every blocking call between `await` points goes
   through `asyncio.to_thread` (the pattern rhapsody's `_prepare_batch` already
   uses; the un-offloaded Dragon `rh.Session(...)` construction + node probe in
   `RhapsodySession.initialize` is the canonical violation and the reason the
   current WS ping tolerance is 600 s). *Rejected as the primary mechanism:* it is
   an unenforceable convention — one missed offload anywhere in any plugin
   silently re-couples liveness to plugin behaviour, and the failure mode is a
   spurious `lost` on a healthy node. It survives as **work-loop hygiene** (a
   responsive work loop is still better), no longer as the correctness
   foundation.
2. **A dedicated control socket** — heartbeat on its own WS. *Rejected for v1:*
   it isolates the heartbeat at OS level but buys that only on the send path,
   against the substantial cost of a two-socket lifecycle (bind order,
   lose-control-vs-lose-data state machine, two grace-protected reconnect
   paths). It also does nothing about the receive/processing side.
3. **Transport isolation (chosen):** put the transport — the WS connection,
   keepalive, reconnect/backoff, raw frame I/O — on **its own thread with its own
   event loop**, and all plugin/handler work on the **work loop**. On the broker,
   the same isolation inverted: the server/routing loop does transport + routing
   only (tiny dict operations), and the hosted-plugin host runs on its own
   loop/thread.

**Why transport isolation wins:**

- *Structural, not conventional.* Liveness reflects process/host/network health;
  no plugin author can break it by forgetting an offload. The property is
  enforced by architecture, not review.
- *It is what makes fast detection possible at all.* With the transport loop doing
  nothing but I/O, pong latency is bounded by scheduler + GIL handoff (§4), not by
  plugin behaviour — so second-scale (and later sub-second-scale) timeouts stop
  being reckless.
- *No protocol or socket change.* Single socket, no envelope surface, no
  two-socket state machine.
- *The handoff already exists in spirit.* The codebase routinely crosses threads
  (`run_coroutine_threadsafe` in `Plugin._dispatch_notify` and throughout the
  dispatcher); this formalizes one crossing instead of scattering many.
- *Richer health signal for free.* Transport liveness ("process reachable") and
  work-loop responsiveness ("requests progressing") become separable; a wedged
  work loop shows up as request timeouts/503s on a `present` endpoint — a
  diagnosable state — rather than as a fake death.

**Costs, honestly:**

- *Cross-thread handoff on every message* — a bounded queue + `call_soon_threadsafe`
  wakeup (~tens of µs). At task-storm rates the wakeups batch; M0 measures.
- *Two-loop lifecycle* — startup/shutdown ordering, and stack traces that span
  threads. Contained in the runtime; plugin authors never see it.
- *The GIL is the residual coupling.* A C extension that holds the GIL without
  release (a large `msgpack.unpackb`, cloudpickle loads, possibly Dragon-native
  init) delays the transport thread by up to the longest hold. CPython's switch
  interval (~5 ms) bounds pure-Python work, but C holds can reach 10⁻¹–10⁰ s.
  **This sets the floor on the heartbeat timeout** — hence the M0 gate: measure
  the max GIL hold during Dragon init and large-frame decode; the pong-timeout
  floor sits above it. (A process wedged for seconds in native code arguably *is*
  unhealthy — but the policy decision needs the numbers.)
- *Liveness over-reports health* (transport pongs while the work loop is stuck) —
  mitigated by the separability above; if wanted later, the transport thread can
  report work-loop lag inside a topology field without protocol change.

**The liveness state machine** is two-stage so aggressive timeouts don't translate
into destructive actions: missed keepalive ⇒ socket close ⇒ **`suspect`**
(immediate topology signal — this is the "much faster than 30 s" detection);
grace ⇒ **`lost`** (actionable: session cascade per policy, reclaim-drain timers
start). A valid re-register cancels both. Two separations keep fast detection
safe:

- **Liveness grace ≠ resource reclaim.** `suspect`/`lost` are signals; reclaiming
  a session-owned pool's pilots is a *separate*, longer, per-pool/policy timer
  (§3.6), so even an aggressive profile never destroys a 1000-node allocation on
  a blip.
- **Profiles per link class.** On-cluster links can run `suspect` ≈ 1 s;
  WAN/oversubscribed login nodes see scheduling jitter of seconds and get slower
  profiles. Inbound traffic counts as proof-of-life (ping only an idle link), so
  a busy link is never suspected merely for being busy.

The **loop-lag watchdog** on the broker survives as a ~20-line safety net: the
routing loop should never stall, but if it somehow does, suppressing the `lost`
declarations that would fire on resumption converts a bug into a logged hiccup
instead of a fleet-wide false reclaim (compounded by drain timers).

### 3.8 Identity / auth

Identity is a **(name, credential) tuple**; the credential is the shared bridge token
from the pre-broker security step (`security_token_mitigation.md`), single-tenant for
now (trusted insiders). Liveness and the reconnect anchor are keyed on the *tuple* —
but with a *shared* credential the tuple alone cannot make "same identity
reconnecting replaces the stale socket" safe: every participant holds the same
token, so bare replace would be a connection-takeover primitive *among token
holders*, not just for outsiders. Hence the **resume key**: the first registration
of an identity gets a per-identity secret in `register_ack` (broker memory only);
a reconnect presenting it replaces the live socket; a same-name register without
it is rejected while the old registration is within grace. ~20 lines, and it makes
the later **mutual TLS** (per-participant) upgrade a *value* change at one place —
the credential in the tuple — not a *logic* change at the delicate reconnect code.
Broker restart clears resume keys → first-come re-registration (unchanged
semantics). Tenant authentication/authorization is out of scope (the pool-ownership
data model lands now; tenant separation is later, alongside full selective event
routing).

### 3.9 Dispatcher hosting

The task dispatcher stays a broker-hosted plugin: zero-deploy, lifecycle-for-free,
co-located with the only persistent node, and a saved hop. The cost is a shared fault
domain, with two distinct protections:

- **Supervised task creation.** Handler *raises* are already isolated on both
  hosts (`BridgePluginHost.handle_request`, `EndpointService._handle_request`
  convert exceptions to error responses); the actual gap is **background
  coroutines** — `create_task`'d work (notification sends, init tasks, watchers,
  cleanup loops) whose exceptions die silently. The plugin host provides a
  `spawn()` helper that wraps `create_task` with logging/reporting, and hosted
  plugins use it.
- **Transport isolation** (§3.7) is what keeps a plugin from harming *liveness*;
  a plugin that blocks its host loop degrades request latency on that loop —
  visible, bounded, and confined to the plugin-host loop on the broker.

The residual enforceability gap is why **untrusted third-party plugins (or per-tenant
isolation) are the trigger to externalize** plugin hosts into managed processes — at
which point standard process management on the broker host (an operator-deployed
unit/container) is the route, not a bespoke broker supervisor that turns "spawn on
request" into a remote-code-execution surface.

---

## 4. Concurrency & correctness model

Each endpoint multiplexes both directions, and the broker hosts plugins that originate
calls and holds real state, so the concurrency and failure model is stated rather than
left implicit. The **transport isolation (§3.7) is the central rule** — liveness is
decoupled from work by construction; everything else assumes only that the
*transport* loops never run application code.

- **Two loops per node, one socket.** Endpoint: transport thread (WS + keepalive +
  reconnect) ↔ work loop (plugins, sessions, pending futures), joined by bounded
  thread-safe queues with `call_soon_threadsafe` wakeups (batched under load).
  Broker: routing loop (transport + correlation + topology + interest sets — all
  tiny, non-blocking operations) ↔ plugin-host loop (dispatcher, gateway-driven
  work). Envelope pack/unpack happens on the work side; large decodes may
  additionally use `to_thread` (the existing rhapsody `_prepare_batch` pattern).
- **Handoff cost, quantified (the rhapsody-over-WAN case).** Crossings are
  per *envelope message*, not per task: producer work-loop→transport (1),
  broker routing loop (0 for pure forwarding), consumer transport→work-loop→
  callback thread (2) — so ≤3 per message, each a bounded-queue put +
  `call_soon_threadsafe` wakeup (~20–50 µs worst-case on an idle loop, ~1–5 µs
  amortized under load when one wakeup drains many items). Consequences on a
  100 ms-RTT remote link:
  *Submission* is frame-batched (`WS_PAYLOAD_LIMIT`-sized frames carrying
  10³–10⁵ tasks with template compression at ~20 B/task), so handoff is
  **<0.1 µs/task** — three-plus orders below serialization cost. Throughput is
  bandwidth/CPU-bound (~12 k tasks/s at 100 Mbit/s with 1 KB tasks; ~10⁵/s
  template-compressed on ≥1 Gbit/s), and RTT adds only ~one constant round trip
  of *latency* since frames pipeline (the per-`src` pending cap must admit a
  few in-flight frames — the bandwidth-delay product is ~1–2 frames).
  *State notifications* are one-way pushes, so RTT again costs latency, not
  throughput. Batched terminal events (`task_status_batch`, 1024/frame) share a
  frame's crossings → >10⁵ events/s transport ceiling. **Unbatched**
  per-event messages are the only regime where handoff is visible: ~30–60 µs of
  single-threaded Python per event across the chain (pack/route/unpack ≈
  25–50 µs, amortized handoff ≈ 2–5 µs — roughly a 10 % share), giving a
  sustained ceiling around **~30 k events/s** (20–50 k band). All of this sits
  an order of magnitude above the *measured current stack* (~6.5 k tasks/s
  localhost, ~1.3 k tasks/s LAN, `rhapsody_throughput.out`) — whose per-event
  path already pays 2–3 thread crossings *plus* double JSON
  (endpoint `dumps` → bridge `loads` → SSE `dumps` → client `loads`), both of
  which the new path removes. Net expectation: equal or better; the M0
  throughput gate verifies exactly this on an emulated 100 ms link, with
  regression against the current baseline as a kill criterion.
- **The fast-heartbeat regime and the wire.** With processing isolated, the only
  thing left that can delay a heartbeat is **wire occupancy**: one maximal frame
  ahead of a ping/pong occupies the socket for `frame_size / bandwidth`. This
  gives a design inequality instead of a mechanism:
  `frame_cap ≤ heartbeat_timeout × min_bandwidth / 2`.
  At the default profile (timeout 3 s) and a conservative 3 MB/s, the cap is
  ~4 MB; at a future 1 s timeout it tightens to ~1.5 MB — still comfortably above
  typical request/response sizes, and oversized payloads are absorbed by
  **size-aware batching the client already does** (rhapsody `WS_PAYLOAD_LIMIT`),
  not by new machinery. Two further levers before any mechanism is added:
  inbound traffic counts as proof-of-life (a link mid-transfer is alive by
  definition; ping only an idle link), and — if a use case ever truly needs huge
  single messages under an aggressive profile — WS-level fragmentation with
  interleaved control frames (RFC 6455 permits control frames between fragments;
  verify library behaviour). **Envelope-level chunk/reassembly is rejected in
  all scenarios**: no msgpack streaming exists in the tree, and it would be new
  protocol machinery for a problem the inequality solves by configuration.
- **Backpressure — the application-visible contract.** Every queue is bounded;
  the two planes fail differently *by design*:
  *Request/response is never silently dropped.* An endpoint at its concurrency
  cap answers overflow **immediately with 503 + `Retry-After`** — the caller gets
  a fast, explicit "busy" (SDK retries with backoff) instead of the latency
  collapse of an unbounded task pile-up; the long 504 deadline remains the
  backstop for work genuinely in progress. The broker's pending table is capped
  per `src`; overflow raises synchronously at the caller ("too many in-flight").
  *Events are observably lossy.* A slow subscriber's bounded queue drops oldest;
  because `seq` is broker-assigned per scope (§3.2), the consumer sees the loss
  as a `seq` gap plus a dropped counter — deterministically, not as silent
  staleness — and recovers by re-sync (state-mirroring data) or via the replay
  plugin (whose buffers are themselves bounded, age+size, with their own
  counter; eviction there surfaces as a gap marker too). A slow subscriber can
  never backpressure the routing loop or other subscribers. User-callback
  dispatch (§3.4) uses the same bounded/drop-oldest+counter policy.
- **Correlation ownership.** One broker-side pending table shared by broker-hosted
  plugins (the gateway's HTTP futures and the dispatcher's async calls) — accessed
  across the routing/plugin-host loop boundary through the caller handle, which is
  thread-aware from M0. `corr_id`s are namespaced by `src` so producers cannot
  collide; each entry has a single owner for timeout and cleanup; the
  reconnect/teardown path cancels pending entries deterministically. The
  **timeout/cleanup race on reconnect-mid-flight** — a 504 firing while the broker
  still resolves an entry; cancellation ordering when a resume-keyed socket
  replaces a stale one — is the part that most needs care, so the M0 spike (§7)
  exercises a *real* correlation table through a reconnect-mid-flight before the
  envelope is frozen.
- **Replay→live splice (replay plugin).** A reconnecting subscriber is replayed up to
  its cursor while live events keep arriving. Delivery is **at-least-once on the wire
  with `seq` dedup at the client = effectively-once**: the cursor advances on **ack**,
  so a drop mid-replay resends rather than loses, and the client discards duplicates by
  `seq`. Same rigor as the correlation race, scoped to the plugin.
- **Loop-lag watchdog.** See §3.7 — retained as a safety net on the routing loop.
- **Broker-crash semantics:** **pool ownership survives** (durable plugin-level
  store); **pending entries are lost** → in-flight calls fail and callers retry;
  **resume keys are lost** → first-come re-registration; **replay buffers/cursors
  are lost** → replay degrades to re-sync on a restart (topology + state-mirroring
  data still recover; ephemeral events around the restart are not retained).

---

## 5. The data plane: strictly star, bulk out-of-band

A direct channel between two firewalled endpoints is not possible in general against
HPC firewalls — the whole premise is outbound-only dialing because inbound is blocked.
Hole-punching breaks on symmetric NAT and HPC firewalls block inbound regardless; a
relay is just "the broker again"; the only real direct paths are out-of-band reachable
ones (SSH tunnels — already implemented — a shared filesystem, or a publicly reachable
peer). So control flows strictly star through the broker, and **bulk data uses
out-of-band mechanisms**. The precedent exists: the `globus` plugin moves data
collection-to-collection so bytes never transit the broker.

---

## 6. Out of scope / deferred (and why)

- **Correlated server-initiated RPC** (a server-side blocking request expecting a
  reply, e.g. prompt-a-GUI). Designed-for in the envelope; not built until a concrete
  in-scope consumer exists (§3.4).
- **Dedicated control socket.** Transport isolation (§3.7) covers its v1
  motivation structurally; the frame-cap inequality (§4) covers the wire. Revisit
  only if measurement under an aggressive heartbeat profile shows wire-level
  starvation the cap cannot fix.
- **Selective live routing beyond the `subscribe` patterns / per-tenant event
  isolation.** v1 has exact/wildcard `(plugin, topic)` interest sets (§3.5); a
  full per-tenant model arrives with multi-tenancy.
- **mTLS / per-participant auth and tenant authz.** Shared token + per-identity
  resume key now (§3.8); mTLS is the per-participant upgrade. Tenant authz is later.
- **Direct endpoint↔endpoint data channels.** Impossible against HPC firewalls.
- **Externalizing plugin hosts into managed processes.** The in-process host is
  hardened with transport isolation + supervised task creation (§3.7/§3.9);
  externalization waits for untrusted third-party plugins or per-tenant isolation.
- **Migrating the Explorer UI to a full participant.** It stays on the gateway.

Note: event **replay** is *not* out of scope — it is an optional plugin (in scope,
loaded per use case; §3.5).

---

## 7. Sequencing

The change is large; the sequencing surfaces the hardest unknowns early, ships user
value before the reframe, and keeps every merge reviewable.

- **Decide the pool-ownership data model on paper before M0** — now under **strict
  per-session isolation** (user decision), which shrinks it: no sharing semantics
  to define, but it still must answer pilot-lifetime × crash-recovery × ownership
  (pilots follow the session lifetime policy, reclaimed via the separate drain
  timer; broker-restart recovery via **eager** durable-log replay — new
  capability, today's reload is lazy — plus reconciliation grace; §3.6). Settling
  it first means M0 and the M7 port build against the *target* ownership — one
  rewrite, honest M0 kill criterion.
- **M0 carries four gates, not one:** the correlation/reconnect race against a
  real table (with resume keys); the transport-isolation prototype (pings
  answered through a synthetic 60 s work-loop block); the **Dragon gates** —
  off-main-thread `rh.Session` viability (unknown; Dragon has thread/process
  affinity expectations) and the **max-GIL-hold measurement** that sets the
  heartbeat-timeout floor (§3.7); and the **throughput gate** — the
  rhapsody-shaped submission + notification-storm benchmark on an emulated
  100 ms link, ≥ the current-stack baseline (§4). The whole campaign runs in a
  **2-node HPC allocation** (broker spike on the login node, Dragon on
  compute; userspace delay proxy for the link — no root, so no `tc netem`),
  with one recorded caveat: 2-node GIL-hold numbers are a lower bound, so the
  heartbeat floor carries a margin and is re-validated at target scale before
  an aggressive profile ships. If Dragon can neither run off-thread nor bound
  its GIL holds, the fallback is a per-role heartbeat profile for
  Dragon-hosting endpoints — decided in M0, not discovered in production.
- **Sessions ship early (M1), on the current stack.** The create-or-reconnect
  `sid`, `ttl`/`persistent` lifetimes, and the persistent `default` session live
  entirely in `plugin_base.py`/`plugin_session_base.py`, which survive the
  rewrite untouched — so feature request 2 and most of feature request 1 land
  months before the cutover, and the trickiest non-transport session logic
  (lifetime conflicts, the `default` lock) de-risks on the stable stack. Only
  the liveness-driven `ephemeral` driver and the owner binding wait for the new
  runtime (M6).
- **Every milestone merges to `devel` green; the flip is its own small PR.**
  `protocol.py` lands beside `models.py`; `broker.py` beside `bridge.py`; the
  runtime beside `service.py`/`client.py` — the old stack stays the `bin/`
  default throughout, so the suite guards both. The final PR flips the `bin/`
  defaults and deletes the superseded modules. *Alternative rejected:* a single
  cutover merge unit (protocol + broker + runtime at once) — it conflates
  *activation* (which must be atomic and is: the flip PR) with *merging* (which
  needn't be), and buys a long-lived divergent branch and a big-bang review for
  no compatibility benefit that build-alongside doesn't already provide.
- **The rename is a separate mechanical PR, dead last.** `bridge`→`broker`
  touches env vars, loggers, cert/key resolvers, the sibling repo, and docs —
  zero functional value, and M3 creating `broker.py` already does the identifier
  half. The `lendpointr` ("ledger") and `wendpoint` ("wedge") manglings left in
  dispatcher prose by a prior mechanical rename are direct evidence that these
  passes need to be isolated, reviewed, and **followed by a collateral grep**.
- **Cross-repo coordination with a green window on both sides.** rhapsody (sibling
  repo) gets the backend on the runtime + a served callback plugin + the
  Dragon-init offload fix (as gated by M0). Its rewrite **builds against the new
  runtime behind a flag** so it stays testable during the work, rather than a
  two-repo flag-day swap; then the repos land together.

---

## 8. Remaining open choices (small, for the reviewer to weigh)

1. **Heartbeat profile.** Proposed default: ping 1 s, pong timeout 3 s (`suspect`
   ≈ 3–4 s), grace 10 s (`lost` ≈ 13 s); LAN-aggressive profile toward `suspect`
   ≈ 1 s; slower WAN/login-node profile. The timeout **floor is set by the M0
   GIL measurement**, not by taste. **Separately**, the resource-reclaim drain is
   a longer, per-pool/policy timer (minutes), *not* the liveness grace.
2. **Frame cap.** From the inequality (§4): ~4 MB at the default profile
   (today's 10 MB is the ceiling); tightens with faster profiles.
3. **Event-replay buffer bound.** Per-session buffers bounded by age and size (so one
   chatty session can't evict another's history) + the global session-less ring
   bounded by total bytes, with a **dropped-events counter** so silent gaps are
   observable.
4. **Session policy spelling.** Two fields `lifetime=` + `ttl=`, rejecting incoherent
   pairs (e.g. `persistent` with a `ttl`).
5. **Admin surface.** `control` ops via the gateway as the primary path, plus the
   process floor (uvicorn's default SIGTERM handling + the self-SIGTERM terminate
   route) so the broker is stoppable without the gateway.
6. **`default`-session pool reclamation — decided: explicit `cancel_all` only.**
   `default` is persistent, so its pools have no liveness-driven reclaim; they
   live until the operator acts. Predictability over cleverness — a per-pool
   idle policy can arrive later as an opt-in.
