# Review: participant runtime + gateway ingress + consumer edge

Scope: `runtime.py`, `runtime_support.py`, `runtime_client.py`, `gateway.py`,
`client.py`, `dispatch.py`, `bin/radical-orbit-broker.py`,
`bin/radical-orbit-endpoint.py` (all under
`/global/u2/m/merzky/radical/radical.orbit/src/radical/orbit/` and `bin/`).

## Overall impression

The two `bin/` entry points and `dispatch.py` are exemplary — small, honest,
followable on first read. The runtime and its client edge are the opposite
problem: the *architecture* (one outbound WS, transport isolated from plugin
work) is defensible, but it is realized through a stack of clever mechanisms —
a dynamically synthesized mixin class, a manually-stepped coroutine driver, two
hand-rolled cross-thread queues plus a hand-rolled callback executor — several
of which are provably replaceable by one-liners or by arguments that already
exist. The prose is a second problem of equal weight: docstrings throughout
narrate a private migration history ("M0 lesson 5", "pre-flip item 1",
"service.py shapes") and repeatedly reference a module (`service.py` /
`EndpointService`) that no longer exists in the tree, so a student who greps
for the cited justification finds nothing. The gateway is mostly straight
compat plumbing and reads better, though one 190-line closure-registering
method and a lazily-captured event loop undercut it.

---

## Findings (most severe first)

### 1. `get_plugin` synthesizes a class at runtime; the whole `RuntimePluginClient` mixin is redundant

- **Location:** `runtime.py:744-754` (`EndpointRuntime.get_plugin`);
  `runtime_client.py:117-144` (`RuntimePluginClient`).
- **What & why it hurts:** For every `get_plugin()` call the runtime creates a
  brand-new class with `type('Runtime%s' % ..., (client_cls,
  RuntimePluginClient), {})` and then also pokes `client._runtime = self` onto
  the instance. Dynamic class synthesis + MRO mixing is among the most
  student-hostile constructs in Python: the resulting class exists nowhere in
  the source, cannot be found by grep, and its method resolution must be
  simulated in the reader's head. And it is unnecessary: the only thing
  `RuntimePluginClient` overrides is notification (un)registration, which it
  forwards to `runtime.register_callback(endpoint_id=…, plugin_name=…,
  topic=…, callback=…)` — **exactly** the call the base
  `PluginClient.register_notification_callback` (`client.py:106-135`) already
  makes on its injected `self._bc`. The runtime's `register_callback` /
  `unregister_callback` signatures (`runtime.py:758-798`) are drop-in
  compatible with what `_bc` must provide. (`plugin_xgfabric.py:71-84` already
  wraps the runtime in exactly this kind of `_bc`-shaped adapter, confirming
  the pattern works.)
- **Simpler alternative:** Delete `RuntimePluginClient` and the `type()` call.
  In `get_plugin`:
  `client = client_cls(_RuntimeHTTP(self, endpoint_name), namespace,
  broker_client=self, endpoint_id=endpoint_name, plugin_name=plugin_name)`.
  Base-class behavior is identical; `client._runtime = self` disappears.
- **Essential?** No. The transport seam requirement ("any participant invokes
  any plugin") is fully met by `_RuntimeHTTP`; the mixin adds nothing the
  existing `broker_client` parameter doesn't provide.
- **Severity:** high. **Effort:** small.

### 2. `_run_sync`: sync facade drives async cores via the raw generator protocol

- **Location:** `client.py:44-64` (`_run_sync`), `client.py:163-175`
  (`_arequest`), `client.py:182-218` (`register_session` /
  `aregister_session` pattern, replicated across the 11 plugin helpers).
- **What & why it hurts:** Every public sync method is a wrapper that calls
  `coro.send(None)` on its `async def` core and *relies on the invariant that
  the coroutine never suspends* (because `_arequest` falls through to the sync
  `_request` when `_async_http is None`). This is three layers of indirection
  (`register_session` → `_run_sync(aregister_session(...))` → `_arequest` →
  `_request` → `getattr(self._http, verb)`) to express "POST this payload". A
  student must understand coroutine internals, the `StopIteration.value`
  convention, and a cross-module invariant just to trace one HTTP call — and
  if the invariant breaks, the failure is a runtime error with a message
  ("suspended on the sync transport") that presumes exactly this background.
- **Simpler alternative:** Factor the shared part — payload construction — into
  a plain function, and keep two thin transport wrappers:
  `def register_session(...): resp = self._request('POST', url,
  json=self._session_payload(...)); ...` and
  `async def aregister_session(...): resp = await
  self._async_http.arequest(...)`. That duplicates ~3 lines per method in
  exchange for removing the generator-protocol driver entirely. (Or: make the
  sync path call `asyncio.run` in helpers that need it — but the plain-function
  split is simpler still.)
- **Essential?** Partially. Sharing one wire implementation between user-thread
  sync callers and the broker host loop is a real requirement (uniform message
  contract, consumed from both contexts). But the *mechanism* — manually
  stepping a coroutine and treating suspension as a bug — is the cleverest
  possible implementation of it; the duplicate-three-lines version satisfies
  the same requirement legibly.
- **Severity:** high. **Effort:** medium (touches the helper pattern).

### 3. `Handoff` is a hand-rolled, falsely-"bounded" perf optimization; the outbound path is triple-buffered

- **Location:** `runtime_support.py:31-76` (`Handoff`); `runtime.py:24-28`
  (docstring: "a bounded deque with a single outstanding
  call_soon_threadsafe"); `runtime.py:194-197`, `557-568` (`_sender`),
  `572-576` (`_drain_outbound`).
- **What & why it hurts:** (a) The custom coalesced queue exists to hit a
  quoted "~2.2 M msgs/s" — a throughput that has no bearing on a control plane
  carrying RPCs and events over one WebSocket. The stdlib equivalent is one
  line: `loop.call_soon_threadsafe(self._on_frame, data)`. (b) The "bounded"
  claim is false: `push()` *counts* overflow past `soft_max` but appends
  anyway (`runtime_support.py:62-65`), so the buffer is unbounded, and the
  `overflow` counter is read by nothing in the codebase. (c) The outbound
  direction stacks three mechanisms: `_outbound` Handoff → `_outbox` deque +
  `_outbox_evt` event → `_sender` task. A frame a student wants to trace passes
  through a lock, a coalesced wakeup, a second deque, an event, and a drain
  loop before reaching `ws.send`.
- **Simpler alternative:** Inbound:
  `self._work_loop.call_soon_threadsafe(self._on_frame, data)` — delete
  `Handoff` and `_drain_inbound`. Outbound: one `asyncio.Queue` on the
  transport loop, fed by
  `self._transport_loop.call_soon_threadsafe(q.put_nowait, data)`; `_sender`
  becomes `while True: await ws.send(await q.get())`. Same thread-safety, same
  non-blocking-producer property, ~50 fewer lines and zero custom locking.
- **Essential?** The *handoff itself* is essential (the transport thread must
  never block — failure-detection decoupling requirement). The coalescing,
  arming flag, bind-order handling, and soft bound are accidental: stdlib
  `call_soon_threadsafe` already provides a thread-safe, non-blocking handoff.
- **Severity:** high. **Effort:** small/medium.

### 4. Docstrings cite a deleted module and a private milestone ledger

- **Location:** `runtime.py:16, 26-28, 97, 133, 154, 489, 919, 932`
  (`service.py` / `EndpointService` / "M0 lesson 3/5");
  `dispatch.py:3, 16, 41`; `gateway.py:26` ("M5"), `gateway.py:54` ("M6
  concern"), `gateway.py:530` ("pre-flip item 1");
  `runtime_support.py:6-12` ("M0-validated", "M0 lesson 5").
- **What & why it hurts:** `service.py` does not exist in the tree (verified:
  no such file under `src/radical/orbit/`). Comments like "mirrors
  `EndpointService._classify_cert_error`", "tunnels (service.py shapes)",
  "pure extraction — behaviour is exactly what service.py carried" send a
  student grepping for code that is gone. "M0/M5/M6/pre-flip" are references
  to planning documents the reader does not have; they *sound* like
  justification while communicating nothing. This is pervasive — the modules'
  self-explanation is written for the author's past, not the student's present.
- **Simpler alternative:** Rewrite the affected docstrings to state the
  *current* behavior and the *requirement* it serves ("the transport thread
  must never block, so this handoff never blocks the producer") and delete
  every `service.py`/`EndpointService`/M-number reference. Pure prose change.
- **Essential?** No. Documentation debt, zero behavior.
- **Severity:** high (it actively misleads readers, everywhere). **Effort:**
  small.

### 5. 16 constructor tunables on `EndpointRuntime`, most of them speculative

- **Location:** `runtime.py:105-126` (`__init__` signature).
- **What & why it hurts:** `request_concurrency`, `accept_queue`,
  `callback_queue`, `call_timeout`, `retry_after`, `ping_interval`,
  `ping_timeout`, `frame_cap`, `backoff_start`, `backoff_max`,
  `backoff_factor` — eleven numeric knobs beside the five identity/connection
  args. Neither `bin/radical-orbit-endpoint.py` nor any example sets any of
  them; they exist for tests. Every knob is a question a student must answer
  ("when would I change `backoff_factor`?") and a doc line to maintain.
- **Simpler alternative:** Module-level constants (`_BACKOFF_START = 0.5`, …)
  with the constructor keeping only what a user plausibly sets:
  `broker_url, cert, name, plugins, role, tunnel, tunnel_via, token,
  resume_key`. Tests that need to shrink timeouts can patch the constants or
  set private attributes.
- **Essential?** No requirement needs per-instance backoff shaping or a
  configurable accept queue. Keepalive cadence arguably deserves one knob;
  the rest is speculative generality.
- **Severity:** medium. **Effort:** small.

### 6. Two-tier request backpressure: counter + semaphore + two knobs for one 503

- **Location:** `runtime.py:211-212`, `369` (`_req_sem`), `613-626`
  (`_admit_request`), `628-641` (`_serve_request`).
- **What & why it hurts:** Admission is governed by a manual `_req_inflight`
  counter checked against `request_concurrency + accept_queue`, and execution
  is separately throttled by an `asyncio.Semaphore(request_concurrency)`. Two
  mechanisms, two knobs, and a subtle intermediate state ("admitted but
  waiting on the semaphore") that the reader must reconstruct to understand
  when a 503 actually fires.
- **Simpler alternative:** One counter, one cap:
  `if self._req_inflight >= CAP: 503; else create_task(...)`. If bounding
  *concurrent execution* separately from *admission* genuinely matters someday,
  add it then; today nothing distinguishes the tiers observably except latency
  shape.
- **Essential?** The 503 fast-fail itself is essential (bounded work queue so
  a flood can't wedge the work loop). The two-tier split is accidental.
- **Severity:** medium. **Effort:** small.

### 7. `CallbackDispatcher` silently drops user callbacks; hand-rolled where stdlib suffices

- **Location:** `runtime_support.py:79-134`; drop at `108-114`; the `dropped`
  counter is read nowhere in the codebase.
- **What & why it hurts:** The dedicated callback thread is justified (see
  Essential below), but the implementation is a hand-rolled condition-variable
  worker with a drop-oldest policy: when the queue fills, a *user's
  notification callback invocation is discarded* with no log line — only an
  unread counter is bumped. A student debugging "my job_status callback never
  fired" has no signal at all. The class also duplicates what
  `queue.Queue` + a 10-line worker loop (or
  `ThreadPoolExecutor(max_workers=1)`) provides.
- **Simpler alternative:** `queue.Queue(maxsize=N)` + a daemon worker thread;
  on `queue.Full`, `log.warning("dropping callback for %s/%s/%s", …)` (or
  block briefly — the work loop can afford milliseconds). Delete the CV dance.
- **Essential?** The *thread* is essential: user callbacks run arbitrary code,
  and a callback that itself calls `runtime.call()` would deadlock the work
  loop — this is the "slow plugin must not look dead / user code can never be
  disciplined" requirement. The custom CV queue and the *silent* drop policy
  are accidental.
- **Severity:** medium. **Effort:** small.

### 8. `_synthesize_lost`: every consumer reconstructs tombstones the wire refuses to carry

- **Location:** `runtime.py:839-886` (`_on_topology`, `_synthesize_lost`).
- **What & why it hurts:** The broker drops a lost participant from the
  topology snapshot without saying so, and every runtime therefore diffs
  consecutive snapshots to *re-synthesize* a `liveness='lost'` entry, with a
  17-line docstring explaining the delivery-only/never-stored subtlety, and
  the invariant duplicated wherever else topology is consumed. Complexity in
  N consumers compensating for a one-line gap in one producer.
- **Simpler alternative:** Have the broker include the departing participant
  once, with `liveness='lost'`, in the final topology broadcast (a real
  tombstone). `_on_topology` shrinks to "store snapshot, fan out"; the
  synthesis function and its docstring disappear. (Broker change — out of this
  scope's files, but this scope pays the cost.)
- **Essential?** The behavior (observing true loss exactly once, so sessions
  are reclaimed — use case 7) is essential; the *client-side reconstruction*
  is an accidental consequence of the wire shape.
- **Severity:** medium. **Effort:** medium (protocol-adjacent, cross-module).

### 9. Gateway captures its event loop lazily, in two places, dropping events until then

- **Location:** `gateway.py:295` (`_on_topology`), `gateway.py:420`
  (`sse_events`), guard at `275-277` (`_on_event` returns if loop is `None`).
- **What & why it hurts:** `self._routing_loop` is assigned as a side effect
  of whichever fires first — a topology change or an SSE connect. Until then,
  `_on_event` silently discards frames. A student cannot see from the
  constructor when (or whether) the loop is ever set; the correctness of the
  cross-thread handoff depends on an implicit "uvicorn is running by the time
  anyone cares" ordering.
- **Simpler alternative:** Pass the routing loop to `Gateway.__init__` (the
  broker knows it), or set it in a FastAPI startup hook:
  `@app.on_event('startup') async def _(): self._routing_loop =
  asyncio.get_running_loop()`. Delete both lazy assignments and the None
  guard's silent drop.
- **Essential?** The cross-loop handoff is essential (tap fires on the
  plugin-host loop). The lazy two-site capture is accidental.
- **Severity:** medium. **Effort:** small.

### 10. Silent TLS hostname-check downgrade decided by string-matching an OpenSSL message

- **Location:** `runtime.py:436-448` (handler), `484-494`
  (`_classify_cert_error`).
- **What & why it hurts:** On an `SSLCertVerificationError` whose message
  contains `'hostname'` or `'ip address'`, and with a pinned cert present, the
  runtime *disables hostname checking and reconnects* ("dev mode"), on the
  strength of substring-matching a library error string. It's a security
  downgrade taken automatically, keyed to text that OpenSSL may reword, and it
  "mirrors `EndpointService._classify_cert_error`" — a class that no longer
  exists (see finding 4).
- **Simpler alternative:** When a cert is explicitly pinned, configure the
  context up front for pinned-cert semantics (`check_hostname=False` +
  `CERT_REQUIRED` against the pinned CA — the docstring itself argues
  `CERT_REQUIRED` already guarantees the peer), or require an explicit
  `insecure_skip_hostname=True`. Either removes the retry-with-relaxation loop
  and the string classifier.
- **Essential?** Supporting self-signed dev certs is a practical need; the
  reactive string-sniffing downgrade is accidental.
- **Severity:** medium. **Effort:** small.

### 11. The runtime carries a FastAPI app that never serves HTTP

- **Location:** `runtime.py:60` (import), `154-169` (app construction +
  `app.state` wiring), `671-673` (`HTTPException` catch);
  `dispatch.py` exists precisely to bypass the ASGI stack.
- **What & why it hurts:** The endpoint constructs
  `FastAPI(title="ORBIT Endpoint Runtime")` purely as a *route-table carrier*:
  plugins register against it, dispatch reads `app.state.direct_routes`, and
  no server ever binds it. A student naturally asks "where does this app
  listen?" — the answer is "nowhere", which costs real reading time, and the
  `app.state.*` indirection (`endpoint_service`, `is_broker`, `direct_routes`
  re-grabbed at `runtime.py:164/169/253`) is a stringly-typed side channel.
- **Simpler alternative:** Give plugins a small `Host` object (route table +
  `notify()` + the few `state` fields) and let the gateway alone own FastAPI.
  This is a plugin-framework change beyond this scope, so at minimum: a
  prominent comment in `runtime.py` stating "this app is never served; it is
  only the plugins' registration substrate", and stop re-assigning
  `self._direct_routes` twice (`164` and `169/253` — the second assignment at
  `169` is a no-op two lines after `164`).
- **Essential?** Plugin compatibility currently forces it; the requirement
  ("core stays small, plugin interface clarity is paramount") argues the
  substrate should eventually be honest rather than a vestigial web framework.
- **Severity:** medium. **Effort:** large (full fix) / small (documentation +
  dead-assignment cleanup).

### 12. `Gateway._register_routes`: 190 lines of nested closures over a `self_` alias

- **Location:** `gateway.py:378-566`; alias at `381`; protected-member calls
  at `459` (`broker._disconnect`) and `466` (`broker._handle_control`).
- **What & why it hurts:** Nine route handlers are defined as closures inside
  one method, all referencing `self_` (an alias needed only because the
  decorator style hides `self`). Handlers can't be unit-tested or read in
  isolation, and the method is the longest thing in the file. Two handlers
  reach into broker *protected* members, which the 58-line module docstring
  then spends a paragraph excusing.
- **Simpler alternative:** Make the handlers bound methods
  (`async def _proxy(self, endpoint_name, path, request)`) registered with
  explicit `app.add_api_route(...)` calls in a short `_register_routes`; give
  the broker public `disconnect(name)` / `terminate()` methods and delete the
  seam apologia from the docstring.
- **Essential?** The routes themselves are the compat-ingress requirement; the
  closure nesting and protected-member coupling are accidental.
- **Severity:** medium. **Effort:** small/medium.

### 13. In-flight consumer calls hang up to 600 s across a broker disconnect

- **Location:** `runtime.py:686-702` (`_call` / `_pending`), default
  `call_timeout=600.0` at `119`; reconnect loop `406-463` never touches
  `_pending`.
- **What & why it hurts:** When the WS drops, pending request futures are left
  to ride out the full timeout — ten minutes by default — with no log or
  error. If the correlated response genuinely can survive a reconnect (same
  name, broker still holds the route), that is a deliberate resilience choice
  (use case 7) — but nothing in the code says so, and a student debugging a
  hung `call()` gets zero signal.
- **Simpler alternative:** Either fail pending futures on disconnect with a
  clear "connection lost" error, or — if surviving reconnects is intended —
  say exactly that in a comment at `_pending`'s declaration and log at
  reconnect time ("N calls still pending across reconnect").
- **Essential?** Disconnect-survival is a named requirement; the *silence* is
  not.
- **Severity:** medium (debuggability). **Effort:** small.

### 14. Small warts: dead branches, ignored readiness, odd argparse, dual-named response fields

- **Location & items:**
  - `runtime.py:712-716` — `timeout` can never be `None` after line 712, so
    `None if timeout is None else timeout + 5.0` is a dead branch.
  - `runtime.py:282, 289` — `wk_ready.wait(timeout=5.0)` return values are
    ignored; if a loop thread fails to come up, the next line dereferences a
    `None` loop with a confusing `AttributeError` instead of a clear error.
  - `bin/radical-orbit-endpoint.py:28-35` — `nargs="?"` on `--name/--url/--cert`
    is unusual for options (allows a bare `--name` meaning `None`); plain
    optional args behave identically for every documented usage.
  - `runtime_client.py:63-68` — `RuntimeResponse` carries every field twice
    (`status`/`status_code`, `body`/`content`) to satisfy two calling
    conventions; pick the httpx names and let `.status`/`.body` go.
  - `runtime.py:983-987` — `_open_tunnel_reverse` polls `os.listdir` on the
    parent directory instead of `relay_file.exists()`; if this is an NFS
    cache-revalidation trick it needs a one-line comment, otherwise use
    `exists()`.
  - `gateway.py:534-540` vs `560-566` vs `runtime_client.py:178-182` — the
    response-dict unpack (`status`/`headers`/`body`, str→bytes) is copied
    three times; one `_dict_to_response(resp)` helper serves all.
- **Simpler alternative:** as listed per item.
- **Essential?** No.
- **Severity:** low. **Effort:** small.

### 15. `get_plugin` silently opens a session on every call

- **Location:** `runtime.py:753` (`client.register_session(**session_kwargs)`).
- **What & why it hurts:** A method named `get_plugin` performs a remote,
  state-creating side effect (session registration) unconditionally. For
  session-less interactions (e.g. `sysinfo.homedir()`, documented as
  session-less) the caller still pays for and leaks a session unless they
  `close()`. The name promises a lookup; the body does a lookup *and* an RPC.
- **Simpler alternative:** Either rename the intent into the API
  (`connect_plugin(...)`) or make session registration explicit/lazy:
  return the bound client and let the first session-requiring call (or an
  explicit `register_session()`) create it.
- **Essential?** No requirement forces implicit sessions; it is a convenience
  default with a naming cost.
- **Severity:** low. **Effort:** small (API-visible, so coordinate with
  examples).

### 16. Three bespoke bounded-ish queue classes in one scope, three different drop policies

- **Location:** `runtime_support.py:31` (`Handoff` — "soft" bound, counts but
  never drops), `runtime_support.py:79` (`CallbackDispatcher` — drop-oldest,
  silent), `gateway.py:116` (`_SSEQueue` — drop-oldest + `asyncio.Event`).
- **What & why it hurts:** A student meets three near-identical
  deque+counter structures with subtly different semantics and must diff them
  mentally to learn that the differences are mostly incidental. (The broker
  reportedly has a fourth, per `gateway.py:46-48`.)
- **Simpler alternative:** After findings 3 and 7 (stdlib for two of the
  three), only `_SSEQueue` remains — 15 lines, fine as-is. If all three must
  stay, unify on one `BoundedQueue` with an explicit drop policy.
- **Essential?** Bounded fan-out queues are essential (a stalled browser must
  not grow broker memory); three private variants are not.
- **Severity:** low (mostly subsumed by findings 3 and 7). **Effort:** small.

---

## What earns its keep (for the record)

- **Transport thread vs work loop split** (`runtime.py:9-28, 364-388`): tied
  directly to "a slow or blocking plugin must not look like a dead host" —
  WS keepalive answered on a thread no plugin can touch. Essential; keep. The
  *handoff machinery between them* is where the fat is (finding 3), not the
  split itself.
- **Callback dispatcher thread** (as a thread): prevents user-callback →
  `runtime.call()` deadlock on the work loop. Essential; only its
  implementation and drop policy are challenged (finding 7).
- **`dispatch.py`**: small, single-purpose, honestly documented (modulo the
  stale `service.py` references, finding 4). A model for the rest.
- **`_RuntimeHTTP` / `_pack_request`** (`runtime_client.py:30-114`): a genuine,
  minimal seam that lets 11 helpers ride the WS unchanged — this is the
  right way to satisfy the uniform-contract requirement.
- **Both `bin/` scripts**: thin, clear, correct order of operations.
- **Gateway auth middleware + header hygiene**
  (`gateway.py:221-242, 353-374`): proportionate, well-commented
  (normalize-then-exempt, token never forwarded), serves the "access gated at
  ingress" requirement without ceremony.
