# Review: wire protocol + broker hub (protocol.py, broker.py, broker_events.py, broker_plugin_host.py)

## Overall impression

For a routing hub with liveness tracking, resume, and event fan-out, this core is
reasonably lean: `protocol.py` is a clean one-envelope design, the routing loop in
`broker.py` really is a short dict-dispatch (`_route_frame` is 15 lines), and the
two-loops/one-host-thread split is genuinely forced by the "failure detection must be
decoupled from plugin behavior" requirement. The accidental complexity is concentrated
in the edges: the protocol carries dead/reserved surface (`peek_routing`, `channel`,
`capabilities`, `is_binary`) that a student will study for nothing; the broker keeps
per-participant state in five parallel dicts plus three more in `EventRouter`; there
are two overlapping correlation tables; a third liveness layer (the loop-lag watchdog)
sits on top of suspect/grace; and comments throughout justify decisions by citing plan
artifacts ("M0 lesson #2", "pre-flip item 1", "m0_results.md") that a reader of the
code alone cannot consult. One seam (`_host_broadcast`'s topology path) has a docstring
that promises behavior the code does not perform.

---

## Findings

### 1. `_host_broadcast` silently drops topology announcements its docstring promises to rebroadcast

- **Location**: `broker.py:820-845` (`Broker._host_broadcast`), fed by
  `broker_plugin_host.py:68-83` (`_announce_topology`) and
  `plugin_host_base.py` (`register_dynamic_plugin` → `_announce_topology`).
- **What & why it hurts**: The docstring says "topology pings just trigger a
  rebroadcast", but the body only handles `topic == 'notification'`; a
  `'topology'` call falls through to `return _noop()` — nothing happens. So when a
  hosted plugin is dynamically registered (the `iri.<endpoint>` flow), the host
  dutifully calls `_announce_topology`, which awaits `_broadcast_fn('topology', ...)`,
  which is a no-op: no wire `topology` frame, no gateway listener fired. A student
  tracing "why doesn't the UI update when I connect an IRI endpoint" will read a
  docstring that says the opposite of the code. On top of that, `_host_broadcast` is a
  *sync* function that returns a coroutine object (`_noop()`) so that the caller's
  `await self._broadcast_fn(...)` doesn't blow up — a sync-function-returning-awaitable
  hybrid is exactly the kind of cleverness that costs every reader.
- **Simpler alternative**: Make `_host_broadcast` an `async def` (callers already
  await it). Handle the `'topology'` topic for real:
  `self._loop.call_soon_threadsafe(lambda: spawn(self._broadcast_topology(), ...))`,
  mirroring the `'notification'` branch. If topology-on-host-change is intentionally
  unsupported, say so in the docstring and delete the misleading sentence.
- **Essential?** No. The underlying behavior (topology change → broadcast) *is*
  required ("uniform message contract ... push events"), but the coroutine-returning
  shim and the doc/code mismatch are purely accidental.
- **Severity**: high
- **Effort**: small

### 2. Dead and reserved protocol surface: `peek_routing`, `channel`, `capabilities`, `is_binary`

- **Location**: `protocol.py:327-348` (`peek_routing`), `protocol.py:145`
  (`channel`, "reserved; always None today"), `protocol.py:199,212`
  (`capabilities` on `Register`/`RegisterAck`), `protocol.py:163,172`
  (`is_binary` on `Request`/`Response`).
- **What & why it hurts**: `peek_routing` is never called anywhere in `src/` or
  `bin/` — the broker inlines `msgpack.unpackb` in `_route_frame` (broker.py:668)
  instead. Its docstring even claims to *be* "the broker's pure-forwarding fast path",
  which is false; a student cross-referencing it against the broker will waste time
  reconciling two fast paths. `channel` is an admitted placeholder ("carries no
  behaviour yet"). `capabilities` has zero consumers outside `protocol.py` (reserved
  for future version negotiation). `is_binary` is never read by anyone (the only
  mention outside `protocol.py` is broker.py:943 setting it to `False`) — with a
  msgpack-native `bytes` body it has no job. Together that is four pieces of the
  *wire contract* — the most-read file in the codebase — that do nothing. The brief
  calls this out directly: speculative generality no current requirement needs.
- **Simpler alternative**: Delete `peek_routing` (or actually call it from
  `_route_frame`, which would also centralize the "is this a dict?" check the inline
  unpack skips). Drop `channel`, `capabilities`, and `is_binary`; the envelope is
  versioned, so re-adding a field later is a v2 bump — exactly what the version field
  is for. That removes ~40 lines and three "why is this always None/empty/False?"
  questions.
- **Essential?** No requirement in the brief needs any of them today. The "uniform
  message contract" requirement is met by the remaining fields.
- **Severity**: medium
- **Effort**: small (protocol + touch-ups at the few construction sites)

### 3. Two overlapping correlation tables (`pending` + `_inflight`), one of them unbounded

- **Location**: `broker.py:237-238` (declarations), `broker.py:721-752`
  (`_broker_call`/`_resolve_pending`), `broker.py:702,708,740` (bookkeeping),
  `broker.py:928-955` (`_fail_inflight_for`).
- **What & why it hurts**: Every broker-originated call writes into *both*
  `self.pending` (corr_id → future) and `self._inflight` (corr_id → (src, dst));
  forwarded endpoint↔endpoint calls only into `_inflight`. A student must reverse-
  engineer the ownership rule ("pending is the futures for src='broker'; inflight is
  everything, for fast-fail") from three scattered call sites plus the branchy
  `_fail_inflight_for`. Two additional wrinkles: (a) the `pending_cap` check
  (broker.py:726) is documented twice as a "per-``src`` cap" (broker.py:115-117, 157)
  but there is only one src — it is a single global cap on the broker's own table;
  the naming oversells it. (b) `_inflight` has no cap and no timeout: an entry for a
  forwarded request whose responder simply never answers (a buggy plugin) lives
  forever, and a busy endpoint pair can grow the table without bound —
  the cap protects only the broker's own calls.
- **Simpler alternative**: One table: `self._calls: Dict[corr_id, _Call]` where
  `_Call = (src, dst, future_or_None, deadline)`. Broker-owned entries have a future;
  forwarded entries don't. `_fail_inflight_for` becomes a single scan; `_resolve_pending`
  becomes "pop, and if it has a future, set it". A periodic sweep (or reuse of the
  existing grace machinery) evicts entries past `deadline`, bounding the forwarded side
  too. Also rename/redoc the cap as what it is: a cap on broker-originated in-flight calls.
- **Essential?** Correlation itself is essential (the "uniform message contract"
  request/response requirement) and fast-fail-on-lost is essential (use case 7). Two
  parallel tables and the unbounded forwarded side are accidental.
- **Severity**: medium
- **Effort**: medium

### 4. Per-participant state smeared across eight parallel dicts in two modules

- **Location**: `broker.py:216-220` (`registry`, `_participants`, `_liveness`,
  `_resume_keys`, `_grace_timers`), `broker_events.py:72-74` (`_subscriptions`,
  `_out`, `_senders`); teardown at `broker.py:957-964` (`_remove_participant`) +
  `broker_events.py:86-90` (`remove_endpoint`).
- **What & why it hurts**: Everything the broker knows about one participant — socket,
  role/plugins, liveness, resume key, grace timer, subscriptions, out-queue, sender
  task — lives in eight name-keyed dicts. Registration must populate five of them in
  the right order (`_register`, broker.py:641-650); removal must pop from all eight
  across two objects. A student asking "what is the state of endpoint X?" has to
  mentally join eight tables, and any future code path that forgets one pop creates a
  leak that is invisible in review (the resume path at broker.py:617-635 already
  updates a different subset than the fresh path). The comment "all touched only on
  the routing loop" (broker.py:215) is the *one* invariant that makes this safe, and
  it must hold for all eight simultaneously.
- **Simpler alternative**: One `class _Participant:` (plain attributes: `ws, role,
  plugins, liveness, resume_key, grace_timer, subscriptions, out_queue, sender_task`)
  in one dict `self._participants[name]`. `_remove_participant` becomes
  `pop + cancel two handles`. `EventRouter` can keep its logic but operate on the
  participant record (or its queue) passed in, rather than shadow-keying its own dicts
  by the same names.
- **Essential?** The *state* is essential (registration/resume/liveness are the hub's
  job); the smearing is accidental — a single record per participant serves the same
  requirements with strictly less bookkeeping.
- **Severity**: medium
- **Effort**: medium

### 5. Third liveness layer: the loop-lag watchdog + suppression window

- **Location**: `broker.py:966-987` (`_watchdog`, `_watchdog_check`),
  `broker.py:916-920` (suppression branch in `_on_lost`), `broker.py:247`
  (`_suppress_until`), constructor knobs `clock`/`watchdog_interval`
  (broker.py:180-181, 210-211).
- **What & why it hurts**: Liveness already has two stages — socket drop → `suspect`,
  grace timer → `lost` — which the module header explains well. On top of that sits a
  background task sampling loop drift, a `_suppress_until` window, and a re-arm branch
  inside `_on_lost`, plus an injectable `clock` and `watchdog_interval` whose only
  purpose is testing this machinery. Note what grace already gives you for free: the
  grace timers live on the *same* loop that stalled, so during a stall nothing can be
  declared lost, and after resume every suspect still gets the *full* grace window to
  re-register (timers are armed in `_on_socket_drop`, which runs post-resume). The
  watchdog therefore only changes behavior when an endpoint fails to reconnect within
  `grace` *after* the loop has already recovered — a scenario where declaring it lost
  is arguably correct anyway. Three interacting time-based mechanisms is a lot for a
  student to simulate in their head, and the payoff is marginal.
- **Simpler alternative**: Delete the watchdog, `_suppress_until`, the `_on_lost`
  branch, and the `clock`/`watchdog_interval` knobs; rely on suspect + grace. If mass
  false-lost after long stalls is a demonstrated problem, the cheaper fix is a one-line
  stamp: record `resumed_at` when a drop is processed and size the grace timer from
  that — no background task, no injected clock.
- **Essential?** The requirement ("failure detection reflects host/network health")
  is served by suspect + grace. The third layer is defensive machinery not traceable
  to a requirement in the brief.
- **Severity**: medium
- **Effort**: small

### 6. Comments justify design by citing plan artifacts the reader cannot see

- **Location**: pervasive — e.g. `protocol.py:7` ("``m0_results.md`` measured..."),
  `protocol.py:24` ("later milestone"), `broker.py:17` ("M0-validated ... handoff"),
  `broker.py:114` ("the M7 dispatcher seam"), `broker.py:226` ("the M5 gateway"),
  `broker.py:308-309` ("the M8 replay plugin's hook and the M5 gateway's"),
  `broker.py:343` / `broker.py:782` ("pre-flip item 2"/"item 1"),
  `broker.py:684` ("plan security decision"), `broker.py:877` / `broker.py:929`
  ("M0 lesson #2"/"#1"), `broker.py:502` ("verified (M3)...").
- **What & why it hurts**: These read as authoritative justifications, but they point
  at milestone documents and measurement files that are not part of the code a student
  has. "Pre-flip item 1" and "M0 lesson #2" convey zero information on their own; worse,
  they train the reader to accept complexity on cited-but-unavailable authority instead
  of an argument in place. Where a real reason exists it is usually *also* present
  (e.g. the teardown-guard comment at broker.py:877-879 explains the race perfectly
  well without "M0 lesson #2"), so the plan tags are pure noise.
- **Simpler alternative**: Strip milestone/plan tags; keep (or inline) the actual
  reasons. "A replaced socket's handler fires its `finally` after resume installed the
  new socket — proceed only if this socket is still the live one" is complete by itself.
  Where the tag is the *only* justification (e.g. "M0-validated handoff"), replace it
  with the one-sentence measurement result or drop the claim.
- **Essential?** No — pure documentation debt.
- **Severity**: medium (pervasive; directly attacks the "students can read it
  standalone" goal)
- **Effort**: small

### 7. The bounded drop-oldest queue is written four times across the codebase

- **Location**: `broker_events.py:35-49` (`_OutQueue`, `__slots__ = ('buf', 'dropped',
  'wake')`) — near-identical twins at `gateway.py:116-136` (`_SSEQueue`, same slots,
  same push logic, same "deque drops the oldest on append" comment) and
  `plugin_replay.py:96-107`; a threaded variant at `runtime_support.py:89-112`.
- **What & why it hurts**: Only `broker_events.py` is in my scope, but its `_OutQueue`
  is byte-for-byte the same idea as the gateway's `_SSEQueue` (same three slots, same
  drop counter comment). Four copies means four places for the drop-accounting to
  drift (the consumer loops already differ subtly: `_sender` waits unconditionally,
  the gateway waits with a 1 s timeout). A student who has understood one copy still
  has to diff the others to be sure they are the same thing.
- **Simpler alternative**: One `BoundedDropOldestQueue` in a shared module (e.g.
  `utils.py` or a tiny `queues.py`), with the deque+counter+`asyncio.Event` push and a
  documented "consumer drains on wake" contract; the threaded runtime variant can
  subclass or wrap it.
- **Essential?** The *behavior* (a slow subscriber must not backpressure the routing
  loop — the failure-isolation requirement) is essential and the deque+Event
  implementation is a good, minimal one. The four-fold duplication is accidental.
- **Severity**: medium
- **Effort**: small

### 8. Three in-process fan-out channels, each with different threading semantics

- **Location**: `broker.py:307-315` (`tap` → runs callbacks on the *host* loop, via
  `broker_events.py:162-175`), `broker.py:317-332` (`add_topology_listener` → runs
  callbacks synchronously on the *routing* loop, broker.py:1020-1023), and the wire
  subscription path (`subscribe` frames → per-endpoint sender tasks).
- **What & why it hurts**: An in-process consumer (the gateway) must use *two*
  different registration APIs with *two* different threading contracts — tap callbacks
  land on the plugin-host loop and may be async; topology listeners are sync,
  argument-less, fire on the routing loop, and must go fetch `topology_snapshot()`
  themselves. Nothing in the names signals this asymmetry; a student wiring a new
  consumer has to read both implementations to learn where their code will run and
  what state they may touch there.
- **Simpler alternative**: Fold topology into the tap: publish a synthetic
  `{'kind': 'topology', ...}` event into `EventRouter.ingest`'s tap fan-out whenever
  `_broadcast_topology` runs (taps are already unfiltered and host-loop-supervised).
  One registration API, one threading rule ("in-process consumers run supervised on
  the host loop"), one place to document it. Wire delivery of `topology` frames stays
  as is.
- **Essential?** In-process fan-out is essential (the gateway's SSE surface is a named
  requirement); having two seams with divergent threading is accidental — the comment
  at broker.py:224-226 concedes topology "does not flow through the raw event tap"
  without saying why it couldn't.
- **Severity**: medium
- **Effort**: medium

### 9. `Event` requires `ts`/`seq` that senders cannot know and the broker overwrites

- **Location**: `protocol.py:183-189` (`Event.ts`, `Event.seq` required),
  `runtime.py:921-922` (endpoint fills `ts=0.0, seq=0`), `broker.py:839-840`
  (broker fills `'ts': 0.0, 'seq': 0`), `broker_events.py:186-195` (broker assigns
  the real values in `ingest`).
- **What & why it hurts**: The model declares `ts` and `seq` as required fields, yet
  both producers stuff dummy zeros in because the broker is the actual authority
  (docstring says so). A student reading the model reasonably concludes the sender
  provides them; the truth is the opposite, discoverable only by reading three files.
  The `if not raw.get('ts')` overwrite (broker_events.py:194) also means a sender
  *can* smuggle its own ts — a half-open contract.
- **Simpler alternative**: `ts: Optional[float] = None` and
  `seq: Optional[int] = None` with a one-line docstring "broker-assigned on ingest;
  senders leave unset", and have `ingest` assign unconditionally. Producers drop their
  dummy zeros.
- **Essential?** Broker-assigned sequencing is essential (drop detection via `seq`
  gaps, use case 4/7); requiring the sender to fake the fields is accidental.
- **Severity**: low
- **Effort**: small

### 10. `get_ui_modules` crosses the thread boundary to call a plain sync method

- **Location**: `broker.py:343-368` (`Broker.get_ui_modules`), target at
  `broker_plugin_host.py:183-195`.
- **What & why it hurts**: To read `{plugin: js}` the code defines a nested `async def
  _fetch`, schedules it with `run_coroutine_threadsafe`, and blocks up to 5 s on the
  result — all to invoke a synchronous method that iterates a dict and reads files.
  The stated reason ("the routing loop never touches host state") is a real invariant,
  but wrapping a sync call in a coroutine does not make the file I/O non-blocking — it
  just moves the same blocking read *onto the host loop*, where it stalls hosted
  plugins instead. The ceremony buys thread-affinity purity at the cost of an extra
  indirection a student must unpack, and it blocks whichever loop calls it.
- **Simpler alternative**: If the invariant matters, keep the hop but use
  `loop.call_soon_threadsafe` + `concurrent.futures.Future` (no fake coroutine). If it
  doesn't — `_plugins` is only mutated on the host loop, and a point-in-time dict
  iteration under the GIL is the same class of read `topology_snapshot` performs — call
  `host.get_ui_modules()` directly and say why it's safe.
- **Essential?** The host-thread isolation itself is essential (failure-detection
  decoupling); this particular crossing is ceremony.
- **Severity**: low
- **Effort**: small

### 11. `mint_corr_id`'s src-namespacing is never consumed

- **Location**: `protocol.py:82-90` (`mint_corr_id`), plus the rationale re-stated at
  `protocol.py:26-28`.
- **What & why it hurts**: corr_ids are minted as `f"{src}:{uuid4()}"` with a
  two-paragraph justification about unambiguous table ownership — but nothing anywhere
  splits, prefixes-matches, or otherwise reads the namespace (no
  `split`/`startswith`/`partition` on corr_id in the tree). A bare `uuid4` is already
  globally unique, and the broker tracks ownership explicitly in its inflight tuple
  `(src, dst)`. The prefix is a solution with no consumer, and its lengthy docstring
  invites students to hunt for the mechanism that uses it.
- **Simpler alternative**: `mint_corr_id() -> str(uuid.uuid4())` (or reuse `mint_id`),
  delete the ownership prose. The prefix *is* handy in log lines — if that's the real
  reason, say "prefix is for log readability only".
- **Essential?** No — uniqueness is the requirement; uuid4 alone provides it.
- **Severity**: low
- **Effort**: small

### 12. `BrokerPluginHost.on_topology_changed` is a dead parameter

- **Location**: `broker_plugin_host.py:29,36,77-81`; sole construction site
  `broker.py:450-453` passes `on_topology_changed=None`.
- **What & why it hurts**: The only caller in the codebase passes `None`, so the
  `if self._on_topology_changed:` branch in `_announce_topology` never runs; the
  docstring ("allows the broker script to update its global state...") describes a
  caller that doesn't exist. Speculative hook — a student will search for the wiring
  and find nothing.
- **Simpler alternative**: Delete the parameter and the branch;
  `_announce_topology` becomes a single `await self._broadcast_fn('topology', ...)`
  (which, per finding 1, should then actually do something).
- **Essential?** No.
- **Severity**: low
- **Effort**: small

### 13. `BrokerCaller` is a two-method pass-through class

- **Location**: `broker.py:110-144`; used via `app.state.broker_caller`
  (`broker_plugin_host.py:50`) and `runtime_client.py:174`.
- **What & why it hurts**: `BrokerCaller.call` forwards verbatim to
  `Broker._broker_call` (a private method it reaches with `_`-access), and
  `call_threadsafe` is `run_coroutine_threadsafe` around it. It exists to hand
  plugins/gateway a narrow handle instead of the whole broker — a legitimate goal —
  but the result is that a student tracing a hosted plugin's call goes
  `CallerHTTP → BrokerCaller.call_threadsafe → BrokerCaller.call → Broker._broker_call`,
  three hops of no logic.
- **Simpler alternative**: Make `call`/`call_threadsafe` public methods on `Broker`
  itself (rename `_broker_call` → `call`) and hand the broker's bound methods (or the
  broker) to the host; or keep the handle but have it *own* the pending-table logic so
  the class has a body. Either removes one layer.
- **Essential?** The seam (plugins must not grab arbitrary broker internals) is a
  reasonable reading of "the core stays small"; the extra hop is accidental.
- **Severity**: low
- **Effort**: small

### 14. 18-parameter constructor mixes transport config, tunables, and test seams

- **Location**: `broker.py:165-182` (`Broker.__init__` signature).
- **What & why it hurts**: TLS paths, auth, host/port, plugin spec, five liveness/
  backpressure tunables, an injectable clock, a watchdog interval, and a gateway flag
  all arrive flat in one signature. The docstring honestly explains that several exist
  "so unit tests run with tiny values" — i.e., they are test seams living in the
  production API. Students reading `Broker(...)` call sites can't tell config from
  knobs from test hooks.
- **Simpler alternative**: Dropping the watchdog (finding 5) removes `clock` and
  `watchdog_interval` outright. Group the remaining tunables into a small defaults
  dataclass (`BrokerTuning(ping_interval=1.0, ping_timeout=3.0, grace=10.0, ...)`)
  passed as one optional argument, leaving the constructor with the ~7 things an
  operator actually sets.
- **Essential?** Tunable liveness values are essential for tests of the liveness
  requirement; the flat signature and the clock injection are accidental.
- **Severity**: low
- **Effort**: small

### 15. `EventRouter._session_seq` grows forever

- **Location**: `broker_events.py:75,187-190`.
- **What & why it hurts**: One counter per session-id ever seen, never pruned — there
  is no session-end hook. A long-lived broker accumulates dead counters. Small in
  bytes, but it's an invariant a student can't find: nothing says who cleans it up,
  because nothing does.
- **Simpler alternative**: Prune a session's counter when its owning endpoint is
  removed (sessions are endpoint-scoped), or document explicitly that session counters
  are intentionally immortal so a re-registered session never re-uses seq numbers —
  either is fine; the silent leak is not.
- **Essential?** Per-session monotone `seq` is essential (drop detection / replay
  splice); the unbounded map is accidental (or at least undocumented).
- **Severity**: low
- **Effort**: small
