# Plan: #18 — Routing policy v1 (capability tags + simple failover)

Issue: https://github.com/radical-cybertools/radical.orbit/issues/18
Status check (2026-08-25): **not started**. The broker routes purely by
explicit `dst` lookup (`broker.py` `_handle_request` ~829, 404 on unknown);
the failure path is fail-fast (`_fail_calls_for` ~1089 synthesizes 504 for
in-flight calls when an endpoint drops) — exactly the seam where failover
would live. No tag vocabulary exists in the protocol (`protocol.py` models
are `extra='forbid'`, so tags are a deliberate protocol change). ROADMAP.md
still lists this as planned. De-facto capability signal available today:
per-endpoint `plugins` dict in the topology (the dispatcher's
`_pick_endpoint_name`, plugin_task_dispatcher.py:648, already selects on
it, crudely).

## Design

### 1. Capability tags in the handshake

- `Register` (protocol.py) gains `tags: Dict[str, str] = {}` (e.g.
  `{'site': 'olcf', 'machine': 'frontier', 'gpu': 'mi250x'}`), carried into
  `ParticipantInfo` and the topology snapshot. Version-tolerant: field has a
  default, so old endpoints register fine (protocol `version` already
  exists; no bump needed for an additive optional field — confirm the
  `extra='forbid'` cross-version story in tests).
- Endpoint side: `--tags k=v,k=v` on the bin /
  `EndpointRuntime(tags={...})`; implicit tags added by the runtime:
  detected role (login/compute), batch system, hostname.
- Plugin names remain the primary implicit capability (already in
  topology); tags add the operator-declared dimension.

### 2. Selector addressing (the routing policy)

- New `dst` form: keep exact names untouched; add a selector syntax the
  broker resolves at route time, e.g. `dst='@{plugin=psij,site=olcf}'` (or
  a structured `dst_selector` field on the request — decide in review;
  structured field is cleaner given `extra='forbid'`, string form is
  friendlier to the gateway's path-based addressing
  `/{endpoint}/{plugin}/...` where `@selector` slots into the endpoint
  segment).
- Matching rule v1: all key=value pairs must match; candidates must be
  liveness-`ok` (not `suspect`/`lost`); deterministic choice among matches
  (lexical first — plus optional round-robin later; keep v1 deterministic
  for testability).
- Resolution happens once per request at the broker; the response carries
  the resolved concrete `src`, so the caller learns who answered.

### 3. Simple failover

Scope carefully — v1 is **re-route at failure detection, not transparent
retry**:
- Selector-addressed, still-unanswered in-flight calls: when the resolved
  target transitions `lost`, instead of `_fail_calls_for`'s synthetic 504,
  re-resolve the selector and re-send to the next candidate — **only if**
  the request is marked idempotent (`idempotent: bool = False` on the
  request envelope; default off, so semantics never surprise). Non-idempotent
  or exact-name calls keep today's fail-fast 504.
- No candidate left → 503 with a clear "no participant matches" envelope.
- Session-stateful plugins complicate failover (sessions live on one
  endpoint); v1 restricts failover to session-less routes — document this
  loudly. (Session mobility is its own future issue.)

### 4. Out of scope v1

Load-based selection, weights, round-robin fairness, session migration,
gateway UI for tags (Explorer can show tags in the topology view — trivial,
do it), dispatcher adoption (follow-up: `_pick_endpoint_name` → selector).

## Tests

- Protocol: tags round-trip, old-frame compat (no tags), forbid-extra still
  holds elsewhere.
- Broker unit (FakeWS pattern, test_broker.py): selector resolves / no
  match 503 / suspect excluded; deterministic pick; response src rewrite.
- Failover: idempotent selector call re-routes on `lost` mid-flight;
  non-idempotent gets 504; exhaustion → 503.
- E2E (test_runtime.py harness): two endpoints with different tags, call by
  selector, kill the chosen one, assert the retry lands.

## Effort

Large-ish (protocol + broker core + runtime + gateway addressing). Split:
PR1 tags in handshake/topology (+ Explorer display), PR2 selector routing,
PR3 failover. PR1 is independently useful (dispatcher/xgfabric can select
on tags manually) — do it first and cheap.
