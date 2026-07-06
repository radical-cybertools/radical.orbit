# Broker implementation — session state / continuation notes

Working state for the milestone-by-milestone implementation of
`plans/broker_architecture_plan.md`. Updated by the supervising session as
milestones land; read this first when resuming.

Last update: 2026-07-06. THE BROKER REWRITE IS FULLY MERGED into `devel`
(the stacked chain #64–#77 plus the KISS-simplification pass #83), and all
subsequent open-issue work is merged too (see "Post-broker issue work"
below). `devel` is the single active line — there are **no open PRs**. Unit
suite on devel: **913 passed, 2 skipped**; flake8 clean. A PR-gating CI
(`.github/workflows/pr.yml`: flake8 + pytest on py3.10–3.12) now runs on
every PR into devel; #91 was the first PR merged through it.

The milestone table and per-milestone notes below are retained as the
historical record of the rewrite; they are DONE/MERGED.

## Post-broker issue work (through 2026-07-06)

After the rewrite merged, the open GitHub issues were worked newest-first.
All landed in `devel`:

| Issue | PR | What |
|---|---|---|
| #44 | #78 | sysinfo `disk_usage` → catch `OSError` (vanished/unreadable mount) |
| #70/#71 | #81 | docs: IRI plugin reference + machine setup guide |
| #42 | #85 | edge cert pinning **re-targeted onto `runtime._ssl_context`** (pin-only via `create_default_context(cafile=...)`, fail-closed on missing cert); old bridge-stack PR #80 closed superseded |
| #43 | — | resolved by the rewrite itself (sync `_disconnect`→`_remove_participant`, socket-guarded teardown); issue + PR #79 closed |
| #82 | #86 | graceful child-job-failure — amsc `_JobFailureWatch` races `job_status` vs topology; + the `--tunnel <mode>` argv root-cause fix |
| #22 | #87 | security residuals: gateway body cap, sysinfo/lucid/handler timeouts, `BrokerTuning.forwarded_call_cap` |
| #9 | #88 | PR-gating CI (flake8 + pytest matrix); ruff/mypy deferred; **3.9 dropped** (asyncio event-loop tail) |
| #34/#35/#39 | #89 | Explorer session auto-recovery + `list_tasks`/`list_jobs`-on-open + Cancel-All; **quickjs pytest JS harness** over shared `data/plugins/session_util.js` |
| #40 | #90 | quinfo: surface extra squeue/qstat job fields + one shared `jobStateBadge` |
| #14 | #91 | one canonical error envelope `{error,status_code,detail}` across runtime/broker/gateway/plugins — new `errors.py`, gateway HTTPException handler, staging ladder → `http_exception` |

Every gemini-code-assist review thread on those PRs was replied-to and
resolved. Stale local branches `fix/issue_42` / `fix/issue_43` are dead
(their commits target deleted files) — safe to delete.

**Next pickup:** #14's Phase 4 — job-state-vocabulary normalization through
`batch_system.STATE_*` for `queue_info`/`psij` — was explicitly scoped OUT of
#91 and is the cleanest follow-up. Other open tickets: #18 routing policy v1;
ROSE track (#16/#21/#6); #38 psij Frontier/Anvil (hardware); assorted doc/UI/
packaging (#3/#8/#11/#31/#32/#36/#37/#41).

## Historical record (broker rewrite)

Local-tree caveat: ~/bin/git-branch-status -a walks branches by CHECKOUT —
it parks the repo on feature/broker_9 (lexicographically last) and can
leave partial worktree residue; run it on a clean tree and check the
branch afterwards. Residue is recoverable with
`git checkout -f feature/broker_12` (verified byte-identical, nothing lost).

## Workflow (user-confirmed)

- Sub-agents (Opus for core milestones, Sonnet for well-specified/mechanical
  work) do the coding; the supervisor reviews diffs, independently runs
  tests + flake8, commits, pushes, opens PRs.
- Branches: `feature/broker_<n>`, STACKED (each off the previous); PRs opened
  per milestone, base = previous branch; **the user merges** (policy:
  "stack, you merge"). First PR (#64) bases on `devel`.
- Tests must be green + flake8 clean before every commit.
- Commit trailer: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- `gh` CLI: `~/bin/gh`, authenticated as `andre-merzky` (device flow).

## Milestone status

| Milestone | Branch | PR | State |
|---|---|---|---|
| M0 spike | — (code in `~/radical/m0_spike/`, uncommitted by design) | — | DONE; results in `plans/m0_results.md` (in #64) |
| M1 sessions | feature/broker_1 | #64 → devel | DONE, pushed, PR open |
| M2 protocol.py | feature/broker_2 | #65 → broker_1 | DONE, pushed, PR open |
| M3 broker core | feature/broker_3 | #66 → broker_2 | DONE, pushed, PR open |
| M4 endpoint runtime | feature/broker_4 | #67 → broker_3 | DONE, pushed, PR open |
| M5 gateway | feature/broker_5 | #68 → broker_4 | DONE, pushed, PR open |
| M6 session liveness | feature/broker_6 | #69 → broker_5 | DONE, pushed, PR open |
| M7 dispatcher port | feature/broker_7 | #72 → broker_6 | DONE, pushed, PR open |
| Final flip | feature/broker_8 | #73 → broker_7 | DONE, pushed, PR open (suite 841 after merge-forward of contract tests) |
| M7 contract tests | on broker_7 | in #72 | DONE (9 tests; disposable, die with the proxies) |
| M8 replay plugin | feature/broker_9 | #74 → broker_8 | DONE, pushed, PR open (suite 861) |
| Rename PR | feature/broker_10 | #75 → broker_9 | DONE, pushed, PR open (suite 867; legacy env/file fallbacks kept read-only) |

**PLAN COMPLETE** — every milestone implemented and stacked:
devel ← #64 ← #65 ← #66 ← #67 ← #68 ← #69 ← #72 ← #73 ← #74 ← #75.
User merges top-down. Remaining work is only the follow-up list below plus:
5. Docs overhaul PR — docs/source/ Sphinx tree, system_architecture.svg,
   roadmap_q1.txt still describe the pre-star architecture (rewrite, don't
   rename; flagged in PR #75).
6. Cross-repo: rhapsody backend onto the runtime behind a flag
   (~/radical/rhapsody, feature/orbit) — land together with the orbit
   merges per plan §Cross-repo.
7. Deferred M0 throughput gate (rhapsody-shaped e2e vs current-stack
   baseline; baseline location still unresolved).

NOTE: a secondary git worktree exists at
`/global/u2/m/merzky/radical/radical.orbit.wt/broker_7` (checked out on
feature/broker_7) for M7/PR-#72 review-round fixes. Any new commit made
there must be merged forward through the stack (broker_7 → broker_8 →
broker_9) so the flip branch doesn't silently trail.

NOTE: git push over SSH stopped working in this environment (publickey);
pushes go over HTTPS via gh credentials (`~/bin/gh auth setup-git` done;
push to `https://github.com/radical-cybertools/radical.orbit.git`).

Test baseline progression: 793 (devel) → 811 (M1) → 846 (M2) → 873 (M3) →
885 (M4) passed, +2 skips (radical.pilot missing) throughout. Run with:
`PYTHONPATH=$PWD/src ve3/bin/python -m pytest tests/unittests/ -q`
Lint: `ve3/bin/python -m flake8 src/ bin/`

## Decisions settled during implementation (beyond the plan doc)

1. **Single uvicorn server, single port** (HPC firewalls): broker core owns
   only the token-gated WS `/register`; the M5 gateway attaches HTTP/SSE/UI
   onto `Broker.app` via the constructor-declared seam
   (`app`, `pending`, `caller`, `tap()`, `topology_snapshot()`, token config).
2. **uvicorn keepalive VERIFIED**: uvicorn 0.46 + websockets 16 honor
   server-wide `ws_ping_interval`/`ws_ping_timeout` (silent client dropped at
   interval+timeout, measured 0.6 s for 0.3+0.3). No fallback needed.
3. **Envelope validation = pydantic v2** (M0 measured; dataclasses no edge);
   broker forwarding path stays raw msgpack dicts (`peek_routing`).
4. **M1 reconnect semantics**: reconnect must RE-STATE the original policy
   (omitted lifetime defaults to 'ephemeral' and 409s against a ttl session).
5. **client.py port strategy (M4)**: centralize PluginClient's transport into
   one seam; `RuntimePluginClient` overrides only that seam → all 11 plugin
   client helpers work over the runtime unchanged.
6. **Heartbeat defaults stand** (ping 1 s / pong 3 s / grace 10 s); pong floor
   ≥ 500 ms from the M0 GIL numbers; aggressive profiles need the M4 offload
   hygiene + frame cap.
7. Dragon: `rh.Session` constructs off-thread but the PROCESS must be started
   under the `dragon` launcher (endpoint wrapper's job, M4 docs note).
8. **Plugin namespaces are endpoint-relative in the star model (M4)**:
   `plugins[*].namespace = /{instance}`, routing is by `dst`; the M5 gateway
   maps the bridge-era URL shape `/{endpoint}/{ns}/...` onto (dst, path).
9. `register_ack` is the one frame parsed on the transport loop (handshake
   gates everything); all other frames parse on the work loop.

## Resuming M4 (if it did not land)

If `feature/broker_4` has no commit beyond M3's head: relaunch an Opus agent
with the M4 spec. The spec's essentials (full version in the supervisor
transcript; reconstructable from plan M4 + these deltas):
- `dispatch.py`: pure move of `RequestShim`/`_match_route` out of service.py
  (thin re-export keeps old imports); bridge_plugin_host imports from it.
- `runtime.py` `EndpointRuntime`: transport thread (own loop: connect/TLS/
  tunnel reuse from service.py, register+ack, resume_key, name-in-use retry
  w/ backoff, ws keepalive 1/3, max_size=FRAME_CAP) + work loop (parse,
  served-plugin dispatch via dispatch.py, bounded request concurrency w/
  immediate 503+Retry-After, pending table, topology snapshot, callback
  registry w/ auto-Subscribe) + dedicated callback-dispatcher thread
  (bounded, drop-oldest); consumer sync facade `call`/`get_plugin`/
  `register_callback`/`serve`; auto-name `consumer.<uuid8>`; M0 coalesced
  handoff pattern both directions.
- client.py seam (decision 5 above); `RuntimePluginClient` in runtime.py.
- plugin_rhapsody: `asyncio.to_thread` around blocking Dragon session init
  (hygiene, not liveness-critical — M0 measured 2.8 ms GIL hold).
- tests/unittests/test_runtime.py per the coverage list in plan M4 +
  transport-isolation smoke.

## Pre-flip checklist (must land before the final-flip PR)

- Gateway cannot reach broker-hosted plugins over HTTP (caller.call routes
  via the registry only) — needed for iri_connect/dispatcher UI; natural
  home M7 (host rework). Recorded in PR #68.
- Dynamic `ui_module` JS cache not ported to the gateway (`/plugins/*` is
  packaged-static only) — needed for `iri.<endpoint>` UIs.
- M6 owner channel: the serving runtime injects the broker-stamped envelope
  `src` as a trusted request header (overwriting any client-supplied copy);
  plugin_base reads it as the session owner. Gateway-originated requests
  carry no identity → owner None → capability-style (documented, per plan).

## M7 spec seeds (next milestone; branch feature/broker_7 off broker_6)

Plan M7 + these deltas. Opus agent; expect 1–2 review rounds.
- plugin_task_dispatcher.py: replace the loopback-HTTP BridgeClient
  (`_start_bridge_client`, `_wait_for_bc`, the SSE thread) with the broker
  in-process caller (`Broker.caller` / `BrokerCaller.call_threadsafe` — the
  M0-validated handle; broker.py exposes it on the gateway seam). All 16
  `to_thread` wrappers go (every one wraps a BridgeClient HTTP call).
  Consume rich topology via `on_topology_change` (framework suspect/lost +
  grace replaces `_seen_child_endpoints`).
- Pools → `(owning_sid, pool_name)` strict isolation: task_dispatcher_state
  store gains `owning_sid` + sid-scoped state-dir layout; restart-time
  replay BUILT here (today recovery is lazy); session-close teardown added;
  pilots reclaimed via the M6 reclaim-drain (plugin_base arms it; the
  dispatcher's session close must cancel pool pilots). DELETE the sharing
  machinery: `_pool_configs_compatible`, attach-or-409 in
  `_materialise_pool`.
- Also natural in M7 (pre-flip item 1): gateway HTTP access to
  broker-hosted plugins — route `dst=='broker'` in the gateway proxy into
  the host loop (`broker._dispatch_to_host` path) instead of 404.
- Note: the dispatcher runs broker-hosted (host thread/loop); its caller
  usage must be thread-aware (call_threadsafe), never touch routing-loop
  state directly.

## Follow-up status (updated)

- Items 1–4 and the smoke-test items 14–22 (numbering per the deferred-item
  inventory shown to the user): DONE in PR #76 (feature/broker_11 →
  broker_10; two commits: fix bundle 492160c, transport convergence).
  Item 1's contract tests retired with the proxies per their deletion
  contract; a non-disposable drift test + route constants replace them.
- Item 5 (docs overhaul): DONE — PR #77 (feature/broker_12 → broker_11).
  Known residuals flagged in the PR: no sphinx in ve3 (no build smoke),
  plugin_queue_info.rst multi-cluster register_client section stale
  (was scoped light-touch), possible benign duplicate-autodoc warnings.
- Items 14–22 (e2e smoke findings + API follow-ups): DONE in PR #76;
  the --host fix (14) re-verified live on the fixed branch (loopback bind
  confirmed at kernel level; default 0.0.0.0 unchanged; hosted-plugin
  call_host path intact after the _plugin_host rename).
- STILL OPEN (the complete remaining list):
  * item 6 — rhapsody cross-repo backend onto the runtime behind a flag
    (~/radical/rhapsody, plan §Cross-repo; repos land together);
  * item 7 — deferred M0 throughput gate (rhapsody-shaped e2e vs
    current-stack baseline; baseline artifacts never located);
  * plan-level deferrals 8–13 — reverse correlated RPC, dedicated control
    socket, per-tenant selective routing, mTLS/per-participant identity,
    direct endpoint↔endpoint data, heartbeat re-validation at scale;
  * docs residuals flagged in #77 — no sphinx in ve3 (add a CI docs
    build when available), plugin_queue_info.rst multi-cluster
    register_client section stale.

## Post-flip follow-ups (accepted from M7 design review)

1. QUEUED NEXT (after the flip commits — single worktree, no branch switch
   while an agent is active): contract tests pinning the M7 async proxies'
   route templates against the real PluginPSIJ/PluginRhapsody route tables
   (via match_route on locally constructed plugins) — lands on broker_7 /
   PR #72, then merge broker_7 forward into broker_8.
2. Single-source route-path constants in plugin_psij.py/plugin_rhapsody.py
   shared by add_route_* registration and helper/proxy URL formatting.
3. Async transport twin (`PluginClient._arequest` + caller-backed adapter)
   so the real helpers run natively on the host loop; delete the M7
   _PsijProxy/_RhapsodyProxy AND the item-1 contract tests in the same PR
   (they guard the proxy window only; helpers-vs-server drift is item 2's
   subject, pre-existing for all 11 helpers). Own PR, after the flip.

4. Surface the broker-stamped `seq` to runtime live callbacks (additive
   callback-signature/metadata option, NOT a wire change — seq is already
   on the event envelope; the runtime drops it before user code). Lets the
   M8 replay→live splice dedup on broker seq instead of an application
   key. Flagged in PR #74.

## After M7

Final flip (small PR: bin/ entry points → broker + runtime; delete
models.py, bridge.py, old service/client paths + superseded tests; check
the pre-flip checklist above is empty). M8 replay plugin (optional,
deferrable past users). Rename PR dead last (bridge→broker residue; grep
for rename collateral per plan). Cross-repo: rhapsody backend onto the
runtime behind a flag (sibling repo ~/radical/rhapsody, branch
feature/orbit context in plan).

## Environment notes

- Repo: `/global/u2/m/merzky/radical/radical.orbit`; venv `ve3/` (python
  3.11, websockets 16, uvicorn 0.46, pydantic 2.13, rhapsody 0.1.2, dragon).
- Milestones after M0 need NO allocation; a login-node session works.
- Non-interactive ssh drops NERSC's `PYTHONUSERBASE`
  (`~/.local/perlmutter/python-3.11`) — where user-site packages live.
- M0 spike code: `~/radical/m0_spike/` (SPEC.md documents it; keep
  uncommitted per user decision).
- Untracked junk in repo root predates this work (`*.pkl`, `*.pem`,
  `69e13a9.patch`, `ve3/`, `build/`) — leave alone.
