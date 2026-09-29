# Open-issue triage — 2026-08-25

Full pass over the 17 open issues: verified each against the committed devel
tree, closed the ones already delivered (with references in the close
comments), drafted implementation plans for the rest. Plans are in this
directory, one file per issue (ROSE's three sequential slices share one
file). Nothing is committed; nothing was implemented.

## Closed as done (see close comments for references)

- **#110** external plugins — entry-point mechanism
  (`radical.orbit.plugins` group, `plugin_host_base._discover_entry_points`)
  \+ plugin writer's tutorial with shipped math example (a6aaeb4,
  `docs/source/tutorial_plugin.rst`) already deliver the out-of-tree
  use case. Reopen path noted for a programmatic instance-registration API.
- **#8** E2E integration harness — exists in
  `tests/unittests/test_gateway.py` / `test_runtime.py` (in-process real
  uvicorn broker + real EndpointRuntime over a real WebSocket; JSON+binary
  round trips, header passthrough, SSE, disconnect, timeout/backpressure).
  Residual gaps listed in the close comment; the notification-path test
  gap is tracked in #103 (see plan 103).
- **#38** psij on Frontier/Anvil — Frontier is supported and exercised
  (machine guide, amsc example, tunnels); Anvil never went beyond a config
  stub — fresh targeted issue if still wanted.

## Remaining, by priority

| # | Issue | Plan file | Why this rank |
|---|-------|-----------|---------------|
| 1 | #115 dispatcher: supersede tasks of same simulation | `115-dispatcher-supersede.md` | Active xGFabric need (data events outdating queued/running sims); dispatcher is the current research vehicle. Medium effort, all primitives exist. |
| 2 | #103 poller no-progress watchdog | `103-poller-no-progress-watchdog.md` | Reliability class-fix; this failure mode already burned the AMSC launch path (silent 30-min hangs). Partial (exception watchdog landed); no-data half + transport test missing. Small-medium. |
| 3 | #105 per-plugin options replace env vars | `105-per-plugin-options.md` | Continuation of the config-surface cleanup; four env knobs now (PR #108 added one). Unblocks cleaner child-endpoint spawning. Medium. |
| 4 | #93 normalize job-state vocabulary | `093-normalize-job-states.md` | Small, low-risk, unlocks cross-plugin state logic + UI consistency. Good warm-up PR. |
| 5 | #11 plugin manager hardening | `011-plugin-manager-hardening.md` | Mostly done since filed; the real residual is "typo in --plugins kills the endpoint" + missing failure tests + reload. Small-medium, independently landable slices. |
| 6 | #32 packaging → pyproject only | `032-packaging-pyproject.md` | Partially done (py>=3.10, RP already optional, plugin selection exists); remaining is the mechanical conversion + extras. Watch the wrapper-script + VERSION derivation. |
| 7 | #13 xgfabric e2e demo (happy+failure) | `013-xgfabric-e2e-demo.md` | Ties into active xGFabric work; gives the collaboration a repeatable check. Harness patterns exist. |
| 8 | #10 xgfabric walkthrough + payloads | `010-xgfabric-walkthrough.md` | Docs-heavy; one plugin reference page serves #10/#13/#20. Issue vocabulary is pre-flip — re-scope in current API terms. |
| 9 | #20 xgfabric multi-resource example | `020-xgfabric-multi-resource.md` | Capability committed (classify/migrate); example+docs missing. Pairs with #13. |
| 10 | #18 routing policy v1 (tags + failover) | `018-routing-policy-v1.md` | Largest item; protocol change. Split into 3 PRs; PR1 (tags in handshake/topology) is cheap and independently useful. |
| 11 | #41 service logs in portal | `041-portal-service-logs.md` | Nice-to-have observability; clean small design (ring buffer + `service_log` plugin). Also: Explorer's "broker jobs" panel calls routes that don't exist — clean up in the same pass. |
| 12 | #6/#16/#21 ROSE track | `006-016-021-rose-track.md` | Blocked on a scope decision: `examples/example_rose.py` already runs ROSE over ORBIT as a consumer library — confirm the control-plane plugin is still wanted before building. That conversation may close the track. |

## Cross-cutting notes

- `plans/xgfabric_plan.md` (pre-existing, committed) is misnamed — its
  content is a Rhapsody/Dragon-V3 plan, not xgfabric. Worth renaming.
- `plans/broker_implementation_state.md` still lists #18/#16/#21/#6/#41 as
  open — consistent; update it when any of these land.
- The Q1 planning issues (#6–#21) all predate the star flip; their
  register/allocate/rid/aid vocabulary maps onto the current topology/config
  API — each plan restates the goal in current terms.
