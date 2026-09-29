# Plan: #13 — xGFabric end-to-end scripted demo (happy path + failure cases)

Issue: https://github.com/radical-cybertools/radical.orbit/issues/13
Status check (2026-08-25): **mostly not done**. `examples/xgfabric.py` covers
an interactive happy path over HTTPS/WSS (self-signed cert supported, exits
non-zero on failed workflow), and `data/xgfabric_workflow_test.json` provides
a mock-sensor workflow that runs without CSPOT. But there is no repeatable
unattended demo, no scripted failure case, and zero xgfabric coverage in
tests/integration/.

## Goal restated in current vocabulary

A single driver that stands up broker + endpoint(s), runs the test workflow
to completion, probes the error paths, and exits 0/1 — usable both as a demo
(verbose) and as an integration test (quiet, assert-driven).

## Design

1. **Driver**: `tests/integration/test_xgfabric_e2e.py` (pytest) +
   a thin `examples/xgfabric_demo.sh` that invokes the same flow verbosely.
   Reuse the proven in-process harness from
   `tests/unittests/test_gateway.py` (`_RunningBroker`, real uvicorn on a
   random port + real `EndpointRuntime`) rather than subprocesses —
   subprocess spin-up is what makes `tests/integration/test_notifications_local.py`
   integration-only; in-process keeps it CI-runnable. If a subprocess
   variant is wanted for demo fidelity, the shell wrapper provides it.
2. **Happy path** (maps the issue's register→list→describe→allocate→submit→
   state/cancel onto the current API):
   - endpoint connects → topology shows it classified (immediate vs
     allocate) — the "register/describe" equivalent;
   - `list_configs` / `load_config('test')` — "list/describe";
   - `start_workflow(test_workflow, test_resource)` with
     `mock_sensor_data: true` — "submit";
   - follow `workflow_status` notifications to `completed` — "state";
   - second run interrupted by `stop_workflow` — "cancel".
3. **Failure cases** (≥1 required; do these four, they're one line each):
   - unknown session id → 404 (shared envelope);
   - `load_config('nope')` → 404 Config not found;
   - `start_workflow` while running → 409;
   - `stop_workflow` with none running → 409.
4. **HTTPS/WSS**: parametrize the harness with a generated self-signed cert
   (the endpoint already pins the broker cert — reuse whatever
   test_runtime.py's TLS-pin tests generate) so the demo satisfies the
   issue's "run via HTTPS/WSS" clause; plain-HTTP fallback for speed in CI.
5. Mark with a `@pytest.mark.integration`-style gate only if runtime is
   long; target < 60 s with the mock workflow so it can live in CI.

## Deliverables

- `tests/integration/test_xgfabric_e2e.py` (or unittests/ if fast enough).
- `examples/xgfabric_demo.sh` wrapper (broker + endpoint + client, verbose).
- README snippet or doc pointer from `docs/source/plugin_xgfabric.rst` (#10).

## Effort

Medium — the harness exists; the work is workflow-runtime wrangling (the
mock workflow's task execution path on a bare test box) and the TLS
parametrization. Depends loosely on #10 (shared doc page); independent of
#20.
