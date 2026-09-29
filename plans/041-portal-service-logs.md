# Plan: #41 — broker and endpoint output/error streams in the portal

Issue: https://github.com/radical-cybertools/radical.orbit/issues/41
(pre-flip title: "include bridge and edge output/error streams in portal")
Status check (2026-08-25): **not done**. psij *job* stdout/stderr streaming
exists (Explorer job panels), but nothing exposes the broker's or an
endpoint's *own process* logs. `logging_config.configure_logging` can write
a log file but nothing serves it. Bonus finding: the Explorer's broker page
has a "broker jobs" panel calling `/endpoint/jobs`, `/endpoint/submit`,
`/endpoint/job/{id}/cancel` — routes that don't exist anywhere (UI falls
back to 501 stubs); clean that up in the same pass.

## Design

Treat "process output" as **the structured log stream**, not raw
stdout/stderr capture (services run under operators/batch systems that own
the real fds; the logging stream is what we control and it contains the
same information).

### 1. In-memory ring buffer at each participant

- `logging_config`: add a `RingBufferHandler` (bounded deque, say 2000
  records, formatted lines + level + ts) installed on the
  `radical.orbit` logger at configure time. Cheap, no disk dependency.

### 2. Expose as a tiny plugin, not a special surface

A `service_log` plugin (loads on broker and endpoints by default,
session-less like `sysinfo.homedir`):
- `get_log(offset, max_lines, min_level)` → `{lines, next_offset}` —
  offset-based tailing exactly like psij's stdout streaming
  (`stdout_offset` pattern), so the Explorer polling idiom is reused.
- Optional `topic='log'` notification stream for level ≥ WARNING (live
  errors surface in the UI without polling) — rate-limited, off by default.
- Because it's a plugin, the existing routing gives the portal broker logs
  (`/broker/service_log/...` via the broker's own participant identity) and
  every endpoint's logs (`/{endpoint}/service_log/...`) with zero gateway
  changes, token-gated like everything else.
- Security note: log lines can contain paths/hostnames — fine behind the
  ingress token; ensure the token itself is never logged (existing CWE-532
  discipline; add a test greping the ring for the token after an auth
  cycle).

### 3. Explorer

- Per-endpoint page + broker page: a collapsible "Service log" panel with
  level filter and follow-tail toggle, reusing the psij output-panel
  styles/JS (`.out-stream`/`.err-stream` classes).
- Remove or wire the dead "broker jobs" panel (separate small commit;
  removal is the cheap default — submitting jobs *from* the broker page is
  #107 psij-on-broker territory and now actually exists, so wiring it to
  `/broker/psij/...` may be a two-line fix — check first).

## Tests

- RingBufferHandler: bounded, format, level filter.
- Plugin: offset paging semantics (empty, wrap-around, min_level).
- Harness (test_gateway.py pattern): HTTP GET through gateway returns
  broker log lines; endpoint variant returns endpoint lines.
- Token-not-in-log test.

## Effort

Small-medium; self-contained. Nice property: first plugin exercising the
"loads on both broker and endpoint" path symmetrically.
