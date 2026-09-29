# Plan: #103 — status pollers can go silently blind: no-progress watchdog

Issue: https://github.com/radical-cybertools/radical.orbit/issues/103
Status check (2026-08-25): **partially addressed**. The shared poller
(`plugin_session_base.start_status_poller`, line 81) has since gained
`max_failures` — a WARNING after N consecutive `fetch` **exceptions**. That
covers the "poller keeps throwing" case, but NOT the issue's case: `fetch`
returning `None` ("no change") forever — permanent blindness that looks like
a healthy cadence. The transport-level notification-delivery test asked for
in the issue also does not exist (tests/integration has only
notifications_local + rhapsody files; nothing covering broker
`_host_broadcast` → EventRouter → subscribed WS participant).

## Part 1 — no-data watchdog in the shared poller

In `start_status_poller`:

- Track per-key consecutive no-data sweeps: increment when `fetch` returns
  `None`, reset on any truthy result (state change) **and** on item removal
  / terminality.
- New kwargs (defaults preserve current behavior for all callers):
  - `max_no_data: int = 30` — sweeps of silence before reacting (at the
    typical 10 s interval ≈ 5 min, matching the issue's suggestion).
  - `on_blind: Optional[Callable[[key, item], Optional[dict]]] = None` —
    called once when a key crosses the threshold (and again every
    `max_no_data` thereafter, so a permanently blind poller stays visible in
    the log without spamming every sweep).
- Behavior at threshold:
  1. `log.warning` naming the poller (`name`/`topic`), the key, and the
     silence duration — unconditionally.
  2. If `on_blind` returns a dict, emit it as a `topic` notification —
     the synthetic `state: 'unknown'` event from the issue. Making the
     payload caller-supplied keeps the poller generic (psij/globus/iri
     payload shapes differ) and makes the synthetic event **opt-in per
     plugin**, so existing consumers don't suddenly see unknown-state
     events unless the plugin chooses to surface them.
- Subtlety from the issue: "previously seen" vs "never appeared since
  submit" — with the counter keyed per item and reset on truthy fetch, both
  cases converge (a job that never appeared just counts from its first
  sweep). No extra state needed.

Wire `on_blind` into the SFAPI/IRI instance pollers first (the class that
motivated the issue): synthesize
`{job_id, state: 'unknown', error: 'no status data for <n>s via <route>'}`.
psij/globus: WARNING only for now (their backends are local; blindness is
less plausible), can opt in later.

## Part 2 — transport-level test: hosted-plugin notification delivery

The untested path: broker-hosted plugin session `_notify` →
`Broker._host_broadcast` → EventRouter → subscribed WS participant
(`EndpointRuntime` callback). A working reproduction existed in the #102
debugging session; recreate with the in-process harness pattern from
`tests/unittests/test_gateway.py` (`_RunningBroker`, line 99):

- Boot a real broker with a tiny hosted test plugin, connect a real
  `EndpointRuntime` consumer, `register_notification_callback`, have the
  hosted session `_notify`, assert the callback fires with
  endpoint='broker', correct plugin/topic/data.
- Add one case where the consumer subscribed via topic pattern, and one
  where an unsubscribed consumer does NOT receive it.
- Place in `tests/unittests/` next to the existing harness tests (they run
  in CI; `tests/integration/` does not — see .github/workflows/pr.yml).

## Tests for Part 1 (pure-asyncio unit tests, mock fetch)

- Fetch returns None `max_no_data` times → exactly one WARNING + one
  synthetic notification (with `on_blind` set); counter resets on a real
  result; re-fires after another full window.
- `on_blind=None` → WARNING only, no notification.
- Interplay with `max_failures`: exceptions and no-data are separate
  counters; an exception sweep does not reset the no-data counter.
- Terminal item → counters dropped, no late blind-fire.

## Effort

Small-medium, one PR. Poller change is ~40 lines; the transport test is the
larger half but reuses the existing `_RunningBroker` harness.
