# 122 — An endpoint inside an allocation *is* the pilot

Status: revised after review round 2, 2026-09-08 -- ready to implement. Branch: `feature/atomic-federation`.
Depends on: 121 (class pools with members). Companion: ATOMIC `plans/09`.

## Problem

An allocation-mode resource (Perlmutter or Odo compute node, `atomic-join
--mode allocation`) already runs an Orbit endpoint on the allocation. Today
the task dispatcher still treats its one implicit member like a login-node
member: it submits a *second* endpoint (`fed-gpu_odo.default_p.<id>`)
through the first endpoint's `psij` plugin with the `local` executor, and
only that child runs tasks. Seen on 2026-09-08:

- the compute-node default plugin set has no `psij`, so no pilot launched
  until the join passed `--plugins default,psij`;
- the child inherited a broker-host cert path and a broker-host tool prefix
  (both fixed since), and every such host-specific detail has to be
  re-derived for a process the dispatcher does not need;
- the UI shows a `default` member row that users read as "what is this?";
- one extra process per resource, one extra registration, one extra
  liveness to track.

The endpoint that joined is the pilot. The dispatcher should adopt it.

## Design

### Member flag

`PoolMember` gains `pilot: "endpoint" | "submit"` (default `submit`) and
`end_time` (epoch or null). `parse_member` accepts both; `PoolConfig.to_dict`
persists them for replay. `_member_fingerprint` is `asdict(member)`, so
both are part of it automatically -- which means the first broker restart
after the upgrade replays a pool whose member says `submit` while the
federation re-POSTs `endpoint`: a one-time 409, after which
`_replay_attachments` marks the member lost and re-adds it. Upgrade step:
document "restart the broker with an empty dispatcher state dir" (the demo's
broker.sh does that anyway), or tolerate a fingerprint that differs only in
`pilot`/`end_time` by treating the POST as an update. The federation sets
`pilot: endpoint` on the implicit member of an allocation-mode resource; a
declared member may set it too. For such a member `min_pilots` and
`max_pilots` are both forced to 1 (adoption is driven by the floor step;
`min_pilots = 0` would never adopt until backlog; `max_pilots_total` stays
a plain sum).

### Adopt through the existing topology path, not by hand

`ACTIVE` is set today by `on_topology_change` → `_reconcile_pilots_for`
(`plugin_task_dispatcher.py` ~2469-2499): a record whose
`child_endpoint_name` is *present* goes through `_activate_pilot` (capacity,
`active_at`, `policy.on_pilot_state`, drain). An adopted pilot rides the
same path:

- `_submit_pilot` for a `pilot: endpoint` member creates the record
  `PENDING` with `child_endpoint_name = endpoint_name = member.endpoint_name`,
  `psij_job_id = None`, and does **not** schedule `_do_pilot_submit`;
- if the endpoint is in `self._connected_endpoints` right now, call
  `_activate_pilot` immediately; otherwise leave it `PENDING` for the next
  topology delivery;
- a `PENDING` adoption older than `_HANDSHAKE_TIMEOUT_SEC` is failed with
  reason `endpoint <name> not connected` (today `_reconcile_pilot` returns
  early for records without a psij job id, so it would otherwise sit forever
  and count as an in-flight submission in the strategy's guards and the
  pool ceiling);
- a second `_submit_pilot` on the member while a record is PENDING/ACTIVE
  is a no-op with a warning.

Hand-setting `ACTIVE` is out: it would bypass the capacity guard and the
policy notification.

### Liveness, in the dispatcher's existing terms

There is no `LOST` pilot state. The topology hook already handles the three
cases for a child endpoint name, and they apply unchanged to the adopted
name:

- *suspect* (broker-side grace window): pilot paused,
  `accepting_new_tasks = False`;
- *lost*: `_mark_pilot_failed(... 'child endpoint lost before walltime')`,
  or DONE if past `walltime_deadline`; queued tasks requeue. The
  federation's `_sync_attachments` then `del_member`s the member;
- *present again*: the federation re-adds the member; next tick,
  `min_pilots = 1` → a fresh adoption (new pilot id).

No new subscription mechanism. One rule, applied at both places where an
adopted pilot can end: **an adopted pilot (record without a psij job id)
that ends because its endpoint is gone or its member is removed is marked
DONE, never FAILED**, regardless of the deadline -- a leave or an
allocation ending is not a failure and must not feed the member's failure
counter or `pilot_error`. The two entry points are the topology `lost`
branch (`_reconcile_pilots_for`, which today chooses FAILED before the
deadline) and `del_member` → `_do_pilot_cancel` (which today stamps
FAILED 'cancel requested' for a record without a job id); the federation
and dispatcher hooks run in plugin-host order, so either may fire first.
`_last_pilot_error` stopping at the newest pilot that reached ACTIVE is
already in (surfacing commit 9d69484).

### Deadline

`walltime_deadline` is respected only by the *lost* branch today;
`pick_dispatch` merely sorts by it. Add strategy config `min_remaining_sec`
(default 120): `pick_dispatch` skips a pilot with
`walltime_deadline - now < min_remaining_sec`, and `on_tick`'s
`free_capacity` sum excludes it so a near-deadline pilot does not suppress
growth. A pilot past a mis-estimated deadline stays ACTIVE until its
endpoint disappears; that is current behaviour and acceptable.

### Remaining time comes from the allocation, not the broker

`_allocation_walltime` reads `SLURM_JOB_END_TIME` from the **broker's**
environment; wrong for a remote allocation, and that variable needs Slurm
≥ 23.02 anyway. Instead `queue_info/job_allocation` reports `end_time`
(epoch) computed inside the allocation as `now + time_left`: Slurm adds
`%L` (time left, `D-HH:MM:SS`, parsed by the existing `_parse_slurm_time`)
to the `squeue` call already made in `SlurmBatchSystem.job_allocation` --
not `%e`, which prints local wall-clock text; PBS uses `Walltime.Remaining`
(seconds) from `qstat -f` when present, else `stime + Resource_List.walltime`
with `strptime` on the ctime text. The federation sets the member's
`walltime_sec = end_time - now` at join (400 if ≤ 0: no 1-second pilots;
`PilotSize.walltime_sec` must be ≥ 1) and exposes `remaining_sec`
recomputed on every read. No `end_time` → `walltime_sec = runtime` as
today, `remaining_sec = null`.

`end_time` is also stored on the member (`PoolMember.end_time`,
`MemberRecord.end_time`, absolute epoch, fingerprint-stable), because a
member is re-POSTed with the join-time `walltime_sec` on every re-attach
and `_submit_pilot` would otherwise give a re-adopted pilot a deadline past
the allocation's end. `_submit_pilot` uses `min(now + walltime_sec,
end_time)` when `end_time` is set. For a submit-mode member `remaining_sec`
is the max over its live pilots of `walltime_deadline - now` (pilot history
carries `walltime_deadline`), null when it has none.

### Payload: add, never rename

Existing wire names stay: `member` (short name), `pool_name`, `walltime_sec`,
`attributes.mem_gb_per_node`, `usage.*`; `mode` and `endpoint` exist at the
resource level. New per member: `endpoint` (the member's endpoint name),
`pilot` (`endpoint`/`submit`), `remaining_sec`, and the state value `idle`.
`shape` may be added as an alias of `member`, never as a replacement —
`federation.js`, ATOMIC's `atomic_campaign.js`, `smoke.py` and their tests
read `member`.

State per member (`MemberRecord.state()`, already the derivation point
since 9d69484): `lost`, `failing`, `stale`, `idle` (ok, `pilots_active ==
0`, no recorded failure), `ok`. An adopted member is `failing` only on a
recorded failure (`pilot_error` set), never merely because no pilot is
ACTIVE yet -- the join→first-tick window and `_activate_pilot`'s
zero-capacity return would otherwise flash `failing`. A member with a
recorded failure still below the failure threshold reads `ok` -- neither
`idle` nor `failing`. `MemberRecord` gains
`pilot`, `endpoint` and `end_time` (touch `member_from_dict`,
`_derive_member`, `_implicit_member`, `_declared_member`, `_member_decl`,
`to_wire`). Resource state: the worst of its members' with `idle` ranked *below*
`ok` -- lost > failing > suspect > stale > ok > idle -- so a resource with
one busy shape and one idle shape is `ok`, and `idle` at resource level
means every member is idle; the resource row keeps the record's own task
counts (they include unplaced tasks; member counts are placed only).
`federation.js` gets CSS for `idle` (ATOMIC's `statusCell` maps unknown
values to "unknown", so the alias must land on both sides together).

### Restart

`_replay_state` reloads the ACTIVE adopted record with its absolute
`walltime_deadline`; the first topology delivery reconciles it;
`_replay_attachments` re-POSTs the member (no-op) and `live_pilots_for`
already counts 1, so `min_pilots = 1` does not double-adopt; `active_at` is
kept, node-hour charging is continuous. Test it.

### Teardown

`_do_pilot_cancel` already skips psij for a record without a job id;
`del_member` needs no special case beyond the DONE-not-FAILED stamping
above. The federation's leave stops the endpoint, as today.

## Non-goals

Multi-node use of an adopted allocation (rhapsody stays `concurrent` on the
head node); reservation/pinning (still deferred, see 120 notes).

## Tests

- adoption: allocation member with a connected endpoint → ACTIVE at once
  through `_activate_pilot`, zero psij submits (fake psij counts), tasks run
  through the endpoint's fake rhapsody, staging put goes to the endpoint's
  staging plugin; endpoint not connected → PENDING, then ACTIVE on topology
  delivery; still absent after `_HANDSHAKE_TIMEOUT_SEC` → FAILED with the
  reason;
- second `_submit_pilot` is a no-op; `max_pilots` forced to 1;
- suspect → paused; lost → requeue + DONE (adopted: never FAILED); member
  re-add → new adoption with a deadline capped by `end_time`;
- leave → pilot DONE, no failure counted, `pilot_error` null;
- declared member with `pilot: endpoint` → adopted at the first tick
  (`min_pilots` forced to 1);
- `min_remaining_sec`: no placement and no capacity credit near the end;
- `job_allocation.end_time` on Slurm (`%L` parsing, fixture incl.
  `UNLIMITED`) and PBS (`Walltime.Remaining` and the fallback); join 400
  when ≤ 0; `end_time` round-trips through `parse_member`/`to_dict`;
  `remaining_sec` decreases between two reads; submit-mode max-over-pilots;
- payload: new fields present, old names unchanged (assert `member` and
  `pool_name` still there), `idle` derivation, resource worst-of;
- `parse_member` accepts `pilot`; fingerprint and `to_dict` round-trip;
- replay of an adopted ACTIVE pilot after a plugin reload;
- Explorer federation tab render on the new payload (node harness),
  including the `idle` style.

## Rollout

1. Dispatcher adoption + deadline + tests.
2. `job_allocation.end_time` (Slurm, PBS) + federation walltime/remaining.
3. Payload fields + `federation.js` layout (ATOMIC plan 09 does
   `atomic-resources` and `atomic_campaign.js` on the same payload).
4. ATOMIC demo: drop `--plugins default,psij` from the allocation joins once
   1 is in the pinned branch.
