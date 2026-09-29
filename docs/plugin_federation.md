# Federation Plugin

The **task dispatcher** answers *"run this task on that pool"*. The
**federation** answers the question one level up: **which resource should
run it at all?**

It is a broker-hosted plugin that keeps a registry of joined resources and
their **members**, adds every member to the dispatcher pool of its
**capability class**, derives node-hour usage per member from the
dispatcher, and picks a *class* for a set of requirements through a
pluggable policy — the dispatcher then picks the member, at dispatch.

Nothing in it is domain-specific: a "resource" is whatever an operator
joined, and a "requirement" is a plain capability key.

```
   client / workload manager
             │  POST /broker/federation/submit/default
             ▼
      ┌──────────────┐  pick_class(requirements)  ┌──────────────────┐
      │  federation  │◀──────────────────────────▶│ FederationPolicy │
      └──────┬───────┘                            └──────────────────┘
             │  in-process handle_request
             ▼
      ┌──────────────┐   ONE session `fed`, one pool per capability class
      │  dispatcher  │──▶ fed-cpu ── members ─┬─ local_a.default
      └──────┬───────┘                        ├─ local_b.cpu
             │                                └─ local_c.cpu
             │              ──▶ fed-gpu ── members ─┬─ local_b.gpu
             │                                      └─ local_c.gpu
             │  psij submit_tunneled
             ▼
          pilots ──▶ child endpoints ──▶ rhapsody
```

## Concepts

### Resource and member

A **resource** is one joined site. A **member** is one *shape* that
resource is willing to run: one queue, one pilot size, its own software,
attributes and node-hour budget. Everything about both is **declared at
join time** — there is no discovery protocol and no schema change anywhere
else in Orbit.

```
resource  local_b  ── members ──┬── local_b.cpu   class cpu  → pool fed-cpu
                                └── local_b.gpu   class gpu  → pool fed-gpu
resource  local_a  ── members ──── local_a.default  class cpu → pool fed-cpu
```

- **member short name** matches `^[a-z0-9][a-z0-9_-]*$` — **no dot**. A
  resource name may contain dots, so the dot in a member id is
  unambiguously the separator: split with `rpartition('.')`, never
  `partition`.
- **member id** = `<resource>.<member>`, e.g. `local_b.gpu`. This is the id
  the dispatcher sees, and it is unique across the federation.
- **class** = the member's declared `class`, else `gpu` when its
  `gpus_per_node > 0`, else `cpu`. The derivation is a *default*, so new
  classes need no code change. A declared class must match
  `^[a-z0-9][a-z0-9_-]*$`; one that does not is a **400**, never
  lower-cased — silently accepting `GPU` would create a second, invisible
  `fed-GPU` pool.
- **pool name** = `fed-<class>`, created on first use, shared by every
  resource that declares the class, and left in place (inert) when its last
  member leaves.
- **dispatcher session** = the single constant `fed`.

```json
{
  "name": "bridges",
  "endpoint": "ep_br",
  "mode": "login",
  "site": "PSC",
  "kind": "hpc",
  "members": [
    {"member": "cpu", "queue": "RM", "account": "abc123",
     "nodes": 1, "cpus_per_node": 128, "walltime_sec": 3600,
     "max_pilots": 2, "software": ["lammps"],
     "attributes": {"site": "PSC", "mem_gb_per_node": 256},
     "budget": {"node_hours": 20}},
    {"member": "gpu", "queue": "GPU", "account": "abc123",
     "nodes": 1, "cpus_per_node": 64, "gpus_per_node": 8,
     "walltime_sec": 3600, "max_pilots": 1, "software": ["pytorch"],
     "class": "gpu", "budget": {"node_hours": 8}}]
}
```

Resource fields:

| field | meaning |
|----|----|
| `name` | unique in the federation; matches `^[a-z0-9_.-]+$` (it rides in URLs and in every member id) |
| `endpoint` | the connected participant that serves this resource — must be in the current topology |
| `mode` | `allocation` or `login` (see below) |
| `site`, `kind` | free text, for display |
| `capabilities` | the **aggregate view** of the members (see below); still accepted and still returned |
| `budget` | `{"node_hours": <float>}` — the aggregate of the member budgets |
| `scratch_base` | optional; defaults to `<state root>/scratch/<name>`. With `shared_fs` (the default) it must lie under `~` or `/tmp` — the rule the `staging` plugin enforces, since the broker writes that tree. With `shared_fs: false` it names a path on the **resource's** host and only has to be absolute (or `~`-prefixed, expanded there): the broker neither resolves nor creates it. A member that declares none inherits it |
| `shared_fs` | optional bool, default `true`: does the broker host see `scratch_base`? `false` for a resource on another machine — task inputs then travel through the pilot's own `staging` plugin. A member that declares none inherits it |
| `members` | login mode only — one entry per shape (see below) |
| `pool` | login mode only, and only **without** `members` — the flat batch declaration. Sending both is a **400**: one of the two would be silently ignored |

Member fields: `member` (required), `queue` (required, not the literal
`"default"`), `account`, `nodes`, `cpus_per_node`, `gpus_per_node`,
`walltime_sec`, `min_pilots`, `max_pilots`, `rhapsody_backend`,
`scratch_base`, `shared_fs`, `software`, `class`, `attributes`, and
`budget` (required — with members, the budget lives here). Unknown keys are
**rejected**, per member, with the member named in the message.

`pilot` is `submit` (the default) or `endpoint`. **`endpoint` means the
endpoint that joined already runs inside a compute allocation and *is* the
pilot**: the dispatcher adopts it instead of asking psij for a second
process on it, and both `min_pilots` and `max_pilots` are forced to 1,
because there is exactly one of it. An `allocation`-mode resource's single
member is always `endpoint`; a `login`-mode member may declare it too.
`end_time` is **not** declarable — only the allocation knows when it ends,
and only `allocation` mode reads that from the endpoint.

`attributes` is a free-form `{str: str | number | list[str]}` map — `site`
and `mem_gb_per_node` are conventions, not schema — and it is what a task's
`labels` requirement is matched against. `software` is folded into
`attributes.software` on the way to the dispatcher, which knows no
federation vocabulary.

Server-filled on the returned record: `joined_at`, `dispatcher_sid`
(always `fed`), `pool_name`, `usage`, `liveness`, `state`, and per member
`member_id`, `class`, `pool_name`, `endpoint`, `end_time`, `remaining_sec`,
`usage`, `liveness`, `state`.

`remaining_sec` is a **countdown**, recomputed on every read and never
stored: `end_time − now` for a member that has an allocation of its own,
otherwise the most walltime any of its live pilots still has (the
dispatcher's number — a pilot's deadline is dispatcher state), and `null`
when it holds none. It is never negative.

Wire names are **added, never renamed**: `member`, `pool_name`,
`walltime_sec`, `attributes.mem_gb_per_node` and `usage.*` mean exactly what
they did, because `federation.js`, ATOMIC's `atomic_campaign.js` and
`smoke.py` read them.

**The resource-level view is an aggregate.** `capabilities.cores` = Σ
`nodes × cpus_per_node`, `capabilities.gpus` = Σ `nodes × gpus_per_node`,
`capabilities.software` = the union, `budget.node_hours` = Σ member
budgets, `usage` = the sum over the members. Every other declared
capability (`mem_gb` and friends) is left exactly as declared. That keeps
every pre-existing consumer rendering while the member view is added
alongside.

**A record without `members`** — one written before class pools, or a join
body in the flat form — derives exactly one member named `default` from its
stored pool declaration, with its class from `gpus_per_node`, its software
from `capabilities.software`, and its budget from the resource's. So there
is one shape at runtime and two on the wire. Both derivation paths build
their attribute map through one helper (`federation_state
.resource_attributes`), which **drops empty and `None` values**: the
dispatcher's member parser accepts a string, a number or a list of strings,
and since every registration re-sends the full member list, one `None`
anywhere in the state would fail every later join and every restart replay.

**Member id length.** `<pool_name>` plus `<resource>.<member>` must be at
most **64** characters — the dispatcher builds a broker participant name
out of both halves and refuses a longer declaration. The join checks it
before touching the dispatcher, so it is a plain **400** rather than a
half-completed join.

### Join modes

**`allocation`** — the endpoint runs *inside* a compute allocation, and the
whole allocation **is** the resource. The pool is built with
`queue: "allocation"`, `account: null`, `min_pilots: 1`, `max_pilots: 1`,
`pilot: "endpoint"`, and one pilot size taken from the endpoint's own
`queue_info/job_allocation` when it has one, else `nodes=1,
walltime_sec=3600`.

**The endpoint that joined is the pilot.** It already runs inside the
allocation, so the dispatcher adopts it: no second endpoint is submitted
through its `psij` plugin, no second registration, no second liveness to
track, and no host-specific detail (cert paths, tool prefixes, a `psij` in
the plugin set) has to be re-derived for a process nobody needs. What the
dispatcher does instead is create the pilot record `PENDING` with that
endpoint as its own child and activate it the moment the topology carries
it — the same path a submitted pilot's child takes.

Sizing, in order of preference:

| pilot size | from |
|----|----|
| `nodes` | `n_nodes` of the allocation, else 1 |
| `walltime_sec` | `end_time − now` of the allocation, else its `runtime` (the *limit*), else 3600 |
| `cpus_per_node` | the allocation's `cpus_per_node`, else `max(1, capabilities.cores // nodes)` |
| `gpus_per_node` | the allocation's `gpus_per_node`, else `capabilities.gpus // nodes` |

`end_time` is an epoch the **endpoint** computes inside its own allocation
(`queue_info/job_allocation`: `squeue %L` on Slurm, `Walltime.Remaining` —
else `stime` + the walltime limit — on PBS), because `runtime` is the job's
*time limit* and a resource joined an hour in would otherwise give its pilot
a walltime longer than the allocation itself. An allocation that has **no
time left** is a **400** rather than a one-second pilot: `walltime_sec` must
be at least 1, and a pilot that cannot outlive its own submission is not a
resource. `end_time` is also stored on the member, absolute, and caps every
pilot deadline the dispatcher sets for it — a member is re-declared with its
join-time `walltime_sec` on every re-attach, so a re-adopted endpoint would
otherwise be given a deadline past the allocation's end.

Note the division: a declared `cores` / `gpus` is a **total for the
resource**, while `cpus_per_node` is exactly what its name says. The
allocation's own per-node figures are authoritative when it reports them.

Because `min_pilots` is 1, **the pilot is live at join** — before any task
exists — so the resource is warm by the time work arrives. An undeclared
budget defaults to `nodes × walltime_sec / 3600`: the allocation itself.
Declaring `members` in allocation mode is a **400**: the allocation *is* the
resource, so it has exactly one member, `default`.

**`login`** — the endpoint sits on a login node and declares what it may ask
the batch system for. Pilots are submitted on demand (`min_pilots: 0`), and
`budget.node_hours` is **required** — nothing else bounds what the resource
would spend.

With `members`, each entry carries its own budget and its own size, and the
resource-level `budget` is their sum (send one or not; it is recomputed).
Without `members`, the flat `pool` block still works exactly as it did and
becomes the resource's single `default` member:

```json
"pool": {"queue": "regular", "account": "m1234",
         "nodes": 1, "cpus_per_node": 128, "gpus_per_node": 4,
         "walltime_sec": 3600, "min_pilots": 0, "max_pilots": 2,
         "rhapsody_backend": "concurrent"}
```

`queue`, `nodes`, `cpus_per_node` and `walltime_sec` are required;
`account` may be `null`; `gpus_per_node` (0), `min_pilots` (0),
`max_pilots` (1) and `rhapsody_backend` (`concurrent`) have defaults.
`queue` must not be the literal `"default"` — that is the dispatcher's
unconfigured-pool sentinel and it refuses to submit for it.

Both forms are validated **strictly**: every count must be an integer in a
sane range (`min_pilots ≤ max_pilots`), and an **unknown key is rejected**
rather than ignored — a typo that silently drops `max_pilots` is worse than
a refused join. Every violation is a **400**, never a 500 raised deeper in,
and a member's message names the member (`'member gpu.nodes' must be >= 1`).

**A join is all-or-nothing.** If adding member *k* to its class pool fails,
members 0..k−1 are removed again — with `force`, because the first member of
a brand-new class pool is also its last — and the dispatcher's status and
detail are returned.

### Capability discovery

Declared capabilities always win — an operator carving a slice out of a big
machine must be able to say so. Discovery only answers *"the operator did
not say"*: at join, missing `cores` / `gpus` / `mem_gb` are filled from the
endpoint's `sysinfo` metrics (a session is registered, read, and
unregistered, leaving nothing behind). Every discovery call is best-effort;
an endpoint without `sysinfo` or `queue_info` still joins.

### Usage and node-hours

Usage is now **per member**, and read off the dispatcher rather than
recomputed here: one `pool_detail` call per **distinct class pool** (so N
resources in two classes cost two round-trips, not N) yields a per-member
block with `node_hours_used`, `node_hours_remaining` and `pilots_active`.
The Explorer, the CLI and the federation therefore read one number instead
of each re-implementing the arithmetic.

The arithmetic the dispatcher applies is unchanged:

```
node_hours_used = Σ over pilots that reached ACTIVE:
                    nodes × ((finished_at or now) − active_at) / 3600
```

A pilot that never reached `ACTIVE` contributed nothing; a live one is
measured against *now*, so the number ticks while a pilot holds its
allocation; a finished one stops at its `finished_at`. There is no
accumulator to drift, and the number is correct across a broker restart.
A summary that carries no per-member block falls back to this member's own
slice of the pool history (matched on `member_id`).

The resource row is the **sum over its members**, except the task counts:
a task the dispatcher has not placed yet belongs to no member and would
vanish from a sum, so resource-level counts come from the ledger by
resource and member-level ones from the ledger by `member_id`.

Task counts come from the federation's **own submit ledger**, one entry per
task it ever routed — the dispatcher's `recent_tasks` is capped at 50 per
pool and would silently undercount a long run.

Usage is refreshed on `resources`, `resource`, `pick` and `submit`, cached
for 2 s,
with a 3 s timeout. A refresh that cannot reach the dispatcher keeps the
previous numbers and sets `"stale": true` — a member must not blink to zero
because one poll timed out.

A member's `usage` also carries **why it has no pilots**, straight off the
dispatcher's per-member block: `pilot_error` (the reason its most recent
pilot went FAILED, e.g. `psij error: … [Errno 122] Disk quota exceeded`,
truncated to 300 chars), `pilot_failures` (how many died in a row) and
`paused_until` (epoch, or `null` — the policy's failure backoff). The
resource row shows the worst of its members: the first error there is one,
the highest count, the furthest pause. All three clear as soon as the
member produces a healthy pilot again.

The same block carries `remaining_sec` — the most walltime any of that
member's live pilots still has — which is where a submit-mode shape's
countdown comes from. A member with an allocation of its own answers from
its `end_time` instead, so its countdown is right even before its pilot
exists.

### Liveness and state

A resource and every one of its members inherit the endpoint's topology
liveness: `present` → `ok`, `suspect` → `suspect`, anything else → `lost`.
The default policy routes to `ok` members only — a `suspect` endpoint may be
seconds from `lost`, and a task sent there would sit behind a member nobody
is serving.

Alongside `liveness` every record carries a derived **`state`**, which is
what a human is shown. Per member, in precedence order:

| state | means |
|----|----|
| `lost` / `suspect` | the endpoint's own liveness, passed through untouched |
| `failing` | reachable (`ok`), holding no pilot, and either `paused_until` is in the future or `pilot_failures >= 3` |
| `stale` | the last usage refresh could not reach the dispatcher, so the numbers shown are the previous ones |
| `idle` | reachable, holding no pilot, **and no recorded failure** — the join-to-first-pilot window, an adopted endpoint waiting for its first tick, or a login shape with an empty queue |
| `ok` | holding at least one pilot |

A member is `failing` only on a **recorded** failure; it must never flash red
merely because no pilot is ACTIVE yet. A resource shows the **worst** of its
members' states (`lost` > `failing` > `suspect` > `stale` > `ok` > `idle`):
one shape that cannot start a pilot is enough for the row to say so, while
one shape resting beside a working one does not make the machine idle — a
resource reads `idle` only when every shape of it does.

`liveness` is deliberately left alone — it means "can we reach the
endpoint", the routing policy is written against it, and a site whose quota
is full is not gone. A client that knows only `liveness` sees exactly what it
always did.

Attachment is tracked **per member**, so a resource whose two members live
behind one endpoint has each tracked separately (and one can fail to attach
while the other succeeds):

| endpoint | action |
|----|----|
| `present`, member not attached | `add_member` — re-declaring its class pool first when that pool no longer exists (every member of the class may have left while the endpoint was down) |
| `suspect` | mark `suspect` and **do nothing else**. The policy already refuses to route there; a blip must not touch the dispatcher |
| `lost`, member attached | `del_member` with `fail_unsatisfiable: false`. Its pilots died with the endpoint anyway and its RUNNING tasks re-queue onto a sibling — but a task only *this* member could run stays QUEUED instead of failing, because a lost endpoint is very often back in a minute |

### Policy

`FederationPolicy` (`federation_policy.py`) picks a **class**, not a site:

```python
pick_class(requirements, {class: [members]}) -> (class, score) | None
eligible(requirements, members)              -> [(member, score), …]
explain(requirements, members)               -> {member_id: reason}
```

They are called with the same members and must agree on what they reject: a
caller that got `None` from `pick_class` calls `explain` to tell the client
*why*. There is **no compatibility wrapper** for the old
`pick(requirements, resources)` / `explain(requirements, resources)` pair —
it is replaced, not kept beside the new API, because two parallel entry
points would guarantee they drift. An out-of-tree policy that still
implements the old shape fails at the first route.

The default **`BudgetLoadPolicy`** filters members, scores them, then picks
a class:

- **liveness** must be `ok`;
- **shape and software** are matched by the *same* matcher the dispatcher
  uses at dispatch (`software` ⊆ the member's, `cores`/`gpus` **per node**
  against its pilot size, `mem_gb` against `attributes.mem_gb_per_node` when
  declared, `labels` against the attribute map, `mpi` refused on a
  `dragon_v1` backend; every other key, `ranks` included, ignored). So the
  federation and the dispatcher agree by construction, and their reasons
  read alike;
- **budget**: the requested `node_hours` must still be available **on that
  member**;
- **score** = `remaining_budget_fraction − load`, where `load` is
  `tasks_running / (nodes × cpus_per_node)`. Higher wins; ties break
  deterministically on the `member_id`;
- **class choice**: among the classes with at least one eligible member, the
  one whose *cheapest* eligible member is cheapest overall, where cheap is
  `(gpus_per_node, cpus_per_node, class name)`. So a CPU task that also
  happens to fit a GPU member lands in `fed-cpu` and never burns a GPU
  allocation while a CPU one is free.

> **Semantic change worth stating.** Before class pools, `cores` was
> compared against the resource's *total* declared capability, which was
> laxer than the dispatcher. It is now compared **per node** against the
> member's pilot size, and `mem_gb` against
> `attributes.mem_gb_per_node`. A federation that used to accept a
> 64-core task for a 4×16-core resource now refuses it — as the dispatcher
> always would have.

Point the plugin at your own class with a `module:Class` spec:

```python
PluginFederation(app, policy='mysite.policies:NearestFirst',
                 policy_config={'home_site': 'NERSC'})
```

Policies **choose only**: they never mutate a record, talk to the
dispatcher, or perform I/O.

## Routes

All routes live under `/broker/federation/…` through the gateway, and all
use the reserved **`default`** session — federation state is global to the
plugin instance, and a client-minted session would be swept after an idle
hour. Clients never register a session of their own.

| Method | Path | Description |
|----|----|----|
| `POST` | `join/{sid}` | Join a resource. Body = the client fields of a resource record, with an optional `members` list. Returns the full record, members and all. |
| `POST` | `leave/{sid}/{name}` | Remove each member from its class pool. Optional body `{"cancel_tasks": false}`. Returns `{resource, ok, members_removed, tasks_requeued, tasks_failed, tasks_cancelled}`, plus `errors` (and `ok: false`) when a member could not be removed. |
| `GET` | `resources/{sid}` | `{"resources": [record, …]}`, usage refreshed, sorted by name. |
| `GET` | `resource/{sid}/{name}` | One record, usage refreshed. |
| `POST` | `pick/{sid}` | Body `{"requirements": {...}}` → `{class, pool, dispatcher_sid, members, resource, score}`. |
| `POST` | `submit/{sid}` | Body `{"task": {...}, "requirements": {...}}` → `{task, pool, class, dispatcher_sid, resource, member, members_eligible}`. |
| `GET` | `task/{sid}/{task_id}` | The dispatcher task record plus `member_id`, `member`, `resource`, `class`, and `child_endpoint` while the pilot lives. |

Plus the base plugin routes (`register_session`, `unregister_session/{sid}`,
`version`, `list_sessions`, `health`, `ui_config`).

Status codes: **400** invalid declaration, a client-supplied `task.cwd`, or
a dispatcher refusal forwarded verbatim; **404** unknown resource / task /
session, or an endpoint that is not connected; **409** duplicate resource
name, or no member satisfying the requirements (the body then carries
`reasons`, keyed by `member_id`); **503** the task dispatcher is not hosted
on this broker.

### leave

```json
{"cancel_tasks": false}
```

`leave` removes each of the resource's members from its class pool (with
`force`, since the last member of a pool may not be removed without it) and
leaves the emptied pool in place — with no members it has no pilots and
dispatches nothing, and the next join of that class reuses it. It does
**not** unregister the `fed` session: that session holds every other
resource's class pools.

Its in-flight tasks are **not cancelled** by default. They live in a class
pool and may keep running, or start running, on a sibling member of another
resource; the dispatcher re-queues what was RUNNING on this resource's
pilots and reports how many in `tasks_requeued` / `tasks_failed`. The
federation therefore keeps their ledger entries, re-pointed to
`resource: null` and `member_id: null` (the `pool` and `dispatcher_sid`
stay valid), and the next poll fills the real placement back in — dropping
the entries would make `GET task/…` answer 404, which a campaign runner
reads as a hard failure.

`{"cancel_tasks": true}` is the full teardown. It takes two steps, not one:
`del_member(cancel_tasks=true)` fails the tasks that were on the removed
member's **pilots**, and the federation then cancels every remaining
non-terminal ledger entry of the resource itself — a task the advisory
submit merely *attributed* here is still QUEUED with no pilot and no
member, so the drain never sees it, and dropping its entry without
cancelling it would leave a live task that `GET task/…` answers 404 for.
`tasks_cancelled` counts that second step; the ledger entries are then
dropped.

A member the dispatcher refused to remove does not fail the call — the
resource is forgotten either way, because its endpoint may be gone for good
— but it is **reported**: `ok` is `false` and `errors` lists each member and
its error, rather than the call answering `ok: true, members_removed: 0`.

### pick

```json
{"pool": "fed-gpu", "class": "gpu", "dispatcher_sid": "fed",
 "members": [{"member_id": "bridges.gpu", "resource": "bridges",
              "score": 0.83, "reason": null},
             {"member_id": "local_b.gpu", "resource": "local_b",
              "score": 0.4, "reason": null}],
 "resource": "bridges", "score": 0.83}
```

`resource` is the highest-scoring member's resource, kept so existing
callers still read something sensible. It is **advisory**: the binding
placement is made by the dispatcher at dispatch and reported by `task`.

### submit

`submit` is the single call a workload manager needs — it never learns a
pool name, a dispatcher session, or an endpoint:

```json
{"task": {"task_id": "t.1", "cmd": ["/bin/echo", "hi"],
          "inputs": ["md.json"], "outputs": ["out.json"], "priority": 0,
          "inputs_b64": {"md.json": "<base64>"}},
 "requirements": {"cores": 4, "gpus": 0, "software": ["lammps"],
                  "labels": {"site": "NERSC"}, "node_hours": 0.1}}
```

- **No `cwd`, ever.** The dispatcher assigns it at dispatch from the member
  that actually runs the task — the only correct answer once one pool can
  mix members on different filesystems. A client-supplied `task.cwd` is a
  **400**: the federation cannot honour it across members.
- **`inputs_b64` rides along**, forwarded verbatim. That is what removes the
  client's need to know the placement before the placement exists: the
  dispatcher spools the files and puts them wherever the task lands (it is
  size-capped dispatcher-side, `413`). The federation neither decodes nor
  stores them; it only refuses a non-object with a 400.
- **`requirements` are forwarded** so the dispatcher can match software and
  shape against each member — minus `node_hours`, which is federation-only
  and which the dispatcher's parser would reject as an unknown key. A
  requirements object that is empty after that strip is not sent at all, so
  the wire body for a caller with no requirements is unchanged.

```json
{"task": {...}, "pool": "fed-gpu", "class": "gpu",
 "dispatcher_sid": "fed", "resource": "bridges", "member": null,
 "members_eligible": ["bridges.gpu", "local_b.gpu"]}
```

`resource` is advisory (as in `pick`) so a UI has a chip to show at once;
`member` is `null` until dispatch. A dispatcher `400` — "no member
satisfies the task requirements: software missing: lammps" — propagates
verbatim, status and detail.

### task

`GET task/{sid}/{task_id}` returns the dispatcher's task dict (`state`,
`exit_code`, `error`, `finished_at`, `pilot_id`, `cwd`, …) plus:

- `member_id` — the authoritative placement, set by the dispatcher at
  dispatch. Reading it also **updates the ledger**, which is what re-points
  a task whose resource has left — in both directions: a dispatcher that
  reports **no** `member_id` on a still-running task has re-queued it off a
  lost pilot, and the ledger entry's member is cleared with it, so
  `tasks_running` stops counting the task on a member that is not running
  it. (`resource` stays as it was — advisory — until the next dispatch.)
  A terminal task keeps the member it ran on;
- `member` and `resource` — that id split on its **last** dot (a resource
  name may contain dots, a member name may not);
- `class` — the capability class the task was submitted into;
- `child_endpoint` — the pilot's endpoint name, **while the pilot is
  alive**. That is the participant whose own `staging` plugin can hand back
  output files; it disappears from the dispatcher API the moment the pilot
  ends, so a collector should read it while the task is still terminal-fresh.

**`resource` may be `null`**, and a consumer must tolerate it: before
dispatch it is the advisory value from the submit, and for a task whose
original resource left and which has not been re-dispatched yet there is no
answer at all. That is "not placed", not a failure.

**One class of task is answered from the ledger alone**: a pre-08 entry
whose per-resource dispatcher session the upgrade released (below). Asking
the dispatcher for it would be a 404 on a session it no longer holds, so
the route returns the ledger's own view — `state: "FAILED"` with
`detail: "the federation was upgraded"` — in the same key set. No
`child_endpoint`: the pilot that had one is gone.

## How the pieces are wired

**The federation reaches the dispatcher in-process, never through the
broker caller.** `BrokerCaller` resolves `dst` through the participant
registry and raises for the broker itself, so a broker-hosted plugin cannot
address another broker-hosted plugin that way. The supported path is

```python
host = app.state.endpoint_service          # the BrokerPluginHost
resp = await host.handle_request('POST', '/task_dispatcher/register_session',
                                 {}, body_bytes)
```

— same event loop, exact route semantics including `HTTPException` status
codes, no token. `_DispatcherAPI` is the one place this happens; it resolves
the dispatcher *lazily per call* (plugins load in filter order, and a broker
may run without one) and answers **503** when it is absent.

Endpoint calls (`sysinfo`, `queue_info`) go the *other* way, over the broker
caller, because their target is a real participant.

**There is exactly one dispatcher session, `fed`, and it is persistent.** A
session registered through the in-process path carries no `x-orbit-src`
owner, so it would be an owner-less *ephemeral* session and the base sweep
would drop it an hour after its last access — taking every class pool and
every pilot with it, since dispatcher routes do not bump `last_access`. It
is therefore `lifetime: "persistent"`, and **nothing** unregisters it: a
`leave` removes members, not the session. That single session is a single
point of failure by design (one place to get right), and a test pins that
the sweep cannot reclaim it.

**Membership is driven through the member routes**, `POST
pool/{sid}/{pool}/members` and `DELETE pool/{sid}/{pool}/members/{id}`. The
delete flags (`cancel_tasks`, `force`, `fail_unsatisfiable`) travel in the
**body**, not the query string: the plugin host's `handle_request` takes no
query string, so `?force=true` would end up in the path and match no route.
A host that does not route `DELETE` falls back to the
`POST …/members/{id}/remove` twin with the identical body.

**Re-registration is always the FULL pool list**, never a delta:
`parse_pools` rejects an empty `pools` list and `_materialise_pool` is
idempotent by name, so re-sending every class pool is both required and
free. One helper, `_class_pool_decls`, builds it for `join`, restart replay
and liveness re-attach alike — three callers, one declaration shape. The
`members` carried in a declaration only matter for a dispatcher that does
not have the pool yet; a re-declaration of an existing pool is ignored,
which is exactly why the member routes exist.

**In-process callers must check `resp.status_code`.** `handle_request`
re-raises an `HTTPException` verbatim, so most failures arrive as
exceptions — but `pick` and `submit` answer "nothing fits" with a
`JSONResponse(409)` carrying `{"detail", "reasons": {member_id: why}}`,
because the per-member reasons belong in the body rather than smuggled
through an error `detail`. An in-process caller that only catches
`HTTPException` would read that 409 as success.

**Restart.** State is loaded at construction, and every loaded record — and
every one of its members — starts `lost`: nothing has seen a participant
yet, and a resource must not look routable on the strength of a file. The
re-attach then runs **once**, whichever comes first: the first topology
delivery (the normal case) or the first route. It

1. releases any pre-08 per-resource session (see below);
2. registers `fed` with the full class-pool list;
3. re-POSTs *every* member of *every* stored record. That is a no-op when
   the dispatcher still holds it (an identical re-POST must be), and it is
   the recovery path when the dispatcher's own state was wiped.

Forcing the re-attach from a route matters for the window right after a
restart: a client polling a task through it must reach a live dispatcher
session, not a 404 for one that simply has not been re-registered yet. In
that window `resources` and `task` work normally while `pick` and `submit`
correctly refuse — no endpoint has been seen, so nothing is routable.

**Upgrading a broker that predates class pools.** Every stored record then
carries its own `dispatcher_sid` (`fed-<name>`), and the dispatcher has
replayed those pools off disk **owner-less** — housekeeping skips a pool
whose session is not live, while `unregister_session` on a sid the
dispatcher does not know is a 404 that tears down nothing. So each legacy
sid is **re-owned first and released second**: `register_session(old_sid,
[stored pool])` and *then* `unregister_session(old_sid)`, which runs the
ordinary teardown and actually cancels the pilots instead of leaving them
holding a psij job and a child endpoint. The live ledger entries of those
pools are then failed with `detail: "the federation was upgraded"` — their
pool is gone, the task cannot be recovered — while terminal entries keep
their history. Nothing else is lost: each record's single member was
already derived from its stored declaration at load time.

## Dispatcher changes this plugin required

All in the dispatcher, all generic.

Capability-class pools rest on the **multi-member pool** work: a pool holds
a `pool_class` and a map of `members`, each with its own endpoint, queue,
pilot sizes, budget and attributes; `POST pool/{sid}/{name}/members` and
`DELETE pool/{sid}/{name}/members/{member_id}` add and drain them; a task's
`cwd` and `member_id` are assigned at dispatch; `inputs_b64` on a submit is
spooled by the dispatcher and placed on whichever member runs the task; and
the verbose pool summary gains a per-member block with `node_hours_used`,
`node_hours_remaining` and `pilots_active` — plus, since 122, the member's
`pilot` and `end_time` and the derived `remaining_sec`. Requirement matching lives in one
shared, stateless function (`task_dispatcher_match.satisfies`) so the
federation and the dispatcher answer the same question with the same rules;
`federation_policy.satisfies` is that function, re-exported.

**Endpoint adoption** (plan 122) is the other one: a member declared
`pilot: endpoint` gets no psij job at all. `_submit_pilot` creates its record
`PENDING` with `child_endpoint_name = endpoint_name` and activates it through
the ordinary `_reconcile_pilots_for` → `_activate_pilot` path — immediately
when that endpoint is already connected, otherwise on the next topology
delivery, and **failed after the handshake timeout** (`endpoint … not
connected`) if it never appears, because a record stuck PENDING counts
against the strategy's in-flight guards forever. Such a pilot is marked
**DONE, never FAILED**, at both places it can end — the topology `lost`
branch and `del_member` → `_do_pilot_cancel` — since a leave or an allocation
ending is not a failure and must not feed the member's failure counter. The
member's `end_time` caps the pilot's `walltime_deadline`, and the strategy's
`min_remaining_sec` (default 120 s) keeps a pilot inside two minutes of that
deadline from taking new tasks or counting as free capacity. There is no new
liveness mechanism anywhere in this: one endpoint, one topology signal.

Three earlier ones this plugin also required:

1. **`min_pilots` floor** — `ConservativePolicy.on_tick` now submits while
   a member's live pilots are below its `min_pilots`, even with an empty
   backlog (still bounded by backoff, `max_in_flight_submissions`,
   `max_pilots` and `min_dwell_sec`). The knob was parsed but never
   honoured; without it an allocation-mode resource has no pilot until its
   first task.
2. **Pilot history** — `PilotRecord.finished_at`, stamped in
   `_finalize_pilot`, plus a `pilot_history` block in the verbose pool
   summary listing *all* pilots including terminal ones. `fleet` and
   `pilots` carry live pilots only, so accounting would otherwise lose every
   pilot the moment it ended.
3. **Orphan-pool guard** — housekeeping skips `on_tick` for a pool whose
   owning session is not live. `_replay_state` re-materialises every on-disk
   pool at broker start, before any owner has re-registered; with the floor
   in place, such an orphan would submit pilots forever.

## Persistence

`<state root>/<instance_name>/state.json` — the resource registry (each
record with its members) plus the submit ledger, rewritten atomically
(tempfile + `os.replace`) on every mutation, using the same helper the
dispatcher does. Recovery is a single `json.load`; a record with no
`members` key derives its one member on load, so an older file needs no
migration step. A ledger entry carries `pool`, `dispatcher_sid`, `cls`,
and the nullable `resource` / `member_id` placement.

The state root is `~/.radical/orbit/federation` by default, overridable with
**`RADICAL_ORBIT_FEDERATION_STATE`** (the task dispatcher has no env
override of its own — its state root is a constructor argument — so tests
and demos isolate it by other means).

Default scratch trees live at `<state root>/scratch/<name>`.

## Explorer UI

`data/plugins/federation.js` renders one table with **two column sets**: a
resource row — name, site, software, the `fed-<class>` badges its shapes land
in, then run/done/failed from the record's own usage and the worst state of
its shapes — and, indented under it, one **pilot row** per shape: mode
(`alloc`/`login`), nodes, cpn, gpn, mpn, runtime, left, run, done, failed,
state. Hours are two decimals, and `left` is `-` when nothing is known
(no live pilot, or a pre-122 broker). Node-hours used/remaining moved into
the row's tooltip.

A pilot row is named after the endpoint that runs it: `ep_odo` in allocation
mode, where the endpoint *is* the pilot, and `ep_perlmutter/gpu` in login
mode, where the endpoint submits one pilot per shape. The word "member" is
the wire field, not a label. A record without `members` renders its resource
row and no sub-rows.

The state column shows the derived `state` (`ok` / `idle` / `stale` /
`suspect` / `lost` / `failing`), so a `failing` row gets a red badge and one
monospace line underneath with its `pilot_error` (truncated, full text in the
tooltip) and, when the dispatcher has paused submissions, until when. It
polls `resources/default` every 3 s. The gateway caches plugin JS until a
miss, so **restart the broker after editing the module**.

## Known limitations

- **The psij executor is detected on the broker host**
  (`detect_batch_system().psij_executor` in the dispatcher's
  `_do_pilot_submit`), not on the endpoint that runs the pilot. Correct for
  a co-located broker and for `allocation` mode; a `login`-mode resource on
  a Slurm cluster reached from a non-Slurm broker host will submit with the
  wrong executor.
- **A class is declared, not reserved.** `fed-gpu` means "every member here
  declares GPUs", not "a GPU is held exclusively for your task": pilot
  capacity is task-count based and nothing reserves a device, so two
  GPU-tagged tasks can share a one-GPU pilot. Say *declared*, not
  *reserved*, wherever this surfaces to a user.
- **Placement is late, and advisory until it is not.** `submit` names a
  class, not a site; the `resource` it answers with is the top-scoring
  eligible member's and may change once, on the first poll that reports a
  `member_id`. `GET task/…` can legitimately answer `resource: null`.
- **Inputs are solved, outputs are not.** `inputs_b64` rides the submit and
  the dispatcher places it on whichever member runs the task. Results from
  a member that does not share a filesystem with the broker must still be
  pulled through the pilot's own `staging` plugin — which is what `task`'s
  `child_endpoint` is for. Base64 in a JSON body is ~1.33× the file size
  and rides the broker frame path: fine for a few-KB input file, wrong for
  a multi-MB restart file, and capped dispatcher-side with a `413`.
- **A member's declaration is immutable, bar two fields.** There is no
  partial update and no quiesce: an identical re-POST is a no-op, and
  changing a member's budget or size means removing and re-adding it, i.e.
  re-joining the resource. Budget top-up is out of scope. The exceptions are
  `pilot` and `end_time` — facts about the resource rather than a
  redeclaration of it — which a re-POST updates in place (see the upgrade
  note below); a declaration differing in anything else is still a 409.
- **The dispatcher's `recent_tasks` is capped at 50** per pool, which is why
  task counts come from the federation's own ledger. That ledger grows
  without bound; a `leave` drops only a resource's *terminal* entries and
  keeps the rest, re-pointed, because the work outlives the resource.
- **Usage is polled**, not pushed. The plugin does not subscribe to the
  broker event tap, so accounting is at best 2 s stale.
- **A budget is per member, per join.** Leaving and re-joining a resource
  resets its node-hour accounting, because usage is derived from the pilot
  history the dispatcher keeps for that member id.
- **An allocation's remaining time is only as good as the scheduler's
  answer.** `job_allocation.end_time` needs `squeue %L` (Slurm) or
  `Walltime.Remaining` / `stime` (PBS); an endpoint whose scheduler reports
  neither falls back to `runtime`, the job's *time limit*, and a resource
  joined late into its allocation then has a pilot deadline later than the
  allocation's own end. The pilot is reclaimed when its endpoint disappears
  either way — the deadline only decides DONE-vs-FAILED for a *submitted*
  pilot, and an adopted one is always DONE — so the cost is a `left` column
  that reads optimistically.
- **Multi-node use of an adopted allocation is out of scope.** The adopted
  endpoint runs rhapsody on the allocation's head node with the `concurrent`
  backend; nothing spreads tasks across the other nodes of that allocation,
  and nothing reserves or pins a device.
- **Upgrading to 122 is transparent — no state dir has to be wiped.** Both
  halves of the stale-declaration problem are handled on load. The
  federation's `state.json`: an allocation-mode member stored without a
  `pilot` key is read back as `pilot: endpoint` (its mode says what it is),
  so the first re-POST after the upgrade does not ask for the psij child
  again. The dispatcher's pool state: `pilot` and `end_time` are the two
  fields a re-POST may **update in place**, so a replayed member still
  saying `submit` is corrected rather than answering 409 — the response says
  `{"created": false, "updated": true}`. Every other field stays immutable,
  and a declaration differing in one of those is still a 409. The updated
  `end_time` caps the *next* pilot's deadline; a pilot already live keeps
  the deadline it was given.
- **Sanity ceilings, not policy.** A declared login-mode pool is capped at
  1024 pilots, 100 000 nodes, 4096 cpus/node, 256 gpus/node and 30 days of
  walltime. These only catch a typo before it reaches a batch system; they
  are not an admission-control mechanism.
