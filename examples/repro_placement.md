# Placement-debugging session state — handoff

**Date frozen:** 2026-05-09
**Branch:** `feature/amsc` (radical.edge), `feature/edge` (rhapsody)

The reproducer for this debug is `examples/repro_placement.py`.

---

## Symptom

On a Perlmutter allocation of 16 nodes × 4 GPUs/node:

- amsc.py builds 64+ matey tasks, each pinned via
  ```python
  Policy(placement=HOST_NAME,
         host_name    = nodelist[i % n_hosts],         # round-robin
         gpu_affinity = [(i // n_hosts) % gpus_per_node])
  ```
- Expected: exactly **4 matey processes per node** (one per GPU) under a
  global `asyncio.Semaphore(n_gpus = n_hosts * gpus_per_node = 64)`.
- Observed: count per node varies **1..7** — i.e. some nodes have up to
  3 extra matey processes, oversubscribing GPUs.
- User reports the oversubscription appears in the **first batch** —
  before any task should have completed and freed a semaphore slot.

`pgrep -af basic_inference.py` on the busy node showed 7 distinct
processes with 7 distinct parent PIDs.  Wrapper is `matey_wrapper.sh`,
which only does env-setup then `exec "$@"` — no fan-out.

---

## What's already confirmed (not the cause)

1. **Hostname strings match exactly** between Dragon and amsc.py.
   Dragon `System().nodes` (via `Node(huid).hostname`) and the
   `queue_info.nodelist()` SLURM-derived list both contain the same 16
   `nidNNNNNN` strings, just in different order.  HOST_NAME placement
   should therefore be matching and not silently falling back.
   Probe lives in `src/radical/edge/plugin_rhapsody.py`,
   `RhapsodySession.initialize()`.

2. **`matey_wrapper.sh` does not fan out.**  It loads conda, sources
   `~/MATEY/prep`, sets `MASTER_ADDR`/`MASTER_PORT`, then `exec "$@"`.
   One bash invocation = one Python process.  `--use_ddp` does not
   spawn ranks without a launcher (no `torchrun` / no
   `torch.distributed.launch`).

3. **Round-robin assignment formula is correct on paper.**
   For `i ∈ [0..63]`, `host = nodelist[i % 16]` distributes 4 indices
   per host, and `gpu = (i // 16) % 4` rotates GPU 0..3 across the
   four indices that share a host.  Not yet **printed in a real run**
   — see open questions.

4. **Notifications, JSON-safety, V3 monitor-loop wedging on
   `results_ddict[tuid]`** are *separate* fixes already in flight
   (rhapsody PRs #52 and #53; cherry-picked onto `feature/edge` as
   `54638ee` and similar).  Notifications now flow correctly on
   2-node and 16-node allocs.

5. **The earlier "rebalance after completion" theory is contradicted**
   by the user's "first batch" claim — but see open questions for the
   one diagnostic that would conclusively rule it out.

---

## Suspects, in order of current confidence

### S1.  Dragon's `gpu_affinity` is *advisory*, not a hard cap

`gpu_affinity=[gpu_idx]` may declare *which* GPU a process should use
but **does not prevent** Dragon from placing additional processes on
the same node.  HOST_NAME only constrains the host, not the per-host
process count.  If Dragon's per-node worker pool exceeds
`gpus_per_node`, multiple HOST_NAME-pinned tasks would all schedule on
the same node, with each task happily binding to a GPU even if it
overlaps another task's `gpu_affinity`.

**To verify:** look at Dragon's V3 init log for "workers, N managers".
On 2-node alloc we saw `64 workers, 2 managers` (32 workers/node).
If the same density holds on 16 nodes, Dragon has plenty of capacity
to place 7+ HOST_NAME tasks on a single node.

### S2.  A subset of HOST_NAME tasks fail their host match silently

Even though the **string** lists match, Dragon's internal mapping may
lookup hosts by canonicalised form (FQDN, lowercased, etc.) and miss
on a few entries; those would fall back to default placement and
cluster.  Less likely given the pure `nidNNNNNN` form but worth a
direct test.

### S3.  Race during initial dispatch

When 64 tasks are submitted concurrently in a tight loop, Dragon's
launcher might dispatch them in a way that violates per-node ordering
under contention.  Submitting all 64 in a **single batch** (the
reproducer does this) instead of one-at-a-time should isolate this.

### S4.  Rebalance after completion

Original theory: a global `asyncio.Semaphore(64)` releases on any
task's completion, the next task in the FIFO queue gets that slot
regardless of which node it was statically pinned to.  Mathematically
this *can* oversubscribe a node if some tasks finish faster than
others.  User says the symptom appears in the "first batch" before
any completion, which would refute S4 — but we have **not** confirmed
that the matey progress bar still showed `done=0` at the moment of
the offending `ps`.  See open question O3.

---

## Open questions (need fresh data to resolve)

| # | Question | How to answer |
|---|---|---|
| O1 | Does the reproducer (single-batch, no semaphore, `sleep 10; hostname`) reproduce the 1..7 distribution? | Run `examples/repro_placement.py` under `dragon python`. After it finishes: `sort repro_placement.out \| uniq -c` — each host should appear `GPUS_PER_NODE` times. |
| O2 | What's the total matey count across **all 16 nodes** at the moment of oversub (separate from the reproducer — for the original amsc.py run)? | `clush -a 'pgrep -caf basic_inference.py' \| paste -sd+ \| bc` (or whatever fan-out tool is available). |
| O3 | Did the matey progress bar show `done=0` when 7 was observed? | Look at amsc.py's rich progress output at the moment of the original `ps`.  Confirms whether S4 was actually ruled out. |
| O4 | What does Dragon's V3 init log on 16 nodes report for `workers, managers`? | `grep "DragonExecutionBackendV3:" ~/.radical/edge/logs/<edge>.log` |
| O5 | Does `Policy(HOST_NAME, …)` actually pin a single isolated task to its host? | Replace matey with `executable=/bin/hostname` (instant) in the reproducer; run; tasks return `t.stdout`; assert `task.stdout.strip() == task.host_name`.  If any miss, S2 is real. |

---

## What we have not yet tried

- **`Policy.Placement.HOST_NAMES`** (plural) with a single-element
  list — alternative API form that may behave differently.
- **`gpu_affinity` as a tuple** instead of a list.
- **FQDN hostnames** (`nidNNNNNN.chn.perlmutter.nersc.gov` vs short).
- **Dragon `Node` objects** passed directly into the policy (if the API
  accepts them — needs checking).
- **Stripping `--use_ddp`** from matey args to remove a possible (but
  weak) cause of subprocess fan-out.
- ~~`/bin/hostname` reproducer (smaller than matey; faster turnaround;
  pure placement test)~~ — **done**: `examples/repro_placement.py`
  now uses ``sleep 10; hostname >> ./repro_placement.out``.  Each
  ``hostname`` write is small and uses ``O_APPEND`` so it's atomic on
  Linux; ``sort | uniq -c`` on the output file gives a clean
  per-host count.

---

## Useful files / locations

- `~/radical/radical.edge/examples/amsc.py` — full demo, lines
  ~1100-1230 for the rhapsody-workload submission code.
- `~/radical/radical.edge/examples/repro_placement.py` — minimal
  reproducer using same matey command.
- `~/radical/radical.edge/matey_wrapper.sh` — the wrapper, only
  `exec "$@"` after env setup.
- `~/.amsc/rhapsody/src/rhapsody/backends/execution/dragon.py` —
  Dragon V3 backend on the deploy target.  `_build_task_sync` (~3460)
  is where the `process_template` policy gets attached.
- `~/.radical/edge/logs/<edge>.log` — radical.edge + rhapsody log
  output, both protected from foreign `basicConfig(force=True)` wipes
  (commit `cf7e08a` on `feature/amsc`).

## Relevant commits (radical.edge `feature/amsc`)

- `3ec7ff7` — Dragon hostname probe (`System().nodes` via
  `Node(huid).hostname`).  Used to compare against
  `queue_info.nodelist()`.
- `c48ea0e` — explicit task UIDs (`matey.NNNN` / `gkeyll.NNNN`) so
  on-disk capture files have predictable names.
- `cf7e08a` — protect `radical.edge` and `rhapsody` loggers from foreign
  `basicConfig(force=True)`; this is what makes the rhapsody DEBUG
  lines actually land in the file under Dragon.

## Relevant rhapsody PRs (open against `main`)

- `#52` — `fix(dragon V3): guard results_ddict subscript with
  __contains__` — required for any multi-node V3 run to deliver
  terminal callbacks.
- `#53` — `fix(dragon V3): chunk submit_tasks via asyncio.to_thread`
  — required to keep the asyncio loop responsive at scale.

Both are cherry-picked onto `feature/edge` already; `feature/edge` tip
is `54638ee` (or whatever the local cherry-pick produced — verify with
`git -C ~/.amsc/rhapsody log -3 --oneline feature/edge`).

---

## Recommended next-session opening move

1. Read this file.
2. Run `examples/repro_placement.py` once.  After it finishes:
   ```sh
   sort repro_placement.out | uniq -c
   ```
   Each host should appear exactly `GPUS_PER_NODE` (= 4) times.
3. If counts are off (any host < 4 or > 4): the bug is in Dragon's
   HOST_NAME / gpu_affinity itself.  Try the S2 alternatives — plural
   `Policy.Placement.HOST_NAMES`, tuple vs list `gpu_affinity`, FQDN,
   `Node` objects.  If none work, file against Dragon.
4. If counts are exactly 4-per-host: placement is fine in isolation,
   so the bug is in amsc.py's semaphore/scheduling pattern (S4) under
   matey-specific timing.  Revisit `submit_rhapsody_workload` and
   convert the global semaphore to per-host semaphores.
