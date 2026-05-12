# Placement-debugging session state — handoff

**Last update:** 2026-05-09 (continuing on odo@olcf because Perlmutter is slow)
**radical.edge branch:** `feature/amsc`
**rhapsody branch:** `fix/batch_placement` (off `feature/edge`, see commits below)

---

## Status: diagnosis confirmed, fix implemented, awaiting validation

The original bug — per-host count varies 1..7 instead of exactly 4 with
`Policy(HOST_NAME, host_name=…, gpu_affinity=[…])` round-robin — has
been **root-caused** and a fix is in place on the `fix/batch_placement`
branch of `~/.amsc/rhapsody`.  The fix is also already deployed into
the venv at `~/.amsc/ve/lib/python3.11/site-packages/rhapsody/`.

**What still needs to happen:**

1. Re-run `examples/repro_placement.py` to confirm `sort | uniq -c`
   shows exactly `GPUS_PER_NODE` per host (correctness check).
2. Run `examples/test_placement_throughput.py` and capture the table
   (overhead measurement).
3. Open PRs against rhapsody `main` (or `dev`).

Both validations were started on Perlmutter but turnaround was too slow
to iterate.  Should be quick on Odo.

---

## Root cause

`dragon/workflows/batch/batch.py:Manager.__setstate__` (lines 1151-1184
in the venv copy) builds a fixed worker `Pool` at Manager startup:

```python
if self.pool_node_huids:
    for h_uid in self.pool_node_huids:
        node = Node(h_uid)
        hostname = node.hostname
        num_gpus = node.num_gpus
        for _ in range(self.physical_cores_per_node):
            device_idx = (random.randint(0, num_gpus - 1)
                          if num_gpus > 0 else [])
            policy_list.append(
                Policy(placement=Policy.Placement.HOST_NAME,
                       host_name=hostname,
                       gpu_affinity=[device_idx]))
self.pool = Pool(policy=policy_list, processes_per_policy=1)
```

The pool's per-worker policies are decided **once at startup** with a
**random** GPU per worker, and the pool size per node
(`physical_cores_per_node`, ≈32) is much larger than `num_gpus` (4 on
Perlmutter).  When the user calls
`batch.process(ProcessTemplate(target, policy=Policy(HOST_NAME,…)))`,
the per-task policy is **silently ignored** — placement is determined
by which pool worker happens to grab the task off `work_q`.  With
random GPU assignment + many workers per node + few tasks per node,
the observed 1..7 per-node distribution is exactly what falls out.

The dragon GS path itself (`policy_eval._node_by_name`,
`group_int.evaluate`, `server.choose_shepherd`) is correct *if it ever
sees the per-task policy* — but `dragon.workflows.batch` never feeds
it down.

Confirmed by the user with `examples/repro_placement.py`:
```
$ sort repro_placement.out | uniq -c
      6 nid001220
      4 nid001221
      2 nid001224
      ...
      1 nid001244
```

---

## Fix: rhapsody-side bypass

In `rhapsody/backends/execution/dragon.py` (V3 backend) we detect
host-pinned tasks and bypass `dragon.workflows.batch` entirely:

* Helper `_is_pinned_policy(policy)` → True iff `placement` is
  `HOST_NAME` or `HOST_ID`.
* `_build_task_sync` gains a "Priority 0" branch: if any of the
  task's `process_template` / `process_templates` carries a pinned
  policy, route through `_submit_pinned` (returns `None`).
* `_submit_pinned` chooses between two implementations:
  * **`_submit_pinned_process`** — for the common nproc=1 case
    (covers matey workload and the throughput test).  Spawns a
    `dragon.native.process.Process(..., policy=…)` directly.  One
    GS round-trip per task; no Manager process; no PG handshake.
    Roughly **two orders of magnitude cheaper** than `pg.init()`.
  * **`_submit_pinned_pg`** — fallback for nproc>1 / multi-template
    pinned tasks.  One-shot `ProcessGroup` per task with
    `ignore_error_on_exit=True` (no per-PG `_exq_monitor` thread).
* `_pinned_tasks` dict holds in-flight tasks; slot `kind` is
  `"process"` or `"pg"`.
* `_monitor_loop` extended with a throttled (50 ms) `_sweep_pinned`
  step that walks `_pinned_tasks`, polls `Process.returncode` (or
  `pg._state` for the PG path), feeds completions into the existing
  `_deliver_batch` path so downstream callbacks are unchanged.
* `_pinned_reaper_loop` background thread calls `pg.close()` for the
  PG path so the monitor never blocks on dragon's close patience.
  `Process` slots skip the reaper (no resource to release).
* `cancel_task` and `shutdown` handle both kinds.
* The unpinned `batch.process/job/function` path is **byte-identical**
  to before — no throughput regression for non-pinned tasks.

The legacy ProcessGroup-only path was tried first and timed at **~1.5 s
per task** of GS-bound waiting (init=1.3 s, start=0.3 s) — confirmed
empirically on Perlmutter; see `~/.amsc/radical.edge/examples/log` if
still present, or just re-run.  That's why the Process path was added.

### Affected files

- `~/.amsc/rhapsody/src/rhapsody/backends/execution/dragon.py` — all
  the changes above.  Already deployed into the venv copy.
- `~/.amsc/radical.edge/examples/test_placement_throughput.py` —
  throughput overhead test (new).
- `~/.amsc/radical.edge/examples/repro_placement.py` — unchanged
  (still the placement-correctness reproducer).

### Pre-existing test fix (separate commit)

While verifying, the unit test
`test_v3_deliver_batch_success_stores_value_and_fires_done` was
failing at HEAD of `feature/edge` (unrelated to placement work).  The
fix restores dev's "absent key when no output" semantics in
`_deliver_batch`'s success branch, while keeping the executable-
redirect file-path surfacing that `feature/edge` introduced.  Lives in
its own commit on `fix/batch_placement`.

---

## How to validate on odo@olcf

```sh
# 1. Make sure the fix is in the venv (or pip install -e the source).
diff -q ~/.amsc/rhapsody/src/rhapsody/backends/execution/dragon.py \
        ~/.amsc/ve/lib/python3.11/site-packages/rhapsody/backends/execution/dragon.py

# 2. Allocation.  Adjust account / partition for Odo.
salloc -A <acct> -p <partition> -N 16 -t 30 ...
cd ~/.amsc/radical.edge/examples

# 3. Correctness — should now show exactly GPUS_PER_NODE per host.
dragon python repro_placement.py
sort repro_placement.out | uniq -c

# 4. Overhead — table at the end gives p50 / p95 / max for both modes
#    at 1 task/core and 10 tasks/core.
dragon python test_placement_throughput.py 2>&1 | tee throughput.out
```

If correctness passes but throughput is poor (e.g. < 30 tasks/s for
the bypass mode at 2048 tasks), the bottleneck is likely still the
sequential `Process.start()` call inside `_build_chunk`.  Next
optimization step would be to parallelize `_submit_pinned` calls via a
`ThreadPoolExecutor` inside `_build_chunk` — the framework is in
place; just needs the pool added.

If the bypass is much slower than batch, capture the new
`pinned submit N  in-flight=…` log lines (every 256th task) and the
`pinned sweep: queried=… done=…` lines (1 Hz) — they tell you whether
the bottleneck is build, run, or reap.

---

## Useful files / locations

- `~/.amsc/rhapsody/src/rhapsody/backends/execution/dragon.py` —
  the V3 backend, all changes here; key methods: `_is_pinned_policy`
  (top-level), `_build_task_sync` (Priority 0 branch), `_submit_pinned`
  (router), `_submit_pinned_process`, `_submit_pinned_pg`,
  `_sweep_pinned`, `_pinned_reaper_loop`.
- `~/.amsc/ve/lib/python3.11/site-packages/dragon/workflows/batch/batch.py` —
  the offending Manager pool builder (`__setstate__` lines 1151-1184).
  Not modified; documented in code comments inside our patch.
- `~/.amsc/ve/lib/python3.11/site-packages/dragon/native/process.py` —
  `Process` class used by the cheap path.
- `~/.amsc/radical.edge/examples/repro_placement.py` — placement
  correctness reproducer.
- `~/.amsc/radical.edge/examples/test_placement_throughput.py` —
  throughput overhead test.
- `~/.amsc/radical.edge/examples/log` — last verbose run from
  Perlmutter, if still present.

## Relevant commits (rhapsody `fix/batch_placement`)

- placement bypass commit — adds `_is_pinned_policy`, `_submit_pinned*`,
  `_sweep_pinned`, `_pinned_reaper_loop`, branch in `_build_task_sync`,
  cancel/shutdown integration.
- `_deliver_batch` dev-semantics fix — guards stdout/stderr key
  assignment so absent → absent on success, restoring the test
  contract.

Branch is based on `feature/edge` (which already includes the
cherry-picked PRs #52 and #53 needed for V3 multi-node operation).
