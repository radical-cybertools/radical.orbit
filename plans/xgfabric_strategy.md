# XGFabric Workflow Strategy

## Goal

Run a closed-loop wind simulation and ML training pipeline that adapts dynamically to
available HPC resources.  The data source is a live sensor (CSPOT WooF URL — a UCSB
wind station).  The trained models predict wind loads on a structure (CUPS).

---

## Resource Model

Two tiers of compute clusters:

- **Immediate clusters** — always available, direct execution (no batch scheduler).
  Used for staging, simulations, training, and evaluation when no GPU cluster is
  available.  Currently: `thinkie` and any edge whose name contains `ucsb`
  (hardcoded exception — FIXME).

- **Allocate clusters** — HPC systems with SLURM.  The plugin submits a *pilot job*
  via PsiJ to provision a child edge service inside the allocation.  When the pilot
  comes online it registers as a new edge and takes over from the immediate cluster.
  Currently: `perlmutter`.

Cluster classification is dynamic: edges are re-evaluated on every topology change.
An edge with `queue_info` plugin **and** a working scheduler (`is_enabled` returns
`true`) is classified as `allocate`; everything else is `immediate`.

---

## Workflow Phases (in order)

| # | Phase | Description |
|---|-------|-------------|
| 1 | **Connecting** | Create `BridgeClient`, verify all configured edges are present |
| 2 | **Verifying** | Confirm immediate/allocate cluster edges are online |
| 3 | **Data acquisition** | Fetch `cspot_limit` (default 72) wind records from the CSPOT WooF, one `senspot-get` call per sequence number, backward from latest |
| 4 | **Pilot submit** *(if allocate cluster configured)* | Submit SLURM pilot job via PsiJ to spawn a child edge |
| 5 | **Staging** | Upload `sensor_out.csv` to the immediate cluster's `workflow_path/data/` via the staging plugin |
| 6 | **Simulations** | Run `num_simulations` (default 16) CFD tasks via Rhapsody in batches of `batch_size` (default 4).  After each task completion, check if the allocate cluster came online — if so, abort remaining batch and migrate immediately |
| 7 | **Migration** *(conditional)* | If allocate cluster is online, transfer completed simulation results to it and switch `active_cluster` |
| 8 | **Training** | Run PCR, PINN, and/or FNO model training via Rhapsody on the active cluster |
| 9 | **Evaluation** | Compute sensor metrics via a Python script submitted as a Rhapsody task |

---

## Adaptive Migration

The key dynamic behaviour: if a SLURM pilot job lands while simulations are still
running on the immediate cluster, the workflow **aborts the current batch** and
migrates immediately rather than waiting for the full simulation set to complete.
Rationale: the GPU/HPC speedup for training outweighs the cost of the abandoned
partial batch.

- Pilot availability is checked after every individual task completion (via real-time
  Rhapsody `task_status` SSE callbacks), with a 5-second fallback poll.
- Completed simulation results are migrated; incomplete tasks are abandoned.
- Training and evaluation then run on the allocate cluster.

---

## Implementation Notes

- All blocking bridge/plugin HTTP calls use `asyncio.to_thread` to avoid deadlocking
  the edge service event loop.
- Real-time task completion uses an `asyncio.Queue` fed by a Rhapsody
  `task_status` SSE callback registered on the workflow's `BridgeClient`.
- `WorkflowState.current_batch` / `total_batches` are updated per batch so the UI
  progress bar reflects actual simulation progress.
- Failed tasks are logged with a warning; the workflow continues with whatever
  completed results are available.  If zero tasks complete, a `RuntimeError` is raised.
