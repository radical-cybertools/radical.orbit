# MATEY inference debug session — ODO

Bring-up of `matey/examples/basic_inference.py` on ODO (Frontier-class
OLCF), running on login1. Started from amsc.py rhapsody workload skip,
ended at a working forward pass.

## Working invocation (single line)

```sh
python /autofs/nccsopen-svm1_home/merzky/matey/MATEY/examples/basic_inference.py \
  --model_dir /gpfs/wolf2/olcf/fus183/proj-shared/MATEY/models/Dev_Fusion_Demo_March2026_AR_Final/demo_nbatchsloc100 \
  --pretraining_data_dir /gpfs/wolf2/olcf/fus183/proj-shared/MATEY/Datasets_pretraining/ \
  --use_ddp --AR --leadtime 5 \
  --newxgc_dir /gpfs/wolf2/olcf/fus183/proj-shared/fusiond-seed-xgc1-data/n565pe_PT_xgc1_d3d_adjust_flow2_for_C/
```

## Issue chain (each error fixed in order)

### 1. `AttributeError: 'YParams' object has no attribute 'use_fsdp'`

`matey/utils/distributed_utils.py:49` does
`if params.use_ddp or params.use_fsdp:`. `basic_inference.py` only
defines `--use_ddp`, never `--use_fsdp`. When `--use_ddp` is omitted,
short-circuit fails and Python tries to read `params.use_fsdp` →
`AttributeError`.

**Workaround:** pass `--use_ddp`. The example submit scripts all do.

**Proper fix (upstream):** `distributed_utils.py:49`
`getattr(params, 'use_fsdp', False)` instead of `params.use_fsdp`.

### 2. SOLPS/XGC `train_data_paths` point at NERSC

`basic_inference.py:24` defaults `--pretraining_data_dir` to
`/global/cfs/projectdirs/amsc007/zhan1668/MATEY/Datasets_pretraining/`
(NERSC). Lines 50-60 hardcode `params.train_data_paths` /
`valid_data_paths` off that root, and `Inferencer.__init__` calls
`initialize_data()` unconditionally — so the dataloader walks NERSC
paths even on a pure inference run. On ODO the paths don't exist →
`ValueError: Dataset … is empty`.

**Fix:** pass an ODO-local
`--pretraining_data_dir /gpfs/wolf2/olcf/fus183/proj-shared/MATEY/Datasets_pretraining/`
which has the full layout (`solps/{train,valid,SOLPS2DwION}`,
`fusiond-seed-xgc1-data/<all hardcoded case dirs>`, `gkeyll/`).

User had a partial mirror at `~/matey/data/` missing the three
hardcoded xgc1 case dirs (`n585pe_*`, `n613fr_*`, `n565pe_*`) which
`MixedDataset.__init__` requires whenever `graphxgc` appears in the
data paths.

### 3. Checkpoint architecture mismatch

`RuntimeError: Missing key(s) in state_dict: "module.inconMLP.*",
"module.space_bag.*", "module.tokenizer_ensemble_heads.*"`

`basic_inference.py:31-32` sets `params.supportdata =
[{"input_control_act": True}]` when `--AR` is passed, which adds an
input-control MLP layer to the model. The user's
`~/matey/models/demo_nbatchsloc100` checkpoint was trained without
`--AR`, so it lacks those weights.

**Fix:** use the AR-trained checkpoint on ODO:
`/gpfs/wolf2/olcf/fus183/proj-shared/MATEY/models/Dev_Fusion_Demo_March2026_AR_Final/demo_nbatchsloc100`

Three model dirs are staged on ODO; the `_AR_Final` one is the one
that matches `--AR`.

### 4. `--newxgc_dir` pointing at `demo_*` strip-down

`RuntimeError: [ADIOS2 EXCEPTION] couldn't open file
.../demo_n565pe_PT_xgc1_d3d_adjust_flow2_for_C/xgc.mesh.bp`

The `demo_*` variants of the xgc1 case dirs only contain a
`processed/` subdir — no `xgc.mesh.bp`, no raw XGC output. The
`inference_step_newxgcmesh` → `construct_graph_from_xgc` →
`xgc_base.xgc1` path opens raw XGC files from the case dir.

**Fix:** point `--newxgc_dir` at a full case dir, e.g.
`/gpfs/wolf2/olcf/fus183/proj-shared/fusiond-seed-xgc1-data/n565pe_PT_xgc1_d3d_adjust_flow2_for_C/`
(non-`demo_`). All nine full case dirs there have `xgc.mesh.bp`.

Side note: `XGC_reader/base.py:50-51` does `os.chdir(path); self.path
= os.getcwd() + '/'`. That resolves symlinks, so
`MATEY/Datasets_pretraining/fusiond-seed-xgc1-data` (symlink to
`../../fusiond-seed-xgc1-data`) shows up in errors with the collapsed
physical path. Same physical location, not a bug, just confusing.

### 5. flash-attn ROCm stub (current fix)

`NotImplementedError: flash-attn stubbed; 'flash_attn_func' called`

The user's venv at
`/ccsopen/home/merzky/matey/env/lib/python3.10/site-packages/flash_attn/__init__.py`
was a pure stub:

```python
def __getattr__(name):
    def _stub(*a, **kw):
        raise NotImplementedError(...)
```

— installed in place of real flash-attn (which is non-trivial to build
for AMD/ROCm). Matey calls `flash_attn_func` from six sites in
`matey/models/attention_modules.py` and additional `_flash_attn_*`
symbols in `ringX_attn.py`. The commented-out line at
`attention_modules.py:490` (`#x = F.scaled_dot_product_attention(q, k,
v)`) shows the intended fallback.

**Fix applied:** replaced the venv stub with a working
`flash_attn_func` that delegates to `torch.nn.functional.scaled_dot_product_attention`,
handling the (b, len, he, c) ↔ (b, he, len, c) layout swap so it acts
as a drop-in for the matey call sites:

```python
def flash_attn_func(q, k, v, *args, **kwargs):
    q, k, v = (t.transpose(1, 2) for t in (q, k, v))
    out = F.scaled_dot_product_attention(q, k, v)
    return out.transpose(1, 2)
```

Other flash-attn symbols still raise — only the sequence-parallel
path in `ringX_attn` references them, and that path is unused at
`world_size=1`.

## Open issues / things to watch

- **Login-node run.** The user is running on `login1` with
  `world_size=1`. PyTorch ends up on `cuda:0` if `torch.cuda.is_available()`
  reports true even on the login node, but real GPU work belongs on
  a compute node. SDPA falls back to CPU when there's no GPU; expect
  slow inference for the smoke test.
- **`[c10d] errno 97`** warning when DDP tries to bind on
  `login1.odo.olcf.ornl.gov:29500` — same IPv6-not-supported pattern
  seen with the reverse SSH tunnel. Harmless at world_size=1.
- **amsc.py demo wiring — DONE.** `examples/amsc.py`'s
  `submit_rhapsody_workload` now builds `matey_args` matching the
  working ODO invocation:
  - `--use_ddp` uncommented.
  - `--on_perlmutter` made opt-in via `app_cfg['matey_on_perlmutter']`
    so non-NERSC targets don't get it.
  - `--pretraining_data_dir` made opt-in via
    `app_cfg['matey_pretraining_dir']` so non-NERSC targets can
    override the script's NERSC default.

  `MACHINE_DEFAULTS['odo']['app']` populated with the ODO-side paths
  that worked during the smoke test (AR-trained checkpoint at
  `Dev_Fusion_Demo_March2026_AR_Final`, full xgc1 case dir
  `n565pe_PT_xgc1_d3d_adjust_flow2_for_C`, wolf2 pretraining root).
  `MACHINE_DEFAULTS['perlmutter']['app']` gained `matey_on_perlmutter:
  True` and an explicit `matey_pretraining_dir` so the dispatch logic
  is symmetric across sites.

### 6. `torch.save` failing after xgc1's silent `os.chdir`

After patching flash-attn (issue 5), the forward pass completed and
printed real predictions:

    Prediction of XGC-D3D, rmse_loss:0.3497372269630432; nrmse_loss 0.6057045459747314

`torch.save` then tried to write `matey_XGC_leadtime_1.0.pt` to a
**relative** path.  By that point `XGC_reader/base.py:50` had silently
done `os.chdir(case_path)` during `xgc_base.xgc1(path=case_path)`, so
the CWD was now `/gpfs/wolf2/olcf/fus183/proj-shared/fusiond-seed-xgc1-data/n565pe_PT_xgc1_d3d_adjust_flow2_for_C/`
— `drwxr-sr-x mbt:fus183`, not writable by us → `RuntimeError: File ...
cannot be opened`.

**Fix applied:** capture `os.getcwd()` once at `matey/inference.py`
import time into a module-level `_LAUNCH_CWD`, and rewrite the two
`torch.save(..., f"matey_{case}_leadtime_...pt")` sites (lines 349 and
496) to write to `os.path.join(_LAUNCH_CWD, ...)`.  Module import
happens before any `xgc1.__init__()` chdir, so the launch dir is
preserved.

The proper upstream fix would be to teach `XGC_reader` not to chdir —
track `path` as an attribute and prepend it explicitly to every
filename — but that's a bigger refactor.  The launch-dir capture is
self-contained to `matey/inference.py`.

### 7. Cross-venv `PYTHONPATH` contamination (radical.edge → matey task)

Visible only when matey is launched via radical.edge / rhapsody, not
during the standalone smoke test:

    ModuleNotFoundError: No module named 'numpy._core._multiarray_umath'
    ...
    The Python version is: Python 3.10 from "/ccsopen/home/merzky/matey/env/bin/python"
    The NumPy version is: "2.4.4"
    _multiarray_umath.cpython-311-x86_64-linux-gnu.so

Two venvs with different Python minors, leaking across processes:

- radical.edge child runs under `/autofs/.../.amsc/ve` (Python 3.11),
  spawned by `radical-edge-wrapper.sh` which sets
  `PYTHONPATH="@SITEPKGS@:${PYTHONPATH:-}"`.
- The matey task subprocess inherits that 3.11 `PYTHONPATH`.
- `matey_wrapper.sh` activates the 3.10 conda env at `~/matey/env`,
  but then did `PYTHONPATH="$MATEY:$MATEY/third_party/XGC_reader:${PYTHONPATH:-}"`
  — preserving the inherited 3.11 path.
- Python 3.10 walks `sys.path`, finds the 3.11 numpy site-packages
  ahead of its own, and tries to load a `.cpython-311-…so` extension
  in a 3.10 process → import fails.

**Fix applied:** drop the inherited `PYTHONPATH` in
`~/matey/MATEY/matey_wrapper.sh:19`:

    export PYTHONPATH="$MATEY:$MATEY/third_party/XGC_reader"

(no trailing `:${PYTHONPATH:-}`).  The matey env's site-packages comes
in via the conda activation, so nothing else is lost.

### 8. `MASTER_PORT=29500` collision across per-GPU matey tasks

Visible only on multi-task rhapsody runs, not the standalone smoke test:

    torch.distributed.DistNetworkError: The server socket has failed to
    listen on any local network address. port: 29500, useIpv6: false,
    code: -98, name: EADDRINUSE, message: address already in use

`matey_wrapper.sh` hardcoded `MASTER_PORT=29500`.  amsc.py pins matey
tasks per-GPU via `Policy.Placement.HOST_NAME` + `gpu_affinity`, so up
to `gpus_per_node` tasks share a node.  Each one's
`dist.init_process_group(backend='nccl', init_method='env://',
rank=0, world_size=1)` still binds a TCPStore on
`$MASTER_ADDR:$MASTER_PORT`.  First task binds 29500, the rest die
with EADDRINUSE.

Dragon doesn't propagate `SLURM_NTASKS`, so `setup_dist()` takes the
generic (non-SLURM) branch where `MASTER_PORT` defaults to whatever's
in env — i.e. the wrapper's 29500.

**First attempt (didn't fly):** let the kernel hand out an ephemeral
port per task via `python3 -c 'import socket; s=socket.socket();
s.bind(("",0)); ... s.close(); print(p)'`.  Looked safe in theory but
in practice tasks started near-simultaneously and the close-then-bind
race did produce duplicate ports — same EADDRINUSE returned.

**Fix applied:** coarse bash `$RANDOM`-derived offset, no race
window between port pick and torch's `bind`, in
`~/matey/MATEY/matey_wrapper.sh:28`:

    export MASTER_PORT="${MASTER_PORT:-$((29500 + RANDOM % 100 + 1))}"

100-slot range with ≤8 tasks per node — birthday-paradox collision
chance ≈25%, but each retry is independent, and the user can rerun
or override `MASTER_PORT` externally.  If this turns out to bite at
larger `gpus_per_node`, widen the modulus.

### 9. PsiJ Slurm `parse_status_output` `AssertionError` on truncated Reason

Login-edge log:

    INFO:     [psij] watcher edge=odo.1 job=40039 mode=reverse state='PENDING'
    WARNING:  Polling error: Failed to poll for job status:
      File ".../psij/executors/batch/slurm.py", line 166, in parse_status_output
        assert len(cols) == 3
    AssertionError

PsiJ's `SlurmJobExecutor.get_status_command()` runs
`squeue -O JobArrayID,StateCompact,Reason -t all --me`.  `-O Reason`
defaults to a **20-char field width**; SLURM truncates longer
reasons **mid-word**.  Real squeue output on ODO:

    JOBID               ST                  REASON
    40039               PD                  Nodes required for j

That trailing `"Nodes required for j"` is the single Reason field —
truncated from `"Nodes required for job are DOWN"`.  `line.split()`
splits it into 4 tokens, so `cols` is 6 elements and the
`assert len(cols) == 3` fires.

Doesn't block submission or reverse-tunnel spawn — those use
radical.edge's own `BatchSystem.job_state` (different code path).
It does spam the log every PsiJ poll cycle and may make
`psij.Job.status` stale.

**Fix applied:** in
`/autofs/.../ve/lib/python3.11/site-packages/psij/executors/batch/slurm.py`
(line 165), switch from `line.split()` to `line.split(None, 2)` so any
whitespace inside the Reason gets bundled into the third token; add a
`len == 2` guard for the (rare) empty-Reason case.

Caveat: `_get_message` does a strict `assert reason in
_REASONS_MAP` lookup, which only fires when state is `FAILED`.  A
truncated/multi-word reason on a FAILED job would trip that
secondary assert — same root cause, different code path.  Not seen
yet; flag for a follow-up if it appears.

### 10. Reverse-tunnel watcher races child Dragon startup

Symptom seen on a multi-node ODO run: child log shows
`Reverse tunnel active on localhost:32893` immediately followed by
`[Errno 111] Connect call failed ('127.0.0.1', 32893)` — the ssh -R
listener exists, just not on this node.  Rendezvous file timestamps:

    odo.1.port  2026-05-11 08:43:43   (parent wrote)
    odo.1.req   2026-05-11 08:43:58   (child wrote, 15s LATER)

Watcher's spawn gate was `state == RUNNING` — SLURM reports RUNNING
when the allocation is granted, but the child needs another 15s
to import Python + Dragon + radical.edge before it can write
`.req`.  The earlier `.req`-first fix (issue 0) still kept a
`nodes[0]` fallback, which is what fired in that 15-second gap.

The architectural fix from this turn:

- **Watcher gates ssh -R spawn on `.req` existence, not on SLURM
  state.**  `state == RUNNING` is only used to know *whether* to
  start looking; spawn waits for `.req`.
- **`nodes[0]` fallback removed entirely.**  If `.req` never
  appears, the watcher's existing 300-iteration / ~10 min loop
  bounds the wait, and `_fail_tunnel` cancels with a clear reason.
- **Client `.port` wait shortened from 300s to 15s** in
  `service.py:_open_tunnel_reverse`.  Once `.req` is on disk
  Dragon startup is already past — what remains is the ssh
  handshake + "Allocated port N" parse + .port write, which is
  predictable and small.

Flow on a healthy reverse tunnel is now:

    child  → writes .req (own hostname)
    parent → reads .req, spawns ssh -R, writes .port
    child  → reads .port, rewrites bridge URL to localhost:<port>

with both sides bounded by their own timeouts and the watcher's
outer 10-min loop as the ultimate stop.

### 10b. Watcher's RUNNING precondition was still a single point of failure

ODO 2026-05-11 11:27-11:38: another reverse-tunnel run, this time
with the issue-10 patches in place.  Symptom from
`~/.radical/edge/logs/odo.log`:

    11:27:59  [psij] Watcher started ...
    11:28:59  [psij] watcher edge=odo.1 job=40063 mode=reverse state='UNKNOWN' (attempt 29/300)
    [every 30 attempts, all UNKNOWN, until]
    11:38:02  [psij] Watcher for edge 'odo.1' timed out

Meanwhile in `odo.1.log`:

    11:28:30  [Edge] wrote request file odo.1.req
    11:28:46  RuntimeError: rendezvous file did not appear within 15s

sacct: job 40063 actually ran 11:27:59 → 11:28:47 and COMPLETED.
But this cluster's `squeue --job <id> --format=%T` returned
empty/error for the entire job lifetime — the watcher's
`batch.job_state()` saw UNKNOWN the whole time, never matched
`state == STATE_RUNNING`, never even tried to read `.req`.

Even though `.req` was on disk by 11:28:30, the watcher couldn't
look at it because it was still waiting for the (broken) RUNNING
signal.

**Fix applied** (`src/radical/edge/plugin_psij.py`,
`_tunnel_watcher` reverse-spawn branch):

- Drop the `state == STATE_RUNNING` conjunct entirely.  The
  spawn branch now fires whenever `.req` exists and `ssh_proc is
  None`, regardless of what SLURM state polling reports.
- `.req` is itself authoritative: the child can only have written
  it after Python + Dragon were up and `socket.gethostname()`
  returned a real hostname.  It's a much better readiness signal
  than a `squeue` call.
- Terminal-state branch extended: when the job hits
  `TERMINAL_STATES` *without* `ssh_proc` (i.e. the child died
  before producing `.req`), call `_fail_tunnel` with a clear
  reason — UNLESS state is `CANCELLED`, since that's
  operator-initiated and shouldn't be converted to FAILED.
- Imports updated: drop unused `STATE_RUNNING`, add `STATE_CANCELLED`.

After this change, SLURM state polling is used ONLY for the
abort paths (terminal-state, UNKNOWN-streak, watcher timeout).
Spawn doesn't depend on it.

### 10c. SSH from login → compute denied by `pam_slurm_adopt` race

With 10b in place, watcher correctly waited for `.req`, parsed
`hostname=odo11`, and spawned `ssh -R 0:<bridge>:<port> odo11`.
SSH authenticated successfully via the user's `id_ed25519`, then
sshd on odo11 closed the session:

    Access denied by pam_slurm_adopt: you have no active jobs on this node
    Connection closed by 10.129.16.52 port 22

`pam_slurm_adopt` is ODO's compute-node PAM module that denies any
SSH whose user has no active SLURM job on the destination node.
The user DOES (the child wrote `.req` from there), but there's a
short window after the job is registered with SLURM during which
the PAM module isn't yet aware — `pam_slurm_adopt` rejects, then
within seconds starts accepting.

Manual repro: `ssh odo05 hostname` from login1 succeeds *while* an
`interact` allocation is running on odo05.  So the rejection is a
race, not a policy.

**Fix applied** (`src/radical/edge/plugin_psij.py`,
`_tunnel_watcher` reverse-spawn branch): wrap the
`spawn_reverse_tunnel` call in a retry loop — up to 30 attempts,
1s apart.  Any spawn failure triggers a retry without trying to
classify the error (a transient network blip / pam denial / SSH
hiccup all get the same handling).  Between attempts, poll
`batch.job_state(native_id)` and bail via `_fail_tunnel` if the
job has hit a TERMINAL state — no point retrying once the
allocation is gone.

Transport-agnostic on purpose: this works for any batch system
whose login→compute SSH is gated by a similar
job-must-be-fully-registered check (PBS, LSF, future backends).
Avoided alternatives like `srun --jobid=<id> --overlap` (SLURM-only)
or `socat` tunneling (different topology, more invasive).

Logging discipline: INFO once on first failure ("likely
pam_slurm_adopt race, retrying"), DEBUG per subsequent attempt,
INFO once on the spawn that finally succeeds (the existing
`[tunnel] Reverse SSH allocated remote port ...` line in
tunnel.py).

### 10d. NFSv3 negative-lookup cache hides the child's .req from the parent

ODO 2026-05-11 17:10 run, third attempt with 10b + 10c patches:

    17:10:11  watcher attempt 0: state=RUNNING (.req not on disk yet)
    17:10:30  child writes .req with hostname=odo11
    17:10:31..45  watcher iterates 8 times — req_file.exists() returns False on EVERY check
    17:10:46  child crashes (15s .port timeout)
    17:10:47  SLURM marks job COMPLETED
    17:10:59  watcher: state=UNKNOWN (job purged from squeue)
    17:11:03  watcher aborts on UNKNOWN×3

The `.req` file was on disk for 16 seconds; the parent was checking
for it every 2s; it was visible on **both** mount views to a manual
`ls` after the run (same inode `32680474`).  But the watcher's
`Path.exists()` returned False the entire time.

The mount table tells the story:

    172.30.252.205:/nccsopen/home on /autofs/nccsopen-svm1_home type nfs (..., vers=3, ...)

NFSv3 + Linux client caches negative lookups (`stat → ENOENT`) for
`acregmin` seconds (default 30-60s).  Once the parent's first
`req_file.exists()` returned False at 17:10:11, the cached ENOENT
was reused for every subsequent stat() of that path until the
cache expired — irrespective of the file's actual state on the
server.

A readdir on the parent directory forces the client to fetch
fresh directory attributes; on Linux NFS clients this invalidates
negative-lookup entries for that dir.

**Fix applied** (`src/radical/edge/plugin_psij.py`):

- Replaced the `req_file.exists()` check with
  `req_file.name in set(os.listdir(req_file.parent))`.  The
  `os.listdir` call triggers a readdir RPC which forces fresh
  directory attributes and invalidates the cached negative
  lookup.
- Moved the `.req` unlink in `_fail_tunnel` out of the
  `spawn_proc is not None` guard.  All failure paths
  (UNKNOWN-streak abort, terminal-state abort without spawn,
  watcher timeout) now clean up `.req` so a quick re-submit
  doesn't inherit stale state past submit-time cleanup.

## Files touched outside the radical.edge repo

- `/ccsopen/home/merzky/matey/env/lib/python3.10/site-packages/flash_attn/__init__.py` — flash-attn stub replaced with SDPA delegator (issue 5). Lost if the venv is reinstalled.
- `/ccsopen/home/merzky/matey/MATEY/matey/inference.py` — added `_LAUNCH_CWD = os.getcwd()` at module top; both `torch.save` call sites now join `_LAUNCH_CWD` with the filename (issue 6).
- `/ccsopen/home/merzky/matey/MATEY/matey_wrapper.sh` — (issue 7) no longer preserves the inherited `PYTHONPATH`; (issue 8) picks an ephemeral `MASTER_PORT` so per-GPU matey tasks on one node don't fight over 29500.
- `/autofs/nccsopen-svm1_home/merzky/.amsc/ve/lib/python3.11/site-packages/psij/executors/batch/slurm.py` — `parse_status_output` uses `split(None, 2)` so SLURM's mid-word-truncated `Reason` field doesn't blow up the assert (issue 9).  Lost if psij is reinstalled in the radical.edge venv.
