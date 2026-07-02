# MPI Function Tasks: Deployment Notes

Captured 2026-05-02 from the AMSC-on-Odo debugging arc.  The AMSC demo does
not actually need MPI functions right now, so we backed out the MPI work and
parked it.  This file is a reference for the next time someone wants
`mpi4py`-using function tasks via the rhapsody V3 dragon backend, and a
checklist of what should be documented in the radical.edge and rhapsody
docs (separate PR for rhapsody later).

---

## Current State (May 2026)

### What works

1. **Rhapsody V3 silent-traceback fix** — landed in
   `rhapsody/src/rhapsody/backends/execution/dragon.py`:
   - Module-level `_v3_function_wrapper` redirects per-rank stdout/stderr to
     files in `self._work_dir`, catches `BaseException`, writes the Python
     traceback to the rank's stderr file before re-raising.
   - V3 `build_task` wraps every function target (Priorities 1–4) with
     `functools.partial(_v3_function_wrapper, target, stdout_path, stderr_path)`.
   - V3 `_deliver_batch` globs `<stderr_path>.<rank>` files on `raised=True`
     and folds their contents into the augmented exception's message
     (`--- worker output ---\n[rank N]\n…`), preserving the original
     exception type so `await sim_task` re-raises with full context.
   - Rank label prefers MPI env vars (`PMI_RANK` / `OMPI_COMM_WORLD_RANK` /
     `PALS_RANKID` / `SLURM_PROCID`) and falls back to `pid<N>` for non-MPI
     ProcessGroup ranks.

2. **Edge wrapper site-setup hook** — `bin/radical-edge-wrapper.sh.in`
   evaluates `RADICAL_EDGE_SETUP` before the dragon-vs-python interpreter
   decision; the chosen interpreter is anchored to `@BINDIR@` so a
   `module load <python>` in the snippet cannot hijack the selection.  The
   wrapper echoes `[radical-edge] applying RADICAL_EDGE_SETUP: …` to stderr
   when the var is set — visible in the PsiJ job's `.err` file.

3. **PsiJ keep-files toggle** — `RADICAL_EDGE_PSIJ_KEEP_FILES=1` retains the
   generated submit script for diagnosis (see `plugin_psij.py`).

### What does not work yet

On HPE/Cray Odo (Cray EX, `cray-mpich/8.1.31`, `PrgEnv-gnu/8.6.0`,
`libfabric/1.22.0`, `cray-python/3.11.7`):

1. **`module load PrgEnv-gnu cray-mpich` in the edge-service environment
   hangs dragon at infrastructure init.**  The dragon launcher gets through
   `connecting to infrastructure` and `debug entry hooked`, prints the
   radical/logging fork-deadlock warning a few times, then never reaches
   `DragonExecutionBackendV3: N workers, M managers`.  The HTTP request the
   client made to the rhapsody plugin's `submit_tasks` is closed without
   response and surfaces as `httpx.RemoteProtocolError: Server disconnected
   without sending a response`.  Direct repro outside edge:

   ```sh
   salloc -A <proj> -N 1 -t 00:30:00
   module load cray-python/3.11.7 PrgEnv-gnu cray-mpich
   source ~/.amsc/ve/bin/activate
   ~/.amsc/ve/bin/dragon python3 -c \
       'from dragon.workflows.batch import Batch; b = Batch(); b.close()'
   # → "Detected an abnormal exit. Will attempt to clean up Dragon resources..."
   ```

   Hypothesis (not confirmed by upstream): cray-mpich brings a different
   `libfabric` / shared comm state into the same process tree dragon tries
   to manage with its bundled `libdfabric_ofi.so`.  Same failure with cray-
   mpich + PrgEnv-gnu loaded at the dragon parent level regardless of whether
   the venv's mpi4py is rebuilt.

2. **mpi4py's runtime ABI loader expects `libmpi.so` reachable.**  Without
   cray-mpich loaded the rank's import fails with the trail
   `_dlopen_libmpi → cannot load MPI library` — visible now (and only now)
   thanks to the V3 stderr-capture fix.

### Two unfinished resolution paths

The original session sketched four options (a–d).  We tried and abandoned:

- **(a)** *Load cray-mpich only inside the rank, not at the edge-service
  parent level.*  Would require either `os.execvpe` from the wrapper into
  a re-modulised env, or threading site-specific `module load` into
  `_v3_function_wrapper`.  Untried.  Likely the cleanest path on Cray.

- **(c)** *Build mpi4py against MPICH whose libfabric is independent from
  dragon's.*  PyPI's `mpich` package is a 770-byte stub — does NOT build
  MPICH.  Real path is a manual source build:

  ```sh
  source ~/.amsc/ve/bin/activate
  module load cray-python/3.11.7 PrgEnv-gnu

  cd /tmp && wget https://www.mpich.org/static/downloads/4.2.3/mpich-4.2.3.tar.gz
  tar xzf mpich-4.2.3.tar.gz && cd mpich-4.2.3
  CC=gcc CXX=g++ ./configure \
      --prefix="$VIRTUAL_ENV" \
      --disable-fortran \
      --enable-shared \
      --with-device=ch4:ofi \
      --with-libfabric=embedded
  make -j8 && make install
  ```

  Drops `libmpi.so` into `$VIRTUAL_ENV/lib/`, where mpi4py's runtime ABI
  loader looks first.  Embedded `libfabric` keeps MPICH's transport stack
  isolated from dragon's.  Build takes ~5–15 min; not run yet.

---

## What Should Be Documented

### In `radical.edge` (this repo, near term)

The `RADICAL_EDGE_SETUP` mechanism is currently mentioned only in
`bin/radical-edge-wrapper.sh.in`'s comments.  It should have a dedicated
section in the top-level README or a `docs/site_setup.md`, covering:

- How to set it (it's a per-job env var passed via the IRI/PsiJ job spec
  — see `examples/amsc.py`'s `setup` field for the shape).
- The trust model (line 18 of the wrapper: same boundary as job submission
  itself; no new attack surface).
- Verification: grep for the `[radical-edge] applying RADICAL_EDGE_SETUP:`
  line in the PsiJ job's `.err` file.
- A diagnostic block users can paste into their setup snippet for a one-
  shot sanity dump (we used this on Odo):

  ```sh
  echo "post-setup LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-(unset)}"
  echo "post-setup PATH=$PATH"
  echo "post-setup python3: $(command -v python3)"
  echo "post-setup mpiexec: $(command -v mpiexec)"
  ```

  with the caveat that on Cray, `mpiexec` is intentionally absent (Cray
  uses `srun`) and `ldconfig -p` won't list cray-mpich libs (they live in
  `CRAY_LD_LIBRARY_PATH`, not `/etc/ld.so.conf.d/`).  These were both
  red-herring checks in our debugging session.

- A site-example block — Odo: requires `cray-python/3.11.7` for a working
  Python with headers; **must not** load `cray-mpich` / `PrgEnv-gnu` while
  dragon is also being launched, or dragon's infra hangs.

### In `rhapsody` (separate PR, later)

The rhapsody V3 backend should ship documentation around its MPI / runtime
expectations.  Items worth covering:

- Dragon's runtime brings `libdfabric_ofi.so` / `libdfabric_tcp.so` /
  `libdfabric_ucx.so` (under `dragon/lib/`).  Loading a vendor MPI that
  also manages OFI / UCX state in the same parent process is known to hang
  dragon's infra init (concrete case: cray-mpich/8.1.31 on HPE Cray EX).
- The function-task execution model: dragon `ProcessGroup` workers run the
  user function natively — there is no automatic stdout/stderr capture in
  Dragon's `results_ddict` for ranks that exit non-zero, only a dragon-side
  trace for `DragonUserCodeError`.  Rhapsody's `_v3_function_wrapper`
  closes that gap by writing per-rank Python tracebacks to files under
  `_work_dir` and folding them into the failure path's exception message.
  Files are named `<uid>.stderr.<rank>` (and `.stdout.<rank>`) and persist
  for post-mortem.
- For mpi4py inside ranks: mpi4py 4.x uses a runtime ABI loader
  (`mpi4py/_mpiabi.py`); its dlopen search starts at `<venv>/lib/libmpi.so`.
  Sites needing MPI inside function tasks should either (a) build a vendor-
  independent MPICH into the venv prefix, or (b) push the vendor `module
  load` into the rank's process env without leaking it to dragon's parent
  process.  No turnkey recipe yet for option (b) on Cray.

### Site-specific recipes (where they should live)

Open question — this repo, `rhapsody`, or a separate "site-cookbook"?
Recommendation: a `docs/sites/` directory in `radical.edge` (since
`RADICAL_EDGE_SETUP` lives here), with one short markdown per site
(e.g. `odo.md`, `perlmutter.md`, `aurora.md`) containing:

- Working setup snippet (modules, env vars).
- Tunnel mode (none / forward / reverse) and rationale.
- Known compatibility caveats (Odo: no cray-mpich at parent level).
- Last-verified date and pip install command.

---

## Status Summary

| Component                                  | State    |
|--------------------------------------------|----------|
| V3 silent-traceback capture                | Landed   |
| Edge `RADICAL_EDGE_SETUP` hook             | Landed   |
| PsiJ keep-files toggle                     | Landed   |
| MPI functions on Odo (compute-side)        | Parked   |
| Edge `docs/site_setup.md`                  | TODO     |
| Rhapsody MPI compatibility doc             | Future PR|
| Site cookbook (`docs/sites/odo.md` etc.)   | TODO     |

The rhapsody fix is the load-bearing piece for visibility — without it,
any future MPI-on-dragon debugging is blind.  The MPI-on-Cray work is
deferred until a workload actually needs it.
