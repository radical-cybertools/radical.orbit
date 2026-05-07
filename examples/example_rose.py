#!/usr/bin/env python3

"""
ROSE active learning over RADICAL Edge
──────────────────────────────────────

A self-contained, end-to-end illustration of how to drive a ROSE
``SequentialActiveLearner`` workflow against a remote HPC node via the
RADICAL Edge bridge.  The compute itself is an MPI simulation that
shares state through a Dragon distributed dictionary (DDict) — but the
focus of this example is the *plumbing*:

    Client (this script)
        │
        ▼  rhapsody.get_backend('edge')      ← auto-discovers a suitable
        │                                       edge through the bridge
        ▼  WorkflowEngine.create(engine)     ← asyncflow on top
        │
        ▼  SequentialActiveLearner(asyncflow) ← ROSE driver
        │
        ▼  @acl.simulation_task / .training_task / .active_learn_task
                                              ← function tasks shipped to
                                                the edge for execution

What makes this example interesting
───────────────────────────────────
  • **Edge auto-selection** — no bridge URL or edge name hard-coded.
    ``rhapsody.get_backend('edge')`` reads ``RADICAL_BRIDGE_URL`` from
    the environment and picks the first connected edge that advertises
    a Rhapsody plugin (defaults: ``https://localhost:8000`` is *not*
    assumed; set ``RADICAL_BRIDGE_URL`` if your bridge runs elsewhere).

  • **Closure discipline** — DragonExecutionBackendV3 cloudpickles every
    function task before launching it.  Live DDict handles, the ACL
    object, asyncio internals, etc. are *not* portable.  The single
    captured variable in every task below is ``ddict_descriptor`` (a
    plain ``str``); each task re-attaches to the DDict on the remote
    side and derives the current AL iteration from sentinel keys.

  • **Cumulative training** — the GP surrogate trains on samples from
    *all* completed iterations, not just the latest batch — that is
    what active learning expects.

DDict key layout (shared across all Dragon-managed processes)
──────────────────────────────────────────────────────────────
    sim_meta_iter_{i}       rank-0 sentinel — also used to detect the
                            current iteration without capturing ACL
    sim_rank_{r}_iter_{i}   per-rank simulation samples
    model_iter_{i}          trained GP surrogate
    mse_iter_{i}            scalar MSE for that surrogate
    query_points_iter_{i}   AL-selected query points for next iteration

Run
───
    # In one terminal: start the bridge
    ./bin/radical-edge-bridge.py

    # In another: start an edge with the rhapsody plugin loaded
    ./bin/radical-edge-wrapper.sh

    # Then:
    export RADICAL_BRIDGE_URL=https://localhost:8000   # optional
    python examples/example_rose.py
"""

import asyncio
import logging

import numpy as np
from sklearn.gaussian_process         import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, WhiteKernel
from sklearn.metrics                  import mean_squared_error

import rhapsody
from radical.asyncflow            import WorkflowEngine
from radical.edge.logging_config  import configure_logging
from rose.al.active_learner       import SequentialActiveLearner
from rose.metrics                 import MEAN_SQUARED_ERROR_MSE


rhapsody.enable_logging(level=logging.INFO)
configure_logging(level=logging.INFO)


# ── Configuration ─────────────────────────────────────────────────────────────
N_MPI_RANKS:        int   = 4      # MPI ranks per simulation launch
N_SAMPLES_PER_RANK: int   = 5      # sparse start — AL drives exploration
N_QUERY:            int   = 8      # query points selected per AL step
MSE_THRESHOLD:      float = 0.01   # convergence target
MAX_ITER:           int   = 15     # hard cap on iterations


# ── 1. Edge backend ───────────────────────────────────────────────────────────
async def make_engine():
    """Build a Rhapsody Edge backend and wrap it in an asyncflow engine.

    With no arguments, ``get_backend('edge')`` resolves the bridge URL
    from ``$RADICAL_BRIDGE_URL`` and auto-selects the first connected
    edge advertising an enabled ``rhapsody`` plugin.  A ``RuntimeError``
    surfaces from ``await backend`` if no candidate is found.
    """
    backend = rhapsody.get_backend('edge')
    engine  = await backend
    print(f"Bridge: {backend._bridge_url}")
    print(f"Edge:   {backend._edge_name}")
    return await WorkflowEngine.create(engine)


# ── 2. Shared-state task: create / destroy DDict on the edge ──────────────────
def make_ddict_tasks(asyncflow):

    @asyncflow.function_task
    async def create_ddict() -> str:
        from dragon.data.ddict.ddict import DDict
        ddict = DDict(
            managers_per_node = 1,
            n_nodes           = 1,
            total_mem         = 512 * 1024 * 1024,
            wait_for_keys     = True,
            working_set_size  = MAX_ITER + 2,
        )
        descriptor = ddict.serialize()
        print(f"[ROSE] DDict ready (descriptor prefix: {descriptor[:32]}…)")
        return descriptor

    @asyncflow.function_task
    async def destroy_ddict(descriptor: str):
        from dragon.data.ddict.ddict import DDict
        DDict.attach(descriptor).destroy()

    return create_ddict, destroy_ddict


# ── 3. AL pipeline tasks ──────────────────────────────────────────────────────
#
#   Closure rule: every task captures ONLY ``ddict_descriptor`` (a plain
#   string).  The current iteration is derived from sentinel keys in
#   the DDict — no reference to the outer ``acl`` or DDict object.
#
def register_al_tasks(acl, ddict_descriptor):

    @acl.simulation_task(as_executable=False)
    async def simulation(
        *args,
        task_description = {"process_templates": [(N_MPI_RANKS, {})]},
    ):
        """MPI simulation — Dragon launches this body on N_MPI_RANKS ranks."""
        from mpi4py                 import MPI
        from dragon.data.ddict.ddict import DDict

        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

        ddict = DDict.attach(ddict_descriptor)

        # current iter = first missing sim_meta_iter_{i} sentinel
        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1

        prev_iter = iteration - 1
        query_key = f"query_points_iter_{prev_iter}"

        if prev_iter >= 0 and query_key in ddict:
            # AL-selected query points — partition across ranks
            all_query = ddict[query_key]
            X_local   = all_query[rank::size]
        else:
            # iter 0: no prior AL output — sample randomly
            rng     = np.random.default_rng(seed=rank + iteration * size)
            X_local = rng.uniform(0.0, 2.0 * np.pi,
                                  (N_SAMPLES_PER_RANK, 1))

        rng     = np.random.default_rng(seed=rank + iteration * size)
        y_local = (np.sin(X_local) * np.sin(5 * X_local)
                   + rng.normal(0.0, 0.1, X_local.shape))

        ddict[f"sim_rank_{rank}_iter_{iteration}"] = {"X": X_local,
                                                       "y": y_local}

        comm.Barrier()
        if rank == 0:
            ddict[f"sim_meta_iter_{iteration}"] = {
                "n_ranks":            size,
                "n_samples_per_rank": len(X_local),
            }
            print(f"[mpi_sim]  iter={iteration} | ranks={size} | "
                  f"total_pts={size * len(X_local)}", flush=True)

        ddict.detach()
        return {}

    @acl.training_task(as_executable=False)
    async def training(*args):
        """Train a GP surrogate on *all* simulation samples so far."""
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"sim_meta_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        # Cumulative training set across every completed iteration
        X_parts, y_parts = [], []
        for it in range(iteration + 1):
            meta = ddict[f"sim_meta_iter_{it}"]
            for r in range(meta["n_ranks"]):
                data = ddict[f"sim_rank_{r}_iter_{it}"]
                X_parts.append(data["X"])
                y_parts.append(data["y"])

        X_train = np.vstack(X_parts)
        y_train = np.vstack(y_parts).ravel()

        kernel = (RBF(length_scale=0.3, length_scale_bounds=(0.01, 5.0))
                  + WhiteKernel(noise_level=1e-2))
        gp     = GaussianProcessRegressor(kernel=kernel,
                                          n_restarts_optimizer=10,
                                          normalize_y=True)
        gp.fit(X_train, y_train)

        X_test = np.linspace(0.0, 2.0 * np.pi, 300).reshape(-1, 1)
        y_pred = gp.predict(X_test)
        y_true = (np.sin(X_test) * np.sin(5 * X_test)).ravel()
        mse    = float(mean_squared_error(y_true, y_pred))

        ddict[f"model_iter_{iteration}"] = gp
        ddict[f"mse_iter_{iteration}"]   = mse

        print(f"[train]    iter={iteration} | n_train={len(X_train)} | "
              f"MSE={mse:.6f}", flush=True)
        ddict.detach()
        return {}

    @acl.active_learn_task(as_executable=False)
    async def active_learn(*args):
        """Max-variance acquisition; writes query points to DDict."""
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"model_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        gp: GaussianProcessRegressor = ddict[f"model_iter_{iteration}"]

        X_candidates = np.linspace(0.0, 2.0 * np.pi, 500).reshape(-1, 1)
        _, std       = gp.predict(X_candidates, return_std=True)
        top_idx      = np.argsort(std)[-N_QUERY:]

        ddict[f"query_points_iter_{iteration}"] = X_candidates[top_idx]

        mean_unc = float(std.mean())
        max_unc  = float(std.max())
        print(f"[active]   iter={iteration} | mean_unc={mean_unc:.4f} | "
              f"max_unc={max_unc:.4f} | n_query={N_QUERY}", flush=True)
        ddict.detach()
        return {"mean_uncertainty": mean_unc,
                "max_uncertainty":  max_unc}

    @acl.as_stop_criterion(metric_name=MEAN_SQUARED_ERROR_MSE,
                           threshold=MSE_THRESHOLD,
                           as_executable=False)
    async def check_mse(*args) -> float:
        """Read the latest MSE for ROSE's stop check."""
        from dragon.data.ddict.ddict import DDict

        ddict = DDict.attach(ddict_descriptor)

        iteration = 0
        while f"mse_iter_{iteration}" in ddict:
            iteration += 1
        iteration -= 1

        mse: float = ddict[f"mse_iter_{iteration}"]
        print(f"[check]    iter={iteration} | MSE={mse:.6f} "
              f"(threshold < {MSE_THRESHOLD})", flush=True)
        ddict.detach()
        return mse


# ── 4. Driver ─────────────────────────────────────────────────────────────────
async def main() -> None:

    asyncflow = await make_engine()
    acl       = SequentialActiveLearner(asyncflow)

    create_ddict, destroy_ddict = make_ddict_tasks(asyncflow)
    ddict_descriptor            = await create_ddict()

    register_al_tasks(acl, ddict_descriptor)

    print("\n[ROSE] Starting active-learning loop\n" + "─" * 60)
    final_state = None
    async for state in acl.start(max_iter=MAX_ITER):
        final_state = state
        print(f"\n[ROSE]  ── iter={state.iteration:2d}"
              f" | MSE={state.metric_value:.6f}"
              f" | mean_unc={state.mean_uncertainty}"
              f" | should_stop={state.should_stop}\n", flush=True)
        if state.should_stop:
            break

    # Convergence summary (history is on IterationState; no DDict access here)
    print("\n── Convergence Summary "
          "──────────────────────────────────────────────")
    if final_state and final_state.metric_history:
        for i, mse in enumerate(final_state.metric_history):
            print(f"  iter {i:2d} │ MSE = {mse:.6f}")
        print(f"  final   │ MSE = {final_state.metric_value:.6f}")
    else:
        print("  (no iterations ran)")

    # Cleanup — destroy the DDict on the edge (client has no Dragon runtime)
    await destroy_ddict(ddict_descriptor)
    await acl.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
