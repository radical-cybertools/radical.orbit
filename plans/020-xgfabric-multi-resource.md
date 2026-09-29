# Plan: #20 — xGFabric multi-resource example (documented use case)

Issue: https://github.com/radical-cybertools/radical.orbit/issues/20
Status check (2026-08-25): **not done** as an example — but the *capability*
is committed: `plugin_xgfabric._classify()` splits topology into
`immediate_clusters` / `allocate_clusters` (unit-tested, incl. config
overrides), `_submit_pilot` / `_migrate_data` / `_run_simulations` implement
batch abort-and-migrate, and `data/xgfabric_resource_test.json` already
defines three clusters (anvil, frontier, ucsb). `examples/xgfabric.py` takes
exactly one `--endpoint` and one `--resource`; nothing drives multiple
resources, and no doc captures expected multi-resource behavior/limitations.

## Work

1. **Example**: extend `examples/xgfabric.py` (or add
   `examples/xgfabric_multi.py`) to:
   - start N endpoints' worth of topology (for a stub run: two local
     endpoints with different plugin sets so one classifies immediate and
     one allocate — queue_info presence is the classifier);
   - load a resource config naming both clusters (derive from
     `xgfabric_resource_test.json`);
   - run the mock workflow and print the classification + which cluster
     each batch lands on;
   - demonstrate migration: drop/disconnect the active cluster mid-run and
     show the workflow migrating to the other (the abort-and-migrate path),
     or — if that's too flaky for a scripted example — trigger it via the
     config override (`force immediate/allocate`) between runs.
2. **Documented use case**: a "multi-resource operation" section in the
   `docs/source/plugin_xgfabric.rst` page from #10:
   - the two-tier model (immediate vs allocate) and the classification rule
     + config override;
   - batch semantics and what abort-and-migrate does and does not preserve
     (lift the behavior/limitations narrative out of
     `plans/xgfabric_strategy.md`, which still uses pre-flip edge/bridge
     vocabulary — refresh the terms while moving it);
   - allocation flow on the allocate tier (pilot submit via psij/SLURM),
     with the caveat list (what happens when the allocation never comes up,
     interaction with #115 supersede once that lands).
3. Keep it stub-friendly: the issue explicitly allows stubs — the mock
   sensor + two local endpoints is the deliverable; a real
   Frontier+Anvil run is a bonus, not the bar.

## Dependencies / ordering

After #10 (doc page exists) and ideally after #13 (harness patterns for
multi-endpoint spin-up). The migration demo doubles as a failure-case for
#13 if written test-shaped — consider landing #13 and #20 as one PR pair.

## Effort

Small-medium. Example + docs; only plugin change would be if the
mid-run-migration demo exposes rough edges (likely — budget for small fixes).
