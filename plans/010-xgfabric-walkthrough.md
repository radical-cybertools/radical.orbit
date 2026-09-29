# Plan: #10 — xGFabric usage walkthrough: client scripts + expected payloads

Issue: https://github.com/radical-cybertools/radical.orbit/issues/10
Status check (2026-08-25): **partial**. Committed today:
`examples/xgfabric.py` (working client: get_status, start_workflow,
workflow_status SSE stream, stop_workflow), the xgfabric route table in
`docs/source/rest_api.rst` (request bodies for workdir/config/start + the
repo-wide error envelope), and the API summary in CLAUDE.md.

Note the issue's requested verbs (`register/list/describe/allocate/submit`,
`rid`/`aid`) are pre-flip vocabulary — the shipped plugin's surface is
config-based (`list_configs`/`load_config`/`save_config`/`delete_config`,
`get_workdir`/`set_workdir`, `get_status`, `start_workflow(workflow,
resource)`, `stop_workflow`) with registration implicit via broker topology.
The walkthrough should document the *current* surface, and the issue can be
re-scoped accordingly when this lands.

## Missing pieces (the actual work)

1. **Config-management walkthrough**: nothing committed exercises
   `list_configs → load_config → (edit) → save_config → delete_config` or
   `get_workdir`/`set_workdir`. Add a second example
   (`examples/xgfabric_configs.py`) or a `--list/--show-config` mode on the
   existing script, driving the full config lifecycle including the
   `'default'`/`'test'` builtins.
2. **Response payloads documented**: the status dict shape (`status`,
   `phase`, `progress`, `active_cluster`, `current_batch`/`total_batches`,
   `completed_simulations`, `immediate_clusters`, `allocate_clusters`,
   `log[]`) exists only implicitly in the example's printer. Write it up —
   proposal: a `docs/source/plugin_xgfabric.rst` page (mirroring the IRI
   plugin reference from PR #81) with: client API, request/response payloads
   per route, the `workflow_status` notification payload, and the
   immediate/allocate classification rule.
3. **Error-condition catalog**: document the concrete errors the plugin
   raises per route — `400 Directory not found` (plugin_xgfabric.py:249),
   `400 Config name is required` (:314), `404 Config not found` (:303,
   :330), `404 Resource config not found` (:491), `409 Workflow already
   running` (:451), `409 No workflow running` (:498) — in the same doc page,
   referencing the shared error envelope from rest_api.rst.

## Deliverables

- `examples/xgfabric_configs.py` (or extended `examples/xgfabric.py`).
- `docs/source/plugin_xgfabric.rst` + toctree entry.
- Close-comment for the issue translating old verbs → current API.

## Effort

Small; documentation-heavy, no plugin code changes. Payload shapes should be
captured from a live run (untracked `demo/`/`configs/` material on this
machine may already contain sample outputs to lift). Coordinate with #13
(demo) and #20 (multi-resource) — one doc page can serve all three; keep the
walkthrough happy-path here, failure cases belong to #13.
