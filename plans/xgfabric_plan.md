# Plan: Dragon V3 Support for Edge Rhapsody Plugin

## Context

The Edge Rhapsody plugin wraps Rhapsody's Session API to submit/monitor tasks on remote HPC nodes via REST. Currently it covers ~15-20% of Dragon V3's capabilities. The goal is to close the major gaps so the plugin can properly drive a Dragon V3 backend. All changes are in the Edge plugin — no Rhapsody-side modifications needed.

## Key Insight

`BaseTask.from_dict()` already preserves extra fields (including `task_backend_specific_kwargs` and `backend`) via `**extra_kwargs → self.update(kwargs)`. Per-task backend routing also works out of the box — `Session.submit_tasks()` routes via `task["backend"]`. So several "gaps" are already free; we just need to harden `_sanitize_task()` and add a few routes.

## Implementation Phases (in order)

### Phase 1: Resource Spec Passthrough + `_sanitize_task` Hardening

**What:** Ensure `task_backend_specific_kwargs` (timeout, ranks, type, process_template/cwd) flows through cleanly, and harden `_sanitize_task` for V3 edge cases.

**Changes to `_sanitize_task()` in `RhapsodySession`:**
- Stringify callable `function` fields → `"module.qualname"`
- Stringify non-JSON-serializable `return_value` (e.g. Dragon DataReference)
- Decode `bytes` stdout/stderr → `str`
- Join `list` stdout/stderr (multi-rank) → single string

**Changes to UI config:**
- Add fields: `timeout` (number), `ranks` (number), `type` (select: mpi/blank), `cwd` (text)
- Pack these into `task_backend_specific_kwargs` in the submit form

**File:** `src/radical/edge/plugin_rhapsody.py`

### Phase 2: Function Task Serialization (cloudpickle)

**What:** Allow submitting Python callables over REST via cloudpickle + base64 encoding.

**Wire format:**
```json
{"function": "cloudpickle::<base64>", "args": "<base64>", "_args_pickled": true}
```

**Client-side** (`RhapsodyClient.submit_tasks`): if `task["function"]` is callable, serialize with cloudpickle+base64, same for args/kwargs.

**Server-side** (`RhapsodySession.submit_tasks`): detect `cloudpickle::` prefix, deserialize before `BaseTask.from_dict()`.

**Safety:** Add `allow_function_tasks` flag on session (default `True`). The Edge already runs user code on authenticated HPC nodes, so pickle deserialization is acceptable.

**Dependency:** `cloudpickle` (already common in HPC stacks).

**File:** `src/radical/edge/plugin_rhapsody.py`

### Phase 3: `fence()` Route

**What:** Expose Dragon V3's batch synchronization barrier.

**New route:** `POST /rhapsody/fence/{sid}` with optional `{"backend": "name"}` body.

**Implementation:** Look up backend, check `hasattr(backend, 'fence')`, call it, return `{"status": "ok"}`.

**Files:** `src/radical/edge/plugin_rhapsody.py` (session + plugin + client)

### Phase 4: `cancel_all_tasks` Route

**What:** Bulk cancel by iterating tracked non-terminal tasks.

**New route:** `POST /rhapsody/cancel_all/{sid}`

**Implementation:** Iterate `self._tasks`, skip terminal states, call `cancel_task()` per task, return `{"canceled": N}`.

**Files:** `src/radical/edge/plugin_rhapsody.py` (session + plugin + client)

### Phase 5: AITask UI (low priority)

**What:** Add a second submit form in `ui_config` for AI tasks (prompt, model, temperature, max_tokens).

**File:** `src/radical/edge/plugin_rhapsody.py` (ui_config only)

## What NOT to Do

- **DDict:** Cannot be proxied over REST (shared-memory primitive). Users should use it within function tasks. Document this, don't expose it.
- **Data dependencies:** V3 has stubs only (both methods are `pass`). No gap to close.
- **Worker pool visibility:** No runtime introspection API in Dragon V3. Defer.
- **Modify Rhapsody itself:** All changes are Edge-side only.

## Files Modified

- `src/radical/edge/plugin_rhapsody.py` — all phases
- `tests/unittests/test_plugin_rhapsody.py` — new tests per phase
- `CLAUDE.md` — update API docs (new routes, function task support)

## Test Plan

New unit tests:
1. `test_submit_with_backend_specific_kwargs` — kwargs survive round-trip
2. `test_sanitize_callable_function` — callable → string
3. `test_sanitize_non_serializable_return_value` — stringified
4. `test_sanitize_bytes_stdout` — decoded
5. `test_sanitize_list_stdout` — joined
6. `test_function_task_cloudpickle_roundtrip` — serialize/deserialize
7. `test_function_task_disabled` — error when `allow_function_tasks=False`
8. `test_fence_route` — calls `backend.fence()`
9. `test_fence_unsupported_backend` — 400 error
10. `test_cancel_all_tasks` — bulk cancel returns correct count

Verification: `pytest tests/unittests/test_plugin_rhapsody.py -v` + `flake8 src/radical/edge/plugin_rhapsody.py`
