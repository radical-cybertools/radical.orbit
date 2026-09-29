# Plan: #93 — Normalize job-state vocabulary across plugins

Issue: https://github.com/radical-cybertools/radical.orbit/issues/93
Status check (2026-08-25): **still open** — no `normalize_job_state` /
`state_norm` anywhere in src/; `plugin_psij.py` still has its private
`_normalize_state` (line 81); `queue_info_slurm.py` / `queue_info_pbs.py`
still emit raw scheduler strings in job listings.

## Design (as proposed in the issue — non-destructive)

### 1. Shared normalizer in `batch_system.py`

```python
def normalize_job_state(raw: str) -> str:
    """Map a raw scheduler state string to a STATE_* constant."""
```

- Case-insensitive; accepts both SLURM long forms (`PENDING`, `COMPLETED`,
  `NODE_FAIL`, `OUT_OF_MEMORY`, …) and short codes (`PD`, `R`, `CG`, `CD`,
  `F`, `CA`, `TO`, `OOM`, …) as emitted by `squeue -h -o %t`, plus PBSPro
  single-letter codes (`Q`, `R`, `H`, `E`, `F`, `S`, `W`, `B`, `X`).
  Unknown → `STATE_UNKNOWN` (never raise).
- Also accept PsiJ `JobState` names so `psij._normalize_state` can delegate.
- Seed the tables from the two existing per-backend maps: the SLURM/PBS
  `BatchSystem` subclasses already normalize *their own* `job_status()`
  states — hoist/share those tables rather than writing a third copy
  (single source of truth; check `batch_system_slurm.py` /
  `batch_system_pbs.py` for the existing dicts and reuse them).
  A short-code collision between SLURM and PBS single letters is harmless in
  practice (`R`→RUNNING, `F`→FAILED/DONE differs: PBS `F` is "finished",
  needs exit-status to disambiguate) — where ambiguous, allow an optional
  `backend=` hint: `normalize_job_state(raw, backend='pbs')`; the queue_info
  backends know which scheduler they are.

### 2. `queue_info`: add `state_norm` alongside raw `state`

- `queue_info_slurm.py` and `queue_info_pbs.py`: every job dict in
  `list_jobs` / `list_all_jobs` gains `state_norm: STATE_*`; the raw
  `state` field is untouched (so #90's Explorer `jobStateBadge` raw-string
  map keeps working).
- `queue_info_none.py`: nothing to do (no jobs).

### 3. `plugin_psij`: re-point `_normalize_state`

- Keep the function (call sites at plugin_psij.py:183,328,363 unchanged) but
  make its body delegate to `batch_system.normalize_job_state`, preserving
  psij-specific pre-mapping (PsiJ `JobState` enum → str) if needed.
- Verify the psij status vocabulary the clients/Explorer see is unchanged
  (it already emits normalized-ish states; the delegation must be
  value-identical for the common states — write a table test).

### 4. Explorer (optional, small)

- `jobStateBadge` can grow a fallback: if `state_norm` present, badge from
  the normalized value, else the existing raw map. Not required by the
  issue; do it only if trivial.

## Tests

- `tests/unittests/test_batch_system*.py`: table-driven mapping test —
  every SLURM long form + short code, every PBS letter, PsiJ names, garbage
  input → UNKNOWN; ambiguity-hint cases (`F` with/without `backend='pbs'`).
- queue_info slurm/pbs listing tests: mock scheduler output → assert both
  `state` (raw, unchanged) and `state_norm` present and correct.
- psij: `_normalize_state` delegation equivalence test over the states the
  old implementation handled.

## Out of scope

HTTP-error envelope (done in #91). Changing any raw field. Explorer rework.

## Effort / risk

Small, low-risk, self-contained. One PR. Risk to watch: consumers that
already string-compare psij states — delegation must not change emitted
values (the equivalence test guards this).
