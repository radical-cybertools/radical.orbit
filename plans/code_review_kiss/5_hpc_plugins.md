# Review 5 — HPC-facing plugins and backends (KISS / structural)

Scope: `plugin_psij.py`, `plugin_queue_info.py`, `queue_info*.py`, `batch_system*.py`,
`tunnel.py`, `plugin_sysinfo.py`, `plugin_staging.py`.

## Overall impression

The simple plugins (staging, sysinfo, queue_info) mostly use the framework the
way a student would hope: Session holds domain state, Plugin registers routes
and `_forward`s, Client formats URLs — the pattern is learnable from any one of
them in ten minutes. Complexity concentrates almost entirely in `plugin_psij.py`,
whose tunnel-watcher machinery (~430 lines of the file) is an order of magnitude
harder to follow than everything else in scope combined. The two parallel
backend hierarchies (`batch_system_*` for imperative scheduler ops,
`queue_info_*` for cached reporting) are individually clean but the boundary
between them leaks into the plugin layer, and the codebase carries **three
different job-state vocabularies plus three separate state-override layers**,
which is the single biggest comprehension hazard for a student debugging "why
does my job show FAILED". A sprinkling of back-compat shims and inconsistent
client idioms adds noise that the project, which controls all its callers,
does not need.

---

## Finding 1 — `_tunnel_watcher` is a 230-line, four-concern state machine

- **Location**: `src/radical/orbit/plugin_psij.py:970-1204` (`PluginPSIJ._tunnel_watcher`), plus its satellites `_await_reverse_teardown` (1206-1230), `_fail_tunnel` (1232-1263), `_dispatch_cancel` (1265-1279).
- **What & why it hurts**: One coroutine interleaves (a) scheduler job-state polling with an UNKNOWN-streak heuristic, (b) rendezvous-file watching including an NFS negative-lookup-cache workaround via `os.listdir` (1074-1079), (c) a 30-attempt ssh-spawn retry loop written as `for/else` (1109-1141) with a nested terminal-state re-check inside the exception handler, and (d) teardown responsibility for a process it may or may not have spawned, split between an inline branch, a `finally`, and a helper. `forward` and `reverse` modes share the loop but only ~20% of the body; every iteration a reader must re-derive which mode they are in. Failure reporting takes yet another path: `_fail_tunnel` writes `_failure_reasons[job_id]`, which `get_job_status` (784-788) later uses to rewrite the state — an invisible side channel between two methods 400 lines apart. `_dispatch_cancel` then has to *search all sessions' private `_jobs` dicts* to find the owner (1273-1277) because the watcher never recorded which session submitted the job. Debugging any tunnel failure requires holding all of this in one's head.
- **Simpler alternative**: Split by mode: `_watch_forward(...)` (~25 lines: poll state, return when file appears or job dies) and `_watch_reverse(...)`, each straight-line. Move rendezvous-file knowledge (the `.port`/`.pid`/`.req` naming, existence-with-NFS-workaround, cleanup) into `tunnel.py` as three small functions (`rendezvous_read`, `rendezvous_wait`, `rendezvous_clear`) so the watcher reads as policy, not file plumbing. Pass the owning `sid` into the watcher so `_fail_tunnel` can call `self._forward(sid, PSIJSession.cancel_job, ...)` instead of scanning sessions.
- **Essential?**: The *behavior* is essential — outbound-only firewall traversal is a named architectural requirement, and site quirks (NFS caching, pam_slurm_adopt races) are real. But nothing forces a single monolithic coroutine; the same requirement decomposes cleanly per mode.
- **Severity**: high
- **Effort**: medium

## Finding 2 — Three job-state vocabularies and three state-override layers

- **Location**:
  - psij vocabulary: `plugin_psij.py:76` (`TERMINAL_STATES = {'COMPLETED','FAILED','CANCELED'}` — one L);
  - normalized vocabulary: `batch_system.py:27-35` (`STATE_*`, `'CANCELLED'` — two Ls, `TERMINAL_STATES = {DONE,FAILED,CANCELLED}`);
  - display vocabulary: `queue_info_pbs.py:98-111` (`_STATE_DISPLAY`, a third mapping of the *same* PBS letters already mapped in `batch_system_pbs.py:22-35`).
  - Overrides: `PSIJSession._cancelled_jobs`/`_effective_state` (`plugin_psij.py:131,140-151`), `PBSProBatchSystem._cancelled` (`batch_system_pbs.py:151-158,192-194`), `PluginPSIJ._failure_reasons` (`plugin_psij.py:747,784-788`).
- **What & why it hurts**: A student tracing one job's state sees it translated up to three times, with two near-identical constants named `TERMINAL_STATES` in different modules whose members differ (`COMPLETED` vs `DONE`, `CANCELED` vs `CANCELLED` — a one-letter trap that will absolutely bite someone comparing strings). Worse, the "remember cancel intent, rewrite terminal state to CANCELLED" trick is implemented **twice independently** — once in the psij session against psij states, once in the PBS batch backend against normalized states — and then a third layer (`_failure_reasons`) rewrites CANCELLED back to FAILED. Which layer wins depends on which code path queried the state. This is the hardest-to-debug structure in the whole scope.
- **Simpler alternative**: (1) Adopt the `batch_system.py` normalized vocabulary as *the* vocabulary everywhere outside raw parsers: map psij's states into it at the `_normalize_state` boundary (`plugin_psij.py:79-82`) and derive `queue_info_pbs`'s display strings from it (or delete `_STATE_DISPLAY` and reuse `batch_system_pbs._STATE_MAP`, which the module already imports other parsers from). (2) Keep cancel-intent tracking in exactly one place — the session that received the cancel request — and delete `PBSProBatchSystem._cancelled` (which also makes the "stateless" claim in `batch_system.py:39-43` true again; today the PBS backend is stateful in a cached singleton, contradicting its own base-class docstring).
- **Essential?**: Distinguishing operator-cancel from tunnel-failure is a real requirement (use case 7's "reclaimed only when the owner truly goes away" adjacent). Three vocabularies and duplicated intent-tracking are accidental.
- **Severity**: high
- **Effort**: medium

## Finding 3 — The `batch_system_*` / `queue_info_*` split leaks; the plugin uses both interchangeably

- **Location**: `plugin_queue_info.py:113-121` (`QueueInfoSession.cancel_job` bypasses `self._backend` and calls `detect_batch_system().cancel`), `plugin_queue_info.py:407-421` (`nodelist_endpoint` ditto, with an apologetic comment: "nodelist lives on the BatchSystem hierarchy … not on QueueInfo"), `queue_info.py:288-316` (`make_queue_info` dispatches *on the BatchSystem's* `psij_executor`), `queue_info_pbs.py:16` (imports its parsers from `batch_system_pbs`).
- **What & why it hurts**: There are two abstract bases, two registries/factories, and two slurm/pbs/none backend triples for what a student perceives as one concept: "talk to the scheduler". The intended split (BatchSystem = uncached imperative ops; QueueInfo = cached bulk reporting) is never stated where a reader would look, and the queue_info *plugin* itself doesn't respect it — half its routes go to `self._backend`, the other half to `detect_batch_system()`. So the abstraction does not even spare its primary consumer from knowing both hierarchies; it only doubles the files to read. The cross-import of parsers shows the real seam is "SLURM code vs PBS code", not "batch ops vs queue info".
- **Simpler alternative**: Either (a) merge per scheduler: one `SlurmBackend` / `PbsBackend` exposing both the imperative ops and the (cached) collection methods, one registry, one `detect()` — the caching mixin from `queue_info.QueueInfo._get_cached` stays as-is; or, if the split must stay, (b) give `QueueInfo` an explicit `self.batch` (the detected `BatchSystem`) at construction and route `cancel_job`/`nodelist` through the QueueInfo backend, so the plugin only ever talks to one object and the seam is visible in one constructor line instead of scattered `detect_batch_system()` calls.
- **Essential?**: Supporting multiple schedulers is essential (plugins own domain semantics). Two parallel hierarchies for one scheduler is accidental — the requirement is satisfied by one backend class per scheduler.
- **Severity**: high (comprehension: a student must reverse-engineer an undocumented two-hierarchy design to add a scheduler)
- **Effort**: large (option a) / small (option b)

## Finding 4 — Job-status notification logic duplicated in callback and poller (and can double-fire)

- **Location**: `plugin_psij.py:274-300` (`_on_status` closure inside `submit_job`) vs `plugin_psij.py:439-466` (`_poll_jobs` body).
- **What & why it hurts**: Both blocks do the identical sequence — normalize state, apply `_effective_state`, dedupe against a last-seen state, read stdout/stderr if terminal, `_dispatch_notify("job_status", …)` — but each keeps its **own** dedupe store (`last_state` nonlocal in the closure vs `self._job_states` dict in the poller). Because the stores are independent, the same transition can be notified twice (once by psij's callback, once by the next poll), and any future change to the payload must be made in two places. A student asked "where do job_status events come from" finds two answers.
- **Simpler alternative**: One method `PSIJSession._notify_state(job_id, status)` that does normalize/override/dedupe (against the single `self._job_states`) and notify; the psij callback and the poll loop both call it. ~30 lines deleted, one dedupe store, double-fire impossible.
- **Essential?**: Having both a callback and a poll fallback may be essential (backend callback reliability varies); duplicating the body is not.
- **Severity**: medium
- **Effort**: small

## Finding 5 — Staging plugin bypasses `_forward` and hand-rolls session lookup + error mapping three times; sessions across plugins disagree on who raises HTTP errors

- **Location**: `plugin_staging.py:380-481` (`put_endpoint`, `get_endpoint`, `list_endpoint` each do `self._sessions.get(sid)` → manual 404 → try/except mapping `FileExistsError/PermissionError/ValueError/...` to status codes). Contrast `plugin_psij.py:767-769` and every other plugin, which route through `Plugin._forward` (`plugin_base.py:756-799`).
- **What & why it hurts**: Three consequences. (1) The framework's `_forward` also handles session expiry (410) and last-access bookkeeping — staging sessions silently never get either, an invisible behavioral divergence a student won't suspect. (2) The near-identical try/except ladder is pasted three times. (3) It exposes an unresolved project-wide convention: `PSIJSession` raises `HTTPException` *inside* session methods (transport type in domain code, e.g. `plugin_psij.py:312,322`), while `StagingSession` raises domain exceptions and maps them in the plugin. Both conventions coexist in the same codebase; a student writing plugin #12 has no way to know which to copy.
- **Simpler alternative**: Use `_forward` in staging and pick one convention repo-wide. Cleanest: sessions raise domain exceptions; add a tiny per-plugin `exception_map = {FileExistsError: 409, PermissionError: 403, ...}` honored by `_forward`'s except clause. That deletes staging's three ladders and de-HTTP-ifies psij's session.
- **Essential?**: Mapping domain errors to HTTP codes is essential (compat ingress requirement); doing it differently in every plugin is not.
- **Severity**: medium
- **Effort**: small

## Finding 6 — PSIJClient mixes two client idioms in one class; no other client in scope uses the async-core pattern

- **Location**: `plugin_psij.py:487-505,541-552,641-661` (plain sync via `self._http` + `_raise`) vs `plugin_psij.py:507-539,554-574,576-639` (sync wrapper → `_run_sync(self.a<method>(...))` → async core using `_arequest`).
- **What & why it hurts**: Within one class, `submit_job`/`list_jobs`/`tunnel_status` are plain sync while `get_job_status`/`cancel_job`/`submit_tunneled` each come as a *pair* (sync shim + `a`-prefixed async core), driven through `client._run_sync`'s single-step coroutine trick — subtle machinery whose docstring needs 12 lines to argue it is "bit-identical". `QueueInfoClient`, `SysInfoClient`, and `StagingClient` are 100% plain sync. A student cannot infer the rule for when a method needs an async twin (the actual rule — "whatever the broker-hosted dispatcher happens to call" — is invisible from the code) and will copy the pattern inconsistently.
- **Simpler alternative**: Make the seam uniform: either give *every* helper the async core (mechanical, but doubles method count), or — simpler — route all helpers through the sync `self._request`/`_raise` and let the broker-hosted dispatcher wrap the sync call (`asyncio.to_thread`) at *its* layer, deleting the `a*` twins and the mixed idiom from plugin files entirely.
- **Essential?**: Not blocking the broker's host loop is essential (failure-detection decoupling requirement). But the requirement lives in the dispatcher, not in each plugin's client; pushing it into per-method twins in some plugins is accidental.
- **Severity**: medium
- **Effort**: medium

## Finding 7 — `_parse_allocated_port`: 65 lines of non-blocking fd machinery for "read stderr lines until a regex matches, with timeout"

- **Location**: `src/radical/orbit/tunnel.py:191-255`; also the mid-file `import re` at `tunnel.py:184` and the "Reverse tunnel" section banner splitting one small module in two.
- **What & why it hurts**: The function manually flips the fd to non-blocking, `select()`s in 100 ms slices, accumulates a byte buffer, splits on `\n`, *and additionally* regex-checks the unsplit remainder (242-246) — machinery a student must verify line-by-line to trust. Meanwhile the file already contains the tool for the job: `_start_stderr_drain` (88-101), a daemon thread appending decoded lines to a shared list.
- **Simpler alternative**: Start the drain thread immediately after `Popen`; then in the caller loop `while time.monotonic() < deadline: scan new entries of log_lines for _ALLOCATED_PORT_RE; check proc.poll(); sleep(0.1)`. Same timeout and same failure messages in ~15 straight-line lines; the fd never needs to change modes.
- **Essential?**: Parsing sshd's "Allocated port N" with a timeout is essential to reverse mode. The `select`/non-blocking implementation is accidental; the thread-based one meets the same requirement.
- **Severity**: medium
- **Effort**: small

## Finding 8 — Back-compat shims for callers the project itself controls

- **Location**: `queue_info_slurm.py:356-370` (monkeypatched `QueueInfoSlurm.get_job_nodes = staticmethod(_get_job_nodes)` "for the remainder of the rewire"); `plugin_queue_info.py:22-29` (re-exports `QueueInfoSlurm` and `_parse_slurm_time` "for tests / external callers"); `queue_info.py:319-321` (another `QueueInfoSlurm` re-export, forcing the `noqa: F811` on the factory's local import at 309); `plugin_queue_info.py:313-331` (`slurm_conf` deprecated alias parameter).
- **What & why it hurts**: Four separate "old name still works" mechanisms in one plugin's import graph. Each one is a false trail: a student greps for `get_job_nodes` and lands on a shim that constructs a whole `SlurmBatchSystem` to delegate one call; the double-export of `QueueInfoSlurm` makes "where does this class live" a three-file question. In a repo where the tests and callers are in-tree, these earn nothing.
- **Simpler alternative**: Update the in-tree callers/tests to the real locations and delete all four shims (the monkeypatch block especially — it is self-described scaffolding).
- **Essential?**: No architectural requirement; pure migration residue.
- **Severity**: medium
- **Effort**: small

## Finding 9 — Four hand-rolled `subprocess.run` wrappers across the backend files

- **Location**: `queue_info_slurm.py:35-45` (method `_run`, env-aware, `check=True`), `queue_info_pbs.py:184-192` (module `_run`, same shape), plus the inline pattern `subprocess.run([...], capture_output=True, text=True, timeout=10)` + returncode check repeated ~8 times in `batch_system_slurm.py:78-135` and `batch_system_pbs.py:171-217,240-277`.
- **What & why it hurts**: The same five keyword arguments and the same two failure modes (`OSError/TimeoutExpired` → empty/UNKNOWN, nonzero rc → error) are re-typed in every method. Divergence has already begun (timeouts 10 vs 60; `check=True` vs manual rc; RuntimeError vs silent fallback), so a student can't tell which differences are deliberate.
- **Simpler alternative**: One module-level helper, e.g. in `batch_system.py`: `run_cmd(cmd, timeout=10, env=None) -> str|None` returning stdout or `None` on any failure, plus a `run_cmd_strict` that raises. Both hierarchies use it; ~40 lines of ceremony deleted.
- **Essential?**: No; pure duplication.
- **Severity**: medium
- **Effort**: small

## Finding 10 — `plugin_queue_info` repeats the same handler/param boilerplate five times on each side

- **Location**: `plugin_queue_info.py:423-468` (five handlers each doing `sid = ...; user = request.query_params.get('user'); force = request.query_params.get('force','').lower()=='true'; return await self._forward(...)`); mirrored in the client at 144-231 (five methods each building `params = {"force": str(force).lower()}` + conditional `user`).
- **What & why it hurts**: Ten near-identical stanzas; the interesting information (which session method, which route) is one line in each, buried under repeated query-string plumbing. Any change to the `force` encoding touches ten places.
- **Simpler alternative**: Two 3-line helpers: server-side `_uf(request) -> (user, force)` and client-side `_uf_params(user, force) -> dict`. Each stanza collapses to two lines.
- **Essential?**: No.
- **Severity**: low
- **Effort**: small

## Finding 11 — SLURM-specific helpers parked in the "generic" `queue_info.py` base module

- **Location**: `queue_info.py:15-20` (`_UNAVAIL_STATES`, whose own comment says "kept here for legacy reasons but only used by the SLURM backend"), `queue_info.py:38-93` (`_unwrap` — SLURM's `{set,infinite,number}` wrapper — and `_parse_gpus` — SLURM GRES strings).
- **What & why it hurts**: The base module advertises itself as scheduler-agnostic ("abstract base + shared helpers + factory") but half its helpers only make sense for SLURM JSON. A student modeling a new backend on this file will wonder which helpers they must honor.
- **Simpler alternative**: Move all three into `queue_info_slurm.py`; the base keeps only `_resolve_user`, the cache, and the ABC.
- **Essential?**: No; the comment on `_UNAVAIL_STATES` admits it.
- **Severity**: low
- **Effort**: small

## Finding 12 — Ceremonial exception tuples and speculative parallelism in sysinfo detection

- **Location**: `plugin_sysinfo.py:192` (`except (FileNotFoundError, subprocess.CalledProcessError, ImportError, Exception)` — the trailing `Exception` makes the other three decoration), `plugin_sysinfo.py:126` (`except (FuturesTimeout, Exception)` — same), `plugin_sysinfo.py:107-128` (`ThreadPoolExecutor` to run three GPU detectors in parallel).
- **What & why it hurts**: The tuples *look* like careful error handling but are `except Exception` in costume — actively misleading about what can fail. The thread pool parallelizes a one-shot detection that already runs on a background prefetch thread (`start_prefetch`, 41-54) that nothing waits on; three sequential calls with per-call timeouts would behave identically for every caller.
- **Simpler alternative**: Write `except Exception:` where that is meant; run the three detectors sequentially inside the existing prefetch thread (worst case ~15 s of a daemon thread's time, invisible to users).
- **Essential?**: Not blocking metrics queries on hardware probing is reasonable — the single prefetch thread already achieves it. The inner pool is accidental.
- **Severity**: low
- **Effort**: small

## Finding 13 — psij-only conventions: scattered inline imports and a triple-knobbed poll interval

- **Location**: Inline imports repeated per-method: `plugin_psij.py:209` (`detect_batch_system` inside `submit_job`), `1000-1002`, `1210-1211`, `1240` (`from . import tunnel as _tunnel` three separate times), `1081` (`import json as _json` inside a polling loop) — while the same module already imports `tunnel` at top (line 36) and no import cycle forces the rest. Poll interval configurable three ways: module constant `PSIJ_POLL_INTERVAL` (55) → class attr `poll_interval` (124) → per-session kwarg (132), with no in-tree caller using the latter two. Also: psij alone lifts route strings to `ROUTE_*` module constants with a 7-line justification (40-52) while every other plugin inlines its paths — two conventions for the same thing.
- **What & why it hurts**: Each is small, but together they make psij read as if written under different rules than its siblings; students learn plugin conventions by imitation, and this file teaches three contradictory ones.
- **Simpler alternative**: Hoist the repeated imports to the top of the module; keep one poll-interval knob (the module constant); pick one route-string convention repo-wide (either is fine).
- **Essential?**: No.
- **Severity**: low
- **Effort**: small
