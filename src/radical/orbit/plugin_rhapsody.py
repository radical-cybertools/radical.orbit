'''
Rhapsody Plugin for ORBIT.

Exposes the RHAPSODY Session/Task API so that remote clients can submit
and monitor compute / AI tasks on endpoint nodes.
'''

import asyncio
import base64
import importlib
import json
import logging
import os
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException, Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient

log = logging.getLogger("radical.orbit")

TERMINAL_STATES     = {'DONE', 'FAILED', 'CANCELED', 'COMPLETED'}
WATCH_CONCURRENCY   = 64
WS_PAYLOAD_LIMIT    = 8 * 1024 * 1024  # target max per batch (conservative)
NOTIFY_BATCH_SIZE   = 1024             # max tasks per bulk notification
NOTIFY_BATCH_WINDOW = 0.25             # seconds to accumulate before flush

# Guard optional dependencies
try:
    import rhapsody as rh
except ImportError:
    rh = None

import cloudpickle as _cp
import msgpack
from . import _prof as rprof


def _assert_json_serializable(obj, path=""):
    """Recursively verify that *obj* is JSON-serializable.

    Raises ``TypeError`` with the exact key path on the first
    non-serializable value encountered.
    """
    if isinstance(obj, dict):
        for key, val in obj.items():
            _assert_json_serializable(val, f"{path}.{key}" if path else key)
    elif isinstance(obj, (list, tuple)):
        for i, val in enumerate(obj):
            _assert_json_serializable(val, f"{path}[{i}]")
    elif isinstance(obj, (str, int, float, bool, type(None))):
        return
    else:
        raise TypeError(
            f"non-serializable value at '{path}': "
            f"{type(obj).__name__} = {repr(obj)!s:.200}")


def _json_safe(v):
    """Coerce *v* to a JSON-serializable form.

    Backend-specific kwargs (e.g. dragon ``Policy`` objects passed via
    ``task_backend_specific_kwargs``) are not JSON-encodable; without
    this, any read of the cached task dict (list_tasks / wait_tasks /
    notification serialization) raises ``TypeError`` and the response
    or WS frame is dropped.  Falls back through ``to_dict`` / recursion
    / ``str``.
    """
    try:
        json.dumps(v)
        return v
    except (TypeError, ValueError):
        if hasattr(v, 'to_dict'):
            try:                  return _json_safe(v.to_dict())
            except Exception:     pass
        if isinstance(v, dict):
            return {str(k): _json_safe(x) for k, x in v.items()}
        if isinstance(v, (list, tuple, set)):
            return [_json_safe(x) for x in v]
        return str(v)


# ---------------------------------------------------------------------------
# Endpoint-side session
# ---------------------------------------------------------------------------

class RhapsodySession(PluginSession):
    """
    Rhapsody session (service-side).

    Wraps a ``rhapsody.Session`` instance, forwarding task submission,
    monitoring, cancellation and statistics queries.
    """

    def __init__(self, sid: str, backend_names: list[str] | None = None,
                 allow_pickled_tasks: bool = True,
                 notify_batch_window: float = NOTIFY_BATCH_WINDOW,
                 notify_batch_size: int = NOTIFY_BATCH_SIZE):
        """
        Initialize a RhapsodySession.

        Args:
            sid (str):  Unique session identifier.
            backend_names (list[str] | None):
                Backends to configure.  Defaults to ``['dragon_v3']``.
            allow_pickled_tasks (bool):
                Allow cloudpickle-encoded function tasks.  Defaults to ``True``.
            notify_batch_window (float):
                Seconds to accumulate notifications before flushing.
            notify_batch_size (int):
                Max notifications per flush — triggers immediate send.
        """
        super().__init__(sid)

        if rh is None:
            raise RuntimeError("rhapsody package is not installed")

        self.backend_names       = backend_names or ['dragon_v3']
        self.allow_pickled_tasks = allow_pickled_tasks
        self._rh_session         = None
        self._telemetry          = None
        self._tasks: dict[str, dict] = {}

        # Async init tracking
        self._init_ready = threading.Event()
        self._init_error: str | None = None

        # Limit concurrent task watchers
        self._watch_sem: asyncio.Semaphore | None = None

        # Notification batcher: accumulate completions and flush in bulk
        self._notify_buf: list[dict]  = []
        self._notify_lock             = threading.Lock()
        self._notify_batch_window     = notify_batch_window
        self._notify_batch_size       = notify_batch_size

        # Cache for deserialized cloudpickle payloads — avoids decoding the
        # same encoded string N times for template-expanded homogeneous batches.
        self._pickle_cache: dict[str, object] = {}

        # Profiler — resolved lazily via the injected _plugin reference
        self._prof: rprof.Profiler | None = None

    @property
    def prof(self) -> rprof.Profiler:
        if self._prof is None:
            svc = getattr(getattr(self._plugin, '_app', None),
                          'state', None)
            svc = getattr(svc, 'endpoint_service', None) if svc else None
            self._prof = getattr(svc, '_prof', None) or \
                         rprof.Profiler('rhapsody', ns='radical.orbit')
        return self._prof

    async def initialize(self) -> None:
        """Asynchronously initialize the session and its backends."""
        try:
            backends = []
            for name in self.backend_names:
                b = rh.get_backend(name)
                if hasattr(b, '__await__'):
                    b = await b
                backends.append(b)

            self._rh_session = rh.Session(backends=backends, uid=self._sid)

            # start_telemetry only exists on newer rhapsody; older
            # installs don't support it. Skip silently if unavailable.
            self._telemetry = None
            start_telemetry = getattr(self._rh_session, 'start_telemetry', None)
            if start_telemetry is not None:
                self._telemetry = await start_telemetry(
                    resource_poll_interval=0.1,
                    checkpoint_path="telemetry-output")

            # Register state-change callbacks for intermediate notifications
            self._notified_states: dict[str, str] = {}
            self._notified_lock = threading.Lock()
            for b in backends:
                if hasattr(b, 'register_callback'):
                    orig = getattr(b, '_callback_func', None)

                    def _on_state(task, state, _orig=orig):
                        self._on_task_state_change(task, state)
                        if _orig:
                            _orig(task, state)

                    b.register_callback(_on_state)

            # Create semaphore now that we're in an event loop
            self._watch_sem = asyncio.Semaphore(WATCH_CONCURRENCY)

            self._init_ready.set()
            log.info("[%s] Session initialization complete", self._sid)

            # Probe: log Dragon's view of node hostnames so we can compare
            # against what queue_info.nodelist() / amsc.py is using for
            # Policy(host_name=…).  A mismatch silently turns HOST_NAME
            # placement into a no-op and tasks pile up on whichever node
            # Dragon's default policy lands them.
            #
            # ``System().nodes`` returns huids (int).  ``Node(huid)``
            # wraps each one and exposes ``.hostname``.
            try:
                from dragon.native.machine import (
                    System as _DragonSystem, Node as _DragonNode)
                _dragon_hosts = [str(_DragonNode(h).hostname)
                                 for h in _DragonSystem().nodes]
                log.info("[%s] dragon System().nodes hostnames: %s",
                         self._sid, _dragon_hosts)
                # Probe accelerator visibility — if Policy(gpu_affinity=…)
                # is being silently dropped, dragon's _get_gpu_affinity
                # filters our request against node.accelerators.device_list
                # (== Node.gpus).  Empty/None/wrong-vocabulary list here
                # explains why CUDA_VISIBLE_DEVICES never gets set.
                for _h in _DragonSystem().nodes:
                    _n = _DragonNode(_h)
                    log.info("[%s] dragon node %s: gpus=%s vendor=%s env=%s",
                             self._sid, _n.hostname,
                             _n.gpus, _n.gpu_vendor, _n.gpu_env_str)
            except Exception as _e:
                log.info("[%s] dragon hostname probe skipped: %s",
                         self._sid, _e)

        except Exception as e:
            self._init_error = str(e)
            self._init_ready.set()  # unblock waiters
            log.error("[%s] Session initialization failed: %s",
                      self._sid, e)
            raise

    def _on_task_state_change(self, task, state):
        """Fire notification on intermediate state changes (e.g. RUNNING).

        Called from backend threads — uses lock for _notified_states access.
        """
        uid = self._get_attr(task, 'uid')
        uid_str = str(uid) if uid else '?'
        state_str = str(state)

        with self._notified_lock:
            # Skip if we already notified this state
            if self._notified_states.get(uid_str) == state_str:
                return
            self._notified_states[uid_str] = state_str

        # Only fire for non-terminal states; terminal is handled by _watch_task
        if state_str.upper() in TERMINAL_STATES:
            return

        if self._plugin:
            self._plugin._dispatch_notify("task_status", {
                "uid":   uid_str,
                "state": state_str,
            })

    def _check_initialized(self) -> None:
        """Check that the session is active and fully initialized.

        Raises:
            HTTPException 409: Session is still initializing.
            HTTPException 500: Session initialization failed.
            RuntimeError:      Session is closed.
        """
        self._check_active()
        if not self._init_ready.is_set():
            raise HTTPException(status_code=409,
                                detail="session is still initializing")
        if self._init_error:
            raise HTTPException(
                status_code=500,
                detail=f"session init failed: {self._init_error}")

    def _deserialize_task(self, td: dict) -> dict:
        """Deserialize pickled or import-path function fields in a task dict.

        Handles two formats:
        - cloudpickle:  ``"function": "cloudpickle::<base64>"`` with
          ``"_pickled_fields": ["function", "args", ...]``
        - import path:  ``"function": "module.path:func_name"``

        Returns the (possibly modified) task dict.
        """
        # --- cloudpickle-encoded fields ---
        pickled_fields = td.pop('_pickled_fields', None)
        if pickled_fields:
            if not self.allow_pickled_tasks:
                raise HTTPException(
                    status_code=400,
                    detail="pickled function tasks are disabled")
            for field in pickled_fields:
                val = td.get(field)
                if isinstance(val, str) and val.startswith('cloudpickle::'):
                    encoded = val[len('cloudpickle::'):]
                    cached = self._pickle_cache.get(encoded)
                    if cached is not None:
                        td[field] = cached
                    else:
                        obj = _cp.loads(base64.b64decode(encoded))
                        self._pickle_cache[encoded] = obj
                        td[field] = obj
            return td

        # --- import-path string (e.g. "mymodule.sub:func_name") ---
        fn = td.get('function')
        if isinstance(fn, str) and ':' in fn and \
                not fn.startswith('cloudpickle::'):
            mod_path, _, attr_name = fn.partition(':')
            try:
                mod = importlib.import_module(mod_path)
                td['function'] = getattr(mod, attr_name)
            except (ImportError, AttributeError) as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"cannot resolve function '{fn}': {e}") from e

        return td

    def _prepare_batch(self, task_dicts: list[dict],
                       pre_expanded: bool = False) -> list:
        """Deserialize and create task objects (runs in worker thread).

        CPU-bound work (cloudpickle, from_dict) is offloaded here so the
        event loop stays responsive for WebSocket keepalive.

        When *pre_expanded* is True the batch came from template expansion:
        all dicts already share field values by reference and the first
        dict's cloudpickle fields seed the pickle cache so subsequent
        dicts hit the cache.
        """
        deserialized = [self._deserialize_task(td) for td in task_dicts]
        return [rh.BaseTask.from_dict(td) for td in deserialized]

    async def submit_tasks(self, task_dicts: list[dict],
                           pre_expanded: bool = False) -> list[dict]:
        """
        Submit a list of tasks.

        Each dict is converted to a ``ComputeTask`` or ``AITask`` via
        ``BaseTask.from_dict()``.  Function fields encoded as cloudpickle
        blobs or import-path strings are deserialized first.

        Uses a pipeline: deserialization of chunk N+1 runs concurrently
        with backend submission of chunk N, so the two dominant costs
        overlap.

        Returns:
            list[dict]: Minimal ack dicts ``{uid, state}``.
        """
        self._check_initialized()

        prof    = self.prof
        batch_n = len(task_dicts)
        bid     = task_dicts[0].get('uid', '?') if task_dicts else '?'
        prof.prof('rh_submit', uid=bid, msg=str(batch_n))

        _CHUNK  = 4096
        results = []
        all_tasks = []   # collect for batch watcher

        # -- pipelined deser / submit ----------------------------------------
        # Kick off deserialization of the first chunk while we have nothing
        # else to do; then overlap deser(N+1) with submit(N).

        chunks = [task_dicts[i:i + _CHUNK]
                  for i in range(0, len(task_dicts), _CHUNK)]

        prev_submit_fut = None   # future for the previous submit
        prev_tasks      = None   # tasks from the previous chunk

        for ci, chunk_dicts in enumerate(chunks):

            # Offload CPU-bound deserialization to a worker thread
            prof.prof('rh_deser', uid=bid, msg=str(len(chunk_dicts)))
            deser_fut = asyncio.ensure_future(asyncio.to_thread(
                self._prepare_batch, chunk_dicts, pre_expanded))

            # While deser runs, await the *previous* chunk's submit
            if prev_submit_fut is not None:
                await prev_submit_fut
                prof.prof('rh_backend_submit_done', uid=bid)

                # Register the previously submitted tasks
                prof.prof('rh_register', uid=bid)
                for t in prev_tasks:
                    uid_str = str(t.uid)
                    self._tasks[uid_str] = t
                    results.append({"uid": uid_str,
                                    "state": str(t.get("state"))})
                all_tasks.extend(prev_tasks)
                prof.prof('rh_register_done', uid=bid)

            tasks = await deser_fut
            prof.prof('rh_deser_done', uid=bid)

            # Fire off the backend submit (will be awaited next iteration
            # or after the loop)
            prof.prof('rh_backend_submit', uid=bid)
            prev_submit_fut = asyncio.ensure_future(
                self._rh_session.submit_tasks(tasks))
            prev_tasks = tasks

            # Yield so the event loop can process WS pings
            await asyncio.sleep(0)

        # Await the final chunk's submit
        if prev_submit_fut is not None:
            await prev_submit_fut
            prof.prof('rh_backend_submit_done', uid=bid)

            prof.prof('rh_register', uid=bid)
            for t in prev_tasks:
                uid_str = str(t.uid)
                self._tasks[uid_str] = t
                results.append({"uid": uid_str,
                                "state": str(t.get("state"))})
            all_tasks.extend(prev_tasks)
            prof.prof('rh_register_done', uid=bid)

        # Start a single batch watcher instead of per-task watchers
        if self._plugin and all_tasks:
            asyncio.ensure_future(self._watch_batch(all_tasks))

        prof.prof('rh_submit_done', uid=bid, msg=str(batch_n))
        return results

    def _queue_notification(self, payload: dict) -> None:
        """Add a task notification to the batch buffer and ensure a
        flush is scheduled.

        Thread-safe — called from watcher coroutines.
        """
        uid = payload.get('uid', '?')
        self.prof.prof('notify_queue', uid=uid)

        with self._notify_lock:
            self._notify_buf.append(payload)
            buf_len = len(self._notify_buf)

        if buf_len >= self._notify_batch_size:
            # Buffer full — flush immediately (sync, from event loop)
            self._flush_notifications()
        else:
            # Schedule a delayed flush so tail items aren't stranded.
            # Always schedule — it's cheap and a no-op if buffer is
            # empty when it fires.
            self._schedule_flush(delay=self._notify_batch_window)

    def _schedule_flush(self, delay: float = 0) -> None:
        """Schedule a notification flush on the event loop."""
        if not self._plugin:
            return

        async def _do_flush():
            if delay > 0:
                await asyncio.sleep(delay)
            self._flush_notifications()

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_do_flush())
        except RuntimeError:
            if hasattr(self._plugin, '_main_loop') and \
                    self._plugin._main_loop:
                asyncio.run_coroutine_threadsafe(
                    _do_flush(), self._plugin._main_loop)

    def _flush_notifications(self) -> None:
        """Flush the notification buffer as a bulk message."""
        with self._notify_lock:
            if not self._notify_buf:
                return
            batch = list(self._notify_buf)
            self._notify_buf.clear()

        if not self._plugin:
            return

        prof = self.prof
        for item in batch:
            prof.prof('notify_flush', uid=item.get('uid', '?'))

        if len(batch) == 1:
            self._plugin._dispatch_notify("task_status", batch[0])
        else:
            self._plugin._dispatch_notify("task_status_batch",
                                          {"tasks": batch})

    async def _watch_task(self, task):
        """Background watcher for a single task: notify as soon as it completes.

        Concurrency is bounded by ``self._watch_sem`` so that thousands of
        simultaneous watchers do not overwhelm the event loop.
        """
        uid = self._get_attr(task, 'uid')
        uid_str = str(uid) if uid else '?'

        # Acquire semaphore — queues here if too many watchers are active
        sem = self._watch_sem or asyncio.Semaphore(WATCH_CONCURRENCY)
        prof = self.prof
        async with sem:
            log.debug("[%s] Watcher started for task %s", self._sid, uid_str)
            prof.prof('rh_task_exec', uid=uid_str)
            try:
                if not self._rh_session:
                    log.warning("[%s] Session closed before task %s completed",
                                self._sid, uid_str)
                    self._queue_notification({
                        "uid": uid_str, "state": "FAILED",
                        "error": "Session closed"})
                    prof.prof('rh_task_done', uid=uid_str, state='FAILED')
                    return

                await self._rh_session.wait_tasks([task])

                state = self._get_attr(task, 'state')
                log.debug("[%s] Task %s completed with state: %s",
                          self._sid, uid_str, state)

                prof.prof('rh_task_done', uid=uid_str,
                          state=str(state))

                d = self._notification_payload(task)
                self._queue_notification(d)

            except Exception as e:
                prof.prof('rh_task_done', uid=uid_str, state='FAILED')
                log.warning("[%s] Rhapsody watch error for task %s: %s",
                            self._sid, uid_str, e)
                self._queue_notification({
                    "uid": uid_str, "state": "FAILED",
                    "error": str(e)})

    async def _watch_batch(self, tasks):
        """Watch a batch of tasks, notifying as each completes.

        Uses ``asyncio.wait(FIRST_COMPLETED)`` to drain completions
        incrementally.  Notifications are queued per-task as soon as
        each finishes — the existing notification buffer
        (``_queue_notification``) batches them opportunistically so
        fast-completing tasks are grouped into single SSE messages
        while slow tasks don't block others.
        """
        prof = self.prof

        # Build uid map and per-task futures
        fut_to_uid: dict[asyncio.Future, str] = {}
        uid_to_task: dict[str, object]         = {}

        for t in tasks:
            uid     = self._get_attr(t, 'uid')
            uid_str = str(uid) if uid else '?'
            uid_to_task[uid_str] = t
            prof.prof('rh_task_exec', uid=uid_str)

        if not self._rh_session:
            for uid_str in uid_to_task:
                prof.prof('rh_task_done', uid=uid_str, state='FAILED')
                self._queue_notification({
                    "uid": uid_str, "state": "FAILED",
                    "error": "Session closed"})
            return

        # Obtain per-task futures from the session's state manager
        sm = self._rh_session._state_manager
        for uid_str, t in uid_to_task.items():
            fut = sm.get_wait_future(uid_str, t)
            fut_to_uid[fut] = uid_str

        # Drain completions incrementally
        pending = set(fut_to_uid.keys())

        while pending:
            try:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED)
            except Exception as e:
                log.warning("[%s] Batch watch error: %s", self._sid, e)
                break

            # Notify for every task that just completed
            for fut in done:
                uid_str = fut_to_uid[fut]
                t       = uid_to_task[uid_str]
                state     = self._get_attr(t, 'state')
                state_str = str(state) if state else 'UNKNOWN'

                prof.prof('rh_task_done', uid=uid_str, state=state_str)

                if state_str.upper() in TERMINAL_STATES:
                    d = self._notification_payload(t)
                    self._queue_notification(d)
                else:
                    self._queue_notification({
                        "uid": uid_str, "state": state_str,
                        "error": f"unexpected state: {state_str}"})

    async def wait_tasks(self, uids: list[str],
                         timeout: float | None = None) -> list[dict]:
        """
        Return current task states (non-blocking snapshot).

        This method no longer blocks until tasks complete.  Clients
        should rely on SSE ``task_status`` notifications for real-time
        completion events, and call this endpoint only to fetch the
        current state snapshot.

        Args:
            uids (list[str]):  Task UIDs to query.
            timeout (float | None):  Ignored (kept for API compat).

        Returns:
            list[dict]: Current task state dicts.
        """
        self._check_initialized()

        tasks = [self._tasks[uid] for uid in uids if uid in self._tasks]
        if not tasks:
            raise HTTPException(status_code=404,
                                detail="none of the requested tasks found")

        return [self._sanitize_task(t) for t in tasks]

    def _get_attr(self, obj, attr, default=None):
        """Helper to get attribute from object or dict."""
        val = getattr(obj, attr, None)
        if val is None and isinstance(obj, dict):
            val = obj.get(attr)
        return val if val is not None else default

    def _sanitize_task(self, t) -> dict:
        """Sanitize a Rhapsody task dict so it's JSON serializable."""
        if hasattr(t, 'to_dict'):
            d = t.to_dict()
        else:
            d = dict(t)

        # Ensure 'uid' is present and a string
        uid = self._get_attr(t, 'uid')
        if uid:
            d['uid'] = str(uid)

        # Ensure 'state' is present and a string
        state = self._get_attr(t, 'state')
        if state:
            d['state'] = str(state)

        d.pop('future', None)
        if 'exception' in d and d['exception'] is not None:
            d['exception'] = str(d['exception'])

        # Stringify callable function fields
        fn = d.get('function')
        if callable(fn):
            d['function'] = f"{fn.__module__}.{fn.__qualname__}"

        # Decode bytes stdout/stderr; join lists (multi-rank)
        for key in ('stdout', 'stderr'):
            val = d.get(key)
            if isinstance(val, bytes):
                d[key] = val.decode('utf-8', errors='replace')
            elif isinstance(val, list):
                d[key] = '\n'.join(str(v) for v in val)

        # Ensure return_value is JSON-serializable
        rv = d.get('return_value')
        if rv is not None:
            if isinstance(rv, bytes):
                d['return_value'] = base64.b64encode(rv).decode('ascii')
                d['_return_value_encoding'] = 'base64'
            else:
                try:
                    json.dumps(rv)
                except (TypeError, ValueError):
                    d['return_value'] = str(rv)

        return {k: _json_safe(v) for k, v in d.items()}

    _NOTIFICATION_KEYS = {'uid', 'state', 'exit_code',
                          'return_value', '_return_value_encoding',
                          'error', 'exception', 'traceback'}

    def _notification_payload(self, t) -> dict:
        """Build a minimal notification dict for a completed task.

        Only essential fields are included to keep WebSocket/SSE
        payloads small.  Clients needing the full task dict (e.g.
        stdout/stderr) can fetch it via ``GET /task/{sid}/{uid}``.
        """
        full = self._sanitize_task(t)
        return {k: v for k, v in full.items()
                if k in self._NOTIFICATION_KEYS}

    async def list_tasks(self) -> dict:
        """Return all tasks in this session with current state."""
        self._check_initialized()
        tasks = []
        for uid, task in self._tasks.items():
            tasks.append(self._sanitize_task(task))
        return {"tasks": tasks}

    async def get_task(self, uid: str) -> dict:
        """
        Return info for a single cached task.
        """
        self._check_initialized()

        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")
        return self._sanitize_task(task)

    async def cancel_task(self, uid: str) -> dict:
        """
        Cancel a running task.
        """
        self._check_initialized()

        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")

        backend_name = task.get("backend")
        if backend_name and backend_name in self._rh_session.backends:
            backend = self._rh_session.backends[backend_name]
            await backend.cancel_task(uid)

        return {"uid": uid, "status": "canceled"}

    async def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.

        Best-effort: Dragon V3 marks tasks as CANCELED but cannot truly
        abort running work.  Per-task errors are swallowed.  Cancels are
        issued concurrently via ``asyncio.gather``.
        """
        self._check_initialized()

        uids = []
        for uid, task in list(self._tasks.items()):
            state = str(self._get_attr(task, 'state', '')).upper()
            if state not in TERMINAL_STATES:
                uids.append(uid)

        if not uids:
            return {"canceled": 0}

        async def _try_cancel(uid):
            try:
                await self.cancel_task(uid)
                return True
            except Exception:
                return False

        results = await asyncio.gather(*[_try_cancel(u) for u in uids])
        return {"canceled": sum(1 for r in results if r)}

    async def close(self) -> dict:
        """
        Shutdown RHAPSODY session and clean up.
        """
        if self._rh_session:
            if self._telemetry is not None:
                summary = json.dumps(self._telemetry.summary(), indent=4)
                log.info(summary)
                print(summary, flush=True)
                self._telemetry = None
            await self._rh_session.close()
            self._rh_session = None
        self._tasks = {}
        return await super().close()


# ---------------------------------------------------------------------------
# Application-side client
# ---------------------------------------------------------------------------

class RhapsodyClient(PluginClient):
    """
    Client-side interface for the Rhapsody plugin.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Session-wide accumulator for terminal task notifications.
        # Populated by a persistent SSE callback registered after
        # session init, so no notification is ever lost.
        self._completed: dict[str, dict] = {}
        self._completed_lock = threading.Lock()
        # Waiters: list of (set_of_uids, threading.Event) for wait_tasks
        self._waiters: list[tuple[set, threading.Event]] = []
        self._waiters_lock = threading.Lock()

    def _on_task_done(self, endpoint, plugin, topic, data):
        """Persistent SSE callback: accumulate terminal task states.

        Handles both single ``task_status`` and bulk
        ``task_status_batch`` notifications.
        """
        if topic == 'task_status_batch':
            tasks = data.get('tasks', [])
        else:
            tasks = [data]

        newly_done: list[str] = []
        with self._completed_lock:
            for t in tasks:
                # Decode base64-encoded return values
                if t.get('_return_value_encoding') == 'base64':
                    t['return_value'] = base64.b64decode(t['return_value'])
                    del t['_return_value_encoding']

                uid   = t.get('uid')
                state = str(t.get('state', '')).upper()
                if uid and state in TERMINAL_STATES:
                    self._completed[uid] = t
                    newly_done.append(uid)

            # Wake waiters under completed_lock so no waiter can
            # register between adding to _completed and checking
            # pending sets.  Lock order: completed → waiters.
            if newly_done:
                with self._waiters_lock:
                    for pending, event in self._waiters:
                        for uid in newly_done:
                            pending.discard(uid)
                        if not pending:
                            event.set()

    def register_session(self, backends: list[str] | None = None,
                         init_timeout: float = 120,
                         notify_batch_window: float | None = None,
                         notify_batch_size: int | None = None):
        """
        Register a session, optionally specifying backend names.

        The endpoint initializes the session asynchronously.  This method
        blocks until a ``session_status`` SSE notification confirms
        that the session is ready (or until *init_timeout* seconds).

        Falls back to polling when no ``BridgeClient`` is available.

        Args:
            backends: List of backend names (e.g. ``['dragon_v3']``).
                      Defaults to ``['dragon_v3']`` on the server side.
            init_timeout: Seconds to wait for session init (default 120).
            notify_batch_window: Seconds to accumulate notifications
                      before flushing (endpoint-side).
            notify_batch_size: Max notifications per flush (endpoint-side).
        """
        has_sse = (self._bc is not None and
                   self._endpoint_id is not None and
                   self._plugin_name is not None)

        # Ensure the SSE listener is connected BEFORE we send the POST,
        # so we never miss a fast init notification.
        ready = threading.Event()
        error = [None]

        if has_sse:
            self._bc.wait_for_listener(timeout=30)

            def _on_session_status(endpoint, plugin, topic, data):
                st = data.get('status')
                if st == 'ready':
                    ready.set()
                elif st == 'failed':
                    error[0] = data.get('error', 'unknown init error')
                    ready.set()

            self.register_notification_callback(_on_session_status,
                                                topic="session_status")

        payload = {}
        if backends:
            payload['backends'] = backends
        if notify_batch_window is not None:
            payload['notify_batch_window'] = notify_batch_window
        if notify_batch_size is not None:
            payload['notify_batch_size'] = notify_batch_size
        resp = self._http.post(self._url('register_session'), json=payload)
        self._raise(resp)

        data      = resp.json()
        self._sid = data['sid']
        status    = data.get('status')

        # Reset the session-wide task completion accumulator
        with self._completed_lock:
            self._completed.clear()

        if status == 'ready':
            if has_sse:
                self.unregister_notification_callback(
                    _on_session_status, topic="session_status")
            self._start_task_listener()
            return  # fast path: init was synchronous

        if not has_sse:
            self._poll_session_ready(init_timeout)
            return

        # Wait for async init to complete via SSE notification
        try:
            ready.wait(timeout=init_timeout)
            if error[0]:
                raise RuntimeError(
                    f"Session init failed on endpoint: {error[0]}")
            if not ready.is_set():
                raise RuntimeError(
                    f"Session init timed out after {init_timeout}s")
        finally:
            self.unregister_notification_callback(_on_session_status,
                                                  topic="session_status")

        self._start_task_listener()

    def _start_task_listener(self):
        """Register persistent SSE callback that accumulates completions."""
        has_sse = (self._bc is not None and
                   self._endpoint_id is not None and
                   self._plugin_name is not None)
        if has_sse:
            self.register_notification_callback(self._on_task_done,
                                                topic="task_status")
            self.register_notification_callback(self._on_task_done,
                                                topic="task_status_batch")

    def _poll_session_ready(self, timeout: float = 120) -> None:
        """Fallback: poll until the session is ready (no SSE available)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = self._http.get(
                    self._url(f"list_tasks/{self.sid}"))
                if resp.status_code != 409:
                    return  # session is ready (or already errored)
            except Exception:
                pass
            time.sleep(1.0)
        raise RuntimeError(
            f"Session init timed out after {timeout}s (poll)")

    @staticmethod
    def _serialize_task(td: dict) -> None:
        """Prepare a task dict for JSON transport (in-place).

        - Encodes callable ``function``, ``args``, ``kwargs`` via
          cloudpickle + base64.
        - Strips non-serializable internal fields (``future``,
          ``_future``, ``backend``).
        """
        pickled_fields = td.get('_pickled_fields', [])

        # Serialize callable function
        fn = td.get('function')
        if callable(fn):
            encoded = base64.b64encode(_cp.dumps(fn)).decode('ascii')
            td['function'] = 'cloudpickle::' + encoded
            if 'function' not in pickled_fields:
                pickled_fields.append('function')

        # Serialize args/kwargs if not JSON-safe
        for field in ('args', 'kwargs'):
            val = td.get(field)
            if val is None:
                continue
            if isinstance(val, str) and val.startswith('cloudpickle::'):
                continue
            try:
                json.dumps(val)
            except (TypeError, ValueError):
                encoded = base64.b64encode(_cp.dumps(val)).decode('ascii')
                td[field] = 'cloudpickle::' + encoded
                if field not in pickled_fields:
                    pickled_fields.append(field)

        if pickled_fields:
            td['_pickled_fields'] = pickled_fields

        # Strip non-serializable internal fields
        td.pop('future', None)
        td.pop('_future', None)
        td.pop('backend', None)

        # Strip None-valued type-discriminator fields so that
        # BaseTask.from_dict() routes to the correct task class
        # (it checks key existence, not truthiness).
        for key in ('prompt', 'executable', 'function'):
            if key in td and td[key] is None:
                del td[key]


    def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit tasks to the endpoint.

        Large batches are automatically split so each payload stays
        within the WebSocket frame limit.  Batches are submitted
        concurrently via a thread pool so that network round-trips
        overlap (pipelining).

        UIDs are assigned client-side (if absent) so the caller can
        start waiting for SSE notifications immediately.

        Args:
            task_dicts: List of task specification dicts.

        Returns:
            list[dict]: Submitted task info (uid, state).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self._require_session()

        # --- serialize callables and clean up internal fields ---
        for td in task_dicts:
            self._serialize_task(td)

        # --- assign UIDs client-side so we know them before submit ---
        for td in task_dicts:
            if 'uid' not in td:
                td['uid'] = f"task.{uuid.uuid4().hex[:8]}"

        # --- try template compression for homogeneous batches ---
        # If all tasks share the same fields (except uid), send a
        # template + list of UIDs instead of N full copies.
        url = self._url(f"submit/{self.sid}")
        if len(task_dicts) > 1:
            ref      = task_dicts[0]
            ref_keys = set(ref) - {'uid'}
            homogeneous = all(
                set(td) - {'uid'} == ref_keys and
                all(td[k] is ref[k] or td[k] == ref[k] for k in ref_keys)
                for td in task_dicts[1:])
        else:
            homogeneous = False

        if homogeneous and len(task_dicts) > 1:
            first = {k: v for k, v in ref.items() if k != 'uid'}
            return self._submit_template(
                url, first, [td['uid'] for td in task_dicts])

        # --- split into size-aware batches (byte limit only) ---
        batches: list[list[dict]] = []
        batch: list[dict]         = []
        batch_bytes                = 0

        for td in task_dicts:
            td_size = len(str(td)) + 2
            if batch and batch_bytes + td_size > WS_PAYLOAD_LIMIT:
                batches.append(batch)
                batch       = []
                batch_bytes = 0
            batch.append(td)
            batch_bytes += td_size
        if batch:
            batches.append(batch)

        # --- submit batches concurrently (pipelining) ---
        errors: list[str] = []

        def _submit_batch(b):
            resp = self._http.post(
                url,
                data=msgpack.packb({"tasks": b}, use_bin_type=True),
                headers={"Content-Type": "application/msgpack"})
            self._raise(resp, f"submit {len(b)} task(s)")
            return resp.json()

        if len(batches) == 1:
            return _submit_batch(batches[0])

        results = []
        batch_results: dict[int, list] = {}
        with ThreadPoolExecutor(max_workers=len(batches)) as pool:
            futures = {pool.submit(_submit_batch, b): i
                       for i, b in enumerate(batches)}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    batch_results[idx] = fut.result()
                except Exception as e:
                    errors.append(str(e))

        if errors:
            detail = '; '.join(errors[:3])
            raise RuntimeError(
                f"submit failed for {len(errors)} batch(es): {detail}")

        for i in sorted(batch_results):
            results.extend(batch_results[i])
        return results

    def _submit_template(self, url: str, template: dict,
                         uids: list[str]) -> list[dict]:
        """Submit homogeneous tasks via template compression.

        Sends one template + list of UIDs instead of N full task dicts.
        Falls back to regular submit in WS_PAYLOAD_LIMIT-sized chunks.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Cap chunks by both byte size and count so the server
        # doesn't block the event loop processing too many at once.
        max_by_bytes = max(
            1, (WS_PAYLOAD_LIMIT - len(str(template))) // 20)
        uids_per_chunk = min(max_by_bytes, 8192)
        chunks = [uids[i:i + uids_per_chunk]
                  for i in range(0, len(uids), uids_per_chunk)]

        def _submit_chunk(uid_chunk):
            payload = {"template": template, "uids": uid_chunk}
            resp = self._http.post(
                url,
                data=msgpack.packb(payload, use_bin_type=True),
                headers={"Content-Type": "application/msgpack"})
            self._raise(resp, f"submit template {len(uid_chunk)} task(s)")
            return resp.json()

        if len(chunks) == 1:
            return _submit_chunk(chunks[0])

        results = []
        batch_results: dict[int, list] = {}
        errors: list[str] = []
        with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
            futures = {pool.submit(_submit_chunk, c): i
                       for i, c in enumerate(chunks)}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    batch_results[idx] = fut.result()
                except Exception as e:
                    errors.append(str(e))

        if errors:
            detail = '; '.join(errors[:3])
            raise RuntimeError(
                f"submit failed for {len(errors)} chunk(s): {detail}")

        for i in sorted(batch_results):
            results.extend(batch_results[i])
        return results

    def wait_tasks(self, uids: list[str],
                   timeout: float | None = None) -> list[dict]:
        """
        Wait for tasks to reach terminal state via SSE notifications.

        Purely client-side: the persistent ``_on_task_done`` callback
        (registered at session init) accumulates completions into
        ``self._completed``.  This method checks the accumulator and
        blocks only until every requested UID appears there.

        Falls back to periodic polling when no ``BridgeClient`` is
        available (e.g. direct construction in tests).

        Args:
            uids: Task UIDs to wait for.
            timeout: Seconds to wait (None = forever).

        Returns:
            list[dict]: Completed task dicts.
        """
        self._require_session()

        # ------------------------------------------------------------------
        # Check if SSE notifications are available
        # ------------------------------------------------------------------
        has_sse = (self._bc is not None and
                   self._endpoint_id is not None and
                   self._plugin_name is not None)

        if not has_sse:
            return self._wait_tasks_poll(uids, timeout)

        # ------------------------------------------------------------------
        # SSE-based wait (preferred path)
        # ------------------------------------------------------------------

        # Build pending set and register waiter atomically so no
        # completions slip through between the check and the
        # registration.  Lock order matches _on_task_done:
        # completed_lock → waiters_lock.
        done = threading.Event()
        with self._completed_lock:
            if all(uid in self._completed for uid in uids):
                return [self._completed[uid] for uid in uids]

            pending = set(uid for uid in uids
                          if uid not in self._completed)

            with self._waiters_lock:
                waiter = (pending, done)
                self._waiters.append(waiter)

        try:
            done.wait(timeout=timeout)
        finally:
            with self._waiters_lock:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass

        with self._completed_lock:
            return [self._completed.get(uid, {"uid": uid,
                                              "state": "UNKNOWN"})
                    for uid in uids]

    def _wait_tasks_poll(self, uids: list[str],
                         timeout: float | None = None) -> list[dict]:
        """Fallback wait via periodic polling (no SSE available)."""

        url     = self._url(f"wait/{self.sid}")
        payload: dict = {"uids": uids}
        if timeout is not None:
            payload["timeout"] = timeout

        deadline = (time.time() + timeout) if timeout else None

        while True:
            resp = self._http.post(url, json=payload)
            self._raise(resp, f"wait {len(uids)} task(s)")
            tasks = resp.json()

            # Check if all are terminal
            all_done = all(
                str(t.get('state', '')).upper() in TERMINAL_STATES
                for t in tasks)
            if all_done:
                return tasks

            if deadline and time.time() >= deadline:
                return tasks  # return whatever we have

            time.sleep(1.0)

    def list_tasks(self) -> dict:
        """List all tasks in this session."""
        self._require_session()

        resp = self._http.get(self._url(f"list_tasks/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def get_task(self, uid: str) -> dict:
        """
        Retrieve info for a single task.
        """
        self._require_session()

        url = self._url(f"task/{self.sid}/{uid}")
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()

    def cancel_task(self, uid: str) -> dict:
        """
        Cancel a task.
        """
        self._require_session()

        url = self._url(f"cancel/{self.sid}/{uid}")
        resp = self._http.post(url)
        self._raise(resp)
        return resp.json()

    def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.
        """
        self._require_session()

        url = self._url(f"cancel_all/{self.sid}")
        resp = self._http.post(url)
        self._raise(resp)
        return resp.json()


# ---------------------------------------------------------------------------
# Server-side plugin
# ---------------------------------------------------------------------------

class PluginRhapsody(Plugin):
    '''
    Rhapsody plugin for ORBIT.

    Exposes the RHAPSODY Session / Task API via REST endpoints:

    - POST  /rhapsody/register_session      – create session
    - POST  /rhapsody/submit/{sid}           – submit tasks
    - POST  /rhapsody/wait/{sid}             – query task states
    - GET   /rhapsody/list_tasks/{sid}       – list all tasks
    - GET   /rhapsody/task/{sid}/{uid}       – get single task
    - POST  /rhapsody/cancel/{sid}/{uid}     – cancel single task
    - POST  /rhapsody/cancel_all/{sid}       – cancel all tasks

    Notification topics: ``session_status``, ``task_status``,
    ``task_status_batch``.
    '''

    plugin_name = "rhapsody"
    session_class = RhapsodySession
    client_class = RhapsodyClient
    version = '0.0.1'

    ui_config = {
        "icon": "🎼",
        "title": "Rhapsody Tasks",
        "description": "Submit compute tasks, wait for results, view stdout/stderr.",
        "forms": [{
            "id": "submit",
            "title": "📝 Submit Task",
            "layout": "single",
            "fields": [
                {"name": "exec", "type": "text", "label": "Executable",
                 "default": "/bin/echo", "css_class": "rh-exec"},
                {"name": "args", "type": "text", "label": "Arguments (space-separated)",
                 "default": "hello from rhapsody", "css_class": "rh-args"},
                {"name": "backends", "type": "select", "label": "Backend",
                 "options": ["dragon_v3", "concurrent"],
                 "css_class": "rh-backends"},
                {"name": "timeout", "type": "number", "label": "Timeout (s)",
                 "default": "", "css_class": "rh-timeout"},
                {"name": "ranks",   "type": "number", "label": "MPI Ranks",
                 "default": "", "css_class": "rh-ranks"},
                {"name": "type",    "type": "select", "label": "Task Type",
                 "options": ["", "mpi"],
                 "css_class": "rh-type"},
                {"name": "cwd",     "type": "text",   "label": "Working Dir",
                 "default": "", "css_class": "rh-cwd"},
            ],
            "submit": {"label": "▶ Submit Task", "style": "success"}
        }],
        "monitors": [{
            "id": "tasks",
            "title": "📊 Task Monitor",
            "type": "task_list",
            "css_class": "rh-output",
            "empty_text": "No tasks submitted yet."
        }],
        "notifications": {
            "topic": "task_status",
            "id_field": "uid",
            "state_field": "state"
        }
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Rhapsody loads on compute nodes (inside an allocation) and on
        standalone hosts (no batch system at all).  Both can host Dragon
        workers; bridges and login nodes deliberately don't load Rhapsody.
        """
        from .utils import host_role
        return host_role(app)['role'] in ('compute', 'standalone')

    def __init__(self, app: FastAPI, instance_name: str = "rhapsody"):
        super().__init__(app, instance_name)

        self.add_route_post('submit/{sid}', self.submit_tasks)
        self.add_route_post('wait/{sid}', self.wait_tasks)
        self.add_route_get('list_tasks/{sid}', self.list_tasks)
        self.add_route_get('task/{sid}/{uid}', self.get_task)
        self.add_route_post('cancel/{sid}/{uid}', self.cancel_task)
        self.add_route_post('cancel_all/{sid}', self.cancel_all_tasks)

    async def register_session(self, request: Request) -> dict:
        """Register a new Rhapsody session.

        Accepts an optional JSON body with ``{"backends": ["name", ...]}``.

        Session initialization happens asynchronously in the background.
        The SID is returned immediately.  The client should wait for a
        ``session_status`` SSE notification (``status: "ready"``) before
        submitting tasks, or handle HTTP 409 on early requests.
        """
        try:
            data = await request.json()
        except Exception:
            data = {}

        backend_names       = data.get('backends')
        # Endpoint-startup default: when the client doesn't specify a
        # backend, honour $RADICAL_ORBIT_RHAPSODY_BACKEND so the endpoint
        # operator can pick the right backend at service launch time
        # (e.g. 'concurrent' on a laptop without Dragon).  Falls back
        # to the session class's own default ('dragon_v3') if neither
        # is set.
        if not backend_names:
            env_backend = os.environ.get('RADICAL_ORBIT_RHAPSODY_BACKEND')
            if env_backend:
                backend_names = [env_backend]
        notify_batch_window = data.get('notify_batch_window',
                                       NOTIFY_BATCH_WINDOW)
        notify_batch_size   = data.get('notify_batch_size',
                                       NOTIFY_BATCH_SIZE)

        # Build session directly to avoid race on shared plugin state
        self._ensure_cleanup_task()
        sid     = f"session.{uuid.uuid4().hex[:8]}"
        session = self.session_class(
            sid,
            backend_names=backend_names,
            notify_batch_window=float(notify_batch_window),
            notify_batch_size=int(notify_batch_size),
        )
        session._plugin = self
        self._sessions[sid]            = session
        self._session_last_access[sid] = time.time()
        log.info("[%s] Registered session %s", self.instance_name, sid)

        # Kick off initialization in the background so the HTTP response
        # (and therefore the WebSocket slot) is released immediately.
        asyncio.create_task(self._init_session(sid, session))

        return {"sid": sid, "status": "initializing"}

    async def _init_session(self, sid: str, session) -> None:
        """Background task: initialize a session and notify via SSE."""
        if session._init_ready.is_set():
            return  # already initialized (e.g. by test setup)

        try:
            if hasattr(session, 'initialize'):
                await session.initialize()

            self._dispatch_notify("session_status", {
                "sid":    sid,
                "status": "ready",
            })

        except Exception as e:
            log.error("[%s] Session %s init failed: %s",
                      self.instance_name, sid, e)
            self._dispatch_notify("session_status", {
                "sid":    sid,
                "status": "failed",
                "error":  str(e),
            })

    # -- route handlers -----------------------------------------------------

    async def submit_tasks(self, request: Request) -> dict:
        sid  = request.path_params['sid']

        prof = getattr(getattr(self._app.state, 'endpoint_service', None),
                       '_prof', None)

        if prof: prof.prof('rh_parse_body', msg=sid)
        data = await request.json()
        if prof: prof.prof('rh_parse_body_done', msg=sid)

        # Support template compression: {"template": {...}, "uids": [...]}
        template = data.get('template')
        if template is not None:
            uids = data.get('uids', [])
            if prof: prof.prof('rh_template_expand',
                               msg='%d tasks' % len(uids))
            task_dicts = [dict(template, uid=uid) for uid in uids]
            if prof: prof.prof('rh_template_expand_done')
            pre_expanded = True
        else:
            task_dicts = data.get('tasks', [])
            pre_expanded = False

        return await self._forward(sid, RhapsodySession.submit_tasks,
                                   task_dicts=task_dicts,
                                   pre_expanded=pre_expanded)

    async def wait_tasks(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        uids = data.get('uids', [])
        timeout = data.get('timeout')
        return await self._forward(sid, RhapsodySession.wait_tasks,
                                   uids=uids, timeout=timeout)

    async def list_tasks(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.list_tasks)

    async def get_task(self, request: Request) -> dict:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.get_task, uid=uid)

    async def cancel_task(self, request: Request) -> dict:
        sid = request.path_params['sid']
        uid = request.path_params['uid']
        return await self._forward(sid, RhapsodySession.cancel_task, uid=uid)

    async def cancel_all_tasks(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.cancel_all_tasks)

