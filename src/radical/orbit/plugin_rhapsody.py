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
import math
import os
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException, Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin, DEFAULT_SID
from .client import PluginClient

log = logging.getLogger("radical.orbit")

# ---------------------------------------------------------------------------
# Route templates — single-sourced so the ``add_route_*`` registration and the
# client-helper (and broker-hosted dispatcher) URL formatting cannot drift.
# Each template doubles as the ``add_route_*`` path (``{name}`` = path param)
# and as a ``.format(...)`` string for the helpers.  Every rhapsody route is
# formatted by a ``RhapsodyClient`` helper, so all of them are lifted here.
# ---------------------------------------------------------------------------
ROUTE_SUBMIT      = 'submit/{sid}'
ROUTE_WAIT        = 'wait/{sid}'
ROUTE_LIST_TASKS  = 'list_tasks/{sid}'
ROUTE_TASK        = 'task/{sid}/{uid}'
ROUTE_CANCEL      = 'cancel/{sid}/{uid}'
ROUTE_CANCEL_ALL  = 'cancel_all/{sid}'

TERMINAL_STATES     = {'DONE', 'FAILED', 'CANCELED', 'COMPLETED'}
WS_PAYLOAD_LIMIT    = 8 * 1024 * 1024  # target max per batch (conservative)
NOTIFY_BATCH_SIZE   = 1024             # max tasks per bulk notification
NOTIFY_BATCH_WINDOW = 0.25             # seconds to accumulate before flush

# Endpoint-level override for the coalescing window above (seconds).  The
# default trades ~250 ms of completion latency for fewer WS frames; a
# latency-sensitive endpoint (co-located service, sequential task round
# trips) sets `0` to flush every completion immediately.
ENV_NOTIFY_WINDOW   = 'RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW'

# Guard optional dependencies
try:
    import rhapsody as rh
except ImportError:
    rh = None

import cloudpickle as _cp
import msgpack
from . import _prof as rprof


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


def _resolve_notify_window() -> float:
    """Resolve the notification coalescing window (seconds).

    Honours ``$RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW`` so the endpoint
    operator can pick the latency / frame-count trade-off at service launch
    (``0`` flushes every completion immediately).  Read once per plugin, not
    per flush.  A non-numeric, negative or non-finite value warns and falls
    back to ``NOTIFY_BATCH_WINDOW`` — ``nan`` would otherwise pass every
    comparison and silently act as a zero window.
    """
    raw = os.environ.get(ENV_NOTIFY_WINDOW, '').strip()
    if not raw:
        return NOTIFY_BATCH_WINDOW
    try:
        window = float(raw)
        if window < 0 or not math.isfinite(window):
            raise ValueError('must be finite and non-negative')
    except ValueError:
        log.warning("[rhapsody] invalid %s=%r — using %ss",
                    ENV_NOTIFY_WINDOW, raw, NOTIFY_BATCH_WINDOW)
        return NOTIFY_BATCH_WINDOW
    return window


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
                 allow_pickled_tasks: bool = True):
        """
        Initialize a RhapsodySession.

        Args:
            sid (str):  Unique session identifier.
            backend_names (list[str] | None):
                Backends to configure.  Defaults to ``['dragon_v3']``.
            allow_pickled_tasks (bool):
                Allow cloudpickle-encoded function tasks.  Defaults to ``True``.
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

        # Notification batcher: accumulate completions and flush in bulk.
        # Batch size is the module constant; the coalescing window defaults
        # to the module constant and is overridden per endpoint by
        # PluginRhapsody._create_session.  A single pending flush task
        # drains the buffer (see _queue_notification).
        self._notify_buf: list[dict] = []
        self._notify_lock            = threading.Lock()
        self._flush_scheduled        = False
        self._notify_window          = NOTIFY_BATCH_WINDOW

        # Cache for deserialized cloudpickle payloads — avoids decoding the
        # same encoded string N times when a batch repeats identical blobs.
        self._pickle_cache: dict[str, object] = {}

        # Profiler — injected by PluginRhapsody._create_session (shared with
        # the endpoint runtime); ``None`` for sessions built directly in tests.
        self._prof: rprof.Profiler | None = None

    async def initialize(self) -> None:
        """Asynchronously initialize the session and its backends."""
        try:
            backends = []
            for name in self.backend_names:
                b = rh.get_backend(name)
                if hasattr(b, '__await__'):
                    b = await b
                backends.append(b)

            # Offload the blocking ``rh.Session`` construction to a worker
            # thread (the established ``_prepare_batch`` offload pattern) so it
            # keeps the work loop responsive to other requests during init.
            self._rh_session = await asyncio.to_thread(
                rh.Session, backends=backends, uid=self._sid)

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

            self._init_ready.set()
            log.info("[%s] Session initialization complete", self._sid)

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

        # Only fire for non-terminal states; terminal states are handled by
        # _watch_batch (which carries the full completion payload).
        if state_str.upper() in TERMINAL_STATES:
            return

        self.notify("task_status", {
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

    def _prepare_batch(self, task_dicts: list[dict]) -> list:
        """Deserialize and create task objects (runs in a worker thread).

        CPU-bound work (cloudpickle, from_dict) is offloaded here so the
        work loop stays responsive for WebSocket keepalive.
        """
        deserialized = [self._deserialize_task(td) for td in task_dicts]
        return [rh.BaseTask.from_dict(td) for td in deserialized]

    async def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        """
        Submit a list of tasks.

        Each dict is converted to a ``ComputeTask`` or ``AITask`` via
        ``BaseTask.from_dict()``.  Function fields encoded as cloudpickle
        blobs or import-path strings are deserialized first.

        Large submissions are processed one chunk at a time: the CPU-bound
        deserialization is offloaded to a worker thread, then the chunk is
        handed to the backend.  Both the ``to_thread`` offload and the
        ``await`` on the backend keep the work loop responsive.

        Returns:
            list[dict]: Minimal ack dicts ``{uid, state}``.
        """
        self._check_initialized()

        batch_n = len(task_dicts)
        bid     = task_dicts[0].get('uid', '?') if task_dicts else '?'
        if self._prof:
            self._prof.prof('rh_submit', uid=bid, msg=str(batch_n))

        _CHUNK    = 4096
        results   = []
        all_tasks = []   # collect for batch watcher

        for i in range(0, len(task_dicts), _CHUNK):
            chunk = task_dicts[i:i + _CHUNK]
            tasks = await asyncio.to_thread(self._prepare_batch, chunk)
            await self._rh_session.submit_tasks(tasks)
            for t in tasks:
                uid_str = str(t.uid)
                self._tasks[uid_str] = t
                results.append({"uid": uid_str,
                                "state": str(t.get("state"))})
            all_tasks.extend(tasks)

        # Start a single batch watcher instead of per-task watchers
        if self._plugin and all_tasks:
            asyncio.ensure_future(self._watch_batch(all_tasks))

        if self._prof:
            self._prof.prof('rh_submit_done', uid=bid, msg=str(batch_n))
        return results

    def _queue_notification(self, payload: dict) -> None:
        """Add a task notification to the batch buffer and ensure a
        flush is scheduled.

        Thread-safe — called from watcher coroutines.  A full buffer — or a
        zero coalescing window (latency-sensitive endpoint) — flushes
        immediately; otherwise a single delayed flush is scheduled (and only
        one is ever pending at a time).
        """
        with self._notify_lock:
            self._notify_buf.append(payload)
            window   = self._notify_window
            now      = window <= 0 \
                       or len(self._notify_buf) >= NOTIFY_BATCH_SIZE
            schedule = not now and not self._flush_scheduled
            if schedule:
                self._flush_scheduled = True

        if now:
            self._flush_notifications()
        elif schedule:
            self._schedule_flush(delay=window)

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
            self._flush_scheduled = False
            if not self._notify_buf:
                return
            batch = list(self._notify_buf)
            self._notify_buf.clear()

        if len(batch) == 1:
            self.notify("task_status", batch[0])
        else:
            self.notify("task_status_batch", {"tasks": batch})

    def _task_future(self, uid_str: str, task) -> "asyncio.Future":
        """Return the rhapsody wait-future for a single task.

        Reaches into rhapsody's private ``_state_manager`` — the sole
        coupling to an undocumented backend internal, isolated here so the
        dependency is a single greppable line that a dependency upgrade can
        find.
        """
        return self._rh_session._state_manager.get_wait_future(uid_str, task)

    async def _watch_batch(self, tasks):
        """Watch a batch of tasks, notifying as each completes.

        Uses ``asyncio.wait(FIRST_COMPLETED)`` to drain completions
        incrementally.  Notifications are queued per-task as soon as
        each finishes — the notification buffer (``_queue_notification``)
        batches them opportunistically so fast-completing tasks are grouped
        into single SSE messages while slow tasks don't block others.
        """
        # Build uid map and per-task futures
        fut_to_uid: dict[asyncio.Future, str] = {}
        uid_to_task: dict[str, object]         = {}

        for t in tasks:
            uid     = self._get_attr(t, 'uid')
            uid_str = str(uid) if uid else '?'
            uid_to_task[uid_str] = t

        if not self._rh_session:
            for uid_str in uid_to_task:
                self._queue_notification({
                    "uid": uid_str, "state": "FAILED",
                    "error": "Session closed"})
            return

        for uid_str, t in uid_to_task.items():
            fut_to_uid[self._task_future(uid_str, t)] = uid_str

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
        # Session-wide accumulator for terminal task notifications, guarded
        # by a single Condition.  Populated by a persistent SSE callback
        # registered after session init, so no notification is ever lost;
        # waiters block on the same Condition and wake on each completion.
        self._cond = threading.Condition()
        self._completed: dict[str, dict] = {}
        # UIDs submitted via this client — lets wait_tasks() reject UIDs that
        # were never submitted instead of blocking on them forever.
        self._submitted: set[str] = set()

    def _on_task_done(self, endpoint, plugin, topic, data):
        """Persistent SSE callback: accumulate terminal task states.

        Handles both single ``task_status`` and bulk
        ``task_status_batch`` notifications.
        """
        if topic == 'task_status_batch':
            tasks = data.get('tasks', [])
        else:
            tasks = [data]

        with self._cond:
            changed = False
            for t in tasks:
                # Decode base64-encoded return values
                if t.get('_return_value_encoding') == 'base64':
                    t['return_value'] = base64.b64decode(t['return_value'])
                    del t['_return_value_encoding']

                uid   = t.get('uid')
                state = str(t.get('state', '')).upper()
                if uid and state in TERMINAL_STATES:
                    self._completed[uid] = t
                    changed = True

            if changed:
                self._cond.notify_all()

    def register_session(self, backends: list[str] | None = None,
                         init_timeout: float = 120):
        """
        Register a session, optionally specifying backend names.

        The endpoint initializes the session asynchronously.  This method
        blocks until a ``session_status`` SSE notification confirms
        that the session is ready (or until *init_timeout* seconds).

        Falls back to polling when no event subscription is available.

        Args:
            backends: List of backend names (e.g. ``['dragon_v3']``).
                      Defaults to ``['dragon_v3']`` on the server side.
            init_timeout: Seconds to wait for session init (default 120).
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
        resp = self._http.post(self._url('register_session'), json=payload)
        self._raise(resp)

        data      = resp.json()
        self._sid = data['sid']
        status    = data.get('status')

        # Reset the session-wide task completion accumulator
        with self._cond:
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
                    self._url(ROUTE_LIST_TASKS.format(sid=self.sid)))
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
        - Encodes ``metadata`` the same way when it is not JSON-safe
          (e.g. asyncflow dependency descriptors carry raw callables).
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

        # Serialize args/kwargs/metadata if not JSON-safe
        for field in ('args', 'kwargs', 'metadata'):
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

        Large submissions are split so each payload stays within the
        WebSocket frame limit (:data:`WS_PAYLOAD_LIMIT`); the batches are
        submitted sequentially.

        UIDs are assigned client-side (if absent) so the caller can
        start waiting for SSE notifications immediately.

        Args:
            task_dicts: List of task specification dicts.

        Returns:
            list[dict]: Submitted task info (uid, state).
        """
        self._require_session()

        # --- serialize callables and clean up internal fields ---
        for td in task_dicts:
            self._serialize_task(td)

        # --- assign UIDs client-side so we know them before submit ---
        for td in task_dicts:
            if 'uid' not in td:
                td['uid'] = f"task.{uuid.uuid4().hex[:8]}"

        # Remember what we submitted so wait_tasks() can validate UIDs.
        with self._cond:
            self._submitted.update(str(td['uid']) for td in task_dicts)

        # --- split into frame-size-bounded batches ---
        url = self._url(ROUTE_SUBMIT.format(sid=self.sid))
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

        # --- submit batches sequentially ---
        results: list[dict] = []
        for b in batches:
            resp = self._http.post(
                url,
                data=msgpack.packb({"tasks": b}, use_bin_type=True),
                headers={"Content-Type": "application/msgpack"})
            self._raise(resp, f"submit {len(b)} task(s)")
            results.extend(resp.json())
        return results

    def wait_tasks(self, uids: list[str],
                   timeout: float | None = None) -> list[dict]:
        """
        Wait for tasks to reach terminal state via SSE notifications.

        Purely client-side: the persistent ``_on_task_done`` callback
        (registered at session init) accumulates completions into
        ``self._completed``.  This method checks the accumulator and
        blocks only until every requested UID appears there.

        Falls back to periodic polling when no event subscription is
        available (e.g. direct construction in tests).

        Args:
            uids: Task UIDs to wait for.
            timeout: Seconds to wait (None = forever).

        Returns:
            list[dict]: Completed task dicts.
        """
        self._require_session()

        # ------------------------------------------------------------------
        # Normalize input and fail fast.  Accept task UIDs (str) or task
        # objects/dicts (extract their 'uid'); reject anything else with a
        # TypeError, and reject UIDs never submitted via this session with a
        # ValueError.  Either way is better than blocking forever on a value
        # the wait can never satisfy.
        # ------------------------------------------------------------------
        norm: list[str] = []
        for item in uids:
            if isinstance(item, str):
                norm.append(item)
                continue
            uid = getattr(item, 'uid', None)
            if uid is None:
                try:
                    uid = item['uid']
                except (TypeError, KeyError, IndexError):
                    uid = None
            if uid is None:
                raise TypeError(
                    "wait_tasks: each item must be a task UID (str) or a "
                    "task object/dict with a 'uid'; got "
                    f"{type(item).__name__}")
            norm.append(str(uid))
        uids = norm

        with self._cond:
            unknown = [u for u in uids
                       if u not in self._submitted
                       and u not in self._completed]
        if unknown:
            raise ValueError(
                "wait_tasks: unknown task UID(s) not submitted via this "
                f"session: {unknown}")

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
        # The persistent `_on_task_done` callback fills `_completed` and
        # notifies the Condition on every completion; block on it until every
        # requested UID has landed (or the deadline passes).
        deadline = (time.time() + timeout) if timeout is not None else None
        with self._cond:
            while not all(uid in self._completed for uid in uids):
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                self._cond.wait(remaining)

            return [self._completed.get(uid, {"uid": uid,
                                              "state": "UNKNOWN"})
                    for uid in uids]

    def _wait_tasks_poll(self, uids: list[str],
                         timeout: float | None = None) -> list[dict]:
        """Fallback wait via periodic polling (no SSE available)."""

        url     = self._url(ROUTE_WAIT.format(sid=self.sid))
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

        resp = self._http.get(self._url(ROUTE_LIST_TASKS.format(sid=self.sid)))
        self._raise(resp)
        return resp.json()

    def get_task(self, uid: str) -> dict:
        """
        Retrieve info for a single task.
        """
        self._require_session()

        url = self._url(ROUTE_TASK.format(sid=self.sid, uid=uid))
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()

    def cancel_task(self, uid: str) -> dict:
        """
        Cancel a task.
        """
        self._require_session()

        url = self._url(ROUTE_CANCEL.format(sid=self.sid, uid=uid))
        resp = self._http.post(url)
        self._raise(resp)
        return resp.json()

    def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.
        """
        self._require_session()

        url = self._url(ROUTE_CANCEL_ALL.format(sid=self.sid))
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
        workers; brokers and login nodes deliberately don't load Rhapsody.
        """
        from .utils import host_role
        return host_role(app)['role'] in ('compute', 'standalone')

    def __init__(self, app: FastAPI, instance_name: str = "rhapsody"):
        super().__init__(app, instance_name)

        # Endpoint-wide notification coalescing window — resolved once here
        # (not per flush) and handed to every session this plugin creates.
        self._notify_window = _resolve_notify_window()

        self.add_route_post(ROUTE_SUBMIT,     self.submit_tasks)
        self.add_route_post(ROUTE_WAIT,       self.wait_tasks)
        self.add_route_get (ROUTE_LIST_TASKS, self.list_tasks)
        self.add_route_get (ROUTE_TASK,       self.get_task)
        self.add_route_post(ROUTE_CANCEL,     self.cancel_task)
        self.add_route_post(ROUTE_CANCEL_ALL, self.cancel_all_tasks)

    async def register_session(self, request: Request) -> dict:
        """Register a new Rhapsody session.

        Accepts an optional JSON body with ``{"backends": ["name", ...]}``
        plus the base session-policy fields ``sid`` / ``lifetime`` / ``ttl``
        (see :meth:`Plugin.register_session`).

        Session initialization happens asynchronously in the background.
        The SID is returned immediately.  The client should wait for a
        ``session_status`` SSE notification (``status: "ready"``) before
        submitting tasks, or handle HTTP 409 on early requests.
        """
        try:
            data = await request.json()
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}

        # Validate `backends` synchronously: a non-list (e.g. a bare string)
        # would otherwise be iterated char-by-char and fail only in the
        # background session init, long after the 200 was returned.
        backends = data.get('backends')
        if backends is not None and not isinstance(backends, list):
            raise HTTPException(
                status_code=400,
                detail="'backends' must be a list of backend names")

        owner              = self._request_owner(request)
        sid, lifetime, ttl = self._normalize_session_policy(data)
        backend_names      = self._backend_names(backends)

        self._ensure_cleanup_task()

        # Reserved persistent 'default' — created on demand (see the
        # `_ensure_default_session` override), reporting its real status.
        if sid == DEFAULT_SID:
            session = await self._ensure_default_session()
            self._touch(sid)
            return {"sid": sid, "status": self._session_status(session)}

        if sid is None:
            sid = f"session.{uuid.uuid4().hex[:8]}"
            while sid in self._sessions:
                sid = f"session.{uuid.uuid4().hex[:8]}"

        # Reconnect to an existing session — do not rebuild it.  Owner check
        # first (bug A5: a session created with an owner must not be hijacked
        # by another participant).  Report the session's real status: its init
        # may have long completed, and the reconnecting client would otherwise
        # wait for a `session_status` notification emitted before it attached.
        if sid in self._sessions:
            self._check_owner(sid, owner)
            self._check_policy_conflict(sid, lifetime, ttl)
            self._touch(sid)
            log.info("[%s] Reconnected session %s", self.instance_name, sid)
            return {"sid": sid,
                    "status": self._session_status(self._sessions[sid])}

        # Build (with the injected profiler) and record, then kick the
        # background init so the HTTP response — and the WebSocket slot — is
        # released immediately, before Dragon is up.
        session = self._create_session(sid, backend_names=backend_names)
        self._record_session(sid, session, lifetime, ttl, owner)
        log.info("[%s] Registered session %s", self.instance_name, sid)
        asyncio.create_task(self._init_session(sid, session))

        return {"sid": sid, "status": "initializing"}

    @staticmethod
    def _backend_names(backends: list[str] | None) -> list[str] | None:
        """Resolve the backend list for a new session.

        When the client names no backend, honour
        ``$RADICAL_ORBIT_RHAPSODY_BACKEND`` so the endpoint operator can pick
        the right backend at service launch (e.g. ``concurrent`` on a laptop
        without Dragon).  Returns ``None`` when neither is set so the session
        falls back to its own default (``dragon_v3``).
        """
        if backends:
            return backends
        env_backend = os.environ.get('RADICAL_ORBIT_RHAPSODY_BACKEND')
        return [env_backend] if env_backend else None

    def _create_session(self, sid: str, **kwargs) -> RhapsodySession:
        """Build a session and inject the endpoint's shared profiler and
        notification window.

        Extends the base factory (which injects ``_plugin``) so every
        rhapsody session profiles against the one endpoint-runtime profiler
        instead of digging it out of app state on first use, and coalesces
        notifications over the endpoint-configured window.
        """
        session = super()._create_session(sid, **kwargs)
        svc = getattr(self._app.state, 'endpoint_service', None)
        session._prof = getattr(svc, '_prof', None) or \
                        rprof.Profiler('rhapsody', ns='radical.orbit')
        session._notify_window = self._notify_window
        return session

    @staticmethod
    def _session_status(session) -> str:
        """Report a session's real initialization state.

        ``initializing`` while the background init is still running,
        ``failed`` once init errored, ``ready`` otherwise.
        """
        if not session._init_ready.is_set():
            return "initializing"
        if session._init_error:
            return "failed"
        return "ready"

    async def _ensure_default_session(self):
        """Return the persistent ``default`` session, creating it on demand.

        Rhapsody sessions need their background initialization kicked, so
        the base on-demand path does not suffice: the session is built
        through rhapsody's direct-build path (endpoint-startup backend
        defaults — no client body is available here) and its init task is
        started, exactly like a fresh create.  Creation is guarded by the
        base default lock so two concurrent first requests cannot
        double-create it.
        """
        session = self._sessions.get(DEFAULT_SID)
        if session is not None:
            return session
        async with self._default_lock:
            session = self._sessions.get(DEFAULT_SID)
            if session is None:
                session = self._create_session(
                    DEFAULT_SID, backend_names=self._backend_names(None))
                self._record_session(DEFAULT_SID, session, 'persistent', None)
                log.info("[%s] Created persistent 'default' session",
                         self.instance_name)
                asyncio.create_task(self._init_session(DEFAULT_SID, session))
            return session

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
        sid        = request.path_params['sid']
        data       = await request.json()
        task_dicts = data.get('tasks', [])
        return await self._forward(sid, RhapsodySession.submit_tasks,
                                   task_dicts=task_dicts)

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

