'''
Rhapsody Plugin for ORBIT.

Exposes the RHAPSODY Session/Task API so that remote clients can submit
and monitor compute / AI tasks on endpoint nodes.
'''

import asyncio
import base64
import functools
import importlib
import importlib.util
import json
import logging
import math
import os
import re
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException, Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin, DEFAULT_SID
from .client import PluginClient
from .protocol import FRAME_CAP

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
ROUTE_DATA_START     = 'data_start/{sid}'
ROUTE_DATA_ENDPOINTS = 'data_endpoints/{sid}'
ROUTE_DATA_SHUTDOWN  = 'data_shutdown/{sid}/{name}'

# data backend names travel as a URL path segment (data_shutdown)
DATA_NAME_RE = re.compile(r'[A-Za-z0-9_.-]+')

TERMINAL_STATES     = {'DONE', 'FAILED', 'CANCELED', 'COMPLETED'}
WS_PAYLOAD_LIMIT    = 8 * 1024 * 1024  # target max per batch (conservative)
NOTIFY_BATCH_SIZE   = 1024             # max tasks per bulk notification
NOTIFY_BATCH_WINDOW = 0.25             # seconds to accumulate before flush
CLOSE_DRAIN_TIMEOUT = 30.0             # close waits this long for ops in flight

# Both budgets below are halves of the transport frame cap.  The *effective*
# cap is resolved per plugin from the hosting runtime (see
# ``_resolve_frame_cap``) because it is tunable; these module constants are
# the protocol-default fallback, and the default for a session built without
# a plugin (direct construction, tests).
BUDGET_RATIO        = 2

# Cumulative payload budget for one notification frame.  ``NOTIFY_BATCH_SIZE``
# alone cannot bound a batch: a single completion may carry an encoded
# ``return_value`` of up to the return-value limit, so a handful of them
# exceeds the frame cap, the transport drops the frame, and every waiter
# hangs.  Flushes therefore split on bytes as well as on count — the same
# size-aware batching the client applies to submission (WS_PAYLOAD_LIMIT).
NOTIFY_BATCH_BYTES  = FRAME_CAP // BUDGET_RATIO

# Size bound for one encoded ``return_value``.  The frame cap bounds the
# *whole* notification / response frame, so a single result may claim at most
# a fraction of it.  A larger result is reported as a task error instead of
# being encoded: an oversized frame is dropped by the transport, which would
# leave the client waiting forever, and truncating would hand back a corrupt
# object.
RETURN_VALUE_LIMIT  = FRAME_CAP // BUDGET_RATIO

# Size bound (UTF-8 bytes) for the stdout and for the stderr a terminal
# notification carries.  Unlike a return value, output can be shortened
# meaningfully: a longer stream keeps its tail behind a truncation marker,
# and the full text stays available via this session's ``get_task``.  Both
# streams together claim a quarter of the frame, so a payload at the
# return-value limit still fits one frame.
OUTPUT_RATIO        = 8
OUTPUT_NOTIFY_LIMIT = FRAME_CAP // OUTPUT_RATIO

# Endpoint-level override for the coalescing window above (seconds).  The
# default trades ~250 ms of completion latency for fewer WS frames; a
# latency-sensitive endpoint (co-located service, sequential task round
# trips) sets `0` to flush every completion immediately.
ENV_NOTIFY_WINDOW   = 'RADICAL_ORBIT_RHAPSODY_NOTIFY_WINDOW'

# Endpoint-level telemetry switch: rhapsody session telemetry (when its
# ``[telemetry]`` extra is installed) is on unless this is set to one of
# ``0`` / ``false`` / ``no`` / ``off``.  Resolved once per process.
ENV_TELEMETRY       = 'RADICAL_ORBIT_RHAPSODY_TELEMETRY'

# Guard optional dependencies
try:
    import rhapsody as rh
except ImportError:
    rh = None

import cloudpickle as _cp
import msgpack
from . import _prof as rprof


@functools.cache
def _telemetry_available() -> bool:
    """Whether rhapsody's ``[telemetry]`` extra (opentelemetry-sdk) is
    installed.  Probed up front because rhapsody 0.4 fails only inside
    ``start_telemetry()``, after wiring a half-built manager into the
    session.  Cached, so the warning is logged once per process.
    """
    try:
        ok = importlib.util.find_spec('opentelemetry.sdk') is not None
    except ImportError:              # parent package missing
        ok = False
    if not ok:
        log.warning("rhapsody telemetry disabled: opentelemetry-sdk "
                    "(the rhapsody [telemetry] extra) is not installed")
    return ok


@functools.cache
def _resolve_telemetry() -> bool:
    """Whether ``$RADICAL_ORBIT_RHAPSODY_TELEMETRY`` leaves telemetry on.

    Unset means on.  An unrecognised value warns and keeps the default.
    Cached, so the switch is read (and any warning logged) once per process.
    """
    raw = os.environ.get(ENV_TELEMETRY, '').strip().lower()
    if raw in ('0', 'false', 'no', 'off'):
        return False
    if raw not in ('', '1', 'true', 'yes', 'on'):
        log.warning("[rhapsody] invalid %s=%r — telemetry stays on",
                    ENV_TELEMETRY, raw)
    return True


def _json_safe(v):
    """Coerce *v* to a JSON-serializable form.

    Backend-specific kwargs (e.g. dragon ``Policy`` objects passed via
    ``task_backend_specific_kwargs``) are not JSON-encodable; without
    this, any read of the cached task dict (list_tasks / wait_tasks /
    notification serialization) raises ``TypeError`` and the response
    or WS frame is dropped.  Falls back through ``to_dict`` / recursion
    / ``str``.
    """
    if isinstance(v, str):
        return v                    # always encodable; skip a full dumps
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


def _payload_size(payload: dict) -> int:
    """Estimate the wire size (bytes) of one notification payload.

    A notification payload is flat (see ``_NOTIFICATION_KEYS``), so summing
    key and value lengths bounds the frame accurately enough — and, unlike
    ``len(str(payload))``, it does not copy a multi-MiB encoded return value
    just to measure it.  Strings count in UTF-8 bytes (an ASCII string, e.g.
    an encoded return value, without encoding it); non-string values are
    small scalars; 16 bytes each covers them with room to spare.
    """
    size = 2
    for k, v in payload.items():
        if isinstance(v, str):
            size += len(str(k)) + (len(v) if v.isascii()
                                   else len(v.encode('utf-8', 'replace')))
        else:
            size += len(str(k)) + 16
    return size


def _resolve_frame_cap(app) -> int:
    """Resolve the effective transport frame cap for this plugin host.

    ``protocol.FRAME_CAP`` is only the default: the serving runtime packs
    with its own ``frame_cap`` tunable (``EndpointRuntime._frame_cap``), so a
    lower configured cap must shrink the plugin's budgets too — otherwise an
    "allowed" result or batch still overruns the real transport limit and the
    frame is dropped.  The runtime is reached through the established
    ``app.state.endpoint_service`` handle (same one ``_create_session`` uses
    for the profiler); hosts without one (broker plugin host, tests) fall
    back to the protocol default.

    Known limitation: only the *local* cap is discoverable.  The broker's cap
    is configurable as well and is not advertised in the handshake
    (``RegisterAck`` carries no tuning), so a broker configured below its
    endpoints can still reject a frame this budget allows.
    """
    svc = getattr(getattr(app, 'state', None), 'endpoint_service', None)
    cap = getattr(svc, '_frame_cap', None)
    if isinstance(cap, int) and cap > 0:
        return cap
    return FRAME_CAP


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

class _DataEntry:
    """One data backend of a session: the store, its kind, whether its
    start is still running, whether it was asked to stop, and the one stop
    every stopper shares."""

    __slots__ = ('backend', 'kind', 'starting', 'doomed', 'stop')

    def __init__(self, backend, kind: str, starting: bool = False):
        self.backend  = backend
        self.kind     = kind
        self.starting = starting             # only data_start may stop it
        self.doomed   = False                # never register it any more
        self.stop: asyncio.Future | None = None


def _tracked(func):
    """Count a state-changing session route as in flight while it runs.

    ``close()`` waits (bounded) until no tracked route is in flight before
    it tears the rhapsody session down, so such a route never sees a half
    torn-down session.  Read and wait routes stay untracked: a long
    ``wait_tasks`` must not hold up a close.
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        self._check_initialized()
        self._ops += 1
        self._ops_idle.clear()
        try:
            return await func(self, *args, **kwargs)
        finally:
            self._ops -= 1
            if not self._ops:
                self._ops_idle.set()
    return wrapper


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

        # Data backends started via ``data_start``, keyed by name.  Once
        # started they are also registered with the rhapsody session, so
        # session close shuts them down; this map is what the data routes
        # report on, and it holds a name reserved while its start runs.
        self._data: dict[str, _DataEntry] = {}
        # stores whose start failed and whose cleanup failed too: their name
        # is released, but the session close still tries to stop them
        self._leftovers: list[_DataEntry] = []
        self._torn_down = False                # teardown took the leftovers
        self._bg: set[asyncio.Future] = set()  # strong refs to shutdown tasks

        # Close coordination: ``_closing`` turns new requests away (410),
        # tracked routes (``_tracked``) are drained before teardown, and
        # the teardown itself runs as one shielded task.
        self._closing    = False
        self._ops        = 0
        self._ops_idle   = asyncio.Event()
        self._ops_idle.set()
        self._close_task: asyncio.Future | None = None

        # Async init tracking
        self._init_ready = threading.Event()
        self._init_error: str | None = None

        # Notification batcher: accumulate completions and flush in bulk.
        # Batch size is the module constant; the coalescing window and the
        # two frame-cap-derived byte budgets default to the module constants
        # and are overridden per endpoint by PluginRhapsody._create_session
        # (which knows the host runtime's effective cap).  A single pending
        # flush task drains the buffer (see _queue_notification).
        self._notify_buf: list[dict] = []
        self._notify_bytes           = 0
        self._notify_lock            = threading.Lock()
        self._flush_scheduled        = False
        self._notify_window          = NOTIFY_BATCH_WINDOW
        self._notify_batch_bytes     = NOTIFY_BATCH_BYTES
        self._sid_bytes              = _payload_size({'sid': sid})
        self._return_value_limit     = RETURN_VALUE_LIMIT
        self._output_notify_limit    = OUTPUT_NOTIFY_LIMIT

        # Cache for deserialized cloudpickle payloads — avoids decoding the
        # same encoded string N times when a batch repeats identical blobs.
        self._pickle_cache: dict[str, object] = {}

        # Profiler — injected by PluginRhapsody._create_session (shared with
        # the endpoint runtime); ``None`` for sessions built directly in tests.
        self._prof: rprof.Profiler | None = None

    # task notifications carry the session id: they are broadcast to every
    # consumer of the plugin, and task uids are only unique per client
    _SCOPED_TOPICS = ('task_status', 'task_status_batch')

    def notify(self, topic: str, data: dict) -> None:
        """Send a notification; task notifications get this session's ``sid``.

        The payload is copied, so buffered task dicts are not modified.
        """
        if topic in self._SCOPED_TOPICS:
            data = {**data, 'sid': self.sid}
        super().notify(topic, data)

    async def initialize(self) -> None:
        """Asynchronously initialize the session and its backends."""
        try:
            backends = []
            for name in self.backend_names:
                b = rh.get_backend(name)
                if hasattr(b, '__await__'):
                    b = await b
                backends.append(b)

            if self._closing:
                # closed while the backends were created: release them
                for b in backends:
                    try:
                        if hasattr(b, 'shutdown'):
                            await b.shutdown()
                    except Exception as e:
                        log.warning("[%s] backend shutdown failed: %s",
                                    self._sid, e)
                raise RuntimeError("session closed during initialization")

            # Offload the blocking ``rh.Session`` construction to a worker
            # thread (the established ``_prepare_batch`` offload pattern) so it
            # keeps the work loop responsive to other requests during init.
            rh_session = await asyncio.to_thread(
                rh.Session, backends=backends, uid=self._sid)
            if self._closing:
                # closed while the session was being built: the teardown
                # found nothing to close, so this one is ours to close
                await rh_session.close()
                raise RuntimeError("session closed during initialization")
            self._rh_session = rh_session

            # Telemetry is optional: older rhapsody lacks start_telemetry,
            # it needs the [telemetry] extra, the operator can switch it off,
            # and a failure to start it must not fail the session.
            self._telemetry = None
            start_telemetry = getattr(self._rh_session, 'start_telemetry', None)
            if start_telemetry is not None and _resolve_telemetry() \
                    and _telemetry_available():
                try:
                    self._telemetry = await start_telemetry(
                        resource_poll_interval=0.1,
                        checkpoint_path="telemetry-output")
                except Exception as e:
                    log.warning("[%s] telemetry start failed: %r; not "
                                "reporting telemetry for this session",
                                self._sid, e)
                    await self._unwind_telemetry(self._rh_session,
                                                 failed_start=True)
                except BaseException:
                    # cancelled mid-start: unwind and unblock waiters
                    await self._unwind_telemetry(self._rh_session,
                                                 failed_start=True)
                    self._init_error = "session initialization cancelled"
                    self._init_ready.set()
                    raise
            if self._closing:
                # closed while telemetry started: the teardown has closed
                # the session already but could not see this manager
                await self._unwind_telemetry(rh_session, failed_start=True)
                self._telemetry = None
                raise RuntimeError("session closed during initialization")

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

    async def _unwind_telemetry(self, rh_session,
                                failed_start: bool = False) -> None:
        """Stop rhapsody's telemetry manager best effort and detach it.

        Workaround: rhapsody (<= 0.4) ``Session.close()`` stops telemetry
        before shutting the backends down, so a raising ``stop()`` leaks
        the backends -- and a failed ``start_telemetry()`` leaves a
        half-started manager whose ``stop()`` does raise.  Used after a
        failed start and before every rhapsody close; drop this once
        rhapsody isolates telemetry failures itself.

        A stop failure is expected after a failed start (logged at debug)
        and unexpected otherwise (logged as a warning).
        """
        tm = getattr(rh_session, '_telemetry', None)
        if tm is not None:
            try:
                await tm.stop()
            except Exception as e:
                if failed_start:
                    log.debug("[%s] stopping half-started telemetry: %r",
                              self._sid, e)
                else:
                    log.warning("[%s] telemetry stop failed: %r",
                                self._sid, e)
            rh_session._telemetry = None
        sm = getattr(rh_session, '_state_manager', None)
        if hasattr(sm, '_telemetry_observer'):
            sm._telemetry_observer = None

    def _on_task_state_change(self, task, state):
        """Fire notification on intermediate state changes (e.g. RUNNING).

        Called from backend threads — uses lock for _notified_states access.
        The notification goes through the batch buffer like a completion, so
        a burst of state changes is coalesced instead of sent per task.
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

        self._queue_notification({
            "uid":   uid_str,
            "state": state_str,
        })

    def _check_initialized(self) -> None:
        """Check that the session is active and fully initialized.

        Raises:
            HTTPException 409: Session is still initializing.
            HTTPException 410: Session is closing.
            HTTPException 500: Session initialization failed.
            RuntimeError:      Session is closed.
        """
        self._check_active()
        if self._closing:
            # 410 like an expired session; 409 means "still initializing"
            raise HTTPException(status_code=410, detail="session is closing")
        if not self._init_ready.is_set():
            raise HTTPException(status_code=409,
                                detail="session is still initializing")
        if self._init_error:
            raise HTTPException(
                status_code=500,
                detail=f"session init failed: {self._init_error}")
        if self._rh_session is None:
            raise HTTPException(status_code=410, detail="session is closing")

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

    @_tracked
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
            if self._rh_session is None:     # torn down after a drain timeout
                raise HTTPException(status_code=410,
                                    detail="session is closing")
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

        Thread-safe — called from watcher coroutines and backend threads.  A
        buffer full by count or by bytes flushes immediately; otherwise a
        single flush is scheduled (and only one is ever pending at a time).
        A zero coalescing window (latency-sensitive endpoint) schedules that
        flush without delay: it runs on the next loop iteration, so the
        completions a watcher drains in one pass still share one frame.
        """
        size = _payload_size(payload)
        with self._notify_lock:
            self._notify_buf.append(payload)
            self._notify_bytes += size
            window   = max(self._notify_window, 0)
            now      = len(self._notify_buf) >= NOTIFY_BATCH_SIZE \
                       or self._notify_bytes >= self._frame_budget()
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
            else:
                # no loop to flush on: flush here rather than leave the
                # buffer behind a flush that never runs
                self._flush_notifications()

    def _flush_notifications(self) -> None:
        """Flush the notification buffer as one or more bulk messages.

        The drained buffer is split so that no single frame exceeds this
        session's notification byte budget — completions carrying encoded
        return values would otherwise pack a frame past the transport cap,
        and the dropped frame would strand every waiter on those tasks.
        """
        with self._notify_lock:
            self._flush_scheduled = False
            if not self._notify_buf:
                return
            batch = list(self._notify_buf)
            self._notify_buf.clear()
            self._notify_bytes = 0

        for frame in self._split_frames(batch):
            if len(frame) == 1:
                self.notify("task_status", frame[0])
            else:
                self.notify("task_status_batch", {"tasks": frame})

    def _frame_budget(self) -> int:
        """Bytes available for task payloads per frame: ``notify`` adds the
        session id to each frame after the split."""
        # never below half the budget: a tiny cap plus a long sid must not
        # disable coalescing
        return max(self._notify_batch_bytes - self._sid_bytes,
                   self._notify_batch_bytes // 2)

    def _split_frames(self, batch: list[dict]) -> list[list[dict]]:
        """Split notifications into frames under ``_frame_budget()``.

        A single payload larger than the budget still ships alone — it is
        bounded by ``_return_value_limit`` (half the frame) plus
        ``_output_notify_limit`` per output stream (an eighth each), which
        leaves a quarter of the frame for the remaining fields.
        """
        frames: list[list[dict]] = []
        frame:  list[dict]       = []
        nbytes                   = 0
        budget = self._frame_budget()

        for payload in batch:
            size = _payload_size(payload)
            if frame and nbytes + size > budget:
                frames.append(frame)
                frame  = []
                nbytes = 0
            frame.append(payload)
            nbytes += size

        if frame:
            frames.append(frame)
        return frames

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
                d[key] = '\n'.join(
                    v.decode('utf-8', errors='replace')
                    if isinstance(v, bytes) else str(v) for v in val)

        # Ensure return_value is JSON-serializable (see _encode_return_value:
        # rich results keep full fidelity, they are not stringified).  A
        # JSON-safe result keeps its wire form but is size-checked all the
        # same — a 10 MiB plain list overruns the frame just as a pickle does.
        rv = d.get('return_value')
        if rv is not None:
            try:
                as_json = json.dumps(rv)
            except (TypeError, ValueError):
                self._encode_return_value(d, rv)
            else:
                if len(as_json) > self._return_value_limit:
                    self._oversized_return_value(d, len(as_json))

        return {k: _json_safe(v) for k, v in d.items()}

    def _encode_return_value(self, d: dict, rv) -> None:
        """Encode a non-JSON-safe ``return_value`` in place.

        ``bytes`` keep the established plain-base64 form; any other object
        (numpy array, dataclass, model weights, ...) is cloudpickled with the
        very same ``cloudpickle::<base64>`` convention that carries function
        arguments the other way — so a function task keeps full fidelity in
        both directions instead of coming back as its ``str()``.  The
        ``_return_value_encoding`` field names the encoding for the client
        (:meth:`RhapsodyClient._decode_return_value`).

        Two failure modes are reported rather than silently degraded: an
        unpicklable result falls back to ``str(rv)`` (all the wire can
        carry), and a result whose encoding exceeds this session's
        return-value limit is dropped in favour of an ``error`` — the
        oversized frame would otherwise be discarded by the transport.
        """
        if isinstance(rv, bytes):
            encoding = 'base64'
            encoded  = base64.b64encode(rv).decode('ascii')
        else:
            try:
                encoding = 'cloudpickle'
                encoded  = 'cloudpickle::' \
                         + base64.b64encode(_cp.dumps(rv)).decode('ascii')
            except Exception as e:
                log.warning("[%s] return_value is not picklable (%s) — "
                            "falling back to str()", self._sid, e)
                d['return_value'] = str(rv)
                return

        if len(encoded) > self._return_value_limit:
            self._oversized_return_value(d, len(encoded))
            return

        d['return_value']           = encoded
        d['_return_value_encoding'] = encoding

    def _oversized_return_value(self, d: dict, size: int) -> None:
        """Replace an over-budget ``return_value`` with a task error.

        Applies to both wire forms (encoded and plain JSON): a frame past
        the transport cap is dropped, so the client would wait forever —
        and truncating would hand back a corrupt result.
        """
        msg = (f"return_value too large: {size} wire bytes exceed the "
               f"{self._return_value_limit} byte limit — stage the result "
               f"to a file instead")
        log.error("[%s] %s", self._sid, msg)
        d['return_value'] = None
        d.pop('_return_value_encoding', None)
        if not d.get('error'):
            d['error'] = msg

    _NOTIFICATION_KEYS = {'uid', 'state', 'exit_code', 'stdout', 'stderr',
                          'return_value', '_return_value_encoding',
                          'error', 'exception', 'traceback'}

    # Every key a terminal notification may carry: the selected task fields
    # plus the flags ``_notification_payload`` adds (forwarded verbatim by
    # the task dispatcher).
    NOTIFICATION_FORWARD_KEYS = frozenset(
        _NOTIFICATION_KEYS | {'stdout_truncated', 'stderr_truncated'})

    def _notification_payload(self, t) -> dict:
        """Build a minimal notification dict for a completed task.

        Only essential fields are included to keep WebSocket/SSE
        payloads small.  ``stdout`` / ``stderr`` are kept to their last
        ``_output_notify_limit`` bytes each (see ``_fit_output``); a cut
        stream is flagged ``stdout_truncated`` / ``stderr_truncated``.  On
        a direct rhapsody session the full task dict is available via
        ``GET /task/{sid}/{uid}``.
        """
        full    = self._sanitize_task(t)
        payload = {k: v for k, v in full.items()
                   if k in self._NOTIFICATION_KEYS}
        for key, val in payload.items():
            if isinstance(val, str) and key not in ('stdout', 'stderr') \
                    and not val.isascii():
                # lone surrogates (surrogateescape-decoded text in any
                # field) make msgpack refuse the whole batch frame; the
                # output streams get the same in _fit_output, bounded
                payload[key] = val.encode('utf-8', 'replace').decode('utf-8')
        for key in ('stdout', 'stderr'):
            if isinstance(payload.get(key), str):
                payload[key], cut = self._fit_output(payload[key])
                if cut:
                    payload[f'{key}_truncated'] = True
        return payload

    def _fit_output(self, text: str) -> tuple[str, bool]:
        """Fit ``text`` into ``_output_notify_limit`` UTF-8 bytes.

        Returns ``(text, truncated)``.  The result is always valid UTF-8
        (lone surrogates become ``?``), else msgpack refuses the frame.  A
        longer stream keeps its tail (the end of a log is where errors are),
        cut on a character boundary behind a marker naming the dropped
        character count; below 256 bytes of limit there is no room for the
        marker and the bare tail is returned.  Encoding is bounded by the
        limit: the last ``limit`` characters always cover the kept bytes.
        """
        limit = self._output_notify_limit
        if limit <= 0:
            return '', bool(text)
        if text.isascii():
            if len(text) <= limit:
                return text, False
            raw = text[-limit:].encode('ascii')
        elif len(text) <= limit:
            raw = text.encode('utf-8', errors='replace')
            if len(raw) <= limit:
                return raw.decode('utf-8'), False
        else:
            raw = text[-limit:].encode('utf-8', errors='replace')

        keep  = limit - 128 if limit >= 256 else limit  # 128: marker room
        start = len(raw) - keep
        while start < len(raw) and (raw[start] & 0xC0) == 0x80:
            start += 1                            # skip continuation bytes
        tail  = raw[start:].decode('utf-8')
        if limit < 256:
            return tail, True
        return (f"[... {len(text) - len(tail)} characters truncated ...]\n"
                + tail), True

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

    @_tracked
    async def cancel_task(self, uid: str) -> dict:
        """
        Cancel a running task.
        """
        return await self._cancel_one(uid)

    async def _cancel_one(self, uid: str) -> dict:
        if self._rh_session is None:
            # cancel_all's gathered cancels can resume after a teardown that
            # followed a drain timeout
            raise HTTPException(status_code=410, detail="session is closing")
        task = self._tasks.get(uid)
        if not task:
            raise HTTPException(status_code=404,
                                detail=f"task {uid} not found")

        backend_name = task.get("backend")
        if backend_name and backend_name in self._rh_session.backends:
            backend = self._rh_session.backends[backend_name]
            await backend.cancel_task(uid)

        return {"uid": uid, "status": "canceled"}

    @_tracked
    async def cancel_all_tasks(self) -> dict:
        """
        Cancel all non-terminal tasks in this session.

        Best-effort: Dragon V3 marks tasks as CANCELED but cannot truly
        abort running work.  Per-task errors are swallowed.  Cancels are
        issued concurrently via ``asyncio.gather``.
        """
        uids = []
        for uid, task in list(self._tasks.items()):
            state = str(self._get_attr(task, 'state', '')).upper()
            if state not in TERMINAL_STATES:
                uids.append(uid)

        if not uids:
            return {"canceled": 0}

        async def _try_cancel(uid):
            try:
                await self._cancel_one(uid)   # one tracked op for the batch
                return True
            except Exception:
                return False

        results = await asyncio.gather(*[_try_cancel(u) for u in uids])
        return {"canceled": sum(1 for r in results if r)}

    # -- data backends -------------------------------------------------------

    @_tracked
    async def data_start(self, kind: str, name: str | None = None,
                         kwargs: dict | None = None) -> dict:
        """
        Start a rhapsody data backend (``redis``, ``dragon``, ...) inside this
        session, so the store runs next to the session's tasks.

        ``name`` defaults to ``<kind>_data`` and must not collide with any
        backend of the session, execution backends included.

        Returns the backend's name, kind and serialized endpoints.
        """
        get_data_backend = getattr(rh, 'get_data_backend', None)
        if get_data_backend is None:
            raise HTTPException(status_code=501,
                                detail="installed rhapsody has no data backends")

        name = name or f'{kind}_data'
        if not DATA_NAME_RE.fullmatch(name):
            raise HTTPException(status_code=400,
                                detail=f"invalid data backend name {name!r}")
        if name in self._data or name in self._rh_session.backends:
            raise HTTPException(status_code=409,
                                detail=f"backend {name!r} exists")

        try:
            backend = get_data_backend(kind, name=name, **(kwargs or {}))
        except (ValueError, ImportError, TypeError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        # reserve the name before the (slow) start so a concurrent request
        # with the same name gets 409, not a second store
        entry = _DataEntry(backend, kind, starting=True)
        self._data[name] = entry
        try:
            await backend.start()
        except BaseException:
            # includes cancellation by the endpoint's call timeout; a failing
            # cleanup must not mask the original error
            self._start_done(entry)
            await self._stop_quietly(name, entry)
            if self._data.get(name) is entry:
                # its cleanup failed: release the name (or it stays reserved
                # for good); the session close tries once more, unless it
                # already ran (after a drain timeout)
                del self._data[name]
                if not self._torn_down:
                    self._leftovers.append(entry)
                else:
                    log.warning("[%s] data backend %s may still run: its "
                                "cleanup failed after session close",
                                self._sid, name)
            raise
        self._start_done(entry)

        # data_shutdown (even a failed one) or session close may have run
        # while start awaited: then this store must not be registered
        if self._closing or entry.doomed or self._data.get(name) is not entry:
            await self._stop_quietly(name, entry)
            raise HTTPException(status_code=409,
                                detail=f"data backend {name!r} was shut "
                                        "down during start")

        try:
            self._rh_session.add_backend(backend)
        except Exception:
            # a store nobody can reach or close must not keep running;
            # rhapsody may have registered it before failing
            await self._stop_quietly(name, entry)
            raise
        return self._data_info(name)

    def _stop(self, name: str, entry: _DataEntry) -> asyncio.Future:
        """The one stop of a data backend; later callers share it.

        Marks the store doomed (an in-flight start gives up on it, also if
        this stop fails) and detaches it from the rhapsody session, so
        from here our own close stops it.  A store whose start is still
        running is not shut down here -- that shutdown would race the
        start; ``data_start`` runs it once start returns, and the returned
        future settles then.  The entry (and with it the name) goes away
        once the store is down; after a failure it stays for a retry or
        the session close.
        """
        entry.doomed = True
        if entry.stop is None:
            rhs = self._rh_session
            if rhs is not None and rhs.backends.get(name) is entry.backend:
                del rhs.backends[name]
            entry.stop = asyncio.get_running_loop().create_future()
            entry.stop.add_done_callback(
                lambda f: self._stopped(name, entry, f))
            if not entry.starting:
                self._shutdown_into(entry)
        return entry.stop

    def _start_done(self, entry: _DataEntry) -> None:
        """The start returned (or failed): run a stop requested meanwhile."""
        entry.starting = False
        if entry.stop is not None and not entry.stop.done():
            self._shutdown_into(entry)

    def _shutdown_into(self, entry: _DataEntry) -> None:
        """Shut the store down and settle ``entry.stop`` with the outcome."""
        stop = entry.stop
        bg   = self._bg

        def _settle(t):
            if stop.done():
                return
            if t.cancelled():
                stop.cancel()
            elif t.exception() is not None:
                stop.set_exception(t.exception())
            else:
                stop.set_result(None)
        task = asyncio.ensure_future(entry.backend.shutdown())
        bg.add(task)                        # the loop keeps tasks weakly
        task.add_done_callback(bg.discard)
        task.add_done_callback(_settle)

    def _stopped(self, name: str, entry: _DataEntry, fut) -> None:
        """Book-keeping for a finished shutdown, also when every caller was
        cancelled (the shutdown is shielded and runs on)."""
        if fut.cancelled() or fut.exception() is not None:
            log.warning("[%s] data backend %s shutdown failed: %s", self._sid,
                        name, 'cancelled' if fut.cancelled() else fut.exception())
            entry.stop = None               # a retry starts a new shutdown
            return
        if self._data.get(name) is entry:
            del self._data[name]

    async def _stop_quietly(self, name: str, entry: _DataEntry) -> None:
        """Shielded stop for cleanup paths: a failure is logged by
        ``_stopped``, never raised over the error that caused the cleanup."""
        stop = self._stop(name, entry)
        try:
            await asyncio.shield(stop)
        except asyncio.CancelledError:
            if not stop.cancelled():
                raise                       # we were cancelled, not the stop
        except Exception:
            pass

    def _data_info(self, name: str) -> dict:
        entry = self._data[name]
        state = entry.backend.state.name
        eps   = [e.serialize() for e in entry.backend.endpoints] \
                if state == 'READY' else []
        return {"name": name, "kind": entry.kind, "state": state,
                "endpoints": eps}

    async def data_endpoints(self) -> dict:
        """Report all data backends of this session with their endpoints."""
        self._check_initialized()
        return {"backends": {n: self._data_info(n) for n in list(self._data)}}

    @_tracked
    async def data_shutdown(self, name: str) -> dict:
        """Shut down one data backend of this session."""
        entry = self._data.get(name)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail=f"data backend {name} not found")
        # shared with any other stopper; a cancelled caller does not stop it
        await asyncio.shield(self._stop(name, entry))
        return {"name": name, "status": "shutdown"}

    async def close(self) -> dict:
        """
        Shutdown RHAPSODY session and clean up.

        Idempotent.  The teardown runs as one task that a cancelled caller
        does not interrupt, so it always stops every data backend and ends
        with the base class close.
        """
        if self._close_task is not None and self._close_task.done():
            return {}                       # torn down; errors reported once
        if self._close_task is None:
            self._closing    = True         # turn new requests away
            self._close_task = asyncio.ensure_future(self._teardown())
            # retrieved even if every caller was cancelled
            self._close_task.add_done_callback(
                lambda t: t.cancelled() or t.exception())
        await asyncio.shield(self._close_task)
        return {}

    async def _teardown(self) -> None:
        # let tracked routes in flight finish (bounded), so none of them
        # sees a half torn-down session
        if self._ops:
            try:
                await asyncio.wait_for(self._ops_idle.wait(),
                                       CLOSE_DRAIN_TIMEOUT)
            except asyncio.TimeoutError:
                log.warning("[%s] closing with %d request(s) still in "
                            "flight", self._sid, self._ops)

        data, self._data = self._data, {}
        leftovers, self._leftovers = self._leftovers, []
        self._torn_down = True
        rh_session, self._rh_session = self._rh_session, None
        # a successful rhapsody close stopped the backends it owned; the
        # rest is stopped below (all of them if that close failed)
        owned = set()
        try:
            if rh_session:
                # Telemetry is best effort: never let it block teardown.
                if self._telemetry is not None \
                        and log.isEnabledFor(logging.INFO):
                    try:
                        log.info(json.dumps(self._telemetry.summary(),
                                            indent=4))
                    except Exception as e:
                        log.warning("[%s] telemetry summary failed: %r",
                                    self._sid, e)
                self._telemetry = None
                await self._unwind_telemetry(rh_session)
                held = set(rh_session.backends)
                await rh_session.close()
                owned = held
        finally:
            self._tasks = {}
            starting = [(n, e) for n, e in data.items()
                        if n not in owned and e.starting]
            for name, entry in starting:
                log.warning("[%s] data backend %s still starting at close: "
                            "stopped when its start returns", self._sid, name)
                self._stop(name, entry)     # data_start stops it on return
            await asyncio.gather(
                *(self._stop_quietly(name, entry)
                  for name, entry in data.items()
                  if name not in owned and not entry.starting),
                *(self._stop_quietly(e.backend.name, e) for e in leftovers))
            await super().close()


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
        self._warned_no_sid = False

    def _on_task_done(self, endpoint, plugin, topic, data):
        """Persistent SSE callback: accumulate terminal task states.

        Handles both single ``task_status`` and bulk
        ``task_status_batch`` notifications.  Notifications of other
        sessions (a different ``sid``) are ignored: they are broadcast to
        every consumer of the plugin and their uids can collide with ours.
        A notification without ``sid`` (older endpoint) is accepted.
        """
        sid = data.get('sid')
        if sid is None and not self._warned_no_sid:
            self._warned_no_sid = True
            log.warning("[rhapsody] task notifications without a session id "
                        "(older endpoint, or the pool task dispatcher): results "
                        "of other sessions with the same task uids cannot be "
                        "told apart")

        if topic == 'task_status_batch':
            tasks = data.get('tasks', [])
        else:
            tasks = [data]

        with self._cond:
            if sid is not None and sid != self.sid:
                return
            changed = False
            for t in tasks:
                self._decode_return_value(t)

                uid   = t.get('uid')
                state = str(t.get('state', '')).upper()
                if uid and state in TERMINAL_STATES:
                    # stored without the envelope's sid: a completion looks
                    # the same whether it came alone or in a batch frame
                    self._completed[uid] = t if 'sid' not in t else \
                        {k: v for k, v in t.items() if k != 'sid'}
                    changed = True

            if changed:
                self._cond.notify_all()

    @staticmethod
    def _decode_return_value(t: dict) -> None:
        """Reverse :meth:`RhapsodySession._encode_return_value` in place.

        ``base64`` yields the original ``bytes``, ``cloudpickle`` the original
        Python object; the marker is dropped once decoded so the task dict
        looks exactly like a locally executed one.  A value the client cannot
        decode (e.g. the result's class is not importable here) is left on the
        wire form with a warning — better a visible blob than an exception
        that would take down the notification dispatch for every other task.
        """
        if not isinstance(t, dict):
            return

        enc = t.get('_return_value_encoding')
        raw = t.get('return_value')
        if enc not in ('base64', 'cloudpickle') or not isinstance(raw, str):
            return

        try:
            if enc == 'base64':
                t['return_value'] = base64.b64decode(raw)
            else:
                if raw.startswith('cloudpickle::'):
                    raw = raw[len('cloudpickle::'):]
                t['return_value'] = _cp.loads(base64.b64decode(raw))
        except Exception as e:
            log.warning("cannot decode %s return_value of %s: %s",
                        enc, t.get('uid'), e)
            return

        del t['_return_value_encoding']

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
        # so we never miss a fast init notification.  session_status is
        # broadcast to every client of the plugin, and our sid is only known
        # once the POST returns: record outcomes by sid, pick ours after.
        changed  = threading.Event()
        outcomes = {}      # sid (None: older endpoint) -> (status, error)

        if has_sse:
            self._bc.wait_for_listener(timeout=30)

            def _on_session_status(endpoint, plugin, topic, data):
                st = data.get('status')
                if st in ('ready', 'failed'):
                    outcomes[data.get('sid')] = (
                        st, data.get('error', 'unknown init error'))
                    changed.set()

            self.register_notification_callback(_on_session_status,
                                                topic="session_status")
        try:
            self._register_and_wait(backends, init_timeout, has_sse,
                                    outcomes, changed)
        finally:
            # also when the POST fails: never leave the callback behind
            if has_sse:
                self.unregister_notification_callback(_on_session_status,
                                                      topic="session_status")
        self._start_task_listener()

    def _register_and_wait(self, backends, init_timeout, has_sse,
                           outcomes, changed):
        """POST register_session, then wait until our session is ready."""
        payload = {}
        if backends:
            payload['backends'] = backends
        resp = self._http.post(self._url('register_session'), json=payload)
        self._raise(resp)

        data   = resp.json()
        status = data.get('status')

        # Switch to the new session and reset the session-wide completion
        # accumulator in one step, so a late notification of the old session
        # cannot pass the sid check and land after the reset.
        with self._cond:
            self._sid = data['sid']
            self._completed.clear()

        if status == 'ready':
            return  # fast path: init was synchronous
        if status == 'failed':
            # reconnect to a session whose init already failed: its
            # session_status event went out before we listened
            raise RuntimeError(
                f"Session init failed on endpoint: "
                f"{data.get('error', 'unknown init error')}")

        if not has_sse:
            self._poll_session_ready(init_timeout)
            return

        # Wait for async init to complete via SSE notification
        deadline = time.monotonic() + init_timeout
        mine     = None
        while mine is None:
            mine = outcomes.get(self._sid) or outcomes.get(None)
            remaining = deadline - time.monotonic()
            if mine is not None or remaining <= 0:
                break
            changed.wait(timeout=remaining)
            changed.clear()            # outcomes is re-checked next loop
        if mine is None:
            raise RuntimeError(
                f"Session init timed out after {init_timeout}s")
        if mine[0] == 'failed':
            raise RuntimeError(
                f"Session init failed on endpoint: {mine[1]}")

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
                status = resp.status_code
            except Exception:
                status = None                # transient: retry
            if status == 410:
                # closing or expired.  Other statuses end the poll as
                # before (404 included: it is not conclusive this early);
                # the next real call reports them
                raise RuntimeError(f"session {self.sid} is gone (closing "
                                   "or expired)")
            if status is not None and status != 409:
                return  # session is ready (or already errored)
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
        """Fallback wait via periodic polling (no SSE available).

        Decodes encoded return values just like the SSE path, so a caller
        sees the same objects no matter which transport served the wait.
        """

        url     = self._url(ROUTE_WAIT.format(sid=self.sid))
        payload: dict = {"uids": uids}
        if timeout is not None:
            payload["timeout"] = timeout

        deadline = (time.time() + timeout) if timeout else None

        while True:
            resp = self._http.post(url, json=payload)
            self._raise(resp, f"wait {len(uids)} task(s)")
            tasks = resp.json()

            for t in tasks:
                self._decode_return_value(t)

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

    def data_start(self, kind: str, name: str | None = None,
                   **kwargs) -> dict:
        """
        Start a data backend of type ``kind`` in the remote session.

        ``kwargs`` go to the remote data backend's constructor and must be
        JSON-serializable.  Returns ``{name, kind, state, endpoints}`` with
        the endpoints serialized as seen from the endpoint's side.
        """
        self._require_session()

        url  = self._url(ROUTE_DATA_START.format(sid=self.sid))
        resp = self._http.post(url, json={"kind": kind, "name": name,
                                          "kwargs": kwargs})
        self._raise(resp)
        return resp.json()

    def data_endpoints(self) -> dict:
        """Return ``{"backends": {name: {kind, state, endpoints}}}``."""
        self._require_session()

        url  = self._url(ROUTE_DATA_ENDPOINTS.format(sid=self.sid))
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()

    def data_shutdown(self, name: str) -> dict:
        """Shut down the remote data backend ``name``."""
        self._require_session()

        url  = self._url(ROUTE_DATA_SHUTDOWN.format(sid=self.sid, name=name))
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
    - POST  /rhapsody/data_start/{sid}       – start a data backend
    - GET   /rhapsody/data_endpoints/{sid}   – list data backends
    - POST  /rhapsody/data_shutdown/{sid}/{name} – stop a data backend

    Notification topics: ``session_status``, ``task_status``,
    ``task_status_batch``.
    '''

    plugin_name = "rhapsody"
    default_roles = ('compute', 'standalone')
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

        # Endpoint-wide frame budgets — likewise resolved once, from the
        # hosting runtime's *effective* frame cap rather than the protocol
        # default, so a runtime tuned below FRAME_CAP shrinks both budgets
        # instead of letting the transport drop the frames they permit.
        self._frame_cap           = _resolve_frame_cap(app)
        self._notify_batch_bytes  = self._frame_cap // BUDGET_RATIO
        self._return_value_limit  = self._frame_cap // BUDGET_RATIO
        self._output_notify_limit = self._frame_cap // OUTPUT_RATIO

        self.add_route_post(ROUTE_SUBMIT,     self.submit_tasks)
        self.add_route_post(ROUTE_WAIT,       self.wait_tasks)
        self.add_route_get (ROUTE_LIST_TASKS, self.list_tasks)
        self.add_route_get (ROUTE_TASK,       self.get_task)
        self.add_route_post(ROUTE_CANCEL,     self.cancel_task)
        self.add_route_post(ROUTE_CANCEL_ALL, self.cancel_all_tasks)

        self.add_route_post(ROUTE_DATA_START,     self.data_start)
        self.add_route_get (ROUTE_DATA_ENDPOINTS, self.data_endpoints)
        self.add_route_post(ROUTE_DATA_SHUTDOWN,  self.data_shutdown)

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
        """Build a session and inject the endpoint's shared profiler,
        notification window and frame budgets.

        Extends the base factory (which injects ``_plugin``) so every
        rhapsody session profiles against the one endpoint-runtime profiler
        instead of digging it out of app state on first use, coalesces
        notifications over the endpoint-configured window, and sizes its
        return values / notification frames against the host runtime's
        effective frame cap.
        """
        session = super()._create_session(sid, **kwargs)
        svc = getattr(self._app.state, 'endpoint_service', None)
        session._prof = getattr(svc, '_prof', None) or \
                        rprof.Profiler('rhapsody', ns='radical.orbit')
        session._notify_window       = self._notify_window
        session._notify_batch_bytes  = self._notify_batch_bytes
        session._return_value_limit  = self._return_value_limit
        session._output_notify_limit = self._output_notify_limit
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

    async def data_start(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        data = await request.json()
        if not isinstance(data, dict) or not isinstance(data.get('kind'), str):
            raise HTTPException(status_code=400,
                                detail="body needs a string 'kind'")
        if not isinstance(data.get('name'), (str, type(None))):
            raise HTTPException(status_code=400,
                                detail="'name' must be a string")
        kwargs = data.get('kwargs') or {}
        if not isinstance(kwargs, dict):
            raise HTTPException(status_code=400,
                                detail="'kwargs' must be an object")
        return await self._forward(sid, RhapsodySession.data_start,
                                   kind=data['kind'], name=data.get('name'),
                                   kwargs=kwargs)

    async def data_endpoints(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, RhapsodySession.data_endpoints)

    async def data_shutdown(self, request: Request) -> dict:
        sid  = request.path_params['sid']
        name = request.path_params['name']
        return await self._forward(sid, RhapsodySession.data_shutdown,
                                   name=name)
