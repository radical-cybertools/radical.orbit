'''
PsiJ plugin for ORBIT — HPC job submission.

Three-class pattern
-------------------
PSIJSession   Endpoint-side session: holds one PsiJ ``Executor`` per submit call,
              manages job state via callbacks and background polling, streams
              stdout/stderr incrementally.

PSIJClient    Application-side thin HTTP wrapper: delegates to the endpoint service
              over the broker (``submit_job``, ``get_job_status``, ``list_jobs``,
              ``cancel_job``, ``submit_tunneled``, ``tunnel_status``).

PluginPSIJ    Registers the plugin with the endpoint, adds URL routes, and wires
              requests to the correct PSIJSession via ``_forward()``.
'''

import asyncio
import logging
import os
import pathlib
import shutil
import socket
import time

from datetime import timedelta
from typing import Any, Dict
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request

import psij

from . import batch_system
from . import tunnel
from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient, _run_sync

log = logging.getLogger("radical.orbit")

# ---------------------------------------------------------------------------
# Route templates — single-sourced so the ``add_route_*`` registration and the
# client-helper (and broker-hosted dispatcher) URL formatting cannot drift.
# Each template doubles as the ``add_route_*`` path (``{name}`` = path param)
# and as a ``.format(...)`` string for the helpers.  Every psij route is
# formatted by a ``PSIJClient`` helper, so all of them are lifted here.
# ---------------------------------------------------------------------------
ROUTE_SUBMIT          = 'submit/{sid}'
ROUTE_SUBMIT_TUNNELED = 'submit_tunneled/{sid}'
ROUTE_TUNNEL_STATUS   = 'tunnel_status/{endpoint_name}'
ROUTE_STATUS          = 'status/{sid}/{job_id}'
ROUTE_LIST_JOBS       = 'list_jobs/{sid}'
ROUTE_CANCEL          = 'cancel/{sid}/{job_id}'

# Default poll interval for job status updates (in seconds)
PSIJ_POLL_INTERVAL = 5.0

# consecutive STATE_UNKNOWN polls tolerated after the job has been seen
# RUNNING — beyond this we conclude the job left the queue and bail
UNKNOWN_TOLERANCE = 3

# Persistent directory for job stdout/stderr capture
_OUTPUT_BASE = pathlib.Path.home() / '.radical' / 'orbit' / 'psij' / 'output'

# Maximum age (days) for stale output directories cleaned up on session creation
_OUTPUT_MAX_AGE_DAYS = 7

# Diagnostic: when set (truthy env var), pass ``keep_files=True`` into the
# batch-scheduler executor config so PsiJ leaves its generated submit
# scripts under ``~/.psij/work/<scheduler>/`` for inspection.  Off by
# default to keep the workdir tidy.
_KEEP_PSIJ_FILES = os.environ.get('RADICAL_ORBIT_PSIJ_KEEP_FILES', '').lower() \
                   in ('1', 'true', 'yes', 'on')


# Terminal states that don't need further polling
TERMINAL_STATES = {'COMPLETED', 'FAILED', 'CANCELED'}


def _normalize_state(state) -> str:
    """Normalize a PsiJ JobState to a plain string (strip 'JobState.' prefix)."""
    s = str(state)
    return s[9:] if s.startswith('JobState.') else s


def _read_output_file(job, attr: str, offset: int = 0) -> str:
    """Read stdout or stderr from a job's spec path attribute.

    Args:
        job:    PsiJ job object.
        attr:   Attribute name on job.spec ('stdout_path' or 'stderr_path').
        offset: Byte offset to start reading from (0 = full file).

    Returns:
        Content read from the file starting at offset.
    """
    try:
        path = getattr(job.spec, attr, None)
        if path and os.path.exists(str(path)):
            with open(str(path), 'r') as f:
                if offset > 0:
                    f.seek(offset)
                return f.read()
    except Exception as e:
        log.debug("Failed to read %s for job: %s", attr, e)
    return ""


def _output_file_size(job, attr: str) -> int:
    """Return the byte size of a job's stdout/stderr file, or 0."""
    try:
        path = getattr(job.spec, attr, None)
        if path and os.path.exists(str(path)):
            return os.path.getsize(str(path))
    except Exception:
        pass
    return 0


class PSIJSession(PluginSession):
    '''
    Session-specific PSIJ state.
    '''

    def __init__(self, sid: str, **kwargs: Any):
        super().__init__(sid)
        self._jobs: Dict[str, Any] = {}       # job_id -> psij.Job
        self._job_meta: Dict[str, dict] = {}  # job_id -> submission metadata
        self._job_states: Dict[str, str] = {}  # track last known state per job
        self._cancelled_jobs: set = set()      # job_ids the user asked to cancel
        self._poll_task = None

        # Persistent output directory for this session's job stdout/stderr
        self._output_dir = _OUTPUT_BASE / sid
        self._cleanup_stale_output()
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _effective_state(self, job_id: str, raw_state: str) -> str:
        """Map a psij state to what the caller should see.

        psij's PBS backend only flags Exit_status == 265 as CANCELED
        (pbs_base.py:135-139); sites like Aurora return a different code,
        so a cancelled job surfaces as COMPLETED/FAILED.  We remember
        which jobs the user asked to cancel and report those as CANCELED
        regardless of what the backend says.
        """
        if job_id in self._cancelled_jobs and raw_state in ('COMPLETED', 'FAILED'):
            return 'CANCELED'
        return raw_state

    def _cleanup_stale_output(self) -> None:
        """Remove output directories older than _OUTPUT_MAX_AGE_DAYS."""
        if not _OUTPUT_BASE.exists():
            return
        cutoff = time.time() - _OUTPUT_MAX_AGE_DAYS * 86400
        for entry in _OUTPUT_BASE.iterdir():
            if not entry.is_dir() or entry == self._output_dir:
                continue
            try:
                if entry.stat().st_mtime < cutoff:
                    shutil.rmtree(entry)
                    log.info("Cleaned up stale output dir: %s", entry)
            except Exception as e:
                log.debug("Failed to clean up %s: %s", entry, e)

    def _notify_state(self, job_id: str, status=None) -> None:
        """Normalize, dedupe and emit a ``job_status`` notification.

        The single source of job-status notifications: both the submit-time
        psij callback and the background poll loop call this, so they share
        one dedupe store (``self._job_states``) and one payload shape — a
        transition can therefore never double-fire.  *status* defaults to the
        job's current ``status`` (the poll path); the callback passes the
        transition status it was handed.  A no-op when the state has not
        changed since the last emission.
        """
        job = self._jobs.get(job_id)
        if job is None:
            return
        if status is None:
            status = job.status
        state = self._effective_state(job_id, _normalize_state(status.state))
        if state == self._job_states.get(job_id):
            return
        self._job_states[job_id] = state
        is_terminal = state in TERMINAL_STATES
        self.notify("job_status", {
            "job_id":    job_id,
            "state":     state,
            "exit_code": status.exit_code if is_terminal else None,
            "stdout":    _read_output_file(job, 'stdout_path') if is_terminal else "",
            "stderr":    _read_output_file(job, 'stderr_path') if is_terminal else "",
        })

    async def submit_job(self, job_spec_dict: Dict[str, Any], executor_name: str = 'local') -> Dict[str, Any]:
        '''
        Submit a job via PSIJ.
        '''
        try:

            spec = psij.JobSpec()
            executable = job_spec_dict.get('executable')
            arguments  = job_spec_dict.get('arguments')

            spec.executable = executable
            if arguments:
                spec.arguments = arguments
            if 'directory' in job_spec_dict:
                spec.directory = job_spec_dict['directory']
            if 'environment' in job_spec_dict:
                spec.environment = job_spec_dict['environment']
            if 'attributes' in job_spec_dict:
                attribs = job_spec_dict['attributes']
                spec.attributes = psij.JobAttributes()
                duration = attribs.get("duration")
                if duration:
                    spec.attributes.duration = timedelta(seconds=int(duration))
                spec.attributes.queue_name = attribs.get("queue_name")
                spec.attributes.account = attribs.get("account")
                spec.attributes.reservation_id = attribs.get("reservation_id")

            # Resource spec: pass any ``ResourceSpecV1`` field through
            # verbatim (``node_count``, ``exclusive_node_use``, etc).
            # An unknown key here raises TypeError -- caller bug, not
            # something to silently swallow.
            res = job_spec_dict.get('resources') or {}
            if res:
                spec.resources = psij.ResourceSpecV1(**res)

            # Merge site defaults for PSIJ custom_attributes with the
            # caller's (caller wins on conflict).  Defaults come from the
            # detected batch_system backend — e.g. Aurora's PBS requires
            # filesystems= on every submission, which the UI / Python API
            # users are not expected to know.  Only applied when the
            # chosen executor matches the detected backend.
            backend = batch_system.detect_batch_system()
            defaults = (backend.default_custom_attributes()
                        if backend.psij_executor == executor_name else {})
            caller_ca = dict(job_spec_dict.get('custom_attributes') or {})
            merged_ca = {**defaults, **caller_ca}
            if merged_ca:
                if spec.attributes is None:
                    spec.attributes = psij.JobAttributes()
                spec.attributes.custom_attributes = merged_ca
                if defaults:
                    added = {k: v for k, v in defaults.items()
                             if k not in caller_ca}
                    if added:
                        log.info("[psij] backend=%s injected defaults: %s",
                                 backend.name, added)

            job = psij.Job(spec)

            out_path = str(self._output_dir / f"{job.id}.out")
            err_path = str(self._output_dir / f"{job.id}.err")
            spec.stdout_path = out_path
            spec.stderr_path = err_path

            # ``keep_files=True`` only meaningful for batch-scheduler
            # executors (slurm/pbs/lsf/cobalt/...).  ``local`` ignores it.
            ex_config = None
            if _KEEP_PSIJ_FILES and executor_name in ('slurm', 'pbs', 'lsf',
                                                       'cobalt', 'flux'):
                from psij.executors.batch.batch_scheduler_executor \
                    import BatchSchedulerExecutorConfig
                ex_config = BatchSchedulerExecutorConfig(keep_files=True)
                log.info("[psij] RADICAL_ORBIT_PSIJ_KEEP_FILES set: "
                         "executor=%s keep_files=True", executor_name)

            if ex_config is not None:
                ex = psij.JobExecutor.get_instance(executor_name,
                                                    config=ex_config)
            else:
                ex = psij.JobExecutor.get_instance(executor_name)

            # Set poll interval for status updates
            if hasattr(ex, 'poll_interval'):
                ex.poll_interval = PSIJ_POLL_INTERVAL

            self._jobs[job.id] = job

            # Store submission metadata for later retrieval
            attribs = job_spec_dict.get('attributes', {})
            self._job_meta[job.id] = {
                'executable':  executable,
                'arguments':   arguments or [],
                'executor':    executor_name,
                'directory':   job_spec_dict.get('directory'),
                'queue_name':  attribs.get('queue_name'),
                'account':     attribs.get('account'),
                'node_count':  attribs.get('node_count'),
                'duration':    attribs.get('duration'),
            }

            # Register status callback BEFORE submit so no transitions are
            # missed; it shares the dedupe/payload path with the poll loop.
            job_id = job.id

            def _on_status(j, status):
                self._notify_state(job_id, status)

            job.set_job_status_callback(_on_status)

            ex.submit(job)

            # Start background polling for job status updates
            self._start_polling()

            log.info("Submitted job %s to %s", job.id, executor_name)
            return {"job_id": job.id, "native_id": job.native_id}

        except Exception as e:
            log.exception("Job submission failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def get_job_status(self, job_id: str,
                            stdout_offset: int = 0,
                            stderr_offset: int = 0) -> Dict[str, Any]:
        '''
        Get job status with metadata and optional stdout/stderr offset.
        '''
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        status    = job.status
        state_str = _normalize_state(status.state)
        state_str = self._effective_state(job_id, state_str)

        stdout_content = _read_output_file(job, 'stdout_path', stdout_offset)
        stderr_content = _read_output_file(job, 'stderr_path', stderr_offset)

        meta = self._job_meta.get(job_id, {})

        return {
            "job_id":        job_id,
            "native_id":     job.native_id,
            "state":         state_str,
            "message":       status.message,
            "exit_code":     status.exit_code,
            "time":          status.time,
            "executable":    meta.get('executable'),
            "arguments":     meta.get('arguments', []),
            "executor":      meta.get('executor'),
            "directory":     meta.get('directory'),
            "queue_name":    meta.get('queue_name'),
            "account":       meta.get('account'),
            "node_count":    meta.get('node_count'),
            "duration":      meta.get('duration'),
            "stdout":        stdout_content,
            "stderr":        stderr_content,
            "stdout_offset": _output_file_size(job, 'stdout_path'),
            "stderr_offset": _output_file_size(job, 'stderr_path'),
        }

    async def list_jobs(self) -> Dict[str, Any]:
        '''
        List all jobs in this session with current state and metadata.
        '''
        jobs = []
        for job_id, job in self._jobs.items():
            state_str = _normalize_state(job.status.state)
            state_str = self._effective_state(job_id, state_str)
            meta      = self._job_meta.get(job_id, {})
            jobs.append({
                "job_id":     job_id,
                "native_id":  job.native_id,
                "state":      state_str,
                "exit_code":  job.status.exit_code,
                "executable": meta.get('executable'),
                "arguments":  meta.get('arguments', []),
                "executor":   meta.get('executor'),
                "queue_name": meta.get('queue_name'),
                "account":    meta.get('account'),
                "node_count": meta.get('node_count'),
            })
        return {"jobs": jobs}

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        '''
        Cancel a job.
        '''
        job = self._jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        # Record intent *before* calling cancel() so any status update
        # that races with qdel gets mapped through _effective_state.
        self._cancelled_jobs.add(job_id)
        try:
            job.cancel()
            return {"job_id": job_id, "status": "canceled"}
        except Exception as e:
            log.exception("Job cancellation failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def close(self) -> dict:
        '''
        Close the session and stop polling.
        '''
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        # Clean up this session's output directory
        if self._output_dir.exists():
            try:
                shutil.rmtree(self._output_dir)
            except Exception as e:
                log.debug("Failed to remove output dir %s: %s",
                          self._output_dir, e)

        return await super().close()

    def _start_polling(self):
        '''
        Start the background polling task if not already running.
        '''
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_jobs())

    async def _poll_jobs(self):
        '''
        Background task that polls job status and sends notifications.
        '''
        first = True
        while True:
            try:
                if first:
                    # Short delay on first poll to catch fast state transitions
                    await asyncio.sleep(0.5)
                    first = False
                else:
                    await asyncio.sleep(PSIJ_POLL_INTERVAL)

                # Check all jobs; _notify_state normalizes/dedupes/notifies.
                for job_id in list(self._jobs):
                    try:
                        self._notify_state(job_id)
                    except Exception as e:
                        log.debug("Error polling job %s: %s", job_id, e)

                # Check if all jobs are terminal - if so, stop polling
                if all(self._job_states.get(jid) in TERMINAL_STATES
                       for jid in self._jobs):
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("Polling error: %s", e)


class PSIJClient(PluginClient):
    """
    Client-side interface for the PSIJ plugin.
    """

    def submit_job(self, job_spec: Dict[str, Any], executor: str = 'local') -> Dict[str, Any]:
        """
        Submit a job.

        Args:
            job_spec (dict): The job specification.
            executor (str): The executor to use.

        Returns:
             dict: Job submission result (job_id, native_id).
        """
        self._require_session()

        url = self._url(ROUTE_SUBMIT.format(sid=self.sid))
        payload = {"job_spec": job_spec, "executor": executor}

        resp = self._http.post(url, json=payload)
        self._raise(resp, f"psij submit {job_spec.get('executable','?')!r} on {executor!r}")
        return resp.json()

    def get_job_status(self, job_id: str,
                       stdout_offset: int = 0,
                       stderr_offset: int = 0) -> Dict[str, Any]:
        """
        Get the status of a job.

        Args:
            job_id:        The job ID to query.
            stdout_offset: Byte offset for stdout (0 = full).
            stderr_offset: Byte offset for stderr (0 = full).

        Returns:
            Job status info including metadata and stdout/stderr.
        """
        return _run_sync(self.aget_job_status(
            job_id, stdout_offset, stderr_offset))

    async def aget_job_status(self, job_id: str,
                              stdout_offset: int = 0,
                              stderr_offset: int = 0) -> Dict[str, Any]:
        """Async core for :meth:`get_job_status` (broker-hosted dispatcher)."""
        self._require_session()

        url    = self._url(ROUTE_STATUS.format(sid=self.sid, job_id=job_id))
        params = {}
        if stdout_offset:
            params['stdout_offset'] = str(stdout_offset)
        if stderr_offset:
            params['stderr_offset'] = str(stderr_offset)

        resp = await self._arequest("GET", url, params=params)
        self._raise(resp, f"job status {job_id!r}")
        return resp.json()

    def list_jobs(self) -> Dict[str, Any]:
        """
        List all jobs in this session.

        Returns:
            dict with 'jobs' list.
        """
        self._require_session()

        resp = self._http.get(self._url(ROUTE_LIST_JOBS.format(sid=self.sid)))
        self._raise(resp)
        return resp.json()

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a job.

        Args:
            job_id: The job ID to cancel.

        Returns:
            Cancellation result.
        """
        return _run_sync(self.acancel_job(job_id))

    async def acancel_job(self, job_id: str) -> Dict[str, Any]:
        """Async core for :meth:`cancel_job` (broker-hosted dispatcher)."""
        self._require_session()

        url = self._url(ROUTE_CANCEL.format(sid=self.sid, job_id=job_id))

        resp = await self._arequest("POST", url)
        self._raise(resp, f"cancel job {job_id!r}")
        return resp.json()

    def submit_tunneled(self, job_spec: Dict[str, Any],
                        executor: str = 'local',
                        tunnel: str = 'none') -> Dict[str, Any]:
        """Submit a job that launches a child Endpoint service on a compute node.

        The ``job_spec.arguments`` list *must* contain ``-n <endpoint_name>`` or
        ``--name <endpoint_name>`` so the child endpoint can register under the
        correct name.

        Args:
            job_spec: PsiJ job specification dict.  ``arguments`` must
                      include ``-n <endpoint_name>``.
            executor: PsiJ executor name (default: ``"local"``).
            tunnel:   SSH tunnel mode for the child's broker connection.
                      One of:

                      * ``'none'``    — child connects directly to the
                                        broker.  No SSH spawned anywhere.
                      * ``'forward'`` — child opens its own outbound
                                        ``ssh -L`` to the login host
                                        (compute → login).  Suitable
                                        where outbound SSH from compute
                                        is permitted and login → compute
                                        is blocked (Aurora, Perlmutter).
                      * ``'reverse'`` — login-side parent opens
                                        ``ssh -R`` to the compute host
                                        (login → compute).  Suitable
                                        where compute → login SSH is
                                        blocked but login → compute
                                        works (Odo).

                      Hard-rejects any other value (including ``True`` /
                      ``False``) — there is no boolean back-compat.

        Returns:
            dict with ``job_id``, ``native_id``, and ``endpoint_name``.

        Raises:
            ValueError:   If *tunnel* is not one of the three string values.
            RuntimeError: If the server returns an error response.
        """
        return _run_sync(self.asubmit_tunneled(job_spec, executor, tunnel))

    async def asubmit_tunneled(self, job_spec: Dict[str, Any],
                               executor: str = 'local',
                               tunnel: str = 'none') -> Dict[str, Any]:
        """Async core for :meth:`submit_tunneled` (broker-hosted dispatcher).

        Same validation, path, payload and error mapping as the sync wrapper;
        the dispatcher awaits this directly on the broker host loop.
        """
        if tunnel not in ('none', 'forward', 'reverse'):
            raise ValueError(
                f"tunnel must be one of 'none' / 'forward' / 'reverse'; "
                f"got {tunnel!r}")

        self._require_session()

        url     = self._url(ROUTE_SUBMIT_TUNNELED.format(sid=self.sid))
        payload = {"job_spec": job_spec, "executor": executor, "tunnel": tunnel}

        resp = await self._arequest("POST", url, json=payload)
        self._raise(resp, f"psij submit_tunneled on {executor!r}")
        return resp.json()

    def tunnel_status(self, endpoint_name: str) -> Dict[str, Any]:
        """Return the current tunnel status for a named endpoint.

        This endpoint is session-less (no session required).

        Args:
            endpoint_name: The logical name of the child endpoint service.

        Returns:
            dict with fields:

            - ``endpoint_name`` — echoed back.
            - ``status`` — one of ``"pending"``, ``"active"``, ``"failed"``,
              ``"done"``, or ``"no_tunnel"``.
            - ``port`` — assigned tunnel port (int) once active, else null.
            - ``pid`` — SSH process PID, once spawned, else null.
        """
        resp = self._http.get(self._url(
            ROUTE_TUNNEL_STATUS.format(endpoint_name=endpoint_name)))
        self._raise(resp, f"tunnel_status {endpoint_name!r}")
        return resp.json()


class PluginPSIJ(Plugin):
    '''
    PSIJ plugin for ORBIT.

    This plugin provides an interface to submit and manage jobs via the
    `psij-python` library.
    '''

    plugin_name = "psij"
    session_class = PSIJSession
    client_class = PSIJClient
    version = '0.0.1'

    ui_config = {
        "icon": "🚀",
        "title": "PsiJ Jobs",
        "description": "Submit and monitor HPC batch jobs via PsiJ.",
        "forms": [{
            "id": "submit",
            "title": "📝 Submit Job",
            "layout": "grid2",
            "fields": [
                {"name": "exec", "type": "text", "label": "Executable",
                 "default": "radical-orbit-endpoint-wrapper.sh", "css_class": "p-exec",
                 "column": 0},
                {"name": "args", "type": "text", "label": "Arguments (space-separated)",
                 "placeholder": "auto-filled with --url and --name",
                 "css_class": "p-args", "column": 0},
                {"name": "executor", "type": "select", "label": "Executor",
                 "options": ["local", "slurm", "pbs", "lsf"],
                 "css_class": "p-executor", "column": 0},
                {"name": "queue", "type": "text", "label": "Queue / Partition",
                 "placeholder": "optional", "required": False,
                 "css_class": "p-queue", "column": 1},
                {"name": "account", "type": "text", "label": "Account / Project",
                 "placeholder": "optional", "required": False,
                 "css_class": "p-account", "column": 1},
                {"name": "duration", "type": "text", "label": "Duration (seconds)",
                 "placeholder": "e.g. 600", "required": False,
                 "css_class": "p-duration", "column": 1},
                {"name": "node_count", "type": "number", "label": "Number of Nodes",
                 "placeholder": "e.g. 1", "required": False,
                 "css_class": "p-node-count", "column": 1},
                {"name": "custom", "type": "custom_attributes", "label": "🔧 Custom Attributes",
                 "required": False, "css_class": "p-custom-attr", "column": 1},
            ],
            "submit": {"label": "🚀 Submit Job", "style": "success"}
        }],
        "monitors": [{
            "id": "jobs",
            "title": "📊 Job Monitor",
            "type": "task_list",
            "css_class": "psij-output",
            "empty_text": "No jobs submitted yet."
        }],
        "notifications": {
            "topic": "job_status",
            "id_field": "job_id",
            "state_field": "state"
        }
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """PsiJ loads on endpoint nodes (login or compute) — not on the broker."""
        return not getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI, instance_name: str = "psij"):
        super().__init__(app, instance_name)

        # watcher tasks keyed by endpoint_name (plugin-level, survive session cleanup)
        self._watchers: dict = {}

        # Reverse-tunnel SSH processes keyed by endpoint_name (parent side
        # only — forward-mode tunnels live in the child process and are
        # invisible from here).
        self._tunnel_procs: dict = {}

        # job_id -> error message for jobs we cancelled because their
        # tunnel setup failed.  Read by ``get_job_status`` to override
        # the underlying CANCELLED state to FAILED with context.  An
        # entry is overwritten on next cancel for the same job_id; we
        # never pop on read (repeated reads return a stable result).
        self._failure_reasons: dict = {}

        # Ensure relay directory exists at startup
        tunnel.relay_dir()

        self._app.router.on_shutdown.append(self._cleanup_watchers)

        self.add_route_post(ROUTE_SUBMIT,          self.submit_job)
        self.add_route_post(ROUTE_SUBMIT_TUNNELED, self.submit_tunneled)
        self.add_route_get (ROUTE_TUNNEL_STATUS,   self.tunnel_status)
        self.add_route_get (ROUTE_STATUS,          self.get_job_status)
        self.add_route_get (ROUTE_LIST_JOBS,       self.list_jobs)
        self.add_route_post(ROUTE_CANCEL,          self.cancel_job)

    async def submit_job(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')

        return await self._forward(sid, PSIJSession.submit_job,
                                 job_spec_dict=job_spec,
                                 executor_name=executor)

    async def get_job_status(self, request: Request) -> dict:
        sid    = request.path_params['sid']
        job_id = request.path_params['job_id']
        so     = int(request.query_params.get('stdout_offset', '0'))
        se     = int(request.query_params.get('stderr_offset', '0'))
        status = await self._forward(sid, PSIJSession.get_job_status,
                                     job_id=job_id,
                                     stdout_offset=so,
                                     stderr_offset=se)
        # If we cancelled this job because its tunnel setup failed,
        # override the underlying CANCELLED state with FAILED + the
        # actual reason.  Operator-initiated cancels (no entry in
        # ``_failure_reasons``) keep their natural CANCELLED state.
        err = self._failure_reasons.get(job_id)
        if err:
            status['state'] = 'FAILED'
            status['error'] = err
        return status

    async def list_jobs(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, PSIJSession.list_jobs)

    async def cancel_job(self, request: Request) -> dict:
        sid = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, PSIJSession.cancel_job, job_id=job_id)

    # ─────────────────────────────────────────────────────────────────────────
    #  Endpoint-job submission with optional reverse SSH tunnel
    # ─────────────────────────────────────────────────────────────────────────

    async def submit_tunneled(self, request: Request) -> dict:
        """Submit a job that starts a new Endpoint service on a compute node.

        The job *must* pass ``-n``/``--name <endpoint_name>`` in its arguments so
        the child endpoint service can register under the correct name.

        Tunnel direction is selected by the ``tunnel`` field:

        * ``'none'``    — no SSH tunnel; child connects directly to the broker.
        * ``'forward'`` — child opens its own outbound ``ssh -L`` back to
                          this login node (compute → login).  We inject
                          ``--tunnel forward`` and ``--tunnel-via <login>``
                          into the child's argv.  The child writes the
                          rendezvous file itself; the parent watcher
                          only observes job state.
        * ``'reverse'`` — *parent* (this plugin) opens ``ssh -R`` to the
                          compute node once the job reaches RUNNING and
                          writes the rendezvous file with the remote
                          port allocated by the compute-side sshd.  We
                          inject only ``--tunnel reverse`` so the child
                          waits for the rendezvous file.

        Request body JSON fields:

        - ``job_spec``  (dict)  — PsiJ job specification.
        - ``executor``  (str)   — PsiJ executor name (default: ``"local"``).
        - ``tunnel``    (str)   — One of ``'none'``, ``'forward'``, ``'reverse'``
                                  (default: ``'none'``).  Boolean values
                                  are *not* accepted.

        Returns:
            JSON with ``job_id``, ``native_id``, and ``endpoint_name``.

        Raises:
            400 if ``tunnel`` is not one of the three string values.
            422 if ``-n``/``--name`` is missing from ``job_spec.arguments``.
            409 if a tunnel watcher for the same endpoint name is already active.
        """
        sid  = request.path_params['sid']
        data = await request.json()

        job_spec = data.get('job_spec', {})
        executor = data.get('executor', 'local')
        mode     = data.get('tunnel', 'none')   # not ``tunnel`` — that's the module

        if mode not in ('none', 'forward', 'reverse'):
            raise HTTPException(
                status_code=400,
                detail=f"tunnel must be one of 'none' / 'forward' / 'reverse'; "
                       f"got {mode!r}")

        # --- resolve endpoint name from arguments ---
        args = list(job_spec.get('arguments') or [])
        endpoint_name = None
        for i, a in enumerate(args[:-1]):
            if a in ('-n', '--name'):
                endpoint_name = args[i + 1]
                break

        if not endpoint_name:
            raise HTTPException(
                status_code=422,
                detail="submit_tunneled requires -n/--name <endpoint_name> in job_spec.arguments")

        # --- guard against duplicate watchers ---
        existing = self._watchers.get(endpoint_name)
        if existing and not existing.done():
            raise HTTPException(
                status_code=409,
                detail=f"Tunnel watcher already active for endpoint '{endpoint_name}'")

        # --- clear stale rendezvous files + inject child-side flags ---
        if mode != 'none':
            tunnel.rendezvous_clear(endpoint_name)  # drop any prior-run files

            if '--tunnel' not in args:
                args.extend(['--tunnel', mode])
            if mode == 'forward' and '--tunnel-via' not in args:
                # Forward mode: child needs to know which login host to ssh to.
                args.extend(['--tunnel-via', socket.gethostname()])

            job_spec = dict(job_spec)
            job_spec['arguments'] = args

        resp = await self._forward(sid, PSIJSession.submit_job,
                                   job_spec_dict=job_spec,
                                   executor_name=executor)

        if mode != 'none':
            native_id = resp.get('native_id')
            job_id    = resp.get('job_id')
            log.info("[psij] submit_tunneled mode=%s: endpoint=%s job_id=%s "
                     "native_id=%s -- watcher started",
                     mode, endpoint_name, job_id, native_id)
            if mode == 'forward':
                watch = self._watch_forward(endpoint_name, native_id)
            else:
                watch = self._watch_reverse(sid, endpoint_name,
                                            native_id, job_id)
            self._watchers[endpoint_name] = asyncio.create_task(watch)

        # Augment response with endpoint_name for caller convenience
        resp['endpoint_name'] = endpoint_name
        return resp

    async def tunnel_status(self, request: Request) -> dict:
        """Return the current tunnel status for a named endpoint.

        Path param: ``endpoint_name``

        Returns a JSON object with fields:

        - ``endpoint_name``  — echoed back.
        - ``status``     — one of ``"pending"``, ``"active"``, ``"failed"``,
                           ``"done"``, or ``"no_tunnel"``.
        - ``port``       — allocated tunnel port (int) once the child endpoint
                           has published it, else null.
        - ``pid``        — SSH process PID on the compute node (read from
                           the pid rendezvous file) once active, else null.
        """
        endpoint_name = request.path_params['endpoint_name']
        port = tunnel.rendezvous_read(endpoint_name, 'port')
        pid  = tunnel.rendezvous_read(endpoint_name, 'pid')

        task = self._watchers.get(endpoint_name)
        if task is None:
            status = 'no_tunnel'
        elif port is not None:
            # Relay file present → child endpoint successfully published its port.
            # The SSH process lives on the compute node and is not observable
            # from here, so ``active`` is terminal from the login's point of
            # view.
            status = 'active'
        elif task.done():
            # Watcher finished without a port file → the job terminated or
            # the child never published. Report as failed.
            status = 'failed'
        else:
            # Watcher still running, waiting for the child to publish.
            status = 'pending'

        return {'endpoint_name': endpoint_name,
                'status':    status,
                'port':      port,
                'pid':       pid}

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal tunnel helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _reverse_broker_target(self) -> tuple:
        """Resolve the ``(host, port)`` the reverse ``ssh -R`` forwards to.

        Same value the child would resolve from its broker URL, handed to
        OpenSSH's ``-R`` spec.
        """
        broker_url = getattr(self._app.state, 'broker_url', '') or ''
        parsed = urlparse(broker_url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or (443 if parsed.scheme in ('https', 'wss') else 8000)
        return host, port

    async def _watch_forward(self, endpoint_name: str, native_id) -> None:
        """Observe a forward-mode tunneled job until its port file appears.

        Forward mode (compute → login): the child opens its own ``ssh -L``
        and writes the rendezvous port file, so this watcher only polls job
        state.  When the job goes terminal or vanishes before the file
        appears the failure already surfaces as the job's natural ``FAILED``
        state, so there is nothing to clean up here.
        """
        batch = batch_system.detect_batch_system()
        log.info("[psij] forward watcher for endpoint '%s' (native=%s, "
                 "backend=%s)", endpoint_name, native_id, batch.name)

        last_state     = None
        seen_known     = False
        unknown_streak = 0
        for attempt in range(300):       # up to ~10 min (2s × 300)
            await asyncio.sleep(2)

            if tunnel.rendezvous_read(endpoint_name) is not None:
                log.info("[psij] endpoint '%s' forward tunnel active",
                         endpoint_name)
                return

            state = await asyncio.to_thread(batch.job_state, native_id)

            if state == batch_system.STATE_UNKNOWN:
                unknown_streak += 1
            else:
                seen_known     = True
                unknown_streak = 0

            if state != last_state or attempt % 30 == 0:
                log.info("[psij] forward watcher endpoint=%s job=%s state=%r "
                         "(attempt %d/300)", endpoint_name, native_id,
                         state or '(unknown)', attempt)
                last_state = state

            if state in batch_system.TERMINAL_STATES:
                log.warning("[psij] job %s ended with state %s — forward tunnel "
                            "port file never appeared", native_id, state)
                return

            if seen_known and unknown_streak >= UNKNOWN_TOLERANCE:
                log.warning("[psij] job %s vanished from queue (state=UNKNOWN x "
                            "%d) — forward tunnel port file never appeared",
                            native_id, unknown_streak)
                return

        log.warning("[psij] forward watcher for endpoint '%s' timed out",
                    endpoint_name)

    async def _watch_reverse(self, sid: str, endpoint_name: str, native_id,
                             job_id: 'str | None') -> None:
        """Drive a reverse-mode tunnel and tear it down when the job ends.

        Reverse mode (login → compute): the child publishes its compute
        hostname in the ``.req`` rendezvous file; we spawn ``ssh -R`` from
        this side, which writes the port file the child then reads.  On any
        spawn failure — or the job going terminal / vanishing before the
        tunnel comes up — we record a reason via :meth:`_fail_tunnel` and
        cancel the now-useless allocation, so the client sees ``FAILED``
        (with our reason) via :meth:`get_job_status`.  A plain operator
        cancel keeps its natural ``CANCELLED`` state.
        """
        batch = batch_system.detect_batch_system()
        broker_host, broker_port = self._reverse_broker_target()
        log.info("[psij] reverse watcher for endpoint '%s' (job=%s native=%s "
                 "backend=%s)", endpoint_name, job_id, native_id, batch.name)

        ssh_proc       = None
        last_state     = None
        seen_known     = False
        unknown_streak = 0
        try:
            for attempt in range(300):       # up to ~10 min (2s × 300)
                await asyncio.sleep(2)

                # Success: the remote port has been published.
                if tunnel.rendezvous_read(endpoint_name) is not None:
                    log.info("[psij] endpoint '%s' reverse tunnel active",
                             endpoint_name)
                    await self._await_reverse_teardown(
                        endpoint_name, native_id, ssh_proc, batch)
                    return

                state = await asyncio.to_thread(batch.job_state, native_id)

                # Spawn ssh -R once the child publishes its hostname via .req.
                # The .req file — not job state — is the authoritative "child
                # is ready" signal: squeue reports UNKNOWN for a short job's
                # whole lifetime on some clusters, while .req can only exist
                # once the child is actually running on its compute node.
                if ssh_proc is None:
                    try:
                        req = tunnel.rendezvous_wait(endpoint_name)
                    except (ValueError, OSError) as exc:
                        await self._fail_tunnel(
                            sid, endpoint_name, job_id, native_id,
                            f"reverse SSH: .req file unreadable: {exc}")
                        return
                    if req is not None:
                        compute_host = req.get('hostname')
                        if not compute_host:
                            await self._fail_tunnel(
                                sid, endpoint_name, job_id, native_id,
                                "reverse SSH: .req file has no 'hostname' field")
                            return
                        log.info("[psij] reverse: child .req hostname=%s for "
                                 "job %s, spawning ssh -R to %s:%s", compute_host,
                                 native_id, broker_host, broker_port)
                        ssh_proc = await self._spawn_reverse_ssh(
                            sid, endpoint_name, native_id, job_id,
                            compute_host, broker_host, broker_port, batch)
                        if ssh_proc is None:
                            return   # spawn failed; _fail_tunnel already fired

                if state == batch_system.STATE_UNKNOWN:
                    unknown_streak += 1
                else:
                    seen_known     = True
                    unknown_streak = 0

                if state != last_state or attempt % 30 == 0:
                    log.info("[psij] reverse watcher endpoint=%s job=%s state=%r "
                             "(attempt %d/300)", endpoint_name, native_id,
                             state or '(unknown)', attempt)
                    last_state = state

                if state in batch_system.TERMINAL_STATES:
                    log.warning("[psij] job %s ended with state %s before "
                                "reverse tunnel came up", native_id, state)
                    if ssh_proc is not None:
                        await self._fail_tunnel(
                            sid, endpoint_name, job_id, native_id,
                            f"reverse SSH spawned but rendezvous file never "
                            f"appeared (job {state})", spawn_proc=ssh_proc)
                    elif state != batch_system.STATE_CANCELLED:
                        # A plain operator cancel (CANCELLED) is left as-is;
                        # any other terminal state means the child never got
                        # to write .req, which is a tunnel failure.
                        await self._fail_tunnel(
                            sid, endpoint_name, job_id, native_id,
                            f"child never wrote .req (job ended {state} before "
                            f"reverse tunnel could be set up)")
                    return

                if seen_known and unknown_streak >= UNKNOWN_TOLERANCE:
                    log.warning("[psij] job %s vanished from queue (state="
                                "UNKNOWN x %d) — reverse tunnel never came up",
                                native_id, unknown_streak)
                    if ssh_proc is not None:
                        await self._fail_tunnel(
                            sid, endpoint_name, job_id, native_id,
                            f"reverse SSH spawned but job vanished "
                            f"(UNKNOWN x {unknown_streak})", spawn_proc=ssh_proc)
                    return

            log.warning("[psij] reverse watcher for endpoint '%s' timed out",
                        endpoint_name)
            await self._fail_tunnel(
                sid, endpoint_name, job_id, native_id,
                "tunnel watcher timed out before rendezvous file appeared",
                spawn_proc=ssh_proc)
        finally:
            if ssh_proc is not None and ssh_proc.poll() is None:
                tunnel.cleanup_tunnel(ssh_proc, endpoint_name)
                self._tunnel_procs.pop(endpoint_name, None)

    async def _spawn_reverse_ssh(self, sid: str, endpoint_name: str, native_id,
                                 job_id: 'str | None', compute_host: str,
                                 broker_host: str, broker_port: int, batch):
        """Spawn ``ssh -R`` to *compute_host*, retrying briefly.

        Returns the SSH process on success.  On failure records the reason
        via :meth:`_fail_tunnel` (which cancels the job) and returns ``None``
        so the caller stops watching.  The retry tolerates the
        pam_slurm_adopt race where a compute node's sshd briefly refuses
        login-node logins just after the job registers.
        """
        last_exc = None
        for spawn_attempt in range(30):
            try:
                proc, port = await asyncio.to_thread(
                    tunnel.spawn_reverse_tunnel,
                    compute_host, broker_host, broker_port, endpoint_name)
            except Exception as exc:
                last_exc = exc
                if await asyncio.to_thread(batch.job_state, native_id) \
                        in batch_system.TERMINAL_STATES:
                    await self._fail_tunnel(
                        sid, endpoint_name, job_id, native_id,
                        f"reverse SSH spawn failed and job went terminal: {exc}")
                    return None
                if spawn_attempt == 0:
                    log.info("[psij] reverse: first ssh -R spawn rejected "
                             "(likely pam_slurm_adopt race), retrying: %s", exc)
                else:
                    log.debug("[psij] reverse: ssh -R spawn retry %d/30: %s",
                              spawn_attempt + 1, exc)
                await asyncio.sleep(1)
                continue
            self._tunnel_procs[endpoint_name] = proc
            log.info("[psij] reverse: ssh -R for endpoint '%s' active on remote "
                     "port %d", endpoint_name, port)
            return proc
        await self._fail_tunnel(
            sid, endpoint_name, job_id, native_id,
            f"reverse SSH spawn failed after 30 attempts: {last_exc}")
        return None

    async def _await_reverse_teardown(self, endpoint_name: str, native_id,
                                      ssh_proc, batch) -> None:
        """Poll a live reverse tunnel's job until it ends, then tear down ssh -R."""
        try:
            while True:
                await asyncio.sleep(5)
                state = await asyncio.to_thread(batch.job_state, native_id)
                if state in batch_system.TERMINAL_STATES \
                        or state == batch_system.STATE_UNKNOWN:
                    log.info("[psij] reverse: job %s reached %s — tearing down "
                             "ssh -R for endpoint %s", native_id, state,
                             endpoint_name)
                    return
                if ssh_proc is not None and ssh_proc.poll() is not None:
                    log.warning("[psij] reverse: ssh -R for endpoint %s exited "
                                "(rc=%s) while job %s still running",
                                endpoint_name, ssh_proc.returncode, native_id)
                    return
        finally:
            if ssh_proc is not None:
                tunnel.cleanup_tunnel(ssh_proc, endpoint_name)
            self._tunnel_procs.pop(endpoint_name, None)
            tunnel.rendezvous_clear(endpoint_name)

    async def _fail_tunnel(self, sid: str, endpoint_name: str,
                           job_id: 'str | None', native_id, reason: str,
                           spawn_proc=None) -> None:
        """Record a tunnel failure and cancel the now-useless job.

        The recorded reason surfaces via :meth:`get_job_status` as a
        synthesised ``state='FAILED'`` plus an ``error`` field.  The cancel
        goes through the owning session (via ``sid``) to release the
        allocation.
        """
        log.error("[psij] tunnel failed for endpoint '%s' (job %s): %s",
                  endpoint_name, job_id, reason)
        if job_id:
            self._failure_reasons[job_id] = reason
        if spawn_proc is not None:
            tunnel.cleanup_tunnel(spawn_proc, endpoint_name)
            self._tunnel_procs.pop(endpoint_name, None)
        # Clear rendezvous files on every failure path so a fast re-submit
        # can't pick up stale state.
        tunnel.rendezvous_clear(endpoint_name)
        if job_id is not None:
            try:
                # Fire-and-forget — the watcher has already failed-marked it.
                await self._forward(sid, PSIJSession.cancel_job,
                                    job_id=str(job_id))
            except Exception as exc:
                log.warning("[psij] cancel after tunnel failure raised: %s",
                            exc)

    async def _cleanup_watchers(self) -> None:
        """Cancel all watcher tasks + tear down any open reverse SSH
        processes on plugin shutdown."""
        for _, task in list(self._watchers.items()):
            task.cancel()
        self._watchers.clear()
        for endpoint_name, proc in list(self._tunnel_procs.items()):
            tunnel.cleanup_tunnel(proc, endpoint_name)
            tunnel.rendezvous_clear(endpoint_name)
        self._tunnel_procs.clear()



