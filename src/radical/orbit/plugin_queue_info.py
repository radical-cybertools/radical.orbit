
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



import asyncio
import logging

log = logging.getLogger('radical.orbit')

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient
from .queue_info import make_queue_info, QueueInfo

# Re-exported for tests / external callers that patch this name on the
# plugin_queue_info module; the real class lives in queue_info_slurm.
from .queue_info_slurm import QueueInfoSlurm   # noqa: F401
from .batch_system import detect_batch_system

# Re-exported for tests / external callers that imported this from the
# old location. The real implementation lives in batch_system_slurm.
from .batch_system_slurm import _parse_slurm_time   # noqa: F401


class QueueInfoSession(PluginSession):
    """
    QueueInfo session with shared backend.

    All sessions share a single backend instance for cache efficiency.
    """

    def __init__(self, sid: str, backend: QueueInfo):
        """
        Initialize a QueueInfoSession instance.

        Args:
            sid (str): The unique session ID.
            backend (QueueInfo): Shared backend instance from the plugin.
        """
        super().__init__(sid)
        self._backend = backend

    async def close(self) -> dict:
        """
        Close this session.

        Note: Backend is shared and not cleaned up here.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        return await super().close()

    async def get_info(self, user=None, force=False):
        """
        Return queue/partition info.

        Args:
            user (str): User to filter partitions for. When None (default),
                defaults to the current user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Queue information from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.get_info,
                                       user=user, force=force)

    async def list_jobs(self, queue, user=None, force=False):
        """
        List jobs in a queue.

        Args:
            queue (str): Partition name.
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_jobs,
                                      queue, user, force)

    async def list_all_jobs(self, user=None, force=False):
        """
        List all jobs for the user across all partitions.

        Args:
            user (str): User to filter jobs for. When None (default),
                defaults to the current user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_all_jobs,
                                       user, force)

    async def cancel_job(self, job_id: str) -> dict:
        """Cancel a job via the active batch system (scancel/qdel/...)."""
        self._check_active()
        batch = detect_batch_system()
        try:
            await asyncio.to_thread(batch.cancel, job_id)
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return {'job_id': job_id, 'status': 'canceled'}

    async def list_allocations(self, user=None, force=False):
        """
        List allocations/projects.

        Args:
            user (str): Optional user name to filter on.
            force (bool): Bypass cache if True.

        Returns:
            dict: Allocation listing from the backend.
        """
        self._check_active()
        return await asyncio.to_thread(self._backend.list_allocations,
                                      user, force)


class QueueInfoClient(PluginClient):
    """
    Client-side interface for the QueueInfo plugin.
    """

    def get_info(self, user: str = None, force: bool = False) -> dict:
        """
        Return queue/partition information.

        Args:
            user (str): User to filter partitions for. When None (default),
                uses the endpoint service user. Pass user='*' to return all
                partitions (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Queue information filtered by user access.
        """
        self._require_session()

        url = self._url(f"get_info/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def list_jobs(self, queue: str, user: str = None, force: bool = False) -> dict:
        """
        List jobs in a specified queue/partition.

        Args:
            queue (str): Partition name to list jobs for.
            user (str): User to filter jobs for. When None (default),
                uses the endpoint service user. Pass user='*' to return all
                jobs (admin view).
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing filtered by user.
        """
        self._require_session()

        url = self._url(f"list_jobs/{self.sid}/{queue}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def list_all_jobs(self, user: str = None, force: bool = False) -> dict:
        """
        List all jobs for the user across all partitions.

        Args:
            user (str): User to filter jobs for.
            force (bool): Bypass cache if True.

        Returns:
            dict: Job listing.
        """
        self._require_session()

        url = self._url(f"list_all_jobs/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()

    def cancel_job(self, job_id: str) -> dict:
        """Cancel a job by ID."""
        self._require_session()
        resp = self._http.post(self._url(f"cancel/{self.sid}/{job_id}"))
        self._raise(resp, f"cancel job {job_id!r}")
        return resp.json()

    def list_allocations(self, user: str = None, force: bool = False) -> dict:
        """
        List allocations/projects.
        """
        self._require_session()

        url = self._url(f"list_allocations/{self.sid}")
        params = {"force": str(force).lower()}
        if user:
            params["user"] = user
        resp = self._http.get(url, params=params)
        self._raise(resp)
        return resp.json()



    def job_allocation(self) -> 'dict | None':
        """Return endpoint job allocation info, or None if not inside a batch job.

        No session is required.  The information reflects the environment of
        the **endpoint** process, not the client.

        Returns:
            None: Endpoint is running on a login node.
            dict: Allocation summary with keys ``job_id``, ``partition``,
                ``n_nodes``, ``nodelist``, ``cpus_per_node``,
                ``gpus_per_node``, ``account``, ``job_name``, ``runtime``.

        Raises:
            RuntimeError: Endpoint is inside an allocation but the scheduler did
                not provide enough info to summarise it.
        """
        resp = self._http.get(self._url('job_allocation'))
        self._raise(resp, 'job_allocation')
        return resp.json().get('allocation')

    def nodelist(self) -> list:
        """Return the expanded list of hostnames in the endpoint's allocation.

        No session is required.  Hostnames are returned one per node, in
        scheduler-reported order.  Empty list when the endpoint is on a login
        node, when no batch backend is detected, or when the scheduler
        doesn't expose the info.

        Returns:
            list[str]: Allocated compute-node hostnames.
        """
        resp = self._http.get(self._url('nodelist'))
        self._raise(resp, 'nodelist')
        return list(resp.json().get('nodelist') or [])

    def backend(self) -> str:
        """Return the active batch backend name on the endpoint.

        Returns:
            str: ``'slurm'``, ``'pbs'``, or ``'none'``.
        """
        resp = self._http.get(self._url('backend'))
        self._raise(resp, 'backend')
        return resp.json().get('backend', 'none')


class PluginQueueInfo(Plugin):
    """
    QueueInfo plugin for ORBIT.

    This plugin exposes batch system queue information, job listings, and
    allocation data via REST endpoints.  ``is_enabled()`` prevents loading on
    endpoints where no recognised batch system is installed.

    Backend selection is automatic: the plugin uses
    :func:`make_queue_info` which dispatches to ``QueueInfoSlurm``,
    ``QueueInfoPBSPro``, or ``QueueInfoNone`` based on what's available.
    """

    plugin_name = "queue_info"
    session_class = QueueInfoSession
    client_class = QueueInfoClient
    version = '0.0.1'

    ui_config = {
        "icon": "📋",
        "title": "Queue Info",
        "description": "Inspect batch partitions, jobs and allocations.",
        "refresh_button": True,
        "monitors": [{
            "id": "partitions",
            "title": "Partitions / Queues",
            "type": "table",
            "css_class": "queueinfo-content",
            "auto_load": "get_info/{sid}"
        }]
    }

    def __init__(self, app: FastAPI, instance_name='queue_info',
                 backend_conf=None, slurm_conf=None):
        """
        Initialize the QueueInfo plugin.

        Args:
            app (FastAPI): The FastAPI application instance.
            instance_name (str): Plugin instance name (used in namespace).
                Defaults to 'queue_info'. Override for multi-cluster setups.
            backend_conf (str): Optional path to a scheduler config file
                (e.g. slurm.conf). Forwarded to the backend; only the SLURM
                backend uses it today.
            slurm_conf (str): Deprecated alias for ``backend_conf``.
        """
        super().__init__(app, instance_name)

        # Back-compat: prefer backend_conf when both are given.
        conf = backend_conf if backend_conf is not None else slurm_conf

        # Create shared backend for all sessions
        self._backend = make_queue_info(conf_path=conf)

        # Start background prefetch to populate cache
        self._backend.start_prefetch()

        # Register QueueInfo-specific routes
        self.add_route_get('job_allocation', self.job_allocation_endpoint)
        self.add_route_get('nodelist',       self.nodelist_endpoint)
        self.add_route_get('backend',        self.backend_endpoint)
        self.add_route_get('get_info/{sid}', self.get_info)
        self.add_route_get('list_jobs/{sid}/{queue}', self.list_jobs)
        self.add_route_get('list_all_jobs/{sid}', self.list_all_jobs)
        self.add_route_get('list_allocations/{sid}', self.list_allocations)
        self.add_route_post('cancel/{sid}/{job_id}', self.cancel_job)

    def _create_session(self, sid: str, **kwargs):
        """
        Override to pass shared backend to each session.

        Args:
            sid (str): The session ID.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            QueueInfoSession: A new session instance using the shared backend.
        """
        return self.session_class(sid, backend=self._backend)

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Load on endpoints with a recognised batch system (SLURM or PBSPro)."""
        return detect_batch_system().name != 'none'

    def get_job_allocation(self) -> 'dict | None':
        """Return endpoint job allocation info, or None if not inside a job.

        Delegates to the active :class:`BatchSystem`.

        Returns:
            None: Endpoint is running on a login node.
            dict: Allocation details (see ``BatchSystem.job_allocation``).

        Raises:
            RuntimeError: Endpoint is inside an allocation but the scheduler did
                not provide enough info to summarise it.
        """
        alloc = detect_batch_system().job_allocation()
        log.debug('[queue_info] get_job_allocation result: %s', alloc)
        return alloc

    async def backend_endpoint(self, request: Request) -> dict:
        """Session-less endpoint: report which batch backend is active.

        Response::

            {"backend": "slurm" | "pbs" | "none"}
        """
        return {'backend': self._backend.backend_name}

    async def job_allocation_endpoint(self, request: Request) -> dict:
        """Session-less endpoint: returns current endpoint job allocation info.

        Response::

            {"allocation": null}                              # login node
            {"allocation": {"n_nodes": 4, "runtime": 3600}}  # inside a job
            {"allocation": {"n_nodes": 4, "runtime": null}}  # unlimited walltime
        """
        try:
            alloc = await asyncio.to_thread(self.get_job_allocation)
            return {'allocation': alloc}
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    async def nodelist_endpoint(self, request: Request) -> dict:
        """Session-less endpoint: expanded hostnames in *this* endpoint's allocation.

        Response::

            {"nodelist": []}                                # login node / no scheduler
            {"nodelist": ["nid001234", "nid001235", ...]}   # inside a job

        Implementation note: nodelist lives on the ``BatchSystem``
        hierarchy (``detect_batch_system()``), not on ``QueueInfo``
        (``self._backend``).  Mirrors the ``get_job_allocation`` pattern
        a few methods up.
        """
        nodes = await asyncio.to_thread(detect_batch_system().nodelist)
        return {'nodelist': nodes}

    async def get_info(self, request: Request) -> dict:
        """Return queue/partition information."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.get_info,
                                   user=user, force=force)

    async def list_jobs(self, request: Request) -> dict:
        """List jobs in a specified queue/partition."""
        data = request.path_params
        sid = data['sid']
        queue = data['queue']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_jobs,
                                   queue, user=user, force=force)

    async def list_all_jobs(self, request: Request) -> dict:
        """List all jobs for the user across all partitions."""
        data  = request.path_params
        sid   = data['sid']
        user  = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_all_jobs,
                                   user=user, force=force)

    async def list_allocations(self, request: Request) -> dict:
        """List allocations/projects."""
        data = request.path_params
        sid = data['sid']
        user = request.query_params.get('user')
        force = request.query_params.get('force', '').lower() == 'true'

        return await self._forward(sid, QueueInfoSession.list_allocations,
                                   user=user, force=force)

    async def cancel_job(self, request: Request) -> dict:
        """Cancel a job by ID."""
        sid    = request.path_params['sid']
        job_id = request.path_params['job_id']
        return await self._forward(sid, QueueInfoSession.cancel_job, job_id=job_id)

