
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'



import asyncio
import os

from fastapi import FastAPI
from starlette.requests import Request

import radical.pilot as rp

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient


class LucidSession(PluginSession):
    """
    Lucid session with Radical Pilot session management (Server-side).

    Each session maintains its own RP Session, Pilot Manager, and Task Manager.
    """

    def __init__(self, sid: str):
        """
        Initialize a LucidSession instance with a unique session ID.
        Start a Radical Pilot session, Pilot Manager, and Task Manager, all
        private to this session.

        Args:
            sid (str): The unique session ID.
        """
        super().__init__(sid)

        self._session: rp.Session = rp.Session()
        self._pmgr: rp.PilotManager = rp.PilotManager(session=self._session)
        self._tmgr: rp.TaskManager = rp.TaskManager(session=self._session)

    async def close(self) -> dict:
        """
        Close the Radical Pilot session.

        Returns:
            dict: An empty dictionary indicating successful closure.
        """
        await asyncio.to_thread(self._session.close)
        self._session = None
        self._pmgr = None
        self._tmgr = None

        return await super().close()

    async def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot to the Pilot Manager and return its ID.

        Args:
            description (dict): The pilot description dictionary.

        Returns:
            dict: A dictionary containing the pilot ID ('pid').
        """
        self._check_active()

        pd = rp.PilotDescription(description)
        pilot = await asyncio.to_thread(self._pmgr.submit_pilots, pd)
        await asyncio.to_thread(self._tmgr.add_pilots, pilot)
        return {'pid': pilot.uid}

    async def task_submit(self, description: dict) -> dict:
        """
        Submit a task to the Task Manager and return its ID.

        Args:
            description (dict): The task description dictionary.

        Returns:
            dict: A dictionary containing the task ID ('tid').
        """
        self._check_active()

        td = rp.TaskDescription(description)
        task = await asyncio.to_thread(self._tmgr.submit_tasks, td)
        return {"tid": task.uid}

    async def task_wait(self, tid: str) -> dict:
        """
        Wait for a task to complete and return its result.

        Args:
            tid (str): The task ID to wait for.

        Returns:
            dict: A dictionary containing the task ID ('tid') and task details ('task').
        """
        self._check_active()

        await asyncio.to_thread(self._tmgr.wait_tasks, tid)
        task = await asyncio.to_thread(self._tmgr.get_tasks, tid)
        return {"tid": tid, "task": task.as_dict()}



class LucidClient(PluginClient):
    """
    Client-side interface for the Lucid plugin.
    """

    def pilot_submit(self, description: dict) -> dict:
        """
        Submit a pilot.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"pilot_submit/{self.sid}")
        resp = self._http.post(url, json={'description': description})
        self._raise(resp)
        return resp.json()

    def task_submit(self, description: dict) -> dict:
        """
        Submit a task.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"task_submit/{self.sid}")
        resp = self._http.post(url, json={'description': description})
        self._raise(resp)
        return resp.json()

    def task_wait(self, tid: str) -> dict:
        """
        Wait for a task to complete.
        """
        if not self.sid:
            raise RuntimeError("No active session")

        url = self._url(f"task_wait/{self.sid}/{tid}")
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()


class PluginLucid(Plugin):
    """
    Lucid plugin for Radical Edge.

    This plugin manages multiple Lucid sessions, each with its own Radical Pilot
    session, Pilot Manager, and Task Manager.
    """

    plugin_name = "lucid"
    session_class = LucidSession
    client_class = LucidClient
    version = '0.0.1'

    ui_config = {
        "icon": "🧠",
        "title": "Lucid",
        "description": "Radical Pilot session management.",
        "stub_message": "Advanced web interface for Lucid is not yet available."
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Lucid loads on compute nodes only (RADICAL Pilot)."""
        from .utils import host_role
        return host_role(app)['role'] == 'compute'

    def __init__(self, app: FastAPI):
        """
        Initialize the Lucid plugin with the FastAPI app. Set up routes for
        session management and task handling.

        Args:
            app (FastAPI): The FastAPI application instance.
        """
        super().__init__(app, 'lucid')

        # Register Lucid-specific routes
        self.add_route_post('pilot_submit/{sid}', self.pilot_submit)
        self.add_route_post('task_submit/{sid}', self.task_submit)
        self.add_route_get('task_wait/{sid}/{tid}', self.task_wait)

    async def pilot_submit(self, request: Request) -> dict:
        """
        Submit a pilot to the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'.
                             JSON body must contain 'description' as a pilot description.

        Returns:
            JSONResponse: A JSON response containing the pilot ID ('pid').
        """
        data = request.path_params
        json = await request.json()
        sid = data['sid']
        desc = json['description']

        return await self._forward(sid, LucidSession.pilot_submit, desc)

    async def task_submit(self, request: Request) -> dict:
        """
        Submit a task to the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'.
                             JSON body must contain 'description' as a task description.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid').
        """
        data = request.path_params
        json = await request.json()
        sid = data['sid']
        desc = json['description']

        return await self._forward(sid, LucidSession.task_submit, desc)

    async def task_wait(self, request: Request) -> dict:
        """
        Wait for a task to complete in the specified LucidSession instance.

        Args:
            request (Request): The incoming HTTP request. Path parameters must contain 'sid'
                             and 'tid'.

        Returns:
            JSONResponse: A JSON response containing the task ID ('tid') and
                        task dictionary ('task').
        """
        data = request.path_params
        sid = data['sid']
        tid = data['tid']
        return await self._forward(sid, LucidSession.task_wait, tid)

