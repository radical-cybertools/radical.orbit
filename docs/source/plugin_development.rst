
Plugin Development Guide
************************

Overview
========

The ORBIT plugin system lets you extend endpoint nodes with
domain-specific functionality.  Each plugin gets its own URL namespace,
session management, and notification support out of the box.

Architecture
============

Base Classes
------------

The plugin system provides three base classes:

1. **Plugin** (``plugin_base.py``) — Server-side plugin registered on the endpoint.

   - Manages sessions, routes, and notifications
   - Auto-registers via ``plugin_name`` class attribute
   - Provides ``add_route_post()`` / ``add_route_get()`` helpers
   - Forwards requests to sessions via ``_forward()``

2. **PluginSession** (``plugin_session_base.py``) — Per-client session state.

   - Created when a client calls ``register_session``
   - Holds domain-specific state (jobs, tasks, connections)
   - Sends notifications via ``self._notify(topic, data)``

3. **PluginClient** (``client.py``) — Application-side client helper.

   - Wraps HTTP calls to the bridge/endpoint REST API
   - Manages session registration and lifecycle
   - Optional: only needed for Python client usage

Inheritance Hierarchy
---------------------

.. code-block:: text

    Plugin (server-side)
      ├── PluginSysinfo
      ├── PluginPSIJ
      ├── PluginRhapsody
      ├── PluginQueueInfo
      ├── PluginLucid
      └── PluginXGFabric

    PluginSession (server-side)
      ├── SysinfoSession
      ├── PSIJSession
      ├── RhapsodySession
      ├── QueueInfoSession
      └── ...

    PluginClient (client-side)
      ├── PSIJClient
      ├── RhapsodyClient
      ├── QueueInfoClient
      └── ...

Creating a New Plugin
=====================

Step 1: Define Your Session Class
----------------------------------

Create a session class that inherits from ``PluginSession``:

.. code-block:: python

    from radical.orbit.plugin_session_base import PluginSession

    class MySession(PluginSession):
        """Server-side session for MyPlugin."""

        def __init__(self, sid: str):
            super().__init__(sid)
            self._data = {}  # Per-session state

        async def do_work(self, param: str) -> dict:
            """Perform a domain-specific operation."""
            self._check_active()
            result = f"processed: {param}"
            self._data[param] = result

            # Send real-time notification to clients
            if self._notify:
                self._notify("work_status", {
                    "param": param,
                    "status": "done"
                })

            return {"result": result}

        async def close(self) -> dict:
            """Clean up session resources."""
            self._data = {}
            return await super().close()

**Key Points:**

- Call ``super().__init__(sid)`` to initialize base functionality
- Use ``self._check_active()`` to validate session is open
- Use ``self._notify(topic, data)`` for real-time notifications
- Call ``await super().close()`` in your close method

Step 2: Define Your Plugin Class
---------------------------------

Create a plugin class that inherits from ``Plugin``:

.. code-block:: python

    from fastapi import FastAPI, Request
    from starlette.responses import JSONResponse
    from radical.orbit.plugin_base import Plugin

    class PluginMyService(Plugin):
        """MyService plugin for ORBIT."""

        plugin_name   = "myservice"     # URL namespace and registry key
        session_class = MySession       # Required!
        version       = '0.1.0'

        def __init__(self, app: FastAPI, instance_name: str = "myservice"):
            super().__init__(app, instance_name)

            # Add plugin-specific routes
            self.add_route_post('do_work/{sid}', self.do_work)

        async def do_work(self, request: Request) -> JSONResponse:
            """Route handler — forwards to session method."""
            sid  = request.path_params['sid']
            data = await request.json()
            return await self._forward(sid, MySession.do_work,
                                       param=data['param'])

**Key Points:**

- Set ``plugin_name`` for auto-registration and URL namespace
- Set ``session_class`` to your session class
- Use ``self.add_route_post()`` / ``self.add_route_get()`` for routes
- Use ``self._forward(sid, method, **kwargs)`` to dispatch to sessions
- ``_forward`` handles session lookup, error wrapping, and JSON response

Auto-Registered Routes
-----------------------

Every plugin automatically gets these routes:

- ``POST /{plugin_name}/register_session`` — Create a new session
- ``POST /{plugin_name}/unregister_session/{sid}`` — Close a session
- ``GET  /{plugin_name}/version`` — Plugin version
- ``GET  /{plugin_name}/list_sessions`` — List active sessions
- ``GET  /{plugin_name}/health`` — Health check
- ``GET  /{plugin_name}/ui_config`` — UI configuration for the Explorer

Step 3: Define Your Client Class (Optional)
--------------------------------------------

For Python client access, create a client class:

.. code-block:: python

    from radical.orbit.client import PluginClient

    class MyServiceClient(PluginClient):
        """Client-side interface for MyService plugin."""

        def do_work(self, param: str) -> dict:
            """Call do_work on the endpoint."""
            if not self.sid:
                raise RuntimeError("No active session")

            url  = self._url(f"do_work/{self.sid}")
            resp = self._http.post(url, json={"param": param})
            self._raise(resp, f"do_work({param!r})")
            return resp.json()

**Key Points:**

- ``self.sid`` is set after ``register_session()``
- ``self._url(path)`` builds the full URL with namespace
- ``self._http`` is the HTTP client (``httpx.Client``)
- ``self._raise(resp)`` raises on non-2xx status codes

Advanced Patterns
=================

Custom Session Creation
-----------------------

Override ``_create_session()`` for custom initialization:

.. code-block:: python

    class PluginMyService(Plugin):
        session_class = MySession

        def _create_session(self, sid: str, **kwargs) -> MySession:
            """Pass extra config to sessions."""
            return self.session_class(sid, config=self._config)

Custom Session Registration
----------------------------

Override ``register_session()`` for custom registration logic:

.. code-block:: python

    async def register_session(self, request: Request) -> JSONResponse:
        """Register with custom parameters."""
        import uuid as _uuid

        data     = await request.json()
        backends = data.get('backends', ['default'])
        sid      = f"session.{_uuid.uuid4().hex[:8]}"

        session = self._create_session(sid, backends=backends)
        if hasattr(session, 'initialize'):
            await session.initialize()
        self._sessions[sid] = session

        return JSONResponse({"sid": sid})

Notifications
-------------

Sessions send notifications via ``self._notify(topic, data)``.
Notifications flow: Session → Plugin → Endpoint → Bridge → SSE clients.

.. code-block:: python

    # In your session method:
    if self._notify:
        self._notify("job_status", {
            "job_id": "abc123",
            "state":  "RUNNING"
        })

Clients receive notifications via SSE at ``/events`` on the bridge.
See the main CLAUDE.md for subscription examples (JavaScript, Python).

Topology Updates
----------------

Override ``on_topology_change`` to react when endpoints connect or disconnect:

.. code-block:: python

    class PluginMyService(Plugin):
        async def on_topology_change(self, endpoints: dict):
            for endpoint_name, info in endpoints.items():
                plugins = info.get('plugins', [])
                print(f"Endpoint {endpoint_name}: {plugins}")

UI Configuration
================

Plugins can provide a ``ui_config`` dict that the Explorer UI uses to
render forms, monitors, and notification subscriptions automatically:

.. code-block:: python

    class PluginMyService(Plugin):
        ui_config = {
            "icon": "🔧",
            "title": "My Service",
            "description": "Does useful things.",
            "forms": [{
                "id": "submit",
                "title": "Submit Work",
                "fields": [
                    {"name": "param", "type": "text", "label": "Parameter",
                     "default": "hello"},
                ],
                "submit": {"label": "▶ Submit", "style": "success"}
            }],
            "monitors": [{
                "id": "tasks",
                "title": "Task Monitor",
                "type": "task_list",
                "empty_text": "No tasks yet."
            }],
            "notifications": {
                "topic": "work_status",
                "id_field": "task_id",
                "state_field": "state"
            }
        }

Alternatively, plugins can provide a custom JS module by setting
``ui_module`` to the path of a ``.js`` file.  See the next section for
the complete JS Module API reference.

JS Plugin Module API
====================

When ``ui_module`` is set to a ``.js`` file path, the Explorer loads and runs
the module. The module must be an ES module (``type="module"``) and may export
the following functions and constants:

Required Exports
----------------

.. code-block:: javascript

    // Unique plugin name — used for routing and session lookup
    export const name = 'myplugin';

    // Return the HTML for the plugin page (called once per endpoint)
    export function template() { return '<div>...</div>'; }

    // Return plugin-scoped CSS (injected into a <style> tag)
    export function css() { return '.my-class { ... }'; }

    // Called when the plugin page is mounted; bind event listeners here
    export function init(page, api) { ... }

Optional Exports
----------------

.. code-block:: javascript

    // Called when the plugin's tab is shown (page already mounted)
    export function onShow(page, api) { ... }

    // Called when an SSE notification arrives matching notificationConfig
    export function onNotification(data, page, api) { ... }

    // Declare which SSE topic this plugin subscribes to
    export const notificationConfig = {
        topic:   'job_status',   // SSE topic to subscribe to
        idField: 'job_id',       // Field in data.data used as entity ID
    };

The ``api`` Object
------------------

The ``api`` object is passed to ``init()``, ``onShow()``, and
``onNotification()``. It exposes:

**Session management**

``api.getSession(pluginName)``
    Returns a Promise resolving to the active session ID for the named plugin,
    creating one if needed.

**HTTP**

``api.fetch(path, options)``
    Fetch relative to the current plugin namespace on the bridge.
    Returns parsed JSON. Throws on HTTP errors.

``api.fetchRaw(path, options)``
    Same as ``fetch`` but returns the raw ``Response`` object.
    Used when you need headers or streaming (e.g. file download).

**UI helpers**

``api.flash(message, ok=true)``
    Show a transient status message. ``ok=false`` styles it as an error.

``api.escHtml(s)``
    HTML-escape a string for safe ``innerHTML`` insertion.

``api.showOverlay(title, bodyHtml)``
    Open the shared full-screen overlay with the given title and HTML body.

**Task tracking**

``api.registerTask(plugin, id, label)``
    Register a task ID in the global task list (shown in the taskbar).

**Queue data cache**

``api.getQueueData()``
    Return cached queue/allocation data for this endpoint (populated by
    the ``queue_info`` plugin on load), or ``undefined`` if not available.

``api.setQueueData(data)``
    Store queue data for this endpoint (called by ``queue_info``).

**Endpoint info (read-only properties)**

``api.endpointName``
    The name of the current endpoint (e.g. ``"hpc1"``).

``api.pluginName``
    The plugin module name.

``api.bridgeUrl``
    Full URL of the bridge (e.g. ``"https://bridge:8000"``).

``api.getPluginNames()``
    Returns an array of all plugin names registered on this endpoint.

**Endpoint management**

``api.disconnectEndpoint(event)``
    Initiate graceful disconnection of this endpoint. Pass the click event
    to prevent default and stop propagation.

Notifications
-------------

SSE notifications are delivered to ``onNotification(data, page, api)`` only
if the module exports a matching ``notificationConfig``::

    export const notificationConfig = {
        topic:   'job_status',  // Must match the server-side notify() topic
        idField: 'job_id',      // Field in notification data used as entity key
    };

The ``data`` argument passed to ``onNotification`` has this shape::

    {
        topic: 'job_status',
        data:  { job_id: '...', state: 'RUNNING', ... }
    }

**Buffering pattern**: Notifications may arrive before the entity row exists
in the DOM (e.g. a status update arrives before ``submit`` returns). Buffer
them in a module-level dict keyed by entity ID, then drain the buffer after
adding the row:

.. code-block:: javascript

    const pending = {};  // id -> notification data

    export function onNotification(data, page, api) {
        const id = data.data?.job_id;
        const row = page.querySelector(`[data-job-id="${CSS.escape(id)}"]`);
        if (row) {
            updateRow(page, id, data.data.state);
        } else if (id) {
            pending[id] = data.data;  // buffer for later
        }
    }

    // After creating the row:
    if (pending[id]) {
        updateRow(page, id, pending[id].state);
        delete pending[id];
    }

See ``psij.js`` and ``rhapsody.js`` for complete examples of this pattern.

Session Lifecycle
=================

Sessions are created on the first ``api.getSession()`` call and persist until:

- The browser tab is closed or navigated away
- The endpoint disconnects (all sessions are lost; the endpoint has no persistence)
- The session TTL expires (default 1 hour of inactivity)
- The client calls ``unregister_session/{sid}``

When ``close()`` is called on a ``PluginSession``:

- The session should release all resources (threads, backend connections, file handles)
- Any background polling or watchers must be cancelled
- The base ``super().close()`` sets the session status to inactive

Sessions are **not persisted** across endpoint restarts. Clients must re-register
after an endpoint reconnects.

Async / Sync Guidelines
=======================

All plugin route handlers **must** be ``async def``.  Blocking operations
(file I/O, subprocess calls, network requests) must be offloaded to a thread
pool using ``asyncio.to_thread``::

    async def my_handler(self, param: str) -> dict:
        # Blocking call — run in thread to avoid blocking the event loop
        result = await asyncio.to_thread(subprocess.check_output, ['cmd', param])
        return {'output': result.decode()}

Callbacks from external libraries (e.g. PsiJ status callbacks, Rhapsody
backend callbacks) run in background threads, not the event loop.  Use
``self._notify(topic, data)`` from those callbacks — it is thread-safe and
schedules the SSE send on the main event loop automatically.

Testing Your Plugin
===================

.. code-block:: python

    import pytest
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    @pytest.mark.asyncio
    async def test_my_plugin():
        app    = FastAPI()
        plugin = PluginMyService(app)
        client = TestClient(app)

        # Register session
        resp = client.post(f"{plugin.namespace}/register_session")
        assert resp.status_code == 200
        sid = resp.json()['sid']

        # Call plugin endpoint
        resp = client.post(
            f"{plugin.namespace}/do_work/{sid}",
            json={"param": "test"}
        )
        assert resp.status_code == 200
        assert resp.json()['result'] == "processed: test"

Summary
=======

To create a new plugin:

1. Create a session class inheriting from ``PluginSession``
2. Create a plugin class inheriting from ``Plugin``
3. Set ``plugin_name`` and ``session_class``
4. Add routes in ``__init__`` using ``add_route_post`` / ``add_route_get``
5. Optionally create a ``PluginClient`` subclass for Python clients
6. Optionally provide ``ui_config`` for the Explorer UI

See the existing plugins (``plugin_sysinfo.py``, ``plugin_psij.py``,
``plugin_rhapsody.py``) for real-world examples.
