
Plugin Development Guide
************************

Overview
========

The ORBIT plugin system lets you extend a participant — an endpoint runtime or
the broker itself — with domain-specific functionality.  Each plugin gets its
own endpoint-relative namespace, session management, and notification support
out of the box.

Where plugins run
=================

A plugin is **transport-agnostic**.  Its route handlers are plain ``async``
callables; the host decides how requests reach them:

*   **Served on an endpoint runtime.**  The runtime dispatches broker-routed
    ``request`` frames to the plugin on its work loop and packs the return value
    into a ``response`` frame — no local HTTP port is ever opened.

*   **Hosted on the broker.**  The broker constructs the plugin on its own
    plugin-host loop/thread (via ``BrokerPluginHost``).  A broker-hosted plugin
    is handed extra seam objects on ``app.state``: ``broker_caller`` (a
    :class:`~radical.orbit.broker.BrokerCaller` for calling *other* participants
    from inside the plugin) and ``broker_tap`` (the raw event stream, used by the
    ``replay`` plugin).  ``app.state.is_broker`` is ``True`` there.

The same handler runs unchanged either way because ``add_route_post`` /
``add_route_get`` perform **dual registration**: they add a compiled
direct-dispatch entry to ``app.state.direct_routes`` (matched on the runtime /
host work loop) **and** an ASGI route on the FastAPI app (used by the gateway's
HTTP proxy and by ``TestClient`` in unit tests).

Use :meth:`~radical.orbit.plugin_base.Plugin.is_enabled` to gate where a plugin
loads — for example, ``globus`` disables itself on the broker, and ``replay``
loads only on the broker and only when the event tap is present.

Base Classes
============

The plugin system provides three base classes:

1. **Plugin** (``plugin_base.py``) — the served/hosted plugin.

   - Manages sessions, routes, and notifications
   - Auto-registers via the ``plugin_name`` class attribute
   - Provides ``add_route_post()`` / ``add_route_get()`` helpers
   - Forwards requests to sessions via ``_forward()``

2. **PluginSession** (``plugin_session_base.py``) — per-client session state.

   - Created when a caller invokes ``register_session``
   - Holds domain-specific state (jobs, tasks, connections)
   - Sends notifications via ``self._plugin._dispatch_notify(topic, data)``

3. **PluginClient** (``client.py``) — application-side helper.

   - Speaks a small ``httpx``-shaped transport surface, so it is
     transport-agnostic: the endpoint runtime rides it over the WebSocket by
     swapping in a transport shim (``RuntimePluginClient``); ``TestClient`` tests
     ride it over HTTP
   - Manages session registration and lifecycle
   - Optional: only needed for Python consumer access

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

            # Send a real-time notification to subscribed consumers
            if self._plugin:
                self._plugin._dispatch_notify("work_status", {
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
- Use ``self._check_active()`` to validate the session is open
- Emit notifications with ``self._plugin._dispatch_notify(topic, data)``
  (thread-safe; works from sync/async contexts and background threads)
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

        plugin_name   = "myservice"     # namespace + registry key
        session_class = MySession       # Required!
        version       = '0.1.0'

        def __init__(self, app: FastAPI, instance_name: str = "myservice"):
            super().__init__(app, instance_name)

            # Add plugin-specific routes
            self.add_route_post('do_work/{sid}', self.do_work)

        async def do_work(self, request: Request) -> JSONResponse:
            """Route handler — forwards to a session method."""
            sid  = request.path_params['sid']
            data = await request.json()
            return await self._forward(sid, MySession.do_work,
                                       param=data['param'])

**Key Points:**

- Set ``plugin_name`` for auto-registration and the namespace
- Give ``instance_name`` a default so the host can construct the plugin as
  ``PluginMyService(app=...)``
- Set ``session_class`` to your session class
- Use ``self.add_route_post()`` / ``self.add_route_get()`` for routes
- Use ``self._forward(sid, method, **kwargs)`` to dispatch to sessions;
  ``_forward`` handles session lookup, expiry, error wrapping, and the response

Auto-Registered Routes
-----------------------

Every plugin automatically gets these routes under its namespace:

- ``POST /{instance}/register_session`` — Create or reconnect to a session
- ``POST /{instance}/unregister_session/{sid}`` — Close a session
- ``GET  /{instance}/version`` — Plugin version
- ``GET  /{instance}/list_sessions`` — List active sessions
- ``GET  /{instance}/health`` — Health check
- ``GET  /{instance}/ui_config`` — UI configuration for the Explorer

Step 3: Define Your Client Class (Optional)
--------------------------------------------

For Python consumer access, create a client class:

.. code-block:: python

    from radical.orbit.client import PluginClient

    class MyServiceClient(PluginClient):
        """Consumer-side helper for the MyService plugin."""

        def do_work(self, param: str) -> dict:
            """Call do_work on the target participant."""
            self._require_session()
            resp = self._request("POST", self._url(f"do_work/{self.sid}"),
                                  json={"param": param})
            self._raise(resp, f"do_work({param!r})")
            return resp.json()

Register the helper on the plugin (``client_class = MyServiceClient``) so that
:meth:`~radical.orbit.EndpointRuntime.get_plugin` returns it.

**Key Points:**

- ``self.sid`` is set after ``register_session()``
- ``self._url(path)`` builds the namespaced path
- ``self._request(method, url, **kwargs)`` is the single transport seam; the
  runtime transparently routes it over the WebSocket
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

Sessions
========

Lifetime policies
-----------------

``register_session`` accepts a JSON body (all fields optional) selecting a
lifetime policy and, for reconnect, a client-supplied session id::

    {"sid": <str>, "lifetime": "ephemeral"|"ttl"|"persistent", "ttl": <seconds>}

- ``sid`` omitted → a fresh session id is minted.
- ``sid`` names an existing session → *reconnect* (its last-access is bumped),
  subject to the owner and policy checks below.
- ``sid`` names no session → a session is created under exactly that id.

The three lifetimes:

- ``ephemeral`` (default) — an owner-bound ephemeral session is reclaimed a
  reclaim-drain grace after its owner is declared ``lost`` (see
  `Topology and liveness`_); an owner-less one falls back to the plugin idle
  timeout (``session_ttl``, default 1 hour) so it is not leaked.
- ``ttl`` — expires once ``now - last_access`` exceeds ``ttl`` seconds (requires
  a positive ``ttl``); never touched by owner loss.
- ``persistent`` — never expires; reclaimed only by explicit operator action.

The reserved ``default`` session id is always persistent, shared, and per plugin
*instance* (a ``rhapsody`` default and a ``psij`` default are distinct); it is
created on demand and records owner ``None``.

Owner binding
-------------

The serving runtime injects the broker-stamped participant identity as the
trusted ``x-orbit-src`` request header (overwriting any client-supplied copy).
``register_session`` records it as the session ``owner``.  Reattach is
**owner-checked**: reconnecting to a session whose recorded owner is not ``None``
and differs from the caller is rejected with **HTTP 403** — recovery goes
through re-registering the same participant name.  Requests arriving through the
broker gateway carry no participant identity, so their sessions are owner-less
(``None``) and stay capability-style within the token trust domain.

Status codes: **403** on a cross-owner reattach, **409** on an incoherent or
conflicting lifetime/ttl, **410** on a session that has passed its TTL, **404**
on an unknown session id.

Session teardown
----------------

When ``close()`` is called on a ``PluginSession``:

- The session releases all resources (threads, backend connections, file
  handles) and cancels any background polling/watchers
- The base ``super().close()`` marks the session inactive

Notifications
=============

Plugins push real-time notifications that become broker ``event`` frames.
The flow is: **Session → Plugin → participant runtime → Broker → subscribed
consumers** (and, for HTTP clients, the gateway's SSE ``/events`` fan-out).  The
broker stamps each event with an authoritative ``seq``/``ts`` on ingest.

From a session:

.. code-block:: python

    # In your PluginSession subclass method:
    if self._plugin:
        self._plugin._dispatch_notify("job_status", {
            "job_id": "abc123",
            "state":  "RUNNING"
        })

From a plugin (async context):

.. code-block:: python

    await self.send_notification("my_topic", {"key": "value"})

Consumers subscribe by registering callbacks on the runtime
(``rt.register_callback(...)``, optionally ``with_meta=True`` to receive the
broker ``seq``/``ts``); browsers subscribe over SSE at ``/events``.  See the
project ``CLAUDE.md`` for subscription examples in Python and JavaScript.

Topology and liveness
=====================

The serving runtime (and the broker's host) deliver the **rich topology** on
every change.  Override ``on_topology_change`` to react to
connect / disconnect / liveness transitions:

.. code-block:: python

    class PluginMyService(Plugin):
        async def on_topology_change(self, participants: dict):
            for name, info in participants.items():
                print(f"{name}: {info.get('liveness')}")
            await super().on_topology_change(participants)  # keep reclaim-drain

``participants`` maps each participant name to its rich info::

    {"endpoint1": {"role":     "endpoint",
                   "plugins":  {"sysinfo": {...}, "psij": {...}},
                   "liveness": "present"}}

``liveness`` is one of ``present`` / ``suspect`` / ``lost``.  The wire carries no
tombstone — the broker broadcasts ``suspect`` on a socket drop and simply drops
the participant once its grace elapses — so the host **synthesizes** a
``liveness='lost'`` entry for a participant that vanishes after being seen, and
delivers it exactly once.  The base ``on_topology_change`` uses this to drive
owner-bound ephemeral-session reclaim: a ``lost`` owner arms its reclaim-drain
timer; a ``present`` owner cancels it (a transient ``suspect`` never reclaims).
Overriders that want that behavior call ``super()``.

Plugin Shutdown
===============

Override ``shutdown`` for orderly teardown on host shutdown.  The base
implementation cancels the background session-cleanup task and any pending
reclaim-drain timers (awaiting each so no task is destroyed while pending) and
then closes every open session.  A plugin that spawns extra background tasks
cancels them first, then calls ``super().shutdown()``:

.. code-block:: python

    class PluginMyService(Plugin):
        async def shutdown(self) -> None:
            if self._poller is not None and not self._poller.done():
                self._poller.cancel()
                try:    await self._poller
                except asyncio.CancelledError:
                    pass
            await super().shutdown()

Async / Sync Guidelines
=======================

All plugin route handlers **must** be ``async def``.  Blocking operations (file
I/O, subprocess calls, network requests) must be offloaded to a thread pool with
``asyncio.to_thread`` so a handler never stalls the work loop::

    async def my_handler(self, param: str) -> dict:
        result = await asyncio.to_thread(subprocess.check_output, ['cmd', param])
        return {'output': result.decode()}

Callbacks from external libraries (PsiJ status callbacks, Rhapsody backend
callbacks) run on background threads, not the loop.  Emit notifications from
them with ``self._plugin._dispatch_notify(topic, data)`` — it is thread-safe and
schedules the send on the main loop automatically.

Testing Your Plugin
====================

Because ``add_route_post`` / ``add_route_get`` also register ASGI routes, a
plugin can be exercised over HTTP with Starlette's ``TestClient``:

.. code-block:: python

    import pytest
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    def test_my_plugin():
        app    = FastAPI()
        plugin = PluginMyService(app)
        client = TestClient(app)

        # Register a session
        resp = client.post(f"{plugin.namespace}/register_session")
        assert resp.status_code == 200
        sid = resp.json()['sid']

        # Call the plugin endpoint
        resp = client.post(
            f"{plugin.namespace}/do_work/{sid}",
            json={"param": "test"}
        )
        assert resp.status_code == 200
        assert resp.json()['result'] == "processed: test"

UI Configuration
================

Plugins can provide a ``ui_config`` dict that the Explorer UI uses to render
forms, monitors, and notification subscriptions automatically:

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

Alternatively, plugins can provide a custom JS module by setting ``ui_module`` to
the path of a ``.js`` file.  See the next section for the JS Module API.

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
    Fetch relative to the current plugin namespace on the broker.
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

``api.brokerUrl``
    Full URL of the broker (e.g. ``"https://broker:8000"``).

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

Summary
=======

To create a new plugin:

1. Create a session class inheriting from ``PluginSession``
2. Create a plugin class inheriting from ``Plugin``
3. Set ``plugin_name`` and ``session_class`` (and a default ``instance_name``)
4. Add routes in ``__init__`` using ``add_route_post`` / ``add_route_get``
5. Optionally create a ``PluginClient`` subclass and set ``client_class``
6. Optionally provide ``ui_config`` (or ``ui_module``) for the Explorer UI

See the existing plugins (``plugin_sysinfo.py``, ``plugin_psij.py``,
``plugin_rhapsody.py``) for real-world examples.
