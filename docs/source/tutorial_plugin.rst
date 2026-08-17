
Plugin Writer's Tutorial
************************

This tutorial walks you through writing a complete ORBIT plugin from scratch:
the server-side plugin and session classes, the Python client helper, unit
tests, and the configuration that controls *where* the plugin loads (broker,
login node, compute node).  It is a hands-on companion to the
:doc:`plugin_development` reference — every concept used here is described in
depth there.

The example is deliberately trivial: a **math** plugin that serves the four
basic arithmetic operations (``add`` / ``sub`` / ``mul`` / ``div``) and keeps a
per-session history of the operations it performed.  Because the domain logic
is one line of arithmetic, every remaining line is ORBIT machinery — which is
exactly what this tutorial is about.

The finished plugin ships with ORBIT as ``src/radical/orbit/plugin_math.py``,
so you can run every step of this tutorial verbatim.  All code shown below is
included from that file — it cannot drift from what you install.

Prerequisites: a working local setup as described in
:doc:`getting_started` — a broker and an endpoint you can start in two
terminals.


What you will build
===================

Three classes, one file:

===================  =====================  ==========================================
Class                Runs on                Role
===================  =====================  ==========================================
``MathSession``      serving participant    per-client state + the actual work
``PluginMath``       serving participant    routes, validation, session dispatch
``MathClient``       consuming application  friendly Python API over the wire calls
===================  =====================  ==========================================

At runtime the pieces connect like this::

    application                     broker                endpoint
    -----------                     ------                --------
    MathClient.add(3, 4)
      -> POST /math/add/{sid}  -->  routes by dst  -->  PluginMath.op_add()
                                                          -> _forward(sid, ...)
                                                            -> MathSession.compute()
      <- {"result": 7.0}       <--  correlates     <--  response
                                    by corr_id

The consuming application never opens a connection to the endpoint — both
sides dial the broker, which routes ``request`` / ``response`` frames between
them (see the architecture overview in the project README / ``CLAUDE.md``).


Step 1 — the session
====================

A **session** holds per-client state on the serving side.  For ``psij`` that
is the submitted jobs; for ``rhapsody`` the running tasks; for our tutorial
plugin it is simply the list of operations performed.  Sessions inherit from
:class:`~radical.orbit.plugin_session_base.PluginSession`:

.. literalinclude:: ../../src/radical/orbit/plugin_math.py
   :pyobject: MathSession

Things to note:

* ``super().__init__(sid)`` initializes the base bookkeeping (session id,
  active flag, notification path).
* Session methods are **async** and start with ``self._check_active()`` so a
  call on a closed session fails cleanly with a ``RuntimeError``.
* ``self.notify(topic, data)`` pushes a real-time **notification** to
  subscribed consumers.  It is safe from sync/async contexts and background
  threads, and it is a no-op when the session has no parent plugin (e.g. when
  you instantiate it directly in a unit test).
* ``close()`` releases per-session resources, then calls ``super().close()``
  which marks the session inactive.  Real plugins cancel background pollers
  and close backend connections here.

Session *lifetime* (when an abandoned session is reclaimed) is a policy the
caller selects at registration time — you do not implement anything for it.
See the *Lifetime policies* section in :doc:`plugin_development`.


Step 2 — the plugin
===================

The **plugin** class owns the URL namespace, registers routes, validates
input, and dispatches to sessions.  It inherits from
:class:`~radical.orbit.plugin_base.Plugin`:

.. literalinclude:: ../../src/radical/orbit/plugin_math.py
   :pyobject: PluginMath

Walking through it:

Class attributes
----------------

* ``plugin_name = 'math'`` — the registry key **and** the default URL
  namespace.  Defining it auto-registers the class in the global plugin
  registry the moment the module is imported; that is all "installation"
  means on the code level.
* ``session_class = MathSession`` — what ``register_session`` instantiates.
* ``client_class = MathClient`` — what consumers get from
  ``runtime.get_plugin(endpoint, 'math')`` (Step 3).
* ``ui_config`` — a minimal descriptor so the plugin shows up in the broker's
  Explorer UI with an icon and description.  Forms and monitors can be added
  later; see :doc:`plugin_development`.

Routes
------

``add_route_post`` / ``add_route_get`` register a handler under the plugin
namespace, so ``'add/{sid}'`` becomes ``POST /math/add/{sid}``.  Every plugin
additionally gets the built-in routes for free — ``register_session``,
``unregister_session/{sid}``, ``version``, ``list_sessions``, ``health``, and
``ui_config``.

Handlers are plain ``async`` callables that take a ``Request`` and return a
dict.  The registration is **dual**: the same handler serves broker-routed
``request`` frames on the endpoint's work loop *and* plain HTTP (used by the
gateway proxy and by ``TestClient`` in unit tests) — you never think about
the transport.

Input validation and errors
---------------------------

``_compute`` shows the error convention.  ORBIT uses one canonical error
envelope on the wire — ``{"error": true, "status_code": <int>, "detail":
"<str>"}`` — and ``radical.orbit.errors`` maps stdlib exception types to HTTP
statuses (``ValueError`` → 400, ``FileNotFoundError`` → 404,
``PermissionError`` → 403, …).  Raising ``http_exception(ValueError("division
by zero"))`` in the handler therefore produces a clean HTTP 400 with that
detail string.

Do this mapping **at the handler boundary**: an exception that escapes a
session method is caught by ``_forward`` and surfaces as a generic 500.

Session dispatch
----------------

``self._forward(sid, MathSession.compute, op=op, a=a, b=b)`` looks up the
session for ``sid``, checks it has not expired, bumps its last-access time,
and awaits the method.  Unknown session ids become 404, expired ones 410 —
you write none of that.

Blocking work
-------------

Our arithmetic is instant, but the rule matters for real plugins: handlers
and session methods run on the participant's **work loop**, so anything
blocking (subprocess calls, file I/O, third-party network requests) must be
offloaded::

    result = await asyncio.to_thread(subprocess.check_output, ['sinfo'])

A blocked work loop cannot break liveness (the transport thread owns the
keepalive), but it stalls every other plugin request on this participant.


Step 3 — the client
===================

The client helper is what application code sees.  It inherits from
:class:`~radical.orbit.client.PluginClient` and turns Python calls into the
wire requests defined in Step 2:

.. literalinclude:: ../../src/radical/orbit/plugin_math.py
   :pyobject: MathClient

The base class provides the whole transport:

* ``self._request(method, url, **kwargs)`` — the single transport seam.  The
  endpoint runtime injects a WebSocket-backed shim here, so the same helper
  code works over the broker star and, in tests, over plain HTTP.
* ``self._url(path)`` — prefixes the plugin namespace.
* ``self._raise(resp, context)`` — raises a ``RuntimeError`` carrying the
  HTTP status and the server's ``detail`` on any non-2xx response.  This is
  how the server-side 400 from ``div(1, 0)`` reaches the application.
* ``self.sid`` / ``self._require_session()`` — session bookkeeping;
  ``register_session()`` is called for you by ``runtime.get_plugin``.

Consuming the plugin then looks like this:

.. code-block:: python

    from radical.orbit import EndpointRuntime

    rt   = EndpointRuntime()          # join the star as a consumer
    rt.start(wait=True)

    math = rt.get_plugin('endpoint_1', 'math')   # MathClient, session ready
    print(math.add(3, 4))                        # -> 7.0

``get_plugin`` discovers the namespace from the live topology, instantiates
the ``client_class``, and registers a session — one call, ready to use.


Step 4 — registering the plugin
===============================

Auto-registration happens at **import time** (the ``plugin_name`` attribute
triggers it), so "registering" a plugin means making sure its module gets
imported.  There are two ways:

In-tree plugin
--------------

The tutorial plugin lives in the ORBIT source tree, so it is imported from
``src/radical/orbit/__init__.py`` alongside its siblings::

    from .plugin_staging import PluginStaging  # noqa: F401
    from .plugin_math    import PluginMath     # noqa: F401

Out-of-tree plugin (recommended for your own work)
--------------------------------------------------

You do not need to patch ORBIT.  Package your plugin as its own distribution
and declare a ``radical.orbit.plugins`` entry point; the plugin host discovers
and imports it at startup:

.. code-block:: toml

    # pyproject.toml of your plugin package
    [project]
    name = "orbit-plugin-math"
    version = "0.1.0"
    dependencies = ["radical.orbit"]

    [project.entry-points."radical.orbit.plugins"]
    math = "orbit_plugin_math.plugin_math"

The entry-point *value* is the module to import (importing it runs the
auto-registration); the *name* is informational.  After ``pip install`` of
your package, ``--plugins math`` works exactly as for a built-in plugin.


Step 5 — controlling where the plugin loads
===========================================

Two independent mechanisms decide whether a plugin is active on a given
participant: the operator-facing **plugin filter** and the code-facing
**is_enabled** gate.  A plugin loads only if it passes both.

The ``--plugins`` filter
------------------------

Both ``radical-orbit-endpoint.py`` and ``radical-orbit-broker.py`` accept
``--plugins`` (default: ``default``), a comma-separated list of tokens:

* exact names — ``--plugins math,sysinfo``
* fnmatch wildcards — ``--plugins 'iri*'``
* ``all`` — every registered plugin
* ``default`` — the **role-specific** default set

The ``default`` token expands against the host's detected role — ``broker``,
``login`` (a scheduler is present, not inside an allocation), ``compute``
(inside an allocation), or ``standalone`` (no scheduler) — using
``DEFAULT_PLUGINS_BY_ROLE`` in ``plugin_host_base.py``.  That is why a login
node gets ``psij`` by default while a compute node gets ``rhapsody``.

The math plugin is intentionally **not** in any default set; load it
explicitly on top of the defaults::

    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,math

The ``is_enabled`` gate
-----------------------

The filter is operator policy; ``is_enabled`` is *plugin* policy — a
classmethod checked **before** instantiation (no routes get registered when
it returns ``False``).  Use it when the plugin only makes sense in a specific
environment.  Real examples: ``globus`` disables itself on the broker,
``queue_info`` disables itself when no scheduler is detected.

The role machinery is available to your gate via
``radical.orbit.utils.host_role``.  To load a plugin **only on compute
nodes** (i.e. inside a batch allocation):

.. code-block:: python

    from radical.orbit.utils import host_role

    class PluginMyService(Plugin):
        plugin_name = 'myservice'
        ...

        @classmethod
        def is_enabled(cls, app: FastAPI) -> bool:
            """Load only inside a batch allocation (compute nodes)."""
            return host_role(app)['role'] == 'compute'

``host_role(app)`` returns ``role`` (``broker`` / ``login`` / ``compute`` /
``standalone``), the detected ``scheduler``, and — on compute nodes — the
current allocation's ``job_id``.  Other common gates:

.. code-block:: python

    # broker-only (e.g. plugins that orchestrate other participants):
    return bool(getattr(app.state, 'is_broker', False))

    # endpoint-only (never host on the broker):
    return not getattr(app.state, 'is_broker', False)

    # gate on an optional dependency:
    return HAS_GLOBUS_SDK

A skipped plugin is logged at INFO (``Skipping plugin (not applicable
here)``), so a "why is my plugin not loading?" question is answered by the
participant's log.

The tutorial plugin defines no ``is_enabled`` and therefore loads anywhere it
is asked for — handy for a tutorial, and the right default for most plugins.


Step 6 — trying it out
======================

Three terminals, as in :doc:`getting_started`:

.. code-block:: sh

    # Terminal 1 — broker
    ./bin/radical-orbit-broker.py

    # Terminal 2 — endpoint, defaults plus the math plugin
    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,math

    # Terminal 3 — consumer
    python examples/example_math.py

The consumer script exercises everything built above — the four operations,
the error path, the history, and the notifications:

.. literalinclude:: ../../examples/example_math.py
   :language: python

Expected output (endpoint name will differ)::

    add(3, 4): 7.0
      notification: endpoint_1/math result: add(3.0, 4.0) = 7.0
    sub(3, 4): -1.0
    ...
    div(1, 0) failed as expected: HTTP 400 — [endpoint_1/math] div(1, 0) —
    division by zero
    4 operations recorded in this session

You can also poke the plugin from a browser or ``curl`` through the broker's
gateway, which proxies HTTP onto the same routes
(``/{endpoint_name}/math/...``), and see it as a tile in the Explorer UI at
the broker's root URL.


Step 7 — notifications, closing the loop
========================================

Step 1 planted ``self.notify("result", {...})`` in the session; Step 6's
consumer receives it.  The full path is:

**Session → Plugin → participant runtime → Broker → subscribed consumers**
(and the gateway's SSE ``/events`` stream for browsers).

On the consumer side, subscription is one call on the client helper::

    math.register_notification_callback(on_result, topic='result')

with a callback signature of ``(endpoint, plugin, topic, data)``.  Callbacks
fire on the runtime's dedicated dispatcher thread — a slow callback never
affects the connection's liveness, but do not block in it needlessly.

Two rules of thumb:

* Notifications are **fire-and-forget** state broadcasts (job finished, task
  running); anything the caller must not miss belongs in a response.
* Pick stable topic names and payload keys — they are your plugin's public
  API just as much as the routes are (see the notification topics documented
  per-plugin in the project ``CLAUDE.md``).


Step 8 — unit tests
===================

Because route registration is dual (direct dispatch **and** ASGI), the whole
plugin can be tested over HTTP with Starlette's ``TestClient`` — no broker,
no endpoint, no sockets.  The tutorial plugin's tests live in
``tests/unittests/test_plugin_math.py``; the core round trip:

.. literalinclude:: ../../tests/unittests/test_plugin_math.py
   :pyobject: test_math_http_roundtrip

Session logic that does not need HTTP is tested even more directly — sessions
are plain objects:

.. literalinclude:: ../../tests/unittests/test_plugin_math.py
   :pyobject: test_math_session_notifies

Run them with::

    pytest tests/unittests/test_plugin_math.py -v

Test the error paths too — the typed statuses (400/404/410) are part of your
plugin's contract, and the client helper's exception behaviour depends on
them.


Where to go next
================

* :doc:`plugin_development` — the full reference: session lifetime policies,
  owner binding, topology callbacks, plugin shutdown, Explorer ``ui_config``
  forms, and the JS module API for custom Explorer pages.
* ``src/radical/orbit/plugin_staging.py`` — a small real plugin (adds path
  validation and richer error mapping).
* ``src/radical/orbit/plugin_psij.py`` / ``plugin_rhapsody.py`` — large
  plugins with background pollers, notification batching, and single-sourced
  ``ROUTE_*`` constants shared between routes and client helpers (a pattern
  worth copying once your route table grows).
* ``src/radical/orbit/plugin_queue_info.py`` — a worked ``is_enabled`` gate
  plus a backend-selection factory.
