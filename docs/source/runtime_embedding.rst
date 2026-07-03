
Embedding the Participant Runtime
*********************************

:class:`radical.orbit.EndpointRuntime` (exported as ``Endpoint``) is a library.
You can embed it directly into a Python application to join the star as a
participant — serving plugins, consuming other participants' plugins, or both —
without managing a separate process or opening any local network ports.  A
dedicated transport thread owns the outbound WebSocket to the broker; plugin
work and user callbacks run on separate threads, so embedding the runtime never
couples your application's event loop to liveness.

Architecture
============

One :class:`~radical.orbit.EndpointRuntime` dials the broker over a **single
outbound WebSocket** and:

*   **serves** any plugins it mounts, dispatching broker-routed requests to them
    on its own work loop (no local TCP port is ever opened); and/or
*   **consumes** other participants' plugins via ``get_plugin`` / ``call``.

Zero served plugins ⇒ a pure **consumer** (the Python SDK).  The combined case —
serving *and* consuming from inside one process — is the workflow-manager-in-an-
allocation the star is built for: a runtime inside a batch job both offers its
own plugins and drives plugins on other participants.

Lifecycle
=========

Start / stop
------------

:meth:`~radical.orbit.EndpointRuntime.start` brings up the loops and connects;
:meth:`~radical.orbit.EndpointRuntime.stop` cleanly closes the WebSocket (the
broker treats a clean close as immediate removal) and tears everything down.

.. code-block:: python

   import time
   from radical.orbit import EndpointRuntime, PluginXGFabric

   def main():
       rt = EndpointRuntime(name='xgfabric-endpoint')
       rt.serve(PluginXGFabric)          # mount a served plugin (before start)

       rt.start(wait=True)               # connect; block until registered
       try:
           while True:                   # your application logic
               time.sleep(1)
       except KeyboardInterrupt:
           rt.stop()

   if __name__ == "__main__":
       main()

With ``wait=True`` (the default) :meth:`start` blocks until the first
``register_ack`` **and** the first topology frame land, so on return
:meth:`~radical.orbit.EndpointRuntime.topology` already reflects the broker's
snapshot and an immediate :meth:`~radical.orbit.EndpointRuntime.get_plugin` will
not race a not-yet-populated topology.

Context manager
---------------

The runtime is also a context manager — ``__enter__`` starts it (``wait=True``),
``__exit__`` stops it:

.. code-block:: python

   from radical.orbit import EndpointRuntime

   with EndpointRuntime(name='consumer-1') as rt:
       eids = [n for n in rt.topology() if n != 'broker']
       si   = rt.get_plugin(eids[0], 'sysinfo')
       print(si.get_metrics())
   # WebSocket closed, threads joined on exit

Serving plugins
===============

Mount plugins to serve **before** connecting, in either of two ways:

*   Pass a filter list to the constructor — names, ``'all'``, or ``'default'`` —
    to load registered plugins by name::

       rt = EndpointRuntime(plugins=['sysinfo', 'psij'])

*   Call :meth:`~radical.orbit.EndpointRuntime.serve` with a plugin class or
    instance (it returns the mounted instance)::

       from radical.orbit import PluginPSIJ
       rt = EndpointRuntime()
       rt.serve(PluginPSIJ)

Served plugins register with the broker in the connect handshake; their
namespaces are **endpoint-relative** (``/{instance}``), and the broker routes to
them by ``dst``.  A runtime that serves at least one plugin advertises the
``endpoint`` role; a zero-plugin runtime advertises ``consumer`` (override with
``role=``).

Zero-plugin consumers
=====================

A runtime with no served plugins is a pure consumer.  It reaches any other
participant's plugin with :meth:`~radical.orbit.EndpointRuntime.get_plugin`
(which discovers the namespace from the topology and returns a plugin
``client_class`` helper) or with the low-level
:meth:`~radical.orbit.EndpointRuntime.call`:

.. code-block:: python

   from radical.orbit import EndpointRuntime

   rt = EndpointRuntime()
   rt.start(wait=True)

   psij = rt.get_plugin('my-endpoint', 'psij')      # registers a session
   psij.submit_job({'executable': '/bin/sleep', 'arguments': ['5']})

   rt.stop()

Naming and session recovery
===========================

A runtime given no ``name`` is auto-named ``consumer.<uuid8>`` — fine for
fire-and-forget consumers, but such a name cannot recover sessions on restart.
Session reattach is **owner-checked**, so it needs a **stable, client-supplied**
name: restart the runtime with the *same* ``name`` and its sessions come back.
A ``name-in-use`` registration is retried with capped exponential backoff until
the stale registration passes ``lost`` — a crashed predecessor holds no resume
key and, under the fast keepalive, the name frees within seconds — so "restart
with the same name → sessions return" is automatic from the application's view.

.. code-block:: python

   rt = EndpointRuntime(name='my-workflow')   # stable name → recoverable sessions

The ``radical-orbit-endpoint`` entry point follows this: a serving endpoint
defaults its name to the hostname rather than an auto-generated one.

Configuration
=============

The runtime resolves its broker URL, TLS cert, and ingress token with the
precedence **constructor argument > environment variable > file under
`~/.radical/orbit/`**:

*   ``RADICAL_ORBIT_BROKER_URL`` — the broker URL (``https``/``wss`` require a
    cert).
*   ``RADICAL_ORBIT_BROKER_CERT`` — the broker TLS cert.
*   ``RADICAL_ORBIT_BROKER_TOKEN`` — the shared ingress token.

Log level follows ``RADICAL_ORBIT_LOG_LVL`` (or the generic ``RADICAL_LOG_LVL``).

API Reference
=============

.. autoclass:: radical.orbit.runtime.EndpointRuntime
   :members: start, stop, serve, get_plugin, call, register_callback,
             unregister_callback, register_topology_callback, send_notification,
             topology, name, resume_key, broker_url, wait_registered
   :undoc-members:
   :show-inheritance:

Developing external plugins
===========================

Custom plugins live in your own modules.  A subclass of
:class:`radical.orbit.Plugin` that defines a ``plugin_name`` class attribute is
auto-registered in the global plugin registry on import; you then either name it
in the constructor ``plugins=`` filter or hand the class to
:meth:`~radical.orbit.EndpointRuntime.serve`.  Because
:meth:`~radical.orbit.EndpointRuntime.serve` constructs the plugin as
``PluginClass(app=...)``, the plugin's ``__init__`` must give ``instance_name`` a
default.

Example: Weather plugin
-----------------------

**1. Define the plugin**

.. code-block:: python

   # file: my_project/plugins/weather.py

   from fastapi              import FastAPI, Request
   from starlette.responses  import JSONResponse

   from radical.orbit                     import Plugin
   from radical.orbit.plugin_session_base import PluginSession

   class WeatherPlugin(Plugin):
       """A plugin that provides weather data."""

       plugin_name   = "my_org.weather"   # registry key + get_plugin lookup
       session_class = PluginSession
       version       = "0.1.0"

       def __init__(self, app: FastAPI, instance_name: str = "weather"):
           # instance_name drives the endpoint-relative namespace (/weather)
           super().__init__(app, instance_name)

           self.add_route_get("forecast", self.get_forecast)
           self.add_route_get("current",  self.get_current)

       async def get_forecast(self, request: Request) -> JSONResponse:
           return JSONResponse({"forecast": "sunny", "temp": 72})

       async def get_current(self, request: Request) -> JSONResponse:
           return JSONResponse({"temp": 68, "humidity": 45})

**2. Serve the plugin**

Importing the module registers the class; hand it to
:meth:`~radical.orbit.EndpointRuntime.serve`.

.. code-block:: python

   # file: app.py

   from radical.orbit import EndpointRuntime
   from my_project.plugins.weather import WeatherPlugin

   rt = EndpointRuntime(name="weather-endpoint")
   rt.serve(WeatherPlugin)
   rt.start(wait=True)

Serving the PSIJ plugin
=======================

:class:`~radical.orbit.PluginPSIJ` submits and manages jobs via the
`psij-python <https://exaworks.org/psij-python/>`_ library, giving a unified API
over Slurm, PBS, LSF, and the local executor.  Install ``psij-python`` on the
serving host, then mount the plugin:

.. code-block:: python

   from radical.orbit import EndpointRuntime, PluginPSIJ

   rt = EndpointRuntime(name="hpc-endpoint")
   rt.serve(PluginPSIJ)
   rt.start(wait=True)

A consumer submits a job by reaching the plugin over the star:

.. code-block:: python

   from radical.orbit import EndpointRuntime

   rt   = EndpointRuntime()
   rt.start(wait=True)
   psij = rt.get_plugin("hpc-endpoint", "psij")

   job_spec = {
       "executable": "/bin/sleep",
       "arguments":  ["10"],
       "attributes": {"queue_name": "debug",
                      "account":    "my_project",
                      "duration":   "600"},
   }
   result = psij.submit_job(job_spec, executor="slurm")
   print(psij.get_job_status(result["job_id"]))

   rt.stop()

See :doc:`rest_api` for the underlying request/response shapes and
:doc:`plugin_development` for the full plugin authoring model.
