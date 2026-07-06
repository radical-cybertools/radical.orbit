
#############################################
radical.orbit |version| documentation
#############################################

**ORBIT** is a broker-based distributed framework that connects external
RADICAL-Cybertools (RCT) applications with HPC resources.  It is a **star**:
one active **broker** hub routes between **endpoint** participants, each of
which dials the hub over a single outbound WebSocket and may *serve* plugins,
*consume* other participants' plugins, or both.  A broker-hosted **gateway**
module serves an HTTP/SSE/Explorer compatibility surface for non-participant
callers (browsers, ``curl``, the Explorer UI).

Control flows through the star; bulk data moves out of band (Globus, the shared
filesystem, SSH tunnels).

The three moving parts:

1. **Broker** (``radical-orbit-broker``) — the active hub.  A lean routing loop
   switches ``request``/``response`` frames between participants by ``src`` and
   ``dst``, correlates both directions via ``corr_id``, tracks topology and
   two-stage (``suspect`` → ``lost``) liveness, and fans ``event`` frames out to
   subscribers.  The broker is itself a participant (``src='broker'``) and can
   host plugins on their own loop/thread.  The ``gateway`` module (on by
   default) attaches the HTTP/SSE/Explorer tier onto the same port.

2. **Endpoint** (``radical-orbit-endpoint``) — a participant, typically on an
   HPC login or compute node.  A dedicated transport thread owns the outbound
   WebSocket and its keepalive (firewall-friendly, outbound-only); parse and
   plugin work run on a separate work loop; user callbacks fire on a third
   dispatcher thread — so a slow or blocking plugin never affects liveness.
   The same runtime, with zero served plugins, is a pure **consumer** — the
   Python SDK (:class:`radical.orbit.EndpointRuntime`, exported as ``Endpoint``).

3. **Plugins** — extend a participant (endpoint or broker) with domain-specific
   functionality (job submission, queue info, file staging, task execution,
   and more), each under its own endpoint-relative namespace.

These pages document the plugin API and development model, the participant
runtime and its embedding, the gateway's HTTP surface, and the individual
plugins shipped with the framework.

**Get involved or contact us:**

.. list-table::
   :widths: 5 20 75

   * - |Git|
     - **GitHub project:**
     - https://github.com/radical-cybertools/radical.orbit/
   * - |Goo|
     - **Mailing List:**
     - https://groups.google.com/forum/#!forum/radical.orbit-devel

.. |Git| image:: images/github.jpg
.. |Goo| image:: images/google.png


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 3

   getting_started.md
   module_radical.orbit.rst
   runtime_embedding.rst
   plugin_development.rst
   plugin_api.rst
   plugin_globus.rst
   plugin_queue_info.rst
   plugin_iri.rst
   machine_guide.rst
   rest_api.rst
   task_dispatcher_strategy.md


##################
Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
