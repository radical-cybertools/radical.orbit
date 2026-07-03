
REST API Reference
******************

The broker's **gateway** module serves an HTTP/SSE compatibility surface for
non-participant callers — browsers, ``curl``, the Explorer UI.  It is the compat
tier: broker-native participants speak the WebSocket envelope protocol instead
(see :doc:`module_radical.orbit`).  The gateway is on by default; a broker
started with ``--no-gateway`` exposes only the WebSocket ``/register`` ingress.

All gateway routes are reachable at ``http(s)://<broker_host>:<port>/``.  Plugin
routes are reached through the catch-all proxy under
``/<endpoint_name>/<plugin_name>/…``: the gateway maps that URL onto the star
model ``(dst=<endpoint_name>, path=<remainder>)`` and routes the request over
the broker's WebSocket to that participant's plugins.

Authentication
==============

Every route except the UI shell (``GET /``) and the static plugin assets
(``GET /plugins/*``) requires the shared broker token, sent either as an
``Authorization: Bearer <token>`` header or — for the browser — as the HttpOnly
cookie minted by ``POST /auth``.  A missing or invalid token yields **401**.
Authentication can be disabled for local development (``--no-auth``), in which
case the gate is inert.

Gateway Endpoints
=================

.. list-table::
   :header-rows: 1
   :widths: 10 34 56

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``/``
     - Explorer UI (HTML). Ungated so the browser can prompt for the token.
   * - ``POST``
     - ``/auth``
     - Validate the bearer token and set the HttpOnly auth cookie (used by the
       Explorer / SSE). Reached only with a valid bearer header.
   * - ``POST``
     - ``/endpoint/list``
     - Discovery. Returns ``{"data": {"broker": {"url": …}, "endpoints":
       {name: {"endpoint": {role, liveness}, "plugins": {pname: {namespace,
       …}}}}}}``. Namespaces are the full ``/{endpoint}/{plugin}`` form.
   * - ``GET``
     - ``/endpoints``
     - Flat listing. Returns ``{"endpoints": [{name, plugins, connected,
       plugin_count}], "total": N}``.
   * - ``GET``
     - ``/events``
     - SSE stream for real-time notifications and topology changes (see below).
   * - ``POST``
     - ``/endpoint/disconnect/{endpoint_name}``
     - Gracefully disconnect and terminate an endpoint. **404** if not
       connected; **400** for the reserved ``broker`` name.
   * - ``POST``
     - ``/broker/terminate``
     - Terminate the broker process (endpoints keep running).
   * - ``GET``
     - ``/plugins/{filename}``
     - Serve a JS plugin module file (used by the Explorer). Ungated.
   * - *any*
     - ``/{endpoint_name}/{path}``
     - Catch-all proxy to a plugin on the named participant. Methods: ``GET``,
       ``POST``, ``PUT``, ``PATCH``, ``DELETE``, ``OPTIONS``, ``HEAD``.

Proxy semantics
---------------

The catch-all proxy forwards the request over the broker's routing table and
waits up to a **long deadline** (600 s — a large submit batch whose backend
setup takes seconds per task genuinely runs this long) for the participant's
response.  Status mapping:

- **404** — the target participant (``endpoint_name``) is unknown / not
  connected.
- **503** — the broker's pending-call table is at capacity (too many in-flight
  calls).
- **504** — the participant did not respond within the deadline.

The shared token and hop-by-hop headers are stripped before a request is
forwarded, so the broker credential never rides on to plugins.  A request whose
``endpoint_name`` is ``broker`` is routed into the broker's own hosted-plugin
host instead of the routing-loop registry.

SSE Event Format
================

The ``/events`` stream sends JSON-encoded frames.  The broker sends the current
topology as the first frame on connect.  A per-client queue is bounded and
drop-oldest, so a stalled reader can never backpressure the broker.

Notification frame::

    data: {"topic": "notification", "data": {
        "endpoint": "my_endpoint",
        "plugin":   "psij",
        "topic":    "job_status",
        "data":     { ... plugin-specific ... }
    }}

Topology frame (the same ``{broker, endpoints}`` shape ``/endpoint/list``
returns)::

    data: {"topic": "topology", "data": {
        "broker":    {"url": "https://broker:8000"},
        "endpoints": {"my_endpoint": {
            "endpoint": {"role": "endpoint", "liveness": "present"},
            "plugins":  {"sysinfo": {"namespace": "/my_endpoint/sysinfo", ...}}
        }}
    }}

Plugin Base Routes
==================

Every plugin automatically registers these routes under its namespace
(``/<endpoint_name>/<plugin_name>/`` through the proxy):

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``register_session``
     - Create or reconnect to a session. Body (optional):
       ``{sid, lifetime, ttl}``. Returns ``{"sid": "<session_id>"}``.
   * - ``POST``
     - ``unregister_session/{sid}``
     - Close and remove a session. Returns ``{"ok": true}``
   * - ``GET``
     - ``version``
     - Plugin version. Returns ``{"version": "x.y.z"}``
   * - ``GET``
     - ``list_sessions``
     - Active session IDs. Returns ``{"sessions": [...]}``
   * - ``GET``
     - ``health``
     - Health check. Returns status, uptime, active session count
   * - ``GET``
     - ``ui_config``
     - UI configuration for the Explorer. Returns plugin name, version, ``ui``


PsiJ Plugin
===========

Namespace: ``psij``

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``submit/{sid}``
     - Submit a job. Body: ``{"job_spec": {...}, "executor": "slurm"}``
   * - ``GET``
     - ``status/{sid}/{job_id}``
     - Job status and output. Query params: ``stdout_offset``, ``stderr_offset`` for streaming
   * - ``GET``
     - ``list_jobs/{sid}``
     - All jobs in the session. Returns ``{"jobs": [...]}``
   * - ``POST``
     - ``cancel/{sid}/{job_id}``
     - Cancel a job. Returns ``{"ok": true}``

``submit`` request body::

    {
        "executor": "slurm",
        "job_spec": {
            "executable": "/path/to/bin",
            "arguments":  ["--arg", "val"],
            "attributes": {
                "queue_name": "debug",
                "account":    "myproject",
                "duration":   600,
                "node_count": 2
            }
        }
    }

``status`` response::

    {
        "job_id":      "job.abc123",
        "native_id":   "12345",
        "state":       "COMPLETED",
        "exit_code":   0,
        "executable":  "/path/to/bin",
        "arguments":   ["--arg", "val"],
        "executor":    "slurm",
        "stdout":      "...",
        "stderr":      "...",
        "stdout_offset": 1024,
        "stderr_offset": 0
    }


Rhapsody Plugin
===============

Namespace: ``rhapsody``

``register_session`` accepts an optional body: ``{"backends": ["local", "dragon_v3"]}``

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``submit/{sid}``
     - Submit tasks. Body: ``{"tasks": [{...}, ...]}``
   * - ``POST``
     - ``wait/{sid}``
     - Wait for tasks. Body: ``{"uids": [...], "timeout": 60}``
   * - ``GET``
     - ``list_tasks/{sid}``
     - All tasks in session
   * - ``GET``
     - ``task/{sid}/{uid}``
     - Task details including stdout, stderr, exception
   * - ``POST``
     - ``cancel/{sid}/{uid}``
     - Cancel a task
   * - ``GET``
     - ``statistics/{sid}``
     - Backend execution statistics


Queue Info Plugin
=================

Namespace: ``queue_info``

``is_enabled`` and ``job_allocation`` are session-less and return immediately
without requiring a session.

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``is_enabled``
     - Returns ``{"available": true/false}`` — whether SLURM is present
   * - ``GET``
     - ``job_allocation``
     - Returns current job allocation of the **endpoint** process (see below)
   * - ``GET``
     - ``get_info/{sid}``
     - Partition and allocation info
   * - ``GET``
     - ``list_jobs/{sid}/{queue}``
     - Jobs in a specific queue/partition
   * - ``GET``
     - ``list_all_jobs/{sid}``
     - All jobs visible to the current user
   * - ``GET``
     - ``list_allocations/{sid}``
     - Active allocations
   * - ``POST``
     - ``cancel/{sid}/{job_id}``
     - Cancel a queued or running job

``job_allocation`` response::

    # Endpoint running on a login node (no SLURM job):
    {"allocation": null}

    # Endpoint running inside a SLURM job allocation:
    {"allocation": {"n_nodes": 4, "runtime": 3600}}

    # Endpoint running inside a SLURM job with unlimited walltime:
    {"allocation": {"n_nodes": 4, "runtime": null}}

``n_nodes`` is the number of nodes in the allocation; ``runtime`` is the
walltime limit in seconds (``null`` for UNLIMITED).  A 500 response is
returned if ``SLURM_JOB_ID`` is set but allocation details cannot be
determined (missing env vars, ``squeue`` failure or timeout).


Sysinfo Plugin
==============

Namespace: ``sysinfo``

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``homedir``
     - Home directory path. Returns ``{"homedir": "/home/user"}``
   * - ``GET``
     - ``metrics/{sid}``
     - System metrics (CPU, memory, disk, GPUs, network, filesystems)


Staging Plugin
==============

Namespace: ``staging``

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``put/{sid}``
     - Upload a file to the endpoint. Body: ``{"src": "/local/path", "tgt": "/remote/path"}``
   * - ``POST``
     - ``get/{sid}``
     - Download a file from the endpoint. Body: ``{"src": "/remote/path", "tgt": "/local/path"}``
   * - ``GET``
     - ``list/{sid}``
     - List files in the session staging area


XGFabric Plugin
===============

Namespace: ``xgfabric``

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``workdir/{sid}``
     - Get current config directory
   * - ``POST``
     - ``workdir/{sid}``
     - Set config directory. Body: ``{"path": "/path/to/configs"}``
   * - ``GET``
     - ``configs/{sid}``
     - List saved configurations
   * - ``GET``
     - ``config/{sid}/default``
     - Load the built-in default workflow config
   * - ``GET``
     - ``config/{sid}/test``
     - Load the built-in test workflow config (stub tasks)
   * - ``GET``
     - ``config/{sid}/{name}``
     - Load a named config from disk
   * - ``POST``
     - ``config/{sid}``
     - Save a configuration. Body: workflow config dict with ``"name"`` field
   * - ``POST``
     - ``config/{sid}/{name}/delete``
     - Delete a saved configuration
   * - ``GET``
     - ``status/{sid}``
     - Current workflow state (status, phase, cluster lists, progress)
   * - ``POST``
     - ``start/{sid}``
     - Start workflow. Body: ``{"workflow": "default", "resource": "default"}``
   * - ``POST``
     - ``stop/{sid}``
     - Cancel a running workflow


Error Responses
===============

All plugin endpoints return standard HTTP status codes:

- ``200`` — Success
- ``400`` — Bad request (missing/invalid parameters)
- ``403`` — Session owned by another participant (cross-owner reattach)
- ``404`` — Session, resource, or endpoint not found
- ``409`` — Conflict (e.g. incoherent/conflicting session lifetime policy)
- ``410`` — Session expired (TTL exceeded)
- ``500`` — Internal server error
- ``503`` — Broker at concurrency cap (too many in-flight calls)
- ``504`` — Upstream (participant) timeout

Error body format::

    {"detail": "human-readable error message"}
