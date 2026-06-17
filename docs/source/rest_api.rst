
REST API Reference
******************

This document lists all REST endpoints exposed by the ORBIT system.
All bridge endpoints are reachable at ``http(s)://<bridge_host>:<port>/``.
Plugin endpoints are prefixed with ``/<endpoint_name>/<plugin_name>/``.

Bridge Endpoints
================

These routes are served directly by the bridge process.

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``/``
     - Explorer UI (HTML)
   * - ``GET``
     - ``/events``
     - SSE stream for real-time notifications and topology changes
   * - ``POST``
     - ``/endpoint/list``
     - List connected endpoints and their plugins. Returns ``{"data": {"endpoints": {name: {plugins: {...}}}}}``
   * - ``POST``
     - ``/endpoint/disconnect/{name}``
     - Gracefully disconnect an endpoint and terminate it
   * - ``POST``
     - ``/bridge/terminate``
     - Terminate the bridge process
   * - ``GET``
     - ``/plugins/{filename}``
     - Serve a JS plugin module file (used by Explorer UI)
   * - ``GET``
     - ``/{endpoint_name}/{plugin}/{path}``
     - Proxy a GET request to a plugin on the named endpoint
   * - ``POST``
     - ``/{endpoint_name}/{plugin}/{path}``
     - Proxy a POST request to a plugin on the named endpoint

SSE Event Format
----------------

The ``/events`` stream sends JSON-encoded events::

    data: {"topic": "notification", "data": {
        "endpoint":   "my_endpoint",
        "plugin": "psij",
        "topic":  "job_status",
        "data":   { ... plugin-specific ... }
    }}

    data: {"topic": "topology", "data": {
        "endpoints": {"my_endpoint": {"plugins": ["sysinfo", "psij"]}}
    }}


Plugin Base Routes
==================

Every plugin automatically registers these routes under ``/<plugin_name>/``:

.. list-table::
   :header-rows: 1
   :widths: 10 30 60

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``register_session``
     - Create a new session. Returns ``{"sid": "<session_id>"}``
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
     - UI configuration for the Explorer. Returns plugin name, version, and ``ui`` dict


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
- ``404`` — Session or resource not found
- ``409`` — Conflict (e.g. workflow already running)
- ``410`` — Session expired (TTL exceeded)
- ``500`` — Internal server error

Error body format::

    {"detail": "human-readable error message"}
