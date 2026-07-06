
.. _plugin_queue_info:

######################
Plugin: ``queue_info``
######################

The ``queue_info`` plugin exposes batch system queue information, job
listings, and allocation data via REST endpoints.  It supports SLURM
(24.11.5+) and PBSPro, with a no-op backend on hosts where neither is
present, and is designed for easy extension to other batch systems.


Architecture
============

The plugin consists of three layers:

``QueueInfo`` (ABC)
   Abstract base class defining the backend interface and providing
   thread-safe caching with a configurable TTL (default 1 hour).

``QueueInfoSlurm`` / ``QueueInfoPBSPro`` / ``QueueInfoNone``
   Backend implementations.  ``QueueInfoSlurm`` calls ``sinfo``,
   ``squeue``, ``scontrol``, and ``sacctmgr`` with ``--json`` output;
   ``QueueInfoPBSPro`` parses ``qstat`` / ``pbsnodes`` text output; the
   ``None`` backend is a graceful no-op.  The active backend is chosen
   automatically by :func:`make_queue_info`.

``PluginQueueInfo``
   Endpoint plugin that exposes the backend via REST endpoints and manages
   per-client sessions.


Multi-cluster support
=====================

Multiple instances of the plugin can be loaded on a single endpoint service,
each targeting a different SLURM cluster:

.. code-block:: bash

   # load for cluster A
   curl -X POST "https://broker:8000/endpoint/load_plugin/radical.queue_info?name=cluster_a&slurm_conf=/etc/slurm/cluster_a.conf"

   # load for cluster B
   curl -X POST "https://broker:8000/endpoint/load_plugin/radical.queue_info?name=cluster_b&slurm_conf=/etc/slurm/cluster_b.conf"

Each instance gets its own namespace, client pool, and cache.


REST endpoints
==============

All paths are relative to the plugin namespace
(``/<name>/<uid>/``).

``POST /register_client``
   Register a new client session.  Returns ``{"cid": "<client_id>"}``.

``POST /unregister_client/{cid}``
   Close and remove a client session.

``GET /echo/{cid}?q=<string>``
   Echo service for testing.

``GET /get_info/{cid}?force=true``
   Return queue/partition information.  Set ``force=true`` to bypass cache.

``GET /list_jobs/{cid}/{queue}?user=<name>&force=true``
   List jobs in a partition.  Optionally filter by user.

``GET /list_all_jobs/{cid}?user=<name>&force=true``
   List all of the user's jobs across every partition.

``GET /list_allocations/{cid}?user=<name>&force=true``
   List SLURM associations (accounts/allocations).  Optionally filter by
   user.

``POST /cancel/{cid}/{job_id}``
   Cancel a job via the active batch system (``scancel`` / ``qdel``).

``GET /backend``
   Session-less.  Report the active backend: ``{"backend": "slurm" |
   "pbs" | "none"}``.

``GET /job_allocation``
   Session-less.  Return the **endpoint** process's own batch allocation
   (``{"allocation": {...}}``), or ``{"allocation": null}`` when the
   endpoint runs on a login node.

``GET /nodelist``
   Session-less.  Return the expanded hostname list of the endpoint's own
   allocation (``{"nodelist": [...]}``); empty on a login node.


Data structures
===============

Queue info (``get_info``)
-------------------------

.. code-block:: json

   {
     "queues": {
       "<partition_name>": {
         "name":              "compute",
         "state":             "UP",
         "time_limit":        1440,
         "default":           null,
         "nodes_total":       200,
         "nodes_available":   185,
         "nodes_idle":        65,
         "cpus_per_node":     128,
         "mem_per_node_mb":   524288,
         "gpus_per_node":     8,
         "max_jobs_per_user": null,
         "features":          ["nvme", "skylake"]
       }
     }
   }

``time_limit`` is in minutes or ``"UNLIMITED"``.
``nodes_available`` excludes nodes in DOWN, DRAIN, MAINT, FAIL, and similar
unavailable states.


Job list (``list_jobs``)
------------------------

.. code-block:: json

   {
     "jobs": [
       {
         "job_id":      "100001",
         "job_name":    "simulation_01",
         "user":        "alice",
         "partition":   "compute",
         "state":       "RUNNING",
         "nodes":       10,
         "cpus":        1280,
         "time_limit":  60,
         "time_used":   3600,
         "submit_time": 1699999000,
         "start_time":  1700000000,
         "priority":    50000,
         "account":     "proj_alpha",
         "node_list":   "node010-node019",
         "reason":      "",
         "qos":         "normal",
         "dependency":  "",
         "std_out":     "/home/alice/job.out",
         "std_err":     "/home/alice/job.err",
         "work_dir":    "/home/alice/run",
         "command":     "/home/alice/run.sh",
         "exit_code":   null,
         "array_job_id":  null,
         "array_task_id": null,
         "tres_req":    "cpu=1280,mem=...",
         "tres_alloc":  "cpu=1280,node=10,...",
         "restart_cnt": 0
       }
     ]
   }

``time_limit`` and ``time_used`` are in minutes and seconds respectively.
``time_limit`` is ``null`` for unlimited jobs.

The fields from ``reason`` onward are additional per-job detail surfaced
for the Explorer's job-detail overlay.  They are populated from whatever
the scheduler reports (``squeue --json`` fields on SLURM, ``qstat -f``
attributes on PBSPro — where PBSPro also adds ``owner`` and ``mem_used``)
and default to ``""`` / ``null`` when the scheduler does not provide them,
so older SLURM releases and PBS jobs without the attribute still parse.
Array-job and TRES fields apply to SLURM only.


Allocations (``list_allocations``)
----------------------------------

.. code-block:: json

   {
     "allocations": [
       {
         "account":              "proj_alpha",
         "user":                 "alice",
         "fairshare":            50,
         "qos":                  "normal,high",
         "max_jobs":             100,
         "max_submit":           200,
         "max_wall":             1440,
         "grp_tres":             "cpu=50000,node=200",
         "allocated_node_hours": null,
         "used_node_hours":      null,
         "remaining_node_hours": null
       }
     ]
   }

When ``user`` is empty, the row represents an account-level association.
The ``*_node_hours`` fields are reserved for future accounting integration.


Client API
==========

``QueueInfoClient`` (obtained via ``endpoint.get_plugin(name, 'queue_info')``)
exposes:

``get_info(user=None, force=False)``
   Queue/partition information (session required).

``list_jobs(queue, user=None, force=False)``
   Jobs in a single partition (session required).

``list_all_jobs(user=None, force=False)``
   All of the user's jobs across every partition (session required).

``list_allocations(user=None, force=False)``
   Associations / allocations (session required).

``cancel_job(job_id)``
   Cancel a job through the active batch system (session required).

``job_allocation()``
   Session-less.  The endpoint's own batch allocation summary, or ``None``
   on a login node.

``nodelist()``
   Session-less.  Expanded hostnames of the endpoint's own allocation.

``backend()``
   Session-less.  The active backend name: ``'slurm'``, ``'pbs'``, or
   ``'none'``.


Example
=======

.. code-block:: bash

   ./examples/example_queue_info.py


Extending to other batch systems
=================================

Subclass ``QueueInfo`` and implement ``_collect_info``,
``_collect_jobs``, and ``_collect_allocations``.  Register the new
backend class alongside ``QueueInfoSlurm`` in
``src/radical/orbit/queue_info.py``.
