
.. _machine_guide:

###################
Machine setup guide
###################

ORBIT reaches an HPC facility through **three complementary access paths**,
each provided by a plugin.  A given workflow may use one or several:

.. list-table::
   :header-rows: 1
   :widths: 12 40 30

   * - Path
     - What it does
     - Plugin
   * - **PsiJ**
     - Submit and monitor *batch jobs* on the machine, through an ORBIT edge
       running on that machine's login/compute node.
     - :ref:`psij <rest_api>`
   * - **IRI**
     - Drive the facility's *IRI REST API* (resources, jobs, incidents,
       allocations) — no edge on the machine required.
     - :ref:`iri <plugin_iri>`
   * - **Globus**
     - Move *data* between facility collections, out of band.
     - :ref:`globus <plugin_globus>`

This guide shows how to set each one up, then gives per-facility notes.


PsiJ — batch jobs via an edge
=============================

1. Start the bridge somewhere reachable from the machine (see the
   *Getting Started* guide), and copy its URL (and, for HTTPS, its
   certificate and auth token) to the machine.
2. On the machine's login node, start an edge::

      radical-orbit-endpoint --url https://<bridge-host>:8000 \
                             --cert bridge_cert.pem

   The edge auto-detects the batch system (SLURM ``squeue`` / PBSPro
   ``qstat``) and loads the ``psij`` and ``queue_info`` plugins.
3. Submit jobs through the ``psij`` client
   (``submit_job`` / ``get_job_status`` / ``cancel_job``).

**Firewalled compute nodes.**  When the edge must run on a compute node that
cannot open a direct socket to the bridge, ``submit_tunneled`` launches a child
edge with an SSH tunnel.  Pick the mode that matches the site's SSH policy:

* ``forward`` — the child opens ``ssh -L`` from compute to the login host.
  Use where **compute → login SSH is allowed** and login → compute is blocked
  (e.g. Perlmutter, Aurora).
* ``reverse`` — the login-side parent opens ``ssh -R`` to the compute host.
  Use where **compute → login is blocked** but login → compute works
  (e.g. Odo).
* ``none`` — the child connects directly (no SSH).


IRI — facility REST API
=======================

No edge is required — the bridge talks to the facility's IRI service directly.

1. Obtain a facility access token (NERSC uses Globus, OLCF uses S3M — see the
   per-facility notes below).
2. Connect and use the per-endpoint instance client (see
   :ref:`Plugin: iri <plugin_iri>` for the full API)::

      iri   = bridge.get_plugin("iri_connect")
      nersc = iri.connect("nersc", token="<token>")
      print(nersc.list_resources("compute"))

The token lives only in bridge process memory and is dropped on
``disconnect``.


Globus — data staging
=====================

1. Acquire a Globus Transfer token (or a refresh token + client id for
   long transfers).
2. Register a ``globus`` session with that credential and submit transfers
   between two collection UUIDs.  Bytes flow collection-to-collection, never
   through ORBIT.  See :ref:`Plugin: globus <plugin_globus>`.


Per-facility notes
==================

.. list-table::
   :header-rows: 1
   :widths: 18 20 20 24

   * - Facility
     - IRI auth
     - PsiJ scheduler
     - Compute tunnel
   * - **NERSC / Perlmutter**
     - Globus (``iri.nersc`` → ``api.iri.nersc.gov``)
     - SLURM
     - ``forward``
   * - **OLCF / Frontier**
     - S3M (``iri.olcf`` → ``amsc-open.s3m.olcf.ornl.gov``)
     - SLURM
     - ``forward`` (verify site policy)
   * - **OLCF / Odo**
     - S3M (``iri.olcf``)
     - SLURM
     - ``reverse``

.. note::

   The IRI endpoints and their auth methods are defined in
   ``radical.orbit.iri_endpoints`` (``IRI_ENDPOINTS``); add a facility by
   extending that table.  Scheduler and tunnel choices reflect current site
   SSH policy — confirm against the facility's own documentation before a
   production run, as policies change.
