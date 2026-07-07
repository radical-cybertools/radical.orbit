
.. _plugin_iri:

################
Plugin: ``iri``
################

The IRI integration lets ORBIT drive `IRI
<https://iri.science/>`_ (Integrated Research Infrastructure) facility
endpoints — listing compute resources, submitting and monitoring jobs, and
reading facility incidents, projects and allocations — through the same
plugin/client model as every other ORBIT plugin.

It comes in two cooperating parts:

``iri_connect``
   A **broker-side** configurator.  It lists the known IRI facility endpoints
   and, on ``connect()``, dynamically registers a per-endpoint instance
   plugin named ``iri.<endpoint>`` (e.g. ``iri.nersc``) that then appears as a
   first-class node in the Explorer tree.

``iri.<endpoint>``
   A **dynamic per-endpoint instance** (class ``PluginIRIInstance``) created by
   ``iri_connect``.  It talks to one facility's IRI REST API and exposes its
   resources and jobs.  It is *not* auto-registered — instances exist only for
   endpoints a user has connected to.


Supported endpoints
===================

The facility endpoints are defined in
:mod:`radical.orbit.iri_endpoints` (``IRI_ENDPOINTS``):

.. list-table::
   :header-rows: 1
   :widths: 12 40 15

   * - Key
     - REST base URL
     - Auth
   * - ``nersc``
     - ``https://api.iri.nersc.gov`` (NERSC / Perlmutter)
     - Globus
   * - ``olcf``
     - ``https://amsc-open.s3m.olcf.ornl.gov`` (OLCF / Frontier, Odo)
     - S3M

Add or change facility endpoints by editing ``IRI_ENDPOINTS``.


Authentication
==============

A facility access token is supplied at ``connect`` time.  The token is held in
**broker process memory** (inside the instance's HTTP client) for the lifetime
of the ``iri.<endpoint>`` instance and is **never written to disk**.
Disconnecting removes the instance and drops the token.

The Explorer UI prompts for the token and shares it with the plugin through the
``iri_tokens`` browser storage key; from Python you pass it to
``IRIConnectClient.connect()``.


Client API
==========

``IRIConnectClient`` (the ``iri_connect`` plugin):

.. list-table::
   :header-rows: 1
   :widths: 30 55

   * - Method
     - Description
   * - ``list_endpoints()``
     - The known facility endpoints (from ``IRI_ENDPOINTS``) and which are
       currently connected.
   * - ``connect(endpoint, token)``
     - Connect to *endpoint* with *token*; registers ``iri.<endpoint>`` and
       returns an ``IRIInstanceClient`` bound to it.  Idempotent — an
       already-connected endpoint has its token refreshed in place.
   * - ``disconnect(endpoint)``
     - Tear down ``iri.<endpoint>`` and its sessions.
   * - ``get_status()``
     - Connection status of the configured endpoints.

``IRIInstanceClient`` (the ``iri.<endpoint>`` instance):

.. list-table::
   :header-rows: 1
   :widths: 40 55

   * - Method
     - Description
   * - ``list_resources(resource_type='compute')``
     - Facility resources of the given type.
   * - ``get_resource(resource_id)``
     - Detail for one resource.
   * - ``submit_job(resource_id, job_spec)``
     - Submit a job to a resource; returns the facility job id.
   * - ``get_job_status(resource_id, job_id)``
     - Current job state.
   * - ``list_jobs(resource_id)``
     - Jobs on a resource.
   * - ``cancel_job(resource_id, job_id)``
     - Cancel a job.
   * - ``list_incidents()``
     - Facility incident/outage feed.
   * - ``list_projects()``
     - Projects visible to the token.
   * - ``list_allocations(project_id)``
     - Allocations for a project.

The instance runs a background poller (~10 s) and emits ``job_status``
notifications as jobs change state, so a client can register a notification
callback instead of polling.


Usage
=====

.. code-block:: python

   from radical.orbit import EndpointRuntime

   rt  = EndpointRuntime(broker_url="https://my-broker:8000")
   iri = rt.get_plugin("broker", "iri_connect")   # iri_connect is broker-hosted

   # discover facilities, then connect (token from your facility login flow)
   print(iri.list_endpoints())
   nersc = iri.connect("nersc", token="<globus-access-token>")

   # use the per-endpoint instance client
   resources = nersc.list_resources("compute")
   rid       = resources["resources"][0]["id"]
   job       = nersc.submit_job(rid, {"executable": "/bin/hostname"})
   print(nersc.get_job_status(rid, job["job_id"]))

   iri.disconnect("nersc")

See also the :ref:`Machine setup guide <machine_guide>` for how IRI fits
alongside the ``globus`` and ``psij`` access paths per facility.
