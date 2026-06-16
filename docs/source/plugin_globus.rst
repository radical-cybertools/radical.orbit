
.. _plugin_globus:

###################
Plugin: ``globus``
###################

The ``globus`` plugin stages files via `Globus Online
<https://www.globus.org/>`_ (the Transfer API).  Globus moves data
**collection-to-collection** out of band, so the plugin is an *orchestrator*:
it submits transfers between two Globus collections and monitors task state.
Bytes never flow through the client, edge, or bridge — which distinguishes it
from the byte-streaming :ref:`staging <plugin_api>` plugin.

The plugin is **edge-side** (it is not loaded on the bridge) and is only
enabled when `globus-sdk <https://globus-sdk-python.readthedocs.io/>`_ is
importable.


Architecture
============

``GlobusSession``
   Holds one ``globus_sdk.TransferClient`` bound to the supplied credential,
   tracks submitted tasks, and runs a background poller (~10 s) that emits
   ``transfer_status`` notifications on state changes.  ``globus-sdk`` is
   synchronous, so each Transfer call is offloaded with ``asyncio.to_thread``
   to keep the edge event loop responsive.

``GlobusClient``
   Application-side helper mirroring the session methods.

``PluginGlobus``
   Edge plugin that registers the REST routes and per-client sessions.


Authentication
==============

A Globus Transfer token is supplied at ``register_session`` time, as **one of**:

* ``access_token`` — wrapped in an ``AccessTokenAuthorizer``.  Access tokens
  expire (~48 h) and are **not** renewed; the client re-registers with a fresh
  token when one lapses.
* ``refresh_token`` + ``client_id`` — wrapped in a ``RefreshTokenAuthorizer``,
  which transparently renews access tokens, so long-running transfers survive
  expiry.

The credential lives in the edge process memory (inside the ``TransferClient``)
for the lifetime of the session and is **never** written to disk.

Acquire a token with the bundled ``get_globus_token.py`` helper.

.. note::

   For Globus Connect Server **mapped collections**, the token must already
   carry the per-collection ``data_access`` dependent scope.  Otherwise the
   plugin surfaces a clear ``401`` (``ConsentRequired``) telling the caller to
   re-acquire a token with that scope, rather than an opaque error.


Collections
===========

Collections are identified by **UUID** and passed explicitly on the wire.  The
literal string ``"local"`` (or an omitted collection) resolves to the edge's
configured *local collection*, discovered at plugin start-up in this order:

#. ``RADICAL_EDGE_GLOBUS_COLLECTION`` environment variable;
#. Globus Connect Personal (``globus_sdk.LocalGlobusConnectPersonal``);
#. the config file ``~/.radical/edge/globus.json`` with a
   ``{"local_collection": "<uuid>"}`` key;
#. otherwise ``None`` (an explicit UUID must then be supplied).

A per-session ``local_collection`` passed to ``register_session`` overrides the
auto-detected default.

.. note::

   Facility GCS/DTN collections are generally **not** locally discoverable —
   only Globus Connect Personal exposes its UUID.  On such hosts, set the
   environment variable or the config file.


Client API
==========

.. code-block:: python

   from radical.edge import BridgeClient

   bc     = BridgeClient()
   ec     = bc.get_edge_client(bc.list_edges()[0])
   globus = ec.get_plugin('globus')

   # access token, or refresh_token=… + client_id=…
   globus.register_session(access_token='…', local_collection='…')

   sub = globus.submit_transfer(
       source='<src-uuid>', destination='local',
       items=[{'source': '/data/in/', 'destination': '/~/out/',
               'recursive': True}],
       label='my transfer', sync_level='checksum')

   globus.task_wait(sub['task_id'], timeout=60)
   print(globus.get_task(sub['task_id'])['status'])

Methods:

``register_session(access_token=None, refresh_token=None, client_id=None, local_collection=None)``
   Open a session with a Globus credential.

``submit_transfer(source, destination, items, label=None, sync_level=None)``
   Submit a transfer.  ``items`` is a list of
   ``{"source", "destination", "recursive"}`` dicts.  Returns
   ``{task_id, submission_id, status}``.

``get_task(task_id)`` / ``task_wait(task_id, timeout=60, polling_interval=10)`` /
``cancel_task(task_id)`` / ``list_tasks(limit=100)``
   Task monitoring and control.

``ls(collection, path=None)`` / ``mkdir(collection, path)`` /
``rename(collection, oldpath, newpath)`` / ``delete(collection, paths, recursive=False, label=None)``
   Filesystem operations on a collection (``delete`` submits a Globus delete
   task).

``endpoint_search(filter_text=None, limit=25)`` / ``get_endpoint(endpoint_id)``
   Collection discovery and metadata.


Notifications
=============

Topic ``transfer_status`` is emitted on task state change::

   {task_id, status, label, bytes_transferred, files_transferred, nice_status}

where ``status`` is the Globus task status
(``ACTIVE`` / ``SUCCEEDED`` / ``FAILED``).


Example
=======

See ``examples/example_globus.py`` for a runnable end-to-end transfer (it
defaults to the public Globus Tutorial Collections).
