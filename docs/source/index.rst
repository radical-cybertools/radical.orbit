
#############################################
radical.radical.edge |version| documentation
#############################################

**RADICAL Edge** is a bridge-based distributed framework that connects external
RADICAL-Cybertools (RCT) applications with HPC resources.  It uses a three-tier
architecture — **Client → Bridge → Edge** — communicating over HTTPS and
WebSockets: a public-facing *bridge* acts as a reverse proxy, each *edge*
service runs on an HPC resource and opens an outbound (firewall-friendly)
WebSocket back to the bridge, and *plugins* extend each edge with
domain-specific functionality (job submission, queue info, file staging,
task execution, and more), each under its own isolated URL namespace.

These pages document the plugin API and development model, the embedding and
REST interfaces, and the individual plugins shipped with the framework.

**Get involved or contact us:**

+-------+---------------------+------------------------------------------------------------------+
| |Git| | **GitHub project:** | https://github.com/radical-cybertools/radical.radical.edge/     |
+-------+---------------------+------------------------------------------------------------------+
| |Goo| | **Mailing List:**   | https://groups.google.com/forum/#!forum/radical.edge-devel      |
+-------+---------------------+------------------------------------------------------------------+

.. |Git| image:: images/github.jpg
.. |Goo| image:: images/google.png


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 3

   module_radical.edge.rst
   service_embedding.rst
   plugin_development.rst
   plugin_api.rst
   plugin_globus.rst
   rest_api.rst


##################
Indices and tables
##################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

