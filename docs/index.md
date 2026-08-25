# radical.orbit documentation

**ORBIT** is a broker-based distributed framework that connects external
RADICAL-Cybertools (RCT) applications with HPC resources. It is a **star**:
one active **broker** hub routes between **endpoint** participants, each of
which dials the hub over a single outbound WebSocket and may *serve* plugins,
*consume* other participants' plugins, or both. A broker-hosted **gateway**
module serves an HTTP/SSE/Explorer compatibility surface for non-participant
callers (browsers, `curl`, the Explorer UI).

Control flows through the star; bulk data moves out of band (Globus, the shared
filesystem, SSH tunnels).

The three moving parts:

1.  **Broker** (`radical-orbit-broker`) — the active hub. A lean routing loop
    switches `request`/`response` frames between participants by `src` and
    `dst`, correlates both directions via `corr_id`, tracks topology and
    two-stage (`suspect` → `lost`) liveness, and fans `event` frames out to
    subscribers. The broker is itself a participant (`src='broker'`) and can
    host plugins on their own loop/thread. The `gateway` module (on by
    default) attaches the HTTP/SSE/Explorer tier onto the same port.
2.  **Endpoint** (`radical-orbit-endpoint`) — a participant, typically on an
    HPC login or compute node. A dedicated transport thread owns the outbound
    WebSocket and its keepalive (firewall-friendly, outbound-only); parse and
    plugin work run on a separate work loop; user callbacks fire on a third
    dispatcher thread — so a slow or blocking plugin never affects liveness.
    The same runtime, with zero served plugins, is a pure **consumer** — the
    Python SDK (`radical.orbit.EndpointRuntime`, exported as `Endpoint`).
3.  **Plugins** — extend a participant (endpoint or broker) with domain-specific
    functionality (job submission, queue info, file staging, task execution,
    and more), each under its own endpoint-relative namespace.

These pages document the plugin API and development model, the participant
runtime and its embedding, the gateway's HTTP surface, and the individual
plugins shipped with the framework.

**Get involved or contact us:**

|  |  |  |
|----|----|----|
| ![Git](images/github.jpg) | **GitHub project:** | <https://github.com/radical-cybertools/radical.orbit/> |
| ![Goo](images/google.png) | **Mailing List:** | <https://groups.google.com/forum/#!forum/radical.orbit-devel> |

# Contents

- [Getting Started](getting_started.md)
- [Architecture & Wire Protocol](architecture.md)
- [Runtime & Embedding](runtime_embedding.md)
- [Plugin Tutorial](tutorial_plugin.md)
- [Plugin Development](plugin_development.md)
- [Plugin API Reference](plugin_api.md)
- [Globus Plugin](plugin_globus.md)
- [Queue Info Plugin](plugin_queue_info.md)
- [IRI Plugins](plugin_iri.md)
- [Machine Guide](machine_guide.md)
- [REST API](rest_api.md)
- [Task Dispatcher Strategy](task_dispatcher_strategy.md)
- [Module Reference](api/index.md)
