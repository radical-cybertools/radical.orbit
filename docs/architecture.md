# Architecture & Wire Protocol

## Overview

The `radical.orbit` module provides a broker-based, plugin-oriented framework
for reaching HPC resources and workflows. It is a **star**: one active broker
hub routes between endpoint participants, each dialing the hub over a single
outbound WebSocket. Participants serve plugins, consume other participants'
plugins, or both; a broker-hosted gateway module serves an HTTP/SSE/Explorer
compatibility surface for non-participant callers.

## Core Components

1.  **Broker** (`radical.orbit.broker`) — the active hub: a lean routing
    loop plus an own-thread host for broker-hosted plugins.
2.  **Endpoint runtime** (`radical.orbit.runtime`) — the single node
    abstraction; `radical.orbit.EndpointRuntime` (exported as
    `Endpoint`) serves and/or consumes plugins over one outbound WebSocket.
3.  **Gateway** (`radical.orbit.gateway`) — the broker's compat-tier
    HTTP/SSE/UI ingress.
4.  **Protocol** (`radical.orbit.protocol`) — the symmetric, versioned,
    msgpack-only wire envelope shared by broker and participants.
5.  **Plugin system** (`radical.orbit.plugin_base`,
    `radical.orbit.plugin_session_base`, `radical.orbit.client`) — base
    classes for plugins, per-client sessions, and consumer-side helpers.

## Wire protocol

Broker and participants exchange **one symmetric, versioned envelope**, encoded
msgpack-only (`body` is native bytes). Common fields are
`version, id, corr_id?, channel?, kind, src, dst?`; `kind` discriminates the
payload:

- `request` / `response` — correlated RPC, routable in either direction by
  `dst`.
- `event` — push notification (`plugin, topic, session?, ts, seq, data`);
  the broker stamps an authoritative `seq`/`ts` on ingest.
- `register` / `register_ack` — the identity + resume-key handshake.
- `subscribe` / `unsubscribe` — live-event interest patterns.
- `topology` — a rich participant/plugin/liveness snapshot.
- `control` — `shutdown` / `error` / `terminate` / `disconnect`.

Liveness is transport-level only (WebSocket keepalive); there is no app-level
heartbeat kind. A frame-size cap bounds each frame.

## Session lifecycle

A consumer opens a session on a plugin with `register_session` (create or
reconnect), which selects a lifetime policy (`ephemeral` / `ttl` /
`persistent`) and records the trusted owner identity; it releases it with
`unregister_session`. Reattach is owner-checked; TTL and idle policies expire
sessions on time; owner-bound ephemeral sessions are reclaimed a grace period
after their owner is declared `lost`. See [Plugin Development](plugin_development.md) for the
full model.


The full per-module API reference is auto-generated under
[Module Reference](api/index.md).
