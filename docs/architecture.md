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
- `subscribe` / `unsubscribe` — live-event interest patterns
  (`endpoint`, `plugin`, `topic`, optional `session`); a runtime sends a
  `session` pattern only where the broker advertises `session_events` in
  topology, and only for session-scoped registrations on a concrete
  endpoint and plugin (unscoped ones keep the plain pattern and still
  receive every session's events).  A session pattern narrows
  session-stamped events only; session-less events always pass.
- `topology` — a rich participant/plugin/liveness snapshot.
- `control` — `shutdown` / `error` / `terminate` / `disconnect`.

Liveness is transport-level only (WebSocket keepalive); there is no app-level
heartbeat kind. A frame-size cap bounds each frame.

The keepalive deadline (a socket whose pong is this late is closed) is 3 s
on both ends by default.  `RADICAL_ORBIT_PING_TIMEOUT` sets it for the
broker (`BrokerTuning.ping_timeout`) and every runtime (`ping_timeout=`)
together; `0` disables it.  A broker hosting hundreds of in-process
participants (e.g. one per DT service session) needs more: its loop can
stall longer than 3 s, and every connection would drop at once.  Topology
changes are coalesced (`BrokerTuning.topology_coalesce`, 0.1 s): one
broadcast at a time, carrying every change made meanwhile.

WebSocket permessage-deflate is decided per hop by the side that dials.  It
dominated large payloads on fast links (1 MiB round trips went from ~100 ms
to 16–19 ms without it; small frames are unaffected), so:

- the broker *accepts* it by default (costs nothing unless a client offers
  it); `BrokerTuning(ws_compression=False)` or
  `RADICAL_ORBIT_WS_COMPRESSION=off` turns acceptance off;
- an `EndpointRuntime` *offers* it per mode `off`, `on` or `auto`
  (`ws_compression=` keyword > `RADICAL_ORBIT_WS_COMPRESSION` > `auto`).
  `auto` times a plain TCP connect to the broker before each (re)connect
  and offers deflate when it takes at least 10 ms
  (`ws_compression_rtt_ms` / `RADICAL_ORBIT_WS_COMPRESSION_RTT_MS`) —
  i.e. on WAN links, not inside a site.  The runtime asks for zlib level 1.

The extension is negotiated per connection, so mixed settings interoperate:
frames are compressed only when offered and accepted.

## Session lifecycle

A consumer opens a session on a plugin with `register_session` (create or
reconnect), which fixes its idle timeout (`ttl`, default: the plugin's
`session_ttl`); it releases it with `unregister_session`. Any client holding
the sid can attach; participant loss does not end a session.  A session idle
longer than its timeout expires (requests, notifications and running work
count as activity), and requests on an ended session answer 410 with the
reason. See [Plugin Development](plugin_development.md) for the
full model.


The full per-module API reference is auto-generated under
[Module Reference](api/index.md).
