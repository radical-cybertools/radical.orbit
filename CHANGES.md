
Version 0.0.1 release                                                 2026-03-11
--------------------------------------------------------------------------------

* For a list of bug fixes, see
  https://github.com/radical-cybertools/radical.orbit/issues?q=is%3Aissue+is%3Aclosed+sort%3Aupdated-desc
* For a list of open issues and known problems, see
  https://github.com/radical-cybertools/radical.orbit/issues?q=is%3Aissue+is%3Aopen+

* initial release for radical project 'radical.orbit'


Version 0.1.0 release                                                 2026-03-31
--------------------------------------------------------------------------------

  - development milestone toward AmSC demo


Broker architecture
--------------------------------------------------------------------------------

  - ORBIT is a broker-based star: one active `broker` hub routes between
    `endpoint` participants, each of which dials the hub over a single
    outbound WebSocket and may serve plugins, consume them, or both.
  - The wire protocol is one symmetric, versioned, msgpack-only envelope
    (`src`/`dst`/`corr_id`); transport-level keepalive drives two-stage
    (`suspect` → `lost`) liveness that is structurally isolated from plugin
    behaviour.
  - Control flows through the star; bulk data moves out of band.  The
    HTTP/SSE/Explorer surface is served by a broker-hosted `gateway` module
    (on by default) rather than by every participant.


--------------------------------------------------------------------------------
