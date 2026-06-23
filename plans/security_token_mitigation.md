# ORBIT: bridge ingress token authentication — pre-broker security step

## Context

The bridge HTTP ingress has **no client authentication**: any party that can reach
the bridge port can drive every plugin. The severe case is `psij.submit_job`, which
takes an arbitrary `executable` (`plugin_psij.py:161`) → **arbitrary command
execution** on any connected endpoint; `staging` can create files under `$HOME`/`/tmp`
(`plugin_staging.py:51-58`, never overwriting, but new files such as a missing
`~/.ssh/authorized_keys` are reachable). CORS is not authentication, and the TLS cert
authenticates the *bridge to clients*, not clients to the bridge — `_strip_headers`
even removes `proxy-authorization` on the way through (`bridge.py:310`).

This is a **minimal interim mitigation on the current architecture, shipped as its own
PR before the broker rewrite.** It is forward-compatible: the shared token becomes the
`credential` half of the broker plan's `(name, credential)` identity tuple, and mTLS is
the later per-participant upgrade. See `broker_architecture_plan.md` /
`broker_architecture_rationale.md`.

Root cause is one thing — a missing client credential on the ingress — so the fix gates
the ingress, not individual plugins (disabling `staging` would leave `psij` open).

## Decisions

- A **shared bearer token** gates the whole capability-bearing ingress, **including the
  endpoint WS `/register`** (which also closes today's endpoint-name-hijack: any
  cert-trusting process can currently register as any endpoint).
- The browser / SSE path uses a **post-handshake cookie** (HttpOnly, Secure,
  SameSite=Strict), not a query-param token (no token in logs).
- The token is **auto-generated and written 0600** if unset; an explicit **escape
  hatch** disables auth for pure-local dev. Default is auth-on.

## Scope / changes (current modules — pre-rename, pre-rewrite)

- **`utils.py`** — `resolve_bridge_token` (CLI `--token` > `RADICAL_ORBIT_BRIDGE_TOKEN`
  > `~/.radical/orbit/bridge.token`), mirroring `resolve_bridge_url`/`_cert`. If absent
  and auth is enabled, generate, write the 0600 file, and print it at startup (next to
  the URL).
- **`bridge.py`**
  - **Auth dependency** on the capability routes (proxy catch-all, `/endpoint/*`,
    `/bridge/terminate`, `/events`): accept `Authorization: Bearer <token>` **or** the
    auth cookie; 401 otherwise; **constant-time compare**. Left **ungated**: `/` (UI
    shell) and `/plugins/*.js` (static, no capability) so the Explorer can load and
    prompt.
  - **`POST /auth`**: validate the bearer header, then `Set-Cookie` (HttpOnly, Secure,
    SameSite=Strict) carrying the credential; the browser then rides the cookie for both
    `fetch` and `EventSource`.
  - **`/register` WS**: validate a `token` field in the register frame; on mismatch send
    the existing error message + close (reuses the path at `bridge.py:448-451`).
  - **Escape hatch**: `--no-auth` / `RADICAL_ORBIT_BRIDGE_NO_AUTH=1` disables the gate
    with a loud startup warning. Default auth-on.
- **`client.py` `BridgeClient`** — resolve the token; attach `Authorization: Bearer`
  via the existing httpx request hook on all calls **including the SSE stream** (httpx
  sets headers on streams, so Python clients use the header path, not the cookie).
- **`service.py` `EndpointService`** — resolve the token; include it in
  `RegisterMessage`. It already aborts on an `ErrorMessage` from the bridge, so a
  bad/missing token surfaces as a clean fatal.
- **`models.py`** — `RegisterMessage` gains `token: Optional[str]`.
- **Explorer** (`data/orbit_explorer.html` + `data/plugins/*.js`) — prompt for the
  token, store in `localStorage` (to re-auth), `POST /auth` to obtain the cookie, then
  `fetch` + `EventSource` ride the **HttpOnly** cookie (the live in-browser credential
  is the cookie, not JS-readable).
- **`bin/` wrapper + examples** — token pass-through (`--token` / env).
- **Tests + a short security note** in the docs.

## Security properties / non-goals

- A **shared secret** (deployment-scoped, like the cert), not per-user identity — same
  trust model as "you have the cert/URL".
- **HttpOnly** keeps the browser credential out of reach of XSS; **Secure** pins it to
  TLS; **SameSite=Strict** + the existing CORS allowlist mitigate CSRF.
- No token in query strings or logs (the cookie decision avoids that).
- **Out of scope** (broker plan): per-participant identity, mTLS, tenant authz.

## Verification

- Unauthenticated HTTP request → 401; with header or cookie → 200.
- `psij.submit_job` / `staging.put` unreachable without the token.
- Browser: enter token → `/auth` sets the cookie → Explorer + live SSE work; no token in
  logs.
- Endpoint with wrong/missing token → register rejected (fatal at the endpoint).
- `--no-auth` → gate disabled with a loud warning; same-host dev reads the written token
  file automatically and works.

## Handoff to the broker plan

The shared token is the **`credential`** in the broker's `(name, credential)` identity
tuple; the broker generalizes this gate and carries the cookie path into the `gateway`
plugin; mTLS is the later per-participant upgrade. Because this step already requires a
credential on the ingress (and on `/register`), the broker plan starts from an
authenticated bridge — it no longer needs to "remove the unauthenticated
`POST /bridge/terminate`" (closed here); admin-via-gateway simply inherits the gate.
