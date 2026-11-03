# Orbit-Dragon DDict POC (SwarmSlicer)

Status: in progress, started 2026-11-03.  Target: first half of November.

## Goal

A laptop client creates a Dragon DDict, reads and writes it through the
ORBIT broker, and a process inside the Dragon runtime attaches to the same
DDict and sees the client's data.  The endpoint runs in a container; the
broker runs either next to it (in the cluster) or outside it.

The POC drives DDict directly through public `dragonhpc` 0.15.0.  HPE's
Dragon services API server replaces the direct calls once we have code
access; the backend seam below is where it slots in.

## Design

- **Plugin** `dragon` (`src/radical/orbit/plugin_dragon.py`), endpoint
  only, not in any default plugin set (`--plugins default,dragon`).
  Loaded only when `dragonhpc` is importable.
- **Backend seam** `DDictBackend` (same module): owns the Dragon runtime
  and the DDicts.  Every Dragon call runs on one dedicated thread (spike:
  concurrent DDict calls from several threads hang).  Lifecycle follows
  rhapsody's `DragonDataBackend` (not imported: it forces
  `wait_for_keys=True`, which makes a get of a missing key block, and
  rhapsody pins dragonhpc 0.14.1).
- **Runtime mode** `RADICAL_ORBIT_DRAGON_RUNTIME`:
  `attach` (join a running runtime, `DRAGON_ATTACH_TOKEN`) or `start`
  (`LocalRuntime.start()` on first use, `stop()` at plugin shutdown).
  Default: `attach` when `DRAGON_ATTACH_TOKEN` is set, else `start`.
- **Ownership**: DDicts belong to the plugin, not to a session (Dragon
  services are persistent and shared by ID).  Entry: id, name, params,
  created, creator, serialized descriptor.
- **Values**: JSON, or `bytes` as `{"$b64": ...}` (no pickle: nothing
  client-supplied is unpickled on the endpoint).  Keys are strings.  A get
  reply above half the frame cap fails with 413.
- **Client**: `DragonClient.ddict.create(...)` returns a `DDictHandle`
  with native `DDict` semantics (`dd[k] = v`, `dd[k]`, `del dd[k]`,
  `len`, `keys()`, `in`, `destroy()`); `.ddict.attach(id)` and
  `.ddict.list()`.  No CLI in the POC (the `dragon-cli` surface is not
  verified).  REST comes free through the gateway.
- **Notification** `ddict_status` → `{id, name, state}` on create/destroy.

## Routes

| Verb | Route | Body → result |
|---|---|---|
| POST | `ddict/create/{sid}` | `{name?, managers_per_node?, n_nodes?, total_mem?}` → entry |
| GET  | `ddict/list/{sid}` | → `{ddicts: [entry]}` |
| GET  | `ddict/info/{sid}/{id}` | → entry + `n_keys` |
| POST | `ddict/destroy/{sid}` | `{id}` |
| POST | `ddict/put/{sid}` | `{id, items}` → `{n}` |
| POST | `ddict/get/{sid}` | `{id, keys, values?}` → `{items, missing, errors}` |
| POST | `ddict/keys/{sid}` | `{id}` → `{keys, n_other}` |
| POST | `ddict/pop/{sid}` | `{id, keys}` → `{n}` |
| POST | `ddict/clear/{sid}` | `{id}` |

## Steps

1. Spike (day 1): Dragon attach and DDict create/put/get from inside a
   running endpoint process (`start` mode), before building the rest.
2. Plugin and backend, unit tests with a fake DDict class.
3. Client handle, tests.
4. Laptop run against real dragonhpc 0.15.0, including the in-runtime
   attach demo (`examples/example_dragon_ddict.py`).
5. Container: `examples/dragon/` with Dockerfile and docker-compose
   (broker + endpoint, and endpoint-only for an external broker).  k8s
   manifests once the cluster is chosen.
6. Docs: `docs/plugin_dragon.md`, CLAUDE.md plugin entry.

## Out of scope

CLI, Explorer panel, multi-node Dragon in containers, HPE API server,
per-user ownership, streaming.

## Spike results (2026-11-03, laptop, dragonhpc 0.15.0, py3.11)

- `LocalRuntime.start()` from inside an asyncio process: ~4.5 s.
- 50 concurrent DDict puts from 8 executor threads: hang.  Serialized
  (lock, any thread) or one dedicated thread: work; one thread is simplest
  (50 puts in 0.09 s).
- A Dragon process started from the client attaches by descriptor, reads
  the client's data and writes back.  A missing key raises `KeyError`.
- `LocalRuntime.start()` needs the `dragon` launcher on `PATH`; an
  external head needs `dragon -s -a dragon-waiting-runtime-head`
  (without `-s` it waits for a multi-node setup).

## Status (2026-11-03)

Done: plugin, backend, client, unit tests, laptop runs in `attach` and
`start` mode (example incl. in-runtime attach), runtime teardown on
endpoint exit, container files, docs.  Open: container build and run (no
Docker daemon on the dev laptop), k8s manifests once the cluster is chosen.
