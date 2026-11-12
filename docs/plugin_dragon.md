# Plugin: `dragon`

The `dragon` plugin gives ORBIT clients access to Dragon distributed
dictionaries (DDicts).  A client creates a DDict on an endpoint that runs
next to a [Dragon](https://github.com/DragonHPC/dragon) runtime, reads and
writes it through the broker, and hands the DDict's serialized descriptor
to processes inside the runtime, which attach to the same data.

It is the first slice of Orbit-Dragon (SwarmSlicer): ORBIT as the access
layer for HPE's Dragon Core Workflow Services.  The plugin drives DDicts
directly through `dragonhpc` (>= 0.15.0); the Dragon services API server
replaces those calls later, behind the same routes.

The plugin is **endpoint-side**, loads only when `dragonhpc` is importable,
and is not in any default plugin set:

```sh
./bin/radical-orbit-endpoint-wrapper.sh --plugins default,dragon
```

## Dragon runtime

An endpoint launched by the `dragon` launcher (the endpoint wrapper does
that when dragonhpc is in its venv, inside an allocation) uses the runtime
it runs in.  Otherwise `$RADICAL_ORBIT_DRAGON_RUNTIME` selects how the
plugin reaches a runtime:

- `attach`: join a running runtime (`$DRAGON_ATTACH_TOKEN`, printed by
  `dragon -s -a dragon-waiting-runtime-head` or set by a launcher);
- `start`: start a single-node runtime on the first DDict create, and stop
  it when the endpoint exits.

Default: `attach` when `$DRAGON_ATTACH_TOKEN` is set, else `start`.  The
`dragon` launcher must be on `PATH` or next to the endpoint's interpreter.

All Dragon calls of the endpoint run on one dedicated thread: concurrent
DDict calls from several threads hang in dragonhpc 0.15.0. A put of several
keys is sent as one Dragon batch put (`start_batch_put` / `end_batch_put`),
so the managers do not confirm each key; an error then shows only at the
end, and an unknown subset of the batch may be stored.
`RADICAL_ORBIT_DRAGON_BATCH_PUTS=0` turns that off (key by key, each
confirmed), for comparisons with unbatched native writers.

**Worker processes.** One thread caps an endpoint at about one DDict call
per 0.4 ms. With `RADICAL_ORBIT_DRAGON_WORKERS=N` the endpoint instead
starts N Dragon processes in the runtime, round-robin over its nodes, and
runs put, get, presence checks and pop there. Each worker attaches the
DDicts by descriptor and serves one request at a time; a multi-key request
is split across the workers by key hash and the parts run in parallel.
Create, destroy, `keys`, `len` and `clear` stay on the endpoint's thread.
Workers connect back to the endpoint over TCP (to `$(hostname)`, or
`$RADICAL_ORBIT_DRAGON_WORKER_HOST`), authenticated with a per-endpoint
secret, and exit when the endpoint stops. Natively, more than about two
writers per DDict manager slow puts down, so raise `managers_per_node` with
the worker count.

**Multi-node runtimes under Slurm.** Start the endpoint under `dragon -m`
from the batch script or an interactive `salloc` shell, not from inside an
`srun` step. Inside a step, Slurm narrows `SLURM_JOB_NODELIST` to the step's
nodes, so the launcher's own `srun` only sees those nodes. If you must
launch from a step, first re-export the job's node list and drop the step
variables:

```sh
export SLURM_JOB_NODELIST=$SLURM_NODELIST
export SLURM_JOB_NUM_NODES=$(scontrol show hostnames $SLURM_NODELIST | wc -l)
unset SLURM_NNODES SLURM_NTASKS SLURM_NPROCS SLURM_TASKS_PER_NODE \
      SLURM_DISTRIBUTION SLURM_STEP_ID SLURM_STEPID SLURM_STEP_NODELIST \
      SLURM_STEP_NUM_NODES SLURM_STEP_NUM_TASKS SLURM_STEP_TASKS_PER_NODE \
      SLURM_STEP_LAUNCHER_PORT SLURM_STEP_RESV_PORTS SLURM_NODEID \
      SLURM_PROCID SLURM_LOCALID SLURM_GTIDS
export SLURM_OVERLAP=1
```

**Stopping.** Send SIGTERM to the endpoint process only. The endpoint then
destroys its DDicts and exits, and the launcher tears the runtime down. A
signal to the `dragon` launcher itself (e.g. `pkill -f` on the endpoint
script name, which also matches the launcher's command line) kills the
runtime under the endpoint. That leaves DDict processes and `/dev/shm`
pools behind (found in the endpoint teardown benchmarks on Perlmutter,
which are kept outside this repository).

## DDicts

DDicts belong to the plugin, not to the session that created them: like
Dragon services, they outlive their creator and are shared by ID or name.
When the endpoint exits, the plugin destroys its DDicts.

Keys are strings (keys of other types written inside the runtime are
counted by `len()` and reported as `n_other` by `keys`).  Values are
JSON-serializable objects or `bytes` (top level only; bytes nested in a
JSON value are not encoded); bytes travel as `{"$b64": "<base64>"}`,
and a malformed marker is a 400.  That shape is reserved: the client
refuses to write a dict whose only key is `$b64`, and such a value written
inside the runtime is reported per key in `errors` (nested ones are plain
dicts).  A value written inside the runtime that
is neither (e.g. a numpy array) is reported per key in `errors`.  A `get`
reply above half the transport frame cap fails with 413: get fewer keys per
call.

## Client API

```python
from radical.orbit import EndpointRuntime

rt = EndpointRuntime()
rt.start(wait=True)
dragon = rt.get_plugin('my-endpoint', 'dragon')

dd = dragon.ddict.create(name='scratch', total_mem='512M')
dd['k'] = {'v': 1}
dd['blob'] = b'\x00\x01'
dd['k'], 'k' in dd, len(dd), dd.keys(), dd.get('nope', None)
dd.update({'a': 1, 'b': 2})
del dd['a']

same = dragon.ddict.attach('scratch')       # by name or ID
dragon.ddict.list()

dd.descriptor       # DDict.attach(descriptor) inside the Dragon runtime
dd.destroy()
```

Inside the runtime:

```python
from dragon.data.ddict import DDict

dd = DDict.attach(descriptor)
dd['from_runtime'] = dd['k']
dd.detach()
```

## REST routes

Through the gateway at `/{endpoint}/dragon/...`:

| Verb | Route | Body → result |
|---|---|---|
| POST | `ddict/create/{sid}` | `{name?, managers_per_node?, n_nodes?, total_mem?}` → entry |
| GET  | `ddict/list/{sid}` | → `{ddicts: [entry]}` |
| GET  | `ddict/info/{sid}/{id}` | → entry + `n_keys` |
| POST | `ddict/destroy/{sid}` | `{id}` |
| POST | `ddict/put/{sid}` | `{id, items}` → `{n}` |
| POST | `ddict/get/{sid}` | `{id, keys, values?}` → `{items, missing, errors}`; `values: false` checks presence only |
| POST | `ddict/keys/{sid}` | `{id}` → `{keys, n_other}`: str keys, count of other keys |
| POST | `ddict/pop/{sid}` | `{id, keys}` → `{n, missing}` |
| POST | `ddict/clear/{sid}` | `{id}` |

An entry is `{id, name, managers_per_node, n_nodes, total_mem, created,
creator, state, descriptor}`; `total_mem` accepts bytes or `'512M'`.
Notification topic `ddict_status` → `{id, name, state}` on create
(`ready`) and destroy (`destroyed`).

## Example and containers

- `examples/example_dragon_ddict.py`: create, write, read, and (with
  `--runtime-check`) attach from a Dragon process.
- `examples/dragon/`: container image and docker-compose for a broker plus
  endpoint, or an endpoint that dials an external broker.

## Limits (POC)

Single-node runtimes in containers; no per-user ownership (any client can
destroy any DDict); no streaming, so values are bounded by the frame cap.
Further known limits:

- **No reclamation.** DDicts live until destroyed or until the endpoint
  stops: no TTL, quota or per-creator cap.  `creator` is recorded only.
- **Create bounds.** `managers_per_node <= 64`, `n_nodes <= 1024`,
  `total_mem <= 64 GiB` per node.  A started runtime is single-node
  (`n_nodes > 1` is a 400); an attached or enclosing runtime is asked for
  its node count once.  If that query fails, only the static bounds hold;
  a hanging one blocks the Dragon thread.
- **One instance per process.** A second `dragon` plugin instance with the
  default backend is refused until the first one is shut down.  Other
  Dragon users in the same process (e.g. rhapsody's `dragon_v3` or its
  Dragon data backend) run their calls on their own threads; whether they
  collide with the plugin's thread (concurrent DDict calls hang in 0.15.0)
  is not verified.
- **`$PATH`.** When no `dragon` launcher is on `$PATH`, starting a runtime
  prepends the interpreter's bin directory to `$PATH`, process-wide.
- **Not exercised in unit tests.** The real runtime start, attach, stop and
  detach, and the node-count query, need dragonhpc; the tests use fakes.
