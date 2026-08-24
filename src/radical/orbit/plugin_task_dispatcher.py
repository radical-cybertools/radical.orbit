'''
Task dispatcher plugin — elastic multi-pool task routing for radical.orbit.

Hosts one :class:`PoolState` per pool.  Each ``PoolState`` owns a dispatch
policy (resolved from ``PoolConfig.strategy`` through the manual registry
in :mod:`~radical.orbit.task_dispatcher_policy`; default ``conservative``),
a pilot ledger + pending task queue (one atomic ``state.json``), and a
shared-FS scratch area.

The dispatcher is a **broker-hosted plugin**: it runs on the broker's
plugin-host loop and reaches endpoints through the in-process broker caller.
Child plugin clients (``PSIJClient`` / ``RhapsodyClient``) are ordinary sync
clients whose transport is a blocking caller shim (:class:`_CallerSyncHTTP`);
the dispatcher drives their sync methods via ``asyncio.to_thread`` so the host
loop is never blocked.  Pilots are submitted via ``plugin_psij.submit_tunneled``
on a login-node endpoint.  When the pilot's child endpoint registers with the
broker, its appearance in the rich topology (``on_topology_change``) is the
dispatcher's signal that the pilot is ACTIVE — capacity is taken from the
pool's pilot-size config.  Tasks then flow via ``rhapsody.submit_tasks`` on the
child endpoint; completion arrives as broker ``event`` frames on the raw tap.

Pools are strictly per-session: keyed ``(owning_sid, pool_name)``, pool names
are session-local, and there is no cross-session attach.  A pool's pilots
follow the owning session's lifetime — session close (owner ``lost`` +
reclaim-drain, ttl expiry, or explicit ``cancel_all``) tears the pools down.
Pools arrive only via ``register_session``.
'''

from __future__ import annotations

import asyncio
import base64
import logging
import os
import shutil
import threading
import time
import uuid

import msgpack

from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .client                            import PluginClient
from .plugin_base                       import Plugin
from .plugin_session_base               import PluginSession
from .plugin_rhapsody                   import (
    RhapsodyClient, WS_PAYLOAD_LIMIT, NOTIFY_BATCH_SIZE,
    _payload_size, _resolve_notify_window, _resolve_frame_cap, BUDGET_RATIO,
)
from .task_dispatcher_config            import (
    PoolConfig, PilotSize, PoolConfigError,
    default_pool_config, parse_pools,
)
from .task_dispatcher_state             import (
    PilotRecord, TaskRecord, PoolStore,
    records_from, read_json, write_json_atomic,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE,
    PILOT_DONE, PILOT_FAILED, PILOT_LIVE_STATES,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
    TASK_TERMINAL_STATES,
)
from .task_dispatcher_policy               import make_policy

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_STATE_ROOT  = Path('~/.radical/orbit/task_dispatcher/state'
                            ).expanduser()
_DEFAULT_SCRATCH_ROOT = Path('~/.radical/orbit/task_dispatcher/scratch'
                             ).expanduser()

# State-directory pruning: a pool directory not backing an active pool AND
# older than this threshold is removed by the housekeeping tick.
_STATE_PRUNE_DAYS    = 30
_PRUNE_INTERVAL_SEC  = 86400.0   # stale-dir pruning: once a day

# Housekeeping tick frequency.
_TICK_INTERVAL_SEC = 5.0

# Handshake timeout — a pilot that hasn't handshaken in this long is
# reconciled against psij job state.
_HANDSHAKE_TIMEOUT_SEC = 300.0

# Rhapsody-dialect bulk submit (see ``_route_submit_rh``): task dicts in
# rhapsody's own wire format, a ``pool`` key per task.  Route template shared
# with :meth:`TaskDispatcherClient.submit_tasks` so the two cannot drift.
ROUTE_SUBMIT_RH = 'submit_rh/{sid}'

# Keys a rhapsody child notification carries that are worth forwarding to
# the dispatcher's own subscribers (mirrors plugin_rhapsody's
# ``_NOTIFICATION_KEYS``): the uid/state pair plus result and error data.
_RH_FORWARD_KEYS = {'uid', 'state', 'exit_code',
                    'return_value', '_return_value_encoding',
                    'error', 'exception', 'traceback'}


# ---------------------------------------------------------------------------
# Transport: sync child-plugin clients over the broker caller
# ---------------------------------------------------------------------------

class _CallerSyncHTTP:
    '''Blocking transport over the broker's in-process caller.

    Installed as a child plugin client's ``self._http`` in the broker-hosted
    dispatcher.  Each verb routes over ``caller.call_threadsafe`` and blocks on
    the returned future's ``.result()`` — safe because the dispatcher drives
    every client method through ``asyncio.to_thread``, so the blocking happens
    on a worker thread and the host loop is never held.  Responses are wrapped
    in :class:`~radical.orbit.runtime_client.RuntimeResponse`, so the same
    ``_raise`` / parsers every other transport uses apply unchanged.
    '''

    def __init__(self, caller, dst: str, timeout: float | None = None) -> None:
        self._caller  = caller
        self._dst     = dst
        self._timeout = timeout

    def request(self, method: str, url: str, *, json=None, content=None,
                data=None, params=None, headers=None, **_kw):
        from .runtime_client import (
            _pack_request, unpack_response_dict, RuntimeResponse)
        path, body, hdrs = _pack_request(
            method, url, json=json, content=content, data=data,
            params=params, headers=headers)
        fut  = self._caller.call_threadsafe(
            self._dst, method, path, body=body, headers=hdrs,
            timeout=self._timeout)
        resp = fut.result(self._timeout)
        status, rhdrs, rbody = unpack_response_dict(resp)
        return RuntimeResponse(status, rhdrs, rbody)

    def get(self, url: str, **kw):
        return self.request('GET', url, **kw)

    def post(self, url: str, **kw):
        return self.request('POST', url, **kw)


# ---------------------------------------------------------------------------
# PoolState — per-pool runtime state
# ---------------------------------------------------------------------------

class PoolState:
    '''Plugin-level runtime state for one pool.

    Distinct from :class:`PoolConfig`, which is the static declaration.
    :class:`PoolState` holds the live fleet, pending queue, and policy
    instance, and persists everything to one atomic ``state.json``.

    Concurrency model: all mutations happen from the plugin's asyncio event
    loop thread, so no in-state locking is needed.
    '''

    def __init__(self, config: PoolConfig, state_dir: Path,
                 scratch_base: Path,
                 plugin: 'PluginTaskDispatcher',
                 owning_sid: str = '') -> None:
        self.config       = config
        self.state_dir    = state_dir
        self.scratch_base = scratch_base
        self.owning_sid   = owning_sid
        self._plugin      = plugin

        state_dir.mkdir(parents=True, exist_ok=True)
        scratch_base.mkdir(parents=True, exist_ok=True)

        # Single atomic per-pool store: config + pilots + tasks.
        self.store = PoolStore(state_dir / 'state.json')
        payload = self.store.load()
        self.pilots: dict[str, PilotRecord] = records_from(
            payload.get('pilots'), PilotRecord)
        self.tasks:  dict[str, TaskRecord]  = records_from(
            payload.get('tasks'), TaskRecord)

        # Policy resolved by name through the manual registry (see
        # task_dispatcher_policy; default 'conservative').
        self.policy = make_policy(config)

    def pending_queue(self) -> list[TaskRecord]:
        '''Return pending tasks for this pool, priority-ordered.

        The dispatcher sorts here so the policy sees one canonical ordering.
        '''
        pending = [t for t in self.tasks.values()
                   if t.state == TASK_QUEUED]
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))
        return pending

    def live_pilots(self) -> list[PilotRecord]:
        '''Return live (non-terminal) pilots in this pool.'''
        return [p for p in self.pilots.values()
                if p.state in PILOT_LIVE_STATES]

    def task_scratch_dir(self, task_id: str) -> Path:
        '''Return (creating it) the shared-FS scratch dir for one task.'''
        d = self.scratch_base / task_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def persist(self) -> None:
        '''Rewrite this pool's ``state.json`` atomically.'''
        self.store.save(self.owning_sid, asdict(self.config),
                        self.pilots, self.tasks)

    def close(self) -> None:
        '''Release per-pool resources.  No-op (the store holds no handles).'''
        return None


# ---------------------------------------------------------------------------
# Session — thin identity handle
# ---------------------------------------------------------------------------

class TaskDispatcherSession(PluginSession):
    '''Session handle — owns this session's pools by lifetime.

    Pool/pilot state lives on :class:`PluginTaskDispatcher`, keyed by
    ``(owning_sid, pool_name)``.  The session is the owner: closing it (owner
    ``lost`` + reclaim-drain, ttl expiry, ``unregister_session``, or
    ``cancel_all``) tears down *this session's* pools — cancelling their pilots
    and marking the durable store — so a pool's pilots follow the session
    lifetime policy.
    '''

    async def close(self) -> dict:
        '''Tear down this session's pools, then run the base close.'''
        if self._plugin is not None:
            await self._plugin._teardown_session_pools(self._sid)
        return await super().close()


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class TaskDispatcherClient(PluginClient):
    '''Application-side client for the task dispatcher plugin.'''

    def register_session(self, pools: list | dict | None = None,
                         **_kwargs: Any) -> None:
        '''Register a session, optionally declaring per-workflow pools.

        *pools* may be a list of pool-config dicts or a dict containing a
        ``pools`` key.  ``None`` registers without declaring pools, which
        causes the dispatcher to auto-materialise this session's built-in
        ``default`` pool (idempotent across sessions).
        '''
        body: dict = {}
        if pools is not None:
            body['pools'] = pools
        resp = self._http.post(self._url('register_session'), json=body)
        self._raise(resp)
        self._sid = resp.json()['sid']

    def list_pools(self) -> dict:
        '''List configured pools and their live state (session-less).'''
        resp = self._http.get(self._url('pools'))
        self._raise(resp)
        return resp.json()

    def fleet(self) -> dict:
        '''Return a snapshot of the fleet across all pools (requires session).'''
        self._require_session()
        resp = self._http.get(self._url(f'fleet/{self.sid}'))
        self._raise(resp)
        return resp.json()

    def submit_task(self, task_id: str, cmd: list[str], cwd: str, *,
                    pool: str | None = None, endpoint: str | None = None,
                    priority: int = 0,
                    inputs: list[str] | None = None,
                    outputs: list[str] | None = None) -> dict:
        '''Submit one task to the dispatcher.

        Exactly one of *pool* or *endpoint* must be given:
            - *pool*: route through a dispatcher-managed pilot pool.
            - *endpoint*: bypass pool management and run directly on the
              target endpoint's rhapsody plugin.  Inputs/outputs are not
              supported in this mode (yet).
        '''
        self._require_session()
        if bool(pool) == bool(endpoint):
            raise ValueError(
                'submit_task requires exactly one of pool=... or endpoint=...')
        payload: dict = {
            'task_id' : task_id,
            'cmd'     : cmd,
            'cwd'     : cwd,
            'priority': priority,
            'inputs'  : inputs or [],
            'outputs' : outputs or [],
        }
        if pool is not None:
            payload['pool'] = pool
        else:
            payload['endpoint'] = endpoint
        resp = self._http.post(self._url(f'submit/{self.sid}'), json=payload)
        self._raise(resp, f'submit task {task_id!r}')
        return resp.json()

    def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        '''Bulk submit in the rhapsody execution dialect (pool mode).

        Same wire contract as :meth:`RhapsodyClient.submit_tasks` --
        cloudpickle serialization, client-assigned uids, frame-bounded
        batches -- plus a ``pool`` key per task naming its target pool.
        Mixed-pool batches are grouped by the dispatcher.  This is the
        verb rhapsody's ``OrbitExecutionBackend`` calls, which is what
        lets it point at the dispatcher unchanged.
        '''
        self._require_session()

        for td in task_dicts:
            if not td.get('pool'):
                raise ValueError("each task dict requires a 'pool' key")
            RhapsodyClient._serialize_task(td)
            if 'uid' not in td:
                td['uid'] = f'task.{uuid.uuid4().hex[:8]}'

        url = self._url(ROUTE_SUBMIT_RH.format(sid=self.sid))

        # frame-size-bounded batches, same split as the rhapsody client
        batches: list[list[dict]] = []
        batch: list[dict]         = []
        batch_bytes               = 0
        for td in task_dicts:
            td_size = len(str(td)) + 2
            if batch and batch_bytes + td_size > WS_PAYLOAD_LIMIT:
                batches.append(batch)
                batch       = []
                batch_bytes = 0
            batch.append(td)
            batch_bytes += td_size
        if batch:
            batches.append(batch)

        results: list[dict] = []
        for b in batches:
            resp = self._http.post(
                url,
                data=msgpack.packb({'tasks': b}, use_bin_type=True),
                headers={'Content-Type': 'application/msgpack'})
            self._raise(resp, f'submit {len(b)} task(s)')
            results.extend(resp.json())
        return results

    def get_task(self, task_id: str) -> dict:
        '''Fetch the current :class:`TaskRecord` for *task_id*.'''
        self._require_session()
        resp = self._http.get(self._url(f'task/{self.sid}/{task_id}'))
        self._raise(resp)
        return resp.json()

    def cancel_task(self, task_id: str) -> dict:
        '''Cancel a task.  Idempotent on already-terminal records.'''
        self._require_session()
        resp = self._http.post(self._url(f'cancel/{self.sid}/{task_id}'))
        self._raise(resp, f'cancel task {task_id!r}')
        return resp.json()

    def cancel_all(self) -> dict:
        '''Tear down this session's pools: cancel their pilots, drop the pools.

        The explicit reclaim path for ``persistent``/``default`` pools, which
        have no liveness-driven expiry.
        '''
        self._require_session()
        resp = self._http.post(self._url(f'cancel_all/{self.sid}'))
        self._raise(resp, 'cancel_all')
        return resp.json()

    def stage_in(self, pool: str, task_id: str, filename: str,
                 content: bytes, overwrite: bool = False) -> dict:
        '''Upload one file into a task's scratch dir.  Returns ``{cwd, size}``.

        v1 uses a single base64-in-JSON body per file; bulk-transfer
        optimisation is deferred.
        '''
        self._require_session()
        payload = {
            'pool'       : pool,
            'filename'   : filename,
            'content_b64': base64.b64encode(content).decode('ascii'),
            'overwrite'  : overwrite,
        }
        resp = self._http.post(
            self._url(f'stage_in/{self.sid}/{task_id}'), json=payload)
        self._raise(resp, f'stage_in {filename!r}')
        return resp.json()

    def stage_out(self, task_id: str, filename: str) -> bytes:
        '''Download one file from a task's scratch dir.  Returns raw bytes.'''
        self._require_session()
        resp = self._http.get(self._url(
            f'stage_out/{self.sid}/{task_id}/{filename}'))
        self._raise(resp, f'stage_out {filename!r}')
        body = resp.json()
        return base64.b64decode(body['content_b64'])


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginTaskDispatcher(Plugin):
    '''Broker-hosted task dispatcher: elastic pilot pools + per-pool policy.'''

    plugin_name   = 'task_dispatcher'
    session_class = TaskDispatcherSession
    client_class  = TaskDispatcherClient
    version       = '0.0.1'

    ui_config = {
        'icon'          : '📦',
        'title'         : 'Task Dispatcher',
        'description'   : 'Elastic autoscaling task dispatcher: pools, pilots.',
        'refresh_button': True,
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Return whether to load: broker hosts only.

        The dispatcher owns the global pool/pilot/task state, observes topology
        events directly, and proxies psij calls out to login-node endpoints.
        '''
        from .utils import host_role
        return host_role(app)['role'] == 'broker'

    def __init__(self, app: FastAPI,
                 instance_name: str = 'task_dispatcher',
                 state_root: str | os.PathLike | None = None,
                 scratch_root: str | os.PathLike | None = None) -> None:
        super().__init__(app, instance_name)

        self._state_root   = Path(state_root   or _DEFAULT_STATE_ROOT)
        self._scratch_root = Path(scratch_root or _DEFAULT_SCRATCH_ROOT)

        # Broker seam (broker-hosted only): the in-process caller handle and
        # the raw event tap, injected by BrokerPluginHost.  When absent the
        # dispatcher refuses endpoint calls cleanly (child clients return None).
        self._broker_caller = getattr(app.state, 'broker_caller', None)
        self._broker_tap    = getattr(app.state, 'broker_tap', None)
        self._untap         = None

        # Cached child-endpoint plugin clients (sync PSIJClient/RhapsodyClient
        # over the caller), keyed (dst, plugin, backend).  Invalidated for a
        # dst when it goes ``lost``.
        self._child_clients: dict[tuple, Any] = {}

        # endpoint_name → set of loaded plugin names, refreshed on every
        # topology change.  Used to auto-resolve a pool's endpoint_name and to
        # validate the target of an endpoint-mode task submission.
        self._connected_endpoints: dict[str, set[str]] = {}

        # Endpoint-mode task tracking: task_id → target_endpoint_name.
        # Endpoint mode bypasses pool state — the dispatcher is a transparent
        # proxy to the target endpoint's rhapsody.  Backed by one atomic
        # ``endpoint_mode.json`` so an in-flight task survives a broker restart.
        self._endpoint_mode_path = self._state_root / 'endpoint_mode.json'
        self._endpoint_mode_tasks: dict[str, str] = dict(
            read_json(self._endpoint_mode_path, default={}) or {})

        # rhapsody-task-uid → (owning_sid, pool_name, task_id) so the event tap
        # can find the right TaskRecord when a pilot reports completion.
        self._uid_to_task: dict[str, tuple[str, str, str]] = {}

        # Rhapsody-dialect batching (same NOTIFY_WINDOW semantics as
        # plugin_rhapsody): terminal notifications coalesce into
        # ``task_status_batch`` frames, and ledger persists coalesce into one
        # write per dirty pool per flush -- the per-task fsync is the
        # dominant latency cost of pool mode.
        self._notify_window       = _resolve_notify_window()
        self._notify_batch_bytes  = _resolve_frame_cap(app) // BUDGET_RATIO
        self._rh_notify_buf: list[dict]           = []
        self._rh_notify_bytes                     = 0
        self._rh_notify_lock                      = threading.Lock()
        self._rh_flush_scheduled                  = False
        self._dirty_pools: set[tuple[str, str]]   = set()

        # Single housekeeping loop; started once a running loop exists.
        self._started = False
        self._housekeeping_task: asyncio.Task | None = None

        # Pool state, keyed strictly per session: {owning_sid: {pool_name:
        # PoolState}}.  Sessions declare pools via :meth:`register_session`.
        self._pool_states: dict[str, dict[str, PoolState]] = {}

        # Restart-time replay: rebuild pool/pilot/task bookkeeping for ALL
        # sessions from the sid-scoped durable store.
        self._replay_state()

        # Start the housekeeping loop + event tap if a loop is already running
        # (the broker constructs hosted plugins on the running host loop).
        self._maybe_start()

        # Routes
        self.add_route_get  ('pools',                         self._route_pools)
        self.add_route_get  ('pool/{sid}/{name}',             self._route_pool_detail)
        self.add_route_get  ('fleet/{sid}',                   self._route_fleet)
        self.add_route_post ('submit/{sid}',                  self._route_submit)
        self.add_route_post (ROUTE_SUBMIT_RH,                 self._route_submit_rh)
        self.add_route_get  ('task/{sid}/{task_id}',          self._route_get_task)
        self.add_route_post ('cancel/{sid}/{task_id}',        self._route_cancel_task)
        self.add_route_post ('cancel_all/{sid}',              self._route_cancel_all)
        self.add_route_post ('stage_in/{sid}/{task_id}',      self._route_stage_in)
        self.add_route_get  ('stage_out/{sid}/{task_id}/{filename}',
                             self._route_stage_out)

    # -- pool bookkeeping helpers ---------------------------------------

    def _pools_for(self, sid: str) -> dict[str, 'PoolState']:
        '''Return this session's pools (empty for a fresh sid).'''
        return self._pool_states.setdefault(sid, {})

    def _all_pools(self):
        '''Iterate every pool across every session.'''
        for pools in list(self._pool_states.values()):
            for ps in list(pools.values()):
                yield ps

    def _find_pool(self, sid: str, name: str) -> 'PoolState | None':
        '''Return this session's pool by name, or ``None`` (session-local).'''
        return self._pool_states.get(sid, {}).get(name)

    # -- materialisation ------------------------------------------------

    def _pool_dir(self, sid: str, cfg: PoolConfig) -> Path:
        '''Return the sid-scoped, endpoint-tagged on-disk state dir for a pool.'''
        endpoint_tag = cfg.endpoint_name or 'unbound'
        return self._state_root / sid / f'{cfg.name}__{endpoint_tag}'

    def _scratch_for(self, cfg: PoolConfig) -> Path:
        '''Return the scratch base for *cfg*.'''
        return (Path(cfg.scratch_base).expanduser()
                if cfg.scratch_base
                else self._scratch_root / cfg.name)

    def _materialise_pool(self, sid: str, cfg: PoolConfig) -> 'PoolState':
        '''Create (or return) this session's pool named ``cfg.name``.

        Pools are strictly per-session (keyed ``(sid, cfg.name)``): a
        same-named pool in another session is a *distinct* pool.  Re-declaring
        a pool already present under this session returns the existing
        :class:`PoolState` (idempotent reconnect).
        '''
        if cfg.endpoint_name is None:
            picked = self._pick_endpoint_name()
            if picked:
                cfg.endpoint_name = picked
                log.info('[%s] pool %r: endpoint_name auto-resolved to %r',
                         self.instance_name, cfg.name, picked)

        pools    = self._pools_for(sid)
        existing = pools.get(cfg.name)
        if existing is not None:
            return existing

        state_dir    = self._pool_dir(sid, cfg)
        scratch_base = self._scratch_for(cfg)
        ps = PoolState(cfg, state_dir, scratch_base, self, owning_sid=sid)
        pools[cfg.name] = ps

        # Persist immediately so restart-time replay can rebuild this pool.
        ps.persist()

        # Restart recovery: rebuild the uid→task map from the replayed task log
        # so a terminal event for a task that was RUNNING before the restart
        # can still be correlated and advanced.
        for rec in ps.tasks.values():
            if rec.state == TASK_RUNNING and rec.rhapsody_uid:
                self._uid_to_task[rec.rhapsody_uid] = (sid, cfg.name, rec.task_id)

        log.info('[%s] materialised pool %r (sid=%s) → endpoint %r (sizes=%s)',
                 self.instance_name, cfg.name, sid, cfg.endpoint_name,
                 sorted(cfg.pilot_sizes))
        return ps

    def _replay_state(self) -> None:
        '''Rebuild pools for ALL sessions from the sid-scoped durable store.

        Scan ``<state_root>/<sid>/<pool>__<endpoint>/state.json``, reload each
        pool's config, and re-materialise its :class:`PoolState` (which loads
        the pilot/task maps).  Owner reconnection then reconciles naturally; a
        pool whose owner never returns is drained by the reclaim / ttl path.
        '''
        if not self._state_root.exists():
            return
        for sid_dir in sorted(self._state_root.iterdir()):
            if not sid_dir.is_dir():
                continue
            sid = sid_dir.name
            for pool_dir in sorted(sid_dir.iterdir()):
                cfg_path = pool_dir / 'state.json'
                if not (pool_dir.is_dir() and cfg_path.is_file()):
                    continue
                payload = read_json(cfg_path, default=None)
                if not isinstance(payload, dict) or 'config' not in payload:
                    continue
                try:
                    cfg = self._pool_config_from_dict(payload['config'])
                    self._materialise_pool(sid, cfg)
                except Exception as e:
                    # Any failure to parse the config, instantiate the policy
                    # (unregistered strategy, bad strategy_config), or
                    # materialise the pool (e.g. OSError on mkdir) must not
                    # abort replay of every other pool — skip this one.
                    log.warning('task_dispatcher: skipping unreplayable pool '
                                '%s: %s', cfg_path, e)
                    continue

    @staticmethod
    def _pool_config_from_dict(d: dict) -> PoolConfig:
        '''Reconstruct a :class:`PoolConfig` from a persisted ``asdict``.'''
        parsed = parse_pools({'pools': [d]}, source='replay')
        return next(iter(parsed.values()))

    def _pick_endpoint_name(self) -> str | None:
        '''Auto-pick an endpoint_name for a pool declared without one.

        Policy: lexically first connected endpoint that isn't us (the broker
        endpoint).  Returns ``None`` when no eligible endpoint is available.
        '''
        self_endpoint = getattr(self._app.state, 'endpoint_name', None)
        candidates = sorted(e for e in self._connected_endpoints
                            if e != self_endpoint)
        return candidates[0] if candidates else None

    # -- lifecycle ------------------------------------------------------

    def _maybe_start(self) -> None:
        '''Idempotently start the housekeeping loop + broker event-tap sub.

        Started only when a loop is already running (the broker constructs
        hosted plugins on the running host loop).  Unit tests without a loop
        drive routes and callbacks directly.
        '''
        if self._started:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._started = True
        self._housekeeping_task = loop.create_task(self._housekeeping())

        # Subscribe to the broker's raw event tap for child-pilot task events.
        # The tap fires on the plugin-host loop — the dispatcher's own loop —
        # so terminal handling runs inline with no cross-thread marshalling.
        if self._broker_tap is not None and self._untap is None:
            self._untap = self._broker_tap(self._on_event)

    async def _housekeeping(self) -> None:
        '''One periodic loop: per-pool tick + drain, then handshake + prune.

        Replaces the four separate sweeper loops.  Ticks every
        ``_TICK_INTERVAL_SEC``; reconciles overdue pilot handshakes each tick;
        prunes stale state dirs at most once per ``_PRUNE_INTERVAL_SEC``.
        '''
        last_prune = time.time()
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                now = time.time()
                for ps in list(self._all_pools()):
                    ps.policy.on_tick(ps, self._make_submit_pilot(ps))
                    self._drain_pending(ps)
                await self._reconcile_overdue_pilots(now)
                if now - last_prune >= _PRUNE_INTERVAL_SEC:
                    self._prune_stale_state_dirs()
                    last_prune = now
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] housekeeping error: %s',
                              self.instance_name, e)

    async def shutdown(self) -> None:
        '''Cancel the housekeeping loop and drop the event tap, then base close.'''
        # anything still coalescing gets its persist and its notification now
        self._flush_rh()
        if self._housekeeping_task is not None \
                and not self._housekeeping_task.done():
            self._housekeeping_task.cancel()
            try:    await self._housekeeping_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._untap is not None:
            try:    self._untap()
            except Exception:
                pass
            self._untap = None
        await super().shutdown()

    async def _reconcile_overdue_pilots(self, now: float) -> None:
        '''Reconcile PENDING/STARTING pilots overdue for a handshake.

        For each such pilot older than ``_HANDSHAKE_TIMEOUT_SEC``, query psij
        for its job state and mark it FAILED if the job is terminal.
        '''
        for pool_state in list(self._all_pools()):
            for pilot in list(pool_state.pilots.values()):
                if pilot.state not in (PILOT_PENDING, PILOT_STARTING):
                    continue
                if now - pilot.submitted_at < _HANDSHAKE_TIMEOUT_SEC:
                    continue
                await self._reconcile_pilot(pool_state, pilot)

    def _prune_stale_state_dirs(self) -> None:
        '''Remove state dirs for pools no longer active and older than 30 days.

        The store is sid-scoped: ``<state_root>/<sid>/<pool>__<endpoint>/``.  A
        pool dir is pruned iff it isn't backing an active pool AND its newest
        file is older than ``_STATE_PRUNE_DAYS``; an emptied sid dir is then
        removed too.
        '''
        if not self._state_root.exists():
            return
        cutoff = time.time() - _STATE_PRUNE_DAYS * 86400
        active = {str(ps.state_dir) for ps in self._all_pools()}
        for sid_dir in list(self._state_root.iterdir()):
            if not sid_dir.is_dir():
                continue
            for entry in list(sid_dir.iterdir()):
                if not entry.is_dir() or str(entry) in active:
                    continue
                try:
                    mtimes = [p.stat().st_mtime for p in entry.iterdir()]
                except (FileNotFoundError, PermissionError):
                    continue
                if not mtimes or max(mtimes) >= cutoff:
                    continue
                try:
                    shutil.rmtree(entry)
                    log.info('[%s] pruned stale state dir %s',
                             self.instance_name, entry)
                except OSError as e:
                    log.warning('[%s] could not prune %s: %s',
                                self.instance_name, entry, e)
            try:
                if sid_dir.is_dir() and not any(sid_dir.iterdir()):
                    sid_dir.rmdir()
            except OSError:
                pass

    # -- child plugin clients (sync, over the broker caller) --------------

    def _make_child_client(self, client_cls, plugin: str, dst: str):
        '''Build a sync plugin client whose transport is the broker caller.

        The dispatcher drives psij/rhapsody through the very same
        ``PSIJClient`` / ``RhapsodyClient`` sync helpers the user-thread clients
        run — one implementation of paths, payloads, parsing and error mapping.
        Only the transport differs: :class:`_CallerSyncHTTP` routes over the
        in-process caller.  The client's sync methods are always invoked from
        ``asyncio.to_thread`` so the blocking ``.result()`` never holds the host
        loop.
        '''
        http = _CallerSyncHTTP(self._broker_caller, dst)
        return client_cls(http, f'/{plugin}',
                          endpoint_id=dst, plugin_name=plugin)

    async def _get_psij_client(self, endpoint_name: str):
        '''Return a caller-backed :class:`PSIJClient` for *endpoint_name*.

        Registers (once) a psij session over the caller and caches the client
        per ``(dst, 'psij', None)``.  Returns ``None`` when no caller is wired
        or the endpoint / psij plugin is unreachable.
        '''
        if not endpoint_name:
            log.warning('[%s] _get_psij_client called with empty endpoint_name',
                        self.instance_name)
            return None
        if self._broker_caller is None:
            return None
        key    = (endpoint_name, 'psij', None)
        client = self._child_clients.get(key)
        if client is not None:
            return client
        from .plugin_psij import PSIJClient
        client = self._make_child_client(PSIJClient, 'psij', endpoint_name)
        try:
            await asyncio.to_thread(client.register_session)
        except Exception as e:
            log.warning('[%s] psij session unavailable on %s: %s',
                        self.instance_name, endpoint_name, e)
            return None
        self._child_clients[key] = client
        return client

    async def _get_rhapsody_client(self, child_endpoint: str,
                                   backend: str | None = None):
        '''Return a caller-backed :class:`RhapsodyClient` for a child.

        Registers the rhapsody session (with *backend* when given), caches the
        client per ``(dst, 'rhapsody', backend)``.  Returns ``None`` when no
        caller is wired or the child / rhapsody plugin is unreachable.
        '''
        if self._broker_caller is None:
            return None
        key    = (child_endpoint, 'rhapsody', backend)
        client = self._child_clients.get(key)
        if client is not None:
            return client
        from .plugin_rhapsody import RhapsodyClient
        client = self._make_child_client(RhapsodyClient, 'rhapsody',
                                         child_endpoint)
        try:
            await asyncio.to_thread(
                client.register_session,
                backends=[backend] if backend else None)
        except Exception as e:
            log.warning('[%s] rhapsody session unavailable on %s: %s',
                        self.instance_name, child_endpoint, e)
            return None
        self._child_clients[key] = client
        return client

    # -- routes --------------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Override the base ``register_session`` to accept pool declarations.

        Request body (JSON, all fields optional)::

            {"pools": [<PoolConfig>, ...]}

        Materialisation semantics (strict per-session isolation):

        - The base allocates/reconnects the session ``sid`` first; declared
          pools then materialise under **that** ``sid``.
        - Pools are keyed ``(owning_sid, pool_name)``: a same-named pool in
          another session is a *distinct*, isolated pool.  Re-declaring a pool
          this session already owns returns the existing one.
        - With no pools declared and no pool yet owned by this session, the
          dispatcher auto-materialises this session's own ``default`` pool.
        '''
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}

        pools_body = body.get('pools')

        # Validate/parse the declared pools BEFORE minting the session, so a
        # bad declaration 400s without leaving a dangling session behind.
        configs: dict = {}
        if pools_body is not None:
            if isinstance(pools_body, list):
                wrapped = {'pools': pools_body}
            elif isinstance(pools_body, dict) and 'pools' in pools_body:
                wrapped = pools_body
            else:
                raise HTTPException(
                    status_code=400,
                    detail="'pools' must be a list or {'pools': [...]}")
            try:
                configs = parse_pools(wrapped, source='register_session')
            except PoolConfigError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

        # Base sid-allocation / owner-check / cleanup runs first so pools can
        # be keyed on the resolved sid.
        result = await super().register_session(request)
        sid    = result['sid']

        if configs:
            for cfg in configs.values():
                self._materialise_pool(sid, cfg)
        elif pools_body is None and not self._pools_for(sid):
            # No pools declared; auto-materialise this session's default once.
            self._materialise_pool(sid, default_pool_config())

        return result

    async def _route_pools(self, request: Request) -> dict:
        '''List all pools across sessions, grouped by owning session id.'''
        return {
            'pools': {
                sid: {name: self._summarize_pool(ps)
                      for name, ps in pools.items()}
                for sid, pools in self._pool_states.items()
            }
        }

    async def _route_pool_detail(self, request: Request) -> dict:
        '''Return detailed state for one of this session's pools by name.

        Session-scoped (like ``fleet/{sid}``): a pool is addressable only under
        its owning session, never by bare name across sessions.
        '''
        sid  = request.path_params['sid']
        self._require_known_session(sid)
        name = request.path_params['name']
        ps = self._find_pool(sid, name)
        if ps is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown pool: {name}')
        return self._summarize_pool(ps, verbose=True)

    async def _route_fleet(self, request: Request) -> dict:
        '''Return this session's fleet snapshot.  Strictly isolated.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        return {
            'pools': {
                name: self._summarize_pool(ps, verbose=True)
                for name, ps in self._pool_states.get(sid, {}).items()
            }
        }

    async def _route_submit(self, request: Request) -> dict:
        '''Submit a task in pool mode or endpoint mode.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        body = await request.json()

        pool_name   = body.get('pool')
        target_endpoint = body.get('endpoint')
        task_id     = body.get('task_id')
        cmd         = body.get('cmd')
        cwd         = body.get('cwd')

        # Mutual exclusion: exactly one of 'pool' / 'endpoint' is required.
        if bool(pool_name) == bool(target_endpoint):
            raise HTTPException(
                status_code=400,
                detail="submit requires exactly one of 'pool' or 'endpoint'")
        if not task_id or not cmd or not cwd:
            raise HTTPException(
                status_code=400,
                detail="submit requires 'task_id', 'cmd', 'cwd'")

        # ---------- endpoint mode: transparent proxy to target's rhapsody ----
        if target_endpoint:
            return await self._route_submit_endpoint_mode(
                target_endpoint, task_id, cmd, cwd, body)

        # ---------- pool mode: dispatcher-managed pilot fleet ------------
        pool_state = self._find_pool(sid, pool_name)
        if not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')

        priority = int(body.get('priority', 0))
        inputs   = list(body.get('inputs',  []) or [])
        outputs  = list(body.get('outputs', []) or [])

        # Cached-state behaviour on resubmit:
        #   DONE            → return cached (crash-recovery)
        #   RUNNING/QUEUED  → attach to existing wait (wrapper reconnect)
        #   FAILED/CANCELED → overwrite, re-execute (retry)
        existing = pool_state.tasks.get(task_id)
        if existing is not None:
            if existing.state == TASK_DONE:
                log.info('[%s] task %s DONE cached; returning without '
                         're-execution', self.instance_name, task_id)
                return self._task_dict(existing)
            if existing.state in (TASK_RUNNING, TASK_QUEUED):
                log.info('[%s] task %s already %s; attaching',
                         self.instance_name, task_id, existing.state)
                return self._task_dict(existing)
            # FAILED / CANCELED → re-execute: fall through and overwrite

        now = time.time()
        record = TaskRecord(
            task_id      = task_id,
            pool         = pool_name,
            owning_sid   = sid,
            cmd          = list(cmd),
            cwd          = str(cwd),
            priority     = priority,
            inputs       = inputs,
            outputs      = outputs,
            state        = TASK_QUEUED,
            submitted_at = now,
            arrival_ts   = now,
        )
        pool_state.tasks[task_id] = record
        pool_state.persist()

        self._dispatch_notify('task_status', self._task_dict(record))

        # Drain any ready dispatches now (the policy scales up on the tick).
        self._drain_pending(pool_state)

        return self._task_dict(record)

    async def _route_submit_endpoint_mode(
            self, target_endpoint: str, task_id: str,
            cmd: list, cwd: str, body: dict) -> dict:
        '''Endpoint-mode submit: transparent proxy to target's rhapsody.

        No pool, no state log, no pilot fleet — the dispatcher just forwards the
        task to the target endpoint's rhapsody session and records
        ``task_id -> target_endpoint`` so subsequent get/cancel can route back.
        The mapping is cleared when the task hits a terminal state.
        '''
        plugins = self._connected_endpoints.get(target_endpoint)
        if plugins is None:
            raise HTTPException(
                status_code=404,
                detail=f'unknown endpoint: {target_endpoint}')
        if 'rhapsody' not in plugins:
            raise HTTPException(
                status_code=503,
                detail=f'endpoint {target_endpoint} cannot run tasks')
        if body.get('inputs') or body.get('outputs'):
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        rh = await self._get_rhapsody_client(target_endpoint)
        if rh is None:
            raise HTTPException(
                status_code=503,
                detail=f'rhapsody client unavailable on {target_endpoint}')

        task_dict = {
            'uid'       : task_id,
            'executable': cmd[0] if cmd else '',
            'arguments' : list(cmd[1:]) if len(cmd) > 1 else [],
            'cwd'       : cwd,
            'task_backend_specific_kwargs': {'cwd': cwd},
        }
        try:
            result = await asyncio.to_thread(rh.submit_tasks, [task_dict])
        except Exception as e:
            log.exception('[%s] endpoint-mode submit to %s failed: %s',
                          self.instance_name, target_endpoint, e)
            raise HTTPException(
                status_code=502,
                detail=f'rhapsody submit failed on '
                       f'{target_endpoint}: {e}') from e

        self._endpoint_mode_tasks[task_id] = target_endpoint
        self._persist_endpoint_mode()
        return {
            'task_id' : task_id,
            'endpoint': target_endpoint,
            'state'   : TASK_RUNNING,
            'cmd'     : list(cmd),
            'cwd'     : str(cwd),
            'result'  : result[0] if result else None,
        }

    async def _route_submit_rh(self, request: Request) -> list[dict]:
        '''Bulk submit in the rhapsody execution dialect.

        Body: ``{"tasks": [<task dict>, ...]}`` — rhapsody's own wire format
        (cloudpickled fields as base64 strings, client-assigned ``uid``),
        plus a ``pool`` key per task.  A mixed-pool batch is grouped here;
        the dicts are forwarded verbatim to pilot rhapsody sessions, so the
        dispatcher never deserializes a function body.

        One ledger persist per touched pool, however many tasks the batch
        carries.  The response is rhapsody-shaped acks (``{uid, state}``),
        and completions arrive as ``task_status`` / ``task_status_batch``
        notifications under this plugin's namespace — the same consumer
        contract as plugin_rhapsody.
        '''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        data       = await request.json()
        task_dicts = data.get('tasks', [])

        # validate the whole batch before touching any state
        grouped: dict[str, list[dict]] = {}
        for td in task_dicts:
            uid       = td.get('uid')
            pool_name = td.get('pool')
            if not uid or not pool_name:
                raise HTTPException(
                    status_code=400,
                    detail="each task requires 'uid' and 'pool'")
            if self._find_pool(sid, pool_name) is None:
                raise HTTPException(
                    status_code=404,
                    detail=f'unknown pool: {pool_name}')
            grouped.setdefault(pool_name, []).append(td)

        now  = time.time()
        acks = []
        for pool_name, tds in grouped.items():
            pool_state = self._find_pool(sid, pool_name)
            if pool_state is None:       # validated above; mollify the checker
                continue
            fresh = False
            for td in tds:
                uid = str(td['uid'])

                # same resubmit semantics as exec-mode submit: DONE is
                # cached, live attaches, FAILED/CANCELED re-executes
                existing = pool_state.tasks.get(uid)
                if existing is not None and existing.state in (
                        TASK_DONE, TASK_RUNNING, TASK_QUEUED):
                    acks.append({'uid': uid, 'state': existing.state})
                    continue

                td = dict(td)
                td.pop('pool', None)
                pool_state.tasks[uid] = TaskRecord(
                    task_id      = uid,
                    pool         = pool_name,
                    owning_sid   = sid,
                    cmd          = [],
                    cwd          = '',
                    task_dict    = td,
                    state        = TASK_QUEUED,
                    submitted_at = now,
                    arrival_ts   = now,
                )
                acks.append({'uid': uid, 'state': TASK_QUEUED})
                fresh = True

            if fresh:
                pool_state.persist()          # once per pool, not per task
                self._drain_pending(pool_state)

        return acks

    async def _route_get_task(self, request: Request) -> dict:
        '''Return one task's record (pool mode) or rhapsody info (endpoint mode).'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']

        # Endpoint mode: forward to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await self._get_rhapsody_client(endpoint_name)
            if rh is None:
                raise HTTPException(
                    status_code=503,
                    detail=f'rhapsody client unavailable on {endpoint_name}')
            try:
                info = await asyncio.to_thread(rh.get_task, task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody get_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name,
                    'result': info}

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return self._task_dict(rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_task(self, request: Request) -> dict:
        '''Cancel one task (pool mode) or forward the cancel (endpoint mode).'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']

        # Endpoint mode: forward cancel to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await self._get_rhapsody_client(endpoint_name)
            if rh is None:
                raise HTTPException(
                    status_code=503,
                    detail=f'rhapsody client unavailable on {endpoint_name}')
            try:
                info = await asyncio.to_thread(rh.cancel_task, task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody cancel_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name,
                    'result': info}

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return await self._cancel_task(ps, rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_all(self, request: Request) -> dict:
        '''Tear down this session's pools (cancel pilots, drop the pools).

        The explicit reclaim path for ``persistent``/``default`` pools, which
        have no liveness-driven expiry.  Idempotent.
        '''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        n = await self._teardown_session_pools(sid)
        return {'sid': sid, 'pools_reclaimed': n}

    @staticmethod
    def _check_filename(name: str) -> None:
        '''Reject a staging filename with a slash, ``..``, or empty component.

        Files live at the top of the task scratch dir; relative subpaths are
        not supported (they complicate safety).
        '''
        if '/' in name or '\\' in name or name in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for staging: {name!r}')

    async def _route_stage_in(self, request: Request) -> dict:
        '''Upload one base64 file into a pool task's scratch dir.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id = request.path_params['task_id']
        body    = await request.json()

        if task_id in self._endpoint_mode_tasks:
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        pool_name   = body.get('pool')
        filename    = body.get('filename')
        content_b64 = body.get('content_b64')
        overwrite   = bool(body.get('overwrite', False))

        if not pool_name or not filename or content_b64 is None:
            raise HTTPException(
                status_code=400,
                detail="stage_in requires 'pool', 'filename', 'content_b64'")

        pool_state = self._find_pool(sid, pool_name)
        if not pool_state:
            raise HTTPException(
                status_code=404,
                detail=f'unknown pool: {pool_name}')

        self._check_filename(filename)

        try:
            content = base64.b64decode(content_b64)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f'invalid base64: {e}') from e

        scratch = pool_state.task_scratch_dir(task_id)
        path    = scratch / filename
        if path.exists() and not overwrite:
            raise HTTPException(
                status_code=409,
                detail=f'file exists (set overwrite=true): {path}')

        path.write_bytes(content)
        return {'cwd': str(scratch), 'size': len(content)}

    async def _route_stage_out(self, request: Request) -> dict:
        '''Download one file from a pool task's scratch dir.'''
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id  = request.path_params['task_id']
        filename = request.path_params['filename']

        if task_id in self._endpoint_mode_tasks:
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        self._check_filename(filename)

        for ps in self._pool_states.get(sid, {}).values():
            rec = ps.tasks.get(task_id)
            if rec is None:
                continue
            scratch = ps.scratch_base / task_id
            path    = scratch / filename
            if not path.is_file():
                raise HTTPException(
                    status_code=404,
                    detail=f'output not found: {path}')
            content = path.read_bytes()
            return {
                'filename'   : filename,
                'size'       : len(content),
                'content_b64': base64.b64encode(content).decode('ascii'),
            }

        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def on_topology_change(self, participants: dict) -> None:
        '''Rich-topology hook: bind pilots + honour the owner-session reclaim.

        The broker delivers the rich topology
        (``name -> {role, plugins, liveness}``) on every change, synthesizing a
        ``liveness == 'lost'`` entry for a participant that vanishes after the
        grace.  Two concerns share this signal:

        - **Pilot child liveness.**  A child that becomes ``present`` activates
          a PENDING/STARTING pilot; ``suspect`` pauses scheduling to it (a blip
          must not tear it down); ``lost`` finalises the pilot — DONE if
          walltime elapsed, else FAILED — reclaiming its capacity and
          re-enqueuing unfinished tasks.
        - **Owner-session reclaim.**  ``super().on_topology_change`` arms the
          reclaim-drain for a *session owner* declared ``lost``; the drain then
          closes the session, which tears down its pools.

        Also refreshes the cached per-endpoint plugin set from the non-lost
        participants.
        '''
        participants = participants or {}

        # Refresh {endpoint_name: set(plugin_names)} from the non-lost,
        # non-self participants.
        self_name = getattr(self._app.state, 'endpoint_name', None)
        new: dict[str, set[str]] = {}
        for name, info in participants.items():
            if name == self_name:
                continue
            if (info or {}).get('liveness') == 'lost':
                # Drop cached plugin-clients on a lost endpoint so a
                # reconnecting one re-registers fresh.
                for key in [k for k in self._child_clients if k[0] == name]:
                    self._child_clients.pop(key, None)
                continue
            plugins = (info or {}).get('plugins', {})
            if isinstance(plugins, dict):
                plugins = list(plugins.keys())
            new[name] = set(plugins)
        self._connected_endpoints = new

        for ps in self._all_pools():
            self._reconcile_pilots_for(ps, participants)

        # Owner-session reclaim-drain: arms on a *lost* owner of ephemeral
        # sessions, cancels on its return.
        await super().on_topology_change(participants)

    def _reconcile_pilots_for(self, ps: 'PoolState',
                              participants: dict) -> None:
        '''Apply child-endpoint liveness to one pool's pilots.'''
        for pilot in list(ps.pilots.values()):
            ce = pilot.child_endpoint_name
            if not ce or pilot.state not in PILOT_LIVE_STATES:
                continue
            info     = participants.get(ce)
            liveness = (info or {}).get('liveness') if info else None

            if liveness == 'present':
                # Un-pause a pilot paused on a suspect blip.
                if pilot.state == PILOT_ACTIVE and not pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = True
                    ps.persist()
                    self._drain_pending(ps)
                if pilot.state in (PILOT_PENDING, PILOT_STARTING):
                    self._activate_pilot(ps, pilot)

            elif liveness == 'suspect':
                # Pause scheduling to a suspect child (do NOT demote).
                if pilot.state == PILOT_ACTIVE and pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = False
                    ps.persist()

            elif liveness == 'lost':
                if time.time() >= pilot.walltime_deadline:
                    self._mark_pilot_done(ps, pilot, 'walltime reached')
                else:
                    self._mark_pilot_failed(
                        ps, pilot, 'child endpoint lost before walltime')
                self._drain_pending(ps)

    def _activate_pilot(self, ps: PoolState, pilot: PilotRecord) -> None:
        '''Transition a PENDING/STARTING pilot to ACTIVE on child handshake.'''
        size = ps.config.pilot_sizes.get(pilot.size_key)
        capacity = (size.nodes * size.cpus_per_node) if size else 0
        if capacity <= 0:
            log.warning('[%s] cannot bind pilot %s: pool size %r has zero '
                        'capacity', self.instance_name, pilot.pid,
                        pilot.size_key)
            return

        old_state = pilot.state
        pilot.capacity  = capacity
        pilot.state     = PILOT_ACTIVE
        pilot.active_at = time.time()
        ps.persist()

        if pilot.active_at and pilot.submitted_at:
            log.info('[%s] pilot %s registered as %s; lag=%.1fs',
                     self.instance_name, pilot.pid, pilot.child_endpoint_name,
                     pilot.active_at - pilot.submitted_at)

        self._dispatch_notify('pilot_status', {
            'pilot_id'      : pilot.pid,
            'pool'          : ps.config.name,
            'state'         : pilot.state,
            'child_endpoint': pilot.child_endpoint_name,
            'capacity'      : capacity,
        })

        try:
            ps.policy.on_pilot_state(pilot, old_state, PILOT_ACTIVE)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

        self._drain_pending(ps)

    # -- pilot submission path -----------------------------------------

    def _make_submit_pilot(self, pool_state: PoolState):
        '''Return a ``submit_pilot(size_key)`` callable bound to *pool_state*.'''
        return lambda size_key: self._submit_pilot(pool_state, size_key)

    def _submit_pilot(self, pool_state: PoolState,
                      size_key: str | None) -> str:
        '''Register a pilot and schedule its psij submission.

        Called from the policy's ``on_tick`` (on the housekeeping loop).
        Returns the dispatcher-local pilot id; the actual psij submission runs
        as a background task so the caller returns immediately.
        '''
        size_key = size_key or pool_state.config.default_size
        if size_key not in pool_state.config.pilot_sizes:
            raise KeyError(
                f"pool {pool_state.config.name}: unknown pilot_size "
                f"{size_key!r} (available: "
                f"{sorted(pool_state.config.pilot_sizes)})")

        size   = pool_state.config.pilot_sizes[size_key]
        pid    = f'p.{uuid.uuid4().hex[:10]}'
        record = PilotRecord(
            pid              = pid,
            pool             = pool_state.config.name,
            owning_sid       = pool_state.owning_sid,
            size_key         = size_key,
            rhapsody_backend = size.rhapsody_backend,
            state            = PILOT_PENDING,
            submitted_at     = time.time(),
            walltime_deadline= time.time() + size.walltime_sec,
        )
        pool_state.pilots[pid] = record
        pool_state.persist()

        asyncio.create_task(self._do_pilot_submit(pool_state, record, size))

        self._dispatch_notify('autoscale_decision', {
            'pool'    : pool_state.config.name,
            'action'  : 'submit_pilot',
            'pilot_id': pid,
            'size_key': size_key,
        })
        return pid

    def _build_pilot_env(self, pool_state: PoolState,
                         record: PilotRecord) -> dict[str, str]:
        '''Build bootstrap env vars for the pilot's child endpoint service.'''
        broker_url = getattr(self._app.state, 'broker_url', '') or ''
        env: dict[str, str] = {
            'RADICAL_ORBIT_BROKER_URL'      : str(broker_url),
            'RADICAL_ORBIT_POOL'            : pool_state.config.name,
            'RADICAL_ORBIT_RHAPSODY_BACKEND': record.rhapsody_backend,
            'RADICAL_ORBIT_SCRATCH_BASE'    : str(pool_state.scratch_base),
        }
        cert = os.environ.get('RADICAL_ORBIT_BROKER_CERT')
        if cert:
            env['RADICAL_ORBIT_BROKER_CERT'] = cert
        return env

    def _build_job_spec(self, pool_state: PoolState,
                        size: PilotSize,
                        child_endpoint: str,
                        env: dict[str, str]) -> dict:
        '''Build a psij-compatible JobSpec for the pilot.'''
        resources: dict[str, Any] = {
            'node_count'        : size.nodes,
            'processes_per_node': size.cpus_per_node,
        }
        if size.gpus_per_node:
            resources['gpu_cores_per_process'] = size.gpus_per_node

        attributes: dict[str, Any] = {
            'queue_name': pool_state.config.queue,
            'duration'  : size.walltime_sec,
        }
        if pool_state.config.account:
            attributes['project'] = pool_state.config.account

        return {
            'executable' : 'radical-orbit-endpoint-wrapper.sh',
            'arguments'  : ['-n', child_endpoint, '--plugins', 'default'],
            'environment': env,
            'resources'  : resources,
            'attributes' : attributes,
        }

    async def _do_pilot_submit(self, pool_state: PoolState,
                               record: PilotRecord,
                               size: PilotSize) -> None:
        '''Call psij on the pool's target endpoint to submit the pilot job.'''
        endpoint_name = pool_state.config.endpoint_name
        if not endpoint_name:
            self._mark_pilot_failed(
                pool_state, record,
                f'pool {pool_state.config.name!r} has no endpoint_name set')
            return

        # Fail fast on the unconfigured default-pool queue sentinel rather than
        # submit a pilot to a batch queue literally named 'default'.
        if pool_state.config.queue == 'default':
            self._mark_pilot_failed(
                pool_state, record,
                "pool queue is the 'default' sentinel; re-declare the pool "
                "with an explicit 'queue' before submitting pilots")
            return

        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(
                pool_state, record, 'psij client unavailable')
            return

        child_endpoint = f'{pool_state.config.name}_{record.pid}'
        # Pre-bind so on_topology_change can match the registering child.
        record.child_endpoint_name = child_endpoint
        env      = self._build_pilot_env(pool_state, record)
        job_spec = self._build_job_spec(pool_state, size, child_endpoint, env)

        try:
            from .batch_system import detect_batch_system
            executor = detect_batch_system().psij_executor
            result   = await asyncio.to_thread(
                psij_c.submit_tunneled, job_spec, executor, 'none')
        except Exception as e:
            log.exception('[%s] psij submit_tunneled failed for %s: %s',
                          self.instance_name, record.pid, e)
            self._mark_pilot_failed(pool_state, record, f'psij error: {e}')
            return

        record.psij_job_id = result.get('job_id')
        record.state       = PILOT_STARTING
        pool_state.persist()
        self._dispatch_notify('pilot_status', {
            'pilot_id'   : record.pid,
            'pool'       : pool_state.config.name,
            'state'      : record.state,
            'psij_job_id': record.psij_job_id,
        })

        try:
            pool_state.policy.on_pilot_state(
                record, PILOT_PENDING, PILOT_STARTING)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    async def _do_pilot_cancel(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        '''Best-effort psij cancel + FAILED for one pilot.'''
        if record.is_terminal():
            return
        endpoint_name = pool_state.config.endpoint_name
        if not endpoint_name or not record.psij_job_id:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        try:
            await asyncio.to_thread(psij_c.cancel_job, record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij cancel failed for %s: %s',
                        self.instance_name, record.pid, e)
        self._mark_pilot_failed(pool_state, record, 'cancelled')

    async def _reconcile_pilot(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
        '''Sweeper path: query psij state for an overdue pilot.'''
        if record.is_terminal():
            return
        endpoint_name = pool_state.config.endpoint_name
        if not endpoint_name or not record.psij_job_id:
            return
        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            return
        try:
            status = await asyncio.to_thread(
                psij_c.get_job_status, record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij get_job_status failed for %s: %s',
                        self.instance_name, record.pid, e)
            return

        state = str(status.get('state', '')).upper()
        if state in ('COMPLETED', 'DONE', 'FAILED', 'CANCELED'):
            self._mark_pilot_failed(
                pool_state, record, f'handshake timeout; psij state {state}')

    def _mark_pilot_failed(self, pool_state: PoolState,
                           record: PilotRecord, reason: str) -> None:
        '''Mark a pilot FAILED, re-enqueue assigned tasks, notify the policy.'''
        log.warning('[%s] pilot %s → FAILED (%s)',
                    self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_FAILED, reason)

    def _mark_pilot_done(self, pool_state: PoolState,
                         record: PilotRecord, reason: str) -> None:
        '''Mark a pilot DONE (clean end, e.g. walltime expiry).'''
        log.info('[%s] pilot %s → DONE (%s)',
                 self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_DONE, reason)

    def _finalize_pilot(self, pool_state: PoolState, record: PilotRecord,
                        new_state: str, reason: str) -> None:
        '''Drive a pilot to a terminal state and reclaim its tasks.

        Persists the transition, notifies clients, re-enqueues any non-terminal
        tasks assigned to this pilot (clearing their stale rhapsody-uid mapping
        so a late terminal event from the dead pilot can't clobber the requeued
        task), and signals the policy.
        '''
        old_state = record.state
        record.state = new_state
        self._dispatch_notify('pilot_status', {
            'pilot_id': record.pid,
            'pool'    : pool_state.config.name,
            'state'   : new_state,
            'reason'  : reason,
        })

        for t in list(pool_state.tasks.values()):
            if t.pilot_id == record.pid and \
                    t.state not in TASK_TERMINAL_STATES:
                if t.rhapsody_uid:
                    self._uid_to_task.pop(t.rhapsody_uid, None)
                    t.rhapsody_uid = None
                t.state    = TASK_QUEUED
                t.pilot_id = None
                self._dispatch_notify('task_status', self._task_dict(t))
        pool_state.persist()

        try:
            pool_state.policy.on_pilot_state(record, old_state, new_state)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    # -- dispatch loop -------------------------------------------------

    def _drain_pending(self, pool_state: PoolState) -> None:
        '''Ask the policy for (task, pilot) pairs until it stops.

        Bounded by the pending-queue length: a single drain can dispatch at
        most as many tasks as are QUEUED.  A pick that repeats or returns a
        non-QUEUED (stale) task is logged and breaks the drain — a broken
        policy is visible rather than silently rate-limited.
        '''
        budget   = len(pool_state.pending_queue())
        assigned : dict[str, list[TaskRecord]] = {}
        pilots   : dict[str, PilotRecord]      = {}
        while budget > 0:
            budget -= 1
            try:
                pair = pool_state.policy.pick_dispatch(pool_state)
            except Exception as e:
                log.exception('[%s] pick_dispatch raised: %s',
                              self.instance_name, e)
                break
            if pair is None:
                break
            task, pilot = pair
            if task.state != TASK_QUEUED:
                log.warning('[%s] pool %r: pick_dispatch returned non-QUEUED '
                            'task %s (%s); stopping drain',
                            self.instance_name, pool_state.config.name,
                            task.task_id, task.state)
                break
            self._claim(pool_state, task, pilot)
            assigned.setdefault(pilot.pid, []).append(task)
            pilots[pilot.pid] = pilot

        if not assigned:
            return

        # one ledger write for the whole drain, however many tasks it claimed
        pool_state.persist()

        for pid, tasks in assigned.items():
            for task in tasks:
                self._notify_task(pool_state, task)
            asyncio.create_task(
                self._do_rhapsody_submit(pool_state, tasks, pilots[pid]))

    def _claim(self, pool_state: PoolState,
               task: TaskRecord, pilot: PilotRecord) -> None:
        '''Claim the task for this pilot (state only; no I/O).'''
        task.state      = TASK_RUNNING
        task.pilot_id   = pilot.pid
        task.started_at = time.time()
        pilot.in_flight     += 1
        pilot.started_tasks += 1

    async def _do_rhapsody_submit(self, pool_state: PoolState,
                                  tasks: list[TaskRecord],
                                  pilot: PilotRecord) -> None:
        '''Post claimed tasks to the pilot's rhapsody session, one call.'''
        if not pilot.child_endpoint_name:
            for task in tasks:
                self._mark_task_failed(pool_state, task,
                                       'child endpoint unavailable')
            return

        rh = await self._get_rhapsody_client(
            pilot.child_endpoint_name, pilot.rhapsody_backend)
        if rh is None:
            for task in tasks:
                self._mark_task_failed(pool_state, task,
                                       'rhapsody client unavailable')
            return

        fwds = []
        for task in tasks:
            if task.task_dict is not None:
                # rhapsody dialect: the client's dict, forwarded verbatim.
                # The uid is namespaced by the owning session, because the
                # pilot's rhapsody session is shared across sessions and
                # client-side uid counters (asyncflow's `task.NNNNNN`) are
                # only unique per client process.
                fwd = dict(task.task_dict)
                fwd['uid'] = f'{task.task_id}.{task.owning_sid}'
            else:
                fwd = {
                    'uid'       : task.task_id,
                    'executable': task.cmd[0] if task.cmd else '',
                    'arguments' : task.cmd[1:] if len(task.cmd) > 1 else [],
                    'cwd'       : task.cwd,
                    # rhapsody's concurrent backend reads cwd from
                    # task_backend_specific_kwargs (BaseTask's top-level cwd
                    # is ignored); mirror it here so the task runs in its
                    # scratch dir.
                    'task_backend_specific_kwargs': {'cwd': task.cwd},
                }
            fwds.append((task, fwd))

        try:
            await asyncio.to_thread(rh.submit_tasks, [f for _, f in fwds])
            for task, fwd in fwds:
                task.rhapsody_uid = fwd['uid']
                self._uid_to_task[fwd['uid']] = (pool_state.owning_sid,
                                                 pool_state.config.name,
                                                 task.task_id)
            self._mark_dirty(pool_state)
        except Exception as e:
            log.exception('[%s] rhapsody submit failed for %d task(s): %s',
                          self.instance_name, len(tasks), e)
            for task, _ in fwds:
                self._mark_task_failed(pool_state, task,
                                       f'rhapsody submit error: {e}')

    def _on_event(self, event: dict) -> None:
        '''Broker raw-tap callback: a child rhapsody reported a transition.

        The tap fires on the plugin-host loop — the dispatcher's own loop — so
        terminal handling runs inline.  The tap is unfiltered, so filter here
        on plugin/topic; the rhapsody uid → pool mapping is ``_uid_to_task``.
        '''
        if event.get('plugin') != 'rhapsody':
            return
        if event.get('topic') != 'task_status':
            return
        data  = event.get('data') or {}
        uid   = data.get('uid')
        state = str(data.get('state', '')).upper()
        if not uid or state not in ('DONE', 'FAILED', 'CANCELED', 'COMPLETED'):
            return

        target = {
            'DONE'     : TASK_DONE,
            'COMPLETED': TASK_DONE,
            'FAILED'   : TASK_FAILED,
            'CANCELED' : TASK_CANCELED,
        }[state]
        self._handle_task_terminal(uid, target, data)

    def _handle_task_terminal(self, uid: str, target_state: str,
                              data: dict) -> None:
        '''Host-loop handler for a child rhapsody task completion.'''
        # Endpoint-mode tasks: forget the mapping and re-emit the terminal
        # status under the dispatcher's plugin name so consumers filtering on
        # plugin='task_dispatcher' still see the event.
        if uid in self._endpoint_mode_tasks:
            endpoint_name = self._endpoint_mode_tasks.pop(uid)
            self._persist_endpoint_mode()
            self._dispatch_notify('task_status', {
                'task_id'  : uid,
                'endpoint' : endpoint_name,
                'state'    : target_state,
                'exit_code': data.get('exit_code'),
                'error'    : data.get('error'),
            })
            return

        mapping = self._uid_to_task.pop(uid, None)
        if not mapping:
            return
        sid, pool_name, task_id = mapping
        pool_state = self._find_pool(sid, pool_name)
        if not pool_state:
            return
        task = pool_state.tasks.get(task_id)
        if task is None or task.state in TASK_TERMINAL_STATES:
            return

        task.state       = target_state
        task.exit_code   = data.get('exit_code')
        task.error       = data.get('error')
        task.finished_at = time.time()

        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)

        self._notify_task(pool_state, task, child_data=data)
        self._drain_pending(pool_state)

    # -- rhapsody-dialect batching: notifications + ledger persists -----
    #
    # Same trade as plugin_rhapsody's NOTIFY window: up to `_notify_window`
    # seconds of latency buys one WS frame per batch instead of one per
    # task, and one `state.json` fsync per dirty pool per flush instead of
    # one per completion -- the dominant latency cost of pool mode.  A
    # zero window (latency-sensitive broker) flushes inline, which is also
    # the exec-mode behavior of old.

    def _notify_task(self, pool_state: PoolState, task: TaskRecord,
                     child_data: dict | None = None) -> None:
        '''Emit one task's status: dialect-shaped and batched for a
        rhapsody-dialect task, the classic immediate frame for an
        exec-style one (its consumers -- the Explorer page among them --
        read the exec shape).'''
        if task.task_dict is None:
            self._dispatch_notify('task_status', self._task_dict(task))
            return

        # rhapsody consumer contract: `uid`, not `task_id`, plus whatever
        # result/error fields the pilot's rhapsody reported
        payload = {k: v for k, v in (child_data or {}).items()
                   if k in _RH_FORWARD_KEYS}
        payload['uid']   = task.task_id
        payload['state'] = task.state
        if task.error is not None:
            payload.setdefault('error', task.error)
        if task.exit_code is not None:
            payload.setdefault('exit_code', task.exit_code)
        self._queue_rh_notification(payload)

    def _queue_rh_notification(self, payload: dict) -> None:
        '''Buffer one dialect notification; flush by window, count or bytes.'''
        size = _payload_size(payload)
        with self._rh_notify_lock:
            self._rh_notify_buf.append(payload)
            self._rh_notify_bytes += size
            now      = self._notify_window <= 0 \
                       or len(self._rh_notify_buf) >= NOTIFY_BATCH_SIZE \
                       or self._rh_notify_bytes >= self._notify_batch_bytes
            schedule = not now and not self._rh_flush_scheduled
            if schedule:
                self._rh_flush_scheduled = True

        if now:
            self._flush_rh()
        elif schedule:
            self._schedule_rh_flush(self._notify_window)

    def _mark_dirty(self, pool_state: PoolState) -> None:
        '''Coalesce this pool's next ledger persist into the flush window.'''
        if self._notify_window <= 0:
            pool_state.persist()
            return
        with self._rh_notify_lock:
            self._dirty_pools.add((pool_state.owning_sid,
                                   pool_state.config.name))
            schedule = not self._rh_flush_scheduled
            if schedule:
                self._rh_flush_scheduled = True
        if schedule:
            self._schedule_rh_flush(self._notify_window)

    def _schedule_rh_flush(self, delay: float) -> None:
        '''Arm one delayed flush on the host loop.'''
        async def _do_flush():
            if delay > 0:
                await asyncio.sleep(delay)
            self._flush_rh()

        try:
            asyncio.get_running_loop().create_task(_do_flush())
        except RuntimeError:
            # no running loop (direct-call tests): flush inline
            self._flush_rh()

    def _flush_rh(self) -> None:
        '''Flush buffered notifications and persist every dirty pool.'''
        with self._rh_notify_lock:
            self._rh_flush_scheduled = False
            batch = list(self._rh_notify_buf)
            self._rh_notify_buf.clear()
            self._rh_notify_bytes = 0
            dirty = list(self._dirty_pools)
            self._dirty_pools.clear()

        for sid, name in dirty:
            ps = self._find_pool(sid, name)
            if ps is not None:            # a torn-down pool needs no persist
                ps.persist()

        if not batch:
            return

        # frame-budget splitting, same shape plugin_rhapsody emits
        frame:  list[dict] = []
        nbytes             = 0
        frames             = [frame]
        for payload in batch:
            size = _payload_size(payload)
            if frame and nbytes + size > self._notify_batch_bytes:
                frame  = []
                nbytes = 0
                frames.append(frame)
            frame.append(payload)
            nbytes += size

        for frame in frames:
            if not frame:
                continue
            if len(frame) == 1:
                self._dispatch_notify('task_status', frame[0])
            else:
                self._dispatch_notify('task_status_batch', {'tasks': frame})

    def _mark_task_failed(self, pool_state: PoolState,
                          task: TaskRecord, reason: str) -> None:
        '''Mark one task FAILED and free its pilot slot.'''
        task.state       = TASK_FAILED
        task.error       = reason
        task.finished_at = time.time()
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)
        self._notify_task(pool_state, task)

    async def _cancel_task(self, pool_state: PoolState,
                           task: TaskRecord) -> dict:
        '''Cancel path: either remove from queue or cancel on the pilot.'''
        if task.state in TASK_TERMINAL_STATES:
            return self._task_dict(task)
        if task.state == TASK_QUEUED:
            task.state       = TASK_CANCELED
            task.finished_at = time.time()
            self._mark_dirty(pool_state)
            self._notify_task(pool_state, task)
            return self._task_dict(task)

        # RUNNING — best-effort cancel on the pilot
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot and pilot.child_endpoint_name and task.rhapsody_uid:
            rh = await self._get_rhapsody_client(pilot.child_endpoint_name)
            if rh is not None:
                try:
                    await asyncio.to_thread(rh.cancel_task, task.rhapsody_uid)
                except Exception as e:
                    log.warning('[%s] rhapsody cancel_task failed: %s',
                                self.instance_name, e)
        task.state       = TASK_CANCELED
        task.finished_at = time.time()
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
        self._mark_dirty(pool_state)
        self._notify_task(pool_state, task)
        return self._task_dict(task)

    # -- helpers -------------------------------------------------------

    def _persist_endpoint_mode(self) -> None:
        '''Rewrite the endpoint-mode ledger (task_id → endpoint) atomically.'''
        try:
            write_json_atomic(self._endpoint_mode_path,
                              self._endpoint_mode_tasks)
        except OSError as e:
            log.warning('[%s] could not persist endpoint-mode ledger: %s',
                        self.instance_name, e)

    def _require_known_session(self, sid: str) -> None:
        '''Raise 404 unless *sid* is a known session.'''
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session: {sid}')

    def _task_dict(self, task: TaskRecord) -> dict:
        '''Return the plain-dict view of a task record.'''
        return asdict(task)

    def _pilot_dict(self, pilot: PilotRecord) -> dict:
        '''Return the plain-dict view of a pilot record.'''
        return asdict(pilot)

    def _summarize_pool(self, ps: PoolState, verbose: bool = False) -> dict:
        '''Return a summary dict for one pool (optionally verbose).'''
        live = ps.live_pilots()
        pending = [t for t in ps.tasks.values() if t.state == TASK_QUEUED]
        summary = {
            'name'        : ps.config.name,
            'queue'       : ps.config.queue,
            'account'     : ps.config.account,
            'default_size': ps.config.default_size,
            'pilot_sizes' : {
                name: {
                    'nodes'           : size.nodes,
                    'cpus_per_node'   : size.cpus_per_node,
                    'gpus_per_node'   : size.gpus_per_node,
                    'walltime_sec'    : size.walltime_sec,
                    'rhapsody_backend': size.rhapsody_backend,
                }
                for name, size in ps.config.pilot_sizes.items()
            },
            'live_pilots'  : len(live),
            'pending_tasks': len(pending),
            'min_pilots'   : ps.config.min_pilots,
            'max_pilots'   : ps.config.max_pilots,
        }
        if verbose:
            summary['pilots'] = [self._pilot_dict(p) for p in live]
            summary['recent_tasks'] = [
                self._task_dict(t)
                for t in sorted(ps.tasks.values(),
                                key=lambda t: t.arrival_ts,
                                reverse=True)[:50]
            ]
        return summary

    # -- session-close teardown (owner lost / ttl / cancel_all) ---------

    async def _teardown_session_pools(self, sid: str) -> int:
        '''Tear down every pool owned by *sid*: cancel pilots, drop the pools.

        The session-close hook that ties a pool's pilots to the owning
        session's lifetime.  Each live pilot is cancelled (best-effort psij
        cancel + FAILED); the pools are dropped.  Idempotent.
        '''
        pools = self._pool_states.pop(sid, None)
        if not pools:
            return 0
        for ps in pools.values():
            for pilot in list(ps.pilots.values()):
                if pilot.is_terminal():
                    continue
                try:
                    await self._do_pilot_cancel(ps, pilot)
                except Exception as e:
                    log.warning('[%s] pilot %s teardown-cancel failed: %s',
                                self.instance_name, pilot.pid, e)
            # Drop this pool's uid→task correlations.
            for uid, (usid, _pool, _tid) in list(self._uid_to_task.items()):
                if usid == sid:
                    self._uid_to_task.pop(uid, None)
            try:    ps.close()
            except Exception:
                pass
        log.info('[%s] tore down %d pool(s) for session %s',
                 self.instance_name, len(pools), sid)
        return len(pools)
