'''
Task dispatcher plugin — elastic multi-pool task routing for radical.orbit.

Hosts one :class:`PoolState` per pool declared in ``pools.json``.  Each
``PoolState`` owns:
- a pluggable :class:`DispatchStrategy` instance
- a pilot lendpointr and pending task queue (append-only JSONL logs)
- a shared-FS scratch area
- an arrivals ring buffer and pilot-lag history

The dispatcher is a **broker-hosted plugin**: it runs on the broker's
plugin-host loop and reaches endpoints through the in-process broker caller
(``BrokerCaller.call_threadsafe``, awaited via ``asyncio.wrap_future`` on the
host loop) — never a loopback HTTP client.  Pilots are submitted via
``plugin_psij.submit_tunneled`` on a login-node endpoint.  When the pilot's
child endpoint registers with the broker, its appearance in the rich topology
(``on_topology_change``) is the dispatcher's signal that the pilot is ACTIVE —
capacity is taken from the pool's pilot-size config.  Tasks then flow via
``rhapsody.submit_tasks`` on the child endpoint; completion arrives as broker
``event`` frames on the raw event tap.

Pools are strictly per-session: keyed ``(owning_sid, pool_name)``, pool names
are session-local, and there is no cross-session attach.  A pool's pilots
follow the owning session's lifetime — session close (owner ``lost`` +
reclaim-drain, ttl expiry, or explicit ``cancel_all``) tears the pools down.

See:
- ``plans/task_dispatcher_design.md`` for the architecture
- ``plans/task_dispatcher_makeflow.md`` for the implementation plan
'''

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import time
import uuid

from dataclasses import asdict
from pathlib import Path
from typing import Any

import msgpack

from fastapi import FastAPI, HTTPException, Request

from .client                            import PluginClient
from .plugin_base                       import Plugin
from .plugin_session_base               import PluginSession
from .task_dispatcher_config            import (
    PoolConfig, PilotSize, PoolConfigError,
    default_pool_config, parse_pools,
)
from .task_dispatcher_state             import (
    PilotRecord, TaskRecord, EndpointModeRecord, StateLog,
    PILOT_PENDING, PILOT_STARTING, PILOT_ACTIVE,
    PILOT_DONE, PILOT_FAILED, PILOT_LIVE_STATES,
    TASK_QUEUED, TASK_RUNNING, TASK_DONE, TASK_FAILED, TASK_CANCELED,
    TASK_TERMINAL_STATES,
)
from .task_dispatcher_strategy          import (
    DispatchStrategy, StrategyContext, load_strategy,
)

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_STATE_ROOT  = Path('~/.radical/orbit/task_dispatcher/state'
                            ).expanduser()
_DEFAULT_SCRATCH_ROOT = Path('~/.radical/orbit/task_dispatcher/scratch'
                             ).expanduser()

# State-directory pruning: directories whose mtime is older than this
# threshold AND whose pool is no longer in self._pool_states get
# deleted by the background sweeper (memory/project_broker_dispatcher.md
# Phase 5).
_STATE_PRUNE_DAYS    = 30
_PRUNE_INTERVAL_SEC  = 86400.0   # stale-dir pruning: once a day

# Log compaction (C6): snapshot a pool's append-only logs when they
# accrue _COMPACT_MAX_APPENDS records since the last snapshot, OR when
# any uncompacted records have lingered _COMPACT_MAX_AGE_SEC.  Checked
# every _COMPACT_INTERVAL_SEC so the age trigger can be tighter than the
# daily stale-dir prune.
_COMPACT_INTERVAL_SEC = 300.0    # check for due compactions every 5 min
_COMPACT_MAX_APPENDS  = 1000     # size trigger
_COMPACT_MAX_AGE_SEC  = 3600.0   # age trigger: don't let a tail linger >1h

# Tick frequency for strategy.on_tick loops
_TICK_INTERVAL_SEC = 5.0

# Sliding arrivals window — keep last N entries per pool
_ARRIVALS_BUFFER_MAX = 1024
_LAG_HISTORY_MAX     = 64

# Handshake timeout — if a pilot hasn't handshaken in this long we
# reconcile against psij job state.
_HANDSHAKE_TIMEOUT_SEC = 300.0  # 5 min, adjusted per observed lag history

# Cached-state behavior on resubmit (design doc §5.1)
#   DONE              → return cached (crash-recovery)
#   FAILED/CANCELED   → overwrite, re-execute (Makeflow retry)
#   RUNNING/QUEUED    → attach to existing wait (wrapper reconnect)


# Rhapsody child-session readiness poll (mirrors RhapsodyClient._poll_session_ready)
_RH_READY_TIMEOUT_SEC = 120.0
_RH_READY_POLL_SEC    = 1.0


# ---------------------------------------------------------------------------
# Remote plugin proxies — async calls to child endpoints over the broker caller
# ---------------------------------------------------------------------------

class _RemoteError(Exception):
    '''A non-2xx ``response`` from a child endpoint's plugin route.'''

    def __init__(self, status: int, detail: Any) -> None:
        self.status = status
        self.detail = detail
        super().__init__(f'remote error {status}: {detail}')


class _RemotePlugin:
    '''Async facade for one child-endpoint plugin session.

    All child-endpoint traffic rides the in-process broker caller
    (``call_threadsafe`` → ``asyncio.wrap_future`` on the host loop) — there is
    no loopback HTTP client and no worker-thread offload.  Bound to a resolved
    ``(dst, sid)`` so the concrete proxies below just format paths.
    '''

    def __init__(self, plugin: 'PluginTaskDispatcher', dst: str,
                 sid: str) -> None:
        self._plugin = plugin
        self._dst    = dst
        self.sid     = sid

    async def _get(self, path: str) -> Any:
        return await self._plugin._call_json(self._dst, 'GET', path)

    async def _post(self, path: str, payload: dict | None = None) -> Any:
        return await self._plugin._call_json(self._dst, 'POST', path, payload)


class _PsijProxy(_RemotePlugin):
    '''The three psij operations the dispatcher drives on a login endpoint.'''

    async def submit_tunneled(self, job_spec: dict, executor: str,
                              tunnel: str) -> dict:
        return await self._post(
            f'/psij/submit_tunneled/{self.sid}',
            {'job_spec': job_spec, 'executor': executor, 'tunnel': tunnel})

    async def cancel_job(self, job_id: str) -> dict:
        return await self._post(f'/psij/cancel/{self.sid}/{job_id}')

    async def get_job_status(self, job_id: str) -> dict:
        return await self._get(f'/psij/status/{self.sid}/{job_id}')


class _RhapsodyProxy(_RemotePlugin):
    '''The three rhapsody operations the dispatcher drives on a child pilot.'''

    async def submit_tasks(self, task_dicts: list[dict]) -> list[dict]:
        # msgpack ``{"tasks": [...]}`` body — the shape RhapsodyClient's
        # single-batch submit sends on the wire.
        body = msgpack.packb({'tasks': task_dicts}, use_bin_type=True)
        resp = await self._plugin._call(
            self._dst, 'POST', f'/rhapsody/submit/{self.sid}',
            body=body, headers={'content-type': 'application/msgpack'})
        return self._plugin._decode_json(resp)

    async def get_task(self, uid: str) -> dict:
        return await self._get(f'/rhapsody/task/{self.sid}/{uid}')

    async def cancel_task(self, uid: str) -> dict:
        return await self._post(f'/rhapsody/cancel/{self.sid}/{uid}')


# ---------------------------------------------------------------------------
# PoolState — per-pool runtime state
# ---------------------------------------------------------------------------

class PoolState:
    '''Plugin-level runtime state for one pool.

    Distinct from :class:`PoolConfig`, which is the static declaration
    loaded from disk.  :class:`PoolState` holds the live fleet, pending
    queue, and strategy instance.

    Concurrency model: all mutations happen from the plugin's asyncio
    event loop thread.  The strategy is called *only* from that thread
    (callbacks and tick), so no in-strategy locking is needed.
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

        self.pilot_log = StateLog(state_dir / 'pilot.log',
                                  PilotRecord, 'pid')
        self.task_log  = StateLog(state_dir / 'task.log',
                                  TaskRecord,  'task_id')

        # Replay on startup.  Orphan-pilot reconciliation happens
        # lazily in _reconcile_pilot when a psij status is queried.
        self.pilots: dict[str, PilotRecord] = self.pilot_log.replay()
        self.tasks:  dict[str, TaskRecord]  = self.task_log.replay()

        self.arrivals:      list[float] = []
        self.lag_history:   list[float] = []

        # Strategy instantiated last so it can see replayed state
        self.strategy: DispatchStrategy = load_strategy(
            config.strategy, config, config.strategy_config)

        # Build StrategyContext once and reuse
        self.ctx = StrategyContext(
            config,
            now_fn               = time.time,
            pending_queue_fn     = self._pending_queue_snapshot,
            pilots_fn            = self._pilots_snapshot,
            arrivals_window_fn   = self._arrivals_window,
            pilot_lag_history_fn = lambda: list(self.lag_history),
            submit_pilot_fn      = self._strategy_submit_pilot,
            cancel_pilot_fn      = self._strategy_cancel_pilot,
            drain_pilot_fn       = self._strategy_drain_pilot,
            logger               = log,
        )

    # -- snapshots for StrategyContext ------------------------------------

    def _pending_queue_snapshot(self) -> list[TaskRecord]:
        '''Return pending tasks for this pool, priority-ordered.

        The dispatcher sorts here so every strategy sees the same
        canonical ordering unless it chooses to reorder.
        '''
        pending = [t for t in self.tasks.values()
                   if t.state == TASK_QUEUED]
        pending.sort(key=lambda t: (-t.priority, t.arrival_ts))
        return pending

    def _pilots_snapshot(self) -> list[PilotRecord]:
        '''Return live (non-terminal) pilots in this pool.'''
        return [p for p in self.pilots.values()
                if p.state in PILOT_LIVE_STATES]

    def _arrivals_window(self, seconds: float) -> list[float]:
        '''Arrival timestamps within *seconds* of now.'''
        cutoff = time.time() - seconds
        return [ts for ts in self.arrivals if ts >= cutoff]

    # -- strategy action hooks (called from strategy code) ----------------

    def _strategy_submit_pilot(self, size_key: str | None) -> str:
        '''Implements ``ctx.submit_pilot``: register a pilot and schedule
        its psij submission.  Returns the dispatcher-local pilot id.
        '''
        size_key = size_key or self.config.default_size
        if size_key not in self.config.pilot_sizes:
            raise KeyError(
                f"pool {self.config.name}: unknown pilot_size "
                f"{size_key!r} (available: "
                f"{sorted(self.config.pilot_sizes)})")

        size   = self.config.pilot_sizes[size_key]
        pid    = f'p.{uuid.uuid4().hex[:10]}'
        record = PilotRecord(
            pid              = pid,
            pool             = self.config.name,
            owning_sid       = self.owning_sid,
            size_key         = size_key,
            rhapsody_backend = size.rhapsody_backend,
            state            = PILOT_PENDING,
            submitted_at     = time.time(),
            walltime_deadline= time.time() + size.walltime_sec,
        )
        self.pilots[pid] = record
        self.pilot_log.append(record)

        # Schedule the actual submission asynchronously
        self._plugin._schedule_pilot_submit(self, record, size)

        self._plugin._dispatch_notify('autoscale_decision', {
            'pool'    : self.config.name,
            'action'  : 'submit_pilot',
            'pilot_id': pid,
            'size_key': size_key,
        })

        return pid

    def _strategy_cancel_pilot(self, pid: str) -> None:
        record = self.pilots.get(pid)
        if not record:
            return
        self._plugin._schedule_pilot_cancel(self, record)

    def _strategy_drain_pilot(self, pid: str) -> None:
        record = self.pilots.get(pid)
        if not record:
            return
        record.accepting_new_tasks = False
        self.pilot_log.append(record)

    # -- housekeeping -----------------------------------------------------

    def record_arrival(self, ts: float) -> None:
        self.arrivals.append(ts)
        if len(self.arrivals) > _ARRIVALS_BUFFER_MAX:
            # cheap trim: drop the oldest half when we overflow
            self.arrivals = self.arrivals[-(_ARRIVALS_BUFFER_MAX // 2):]

    def record_pilot_lag(self, seconds: float) -> None:
        self.lag_history.append(seconds)
        if len(self.lag_history) > _LAG_HISTORY_MAX:
            self.lag_history = self.lag_history[-_LAG_HISTORY_MAX:]

    def task_scratch_dir(self, task_id: str) -> Path:
        '''Shared-FS scratch dir for one task.'''
        d = self.scratch_base / task_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def compact_logs(self) -> None:
        '''Snapshot pilot/task logs that are due for compaction (C6).

        Called from the plugin's compaction sweeper on the event-loop
        thread, so it is serialised with appends — the snapshot captures
        the current in-memory map and truncates the log atomically with
        respect to writers.
        '''
        if self.pilot_log.needs_compaction(
                max_appends=_COMPACT_MAX_APPENDS,
                max_age_sec=_COMPACT_MAX_AGE_SEC):
            self.pilot_log.snapshot(self.pilots)
        if self.task_log.needs_compaction(
                max_appends=_COMPACT_MAX_APPENDS,
                max_age_sec=_COMPACT_MAX_AGE_SEC):
            self.task_log.snapshot(self.tasks)

    def close(self) -> None:
        '''Release the logs' persistent file handles.'''
        self.pilot_log.close()
        self.task_log.close()


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

        *pools* may be a list of pool-config dicts (matches the
        ``pools.json`` ``pools`` field) or a dict containing a
        ``pools`` key.  None registers without declaring pools, which
        causes the dispatcher to auto-materialise its built-in
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
        '''Snapshot of the fleet across all pools (requires session).'''
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

        NOTE: v1 uses a single base64-in-JSON body per file — radical.orbit's
        broker forwards JSON over WebSocket, so multipart is not available.
        Bulk-transfer optimization (tar-stream / dedicated binary staging
        plugin) is deferred; see design doc §6.4.
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
    '''Endpoint-side task dispatcher with pluggable autoscaling and routing.'''

    plugin_name   = 'task_dispatcher'
    session_class = TaskDispatcherSession
    client_class  = TaskDispatcherClient
    version       = '0.0.1'

    ui_config = {
        'icon'          : '📦',
        'title'         : 'Task Dispatcher',
        'description'   : 'Pluggable autoscaling task dispatcher: pools, pilots, strategies.',
        'refresh_button': True,
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Broker hosts only.

        The dispatcher is a broker-side plugin: it owns the global
        pool/pilot/task state, observes topology events directly, and
        proxies psij calls out to login-node endpoints that submit batch
        jobs.  Running it on an endpoint would put it in the wrong half of
        the architecture — see ``memory/project_broker_dispatcher.md``.
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

        # Broker seam (the dispatcher is broker-hosted only): the in-process
        # caller handle and the raw event tap, injected by BrokerPluginHost.
        # When these are absent (None) the dispatcher refuses any endpoint call
        # cleanly (see _call).
        self._broker_caller = getattr(app.state, 'broker_caller', None)
        self._broker_tap    = getattr(app.state, 'broker_tap', None)
        self._untap         = None

        # Cached child-endpoint plugin sessions: (dst, plugin, backend) → sid.
        # Invalidated for a dst when it goes ``lost``.
        self._child_sessions: dict[tuple, str] = {}

        # Map of endpoint_name → set of loaded plugin names, refreshed on
        # every topology change.  Used to (a) auto-resolve a pool's
        # endpoint_name when it's None, and (b) validate the target of an
        # endpoint-mode task submission.
        self._connected_endpoints: dict[str, set[str]] = {}

        # Endpoint-mode task tracking: task_id → target_endpoint_name.  Endpoint
        # mode bypasses pool state — the dispatcher is a transparent proxy to
        # the target endpoint's rhapsody (plugin-global, session-less).  Backed
        # by a small on-disk ledger (C4) so an in-flight endpoint-mode task
        # survives a broker restart; terminal records are filtered out on
        # replay so only live entries seed the dict.
        self._endpoint_mode_log = StateLog(
            self._state_root / 'endpoint_mode.log', EndpointModeRecord, 'task_id')
        self._endpoint_mode_tasks: dict[str, str] = {
            rec.task_id: rec.endpoint
            for rec in self._endpoint_mode_log.replay().values()
            if not rec.is_terminal()
        }

        # Map rhapsody-task-uid → (owning_sid, pool_name, task_id) so the event
        # tap can find the right TaskRecord when a pilot reports completion.
        self._uid_to_task: dict[str, tuple[str, str, str]] = {}

        # Background loops (tick, handshake-timeout sweeper, state
        # prune).  Loops don't actually run until _ensure_started is
        # called from the first request handler — _main_loop stays None
        # until then.
        self._loops_started = False
        self._loops_tasks: list[asyncio.Task] = []
        self._main_loop: asyncio.AbstractEventLoop | None = None

        # Pool state, keyed strictly per session: {owning_sid: {pool_name:
        # PoolState}}.  Pool names are session-local; there is no cross-session
        # attach.  Sessions declare pools via :meth:`register_session`.
        self._pool_states: dict[str, dict[str, PoolState]] = {}

        # Restart-time replay: rebuild pool/pilot/task bookkeeping for ALL
        # sessions from the sid-scoped durable store (built here, not lazy).
        self._replay_state()

        # Routes
        self.add_route_get  ('pools',                         self._route_pools)
        self.add_route_get  ('pool/{name}',                   self._route_pool_detail)
        self.add_route_get  ('fleet/{sid}',                   self._route_fleet)
        self.add_route_post ('submit/{sid}',                  self._route_submit)
        self.add_route_get  ('task/{sid}/{task_id}',          self._route_get_task)
        self.add_route_post ('cancel/{sid}/{task_id}',        self._route_cancel_task)
        self.add_route_post ('cancel_all/{sid}',              self._route_cancel_all)
        self.add_route_post ('stage_in/{sid}/{task_id}',      self._route_stage_in)
        self.add_route_get  ('stage_out/{sid}/{task_id}/{filename}',
                             self._route_stage_out)

    # -- pool bookkeeping helpers ---------------------------------------

    def _pools_for(self, sid: str) -> dict[str, 'PoolState']:
        '''This session's pools (created lazily; empty for a fresh sid).'''
        return self._pool_states.setdefault(sid, {})

    def _all_pools(self):
        '''Iterate every pool across every session.'''
        for pools in list(self._pool_states.values()):
            for ps in list(pools.values()):
                yield ps

    def _find_pool(self, sid: str, name: str) -> 'PoolState | None':
        '''This session's pool by name, or ``None`` (strictly session-local).'''
        return self._pool_states.get(sid, {}).get(name)

    # -- materialisation ------------------------------------------------

    def _pool_dir(self, sid: str, cfg: PoolConfig) -> Path:
        '''Sid-scoped, endpoint-tagged on-disk state directory for a pool.'''
        endpoint_tag = cfg.endpoint_name or 'unbound'
        return self._state_root / sid / f'{cfg.name}__{endpoint_tag}'

    def _scratch_for(self, cfg: PoolConfig) -> Path:
        return (Path(cfg.scratch_base).expanduser()
                if cfg.scratch_base
                else self._scratch_root / cfg.name)

    def _materialise_pool(self, sid: str, cfg: PoolConfig) -> 'PoolState':
        '''Create (or return) this session's pool named ``cfg.name``.

        Pools are strictly per-session (keyed ``(sid, cfg.name)``): a
        same-named pool in another session is a *distinct* pool.  Re-declaring
        a pool already present under this session returns the existing
        :class:`PoolState` (idempotent reconnect) — there is no cross-session
        attach and no config-compatibility check.
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

        # Persist the config so restart-time replay can rebuild this pool
        # (the append-only logs hold pilots/tasks, not the pool declaration).
        self._write_pool_config(state_dir, sid, cfg)

        # Restart recovery (C4): rebuild the uid→task map from the replayed
        # task log so a terminal event for a task that was RUNNING before the
        # restart can still be correlated and advanced.
        for rec in ps.tasks.values():
            if rec.state == TASK_RUNNING and rec.rhapsody_uid:
                self._uid_to_task[rec.rhapsody_uid] = (sid, cfg.name, rec.task_id)

        log.info('[%s] materialised pool %r (sid=%s) → endpoint %r '
                 '(strategy=%s, sizes=%s)',
                 self.instance_name, cfg.name, sid, cfg.endpoint_name,
                 cfg.strategy, sorted(cfg.pilot_sizes))

        # If the dispatcher is already running, kick off this pool's
        # tick loop right away (otherwise _ensure_started will catch it).
        if self._loops_started and self._main_loop \
                and not self._main_loop.is_closed():
            try:
                self._loops_tasks.append(
                    self._main_loop.create_task(self._tick_loop(ps)))
            except RuntimeError:
                pass   # loop not running (e.g. cross-request in TestClient)
        return ps

    @staticmethod
    def _write_pool_config(state_dir: Path, sid: str,
                           cfg: PoolConfig) -> None:
        '''Persist a pool's declaration next to its logs (``pool.json``).'''
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
            payload = {'owning_sid': sid, 'pool': asdict(cfg)}
            (state_dir / 'pool.json').write_text(json.dumps(payload))
        except OSError as e:
            log.warning('task_dispatcher: could not persist pool config '
                        'for %s: %s', cfg.name, e)

    def _replay_state(self) -> None:
        '''Rebuild pools for ALL sessions from the sid-scoped durable store.

        Restart-time replay (built here, not lazy): scan
        ``<state_root>/<sid>/<pool>__<endpoint>/pool.json``, reload each pool's
        config, and re-materialise its :class:`PoolState` (which replays the
        pilot/task logs).  Owner reconnection then reconciles naturally (M1
        create-or-reconnect + M6 owner check); a pool whose owner never returns
        is drained by the reclaim-drain / ttl path.
        '''
        if not self._state_root.exists():
            return
        for sid_dir in sorted(self._state_root.iterdir()):
            if not sid_dir.is_dir():
                continue
            sid = sid_dir.name
            for pool_dir in sorted(sid_dir.iterdir()):
                cfg_path = pool_dir / 'pool.json'
                if not (pool_dir.is_dir() and cfg_path.is_file()):
                    continue
                try:
                    payload = json.loads(cfg_path.read_text())
                    cfg = self._pool_config_from_dict(payload.get('pool', {}))
                except (OSError, ValueError, KeyError, PoolConfigError) as e:
                    log.warning('task_dispatcher: skipping unreadable pool '
                                'config %s: %s', cfg_path, e)
                    continue
                self._materialise_pool(sid, cfg)

    @staticmethod
    def _pool_config_from_dict(d: dict) -> PoolConfig:
        '''Reconstruct a :class:`PoolConfig` from a persisted ``asdict``.'''
        parsed = parse_pools({'pools': [d]}, source='replay')
        return next(iter(parsed.values()))

    def _pick_endpoint_name(self) -> str | None:
        '''Auto-pick an endpoint_name when a pool was declared without one.

        Policy: lexically first connected endpoint that isn't us (the
        broker endpoint).  Returns ``None`` if no eligible endpoint is
        available; the caller decides whether to defer or fail.
        '''
        self_endpoint = getattr(self._app.state, 'endpoint_name', None)
        candidates = sorted(e for e in self._connected_endpoints
                            if e != self_endpoint)
        return candidates[0] if candidates else None

    # -- lifecycle ------------------------------------------------------

    def _ensure_started(self) -> None:
        '''Idempotent: start tick loops and the broker event-tap subscription.'''
        if self._loops_started:
            return
        self._loops_started = True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop yet; deferred to first request handler
            self._loops_started = False
            return

        self._main_loop = loop
        for pool_state in self._all_pools():
            self._loops_tasks.append(loop.create_task(
                self._tick_loop(pool_state)))
        self._loops_tasks.append(loop.create_task(
            self._handshake_sweeper()))
        self._loops_tasks.append(loop.create_task(
            self._state_sweeper()))
        self._loops_tasks.append(loop.create_task(
            self._compaction_sweeper()))

        # Subscribe to the broker's raw event tap for child-pilot task events
        # (the old stack read these off the broker SSE stream).  The tap fires
        # on the plugin-host loop — the dispatcher's own loop — so terminal
        # handling runs inline with no cross-thread marshalling.
        if self._broker_tap is not None and self._untap is None:
            self._untap = self._broker_tap(self._on_event)

    # ── transport: async calls to child endpoints over the broker caller ──

    async def _call(self, dst: str, method: str, path: str, *,
                    body: bytes = b'', headers: dict | None = None,
                    timeout: float | None = None) -> dict:
        '''Send one ``request`` to *dst* and await its ``response`` dict.

        The dispatcher is broker-hosted: every endpoint call rides the
        in-process ``BrokerCaller`` (``call_threadsafe`` schedules onto the
        routing loop; ``asyncio.wrap_future`` awaits it on the host loop
        without blocking).  Refuses cleanly when no caller is wired (a host
        where the dispatcher is not meant to run).
        '''
        caller = self._broker_caller
        if caller is None:
            raise RuntimeError(
                'task_dispatcher is broker-hosted only: no broker caller '
                'available (this plugin cannot reach endpoints on this host)')
        fut = caller.call_threadsafe(dst, method, path,
                                     body=body, headers=headers or {},
                                     timeout=timeout)
        return await asyncio.wrap_future(fut)

    @staticmethod
    def _decode_json(resp: dict) -> Any:
        '''Decode a broker ``response`` dict, raising on a non-2xx status.'''
        status = int(resp.get('status', 502))
        rbody  = resp.get('body') or b''
        if isinstance(rbody, str):
            rbody = rbody.encode()
        data = json.loads(rbody) if rbody else {}
        if status >= 400:
            detail = data.get('detail', data) if isinstance(data, dict) else data
            raise _RemoteError(status, detail)
        return data

    async def _call_json(self, dst: str, method: str, path: str,
                         payload: dict | None = None,
                         timeout: float | None = None) -> Any:
        '''JSON-in / JSON-out convenience over :meth:`_call`.'''
        body    = b''
        headers = None
        if payload is not None:
            body    = json.dumps(payload).encode()
            headers = {'content-type': 'application/json'}
        resp = await self._call(dst, method, path, body=body,
                                headers=headers, timeout=timeout)
        return self._decode_json(resp)

    async def _tick_loop(self, pool_state: PoolState) -> None:
        '''Periodic ``strategy.on_tick`` driver, per pool.'''
        log.debug('[%s] tick loop started for pool %r',
                  self.instance_name, pool_state.config.name)
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                pool_state.strategy.on_tick(pool_state.ctx)
                self._drain_pending(pool_state)
                self._apply_termination_policy(pool_state)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] tick loop error in pool %r: %s',
                              self.instance_name,
                              pool_state.config.name, e)

    async def _handshake_sweeper(self) -> None:
        '''Reconcile pilots whose handshake is overdue.

        Runs every tick.  For each PENDING/STARTING pilot older than the
        effective timeout, queries psij for its job state and marks the
        pilot FAILED if the job is terminal.
        '''
        while True:
            try:
                await asyncio.sleep(_TICK_INTERVAL_SEC)
                now = time.time()
                for pool_state in self._all_pools():
                    for pilot in list(pool_state.pilots.values()):
                        if pilot.state not in (PILOT_PENDING, PILOT_STARTING):
                            continue
                        timeout = self._effective_handshake_timeout(pool_state)
                        if now - pilot.submitted_at < timeout:
                            continue
                        await self._reconcile_pilot(pool_state, pilot)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] handshake sweeper error: %s',
                              self.instance_name, e)

    def _effective_handshake_timeout(self, pool_state: PoolState) -> float:
        '''Observed lag-aware timeout for handshake arrival.'''
        history = pool_state.lag_history
        if not history:
            return _HANDSHAKE_TIMEOUT_SEC
        avg = sum(history) / len(history)
        return max(_HANDSHAKE_TIMEOUT_SEC, 2 * avg)

    async def _state_sweeper(self) -> None:
        '''Prune state directories for pools no longer active.

        Daily sweep: any subdir of ``self._state_root`` whose name is
        not in the active pool set AND whose newest mtime is older
        than ``_STATE_PRUNE_DAYS`` gets removed.  Workflow state for
        terminated pools is kept for 30 days so post-mortem debugging
        is possible; older state is dead weight.
        '''
        # First sweep shortly after startup so a restart with stale
        # state on disk doesn't wait a full day to clean up.
        await asyncio.sleep(60.0)
        while True:
            try:
                self._prune_stale_state_dirs()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] state sweeper error: %s',
                              self.instance_name, e)
            try:
                await asyncio.sleep(_PRUNE_INTERVAL_SEC)
            except asyncio.CancelledError:
                return

    async def _compaction_sweeper(self) -> None:
        '''Periodically snapshot append-only logs that are due (C6).

        Runs on the event loop, so per-log compaction is serialised with
        appends.  Each pool's logs are compacted on a size-or-age policy
        (see :meth:`PoolState.compact_logs`); the endpoint-mode lendpointr is
        compacted the same way so its terminal tombstones don't pile up.
        '''
        while True:
            try:
                await asyncio.sleep(_COMPACT_INTERVAL_SEC)
                for pool_state in self._all_pools():
                    pool_state.compact_logs()
                if self._endpoint_mode_log.needs_compaction(
                        max_appends=_COMPACT_MAX_APPENDS,
                        max_age_sec=_COMPACT_MAX_AGE_SEC):
                    live = {
                        tid: EndpointModeRecord(task_id=tid, endpoint=endpoint,
                                            state=TASK_RUNNING)
                        for tid, endpoint in self._endpoint_mode_tasks.items()
                    }
                    self._endpoint_mode_log.snapshot(live)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception('[%s] compaction sweeper error: %s',
                              self.instance_name, e)

    def _prune_stale_state_dirs(self) -> None:
        '''Synchronous worker for :meth:`_state_sweeper`.

        The store is sid-scoped: ``<state_root>/<sid>/<pool>__<endpoint>/``.
        A pool directory is pruned iff it isn't backing an active pool AND its
        newest file is older than ``_STATE_PRUNE_DAYS`` days; an emptied sid
        directory is then removed too.  A live pool's state dir is identified
        by its own :attr:`PoolState.state_dir`, so renames and endpoint changes
        never collide.
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
            # Drop an emptied sid directory (all its pools pruned / gone).
            try:
                if sid_dir.is_dir() and not any(sid_dir.iterdir()):
                    sid_dir.rmdir()
            except OSError:
                pass

    # -- routes --------------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Override the base ``register_session`` to accept per-session
        pool declarations.

        Request body (JSON, all fields optional)::

            {
              "pools": [<PoolConfig>, ...]   # same shape as pools.json
            }

        Materialisation semantics (strict per-session isolation):

        - The base allocates/reconnects the session ``sid`` (and records its
          owner) first; declared pools then materialise under **that** ``sid``.
        - Pools are keyed ``(owning_sid, pool_name)``: a same-named pool in
          another session is a *distinct*, isolated pool — there is no
          cross-session attach and no config-compatibility check.  Re-declaring
          a pool this session already owns returns the existing one.
        - With no pools declared and no pool yet owned by this session, the
          dispatcher auto-materialises this session's own ``default`` pool.

        Each pool's pilots follow the owning session's lifetime: session close
        (owner ``lost`` + reclaim-drain, ttl expiry, or ``cancel_all``) tears
        the pools down.
        '''
        self._ensure_started()
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

        # Base sid-allocation / owner-check / cleanup logic runs first so pools
        # can be keyed on the resolved sid.
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
        self._ensure_started()
        return {
            'pools': {
                sid: {name: self._summarize_pool(ps)
                      for name, ps in pools.items()}
                for sid, pools in self._pool_states.items()
            }
        }

    async def _route_pool_detail(self, request: Request) -> dict:
        '''Detailed state for one pool by name (first owner found).  Session-less.'''
        self._ensure_started()
        name = request.path_params['name']
        for pools in self._pool_states.values():
            ps = pools.get(name)
            if ps is not None:
                return self._summarize_pool(ps, verbose=True)
        raise HTTPException(status_code=404,
                            detail=f'unknown pool: {name}')

    async def _route_fleet(self, request: Request) -> dict:
        '''Snapshot of this session's fleet.  Session-scoped, strictly isolated.'''
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        return {
            'pools': {
                name: self._summarize_pool(ps, verbose=True)
                for name, ps in self._pools_for(sid).items()
            }
        }

    async def _route_submit(self, request: Request) -> dict:
        self._ensure_started()
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

        # Cached-state behavior (design §5.1, §9.3)
        existing = pool_state.tasks.get(task_id)
        if existing is not None:
            if existing.state == TASK_DONE:
                log.info('[%s] task %s DONE cached; returning '
                         'without re-execution', self.instance_name, task_id)
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
        pool_state.task_log.append(record)
        pool_state.record_arrival(now)

        self._dispatch_notify('task_status', self._task_dict(record))

        # Let the strategy react, then drain any ready dispatches.
        try:
            pool_state.strategy.on_task_arrived(pool_state.ctx, record)
        except Exception as e:
            log.exception('[%s] on_task_arrived raised: %s',
                          self.instance_name, e)
        self._drain_pending(pool_state)

        return self._task_dict(record)

    async def _route_submit_endpoint_mode(
            self, target_endpoint: str, task_id: str,
            cmd: list, cwd: str, body: dict) -> dict:
        '''Endpoint-mode submit: transparent proxy to target's rhapsody.

        No pool, no state log, no pilot fleet — the dispatcher just
        forwards the task to the target endpoint's rhapsody session and
        records ``task_id -> target_endpoint`` so subsequent get/cancel
        can route back.  The mapping is cleared when the task hits a
        terminal state (see :meth:`_on_event`).

        The target endpoint's rhapsody plugin owns the backend choice —
        the dispatcher doesn't pass a ``backends`` list so the endpoint's
        own configured default applies.
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
            result = await rh.submit_tasks([task_dict])
        except Exception as e:
            log.exception('[%s] endpoint-mode submit to %s failed: %s',
                          self.instance_name, target_endpoint, e)
            raise HTTPException(
                status_code=502,
                detail=f'rhapsody submit failed on '
                       f'{target_endpoint}: {e}') from e

        self._endpoint_mode_tasks[task_id] = target_endpoint
        # Persist the in-flight ledger entry (C4) so a broker restart can
        # re-correlate this task; rhapsody uid (== task_id here) is what
        # the event tap keys on for terminal cleanup.
        self._endpoint_mode_log.append(
            EndpointModeRecord(task_id=task_id, endpoint=target_endpoint,
                           state=TASK_RUNNING))
        return {
            'task_id': task_id,
            'endpoint'   : target_endpoint,
            'state'  : TASK_RUNNING,
            'cmd'    : list(cmd),
            'cwd'    : str(cwd),
            'result' : result[0] if result else None,
        }

    async def _route_get_task(self, request: Request) -> dict:
        self._ensure_started()
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
                info = await rh.get_task(task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody get_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name, 'result': info}

        for ps in self._pools_for(sid).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return self._task_dict(rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_task(self, request: Request) -> dict:
        self._ensure_started()
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
                info = await rh.cancel_task(task_id)
            except Exception as e:
                raise HTTPException(
                    status_code=502,
                    detail=f'rhapsody cancel_task failed on '
                           f'{endpoint_name}: {e}') from e
            return {'task_id': task_id, 'endpoint': endpoint_name, 'result': info}

        for ps in self._pools_for(sid).values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return await self._cancel_task(ps, rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_all(self, request: Request) -> dict:
        '''Tear down this session's pools (cancel pilots, drop the pools).

        The explicit reclaim path for ``persistent``/``default`` pools, which
        have no liveness-driven expiry (M1 decision).  Idempotent.
        '''
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        n = await self._teardown_session_pools(sid)
        return {'sid': sid, 'pools_reclaimed': n}

    async def _route_stage_in(self, request: Request) -> dict:
        self._ensure_started()
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

        # Validate filename: no slashes, no ".." — files live at the
        # top of the task scratch dir.  Relative subpaths could be
        # supported later but complicate safety.
        if '/' in filename or '\\' in filename or filename in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for stage_in: {filename!r}')

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
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        task_id  = request.path_params['task_id']
        filename = request.path_params['filename']

        if task_id in self._endpoint_mode_tasks:
            raise HTTPException(
                status_code=400,
                detail='stage_in/stage_out not supported for '
                       'endpoint-mode tasks (yet)')

        if '/' in filename or '\\' in filename or filename in ('', '.', '..'):
            raise HTTPException(
                status_code=400,
                detail=f'invalid filename for stage_out: {filename!r}')

        # Find the task's scratch dir (within this session's pools)
        for ps in self._pools_for(sid).values():
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

        - **Pilot child liveness.**  The dispatcher pre-binds
          ``record.child_endpoint_name`` at submit time; a child that becomes
          ``present`` activates a PENDING/STARTING pilot (capacity from the
          pool's pilot-size config).  ``suspect`` pauses scheduling to that
          pilot (a transient blip must not tear it down); ``lost`` finalises
          the pilot — DONE if walltime elapsed, else FAILED — reclaiming its
          capacity and re-enqueuing unfinished tasks.  Because ``lost`` is only
          synthesized for a child that was actually ``present`` in this broker's
          view, a replayed-ACTIVE pilot whose child has not reconnected after a
          broker restart is never wrongly demoted — no ``_seen`` heuristic
          needed.
        - **Owner-session reclaim.**  ``super().on_topology_change`` arms the
          reclaim-drain for a *session owner* declared ``lost`` (plugin_base,
          M6); the drain then closes the session, which tears down its pools.

        Also refreshes the cached per-endpoint plugin set (endpoint auto-resolve
        + endpoint-mode validation) from the non-lost participants.
        '''
        participants = participants or {}

        # Refresh {endpoint_name: set(plugin_names)} from the non-lost, non-self
        # participants.  'plugins' is the rich name-keyed dict in the star model.
        self_name = getattr(self._app.state, 'endpoint_name', None)
        new: dict[str, set[str]] = {}
        for name, info in participants.items():
            if name == self_name:
                continue
            if (info or {}).get('liveness') == 'lost':
                # Drop cached plugin-sessions on a lost endpoint so a
                # reconnecting one (endpoint-mode target) re-registers fresh.
                for key in [k for k in self._child_sessions if k[0] == name]:
                    self._child_sessions.pop(key, None)
                continue
            plugins = (info or {}).get('plugins', {})
            if isinstance(plugins, dict):
                plugins = list(plugins.keys())
            new[name] = set(plugins)
        self._connected_endpoints = new

        if self._loops_started:
            for ps in self._all_pools():
                self._reconcile_pilots_for(ps, participants)

        # Owner-session reclaim-drain (M6): arms on a *lost* owner that owns
        # ephemeral sessions, cancels on its return.
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
                # Un-pause a pilot that had been paused on a suspect blip.
                if pilot.state == PILOT_ACTIVE and not pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = True
                    ps.pilot_log.append(pilot)
                    self._drain_pending(ps)
                if pilot.state in (PILOT_PENDING, PILOT_STARTING):
                    self._activate_pilot(ps, pilot)

            elif liveness == 'suspect':
                # Pause scheduling to a suspect child (do NOT demote — a blip
                # reaches suspect at most and must survive).
                if pilot.state == PILOT_ACTIVE and pilot.accepting_new_tasks:
                    pilot.accepting_new_tasks = False
                    ps.pilot_log.append(pilot)

            elif liveness == 'lost':
                if time.time() >= pilot.walltime_deadline:
                    self._mark_pilot_done(ps, pilot, 'walltime reached')
                else:
                    self._mark_pilot_failed(
                        ps, pilot,
                        'child endpoint lost before walltime')
                self._drain_pending(ps)

    def _activate_pilot(self, ps: PoolState, pilot: PilotRecord) -> None:
        '''Transition a PENDING/STARTING pilot to ACTIVE on child handshake.'''
        size = ps.config.pilot_sizes.get(pilot.size_key)
        capacity = (size.nodes * size.cpus_per_node) if size else 0
        if capacity <= 0:
            log.warning('[%s] cannot bind pilot %s: pool size '
                        '%r has zero capacity',
                        self.instance_name, pilot.pid, pilot.size_key)
            return

        old_state = pilot.state
        pilot.capacity  = capacity
        pilot.state     = PILOT_ACTIVE
        pilot.active_at = time.time()
        ps.pilot_log.append(pilot)

        if pilot.active_at and pilot.submitted_at:
            lag_observed = pilot.active_at - pilot.submitted_at
            ps.record_pilot_lag(lag_observed)
            log.info('[%s] pilot %s registered as %s; lag=%.1fs',
                     self.instance_name, pilot.pid,
                     pilot.child_endpoint_name, lag_observed)

        self._dispatch_notify('pilot_status', {
            'pilot_id'  : pilot.pid,
            'pool'      : ps.config.name,
            'state'     : pilot.state,
            'child_endpoint': pilot.child_endpoint_name,
            'capacity'  : capacity,
        })

        try:
            ps.strategy.on_pilot_state(
                ps.ctx, pilot, old_state, PILOT_ACTIVE)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

        self._drain_pending(ps)

    # -- pilot submission path -----------------------------------------

    def _schedule_pilot_submit(self, pool_state: PoolState,
                                record: PilotRecord,
                                size: PilotSize) -> None:
        '''Launch the actual psij submit in a background task.

        Called from :meth:`PoolState._strategy_submit_pilot`.  We do not
        await here so the strategy call returns immediately with the
        pilot id.
        '''
        if not self._main_loop:
            log.warning('[%s] no event loop; cannot submit pilot %s',
                        self.instance_name, record.pid)
            return
        asyncio.run_coroutine_threadsafe(
            self._do_pilot_submit(pool_state, record, size),
            self._main_loop)

    def _schedule_pilot_cancel(self, pool_state: PoolState,
                                record: PilotRecord) -> None:
        if not self._main_loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._do_pilot_cancel(pool_state, record),
            self._main_loop)

    async def _child_session(self, dst: str, plugin: str,
                             payload: dict | None = None,
                             backend: str | None = None) -> str | None:
        '''Register (once) a plugin session on a child endpoint; cache the sid.

        The session is registered over the broker caller (``/{plugin}/
        register_session``) and cached per ``(dst, plugin, backend)`` so
        repeated ops on the same child reuse it.  Returns ``None`` when the
        child / plugin is unreachable.
        '''
        key = (dst, plugin, backend)
        sid = self._child_sessions.get(key)
        if sid is not None:
            return sid
        try:
            data = await self._call_json(
                dst, 'POST', f'/{plugin}/register_session', payload or {})
        except Exception as e:
            log.warning('[%s] %s session unavailable on %s: %s',
                        self.instance_name, plugin, dst, e)
            return None
        sid = data.get('sid')
        if not sid:
            return None
        self._child_sessions[key] = sid
        return sid

    async def _get_psij_client(self, endpoint_name: str) -> '_PsijProxy | None':
        '''Return an async :class:`_PsijProxy` targeting *endpoint_name*.

        All traffic rides the in-process broker caller (no loopback HTTP, no
        worker-thread offload).  Returns ``None`` when the endpoint / psij
        plugin is unreachable.
        '''
        if not endpoint_name:
            log.warning('[%s] _get_psij_client called with empty endpoint_name',
                        self.instance_name)
            return None
        sid = await self._child_session(endpoint_name, 'psij')
        if sid is None:
            return None
        return _PsijProxy(self, endpoint_name, sid)

    async def _get_rhapsody_client(self, child_endpoint: str,
                                   backend: str | None = None
                                   ) -> '_RhapsodyProxy | None':
        '''Return an async :class:`_RhapsodyProxy` for a child endpoint.

        Registers the rhapsody session (with *backend* when the session does
        not exist yet), waiting for it to report ``ready``.  Returns ``None``
        when the child / rhapsody plugin is unreachable.
        '''
        payload = {'backends': [backend]} if backend else {}
        sid = await self._child_session(child_endpoint, 'rhapsody',
                                        payload=payload, backend=backend)
        if sid is None:
            return None
        await self._await_rhapsody_ready(child_endpoint, sid)
        return _RhapsodyProxy(self, child_endpoint, sid)

    async def _await_rhapsody_ready(self, dst: str, sid: str) -> None:
        '''Poll until a child rhapsody session is ready (mirrors the helper).

        Rhapsody inits its session asynchronously; ``list_tasks`` answers 409
        until it is ready.  Poll that (over the broker caller) until non-409 or
        the timeout — best-effort, so a submit that races init still surfaces
        its own error.
        '''
        deadline = time.time() + _RH_READY_TIMEOUT_SEC
        while time.time() < deadline:
            try:
                resp = await self._call(dst, 'GET',
                                        f'/rhapsody/list_tasks/{sid}')
                if int(resp.get('status', 502)) != 409:
                    return
            except Exception:
                return
            await asyncio.sleep(_RH_READY_POLL_SEC)

    def _build_pilot_env(self, pool_state: PoolState,
                         record: PilotRecord) -> dict[str, str]:
        '''Bootstrap env vars for the pilot's endpoint service.

        The dispatcher signals "this is a pilot child endpoint" via
        ``RADICAL_ORBIT_POOL`` / ``RADICAL_ORBIT_RHAPSODY_BACKEND`` /
        ``RADICAL_ORBIT_SCRATCH_BASE``.  Broker/cert names use the same
        ``RADICAL_ORBIT_BROKER_*`` vars that any plain endpoint service reads, so
        the generic ``radical-orbit-endpoint-wrapper.sh`` works without renames.
        '''
        broker_url = getattr(self._app.state, 'broker_url', '') or ''
        env: dict[str, str] = {
            'RADICAL_ORBIT_BROKER_URL'           : str(broker_url),
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

        psij_c = await self._get_psij_client(endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(
                pool_state, record, 'psij client unavailable')
            return

        child_endpoint = f'{pool_state.config.name}_{record.pid}'
        # Pre-bind so on_topology_change can match the registering child
        # before the psij submit returns and we persist the next state.
        record.child_endpoint_name = child_endpoint
        env        = self._build_pilot_env(pool_state, record)
        job_spec   = self._build_job_spec(
            pool_state, size, child_endpoint, env)

        try:
            from .batch_system import detect_batch_system
            executor = detect_batch_system().psij_executor
            result   = await psij_c.submit_tunneled(job_spec, executor, 'none')
        except Exception as e:
            log.exception('[%s] psij submit_tunneled failed for %s: %s',
                          self.instance_name, record.pid, e)
            self._mark_pilot_failed(pool_state, record, f'psij error: {e}')
            return

        record.psij_job_id = result.get('job_id')
        record.state       = PILOT_STARTING
        pool_state.pilot_log.append(record)
        self._dispatch_notify('pilot_status', {
            'pilot_id'    : record.pid,
            'pool'        : pool_state.config.name,
            'state'       : record.state,
            'psij_job_id' : record.psij_job_id,
        })

        try:
            pool_state.strategy.on_pilot_state(
                pool_state.ctx, record, PILOT_PENDING, PILOT_STARTING)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    async def _do_pilot_cancel(self, pool_state: PoolState,
                               record: PilotRecord) -> None:
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
            await psij_c.cancel_job(record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij cancel failed for %s: %s',
                        self.instance_name, record.pid, e)
        self._mark_pilot_failed(pool_state, record, 'cancelled by strategy')

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
            status = await psij_c.get_job_status(record.psij_job_id)
        except Exception as e:
            log.warning('[%s] psij get_job_status failed for %s: %s',
                        self.instance_name, record.pid, e)
            return

        state = str(status.get('state', '')).upper()
        if state in ('COMPLETED', 'DONE', 'FAILED', 'CANCELED'):
            self._mark_pilot_failed(
                pool_state, record,
                f'handshake timeout; psij state {state}')

    def _mark_pilot_failed(self, pool_state: PoolState,
                           record: PilotRecord, reason: str) -> None:
        '''Mark a pilot FAILED, re-enqueue assigned tasks, notify strategy.'''
        log.warning('[%s] pilot %s → FAILED (%s)',
                    self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_FAILED, reason)

    def _mark_pilot_done(self, pool_state: PoolState,
                         record: PilotRecord, reason: str) -> None:
        '''Mark a pilot DONE (clean end, e.g. walltime expiry).

        Any task still assigned and non-terminal is re-enqueued — a job
        that reached walltime mid-task should be retried on another
        pilot, not silently dropped.
        '''
        log.info('[%s] pilot %s → DONE (%s)',
                 self.instance_name, record.pid, reason)
        self._finalize_pilot(pool_state, record, PILOT_DONE, reason)

    def _finalize_pilot(self, pool_state: PoolState, record: PilotRecord,
                        new_state: str, reason: str) -> None:
        '''Drive a pilot to a terminal state and reclaim its tasks.

        Shared by the FAILED and DONE paths: persists the transition,
        notifies clients, re-enqueues any non-terminal tasks that were
        assigned to this pilot (clearing their stale rhapsody-uid
        mapping so a late terminal event from the dead pilot can't
        clobber the re-queued task), and signals the strategy.
        '''
        old_state = record.state
        record.state = new_state
        pool_state.pilot_log.append(record)
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
                pool_state.task_log.append(t)
                self._dispatch_notify('task_status', self._task_dict(t))

        try:
            pool_state.strategy.on_pilot_state(
                pool_state.ctx, record, old_state, new_state)
        except Exception as e:
            log.exception('[%s] on_pilot_state raised: %s',
                          self.instance_name, e)

    # -- dispatch loop -------------------------------------------------

    def _drain_pending(self, pool_state: PoolState) -> None:
        '''Ask the strategy for (task, pilot) pairs until it stops.'''
        safety = 10_000
        while safety > 0:
            safety -= 1
            try:
                pair = pool_state.strategy.pick_dispatch(pool_state.ctx)
            except Exception as e:
                log.exception('[%s] pick_dispatch raised: %s',
                              self.instance_name, e)
                return
            if pair is None:
                return
            task, pilot = pair
            if task.state != TASK_QUEUED:
                # stale choice; skip and keep asking
                continue
            self._assign(pool_state, task, pilot)

    def _assign(self, pool_state: PoolState,
                task: TaskRecord, pilot: PilotRecord) -> None:
        '''Claim the task for this pilot and schedule the rhapsody submit.

        FIXME(per-task-backend):
            The rhapsody backend used for this task is implicitly
            inherited from ``pilot.rhapsody_backend`` (chosen at pilot
            submit time via ``PilotSize.rhapsody_backend``).  A future
            extension would call
            ``self._strategy.pick_backend(task, pilot)`` here and, if it
            returns non-None, override the task's target backend before
            submit_tasks.  Paired extension-point doc in:
              task_dispatcher_strategy.py::DispatchStrategy
            (search ``FIXME(per-task-backend)``).
        '''
        task.state      = TASK_RUNNING
        task.pilot_id   = pilot.pid
        task.started_at = time.time()
        pilot.in_flight     += 1
        pilot.started_tasks += 1
        pool_state.task_log.append(task)
        pool_state.pilot_log.append(pilot)

        self._dispatch_notify('task_status', self._task_dict(task))

        if self._main_loop:
            asyncio.run_coroutine_threadsafe(
                self._do_rhapsody_submit(pool_state, task, pilot),
                self._main_loop)

    async def _do_rhapsody_submit(self, pool_state: PoolState,
                                   task: TaskRecord,
                                   pilot: PilotRecord) -> None:
        '''Post the task to the pilot's rhapsody session over the broker caller.'''
        if not pilot.child_endpoint_name:
            self._mark_task_failed(pool_state, task,
                                    'child endpoint unavailable')
            return

        rh = await self._get_rhapsody_client(
            pilot.child_endpoint_name, pilot.rhapsody_backend)
        if rh is None:
            self._mark_task_failed(pool_state, task,
                                    'rhapsody client unavailable')
            return

        task_dict = {
            'uid'       : task.task_id,
            'executable': task.cmd[0] if task.cmd else '',
            'arguments' : task.cmd[1:] if len(task.cmd) > 1 else [],
            'cwd'       : task.cwd,
            # rhapsody's concurrent backend reads cwd from
            # task_backend_specific_kwargs (BaseTask's top-level cwd is
            # ignored).  Mirror it here so the task runs in its scratch
            # dir and stage_out can find the outputs.
            'task_backend_specific_kwargs': {'cwd': task.cwd},
        }
        try:
            result = await rh.submit_tasks([task_dict])
            if result:
                rh_uid = result[0].get('uid')
                if rh_uid:
                    task.rhapsody_uid = rh_uid
                    self._uid_to_task[rh_uid] = (pool_state.owning_sid,
                                                  pool_state.config.name,
                                                  task.task_id)
                    pool_state.task_log.append(task)
        except Exception as e:
            log.exception('[%s] rhapsody submit failed for %s: %s',
                          self.instance_name, task.task_id, e)
            self._mark_task_failed(pool_state, task,
                                    f'rhapsody submit error: {e}')

    def _on_event(self, event: dict) -> None:
        '''Broker raw-tap callback: a child rhapsody reported a task transition.

        The tap fires on the plugin-host loop — the dispatcher's own loop — so
        terminal handling runs inline (no cross-thread marshalling, unlike the
        old SSE listener thread).  The tap is unfiltered, so filter here on
        plugin/topic; the rhapsody uid → pool mapping is ``self._uid_to_task``.
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

        # Map rhapsody state → dispatcher state vocabulary
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
        # status under the dispatcher's plugin name so clients that
        # filter on plugin='task_dispatcher' still see the event.
        if uid in self._endpoint_mode_tasks:
            endpoint_name = self._endpoint_mode_tasks.pop(uid)
            # Write the terminal lendpointr record so replay no longer
            # resurrects this entry after a restart (C4).
            self._endpoint_mode_log.append(
                EndpointModeRecord(task_id=uid, endpoint=endpoint_name,
                               state=target_state))
            self._dispatch_notify('task_status', {
                'task_id'  : uid,
                'endpoint'     : endpoint_name,
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
        pool_state.task_log.append(task)

        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)

        self._dispatch_notify('task_status', self._task_dict(task))

        if pilot is not None:
            try:
                pool_state.strategy.on_task_finished(
                    pool_state.ctx, task, pilot)
            except Exception as e:
                log.exception('[%s] on_task_finished raised: %s',
                              self.instance_name, e)

        self._drain_pending(pool_state)

    def _mark_task_failed(self, pool_state: PoolState,
                          task: TaskRecord, reason: str) -> None:
        task.state       = TASK_FAILED
        task.error       = reason
        task.finished_at = time.time()
        pool_state.task_log.append(task)
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)
        self._dispatch_notify('task_status', self._task_dict(task))

    async def _cancel_task(self, pool_state: PoolState,
                           task: TaskRecord) -> dict:
        '''Cancel path: either remove from queue or cancel on pilot.'''
        if task.state in TASK_TERMINAL_STATES:
            return self._task_dict(task)
        if task.state == TASK_QUEUED:
            task.state       = TASK_CANCELED
            task.finished_at = time.time()
            pool_state.task_log.append(task)
            self._dispatch_notify('task_status', self._task_dict(task))
            return self._task_dict(task)

        # RUNNING — best-effort cancel on the pilot
        pilot = pool_state.pilots.get(task.pilot_id or '')
        if pilot and pilot.child_endpoint_name and task.rhapsody_uid:
            rh = await self._get_rhapsody_client(pilot.child_endpoint_name)
            if rh is not None:
                try:
                    await rh.cancel_task(task.rhapsody_uid)
                except Exception as e:
                    log.warning('[%s] rhapsody cancel_task failed: %s',
                                self.instance_name, e)
        task.state       = TASK_CANCELED
        task.finished_at = time.time()
        if pilot is not None:
            pilot.in_flight = max(0, pilot.in_flight - 1)
            pool_state.pilot_log.append(pilot)
        pool_state.task_log.append(task)
        self._dispatch_notify('task_status', self._task_dict(task))
        return self._task_dict(task)

    # -- termination policy --------------------------------------------

    def _apply_termination_policy(self, pool_state: PoolState) -> None:
        '''Consult strategy.should_terminate_pilot for each live pilot.'''
        for pilot in pool_state._pilots_snapshot():
            try:
                if pool_state.strategy.should_terminate_pilot(
                        pool_state.ctx, pilot):
                    pool_state.ctx.cancel_pilot(pilot.pid)
            except Exception as e:
                log.exception('[%s] should_terminate_pilot raised: %s',
                              self.instance_name, e)

    # -- helpers -------------------------------------------------------

    def _require_known_session(self, sid: str) -> None:
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session: {sid}')

    def _task_dict(self, task: TaskRecord) -> dict:
        return asdict(task)

    def _pilot_dict(self, pilot: PilotRecord) -> dict:
        return asdict(pilot)

    def _summarize_pool(self, ps: PoolState, verbose: bool = False) -> dict:
        live = ps._pilots_snapshot()
        pending = [t for t in ps.tasks.values()
                   if t.state == TASK_QUEUED]
        summary = {
            'name'        : ps.config.name,
            'queue'       : ps.config.queue,
            'account'     : ps.config.account,
            'strategy'    : ps.config.strategy,
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
            'live_pilots' : len(live),
            'pending_tasks': len(pending),
            'min_pilots'  : ps.config.min_pilots,
            'max_pilots'  : ps.config.max_pilots,
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
        session's lifetime.  Reached from :meth:`TaskDispatcherSession.close`
        (owner ``lost`` + reclaim-drain, ttl expiry, ``unregister_session``)
        and from ``cancel_all``.  Each live pilot is cancelled (best-effort
        psij cancel + FAILED in the durable store); the pool's log handles are
        closed and the pools are dropped.  Idempotent.
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
