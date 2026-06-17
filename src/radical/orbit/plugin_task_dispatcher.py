'''
Task dispatcher plugin — elastic multi-pool task routing for radical.orbit.

Hosts one :class:`PoolState` per pool declared in ``pools.json``.  Each
``PoolState`` owns:
- a pluggable :class:`DispatchStrategy` instance
- a pilot lendpointr and pending task queue (append-only JSONL logs)
- a shared-FS scratch area
- an arrivals ring buffer and pilot-lag history

Pilots are submitted via ``plugin_psij.submit_tunneled`` on the same endpoint
(routed through the bridge — see design doc §4.3).  When the child endpoint
registers with the bridge, its appearance in the topology event is the
dispatcher's signal that the pilot is ACTIVE — capacity is taken from
the pool's pilot-size config.  Tasks then flow via
``rhapsody.submit_tasks`` on the child endpoint; completion is observed via
the bridge SSE stream.

See:
- ``plans/task_dispatcher_design.md`` for the architecture
- ``plans/task_dispatcher_makeflow.md`` for the implementation plan
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

from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .client                            import BridgeClient, PluginClient
from .plugin_base                       import Plugin
from .plugin_session_base               import PluginSession
from .task_dispatcher_config            import (
    DEFAULT_POOL_NAME, PoolConfig, PilotSize, PoolConfigError,
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
# deleted by the background sweeper (memory/project_bridge_dispatcher.md
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
                 plugin: 'PluginTaskDispatcher') -> None:
        self.config       = config
        self.state_dir    = state_dir
        self.scratch_base = scratch_base
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
    '''Thin session — dispatcher state is plugin-level, not session-level.

    The session exists to fit the radical.orbit ``Plugin`` framework (sid
    tracking, TTL cleanup) but all pool state lives on
    :class:`PluginTaskDispatcher`.
    '''
    pass


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

    def stage_in(self, pool: str, task_id: str, filename: str,
                 content: bytes, overwrite: bool = False) -> dict:
        '''Upload one file into a task's scratch dir.  Returns ``{cwd, size}``.

        NOTE: v1 uses a single base64-in-JSON body per file — radical.orbit's
        bridge forwards JSON over WebSocket, so multipart is not available.
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
        '''Bridge hosts only.

        The dispatcher is a bridge-side plugin: it owns the global
        pool/pilot/task state, observes topology events directly, and
        proxies psij calls out to login-node endpoints that submit batch
        jobs.  Running it on an endpoint would put it in the wrong half of
        the architecture — see ``memory/project_bridge_dispatcher.md``.
        '''
        from .utils import host_role
        return host_role(app)['role'] == 'bridge'

    def __init__(self, app: FastAPI,
                 instance_name: str = 'task_dispatcher',
                 state_root: str | os.PathLike | None = None,
                 scratch_root: str | os.PathLike | None = None) -> None:
        super().__init__(app, instance_name)

        self._state_root   = Path(state_root   or _DEFAULT_STATE_ROOT)
        self._scratch_root = Path(scratch_root or _DEFAULT_SCRATCH_ROOT)

        # Map of endpoint_name → set of loaded plugin names, updated on
        # every bridge topology event.  Used to (a) auto-resolve a
        # pool's endpoint_name when it's None, and (b) validate the target
        # of an endpoint-mode task submission.
        self._connected_endpoints: dict[str, set[str]] = {}

        # Child endpoints we've actually observed connected during this
        # process's lifetime.  A live pilot is only demoted on
        # disconnect if we previously saw its child here — otherwise a
        # replayed-ACTIVE pilot whose child hasn't reconnected yet (just
        # after a bridge restart) would be wrongly torn down before the
        # child gets a chance to reappear.  See on_topology_change (C2).
        self._seen_child_endpoints: set[str] = set()

        # Endpoint-mode task tracking: task_id → target_endpoint_name.  Endpoint
        # mode bypasses pool state — the dispatcher is a transparent
        # proxy to the target endpoint's rhapsody.  This dict lets get/
        # cancel routes know which endpoint to forward to.  Entries are
        # cleared when the task reaches a terminal state (via the SSE
        # callback).  Backed by a small on-disk lendpointr (C4) so an
        # in-flight endpoint-mode task survives a bridge restart; terminal
        # records are filtered out on replay so only live entries seed
        # the dict.
        self._endpoint_mode_log = StateLog(
            self._state_root / 'endpoint_mode.log', EndpointModeRecord, 'task_id')
        self._endpoint_mode_tasks: dict[str, str] = {
            rec.task_id: rec.endpoint
            for rec in self._endpoint_mode_log.replay().values()
            if not rec.is_terminal()
        }

        # BridgeClient lazily created when we need to reach other endpoints
        # or subscribe to SSE.  Lifetime managed by _ensure_started.
        self._bc: BridgeClient | None = None
        self._bc_lock                  = threading.Lock()

        # Map rhapsody-task-uid → (pool_name, task_id) so SSE callbacks
        # can find the right TaskRecord when a pilot reports completion.
        self._uid_to_task: dict[str, tuple[str, str]] = {}

        # Background loops (tick, handshake-timeout sweeper, state
        # prune).  Loops don't actually run until _ensure_started is
        # called from the first request handler — _main_loop stays None
        # until then.
        self._loops_started = False
        self._loops_tasks: list[asyncio.Task] = []
        self._main_loop: asyncio.AbstractEventLoop | None = None

        # Pool state, keyed by pool name.  Empty at startup; sessions
        # declare pools via :meth:`register_session` (per-workflow
        # config, not operator-managed).  See
        # memory/project_bridge_dispatcher.md (Phase 5).
        self._pool_states: dict[str, PoolState] = {}

        # Routes
        self.add_route_get  ('pools',                         self._route_pools)
        self.add_route_get  ('pool/{name}',                   self._route_pool_detail)
        self.add_route_get  ('fleet/{sid}',                   self._route_fleet)
        self.add_route_post ('submit/{sid}',                  self._route_submit)
        self.add_route_get  ('task/{sid}/{task_id}',          self._route_get_task)
        self.add_route_post ('cancel/{sid}/{task_id}',        self._route_cancel_task)
        self.add_route_post ('stage_in/{sid}/{task_id}',      self._route_stage_in)
        self.add_route_get  ('stage_out/{sid}/{task_id}/{filename}',
                             self._route_stage_out)

    # -- materialisation ------------------------------------------------

    def _materialise_pool(self, cfg: PoolConfig) -> 'PoolState':
        '''Create a :class:`PoolState` from *cfg* and register it.

        - Resolves ``endpoint_name=None`` via :meth:`_pick_endpoint_name` against
          the current topology snapshot.  Stores the resolved name back
          onto *cfg*; if no endpoint is available, leaves it None and lets
          the pilot-submit path bail later.
        - If a pool with the same ``name`` already exists, validates
          that the configs match (see :meth:`_pool_configs_compatible`)
          and returns the existing :class:`PoolState`.  Mismatch raises
          :class:`HTTPException` 409.
        - If the dispatcher's tick-loop machinery is already running,
          starts a tick loop for the new pool.
        '''
        if cfg.endpoint_name is None:
            picked = self._pick_endpoint_name()
            if picked:
                cfg.endpoint_name = picked
                log.info('[%s] pool %r: endpoint_name auto-resolved to %r',
                         self.instance_name, cfg.name, picked)

        existing = self._pool_states.get(cfg.name)
        if existing is not None:
            if not self._pool_configs_compatible(existing.config, cfg):
                raise HTTPException(
                    status_code=409,
                    detail=(f'pool {cfg.name!r} already exists with a '
                            f'different config; declare with a unique '
                            f'name or align with the existing pool'))
            return existing

        # Encode (name, endpoint) in the state-dir path so renames are safe
        # and pools across endpoints (future) don't collide on disk.
        endpoint_tag     = cfg.endpoint_name or 'unbound'
        state_dir    = self._state_root / f'{cfg.name}__{endpoint_tag}'
        scratch_base = (Path(cfg.scratch_base).expanduser()
                        if cfg.scratch_base
                        else self._scratch_root / cfg.name)
        ps = PoolState(cfg, state_dir, scratch_base, self)
        self._pool_states[cfg.name] = ps

        # Restart recovery (C4): the uid→task map is in-memory only, but
        # fully derivable from the replayed task log.  Repopulate it so a
        # terminal SSE event for a task that was RUNNING before the
        # restart can still be correlated and advanced.
        for rec in ps.tasks.values():
            if rec.state == TASK_RUNNING and rec.rhapsody_uid:
                self._uid_to_task[rec.rhapsody_uid] = (cfg.name, rec.task_id)

        log.info('[%s] materialised pool %r → endpoint %r '
                 '(strategy=%s, sizes=%s)',
                 self.instance_name, cfg.name, cfg.endpoint_name,
                 cfg.strategy, sorted(cfg.pilot_sizes))

        # If the dispatcher is already running, kick off this pool's
        # tick loop right away (otherwise _ensure_started will catch it).
        if self._loops_started and self._main_loop:
            self._loops_tasks.append(
                self._main_loop.create_task(self._tick_loop(ps)))
        return ps

    def _pool_configs_compatible(self, a: PoolConfig,
                                 b: PoolConfig) -> bool:
        '''Two configs are compatible if all structural fields match.

        Conflict policy is strict reject (see
        memory/project_bridge_dispatcher.md).  Any difference in name,
        endpoint_name, queue, account, sizes, strategy, or min/max pilots
        counts as a conflict.
        '''
        return (a.name            == b.name
            and a.endpoint_name       == b.endpoint_name
            and a.queue           == b.queue
            and a.account         == b.account
            and a.pilot_sizes     == b.pilot_sizes
            and a.default_size    == b.default_size
            and a.min_pilots      == b.min_pilots
            and a.max_pilots      == b.max_pilots
            and a.strategy        == b.strategy
            and a.strategy_config == b.strategy_config
            and a.scratch_base    == b.scratch_base)

    def _pick_endpoint_name(self) -> str | None:
        '''Auto-pick an endpoint_name when a pool was declared without one.

        Policy: lexically first connected endpoint that isn't us (the
        bridge endpoint).  Returns ``None`` if no eligible endpoint is
        available; the caller decides whether to defer or fail.
        '''
        self_endpoint = getattr(self._app.state, 'endpoint_name', None)
        candidates = sorted(e for e in self._connected_endpoints
                            if e != self_endpoint)
        return candidates[0] if candidates else None

    # -- lifecycle ------------------------------------------------------

    def _ensure_started(self) -> None:
        '''Idempotent: start tick loops and bridge subscription.'''
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
        for pool_state in self._pool_states.values():
            self._loops_tasks.append(loop.create_task(
                self._tick_loop(pool_state)))
        self._loops_tasks.append(loop.create_task(
            self._handshake_sweeper()))
        self._loops_tasks.append(loop.create_task(
            self._state_sweeper()))
        self._loops_tasks.append(loop.create_task(
            self._compaction_sweeper()))

        # Spin up the bridge client + SSE subscription in a worker thread
        # — BridgeClient uses blocking httpx under the hood.
        threading.Thread(target=self._start_bridge_client,
                         daemon=True,
                         name='task-dispatcher-bc').start()

    def _start_bridge_client(self) -> None:
        '''Create the internal BridgeClient and register an SSE callback.

        Runs once, in a background thread.  ``BridgeClient`` itself
        spawns another thread for the SSE listener, so this is a
        fire-and-forget wire-up.
        '''
        try:
            bridge_url  = getattr(self._app.state, 'bridge_url', None)
            bridge_cert = os.environ.get('RADICAL_ORBIT_BRIDGE_CERT')
            if not bridge_url:
                log.warning('[%s] bridge_url missing; SSE disabled',
                            self.instance_name)
                return
            bc = BridgeClient(url=bridge_url, cert=bridge_cert)
            bc.register_callback(topic='task_status',
                                 callback=self._on_rhapsody_task_status)
            with self._bc_lock:
                self._bc = bc
            log.info('[%s] bridge client ready at %s',
                     self.instance_name, bridge_url)
        except Exception as e:
            log.error('[%s] failed to start bridge client: %s',
                      self.instance_name, e)

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
                for pool_state in self._pool_states.values():
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
                for pool_state in self._pool_states.values():
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

        A directory is pruned iff:
        1. it isn't backing an active pool (not in
           ``self._pool_states``), AND
        2. its most-recently-touched file is older than
           ``_STATE_PRUNE_DAYS`` days.

        Active pools' state-dir names follow the
        ``<pool>__<endpoint_or_'unbound'>`` scheme from
        :meth:`_materialise_pool`.  Pools that don't fit that scheme
        (e.g. older state from previous versions) are simply
        candidates for pruning by virtue of not being in the active
        set — that's the intended garbage-collection.
        '''
        if not self._state_root.exists():
            return
        cutoff = time.time() - _STATE_PRUNE_DAYS * 86400
        active = {
            f'{ps.config.name}__{ps.config.endpoint_name or "unbound"}'
            for ps in self._pool_states.values()
        }
        for entry in self._state_root.iterdir():
            if not entry.is_dir() or entry.name in active:
                continue
            try:
                mtimes = [p.stat().st_mtime for p in entry.iterdir()]
            except (FileNotFoundError, PermissionError):
                continue
            if not mtimes:
                continue
            if max(mtimes) >= cutoff:
                continue
            try:
                shutil.rmtree(entry)
                log.info('[%s] pruned stale state dir %s',
                         self.instance_name, entry)
            except OSError as e:
                log.warning('[%s] could not prune %s: %s',
                            self.instance_name, entry, e)

    # -- routes --------------------------------------------------------

    async def register_session(self, request: Request) -> dict:
        '''Override the base ``register_session`` to accept per-session
        pool declarations.

        Request body (JSON, all fields optional)::

            {
              "pools": [<PoolConfig>, ...]   # same shape as pools.json
            }

        Materialisation semantics:

        - Each declared pool is parsed via the same loader as pools.json.
        - For each pool, if a pool with the same name already exists,
          its config is checked against the new declaration:
            * compatible  → session attaches to the existing pool
            * incompatible → request rejected with HTTP 409
        - If no pools are declared AND no pool named ``default`` exists
          yet, the dispatcher auto-materialises a ``default`` pool
          (see :func:`task_dispatcher_config.default_pool_config`).

        Pools materialise into ``self._pool_states`` (plugin-level), so
        they outlive the session.  Sessions never own pilots.
        '''
        self._ensure_started()
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}

        pools_body = body.get('pools')

        if pools_body is not None:
            # Wrap into the loader's expected shape if needed.
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
            for cfg in configs.values():
                self._materialise_pool(cfg)
        elif DEFAULT_POOL_NAME not in self._pool_states:
            # No pools declared; auto-materialise the default once.
            self._materialise_pool(default_pool_config())

        # Hand off to the base sid-allocation / cleanup logic.
        return await super().register_session(request)

    async def _route_pools(self, request: Request) -> dict:
        '''List all configured pools.  Session-less.'''
        self._ensure_started()
        return {
            'pools': {
                name: self._summarize_pool(ps)
                for name, ps in self._pool_states.items()
            }
        }

    async def _route_pool_detail(self, request: Request) -> dict:
        '''Detailed state for one pool.  Session-less.'''
        self._ensure_started()
        name = request.path_params['name']
        ps   = self._pool_states.get(name)
        if not ps:
            raise HTTPException(status_code=404,
                                detail=f'unknown pool: {name}')
        return self._summarize_pool(ps, verbose=True)

    async def _route_fleet(self, request: Request) -> dict:
        '''Snapshot of the fleet across all pools.  Session-scoped.'''
        self._ensure_started()
        sid = request.path_params['sid']
        self._require_known_session(sid)
        return {
            'pools': {
                name: self._summarize_pool(ps, verbose=True)
                for name, ps in self._pool_states.items()
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
        pool_state = self._pool_states.get(pool_name)
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
        terminal state (see :meth:`_on_rhapsody_task_status`).

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

        rh = await asyncio.to_thread(self._get_rhapsody_client, target_endpoint)
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
        # Persist the in-flight lendpointr entry (C4) so a bridge restart can
        # re-correlate this task; rhapsody uid (== task_id here) is what
        # the SSE callback keys on for terminal cleanup.
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
        self._require_known_session(request.path_params['sid'])
        task_id = request.path_params['task_id']

        # Endpoint mode: forward to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await asyncio.to_thread(
                self._get_rhapsody_client, endpoint_name)
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
            return {'task_id': task_id, 'endpoint': endpoint_name, 'result': info}

        for ps in self._pool_states.values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return self._task_dict(rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_cancel_task(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
        task_id = request.path_params['task_id']

        # Endpoint mode: forward cancel to the target endpoint's rhapsody.
        endpoint_name = self._endpoint_mode_tasks.get(task_id)
        if endpoint_name is not None:
            rh = await asyncio.to_thread(
                self._get_rhapsody_client, endpoint_name)
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
            return {'task_id': task_id, 'endpoint': endpoint_name, 'result': info}

        for ps in self._pool_states.values():
            rec = ps.tasks.get(task_id)
            if rec is not None:
                return await self._cancel_task(ps, rec)
        raise HTTPException(status_code=404,
                            detail=f'unknown task: {task_id}')

    async def _route_stage_in(self, request: Request) -> dict:
        self._ensure_started()
        self._require_known_session(request.path_params['sid'])
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

        pool_state = self._pool_states.get(pool_name)
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
        self._require_known_session(request.path_params['sid'])
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

        # Find the task's scratch dir
        for ps in self._pool_states.values():
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

    async def on_topology_change(self, endpoints: dict) -> None:
        '''Bridge topology hook: bind pilots as their child endpoints come and go.

        Replaces the explicit handshake POST.  The dispatcher pre-binds
        ``record.child_endpoint_name`` at submit time; this hook then tracks
        that endpoint's presence in the bridge topology:

        - **appears** → a PENDING/STARTING pilot becomes ACTIVE, with
          capacity taken from the pool's pilot-size config (good enough
          for static allocation; runtime capacity discovery would
          re-introduce a handshake).
        - **disappears** (after having been seen) → a live pilot is
          finalised: DONE if its walltime has elapsed (clean batch-job
          end), else FAILED (premature loss).  Either way its capacity
          is reclaimed and any unfinished tasks are re-enqueued — without
          this the pilot would linger ACTIVE forever (a "phantom",
          inflating the live count and the apparent free capacity).

        Also caches each connected endpoint's plugin set so other code paths
        (auto-resolution of ``PoolConfig.endpoint_name`` when None;
        validation of endpoint-mode submissions) have live topology.
        '''
        # Build {endpoint_name: set(plugin_names)}.  Topology payload's
        # 'plugins' field is sometimes a list, sometimes a dict
        # (depending on serialisation path); accept both.
        new: dict[str, set[str]] = {}
        for name, info in (endpoints or {}).items():
            plugins = (info or {}).get('plugins', [])
            if isinstance(plugins, dict):
                plugins = list(plugins.keys())
            new[name] = set(plugins)
        self._connected_endpoints = new
        if not self._loops_started:
            return

        for ps in self._pool_states.values():
            for pilot in list(ps.pilots.values()):
                ce = pilot.child_endpoint_name
                if not ce:
                    continue
                present = ce in new

                if present and pilot.state in PILOT_LIVE_STATES:
                    # Remember we've seen this child so a later
                    # disconnect is recognised as a genuine teardown.
                    self._seen_child_endpoints.add(ce)

                if present and pilot.state in (PILOT_PENDING, PILOT_STARTING):
                    self._activate_pilot(ps, pilot)
                elif (not present and ce in self._seen_child_endpoints
                        and pilot.state in PILOT_LIVE_STATES):
                    self._seen_child_endpoints.discard(ce)
                    if time.time() >= pilot.walltime_deadline:
                        self._mark_pilot_done(
                            ps, pilot, 'walltime reached')
                    else:
                        self._mark_pilot_failed(
                            ps, pilot,
                            'child endpoint disconnected before walltime')
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

    def _get_psij_client(self, endpoint_name: str) -> Any:
        '''Return a :class:`PSIJClient`-shaped helper targeting *endpoint_name*.

        Goes through the bridge (``self._bc.get_endpoint_client(endpoint_name)``).
        The returned client's methods are synchronous (httpx) — callers
        MUST wrap each call in :func:`asyncio.to_thread` to keep the
        dispatcher's event loop responsive (otherwise the sync HTTP
        starves WS heartbeats and the bridge marks our connection
        dead).  This caller-side ``to_thread`` discipline also makes
        same-endpoint calls safe: the event loop continues processing the
        proxied WS request while the blocking HTTP waits on a worker
        thread.

        Returns ``None`` if the bridge client isn't ready yet or the
        target endpoint / psij plugin isn't reachable.
        '''
        if not endpoint_name:
            log.warning('[%s] _get_psij_client called with empty endpoint_name',
                        self.instance_name)
            return None
        bc = self._wait_for_bc()
        if bc is None:
            return None
        try:
            return bc.get_endpoint_client(endpoint_name).get_plugin('psij')
        except Exception as e:
            log.warning('[%s] psij client unavailable on %s: %s',
                        self.instance_name, endpoint_name, e)
            return None

    def _get_rhapsody_client(self, child_endpoint: str,
                             backend: str | None = None) -> Any:
        '''Return a :class:`RhapsodyClient`-shaped helper for a child endpoint.

        Backend is used only when the session doesn't exist yet; an
        already-registered session keeps its backend.
        '''
        bc = self._wait_for_bc()
        if bc is None:
            return None
        try:
            session_kwargs = {'backends': [backend]} if backend else {}
            return bc.get_endpoint_client(child_endpoint).get_plugin(
                'rhapsody', **session_kwargs)
        except Exception as e:
            log.warning('[%s] rhapsody client unavailable on %s: %s',
                        self.instance_name, child_endpoint, e)
            return None

    def _build_pilot_env(self, pool_state: PoolState,
                         record: PilotRecord) -> dict[str, str]:
        '''Bootstrap env vars for the pilot's endpoint service.

        The dispatcher signals "this is a pilot child endpoint" via
        ``RADICAL_ORBIT_POOL`` / ``RADICAL_ORBIT_RHAPSODY_BACKEND`` /
        ``RADICAL_ORBIT_SCRATCH_BASE``.  Bridge/cert names use the same
        ``RADICAL_ORBIT_BRIDGE_*`` vars that any plain endpoint service reads, so
        the generic ``radical-orbit-endpoint-wrapper.sh`` works without renames.
        '''
        bridge_url = getattr(self._app.state, 'bridge_url', '') or ''
        env: dict[str, str] = {
            'RADICAL_ORBIT_BRIDGE_URL'           : str(bridge_url),
            'RADICAL_ORBIT_POOL'            : pool_state.config.name,
            'RADICAL_ORBIT_RHAPSODY_BACKEND': record.rhapsody_backend,
            'RADICAL_ORBIT_SCRATCH_BASE'    : str(pool_state.scratch_base),
        }
        cert = os.environ.get('RADICAL_ORBIT_BRIDGE_CERT')
        if cert:
            env['RADICAL_ORBIT_BRIDGE_CERT'] = cert
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

        # All psij client construction goes through sync httpx via the
        # bridge — do it in a worker thread to keep the asyncio loop
        # free (also avoids deadlock on same-endpoint round-trips).
        psij_c = await asyncio.to_thread(self._get_psij_client, endpoint_name)
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
            result   = await asyncio.to_thread(
                psij_c.submit_tunneled, job_spec, executor, 'none')
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
        psij_c = await asyncio.to_thread(self._get_psij_client, endpoint_name)
        if psij_c is None:
            self._mark_pilot_failed(pool_state, record, 'cancel requested')
            return
        try:
            await asyncio.to_thread(psij_c.cancel_job, record.psij_job_id)
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
        psij_c = await asyncio.to_thread(self._get_psij_client, endpoint_name)
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
        '''Post the task to the pilot's rhapsody session via the bridge.'''
        if not pilot.child_endpoint_name:
            self._mark_task_failed(pool_state, task,
                                    'child endpoint unavailable')
            return

        rh = await asyncio.to_thread(
            self._get_rhapsody_client, pilot.child_endpoint_name,
            pilot.rhapsody_backend)
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
            result = await asyncio.to_thread(rh.submit_tasks, [task_dict])
            if result:
                rh_uid = result[0].get('uid')
                if rh_uid:
                    task.rhapsody_uid = rh_uid
                    self._uid_to_task[rh_uid] = (pool_state.config.name,
                                                  task.task_id)
                    pool_state.task_log.append(task)
        except Exception as e:
            log.exception('[%s] rhapsody submit failed for %s: %s',
                          self.instance_name, task.task_id, e)
            self._mark_task_failed(pool_state, task,
                                    f'rhapsody submit error: {e}')

    def _on_rhapsody_task_status(self, endpoint: str, plugin: str,
                                  topic: str, data: dict) -> None:
        '''SSE callback: a pilot's rhapsody reported a task transition.

        Runs in the BridgeClient listener thread; marshal back to the
        main asyncio loop to mutate state safely.

        The *endpoint* and *topic* parameters are part of the
        ``BridgeClient.register_callback`` signature; they are received
        but not inspected because the topic filter is already set to
        ``'task_status'`` at registration time and the mapping from
        rhapsody uid to pool happens via ``self._uid_to_task``.
        '''
        del endpoint, topic   # part of callback contract; unused here
        if plugin != 'rhapsody':
            return
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

        if self._main_loop is None:
            return
        self._main_loop.call_soon_threadsafe(
            self._handle_task_terminal, uid, target, data)

    def _handle_task_terminal(self, uid: str, target_state: str,
                               data: dict) -> None:
        '''Main-loop-side handler for rhapsody task completion.'''
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
        pool_name, task_id = mapping
        pool_state = self._pool_states.get(pool_name)
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
            rh = await asyncio.to_thread(
                self._get_rhapsody_client, pilot.child_endpoint_name)
            if rh is not None and getattr(rh, 'sid', None):
                try:
                    await asyncio.to_thread(rh.cancel_task,
                                             task.rhapsody_uid)
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

    def _wait_for_bc(self, timeout: float = 10.0) -> BridgeClient | None:
        '''Wait for the bridge client thread to finish init.'''
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._bc_lock:
                if self._bc is not None:
                    return self._bc
            time.sleep(0.05)
        return None
