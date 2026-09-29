'''
Federation plugin — a resource registry and router above the task dispatcher.

The task dispatcher answers "run this task on *that* pool".  The federation
answers the question one level up: **which** resource should run it at all.
It is a broker-hosted plugin that

- keeps a registry of joined resources and their **members** — one member per
  resource shape an operator is willing to run, each with its own queue,
  pilot size, software, attributes and node-hour budget;
- adds every member to the dispatcher pool of its **capability class**
  (``fed-cpu``, ``fed-gpu``, …), all inside one persistent dispatcher
  session, ``fed``;
- derives node-hour usage per member from the dispatcher and keeps its own
  ledger of the tasks it routed;
- picks a *class* for a set of requirements through a pluggable
  :class:`~radical.orbit.federation_policy.FederationPolicy` — the
  dispatcher then picks the member, at dispatch.

Nothing here is domain-specific: a "resource" is whatever an operator joined,
and requirements are plain capability keys.

Why it is built this way
------------------------
- **A pool is a capability class, not a site.**  Every member of a class
  competes for the same task queue, so work spreads across resources
  automatically and a departing member's tasks can keep running elsewhere.
  The federation therefore keeps exactly **one** dispatcher session,
  :data:`FED_SESSION_SID`, holding every class pool, and drives membership
  through the dispatcher's ``pool/{sid}/{pool}/members`` routes.
- **Placement is late.**  ``submit`` names a class, not a site: the binding
  choice happens at dispatch and is reported by ``task`` as ``member_id``.
  The ``resource`` a submit answers with is **advisory** — the top-scoring
  eligible member's resource, so a caller has something to show — and every
  poll may correct it.  That is also why the task's ``cwd`` is never
  computed here: only the dispatcher knows which member's filesystem the
  task landed on.
- **Re-registration is always the FULL pool list.**  ``parse_pools`` rejects
  an empty ``pools`` list and ``_materialise_pool`` is idempotent by name,
  so re-sending every class pool is both required and free.  One helper,
  ``_class_pool_decls``, builds it for join, restart replay and liveness
  re-attach alike — three callers, one declaration shape, no drift.
- **Dispatcher sessions expire.**  A session registered through the
  in-process host path carries no ``x-orbit-src`` owner, so it is an
  owner-less *ephemeral* session and the base sweep drops it
  ``session_ttl`` (3600 s) after its last access — and dropping a dispatcher
  session cancels its pools and pilots.  Dispatcher routes do not bump
  ``last_access``.  Therefore every session the federation creates is
  registered ``lifetime='persistent'`` and released explicitly on ``leave``.
  The federation's *own* routes all run on the reserved ``default`` session,
  which is always persistent.
- **Broker-hosted plugins cannot call each other through the broker
  caller**: ``BrokerCaller`` resolves ``dst`` through the participant
  registry and raises for the broker itself.  The supported path is
  ``app.state.endpoint_service.handle_request(...)`` — same event loop,
  exact route semantics including ``HTTPException`` status codes, no token.
  :class:`_DispatcherAPI` is the one place that call is made, and it
  resolves the dispatcher *lazily per call* (plugins load in filter order,
  and a broker may be started without the dispatcher at all → 503).
- **Finished pilots vanish from the dispatcher API.**  ``fleet`` and a
  pool's ``pilots`` list carry live pilots only, and ``PilotRecord`` had no
  end timestamp.  Node-hour accounting needs both, so the dispatcher gained
  ``PilotRecord.finished_at`` and a ``pilot_history`` block in the verbose
  pool summary, from which it derives each member's node-hours
  (:func:`~radical.orbit.task_dispatcher_state.node_hours`).  Task counts come from this plugin's own ledger, because the
  dispatcher's ``recent_tasks`` is capped at 50 per pool.
- **``min_pilots`` was parsed but never honoured** by the conservative
  policy (``on_tick`` returned early on an empty queue).  An allocation-mode
  resource must have its pilot running *at join*, before any task exists, so
  the policy gained a floor: while the live fleet is under ``min_pilots`` it
  submits even with an empty backlog, still bounded by every other guard.

Known limitations
-----------------
- The psij executor for a pilot is detected on the **broker host**
  (``detect_batch_system().psij_executor`` in ``_do_pilot_submit``), not on
  the endpoint that runs the pilot.  Correct for a co-located broker and for
  ``allocation`` mode; a ``login``-mode resource on a Slurm cluster reached
  from a non-Slurm broker host will submit with the wrong executor.
- **A class is declared, not reserved.**  ``fed-gpu`` means "every member
  here declares GPUs", not "a GPU is held exclusively for your task":
  pilot capacity is task-count based and nothing reserves a device.
- **Task inputs ride the submit** as ``inputs_b64`` and are forwarded
  verbatim to the dispatcher, which spools them and places them wherever the
  task lands.  Outputs have no such answer: for a member that does not share
  a filesystem with the broker they must be pulled through the pilot's own
  ``staging`` plugin — which is why ``task`` reports ``child_endpoint``
  while the pilot is alive.
- Usage is refreshed by polling (cached 2 s).  The plugin does not subscribe
  to the broker event tap; a caller that wants sub-second accounting should.
'''

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse

from .client                import PluginClient
from .plugin_base           import Plugin
from .plugin_session_base   import PluginSession
from .federation_policy     import make_policy
from .task_dispatcher_config import (MAX_POOL_MEMBER_NAME_LEN, PILOT_ENDPOINT,
                                     PILOT_MODES, PILOT_SUBMIT)
from .federation_state      import (
    FederationState, FederationStateError, MemberRecord, ResourceRecord,
    SubmitLedgerEntry,
    DEFAULT_MEMBER, LIVENESS_OK, LIVENESS_SUSPECT, LIVENESS_LOST,
    MODE_ALLOCATION, MODE_LOGIN, MODES,
    resource_attributes, validate_attributes,
    validate_budget, validate_capabilities, validate_class,
    validate_member_name, validate_name, validate_pool_int,
    validate_scratch_for_host, validate_software,
)

log = logging.getLogger('radical.orbit')


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# State root: ``<root>/<instance_name>/state.json``.  The env override exists
# because the dispatcher has no ``RADICAL_ORBIT_STATE_ROOT`` of its own
# (verified: ``PluginTaskDispatcher._state_root`` is a constructor argument
# only), so a test or a demo isolates the federation the same way it would
# isolate any other durable store.
_ENV_STATE_ROOT      = 'RADICAL_ORBIT_FEDERATION_STATE'
_DEFAULT_STATE_ROOT  = Path('~/.radical/orbit/federation').expanduser()

# The dispatcher instance the federation drives.
_DISPATCHER_INSTANCE = 'task_dispatcher'

# Class pools are named ``fed-<class>``.  The prefix is a constant, not a
# per-resource derivation any more: a pool is a capability class shared by
# every resource that declares it.
_FED_PREFIX = 'fed-'

# The single dispatcher session that holds every class pool.  One session,
# because a class pool outlives any one resource: unregistering it would
# take *every* class down (see the R4 note in plan 08).
FED_SESSION_SID = 'fed'

# Usage is recomputed at most this often, and a refresh that cannot reach the
# dispatcher within this long keeps the previous values and flags them stale.
_USAGE_CACHE_SEC   = 2.0
_USAGE_TIMEOUT_SEC = 3.0

# Pool defaults for every federation-made pool.  Short dwell + a single
# in-flight submission: a federation pool is small and its pilot should come
# up promptly, but never more than one at a time.
_STRATEGY_CONFIG   = {'min_dwell_sec': 5, 'max_in_flight_submissions': 1}
_DEFAULT_BACKEND   = 'concurrent'
_DEFAULT_WALLTIME  = 3600
_SIZE_KEY          = 'default'

# Task states the ledger treats as finished.
_TERMINAL_TASK_STATES = frozenset(('DONE', 'FAILED', 'CANCELED'))

# The fields a login-mode ``pool`` block may carry.  Anything else is a
# typo, and a typo that silently drops (say) ``max_pilots`` is worse than a
# refused join — so the block is validated strictly.
_LOGIN_POOL_KEYS = frozenset((
    'queue', 'account', 'nodes', 'cpus_per_node', 'gpus_per_node',
    'walltime_sec', 'min_pilots', 'max_pilots', 'rhapsody_backend',
))

# The fields one entry of a join body's ``members`` list may carry, for the
# same reason: a dropped ``max_pilots`` or a misspelled ``software`` must be
# a refused join, not a silently different resource.
_MEMBER_KEYS = frozenset((
    'member', 'queue', 'account', 'nodes', 'cpus_per_node', 'gpus_per_node',
    'walltime_sec', 'min_pilots', 'max_pilots', 'rhapsody_backend',
    'scratch_base', 'shared_fs', 'software', 'class', 'attributes', 'budget',
    'pilot',
))

# ``node_hours`` is the federation's own budget key.  The dispatcher's
# requirements parser rejects unknown keys with a 400, so it is stripped
# before a submit is forwarded.
_FEDERATION_ONLY_REQUIREMENTS = ('node_hours',)

# Sanity ceilings on a declared pool.  Not policy — just the line past which
# a number is certainly a mistake (a typo'd walltime or node count reaches
# the batch system as a real request).
# ``<pool>_<member_id>_<pid>`` becomes a broker participant name, so the
# dispatcher bounds the operator-chosen part: ``parse_member`` refuses a
# declaration whose ``len(pool_name) + len(member_id)`` exceeds
# ``task_dispatcher_config.MAX_POOL_MEMBER_NAME_LEN`` (121 §14 R6).
# Checking it here -- against that same constant, never a copy of it --
# turns "the join half-succeeded and then a member 400'd" into a plain
# declaration error, before the dispatcher is touched at all.

_MAX_PILOTS_CAP   = 1024
_MAX_NODES_CAP    = 100000
_MAX_CPUS_CAP     = 4096
_MAX_GPUS_CAP     = 256
_MAX_WALLTIME_CAP = 30 * 86400


# ---------------------------------------------------------------------------
# Dispatcher seam
# ---------------------------------------------------------------------------

class _DispatcherAPI:
    '''The federation's whole view of the task dispatcher.

    Every call goes through the broker plugin host's ``handle_request``,
    which runs the dispatcher's own route handler on this same loop with
    exact route semantics — including its ``HTTPException`` status codes,
    which are mapped straight through to the federation's caller.

    The dispatcher is resolved *lazily, per call*: plugins load in filter
    order, so it may not exist yet at construction time, and a broker may be
    started without it entirely.  Either way the answer is a clean 503
    rather than an attribute error.

    Tests substitute a fake for this object; it is the only seam they need.
    '''

    def __init__(self, app: FastAPI,
                 instance: str = _DISPATCHER_INSTANCE) -> None:
        self._app      = app
        self._instance = instance

    def _host(self):
        '''Return the plugin host, or raise 503 if the dispatcher is absent.'''
        host = getattr(self._app.state, 'endpoint_service', None)
        if host is None:
            raise HTTPException(status_code=503,
                                detail='dispatcher plugin not available: '
                                       'no plugin host')
        plugins = getattr(host, 'plugins', None) or {}
        if self._instance not in plugins:
            raise HTTPException(
                status_code=503,
                detail=f'dispatcher plugin not available: '
                       f'{self._instance!r} is not hosted')
        return host

    @staticmethod
    def _decode(resp: Any) -> Any:
        '''Turn the host's response into JSON, mapping error codes through.'''
        status = int(getattr(resp, 'status_code', 200) or 200)
        raw    = getattr(resp, 'body', b'') or b''
        if isinstance(raw, str):
            raw = raw.encode('utf-8')
        try:
            data = json.loads(raw) if raw else None
        except ValueError:
            data = None
        if status >= 400:
            detail = data.get('detail') if isinstance(data, dict) else None
            raise HTTPException(status_code=status,
                                detail=detail or f'dispatcher error {status}')
        return data

    async def _call(self, method: str, route: str,
                    body: dict | None = None) -> Any:
        '''Invoke one dispatcher route in-process and return its JSON.'''
        host    = self._host()
        payload = b'' if body is None else json.dumps(body).encode('utf-8')
        headers = {'content-type': 'application/json'} if body else {}
        resp    = await host.handle_request(
            method, f'/{self._instance}/{route}', headers, payload)
        return self._decode(resp)

    # -- the verbs the federation needs ------------------------------------

    async def register_session(self, sid: str, pools: list) -> dict:
        '''Register a *persistent* dispatcher session declaring *pools*.'''
        return await self._call('POST', 'register_session', {
            'sid': sid, 'pools': pools, 'lifetime': 'persistent'})

    async def unregister_session(self, sid: str) -> dict:
        '''Close a dispatcher session (tears down its pools and pilots).'''
        return await self._call('POST', f'unregister_session/{sid}')

    async def pool_detail(self, sid: str, name: str) -> dict:
        '''Return the verbose summary of one pool (pilots + pilot_history).'''
        return await self._call('GET', f'pool/{sid}/{name}')

    async def add_member(self, sid: str, pool: str, member: dict) -> dict:
        '''Add (or re-assert) one member of a class pool.

        Idempotent by contract: an identical re-POST is a ``200`` no-op;
        one differing **only** in ``pilot`` updates it in place
        (``updated: True``); any other difference is a ``409``.  That is
        what lets restart replay send every member unconditionally.
        '''
        return await self._call('POST', f'pool/{sid}/{pool}/members', member)

    async def del_member(self, sid: str, pool: str, member_id: str, *,
                         cancel_tasks: bool = False,
                         force: bool = False,
                         fail_unsatisfiable: bool = True) -> dict:
        '''Remove one member; returns the drain counters.

        The flags travel in the **body**, not the query string: the plugin
        host's ``handle_request`` takes no query string, so ``?force=true``
        would end up in the path and match no route.
        '''
        body = {'cancel_tasks'      : bool(cancel_tasks),
                'force'             : bool(force),
                'fail_unsatisfiable': bool(fail_unsatisfiable)}
        return await self._call('DELETE',
                                f'pool/{sid}/{pool}/members/{member_id}', body)

    async def submit(self, sid: str, payload: dict) -> dict:
        '''Submit one task into a pool.'''
        return await self._call('POST', f'submit/{sid}', payload)

    async def task(self, sid: str, task_id: str) -> dict:
        '''Return one task record.'''
        return await self._call('GET', f'task/{sid}/{task_id}')

    async def cancel_task(self, sid: str, task_id: str) -> dict:
        '''Cancel one task.'''
        return await self._call('POST', f'cancel/{sid}/{task_id}')

    # There is deliberately no ``cancel_all`` any more: it tears down a
    # session's pools, and the one session now holds *every* class pool, so
    # calling it on behalf of one leaving resource would take the whole
    # federation with it.  Cancellation rides ``del_member(cancel_tasks=…)``.


# ---------------------------------------------------------------------------
# Session / client
# ---------------------------------------------------------------------------

class FederationSession(PluginSession):
    '''Identity handle only — all federation state is plugin-level.

    Resources outlive any one caller, so a session owns nothing.  The class
    exists because :meth:`Plugin._ensure_default_session` needs a
    ``session_class`` to instantiate, and every federation route runs on the
    reserved persistent ``default`` session.
    '''
    pass


class FederationClient(PluginClient):
    '''Application-side client for the federation plugin.

    Every call uses the reserved ``default`` session: federation state is
    global to the plugin instance, and a client-minted session would be
    swept after an idle hour.
    '''

    # Every federation route is addressed with the reserved persistent
    # ``default`` session; a client never registers one of its own, so
    # ``PluginClient.register_session`` is deliberately not used here.
    DEFAULT_SID = 'default'

    def join(self, record: dict) -> dict:
        '''Join a resource into the federation.  Returns the full record.'''
        resp = self._http.post(self._url(f'join/{self.DEFAULT_SID}'), json=record)
        self._raise(resp, 'join')
        return resp.json()

    def leave(self, name: str, cancel_tasks: bool = False) -> dict:
        '''Remove a resource: drop its members from their class pools.

        Its in-flight tasks are **not** cancelled by default: they live in a
        class pool and may keep running, or start running, on a sibling
        member of another resource.  ``cancel_tasks=True`` is the full
        teardown.
        '''
        resp = self._http.post(self._url(f'leave/{self.DEFAULT_SID}/{name}'),
                               json={'cancel_tasks': bool(cancel_tasks)})
        self._raise(resp, f'leave {name!r}')
        return resp.json()

    def resources(self) -> dict:
        '''List every resource with refreshed usage.'''
        resp = self._http.get(self._url(f'resources/{self.DEFAULT_SID}'))
        self._raise(resp, 'resources')
        return resp.json()

    def resource(self, name: str) -> dict:
        '''Return one resource record with refreshed usage.'''
        resp = self._http.get(self._url(f'resource/{self.DEFAULT_SID}/{name}'))
        self._raise(resp, f'resource {name!r}')
        return resp.json()

    def pick(self, requirements: dict) -> dict:
        '''Return the resource the policy would choose for *requirements*.'''
        resp = self._http.post(self._url(f'pick/{self.DEFAULT_SID}'),
                               json={'requirements': requirements})
        self._raise(resp, 'pick')
        return resp.json()

    def submit(self, task: dict, requirements: dict | None = None) -> dict:
        '''Pick a resource and submit *task* to it.'''
        resp = self._http.post(
            self._url(f'submit/{self.DEFAULT_SID}'),
            json={'task': task, 'requirements': requirements or {}})
        self._raise(resp, 'submit')
        return resp.json()

    def task(self, task_id: str) -> dict:
        '''Return the dispatcher task record plus its resource.'''
        resp = self._http.get(self._url(f'task/{self.DEFAULT_SID}/{task_id}'))
        self._raise(resp, f'task {task_id!r}')
        return resp.json()


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PluginFederation(Plugin):
    '''Broker-hosted resource federation: registry, budget, and the pick.'''

    plugin_name   = 'federation'
    session_class = FederationSession
    client_class  = FederationClient
    version       = '0.0.1'
    ui_module     = os.path.join(os.path.dirname(__file__),
                                 'data', 'plugins', 'federation.js')

    ui_config = {
        'icon'          : '🌐',
        'title'         : 'Federation',
        'description'   : 'Joined resources: capabilities, budget, usage.',
        'refresh_button': True,
        'monitors'      : [{
            'id'        : 'resources',
            'title'     : 'Resources',
            'type'      : 'raw',
            'auto_load' : 'resources/default',
            'empty_text': 'No resources joined.',
        }],
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        '''Load on broker hosts only.

        The federation owns global registry state, drives the broker-hosted
        dispatcher in-process, and reads topology directly — none of which
        exists on an endpoint.
        '''
        return getattr(app.state, 'is_broker', False)

    def __init__(self, app: FastAPI,
                 instance_name: str = 'federation',
                 state_root: str | os.PathLike | None = None,
                 policy: str | None = None,
                 policy_config: dict | None = None,
                 dispatcher_instance: str = _DISPATCHER_INSTANCE) -> None:
        super().__init__(app, instance_name)

        root = Path(state_root or os.environ.get(_ENV_STATE_ROOT)
                    or _DEFAULT_STATE_ROOT).expanduser()
        self._root         = root
        self._state_dir    = root / instance_name
        self._scratch_root = root / 'scratch'
        self._state        = FederationState(
            self._state_dir / 'state.json').load()

        self._policy     = make_policy(policy, policy_config)
        self._dispatcher = _DispatcherAPI(app, dispatcher_instance)

        # A resource loaded from disk is ``lost`` until topology says
        # otherwise: nothing here has seen a participant yet, and claiming a
        # resource is ``ok`` on the strength of a file would let the policy
        # route work at an endpoint that may have been gone for days.
        for rec in self._state.resources.values():
            rec.liveness = LIVENESS_LOST
            for member in rec.member_list():
                member.liveness = LIVENESS_LOST

        # endpoint_name → {'liveness': …, 'role': …} from the rich topology.
        self._participants: dict[str, dict] = {}

        # Member ids currently POSTed into their class pool.  Per *member*,
        # not per resource: a resource's two members live behind one
        # endpoint but attach independently, and one can fail while the
        # other succeeds.
        self._attached: set[str] = set()

        # (member_id, end_time) pairs whose "allocation ended" refusal to
        # re-attach was already logged -- once per member and allocation,
        # not on every topology delivery.
        self._ended_logged: set[tuple[str, float]] = set()

        # Class pool names the ``fed`` session was last registered with, so
        # a re-attach knows whether it must re-declare the pool first.
        self._pools: set[str] = set()

        # Restart re-attach runs exactly once, whichever comes first: the
        # first topology delivery or the first route.  See
        # :meth:`_replay_attachments`.
        self._replayed = False

        # pool_name → (fetched_at, verbose summary).  Shared by usage refresh
        # and the ``child_endpoint`` lookup so a poll loop costs one call.
        self._detail_cache: dict[str, tuple[float, dict]] = {}

        self.add_route_post('join/{sid}',              self._route_join)
        self.add_route_post('leave/{sid}/{name}',      self._route_leave)
        self.add_route_get ('resources/{sid}',         self._route_resources)
        self.add_route_get ('resource/{sid}/{name}',   self._route_resource)
        self.add_route_post('pick/{sid}',              self._route_pick)
        self.add_route_post('submit/{sid}',            self._route_submit)
        self.add_route_get ('task/{sid}/{task_id}',    self._route_task)

        log.info('[%s] loaded %d resource(s) from %s', self.instance_name,
                 len(self._state.resources), self._state.path)

    # -- naming ---------------------------------------------------------

    @staticmethod
    def pool_name_for_class(cls: str) -> str:
        '''Return the dispatcher pool name backing capability class *cls*.

        Created on first use and reused by every resource that declares the
        class; an emptied class pool is left in place (no members ⇒ no
        pilots ⇒ nothing runs) and picked up again by the next join.
        '''
        return f'{_FED_PREFIX}{cls}'

    @staticmethod
    def split_member_id(member_id: str) -> tuple[str, str]:
        '''Split ``<resource>.<member>``; a resource name may contain dots.

        Hence ``rpartition`` — the member half is dot-free by
        :data:`~radical.orbit.federation_state.MEMBER_NAME_RE`, so the last
        dot is unambiguously the separator.
        '''
        resource, _, member = str(member_id or '').rpartition('.')
        return resource, member

    def _scratch_for(self, name: str) -> Path:
        '''Return the default scratch base for resource *name*.'''
        return self._scratch_root / name

    # -- session helper --------------------------------------------------

    async def _require_session(self, sid: str) -> None:
        '''Ensure the reserved ``default`` session, then validate *sid*.

        Every federation route is addressed with ``default``; a client never
        registers a session of its own (one would be swept after an idle
        hour, and there is no per-client state to hold anyway).

        This is also where the restart re-attach is forced if it has not run
        yet.  A route can arrive before the first topology delivery — a
        client polling a task across a broker restart will — and the stored
        dispatcher sessions do not exist until they are re-registered, so
        without this such a call would 404 against a session that is merely
        not re-attached yet.
        '''
        await self._ensure_default_session()
        if not self._replayed:
            self._replayed = True
            await self._replay_attachments()
        if sid not in self._sessions:
            raise HTTPException(status_code=404,
                                detail=f'unknown session id: {sid}')

    def _participant(self, endpoint: str) -> dict:
        '''Return the topology entry for *endpoint* (empty when unknown).'''
        return self._participants.get(endpoint) or {}

    def _liveness_for(self, endpoint: str) -> str:
        '''Map an endpoint's topology liveness onto a resource liveness.

        ``present`` → ``ok``; ``suspect`` stays ``suspect`` (the policy will
        not route there, but nothing is torn down over a blip); anything else
        — ``lost``, unknown, never seen — is ``lost``.
        '''
        live = self._participant(endpoint).get('liveness')
        if live == 'present':
            return LIVENESS_OK
        if live == 'suspect':
            return LIVENESS_SUSPECT
        return LIVENESS_LOST

    def _resource(self, name: str) -> ResourceRecord:
        '''Return a joined resource, or raise 404.'''
        rec = self._state.resources.get(name)
        if rec is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown resource: {name}')
        return rec

    # -- endpoint queries (capability discovery) -------------------------

    async def _endpoint_call(self, endpoint: str, method: str, path: str,
                             body: dict | None = None,
                             timeout: float = 5.0) -> Any:
        '''Call one route on a connected endpoint, or return ``None``.

        Capability discovery is strictly best-effort: an endpoint that has no
        ``queue_info``/``sysinfo``, or is slow, must not fail a join — the
        declared capabilities are the authority and these calls only fill
        gaps.  Every failure therefore resolves to ``None``.

        Endpoint calls go over the broker caller (the routing loop lives on
        another thread, hence ``call_threadsafe`` + ``wrap_future``); this is
        the opposite direction from :class:`_DispatcherAPI`, which reaches a
        *broker-hosted* plugin and must not use the caller at all.
        '''
        caller = getattr(self._app.state, 'broker_caller', None)
        if caller is None:
            return None
        payload = b'' if body is None else json.dumps(body).encode('utf-8')
        headers = {'content-type': 'application/json'} if body else None
        try:
            fut  = caller.call_threadsafe(endpoint, method, path,
                                          body=payload, headers=headers,
                                          timeout=timeout)
            resp = await asyncio.wait_for(asyncio.wrap_future(fut),
                                          timeout + 1.0)
        except Exception as e:
            log.info('[%s] %s %s on %s unavailable: %s',
                     self.instance_name, method, path, endpoint, e)
            return None
        try:
            if int(resp.get('status', 500)) >= 400:
                return None
            raw = resp.get('body') or b''
            if isinstance(raw, str):
                raw = raw.encode('utf-8')
            return json.loads(raw) if raw else None
        except (AttributeError, TypeError, ValueError):
            return None

    async def _job_allocation(self, endpoint: str) -> dict | None:
        '''Return the endpoint's batch allocation summary, or ``None``.

        Session-less route.  ``None`` on a login node, on a host with no
        scheduler (a laptop), or when ``queue_info`` is not served.
        '''
        data = await self._endpoint_call(endpoint, 'GET',
                                         '/queue_info/job_allocation')
        alloc = (data or {}).get('allocation')
        return alloc if isinstance(alloc, dict) else None

    async def _sysinfo_metrics(self, endpoint: str) -> dict | None:
        '''Return the endpoint's sysinfo metrics, or ``None``.

        Metrics are session-scoped, so this registers a session, reads, and
        unregisters it — leaving nothing behind on the endpoint.
        '''
        reg = await self._endpoint_call(endpoint, 'POST',
                                        '/sysinfo/register_session', {})
        sid = (reg or {}).get('sid')
        if not sid:
            return None
        try:
            return await self._endpoint_call(endpoint, 'GET',
                                             f'/sysinfo/metrics/{sid}')
        finally:
            await self._endpoint_call(
                endpoint, 'POST', f'/sysinfo/unregister_session/{sid}')

    async def _discover_capabilities(self, endpoint: str,
                                     declared: dict) -> dict:
        '''Fill ``cores`` / ``gpus`` / ``mem_gb`` gaps from sysinfo.

        Declared values always win — a join is a *declaration*, and an
        operator carving a slice out of a big machine must be able to say so.
        Discovery only answers "the operator did not say".
        '''
        caps = dict(declared)
        if all(k in caps for k in ('cores', 'gpus', 'mem_gb')):
            return caps
        metrics = await self._sysinfo_metrics(endpoint)
        if not isinstance(metrics, dict):
            return caps
        cpu = metrics.get('cpu')  or {}
        mem = metrics.get('memory') or {}
        gpus = metrics.get('gpus')
        if 'cores' not in caps:
            cores = cpu.get('cores_logical') or cpu.get('cores_physical')
            if cores:
                caps['cores'] = int(cores)
        if 'gpus' not in caps and isinstance(gpus, list):
            caps['gpus'] = len(gpus)
        if 'mem_gb' not in caps and mem.get('total'):
            caps['mem_gb'] = round(float(mem['total']) / (1024 ** 3), 1)
        return caps

    # -- member construction ----------------------------------------------

    async def _build_members(self, rec: ResourceRecord,
                             body: dict) -> list[MemberRecord]:
        '''Return the members *rec* declares — one per resource shape.

        Three shapes, one result:

        - ``login`` **with** ``members`` — each entry validated on its own
          and turned into a member;
        - ``login`` **without** ``members`` — exactly one member
          ``default``, from today's flat ``pool`` block, so a join body that
          works today keeps working byte for byte;
        - ``allocation`` — exactly one member ``default``, sized from the
          endpoint's own allocation.  Declaring ``members`` here is an error:
          the allocation *is* the resource.
        '''
        raw = body.get('members')
        if raw is not None and not isinstance(raw, list):
            raise FederationStateError("'members' must be a list")
        if isinstance(raw, list) and not raw:
            raise FederationStateError("'members' must not be empty")

        if rec.mode == MODE_ALLOCATION:
            if raw:
                raise FederationStateError(
                    "'members' is not valid in allocation mode: the "
                    'allocation is the resource, so it has exactly one '
                    'member')
            decl = self._allocation_pool(
                rec, await self._job_allocation(rec.endpoint) or {})
            return [self._implicit_member(rec, decl)]

        if not raw:
            return [self._implicit_member(rec, self._login_pool(rec))]

        members: list[MemberRecord] = []
        seen: set[str] = set()
        for entry in raw:
            member = self._declared_member(rec, entry)
            if member.member in seen:
                raise FederationStateError(
                    f'duplicate member name: {member.member}')
            seen.add(member.member)
            members.append(member)
        return members

    @staticmethod
    def _implicit_member(rec: ResourceRecord, decl: dict) -> MemberRecord:
        '''Turn a flat pool declaration into the resource's one member.

        The resource-wide capabilities become the member's ``software`` and
        attributes, and the resource's budget becomes the member's — which
        is exactly the aggregate view read back, so nothing changes for a
        single-member resource.

        The attribute map comes from
        :func:`~radical.orbit.federation_state.resource_attributes`, the one
        helper this and ``_derive_member`` share, so a synthesised member is
        built the same way whether it came from a join body or from a pre-08
        ``state.json``.

        ``pilot`` and ``end_time`` come from the declaration too:
        :meth:`_allocation_pool` puts ``endpoint`` there because the endpoint
        that joined already runs inside the allocation and *is* the pilot,
        while :meth:`_login_pool` leaves both alone.
        '''
        size = decl['pilot_sizes'][_SIZE_KEY]
        caps = rec.capabilities or {}
        member = MemberRecord(
            member           = DEFAULT_MEMBER,
            endpoint         = rec.endpoint,
            pilot            = decl.get('pilot') or PILOT_SUBMIT,
            end_time         = decl.get('end_time'),
            queue            = decl['queue'],
            account          = decl['account'],
            nodes            = size['nodes'],
            cpus_per_node    = size['cpus_per_node'],
            gpus_per_node    = size['gpus_per_node'],
            walltime_sec     = size['walltime_sec'],
            min_pilots       = decl.get('min_pilots', 0),
            max_pilots       = decl.get('max_pilots', 1),
            rhapsody_backend = size['rhapsody_backend'],
            scratch_base     = rec.scratch_base,
            shared_fs        = rec.shared_fs,
            software         = list(caps.get('software') or []),
            attributes       = resource_attributes(rec.site, rec.kind,
                                                   caps.get('mem_gb')),
            budget           = dict(rec.budget or {}),
        )
        member.cls = member.default_class()
        return member

    @staticmethod
    def _declared_member(rec: ResourceRecord, decl: Any) -> MemberRecord:
        '''Validate one entry of a join body's ``members`` list.

        Same strictness as the flat ``pool`` block, per member: every count
        is an integer in a sane range, the queue is not the dispatcher's
        ``default`` sentinel, an unknown key is refused rather than ignored,
        and a declared ``class`` must match ``^[a-z0-9][a-z0-9_-]*$`` — a
        name that does not is a 400, never a lower-cased guess.

        A member may declare ``pilot: endpoint``, which says the resource's
        endpoint already runs inside an allocation and is itself the pilot;
        the dispatcher then adopts it and holds exactly one.  Its
        ``end_time`` is not declarable — only the allocation knows when it
        ends, and only ``allocation`` mode reads that from the endpoint.
        '''
        if not isinstance(decl, dict):
            raise FederationStateError(
                "each entry of 'members' must be an object")
        unknown = set(decl) - _MEMBER_KEYS
        if unknown:
            raise FederationStateError(
                f"unknown member field(s): {', '.join(sorted(unknown))} "
                f"(known: {', '.join(sorted(_MEMBER_KEYS))})")

        name  = validate_member_name(decl.get('member'))
        label = f'member {name}'

        queue = decl.get('queue')
        if not isinstance(queue, str) or not queue:
            raise FederationStateError(
                f"'{label}.queue' must be a non-empty string")
        if queue == 'default':
            raise FederationStateError(
                f"'{label}.queue' must not be the dispatcher sentinel "
                f"'default'")

        account = decl.get('account')
        if account is not None and not isinstance(account, str):
            raise FederationStateError(
                f"'{label}.account' must be a string or null")

        backend = decl.get('rhapsody_backend') or _DEFAULT_BACKEND
        if not isinstance(backend, str) or not backend:
            raise FederationStateError(
                f"'{label}.rhapsody_backend' must be a non-empty string")

        # a member that says nothing shares whatever the resource shares
        shared = decl.get('shared_fs', rec.shared_fs)
        if not isinstance(shared, bool):
            raise FederationStateError(f"'{label}.shared_fs' must be a bool")

        scratch = decl.get('scratch_base')
        if scratch:
            scratch = validate_scratch_for_host(
                scratch, shared=shared, field=f'{label}.scratch_base')
        else:
            # a member that names no scratch inherits the resource's — the
            # common case, where every member shares one home tree
            scratch = rec.scratch_base

        max_pilots = validate_pool_int(decl, 'max_pilots', default=1,
                                       minimum=1, maximum=_MAX_PILOTS_CAP,
                                       label=label)
        try:
            budget = validate_budget(decl.get('budget'), required=True)
        except FederationStateError as e:
            raise FederationStateError(f'{label}: {e}') from e

        pilot = decl.get('pilot') or PILOT_SUBMIT
        if pilot not in PILOT_MODES:
            raise FederationStateError(
                f"'{label}.pilot' must be one of {', '.join(PILOT_MODES)} "
                f'(got {pilot!r})')

        member = MemberRecord(
            member           = name,
            endpoint         = rec.endpoint,
            pilot            = pilot,
            queue            = queue,
            account          = account,
            nodes            = validate_pool_int(
                decl, 'nodes', minimum=1, maximum=_MAX_NODES_CAP,
                label=label),
            cpus_per_node    = validate_pool_int(
                decl, 'cpus_per_node', minimum=1, maximum=_MAX_CPUS_CAP,
                label=label),
            gpus_per_node    = validate_pool_int(
                decl, 'gpus_per_node', default=0, minimum=0,
                maximum=_MAX_GPUS_CAP, label=label),
            walltime_sec     = validate_pool_int(
                decl, 'walltime_sec', minimum=1, maximum=_MAX_WALLTIME_CAP,
                label=label),
            min_pilots       = validate_pool_int(
                decl, 'min_pilots', default=0, minimum=0,
                maximum=max_pilots, label=label),
            max_pilots       = max_pilots,
            rhapsody_backend = backend,
            scratch_base     = scratch,
            shared_fs        = shared,
            software         = validate_software(decl.get('software'),
                                                 label=label),
            attributes       = validate_attributes(decl.get('attributes'),
                                                   label=label),
            budget           = budget,
        )
        declared   = decl.get('class')
        member.cls = (validate_class(declared, label=label) if declared
                      else member.default_class())
        return member

    # -- dispatcher declarations -------------------------------------------

    @staticmethod
    def _member_decl(rec: ResourceRecord, member: MemberRecord) -> dict:
        '''Return the dispatcher's view of one member.

        ``software`` rides inside ``attributes`` because that is the
        dispatcher's vocabulary — it matches a task's declared needs against
        a member's attribute map and knows nothing about federations.

        ``pilot`` and ``end_time`` travel too: the dispatcher adopts an
        ``endpoint`` member's endpoint instead of submitting a job for it,
        and caps that pilot's deadline at the allocation's end — which it
        cannot derive, because a member is re-declared with its join-time
        ``walltime_sec`` on every re-attach.
        '''
        return {
            'member_id'    : member.member_id,
            'endpoint_name': rec.endpoint,
            'pilot'        : member.pilot,
            'end_time'     : member.end_time,
            'queue'        : member.queue,
            'account'      : member.account,
            'pilot_sizes'  : {_SIZE_KEY: {
                'nodes'           : int(member.nodes),
                'cpus_per_node'   : int(member.cpus_per_node),
                'gpus_per_node'   : int(member.gpus_per_node),
                'walltime_sec'    : int(member.walltime_sec),
                'rhapsody_backend': member.rhapsody_backend,
            }},
            'default_size' : _SIZE_KEY,
            'min_pilots'   : int(member.min_pilots),
            'max_pilots'   : int(member.max_pilots),
            'scratch_base' : member.scratch_base,
            'shared_fs'    : bool(member.shared_fs),
            'attributes'   : member.match_attributes(),
            'budget'       : dict(member.budget or {}),
        }

    def _class_pool_decls(self,
                          extra: ResourceRecord | None = None) -> list[dict]:
        '''Return the FULL class-pool declaration list, never a delta.

        One helper for all three callers (``join``, restart replay, liveness
        re-attach): ``parse_pools`` refuses an empty ``pools`` list and
        ``_materialise_pool`` is idempotent by name, so re-sending every
        pool is both required and free — and there is exactly one place the
        declaration shape can drift.

        The ``members`` carried here only matter for a dispatcher that does
        not have the pool yet; a re-declaration of an existing pool is
        ignored, which is precisely why the member routes exist.
        '''
        records = list(self._state.resources.values())
        if extra is not None and \
                self._state.resources.get(extra.name) is not extra:
            records.append(extra)

        by_class: dict[str, list] = {}
        for rec in records:
            for member in rec.member_list():
                by_class.setdefault(member.cls, []).append(
                    self._member_decl(rec, member))

        return [{'name'           : self.pool_name_for_class(cls),
                 'pool_class'     : cls,
                 'multi_member'   : True,
                 'members'        : by_class[cls],
                 'strategy'       : 'conservative',
                 'strategy_config': dict(_STRATEGY_CONFIG)}
                for cls in sorted(by_class)]

    async def _register_fed(self,
                            extra: ResourceRecord | None = None) -> list:
        '''Register the single ``fed`` session with every class pool.

        A federation with no resources at all simply does not register —
        there is nothing to declare, and an empty ``pools`` list is refused.
        '''
        decls = self._class_pool_decls(extra)
        if not decls:
            return []
        await self._dispatcher.register_session(FED_SESSION_SID, decls)
        self._pools = {d['name'] for d in decls}
        return decls

    @staticmethod
    def _allocation_walltime(runtime: Any, end: float | None) -> int:
        '''Return the pilot walltime for an allocation-mode resource.

        ``queue_info``'s ``runtime`` is the job's **time limit**, not the
        time it has left (SLURM ``squeue %l``, PBS ``Resource_List.walltime``).
        A resource joined late into its allocation would therefore give its
        pilot a walltime longer than the allocation itself, and the
        dispatcher would wait on a deadline the batch system will never
        honour.

        So the allocation's own ``end_time`` wins where the endpoint reports
        one — an absolute epoch computed **inside** the allocation
        (``squeue %L`` / PBS ``Walltime.Remaining``), which is the only
        place the answer exists: the broker's environment describes the
        broker's job, not this one.  An allocation already over is a **400**
        rather than a one-second pilot; ``PilotSize.walltime_sec`` must be
        at least 1 and a pilot that cannot outlive its own submission is not
        a resource.

        *runtime* is the allocation's ``runtime`` and *end* its ``end_time``
        (a float or ``None`` -- ``batch_system`` never reports anything
        else).  Without an end the limit is all there is, exactly as before.
        '''
        if end is not None:
            remaining = int(end - time.time())
            if remaining <= 0:
                raise FederationStateError(
                    f'the allocation has no time left (ended {-remaining}s '
                    'ago): join a live allocation, or use mode login')
            return remaining
        return max(1, int(runtime or _DEFAULT_WALLTIME))

    @staticmethod
    def _allocation_pool(rec: ResourceRecord, alloc: dict) -> dict:
        '''Return the allocation-mode half of a pool declaration.

        The pilot **is** the allocation, literally: the endpoint that joined
        runs inside it, so the declaration says ``pilot: endpoint`` and the
        dispatcher adopts that endpoint instead of submitting a second
        process onto it.  One pilot, live at join (``pilot: endpoint`` pins
        ``min_pilots`` / ``max_pilots`` to 1 in ``MemberRecord``), sized
        from what the endpoint reports about its own job and deadlined by
        that job's ``end_time``.

        Per-node counts come from the allocation first, because a declared
        ``cores`` / ``gpus`` is a *total* for the resource while
        ``cpus_per_node`` is exactly what its name says.  Dividing the
        declared total by the node count is the fallback when the endpoint
        reports no per-node figure.
        '''
        nodes = int(alloc.get('n_nodes') or 1) or 1

        cpus = alloc.get('cpus_per_node')
        if not cpus:
            cores = rec.capability('cores')
            cpus  = max(1, int(cores) // nodes) if cores else 1

        gpus = alloc.get('gpus_per_node')
        if not gpus:
            all_gpus = rec.capability('gpus')
            gpus     = (int(all_gpus) // nodes) if all_gpus else 0

        end_time = alloc.get('end_time') or None
        return {
            'queue'      : 'allocation',
            'account'    : None,
            'pilot'      : PILOT_ENDPOINT,
            'end_time'   : end_time,
            'pilot_sizes': {_SIZE_KEY: {
                'nodes'           : nodes,
                'cpus_per_node'   : max(1, int(cpus)),
                'gpus_per_node'   : max(0, int(gpus)),
                'walltime_sec'    : PluginFederation._allocation_walltime(
                    alloc.get('runtime'), end_time),
                'rhapsody_backend': _DEFAULT_BACKEND,
            }},
        }

    @staticmethod
    def _login_pool(rec: ResourceRecord) -> dict:
        '''Return the login-mode half of a pool declaration.

        Everything comes from the declared ``pool`` block, validated here so
        a bad declaration is a 400 rather than a 500 raised deep inside pool
        construction or the dispatcher's own parser.  Unknown keys are
        rejected rather than ignored — a typo that silently drops
        ``max_pilots`` is worse than a refused join.
        '''
        decl = rec.pool
        if not isinstance(decl, dict):
            raise FederationStateError(
                "login mode requires a 'pool' declaration")

        unknown = set(decl) - _LOGIN_POOL_KEYS
        if unknown:
            raise FederationStateError(
                f"unknown 'pool' field(s): {', '.join(sorted(unknown))} "
                f"(known: {', '.join(sorted(_LOGIN_POOL_KEYS))})")

        queue = decl.get('queue')
        if not isinstance(queue, str) or not queue:
            raise FederationStateError(
                "'pool.queue' must be a non-empty string")
        if queue == 'default':
            raise FederationStateError(
                "'pool.queue' must not be the dispatcher sentinel 'default'")

        account = decl.get('account')
        if account is not None and not isinstance(account, str):
            raise FederationStateError(
                "'pool.account' must be a string or null")

        backend = decl.get('rhapsody_backend') or _DEFAULT_BACKEND
        if not isinstance(backend, str) or not backend:
            raise FederationStateError(
                "'pool.rhapsody_backend' must be a non-empty string")

        max_pilots = validate_pool_int(decl, 'max_pilots', default=1,
                                       minimum=1, maximum=_MAX_PILOTS_CAP)
        min_pilots = validate_pool_int(decl, 'min_pilots', default=0,
                                       minimum=0, maximum=max_pilots)
        return {
            'queue'      : queue,
            'account'    : account,
            'min_pilots' : min_pilots,
            'max_pilots' : max_pilots,
            'pilot_sizes': {_SIZE_KEY: {
                'nodes'        : validate_pool_int(
                    decl, 'nodes', minimum=1, maximum=_MAX_NODES_CAP),
                'cpus_per_node': validate_pool_int(
                    decl, 'cpus_per_node', minimum=1,
                    maximum=_MAX_CPUS_CAP),
                'gpus_per_node': validate_pool_int(
                    decl, 'gpus_per_node', default=0, minimum=0,
                    maximum=_MAX_GPUS_CAP),
                'walltime_sec' : validate_pool_int(
                    decl, 'walltime_sec', minimum=1,
                    maximum=_MAX_WALLTIME_CAP),
                'rhapsody_backend': backend,
            }},
        }

    # -- routes ----------------------------------------------------------

    async def _route_join(self, request: Request) -> dict:
        '''Join one resource: validate, build its members, add them to pools.

        Ordering matters: everything that can be rejected is rejected before
        the dispatcher is touched, so a bad join leaves nothing behind — and
        a join that fails *part way through* its members rolls the earlier
        ones back, so a join is all-or-nothing.
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            raise HTTPException(status_code=400,
                                detail='join body must be a JSON object')

        declared_members = bool(body.get('members'))
        try:
            if declared_members and body.get('pool') is not None:
                raise FederationStateError(
                    "'pool' and 'members' are mutually exclusive: the flat "
                    "'pool' block is the single-member shorthand, so a join "
                    'that lists members declares each one there')
            name = validate_name(body.get('name'))
            mode = body.get('mode', MODE_ALLOCATION)
            if mode not in MODES:
                raise FederationStateError(
                    f"'mode' must be one of {', '.join(MODES)} "
                    f'(got {mode!r})')
            endpoint = body.get('endpoint')
            if not isinstance(endpoint, str) or not endpoint:
                raise FederationStateError(
                    "'endpoint' must be a non-empty string")
            caps = validate_capabilities(body.get('capabilities'))
            # with members the budget lives on each member; the record-level
            # one is the aggregate and is recomputed from them
            budget = validate_budget(
                body.get('budget'),
                required=(mode == MODE_LOGIN and not declared_members))
            # a resource on another machine names a scratch tree on ITS host
            shared = body.get('shared_fs', True)
            if not isinstance(shared, bool):
                raise FederationStateError("'shared_fs' must be a bool")
            scratch = body.get('scratch_base')
            scratch = (validate_scratch_for_host(scratch, shared=shared)
                       if scratch else str(self._scratch_for(name)))
        except FederationStateError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        if name in self._state.resources:
            raise HTTPException(status_code=409,
                                detail=f'resource exists: {name}')
        if self._liveness_for(endpoint) == LIVENESS_LOST:
            raise HTTPException(
                status_code=404,
                detail=f'endpoint not connected: {endpoint}')
        # The broker is a participant too, and it hosts no psij — a pool
        # bound to it could never launch a pilot.  Refuse the declaration
        # rather than create a pool that will only ever fail.
        if self._participant(endpoint).get('role') == 'broker':
            raise HTTPException(
                status_code=400,
                detail=f'{endpoint} is the broker, not a compute resource')

        rec = ResourceRecord(
            name           = name,
            endpoint       = endpoint,
            mode           = mode,
            site           = str(body.get('site') or ''),
            kind           = str(body.get('kind') or ''),
            capabilities   = await self._discover_capabilities(endpoint, caps),
            budget         = budget,
            scratch_base   = scratch,
            shared_fs      = shared,
            pool           = None if declared_members else body.get('pool'),
            joined_at      = time.time(),
            dispatcher_sid = FED_SESSION_SID,
            liveness       = self._liveness_for(endpoint),
        )

        try:
            members = await self._build_members(rec, body)
        except FederationStateError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        # An undeclared allocation budget is the allocation itself: the pilot
        # holds ``nodes`` for at most its walltime, so that is exactly what
        # this join may spend.
        for member in members:
            if not member.budget_node_hours():
                member.budget = {'node_hours': round(
                    member.nodes * member.walltime_sec / 3600.0, 4)}
            member.member_id = f'{name}.{member.member}'
            member.pool_name = self.pool_name_for_class(member.cls)
            member.liveness  = rec.liveness
            member.usage.node_hours_remaining = member.budget_node_hours()
            # the dispatcher builds a participant name out of both halves and
            # refuses a declaration past its cap; say so here, before any
            # member has been POSTed, rather than half way through the join
            length = len(member.pool_name) + len(member.member_id)
            if length > MAX_POOL_MEMBER_NAME_LEN:
                raise HTTPException(
                    status_code=400,
                    detail=f'pool name plus member id must be at most '
                           f'{MAX_POOL_MEMBER_NAME_LEN} characters, got '
                           f'{length} for {member.pool_name}/'
                           f'{member.member_id}')
            rec.members[member.member] = member

        rec.aggregate()
        rec.pool_name = members[0].pool_name
        rec.usage.node_hours_remaining = rec.budget_node_hours()

        # R5: a class name is free-form, so a typo would create a second,
        # invisible pool.  Only worth saying once the federation has other
        # members to be inconsistent with — on the very first join every
        # class is new by definition.
        known = {m.cls for r in self._state.resources.values()
                 for m in r.member_list()}
        if known:
            for cls in sorted({m.cls for m in members} - known):
                log.warning('[%s] %r declares class %r, which no other '
                            'member uses — a typo would create a pool '
                            'nothing routes to', self.instance_name, name,
                            cls)

        # The dispatcher is the last thing touched: after this the join has
        # side effects that ``leave`` has to undo.
        await self._register_fed(rec)
        added: list[MemberRecord] = []
        try:
            for member in members:
                await self._dispatcher.add_member(
                    FED_SESSION_SID, member.pool_name,
                    self._member_decl(rec, member))
                added.append(member)
        except Exception:
            # a join is all-or-nothing.  ``force`` matters: the first member
            # of a brand-new class pool is also its last, and the dispatcher
            # refuses to remove a last member without it.
            for member in added:
                try:
                    await self._dispatcher.del_member(
                        FED_SESSION_SID, member.pool_name, member.member_id,
                        cancel_tasks=False, force=True)
                except Exception as e:
                    log.warning('[%s] join %r: rollback of member %s '
                                'failed: %s', self.instance_name, name,
                                member.member_id, e)
            raise

        # only a shared tree is the broker's to create: an unshared
        # scratch_base names a directory on the resource's own host, which
        # its pilot creates -- mkdir'ing that path here would make a stray
        # broker-local directory, never the one the tasks will use.
        if rec.shared_fs:
            try:
                Path(rec.scratch_base).mkdir(parents=True, exist_ok=True)
            except OSError as e:
                log.warning('[%s] could not create scratch %s: %s',
                            self.instance_name, rec.scratch_base, e)

        self._state.resources[name] = rec
        self._attached.update(m.member_id for m in members)
        self._state.save()
        log.info('[%s] joined %r on %s (%s, %d member(s): %s)',
                 self.instance_name, name, endpoint, mode, len(members),
                 ', '.join(f'{m.member_id}→{m.pool_name}' for m in members))
        return rec.to_wire()

    async def _route_leave(self, request: Request) -> dict:
        '''Remove a resource: drop each of its members from its class pool.

        Three things this deliberately does **not** do any more:

        - it does not ``unregister_session`` — the ``fed`` session holds
          every *other* resource's class pools too;
        - it does not blanket-cancel the ledger.  That was right when a pool
          served exactly one resource; with class pools a queued task can
          legitimately run somewhere else.  ``{"cancel_tasks": true}``
          restores the full teardown;
        - it does not drop the live ledger entries.  A re-queued task keeps
          running on a sibling member, and ``GET task/…`` 404s without its
          entry — which a campaign runner reads as a hard failure.  So the
          non-terminal entries stay, re-pointed to ``resource: null``, and
          the next poll fills the real placement back in.

        ``cancel_tasks`` needs one thing done **here**, not by the
        dispatcher: ``del_member(cancel_tasks=True)`` fails the tasks that
        were on the removed member's *pilots* (121 §7.4), and a task merely
        *attributed* to this resource by an advisory submit is still QUEUED
        with no pilot at all.  Those would survive the drain and then lose
        their ledger entry to the teardown — a live task answering 404.  So
        every non-terminal entry of the resource is cancelled explicitly
        first, and only the terminal ones are then dropped.

        ``fail_unsatisfiable`` is left at the dispatcher's default here: an
        explicit ``leave`` means the resource is gone, so a task only it
        could run should fail now rather than wait forever.  (The liveness
        path passes ``false`` — that is the difference between "gone" and
        "blinked".)
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        cancel_tasks = bool((body or {}).get('cancel_tasks'))

        name = request.path_params['name']
        rec  = self._resource(name)

        removed = requeued = failed = 0
        errors: list[str] = []
        for member in rec.member_list():
            try:
                resp = await self._dispatcher.del_member(
                    FED_SESSION_SID, member.pool_name, member.member_id,
                    cancel_tasks=cancel_tasks, force=True)
                removed  += 1
                requeued += int((resp or {}).get('tasks_requeued') or 0)
                failed   += int((resp or {}).get('tasks_failed')   or 0)
            except Exception as e:
                log.info('[%s] leave %r: removing member %s failed: %s',
                         self.instance_name, name, member.member_id, e)
                errors.append(f'{member.member_id}: {e}')
            self._attached.discard(member.member_id)
            self._detail_cache.pop(member.pool_name, None)

        cancelled = await self._cancel_ledger(name) if cancel_tasks else 0

        # An emptied class pool is left in place: with no members it has no
        # pilots and dispatches nothing, and the next join of that class
        # reuses it.
        self._state.drop_resource(name, keep_active=not cancel_tasks)
        self._state.save()
        log.info('[%s] left %r (%d member(s) removed, %d task(s) requeued, '
                 '%d failed, %d cancelled, %d error(s))', self.instance_name,
                 name, removed, requeued, failed, cancelled, len(errors))
        out = {'resource'       : name,
               'ok'             : not errors,
               'members_removed': removed,
               'tasks_requeued' : requeued,
               'tasks_failed'   : failed,
               'tasks_cancelled': cancelled}
        if errors:
            # a partial teardown is not a success: the resource is forgotten
            # either way (its endpoint may be gone for good), but the caller
            # is told which members the dispatcher still believes in
            out['errors'] = errors
        return out

    async def _cancel_ledger(self, name: str) -> int:
        '''Cancel every non-terminal ledger entry of resource *name*.

        The counterpart of ``del_member(cancel_tasks=True)``, for the tasks
        that drain cannot see: the dispatcher fails what was running on the
        removed member's pilots, but a task the federation only *attributed*
        to this resource (the advisory placement a submit answers with) is
        still QUEUED in the class pool, bound to no pilot and to no member.
        Dropping its ledger entry without cancelling it would leave a live
        task that ``GET task/…`` answers 404 for.

        A cancel that fails is logged and the entry is marked ``CANCELED``
        anyway: the caller asked for a teardown, and the entry is about to
        be dropped regardless — the dispatcher's own state stays the
        authority for anything still running there.
        '''
        count = 0
        for entry in self._state.ledger_for(name):
            if entry.state in _TERMINAL_TASK_STATES:
                continue
            try:
                await self._dispatcher.cancel_task(
                    entry.dispatcher_sid or FED_SESSION_SID, entry.task_id)
            except Exception as e:
                log.info('[%s] leave %r: cancelling task %s failed: %s',
                         self.instance_name, name, entry.task_id, e)
            entry.state       = 'CANCELED'
            entry.finished_at = time.time()
            count += 1
        return count

    async def _route_resources(self, request: Request) -> dict:
        '''List every resource, usage refreshed.'''
        await self._require_session(request.path_params['sid'])
        await self._refresh_all()
        return {'resources': [r.to_wire() for r in
                              sorted(self._state.resources.values(),
                                     key=lambda r: r.name)]}

    async def _route_resource(self, request: Request) -> dict:
        '''Return one resource record, usage refreshed.'''
        await self._require_session(request.path_params['sid'])
        rec = self._resource(request.path_params['name'])
        await self._refresh_usage(rec)
        return rec.to_wire()

    async def _route_pick(self, request: Request):
        '''Return the **class** the policy chooses for a requirement set.

        Plus the members the dispatcher would consider, for display only.
        ``resource`` is the highest-scoring member's resource and is
        explicitly **advisory**: the binding placement is made by the
        dispatcher at dispatch and reported by ``task``.
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        requirements = (body or {}).get('requirements') or {}
        if not isinstance(requirements, dict):
            raise HTTPException(status_code=400,
                                detail="'requirements' must be an object")
        await self._refresh_all()
        chosen = self._pick(requirements)
        if chosen is None:
            return self._no_resource(requirements)
        cls, score, ranked = chosen
        return {'pool'          : self.pool_name_for_class(cls),
                'class'         : cls,
                'dispatcher_sid': FED_SESSION_SID,
                'members'       : self._member_view(ranked),
                'resource'      : self._advisory_resource(ranked),
                'score'         : score}

    async def _route_submit(self, request: Request):
        '''Choose a class and submit one task to its pool.

        The single call a workload manager needs: it never learns a pool
        name, a dispatcher session, or an endpoint — and, since class pools,
        not even a resource, because the resource is not chosen yet.

        Three things travel differently from before:

        - **no ``cwd``**, ever.  The dispatcher assigns it at dispatch from
          the member that actually runs the task, which is the only correct
          answer once one pool can mix members on different filesystems.  A
          client-supplied ``task.cwd`` is refused: the federation cannot
          honour it across members.
        - **``inputs_b64`` rides along**, forwarded verbatim.  That is what
          removes the client's need to know the placement before the
          placement exists; the dispatcher spools the files and puts them
          wherever the task lands.
        - **``requirements`` are forwarded** (minus the federation-only
          ``node_hours``, which the dispatcher's parser would reject as an
          unknown key), so the dispatcher can match software and shape
          against each member.
        '''
        await self._require_session(request.path_params['sid'])
        try:
            body = await request.json()
        except Exception:
            body = {}
        task = (body or {}).get('task') or {}
        if not isinstance(task, dict):
            raise HTTPException(status_code=400,
                                detail="'task' must be an object")
        task_id = task.get('task_id')
        cmd     = task.get('cmd')
        if not task_id or not isinstance(cmd, list) or not cmd:
            raise HTTPException(
                status_code=400,
                detail="task requires 'task_id' and a non-empty 'cmd' list")
        if task.get('cwd'):
            raise HTTPException(
                status_code=400,
                detail="'task.cwd' is not accepted: a class pool assigns the "
                       'cwd at dispatch, from the member that runs the task')
        requirements = body.get('requirements') or {}
        if not isinstance(requirements, dict):
            raise HTTPException(status_code=400,
                                detail="'requirements' must be an object")

        # a declaration error, not a server error: ``int('high')`` would
        # otherwise raise out of the payload construction below as a 500
        priority = task.get('priority') or 0
        if isinstance(priority, bool) or not isinstance(priority, (int,
                                                                   float)):
            raise HTTPException(status_code=400,
                                detail="'task.priority' must be a number")

        inputs_b64 = task.get('inputs_b64')
        if inputs_b64 is not None:
            if not isinstance(inputs_b64, dict) or not all(
                    isinstance(k, str) and isinstance(v, str)
                    for k, v in inputs_b64.items()):
                raise HTTPException(
                    status_code=400,
                    detail="'task.inputs_b64' must be an object of "
                           '{filename: base64}')

        await self._refresh_all()
        chosen = self._pick(requirements)
        if chosen is None:
            return self._no_resource(requirements)
        cls, _score, ranked = chosen
        pool = self.pool_name_for_class(cls)

        payload = {
            'pool'    : pool,
            'task_id' : task_id,
            'cmd'     : list(cmd),
            'priority': int(priority),
            'inputs'  : list(task.get('inputs')  or []),
            'outputs' : list(task.get('outputs') or []),
        }
        forward = {k: v for k, v in requirements.items()
                   if k not in _FEDERATION_ONLY_REQUIREMENTS}
        if forward:
            payload['requirements'] = forward
        if inputs_b64:
            payload['inputs_b64'] = dict(inputs_b64)

        # A dispatcher 400 ("no member satisfies the task requirements")
        # propagates verbatim — status and detail — so the caller sees the
        # dispatcher's own vocabulary rather than a re-worded guess.
        result = await self._dispatcher.submit(FED_SESSION_SID, payload)

        advisory = self._advisory_resource(ranked)
        self._state.ledger[str(task_id)] = SubmitLedgerEntry(
            task_id        = str(task_id),
            resource       = advisory,
            pool           = pool,
            dispatcher_sid = FED_SESSION_SID,
            state          = str((result or {}).get('state') or 'QUEUED'),
            submitted_at   = time.time(),
            member_id      = None,
            cls            = cls,
        )
        self._state.save()
        return {'task'            : result,
                'pool'            : pool,
                'class'           : cls,
                'dispatcher_sid'  : FED_SESSION_SID,
                'resource'        : advisory,
                'member'          : None,
                'members_eligible': [m.member_id for m, _ in ranked]}

    async def _route_task(self, request: Request) -> dict:
        '''Proxy one task's dispatcher record, annotated with its placement.

        **This is where placement becomes true.** The dispatcher's
        ``member_id`` is the authoritative answer — it is set at dispatch —
        so it overwrites the advisory resource the submit answered with, and
        it is also what re-points an entry whose resource has left the
        federation.  Before dispatch the advisory value stands, and
        ``resource`` may legitimately be ``null`` (a task whose resource left
        and which has not been re-dispatched yet); a caller must treat that
        as "not placed", not as a failure.

        ``child_endpoint`` is added while the task's pilot is still alive:
        that is the endpoint name a caller needs to reach the pilot's own
        ``staging`` plugin, and it disappears from the dispatcher API the
        moment the pilot ends.

        Two entries are answered **from the ledger alone**: one whose
        dispatcher session is not ``fed`` (a pre-08 task whose per-resource
        session the upgrade released — see :meth:`_upgrade_legacy`), because
        the dispatcher would 404 on a sid it no longer holds and the ledger
        already carries the ``FAILED`` verdict; and, implicitly, nothing
        else — a live task is always the dispatcher's to answer.
        '''
        await self._require_session(request.path_params['sid'])
        task_id = request.path_params['task_id']
        entry   = self._state.ledger.get(task_id)
        if entry is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown task: {task_id}')

        sid = entry.dispatcher_sid or FED_SESSION_SID
        if sid != FED_SESSION_SID:
            return self._ledger_view(entry)

        task = await self._dispatcher.task(sid, task_id)
        task = dict(task or {})

        dirty = False
        state = str(task.get('state') or entry.state)
        if state != entry.state:
            entry.state = state
            if state in _TERMINAL_TASK_STATES:
                entry.finished_at = task.get('finished_at') or time.time()
            dirty = True

        member_id = task.get('member_id') or None
        if member_id and member_id != entry.member_id:
            entry.member_id = member_id
            entry.resource  = self.split_member_id(member_id)[0] or None
            dirty = True
        elif not member_id and entry.member_id and \
                state not in _TERMINAL_TASK_STATES:
            # the dispatcher clears ``member_id`` beside ``pilot_id`` when it
            # re-queues a task off a lost pilot (121 §7.3).  The entry must
            # follow, or ``tasks_running`` keeps counting this task on a
            # member that is no longer running it.  ``resource`` stays as it
            # is: advisory, and the next dispatch overwrites both.
            entry.member_id = None
            dirty = True
        if dirty:
            self._state.save()

        member_id = entry.member_id
        task['member_id'] = member_id
        task['member']    = (self.split_member_id(member_id)[1]
                             if member_id else None)
        task['resource']  = entry.resource
        if entry.cls:
            task['class'] = entry.cls

        pilot_id = task.get('pilot_id')
        if entry.pool and pilot_id:
            detail = await self._pool_detail(entry.pool)
            for pilot in (detail or {}).get('pilots') or []:
                if pilot.get('pid') == pilot_id:
                    task['child_endpoint'] = pilot.get('child_endpoint_name')
                    break
        return task

    def _ledger_view(self, entry: SubmitLedgerEntry) -> dict:
        '''Return the ledger's own answer for a task the dispatcher cannot.

        Same key set a proxied record carries — ``state``, ``member_id``,
        ``member``, ``resource``, ``class`` — so a caller polling in a loop
        does not have to branch on which half answered.  ``detail`` carries
        the reason the ledger recorded (``'the federation was upgraded'``
        for the pre-08 tasks), and no ``child_endpoint`` is offered: the
        pilot that had one is gone.
        '''
        member_id = entry.member_id
        task = {'task_id'       : entry.task_id,
                'state'         : entry.state,
                'pool'          : entry.pool,
                'pilot_id'      : None,
                'submitted_at'  : entry.submitted_at,
                'finished_at'   : entry.finished_at,
                'member_id'     : member_id,
                'member'        : (self.split_member_id(member_id)[1]
                                   if member_id else None),
                'resource'      : entry.resource,
                'dispatcher_sid': entry.dispatcher_sid}
        if entry.detail:
            task['detail'] = entry.detail
        if entry.cls:
            task['class'] = entry.cls
        return task

    # -- policy ----------------------------------------------------------

    def _classes(self) -> dict[str, list]:
        '''Return ``{class: [members]}`` over every joined resource.'''
        out: dict[str, list] = {}
        for rec in self._state.resources.values():
            for member in rec.member_list():
                out.setdefault(member.cls, []).append(member)
        return out

    def _members(self) -> list:
        '''Return every member of every joined resource.'''
        return [m for rec in self._state.resources.values()
                for m in rec.member_list()]

    def _member_view(self, ranked: list) -> list[dict]:
        '''Render an eligible-member ranking for a client (display only).'''
        return [{'member_id': m.member_id,
                 'resource' : self.split_member_id(m.member_id)[0],
                 'score'    : round(float(score), 6),
                 'reason'   : None}
                for m, score in ranked]

    def _advisory_resource(self, ranked: list) -> str | None:
        '''Return the top-scoring member's resource, or ``None``.

        Advisory by construction: the dispatcher makes the binding choice at
        dispatch.  It is populated anyway so a UI has a chip to show at once
        — one that may change exactly once, when the first poll lands.
        '''
        if not ranked:
            return None
        return self.split_member_id(ranked[0][0].member_id)[0] or None

    def _pick(self, requirements: dict):
        '''Return ``(class, score, ranked members)``, or ``None``.

        Swallows a policy blow-up into "nothing fits": a broken custom
        policy must not turn every submit into a 500.
        '''
        try:
            classes = self._classes()
            chosen  = self._policy.pick_class(requirements, classes)
            if chosen is None:
                return None
            cls, score = chosen
            ranked = self._policy.eligible(requirements,
                                           classes.get(cls) or [])
            return cls, score, ranked
        except Exception as e:
            log.exception('[%s] policy pick raised: %s',
                          self.instance_name, e)
            return None

    def _no_resource(self, requirements: dict) -> JSONResponse:
        '''Return the 409 body: the verdict plus why each **member** lost.

        A caller that cannot place work needs the reasons, not just the
        refusal — "gpus 0 < 1 on a.cpu, node_hours exhausted on b.gpu" is
        actionable where a bare 409 is not.  The map is keyed by
        ``member_id`` now, because that is the granularity the decision is
        made at.  Returned as a **response object** rather than raised so the
        reasons are a first-class part of the body: a raised
        ``HTTPException`` renders through the gateway's canonical error
        envelope, whose ``detail`` reads as a human message, and smuggling a
        dict through it would leave in-process and HTTP callers looking at
        different shapes.  In-process callers must therefore check
        ``resp.status_code`` — ``pick`` and ``submit`` are the two routes
        that can answer without raising.
        '''
        try:
            reasons = self._policy.explain(requirements, self._members())
        except Exception as e:
            log.exception('[%s] policy explain raised: %s',
                          self.instance_name, e)
            reasons = {}
        return JSONResponse(status_code=409, content={
            'detail' : 'no resource satisfies requirements',
            'reasons': reasons,
        })

    # -- usage accounting -------------------------------------------------

    async def _pool_detail(self, pool: str) -> dict | None:
        '''Return one class pool's verbose summary, cached for 2 s.

        Keyed by **pool**, not by resource: a pool is a capability class
        shared by every resource that declares it, so N resources in two
        classes cost two dispatcher round-trips, not N.  One round-trip
        serves both the usage refresh and the ``child_endpoint`` lookup, so
        a caller polling a task at 1 Hz does not multiply calls.  ``None``
        when the dispatcher is unreachable or slow — the caller decides what
        a missing answer means.
        '''
        now    = time.time()
        cached = self._detail_cache.get(pool)
        if cached and now - cached[0] < _USAGE_CACHE_SEC:
            return cached[1]
        try:
            detail = await asyncio.wait_for(
                self._dispatcher.pool_detail(FED_SESSION_SID, pool),
                _USAGE_TIMEOUT_SEC)
        except Exception as e:
            log.info('[%s] usage refresh for pool %r failed: %s',
                     self.instance_name, pool, e)
            return None
        self._detail_cache[pool] = (now, detail)
        return detail

    def _apply_member_usage(self, member: MemberRecord,
                            detail: dict | None, now: float) -> None:
        '''Recompute one member's usage from its class pool's summary.

        Node-hours and live pilots come straight off the dispatcher's
        per-member block — no arithmetic here, so the Explorer, the CLI and
        the federation cannot disagree about the same number.  Task counts
        come from the federation's own ledger, because the dispatcher keeps
        only the 50 most recent tasks per pool.

        The pilot-failure fields (``pilot_error``, ``pilot_failures``,
        ``paused_until``) come from the same per-member block and are
        **cleared** when it reports none: a member that has just produced a
        healthy pilot must not keep wearing the last failure.  So does
        ``remaining_sec``, the walltime its longest-lived pilot still has —
        the deadline is dispatcher state, and a member with an ``end_time``
        of its own answers from that instead (see
        :meth:`MemberRecord.remaining_sec`).
        '''
        usage   = member.usage
        summary = None
        if detail is not None:
            summary = next((e for e in (detail.get('members') or [])
                            if e.get('member_id') == member.member_id),
                           None)
        if summary is None:
            # a failed refresh keeps the previous numbers: a member must not
            # blink to zero because one poll timed out.  The dispatcher is
            # hosted in this process, so a class pool summary always
            # carries the per-member block; a missing entry is a member the
            # summary predates, and reads the same as a failed refresh.
            usage.stale = True
        else:
            usage.stale = False
            usage.node_hours_used = float(
                summary.get('node_hours_used') or 0.0)
            usage.pilots_active  = int(summary.get('pilots_active') or 0)
            remaining            = summary.get('node_hours_remaining')
            usage.pilot_error    = summary.get('last_pilot_error') or None
            usage.pilot_failures = int(
                summary.get('consecutive_pilot_failures') or 0)
            usage.paused_until   = summary.get('paused_until') or None
            usage.remaining_sec  = summary.get('remaining_sec')

            budget = member.budget_node_hours()
            if remaining is None:
                remaining = (budget - usage.node_hours_used
                             if budget else 0.0)
            # never negative, whoever computed it: an overspent member has
            # nothing left, not a debt, and a negative term would otherwise
            # drag the resource-level sum below the siblings that still have
            # budget — and score a member as *worse than empty*
            usage.node_hours_remaining = max(0.0, float(remaining or 0.0))

        (usage.tasks_running,
         usage.tasks_done,
         usage.tasks_failed) = self._state.member_task_counts(
            member.member_id)
        usage.updated_at = now

    def _aggregate_usage(self, rec: ResourceRecord, now: float) -> None:
        '''Sum the member usages onto the resource row.

        Task counts are **not** summed: a task the dispatcher has not placed
        yet belongs to no member, so it would vanish.  They come from the
        ledger by resource, which counts it.

        The pilot-failure fields are not summed either — they are a
        *report*, not a quantity.  The resource row shows the worst of its
        members: the first error text there is one, the highest failure
        count and the furthest pause.
        '''
        members = rec.member_list()
        usage   = rec.usage
        usage.node_hours_used      = sum(m.usage.node_hours_used
                                         for m in members)
        usage.node_hours_remaining = sum(m.usage.node_hours_remaining
                                         for m in members)
        usage.pilots_active        = sum(m.usage.pilots_active
                                         for m in members)
        usage.stale                = any(m.usage.stale for m in members)
        usage.pilot_error          = next(
            (m.usage.pilot_error for m in members if m.usage.pilot_error),
            None)
        usage.pilot_failures       = max(
            [m.usage.pilot_failures for m in members] or [0])
        usage.paused_until         = max(
            [m.usage.paused_until for m in members
             if m.usage.paused_until] or [0.0]) or None
        (usage.tasks_running,
         usage.tasks_done,
         usage.tasks_failed) = self._state.task_counts(rec.name)
        usage.updated_at = now

    async def _refresh_usage(self, rec: ResourceRecord) -> None:
        '''Refresh one resource: its members, then its aggregate row.'''
        now = time.time()
        if now - rec.usage.updated_at < _USAGE_CACHE_SEC:
            return
        pools   = sorted({m.pool_name for m in rec.member_list()})
        details = {p: await self._pool_detail(p) for p in pools}
        for member in rec.member_list():
            self._apply_member_usage(member, details.get(member.pool_name),
                                     now)
        self._aggregate_usage(rec, now)

    async def _refresh_all(self) -> None:
        '''Refresh every resource (each still 2 s-cached).

        One dispatcher call per **distinct class pool**, all concurrently:
        each is one round-trip with a 3 s timeout, so a federation with one
        slow pool waits 3 s rather than 3 s × pools.
        '''
        now = time.time()
        due = [r for r in self._state.resources.values()
               if now - r.usage.updated_at >= _USAGE_CACHE_SEC]
        if not due:
            return
        pools   = sorted({m.pool_name for r in due for m in r.member_list()})
        fetched = await asyncio.gather(
            *(self._pool_detail(p) for p in pools), return_exceptions=True)
        details = {p: (d if isinstance(d, dict) else None)
                   for p, d in zip(pools, fetched)}
        for rec in due:
            for member in rec.member_list():
                self._apply_member_usage(
                    member, details.get(member.pool_name), now)
            self._aggregate_usage(rec, now)

    # -- topology / attachment -------------------------------------------

    async def on_topology_change(self, participants: dict) -> None:
        '''Track endpoint liveness and keep dispatcher sessions attached.

        A resource inherits its endpoint's liveness verbatim: ``present`` →
        ``ok``, ``suspect`` → ``suspect`` (the policy will not route there,
        but nothing is torn down over a blip), anything else → ``lost``.
        '''
        participants = participants or {}
        self._participants = {
            name: {'liveness': str((info or {}).get('liveness')
                                   or LIVENESS_LOST),
                   'role'    : str((info or {}).get('role') or '')}
            for name, info in participants.items()
        }

        if not self._replayed:
            self._replayed = True
            await self._replay_attachments()

        await self._sync_attachments()
        await super().on_topology_change(participants)

    async def _upgrade_legacy(self) -> None:
        '''Release the per-resource ``fed-<name>`` sessions of a pre-08 state.

        A record written before class pools carries its own dispatcher sid.
        After an in-place upgrade the dispatcher has replayed those pools off
        disk — but **owner-less**: housekeeping skips a pool whose session is
        not live, while ``unregister_session`` on a sid the dispatcher does
        not know is a 404 that tears down nothing.  So each legacy sid is
        **re-owned first and released second**: ``register_session(old_sid,
        [stored pool])`` then ``unregister_session(old_sid)``, which runs the
        ordinary teardown and actually cancels the pilots instead of leaving
        them holding a psij job and a child endpoint.

        The tasks of those pools cannot be recovered — their pool has just
        been torn down — so their live ledger entries are failed with a plain
        reason rather than left polling a session that no longer exists.  The
        records themselves lose nothing: their single member was already
        derived from the stored declaration at load time.
        '''
        legacy: dict[str, ResourceRecord] = {}
        for rec in self._state.resources.values():
            sid = rec.dispatcher_sid
            if sid and sid != FED_SESSION_SID:
                legacy.setdefault(sid, rec)
        if not legacy:
            return

        log.info('[%s] upgrading %d pre-08 dispatcher session(s) to the '
                 'single %r session', self.instance_name, len(legacy),
                 FED_SESSION_SID)
        for sid, rec in legacy.items():
            try:
                if rec.pool_config:
                    await self._dispatcher.register_session(
                        sid, [rec.pool_config])
                await self._dispatcher.unregister_session(sid)
            except Exception as e:
                log.warning('[%s] releasing legacy session %r failed: %s',
                            self.instance_name, sid, e)

        for entry in self._state.ledger.values():
            if entry.dispatcher_sid and \
                    entry.dispatcher_sid != FED_SESSION_SID and \
                    entry.state not in _TERMINAL_TASK_STATES:
                entry.state       = 'FAILED'
                entry.detail      = 'the federation was upgraded'
                entry.finished_at = time.time()

        for rec in self._state.resources.values():
            rec.dispatcher_sid = FED_SESSION_SID
            members = rec.member_list()
            if members:
                rec.pool_name = members[0].pool_name

    async def _replay_attachments(self) -> None:
        '''Re-attach every stored member to its class pool, once.

        In order: release any pre-08 per-resource session, register ``fed``
        with the **full** class-pool list, then re-POST *every* member of
        *every* stored record.  The dispatcher has already replayed those
        pools with their persisted members, so the re-POST is a no-op — and
        it is the recovery path when the dispatcher's own state was wiped,
        which is exactly why an identical re-POST must be a no-op rather
        than an error.

        Called from whichever comes first after a restart: the first
        topology delivery (the normal case) or the first route
        (:meth:`_require_session`).  Liveness is untouched here — every
        record loaded from disk starts ``lost`` and only topology promotes
        it — so a route arriving in that window can still *read* and *poll*
        a resource, while ``pick`` correctly refuses to route new work to an
        endpoint nobody has seen yet.
        '''
        await self._upgrade_legacy()

        try:
            await self._register_fed()
        except Exception as e:
            log.warning('[%s] could not register the %r session: %s',
                        self.instance_name, FED_SESSION_SID, e)
            for rec in self._state.resources.values():
                rec.liveness = LIVENESS_LOST
            self._state.save()
            return

        for rec in list(self._state.resources.values()):
            for member in rec.member_list():
                try:
                    await self._dispatcher.add_member(
                        FED_SESSION_SID, member.pool_name,
                        self._member_decl(rec, member))
                    self._attached.add(member.member_id)
                except Exception as e:
                    log.warning('[%s] could not re-attach member %s: %s',
                                self.instance_name, member.member_id, e)
                    member.liveness = LIVENESS_LOST
                    rec.liveness    = LIVENESS_LOST
        self._state.save()

    async def _sync_attachments(self) -> None:
        '''Apply endpoint liveness to every member, at member granularity.

        | endpoint | action |
        |---|---|
        | ``present``, not attached | ``add_member`` (re-declaring the class pool first when it no longer exists — every member of that class may have left while the endpoint was down) -- unless the member's ``end_time`` has passed: that endpoint is back in a new allocation, and stays detached (``lost``) until it leaves and joins again |
        | ``suspect`` | mark ``suspect``, **nothing else**.  The policy already refuses to route to a non-``ok`` member, which is the whole point of ``suspect``; a blip must not touch the dispatcher |
        | ``lost``, attached | ``del_member`` with ``fail_unsatisfiable=False``.  Its pilots died with the endpoint anyway and its RUNNING tasks re-queue onto a sibling — but a task only *this* member could run stays QUEUED instead of failing, because a lost endpoint is very often back in a minute |
        '''  # noqa: E501
        dirty = False
        for rec in list(self._state.resources.values()):
            target = self._liveness_for(rec.endpoint)
            result = target

            for member in rec.member_list():
                if target == LIVENESS_LOST:
                    if member.member_id in self._attached:
                        try:
                            await self._dispatcher.del_member(
                                FED_SESSION_SID, member.pool_name,
                                member.member_id, cancel_tasks=False,
                                force=True, fail_unsatisfiable=False)
                        except Exception as e:
                            log.info('[%s] detach %s: %s',
                                     self.instance_name, member.member_id, e)
                        self._attached.discard(member.member_id)
                        self._detail_cache.pop(member.pool_name, None)

                elif target == LIVENESS_SUSPECT:
                    continue

                elif member.member_id not in self._attached:
                    if member.end_time and member.end_time <= time.time():
                        # The same endpoint name back in a *new*
                        # allocation: re-declared with the old end_time,
                        # its adopted pilot would get a past deadline and
                        # never receive a task.  Only a new join knows the
                        # new end.
                        key = (member.member_id, float(member.end_time))
                        if key not in self._ended_logged:
                            self._ended_logged.add(key)
                            log.warning(
                                '[%s] not re-attaching member %s: its '
                                'allocation ended at %s; leave and join '
                                'again', self.instance_name,
                                member.member_id,
                                time.strftime('%Y-%m-%d %H:%M:%S',
                                              time.localtime(
                                                  member.end_time)))
                        result = LIVENESS_LOST
                        continue
                    try:
                        if member.pool_name not in self._pools:
                            await self._register_fed()
                        await self._dispatcher.add_member(
                            FED_SESSION_SID, member.pool_name,
                            self._member_decl(rec, member))
                        self._attached.add(member.member_id)
                        log.info('[%s] re-attached member %s',
                                 self.instance_name, member.member_id)
                    except Exception as e:
                        log.warning('[%s] re-attach %s failed: %s',
                                    self.instance_name, member.member_id, e)
                        result = LIVENESS_LOST

            for member in rec.member_list():
                if member.liveness != result:
                    member.liveness = result
                    dirty = True
            if rec.liveness != result:
                rec.liveness = result
                dirty = True
        if dirty:
            self._state.save()
