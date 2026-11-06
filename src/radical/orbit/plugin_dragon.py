"""Dragon plugin: Dragon distributed dictionaries (DDict) through ORBIT.

First slice of Orbit-Dragon (SwarmSlicer): ORBIT as the access layer for
Dragon services.  A client creates a DDict on an endpoint that runs next to
a Dragon runtime, reads and writes it through the broker, and hands the
DDict's serialized descriptor to processes inside the runtime, which attach
to the same data with ``DDict.attach(descriptor)``.

The plugin drives DDicts directly through ``dragonhpc`` (>= 0.15.0, for
attaching from plain python).  :class:`DDictBackend` is the seam where the
Dragon services API server replaces the direct calls later.

The plugin is endpoint-only, loads only when ``dragonhpc`` is importable,
and is not part of any default plugin set::

    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,dragon

An endpoint launched by the ``dragon`` launcher uses the runtime it runs
in.  Otherwise, runtime selection (``$RADICAL_ORBIT_DRAGON_RUNTIME``):

- ``attach``: join a running Dragon runtime (``$DRAGON_ATTACH_TOKEN``);
- ``start``:  start a single-node runtime on first use, stop it at
              plugin shutdown.

Default: ``attach`` when ``$DRAGON_ATTACH_TOKEN`` is set, else ``start``.
"""

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'


import asyncio
import atexit
import base64
import binascii
import functools
import importlib.util
import json
import logging
import os
import re
import shutil
import sys
import time
import uuid

import queue
import threading

from concurrent.futures import Executor, Future
from concurrent.futures import TimeoutError as FutureTimeout
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from .client import PluginClient
from .errors import http_exception
from .plugin_base import Plugin, resolve_frame_cap
from .plugin_session_base import PluginSession

log = logging.getLogger("radical.orbit")


ENV_RUNTIME      = 'RADICAL_ORBIT_DRAGON_RUNTIME'
RUNTIME_MODES    = ('attach', 'start')
RUNTIME_TIMEOUT  = 120                  # s, runtime start / attach
EXIT_TIMEOUT     = 20                   # s, release at process exit
DEFAULT_MEM      = 64 * 1024 * 1024     # bytes, DDict total_mem default
NAME_RE          = re.compile(r'[A-Za-z0-9_.-]{1,64}')
ID_PREFIX        = 'ddict.'           # DDict IDs; reserved for names
MEM_RE           = re.compile(r'(\d+)\s*([KMGT]?)B?', re.IGNORECASE)
MEM_UNITS        = {'': 1, 'K': 2**10, 'M': 2**20, 'G': 2**30, 'T': 2**40}

NATIVE           = 'native'            # _rt: running inside a runtime

# bytes values travel as ``{B64_KEY: "<base64>"}``; everything else as JSON
B64_KEY          = '$b64'


# ------------------------------------------------------------------------
#
def encode_value(value: Any) -> Any:
    """Wire form of a DDict value: bytes as a base64 marker, else as is."""
    if isinstance(value, (bytes, bytearray)):
        return {B64_KEY: base64.b64encode(value).decode()}
    return value


def decode_value(value: Any) -> Any:
    """Inverse of :func:`encode_value`.  A malformed marker raises
    ``ValueError``."""
    if isinstance(value, dict) and len(value) == 1 and B64_KEY in value:
        data = value[B64_KEY]
        if not isinstance(data, str):
            raise ValueError(f"{B64_KEY}: expected a base64 string")
        try:
            return base64.b64decode(data, validate=True)
        except binascii.Error as e:
            raise ValueError(f"{B64_KEY}: invalid base64: {e}") from e
    return value


def parse_mem(value: Any) -> int:
    """Parse a memory size: an int (bytes) or a string like ``'512M'``."""
    if isinstance(value, bool):
        raise ValueError(f"invalid memory size: {value!r}")
    if isinstance(value, int):
        mem = value
    else:
        m = MEM_RE.fullmatch(str(value).strip())
        if not m:
            raise ValueError(f"invalid memory size: {value!r}")
        mem = int(m.group(1)) * MEM_UNITS[m.group(2).upper()]
    if mem <= 0:
        raise ValueError(f"invalid memory size: {value!r}")
    return mem


def dragon_available() -> bool:
    """Whether ``dragonhpc`` is importable (without importing it).  A
    directory named ``dragon`` on ``sys.path`` (a namespace package, no
    origin) does not count."""
    try:
        spec = importlib.util.find_spec('dragon')
    except (ImportError, ValueError):
        return False
    return spec is not None and spec.origin is not None


def runtime_mode() -> str:
    """The runtime mode from ``$RADICAL_ORBIT_DRAGON_RUNTIME``, defaulting
    to ``attach`` when ``$DRAGON_ATTACH_TOKEN`` is set, else ``start``."""
    mode = os.environ.get(ENV_RUNTIME, '').strip().lower()
    if not mode:
        return 'attach' if os.environ.get('DRAGON_ATTACH_TOKEN') else 'start'
    if mode not in RUNTIME_MODES:
        raise ValueError(f"${ENV_RUNTIME}={mode!r}: expected one of "
                         f"{RUNTIME_MODES}")
    return mode


# ------------------------------------------------------------------------
#
class DragonThread(Executor):
    """
    One daemon worker thread for all Dragon calls.

    A ``ThreadPoolExecutor`` worker is joined at interpreter exit, so one
    hung Dragon call would keep the endpoint from exiting; a daemon thread
    is not.  Started on first use.
    """

    def __init__(self):
        self._q      = queue.SimpleQueue()
        self._down   = False
        self._lock   = threading.Lock()
        self._thread = None

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is None:
                return
            fut, fn = item
            if not fut.set_running_or_notify_cancel():
                continue                    # cancelled while queued
            try:
                fut.set_result(fn())
            except BaseException as e:      # pylint: disable=broad-except
                fut.set_exception(e)

    def submit(self, fn, /, *args, **kwargs) -> Future:
        with self._lock:
            if self._down:
                raise RuntimeError("cannot schedule new futures after "
                                   "shutdown")
            if self._thread is None:
                self._thread = threading.Thread(target=self._run,
                                                name='dragon', daemon=True)
                self._thread.start()
            fut = Future()
            self._q.put((fut, functools.partial(fn, *args, **kwargs)))
            return fut

    def shutdown(self, wait: bool = True, *,
                 cancel_futures: bool = False) -> None:
        with self._lock:
            if not self._down:
                self._down = True
                self._q.put(None)           # after what is queued
        if wait:
            self.join()

    @property
    def down(self) -> bool:
        """True once :meth:`shutdown` was called: no new work."""
        return self._down

    @property
    def started(self) -> bool:
        return self._thread is not None

    def join(self, timeout: Optional[float] = None) -> bool:
        """Wait for the worker to finish; False if it is still busy."""
        if self._thread is None:
            return True
        self._thread.join(timeout)
        return not self._thread.is_alive()


# ------------------------------------------------------------------------
#
class DDictBackend:
    """
    Owns the Dragon runtime connection and the DDicts of one plugin.

    Every Dragon call runs on one dedicated thread: concurrent DDict calls
    from several threads hang (dragonhpc 0.15.0), and serializing them
    keeps the endpoint's loops free.  The runtime is started or attached on
    first use, so loading the plugin costs nothing.

    Subclasses (tests, later the Dragon services API server) override
    :meth:`_runtime_up`, :meth:`_runtime_down` and :meth:`_new_ddict`.
    """

    def __init__(self, mode: str):
        self._mode    = mode
        self._pool    = DragonThread()
        self._rt      = None
        self._closed  = False
        self._ddicts: Dict[str, Any] = {}

    async def _call(self, fn, *args, **kwargs) -> Any:
        if self._pool.down:
            raise HTTPException(status_code=503,
                                detail="dragon backend is closed")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, functools.partial(fn, *args, **kwargs))

    # -- Dragon specifics (run on the Dragon thread) -------------------------

    def _runtime_up(self) -> None:
        if self._closed:
            raise HTTPException(status_code=503,
                                detail="dragon backend is closed")
        if self._rt is not None:
            return

        # launched by the `dragon` launcher (the endpoint wrapper does that
        # when dragonhpc is installed): use the runtime we run in
        import dragon
        if dragon.in_dragon():
            self._rt = NATIVE
            log.info("[dragon] running inside a Dragon runtime")
            return

        # the runtime is spawned through the `dragon` launcher, which sits
        # next to the interpreter when the venv is not activated
        if not shutil.which('dragon'):
            bindir = os.path.dirname(sys.executable)
            if os.path.exists(os.path.join(bindir, 'dragon')):
                path = os.environ.get('PATH')
                os.environ['PATH'] = bindir + (os.pathsep + path
                                               if path else '')

        from dragon.workflows import runtime as drt
        t0 = time.time()
        if self._mode == 'start':
            self._rt = drt.LocalRuntime.start(timeout=RUNTIME_TIMEOUT)
        else:
            self._rt = drt.attach(timeout=RUNTIME_TIMEOUT)
        log.info("[dragon] runtime %s in %.1fs", self._mode, time.time() - t0)

    def _runtime_down(self) -> None:
        if self._rt is None:
            return
        if   self._rt is NATIVE   : pass         # not ours to stop
        elif self._mode == 'start': self._rt.stop()
        else                    : self._rt.detach()
        self._rt = None

    def _new_ddict(self, managers_per_node: int, n_nodes: int,
                   total_mem: int) -> Any:
        from dragon.data.ddict import DDict
        # bounds Dragon's timed operations (default: wait forever), so a
        # stuck one fails instead of blocking the Dragon thread for good
        return DDict(managers_per_node, n_nodes, total_mem,
                     timeout=RUNTIME_TIMEOUT)

    # -- DDict operations ----------------------------------------------------

    def _ddict(self, did: str) -> Any:
        try:
            return self._ddicts[did]
        except KeyError:
            raise FileNotFoundError(f"unknown ddict: {did}") from None

    async def create(self, did: str, managers_per_node: int, n_nodes: int,
                     total_mem: int) -> str:
        """Create a DDict under *did*; returns its serialized descriptor."""
        def _create():
            self._runtime_up()
            dd = self._new_ddict(managers_per_node, n_nodes, total_mem)
            self._ddicts[did] = dd
            return dd.serialize()
        return await self._call(_create)

    async def destroy(self, did: str) -> None:
        def _destroy():
            self._ddict(did).destroy()
            del self._ddicts[did]
        await self._call(_destroy)

    async def put(self, did: str, items: Dict[str, Any]) -> int:
        def _put():
            dd = self._ddict(did)
            for n, (k, v) in enumerate(items.items()):
                try:
                    dd[k] = v
                except Exception as e:
                    raise RuntimeError(f"put stopped at key {k!r} after "
                                       f"{n} of {len(items)} items: "
                                       f"{e}") from e
            return len(items)
        return await self._call(_put)

    async def get(self, did: str, keys: List[str]) -> tuple:
        """Returns ``({key: value}, [missing keys])``."""
        def _get():
            dd = self._ddict(did)
            found, missing = {}, []
            for k in keys:
                try:
                    found[k] = dd[k]
                except KeyError:
                    missing.append(k)
            return found, missing
        return await self._call(_get)

    async def contains(self, did: str, keys: List[str]) -> List[str]:
        """Returns the keys of *keys* that are missing (values not read)."""
        def _contains():
            dd = self._ddict(did)
            return [k for k in keys if k not in dd]
        return await self._call(_contains)

    def discard(self, did: str) -> None:
        """Queue the destroy of *did*, if it exists by then: for a create
        whose caller went away while the create still runs (the single
        Dragon thread runs this after it)."""
        def _discard():
            dd = self._ddicts.pop(did, None)
            if dd is not None:
                try:
                    dd.destroy()
                except Exception as e:
                    log.warning("[dragon] discard %s failed: %s", did, e)
        try:
            self._pool.submit(_discard)
        except RuntimeError:
            pass                        # closed: close() destroyed it

    async def pop(self, did: str, keys: List[str]) -> List[str]:
        """Remove *keys*; returns the ones that were missing."""
        def _pop():
            dd = self._ddict(did)
            missing = []
            for k in keys:
                try:
                    del dd[k]
                except KeyError:
                    missing.append(k)
            return missing
        return await self._call(_pop)

    async def keys(self, did: str) -> List[Any]:
        return await self._call(lambda: list(self._ddict(did).keys()))

    async def length(self, did: str) -> int:
        return await self._call(lambda: len(self._ddict(did)))

    async def clear(self, did: str) -> None:
        await self._call(lambda: self._ddict(did).clear())

    def _close(self) -> None:
        for did, dd in list(self._ddicts.items()):
            try:
                dd.destroy()
            except Exception as e:
                log.warning("[dragon] destroy %s failed: %s", did, e)
        self._ddicts.clear()
        self._runtime_down()
        self._closed = True

    async def close(self) -> None:
        """Destroy every DDict, then stop or detach the runtime.  When the
        await is cancelled before the close ran, :meth:`close_sync` still
        does it at exit."""
        if self._closed or self._pool.down:
            return                      # a cancelled close: close_sync
        # the close is the last job: no create can queue behind it
        fut = self._pool.submit(self._close)
        self._pool.shutdown(wait=False)
        await asyncio.wrap_future(fut)

    def close_sync(self) -> None:
        """:meth:`close` for process exit (no event loop), for hosts that
        exit without running plugin ``shutdown()``: a started runtime must
        not outlive the process.  A no-op after :meth:`close`."""
        if self._closed or not self._pool.started:
            return                      # closed, or Dragon never used
        try:
            if not self._pool.down:
                fut = self._pool.submit(self._close)
                self._pool.shutdown(wait=False)
                fut.result(timeout=EXIT_TIMEOUT)
                return
            # a close was cancelled: let the worker finish what it runs,
            # then close from this thread (serialized calls work from any
            # thread)
            if not self._pool.join(EXIT_TIMEOUT):
                raise FutureTimeout()
            if not self._closed:
                self._close()
        except FutureTimeout:
            log.warning("[dragon] Dragon thread busy after %ds at exit: "
                        "runtime and DDicts may be left behind", EXIT_TIMEOUT)
        except Exception as e:
            log.warning("[dragon] close at exit failed: %s", e)
        finally:
            self._pool.shutdown(wait=False)


def _not_found_404(method):
    """A DDict destroyed while the call waited for the Dragon thread is a
    404, not a 500 (``Plugin._forward`` maps unknown exceptions to 500)."""
    @functools.wraps(method)
    async def _wrapped(*args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except FileNotFoundError as e:
            raise http_exception(e) from e
    return _wrapped


# ------------------------------------------------------------------------
#
class DragonSession(PluginSession):
    """
    Dragon session (service side).

    DDicts belong to the plugin, not to a session: like Dragon services they
    outlive their creator and are shared by ID.  The session is the access
    path only.
    """

    @property
    def _p(self) -> "PluginDragon":
        return self._plugin

    @_not_found_404
    async def create(self, name: Optional[str], managers_per_node: int,
                     n_nodes: int, total_mem: int,
                     creator: Optional[str]) -> dict:
        self._check_active()
        return await self._p.ddict_create(name, managers_per_node, n_nodes,
                                          total_mem, creator)

    async def list(self) -> dict:
        self._check_active()
        ddicts = [dict(e) for e in self._p.entries.values()]
        self._p.check_reply(len(json.dumps(ddicts)), 'too many ddicts')
        return {"ddicts": ddicts}

    @_not_found_404
    async def info(self, did: str) -> dict:
        self._check_active()
        entry = self._p.entry(did)
        return dict(entry, n_keys=await self._p.backend.length(did))

    @_not_found_404
    async def destroy(self, did: str) -> dict:
        self._check_active()
        return await self._p.ddict_destroy(did)

    @_not_found_404
    async def put(self, did: str, items: Dict[str, Any]) -> dict:
        self._check_active()
        self._p.entry(did)
        try:
            items = {k: decode_value(v) for k, v in items.items()}
        except ValueError as e:
            raise http_exception(e) from e
        return {"n": await self._p.backend.put(did, items)}

    @_not_found_404
    async def get(self, did: str, keys: List[str],
                  values: bool = True) -> dict:
        self._check_active()
        self._p.entry(did)
        hint = 'get fewer keys per call'
        if not values:
            missing = await self._p.backend.contains(did, keys)
            self._p.check_reply(len(json.dumps(missing)), hint)
            return {"items": {}, "missing": missing, "errors": {}}
        found, missing = await self._p.backend.get(did, keys)
        items, errors = {}, {}
        # sizes as escaped JSON (ensure_ascii): an upper bound of the reply
        size = len(json.dumps(missing))
        for k, v in found.items():
            v = encode_value(v)
            try:
                # strict JSON: the reply renderer rejects NaN / inf
                size += len(json.dumps(v, allow_nan=False))
                items[k] = v
            except (TypeError, ValueError):
                errors[k] = f"value of type {type(v).__name__} is not " \
                            f"JSON-serializable"
                size += len(json.dumps(errors[k]))
            size += len(json.dumps(k)) + 4
            self._p.check_reply(size, hint)
        self._p.check_reply(size, hint)     # also when nothing was found
        return {"items": items, "missing": missing, "errors": errors}

    @_not_found_404
    async def keys(self, did: str) -> dict:
        self._check_active()
        self._p.entry(did)
        keys = await self._p.backend.keys(did)
        strs = [k for k in keys if isinstance(k, str)]
        self._p.check_reply(len(json.dumps(strs)),
                            'too many keys for one reply')
        # keys written in-runtime may be of any type; the handle uses str
        return {"keys": strs, "n_other": len(keys) - len(strs)}

    @_not_found_404
    async def pop(self, did: str, keys: List[str]) -> dict:
        self._check_active()
        self._p.entry(did)
        missing = await self._p.backend.pop(did, keys)
        return {"n": len(keys) - len(missing), "missing": missing}

    @_not_found_404
    async def clear(self, did: str) -> dict:
        self._check_active()
        self._p.entry(did)
        await self._p.backend.clear(did)
        return {}


# ------------------------------------------------------------------------
#
class DDictHandle:
    """
    Client-side handle on one DDict, with native ``DDict`` semantics::

        dd = dragon.ddict.create(name='scratch')
        dd['k'] = {'v': 1}
        dd['k']             # -> {'v': 1}
        'k' in dd, len(dd), dd.keys()
        del dd['k']
        dd.descriptor       # for DDict.attach() inside the Dragon runtime
        dd.destroy()

    Values are JSON-serializable objects or ``bytes``.
    """

    def __init__(self, client: "DragonClient", entry: dict):
        self._client = client
        self._entry  = entry

    def __repr__(self) -> str:
        return f"DDictHandle({self.id!r}, name={self.name!r})"

    @property
    def id(self) -> str:
        return self._entry['id']

    @property
    def name(self) -> str:
        return self._entry['name']

    @property
    def descriptor(self) -> str:
        """Serialized descriptor for ``DDict.attach()`` in the runtime."""
        return self._entry['descriptor']

    def info(self) -> dict:
        return self._client._call('GET', f'ddict/info/{{sid}}/{self.id}')

    def __setitem__(self, key: str, value: Any) -> None:
        self.update({key: value})

    def update(self, items: Dict[str, Any]) -> None:
        bad = [k for k in items if not isinstance(k, str)]
        if bad:
            # JSON would turn them into str keys the reads cannot match
            raise TypeError(f"ddict keys must be str, got {bad[0]!r}")
        self._client._call('POST', 'ddict/put/{sid}', id=self.id,
                           items={k: encode_value(v)
                                  for k, v in items.items()})

    def __getitem__(self, key: str) -> Any:
        r = self.get_many([key])
        if key in r['missing']:
            raise KeyError(key)
        if key in r['errors']:
            raise TypeError(f"{key}: {r['errors'][key]}")
        return r['items'][key]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def get_many(self, keys: List[str]) -> dict:
        """``{items, missing, errors}`` for *keys*, values decoded."""
        r = self._client._call('POST', 'ddict/get/{sid}', id=self.id,
                               keys=list(keys))
        items = {}
        for k, v in r['items'].items():
            try:
                items[k] = decode_value(v)
            except ValueError as e:     # malformed marker written in-runtime
                r['errors'][k] = str(e)
        r['items'] = items
        return r

    def __contains__(self, key: str) -> bool:
        r = self._client._call('POST', 'ddict/get/{sid}', id=self.id,
                               keys=[key], values=False)
        return key not in r['missing']

    def __delitem__(self, key: str) -> None:
        r = self._client._call('POST', 'ddict/pop/{sid}', id=self.id,
                               keys=[key])
        if r['missing']:
            raise KeyError(key)

    def keys(self) -> List[str]:
        """The ``str`` keys; ``len()`` also counts keys of other types
        written inside the runtime."""
        return self._client._call('POST', 'ddict/keys/{sid}',
                                  id=self.id)['keys']

    def __len__(self) -> int:
        return self.info()['n_keys']

    def clear(self) -> None:
        self._client._call('POST', 'ddict/clear/{sid}', id=self.id)

    def destroy(self) -> None:
        self._client._call('POST', 'ddict/destroy/{sid}', id=self.id)


class DDictAPI:
    """``DragonClient.ddict``: create, attach to and list DDicts."""

    def __init__(self, client: "DragonClient"):
        self._client = client

    def create(self, name: Optional[str] = None, managers_per_node: int = 1,
               n_nodes: int = 1, total_mem: Any = DEFAULT_MEM) -> DDictHandle:
        """Create a DDict; *total_mem* in bytes or as ``'512M'``."""
        entry = self._client._call('POST', 'ddict/create/{sid}', name=name,
                                   managers_per_node=managers_per_node,
                                   n_nodes=n_nodes, total_mem=total_mem)
        return DDictHandle(self._client, entry)

    def attach(self, ddict_id: str) -> DDictHandle:
        """Handle on an existing, ready DDict, by ID (or by name)."""
        for entry in self.list():
            if ddict_id in (entry['id'], entry['name']):
                if entry['state'] != 'ready':
                    raise KeyError(f"ddict {ddict_id} is {entry['state']}")
                return DDictHandle(self._client, entry)
        raise KeyError(f"unknown ddict: {ddict_id}")

    def list(self) -> List[dict]:
        return self._client._call('GET', 'ddict/list/{sid}')['ddicts']


class DragonClient(PluginClient):
    """
    Client-side interface for the Dragon plugin.

    Obtained via ``runtime.get_plugin(endpoint_name, 'dragon')``, which also
    registers a session.  DDicts live at ``client.ddict``.
    """

    @property
    def ddict(self) -> DDictAPI:
        return DDictAPI(self)

    def _call(self, method: str, path: str, **body) -> dict:
        """One request; ``{sid}`` in *path* is the session ID."""
        self._require_session()
        url  = self._url(path.format(sid=self.sid))
        resp = self._request(method, url, json=body) if method == 'POST' \
               else self._request(method, url)
        self._raise(resp, path.split('/{sid}')[0])
        return resp.json()


# ------------------------------------------------------------------------
#
class PluginDragon(Plugin):
    """
    Dragon plugin for ORBIT: Dragon DDicts created, read and written through
    the broker.  See the module docstring.
    """

    plugin_name   = 'dragon'
    session_class = DragonSession
    client_class  = DragonClient
    version       = '0.1.0'

    ui_config = {
        "icon"       : "🐉",
        "title"      : "Dragon",
        "description": "Dragon distributed dictionaries (DDict)."
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """Endpoint-only, and only when dragonhpc is importable."""
        return dragon_available() and not getattr(app.state, 'is_broker',
                                                  False)

    def __init__(self, app: FastAPI, instance_name: str = 'dragon',
                 backend: Optional[DDictBackend] = None):
        super().__init__(app, instance_name)

        self.backend     = backend or DDictBackend(runtime_mode())
        self.entries: Dict[str, dict] = {}
        self.reply_limit = resolve_frame_cap(app) // 2

        # fallback for a host exiting without stop() / shutdown()
        atexit.register(self.backend.close_sync)

        self.add_route_post('ddict/create/{sid}',     self.r_create)
        self.add_route_get ('ddict/list/{sid}',       self.r_list)
        self.add_route_get ('ddict/info/{sid}/{did}', self.r_info)
        self.add_route_post('ddict/destroy/{sid}',    self.r_destroy)
        self.add_route_post('ddict/put/{sid}',        self.r_put)
        self.add_route_post('ddict/get/{sid}',        self.r_get)
        self.add_route_post('ddict/keys/{sid}',       self.r_keys)
        self.add_route_post('ddict/pop/{sid}',        self.r_pop)
        self.add_route_post('ddict/clear/{sid}',      self.r_clear)

    def check_reply(self, size: int, hint: str) -> None:
        """413 when a reply of *size* bytes would exceed the reply limit."""
        if size > self.reply_limit:
            raise HTTPException(status_code=413,
                                detail=f"reply exceeds {self.reply_limit} "
                                       f"bytes: {hint}")

    # -- registry --------------------------------------------------------------

    def entry(self, did: str, ready: bool = True) -> dict:
        """The registry entry of *did* (404 when unknown); with *ready*,
        409 while it is still being created or destroyed."""
        try:
            entry = self.entries[did]
        except KeyError:
            raise http_exception(FileNotFoundError(
                f"unknown ddict: {did}")) from None
        if ready and entry['state'] != 'ready':
            raise http_exception(FileExistsError(
                f"ddict {did} is {entry['state']}"))
        return entry

    async def ddict_create(self, name: Optional[str], managers_per_node: int,
                           n_nodes: int, total_mem: int,
                           creator: Optional[str]) -> dict:
        did  = f'{ID_PREFIX}{uuid.uuid4().hex[:8]}'
        name = name or did
        if any(e['name'] == name for e in self.entries.values()):
            raise http_exception(FileExistsError(f"ddict {name!r} exists"))

        # reserve the name before the (slow) create
        entry = {"id"               : did,
                 "name"             : name,
                 "managers_per_node": managers_per_node,
                 "n_nodes"          : n_nodes,
                 "total_mem"        : total_mem,
                 "created"          : time.time(),
                 "creator"          : creator,
                 "state"            : "creating",
                 "descriptor"       : None}
        self.entries[did] = entry
        try:
            entry['descriptor'] = await self.backend.create(
                did, managers_per_node, n_nodes, total_mem)
        except BaseException:
            self.entries.pop(did, None)
            # a cancelled await may leave the create running on the Dragon
            # thread: destroy what it makes
            self.backend.discard(did)
            raise
        if self.entries.get(did) is not entry:
            # released (endpoint stopping) while the create ran: close()
            # destroys what it made
            raise HTTPException(status_code=503,
                                detail="endpoint is stopping")
        entry['state'] = 'ready'
        log.info("[dragon] created %s (%s)", did, name)
        self._dispatch_notify('ddict_status', {"id": did, "name": name,
                                               "state": "ready"})
        return dict(entry)

    async def ddict_destroy(self, did: str) -> dict:
        entry = self.entry(did, ready=False)
        if entry['state'] not in ('ready', 'failed'):
            raise http_exception(FileExistsError(
                f"ddict {did} is {entry['state']}"))
        entry['state'] = 'destroying'
        try:
            await self.backend.destroy(did)
        except asyncio.CancelledError:
            # the destroy may still run on the Dragon thread, or was
            # dropped from its queue: make sure it happens
            self.entries.pop(did, None)
            self.backend.discard(did)
            self._dispatch_notify('ddict_status', {"id": did,
                                                   "name": entry['name'],
                                                   "state": "destroyed"})
            raise
        except BaseException:
            # may be partly torn down: not usable, destroy can be retried
            entry['state'] = 'failed'
            raise
        self.entries.pop(did, None)
        log.info("[dragon] destroyed %s (%s)", did, entry['name'])
        self._dispatch_notify('ddict_status', {"id": did,
                                               "name": entry['name'],
                                               "state": "destroyed"})
        return {}

    # -- route handlers --------------------------------------------------------

    @staticmethod
    async def _body(request: Request, *required: str) -> dict:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            raise http_exception(ValueError("body must be a JSON object"))
        for field in required:
            if field not in body:
                raise http_exception(ValueError(f"missing field {field!r}"))
        if 'id' in body and not isinstance(body['id'], str):
            raise http_exception(ValueError("'id' must be a string"))
        return body

    @staticmethod
    def _pos_int(body: dict, field: str) -> int:
        val = body.get(field, 1)
        if isinstance(val, bool) or not isinstance(val, int) or val < 1:
            raise http_exception(ValueError(f"{field!r} must be an integer "
                                            f">= 1"))
        return val

    @staticmethod
    def _str_list(body: dict, field: str) -> List[str]:
        val = body[field]
        if not isinstance(val, list) or \
                not all(isinstance(k, str) for k in val):
            raise http_exception(ValueError(f"{field!r} must be a list of "
                                            f"strings"))
        return val

    async def r_create(self, request: Request) -> dict:
        """``POST ddict/create/{sid}``:
        ``{name?, managers_per_node?, n_nodes?, total_mem?}`` -> entry."""
        sid  = request.path_params['sid']
        body = await self._body(request)
        name = body.get('name')
        if name is not None and \
                not (isinstance(name, str) and NAME_RE.fullmatch(name)):
            raise http_exception(ValueError(f"invalid ddict name {name!r}"))
        if name is not None and name.startswith(ID_PREFIX):
            # attach() resolves IDs and names alike
            raise http_exception(ValueError(f"ddict names must not start "
                                            f"with {ID_PREFIX!r}"))
        mpn = self._pos_int(body, 'managers_per_node')
        nn  = self._pos_int(body, 'n_nodes')
        try:
            mem = parse_mem(body.get('total_mem', DEFAULT_MEM))
        except ValueError as e:
            raise http_exception(e) from e
        return await self._forward(sid, DragonSession.create, name=name,
                                   managers_per_node=mpn, n_nodes=nn,
                                   total_mem=mem,
                                   creator=self._request_owner(request))

    async def r_list(self, request: Request) -> dict:
        """``GET ddict/list/{sid}`` -> ``{ddicts: [entry]}``."""
        return await self._forward(request.path_params['sid'],
                                   DragonSession.list)

    async def r_info(self, request: Request) -> dict:
        """``GET ddict/info/{sid}/{did}`` -> entry + ``n_keys``."""
        return await self._forward(request.path_params['sid'],
                                   DragonSession.info,
                                   did=request.path_params['did'])

    async def r_destroy(self, request: Request) -> dict:
        """``POST ddict/destroy/{sid}``: ``{id}``."""
        body = await self._body(request, 'id')
        return await self._forward(request.path_params['sid'],
                                   DragonSession.destroy, did=body['id'])

    async def r_put(self, request: Request) -> dict:
        """``POST ddict/put/{sid}``: ``{id, items}`` -> ``{n}``."""
        body  = await self._body(request, 'id', 'items')
        items = body['items']
        if not isinstance(items, dict):
            raise http_exception(ValueError("'items' must be an object"))
        return await self._forward(request.path_params['sid'],
                                   DragonSession.put, did=body['id'],
                                   items=items)

    async def r_get(self, request: Request) -> dict:
        """``POST ddict/get/{sid}``: ``{id, keys, values?}`` ->
        ``{items, missing, errors}``; ``values=false`` checks presence
        only."""
        body   = await self._body(request, 'id', 'keys')
        values = body.get('values', True)
        if not isinstance(values, bool):
            raise http_exception(ValueError("'values' must be a boolean"))
        return await self._forward(request.path_params['sid'],
                                   DragonSession.get, did=body['id'],
                                   keys=self._str_list(body, 'keys'),
                                   values=values)

    async def r_keys(self, request: Request) -> dict:
        """``POST ddict/keys/{sid}``: ``{id}`` -> ``{keys, n_other}``."""
        body = await self._body(request, 'id')
        return await self._forward(request.path_params['sid'],
                                   DragonSession.keys, did=body['id'])

    async def r_pop(self, request: Request) -> dict:
        """``POST ddict/pop/{sid}``: ``{id, keys}`` -> ``{n, missing}``."""
        body = await self._body(request, 'id', 'keys')
        return await self._forward(request.path_params['sid'],
                                   DragonSession.pop, did=body['id'],
                                   keys=self._str_list(body, 'keys'))

    async def r_clear(self, request: Request) -> dict:
        """``POST ddict/clear/{sid}``: ``{id}``."""
        body = await self._body(request, 'id')
        return await self._forward(request.path_params['sid'],
                                   DragonSession.clear, did=body['id'])

    async def _release(self) -> None:
        self.entries.clear()
        await self.backend.close()
        atexit.unregister(self.backend.close_sync)

    async def on_host_stop(self) -> None:
        """The hosting runtime stops: destroy the DDicts, release the
        Dragon runtime."""
        await self._release()

    async def shutdown(self) -> None:
        """Plugin teardown: sessions, then DDicts and runtime."""
        try:
            await super().shutdown()
        finally:
            await self._release()
