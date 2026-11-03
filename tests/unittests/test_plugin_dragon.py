# pylint: disable=protected-access,redefined-outer-name

import asyncio

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.orbit.plugin_dragon import (PluginDragon, DDictBackend,
                                         DDictHandle, DragonClient,
                                         encode_value, decode_value,
                                         parse_mem, runtime_mode)


# ── fake Dragon ─────────────────────────────────────────────────────────────

class _FakeDDict(dict):
    """dict with the DDict calls the backend uses."""

    def __init__(self, *args):
        super().__init__()
        self.args      = args
        self.destroyed = False

    def serialize(self):
        return f'desc:{id(self)}'

    def destroy(self):
        self.destroyed = True


class _FakeBackend(DDictBackend):

    def __init__(self):
        super().__init__('start')
        self.up    = 0
        self.down  = 0
        self.fail  = None

    def _runtime_up(self):
        if self._rt is None:
            self._rt  = object()
            self.up  += 1

    def _runtime_down(self):
        if self._rt is not None:
            self._rt   = None
            self.down += 1

    def _new_ddict(self, managers_per_node, n_nodes, total_mem):
        if self.fail:
            raise self.fail
        return _FakeDDict(managers_per_node, n_nodes, total_mem)


def _mk():
    app     = FastAPI()
    backend = _FakeBackend()
    plugin  = PluginDragon(app, backend=backend)
    tc      = TestClient(app)
    sid     = tc.post('/dragon/register_session').json()['sid']
    return plugin, backend, tc, sid


@pytest.fixture
def env():
    return _mk()


def _create(tc, sid, **body):
    r = tc.post(f'/dragon/ddict/create/{sid}', json=body)
    assert r.status_code == 200, r.text
    return r.json()


# ── helpers ─────────────────────────────────────────────────────────────────

def test_value_codec_roundtrip():
    for v in (1, 'a', [1, 2], {'x': None}, b'\x00\xff'):
        assert decode_value(encode_value(v)) == v
    assert encode_value(b'ab') == {'$b64': 'YWI='}


@pytest.mark.parametrize('val, exp', [(1024, 1024), ('2K', 2048),
                                      ('64M', 64 * 2**20), ('1GB', 2**30)])
def test_parse_mem(val, exp):
    assert parse_mem(val) == exp


@pytest.mark.parametrize('val', ['x', '', 0, -1, True, '1Q'])
def test_parse_mem_invalid(val):
    with pytest.raises(ValueError):
        parse_mem(val)


def test_runtime_mode(monkeypatch):
    monkeypatch.delenv('RADICAL_ORBIT_DRAGON_RUNTIME', raising=False)
    monkeypatch.delenv('DRAGON_ATTACH_TOKEN', raising=False)
    assert runtime_mode() == 'start'
    monkeypatch.setenv('DRAGON_ATTACH_TOKEN', 't')
    assert runtime_mode() == 'attach'
    monkeypatch.setenv('RADICAL_ORBIT_DRAGON_RUNTIME', 'start')
    assert runtime_mode() == 'start'
    monkeypatch.setenv('RADICAL_ORBIT_DRAGON_RUNTIME', 'bogus')
    with pytest.raises(ValueError):
        runtime_mode()


def test_is_enabled(monkeypatch):
    import radical.orbit.plugin_dragon as pd
    app = FastAPI()
    monkeypatch.setattr(pd, 'dragon_available', lambda: True)
    assert PluginDragon.is_enabled(app)
    app.state.is_broker = True
    assert not PluginDragon.is_enabled(app)
    app.state.is_broker = False
    monkeypatch.setattr(pd, 'dragon_available', lambda: False)
    assert not PluginDragon.is_enabled(app)


# ── lifecycle ───────────────────────────────────────────────────────────────

def test_create_list_info_destroy(env):
    plugin, backend, tc, sid = env

    e = _create(tc, sid, name='scratch', total_mem='1M')
    assert e['name']       == 'scratch'
    assert e['state']      == 'ready'
    assert e['total_mem']  == 2**20
    assert e['descriptor'].startswith('desc:')
    assert backend.up      == 1

    did = e['id']
    assert backend._ddicts[did].args == (1, 1, 2**20)

    ls = tc.get(f'/dragon/ddict/list/{sid}').json()['ddicts']
    assert [x['id'] for x in ls] == [did]

    info = tc.get(f'/dragon/ddict/info/{sid}/{did}').json()
    assert info['n_keys'] == 0

    dd = backend._ddicts[did]
    r  = tc.post(f'/dragon/ddict/destroy/{sid}', json={'id': did})
    assert r.status_code == 200
    assert dd.destroyed
    assert plugin.entries  == {}
    assert backend._ddicts == {}


def test_create_defaults_name_to_id(env):
    _, _, tc, sid = env
    e = _create(tc, sid)
    assert e['name'] == e['id']


def test_create_duplicate_name_409(env):
    _, _, tc, sid = env
    _create(tc, sid, name='a')
    r = tc.post(f'/dragon/ddict/create/{sid}', json={'name': 'a'})
    assert r.status_code == 409


@pytest.mark.parametrize('body', [{'name': 'a b'}, {'name': 5},
                                  {'total_mem': 'lots'},
                                  {'n_nodes': 0}, {'n_nodes': 1.5},
                                  {'n_nodes': True},
                                  {'managers_per_node': 'x'}])
def test_create_invalid_400(env, body):
    _, _, tc, sid = env
    r = tc.post(f'/dragon/ddict/create/{sid}', json=body)
    assert r.status_code == 400, r.text


def test_create_failure_releases_name(env):
    plugin, backend, tc, sid = env
    backend.fail = RuntimeError('no runtime')
    r = tc.post(f'/dragon/ddict/create/{sid}', json={'name': 'a'})
    assert r.status_code == 500
    assert plugin.entries == {}
    backend.fail = None
    _create(tc, sid, name='a')


def test_unknown_ddict_404(env):
    _, _, tc, sid = env
    for path, body in (('put',     {'id': 'x', 'items': {}}),
                       ('get',     {'id': 'x', 'keys': []}),
                       ('keys',    {'id': 'x'}),
                       ('pop',     {'id': 'x', 'keys': []}),
                       ('clear',   {'id': 'x'}),
                       ('destroy', {'id': 'x'})):
        r = tc.post(f'/dragon/ddict/{path}/{sid}', json=body)
        assert r.status_code == 404, path
    assert tc.get(f'/dragon/ddict/info/{sid}/x').status_code == 404


def test_not_ready_409(env):
    plugin, _, tc, sid = env
    e = _create(tc, sid)
    plugin.entries[e['id']]['state'] = 'creating'
    r = tc.post(f'/dragon/ddict/put/{sid}', json={'id': e['id'],
                                                 'items': {}})
    assert r.status_code == 409


def test_unknown_session_404(env):
    _, _, tc, _ = env
    assert tc.get('/dragon/ddict/list/nope').status_code == 404


def test_ddicts_outlive_sessions(env):
    plugin, _, tc, sid = env
    e = _create(tc, sid)
    tc.post(f'/dragon/unregister_session/{sid}')
    sid2 = tc.post('/dragon/register_session').json()['sid']
    ls = tc.get(f'/dragon/ddict/list/{sid2}').json()['ddicts']
    assert [x['id'] for x in ls] == [e['id']]
    assert e['id'] in plugin.entries


# ── data ────────────────────────────────────────────────────────────────────

def test_put_get_keys_pop_clear(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    url = f'/dragon/ddict/{{}}/{sid}'

    r = tc.post(url.format('put'), json={'id': did, 'items': {
        'a': 1, 'b': {'x': [1, 2]}, 'c': encode_value(b'\x01\x02')}})
    assert r.json() == {'n': 3}
    assert backend._ddicts[did]['c'] == b'\x01\x02'

    r = tc.post(url.format('get'), json={'id': did,
                                          'keys': ['a', 'c', 'zz']}).json()
    assert r['items']   == {'a': 1, 'c': {'$b64': 'AQI='}}
    assert r['missing'] == ['zz']
    assert r['errors']  == {}

    r = tc.post(url.format('keys'), json={'id': did}).json()
    assert sorted(r['keys']) == ['a', 'b', 'c']

    r = tc.post(url.format('pop'), json={'id': did,
                                          'keys': ['a', 'zz']}).json()
    assert r == {'n': 1, 'missing': ['zz']}

    tc.post(url.format('clear'), json={'id': did})
    assert backend._ddicts[did] == {}


def test_get_non_json_value_reports_error(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did]['s'] = {1, 2}          # written in-runtime
    r = tc.post(f'/dragon/ddict/get/{sid}',
                json={'id': did, 'keys': ['s']}).json()
    assert r['items'] == {}
    assert 'set' in r['errors']['s']


def test_get_reply_limit_413(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    plugin.reply_limit = 100
    backend._ddicts[did]['big'] = 'x' * 200
    r = tc.post(f'/dragon/ddict/get/{sid}', json={'id': did,
                                                 'keys': ['big']})
    assert r.status_code == 413


@pytest.mark.parametrize('path, body', [
    ('put',  {'id': 'x'}),
    ('put',  {'id': 'x', 'items': [1]}),
    ('get',  {'id': 'x', 'keys': 'a'}),
    ('get',  {'id': 'x', 'keys': [1]}),
    ('keys', {}),
    ('get',  {'id': 'x', 'keys': [], 'values': 'no'}),
])
def test_bad_bodies_400(env, path, body):
    _, _, tc, sid = env
    r = tc.post(f'/dragon/ddict/{path}/{sid}', json=body)
    assert r.status_code == 400, r.text


def test_non_object_body_400(env):
    _, _, tc, sid = env
    r = tc.post(f'/dragon/ddict/keys/{sid}', content=b'[1]',
                headers={'content-type': 'application/json'})
    assert r.status_code == 400


# ── notifications, shutdown ─────────────────────────────────────────────────

def test_status_notifications(env, monkeypatch):
    plugin, _, tc, sid = env
    sent = []
    monkeypatch.setattr(plugin, '_dispatch_notify',
                        lambda topic, data, session=None:
                        sent.append((topic, data['state'])))
    did = _create(tc, sid)['id']
    tc.post(f'/dragon/ddict/destroy/{sid}', json={'id': did})
    assert sent == [('ddict_status', 'ready'), ('ddict_status', 'destroyed')]


def test_shutdown_destroys_and_stops(env):
    plugin, backend, tc, sid = env
    dds = [backend._ddicts[_create(tc, sid)['id']] for _ in range(2)]
    asyncio.run(plugin.shutdown())
    assert all(dd.destroyed for dd in dds)
    assert backend.down   == 1
    assert plugin.entries == {}


def test_backend_runs_on_one_thread():
    import threading
    backend = _FakeBackend()
    seen    = set()

    async def _run():
        await asyncio.gather(*[backend._call(
            lambda: seen.add(threading.get_ident())) for _ in range(20)])
    asyncio.run(_run())
    assert len(seen) == 1


# ── client ──────────────────────────────────────────────────────────────────

class _Http:
    """httpx-like shim over the TestClient (the real one rides the WS)."""

    def __init__(self, tc):
        self._tc = tc

    def get(self, url, **kw):
        return self._tc.get(url, **kw)

    def post(self, url, **kw):
        return self._tc.post(url, **kw)


@pytest.fixture
def client(env):
    _, _, tc, _ = env
    c = DragonClient(_Http(tc), '/dragon')
    c.register_session()
    return c


def test_client_handle_dict_semantics(client):
    dd = client.ddict.create(name='scratch', total_mem='1M')
    assert isinstance(dd, DDictHandle)
    assert dd.name == 'scratch'
    assert dd.descriptor.startswith('desc:')

    dd['k'] = {'v': 1}
    dd['b'] = b'\x00\x01'
    assert dd['k'] == {'v': 1}
    assert dd['b'] == b'\x00\x01'
    assert 'k' in dd and 'zz' not in dd
    assert len(dd) == 2
    assert sorted(dd.keys()) == ['b', 'k']
    assert dd.get('zz', 7) == 7

    with pytest.raises(KeyError):
        dd['zz']
    del dd['k']
    with pytest.raises(KeyError):
        del dd['k']

    dd.update({'x': 1, 'y': 2})
    assert dd.get_many(['x', 'y'])['items'] == {'x': 1, 'y': 2}
    dd.clear()
    assert len(dd) == 0


def test_client_attach_list_destroy(client):
    dd = client.ddict.create(name='shared')
    dd['k'] = 1
    same = client.ddict.attach('shared')
    assert same.id == dd.id and same['k'] == 1
    assert client.ddict.attach(dd.id).name == 'shared'
    assert [e['name'] for e in client.ddict.list()] == ['shared']
    with pytest.raises(KeyError):
        client.ddict.attach('nope')
    dd.destroy()
    assert client.ddict.list() == []
    with pytest.raises(RuntimeError, match='404'):
        dd['k']


def test_close_sync_at_exit_and_idempotent(env):
    plugin, backend, tc, sid = env
    dd = backend._ddicts[_create(tc, sid)['id']]
    backend.close_sync()
    assert dd.destroyed
    assert backend.down == 1
    backend.close_sync()
    asyncio.run(plugin.shutdown())
    assert backend.down == 1


def test_dragon_available_ignores_namespace_dir(tmp_path, monkeypatch):
    import sys
    from radical.orbit.plugin_dragon import dragon_available
    (tmp_path / 'dragon').mkdir()                 # no __init__.py
    monkeypatch.setattr(sys, 'path', [str(tmp_path)])
    monkeypatch.delitem(sys.modules, 'dragon', raising=False)
    assert not dragon_available()
    (tmp_path / 'dragon' / '__init__.py').write_text('')
    assert dragon_available()


# ── review round 1 ──────────────────────────────────────────────────────────

@pytest.mark.parametrize('val', [{'$b64': 5}, {'$b64': 'not base64!'}])
def test_put_malformed_b64_400(env, val):
    _, _, tc, sid = env
    did = _create(tc, sid)['id']
    r = tc.post(f'/dragon/ddict/put/{sid}', json={'id': did,
                                                 'items': {'k': val}})
    assert r.status_code == 400, r.text


def test_decode_value_rejects_malformed():
    with pytest.raises(ValueError):
        decode_value({'$b64': '###'})


def test_get_presence_only(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did]['big'] = 'x' * 200
    plugin = env[0]
    plugin.reply_limit = 100            # a value fetch would be 413
    r = tc.post(f'/dragon/ddict/get/{sid}', json={
        'id': did, 'keys': ['big', 'zz'], 'values': False}).json()
    assert r == {'items': {}, 'missing': ['zz'], 'errors': {}}


def test_keys_reports_other_keys(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({'a': 1, 7: 2})      # 7: written in-runtime
    r = tc.post(f'/dragon/ddict/keys/{sid}', json={'id': did}).json()
    assert r == {'keys': ['a'], 'n_other': 1}


def test_create_cancelled_discards_ddict():
    plugin, backend, _, _ = _mk()

    async def _run():
        gate = asyncio.Event()
        orig = backend._new_ddict

        def _slow(*a):
            asyncio.run_coroutine_threadsafe(_set(gate), loop).result()
            import time
            time.sleep(0.2)
            return orig(*a)

        async def _set(ev):
            ev.set()

        loop = asyncio.get_running_loop()
        backend._new_ddict = _slow
        task = asyncio.ensure_future(
            plugin.ddict_create('a', 1, 1, 1024, None))
        await gate.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await backend._call(lambda: None)    # after create + discard
    asyncio.run(_run())
    assert plugin.entries  == {}
    assert backend._ddicts == {}


def test_destroy_cancelled_drops_entry(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']

    async def _run():
        task = asyncio.ensure_future(plugin.ddict_destroy(did))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await backend._call(lambda: None)
    asyncio.run(_run())
    assert did not in plugin.entries
    assert did not in backend._ddicts


def test_destroy_after_shutdown_cleared_entries(env):
    plugin, _, tc, sid = env
    did = _create(tc, sid)['id']

    async def _run():
        task = asyncio.ensure_future(plugin.ddict_destroy(did))
        await asyncio.sleep(0)
        plugin.entries.clear()
        return await task
    assert asyncio.run(_run()) == {}


def test_close_cancelled_then_close_sync_closes():
    backend = _FakeBackend()

    async def _run():
        await backend.create('d', 1, 1, 1024)
        block = asyncio.ensure_future(backend._call(__import__('time').sleep,
                                                    0.2))
        task  = asyncio.ensure_future(backend.close())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await block
    asyncio.run(_run())
    dd = backend._ddicts.get('d')
    backend.close_sync()
    assert backend.down == 1
    assert dd is None or dd.destroyed


def test_client_attach_not_ready(client, env):
    plugin = env[0]
    dd = client.ddict.create(name='busy')
    plugin.entries[dd.id]['state'] = 'creating'
    with pytest.raises(KeyError, match='creating'):
        client.ddict.attach('busy')


def test_client_contains_skips_value(client, env):
    plugin = env[0]
    dd = client.ddict.create()
    dd['big'] = 'x' * 300
    plugin.reply_limit = 100
    assert 'big' in dd
    assert 'nope' not in dd


# ── review round 2 ──────────────────────────────────────────────────────────

def test_on_host_stop_releases_and_unregisters(env, monkeypatch):
    import atexit
    plugin, backend, tc, sid = env
    gone = []
    monkeypatch.setattr(atexit, 'unregister', gone.append)
    dd = backend._ddicts[_create(tc, sid)['id']]
    asyncio.run(plugin.on_host_stop())
    assert dd.destroyed
    assert backend.down   == 1
    assert plugin.entries == {}
    assert gone == [backend.close_sync]
    assert plugin._sessions             # client sessions survive


def test_keys_reply_limit_413(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({f'key{i:04d}': i for i in range(50)})
    plugin.reply_limit = 100
    r = tc.post(f'/dragon/ddict/keys/{sid}', json={'id': did})
    assert r.status_code == 413


def test_get_nan_is_per_key_error(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({'a': 1, 'n': float('nan')})
    r = tc.post(f'/dragon/ddict/get/{sid}', json={'id': did,
                                                 'keys': ['a', 'n']})
    assert r.status_code == 200
    assert r.json()['items'] == {'a': 1}
    assert 'n' in r.json()['errors']


def test_create_reserved_name_prefix_400(env):
    _, _, tc, sid = env
    r = tc.post(f'/dragon/ddict/create/{sid}', json={'name': 'ddict.1234'})
    assert r.status_code == 400


def test_client_malformed_marker_is_per_key_error(client, env):
    _, backend, _, _ = env
    dd = client.ddict.create()
    dd.update({'ok': 1})
    backend._ddicts[dd.id]['bad'] = {'$b64': '###'}     # written in-runtime
    r = dd.get_many(['ok', 'bad'])
    assert r['items'] == {'ok': 1}
    assert 'bad' in r['errors']


# ── review round 3 ──────────────────────────────────────────────────────────

def test_destroyed_while_waiting_is_404(env):
    _, backend, tc, sid = env
    did = _create(tc, sid)['id']
    del backend._ddicts[did]            # destroyed behind the entry check
    r = tc.post(f'/dragon/ddict/put/{sid}', json={'id': did, 'items': {}})
    assert r.status_code == 404
    assert tc.get(f'/dragon/ddict/info/{sid}/{did}').status_code == 404


def test_keys_reply_limit_counts_utf8(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({'\U0001f600' * 10: 1})   # 10 chars, 40 B
    plugin.reply_limit = 30
    r = tc.post(f'/dragon/ddict/keys/{sid}', json={'id': did})
    assert r.status_code == 413


def test_close_after_cancelled_close_is_noop():
    backend = _FakeBackend()
    backend._pool.shutdown(wait=False)
    asyncio.run(backend.close())        # no "cannot schedule" error


def test_create_released_meanwhile_503():
    plugin, backend, _, _ = _mk()
    orig = backend._new_ddict

    def _new(*a):
        plugin.entries.clear()          # on_host_stop ran meanwhile
        return orig(*a)
    backend._new_ddict = _new

    async def _run():
        return await plugin.ddict_create('a', 1, 1, 1024, None)
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as e:
        asyncio.run(_run())
    assert e.value.status_code == 503


@pytest.mark.parametrize('did', [['x'], {'a': 1}, 5])
def test_non_string_id_400(env, did):
    _, _, tc, sid = env
    r = tc.post(f'/dragon/ddict/keys/{sid}', json={'id': did})
    assert r.status_code == 400


# ── review round 4 ──────────────────────────────────────────────────────────

def test_runtime_native_under_launcher(monkeypatch):
    import sys
    import types
    monkeypatch.setitem(sys.modules, 'dragon',
                        types.SimpleNamespace(in_dragon=lambda: True))
    backend = DDictBackend('start')
    backend._runtime_up()
    assert backend._rt == 'native'
    backend._runtime_down()             # not ours: nothing to stop
    assert backend._rt is None


def test_calls_after_close_503():
    from fastapi import HTTPException
    backend = _FakeBackend()
    asyncio.run(backend.close())
    with pytest.raises(HTTPException) as e:
        asyncio.run(backend.keys('x'))
    assert e.value.status_code == 503


def test_failed_destroy_marks_failed_and_retries(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    dd  = backend._ddicts[did]

    def _boom():
        raise RuntimeError('manager error')
    dd.destroy = _boom
    r = tc.post(f'/dragon/ddict/destroy/{sid}', json={'id': did})
    assert r.status_code == 500
    assert plugin.entries[did]['state'] == 'failed'
    r = tc.post(f'/dragon/ddict/put/{sid}', json={'id': did, 'items': {}})
    assert r.status_code == 409                     # not usable
    dd.destroy = lambda: None
    r = tc.post(f'/dragon/ddict/destroy/{sid}', json={'id': did})
    assert r.status_code == 200
    assert did not in plugin.entries


def test_get_errors_count_toward_reply_limit(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({f'k{i}': {i} for i in range(20)})   # sets
    plugin.reply_limit = 200
    r = tc.post(f'/dragon/ddict/get/{sid}',
                json={'id': did, 'keys': [f'k{i}' for i in range(20)]})
    assert r.status_code == 413


def test_keys_size_counts_json_escapes(env):
    plugin, backend, tc, sid = env
    did = _create(tc, sid)['id']
    backend._ddicts[did].update({'\x01' * 10: 1})     # 10 B, 60 B escaped
    plugin.reply_limit = 40
    r = tc.post(f'/dragon/ddict/keys/{sid}', json={'id': did})
    assert r.status_code == 413


# ── review round 5 ──────────────────────────────────────────────────────────

def test_close_sync_bounded_when_dragon_thread_hangs(monkeypatch):
    import threading
    import time
    import radical.orbit.plugin_dragon as pd
    monkeypatch.setattr(pd, 'EXIT_TIMEOUT', 0.3)
    backend = _FakeBackend()
    gate    = threading.Event()
    backend._pool.submit(gate.wait)     # a hung Dragon call
    t0 = time.time()
    backend.close_sync()
    assert time.time() - t0 < 2.0
    assert backend._pool._thread.daemon  # not joined at interpreter exit
    gate.set()


def test_list_reply_limit_413(env):
    plugin, _, tc, sid = env
    for i in range(5):
        _create(tc, sid, name=f'n{i}')
    plugin.reply_limit = 100
    assert tc.get(f'/dragon/ddict/list/{sid}').status_code == 413


def test_get_all_missing_reply_limit_413(env):
    plugin, _, tc, sid = env
    did = _create(tc, sid)['id']
    plugin.reply_limit = 100
    r = tc.post(f'/dragon/ddict/get/{sid}',
                json={'id': did, 'keys': [f'missing-key-{i}'
                                          for i in range(20)]})
    assert r.status_code == 413


def test_cancelled_destroy_notifies(env, monkeypatch):
    plugin, backend, tc, sid = env
    did  = _create(tc, sid)['id']
    sent = []
    monkeypatch.setattr(plugin, '_dispatch_notify',
                        lambda topic, data, session=None:
                        sent.append(data['state']))

    async def _run():
        task = asyncio.ensure_future(plugin.ddict_destroy(did))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await backend._call(lambda: None)
    asyncio.run(_run())
    assert sent == ['destroyed']


def test_partial_put_reports_progress():
    backend = _FakeBackend()

    class _Full(_FakeDDict):
        def __setitem__(self, k, v):
            if len(self) >= 2:
                raise RuntimeError('ddict full')
            super().__setitem__(k, v)

    backend._new_ddict = lambda *a: _Full(*a)

    async def _run():
        await backend.create('d', 1, 1, 1024)
        await backend.put('d', {'a': 1, 'b': 2, 'c': 3})
    with pytest.raises(RuntimeError, match="after 2 of 3 items"):
        asyncio.run(_run())


# ── review round 6 ──────────────────────────────────────────────────────────

def test_create_racing_close_starts_no_runtime():
    from fastapi import HTTPException
    backend = _FakeBackend()

    async def _run():
        await backend.create('a', 1, 1, 1024)
        close = asyncio.ensure_future(backend.close())
        await asyncio.sleep(0)              # close submitted, not finished
        with pytest.raises(HTTPException) as e:
            await backend.create('b', 1, 1, 1024)
        await close
        return e.value.status_code
    assert asyncio.run(_run()) == 503
    assert backend.up   == 1                # no second runtime
    assert backend.down == 1


def test_close_sync_unused_backend_starts_no_thread():
    backend = _FakeBackend()
    backend.close_sync()
    assert not backend._pool.started
