#!/usr/bin/env python

__author__    = 'Radical Development Team'
# pylint: disable=protected-access,unused-argument
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2026, RADICAL@Rutgers'
__license__   = 'MIT'

'''
Tests for the LUCID portal: the shared flow (``lucid_flow``) against fake
globus / rhapsody / staging plugin clients, and the ``lucid`` plugin on top.
'''

import asyncio
import base64
import json
import os
import threading
import time

import pytest

from fastapi import FastAPI, HTTPException

from radical.orbit            import lucid_flow as lf
from radical.orbit.plugin_lucid import PluginLucid


PNG = b'\x89PNG\r\n\x1a\nfake'


# ------------------------------------------------------------------------------
# fakes
#
class FakeGlobus:
    def __init__(self, fail=False, status=None):
        self.fail      = fail
        self.status    = status            # force a status (e.g. INACTIVE)
        self.transfers = []
        self.cancelled = []
        self.closed    = False

    def submit_transfer(self, source, destination, items, label=None,
                        sync_level=None):
        self.transfers.append((source, destination, items))
        return {'task_id': f't{len(self.transfers)}'}

    def get_task(self, task_id):
        n = len(self.transfers[int(task_id[1:]) - 1][2])
        status = self.status or ('FAILED' if self.fail else 'SUCCEEDED')
        return {'status'           : status,
                'files'            : n, 'files_transferred': n,
                'bytes_transferred': 1000 * n}

    def cancel_task(self, task_id):
        self.cancelled.append(task_id)

    def close(self):
        self.closed = True


class FakeRhapsody:
    def __init__(self, fail_names=()):
        self.fail_names = set(fail_names)
        self.tasks      = {}
        self.closed     = False

    def submit_tasks(self, tasks):
        out = []
        for t in tasks:
            uid    = f'task.{len(self.tasks):04d}'
            script = t['arguments'][1]
            if 'echo scratch=$SCRATCH' in script:
                stdout = 'Welcome to the login banner\nscratch=/scratch/me\n'
            elif 'cellprofiler -c -r' in script:
                stdout = 'host=nid001 files=42\n'
            else:
                stdout = ''
            failed = any(n in script for n in self.fail_names)
            self.tasks[uid] = {'uid': uid, 'state': 'FAILED' if failed else 'DONE',
                               'stdout': stdout, 'stderr': 'boom' if failed else ''}
            out.append({'uid': uid})
        return out

    def list_tasks(self):
        return {'tasks': list(self.tasks.values())}

    def close(self):
        self.closed = True


class FakeStaging:
    def __init__(self, allowed=('/scratch',)):
        self.closed  = False
        self.got     = []
        self.allowed = allowed

    def list(self, path):
        if not path.startswith(self.allowed):
            raise RuntimeError('Path escapes allowed directories')
        return {'path': path, 'entries': [{'name': 'cell_00.png', 'type': 'file'},
                                          {'name': 'cell_01.png', 'type': 'file'},
                                          {'name': 'notes.txt',   'type': 'file'}]}

    def get(self, src, dst):
        self.got.append(src)
        with open(dst, 'wb') as fout:
            fout.write(PNG)

    def close(self):
        self.closed = True


class FakeRuntime:
    def __init__(self, globus=None, rhapsody=None, staging=None, topo=None):
        self.clients = {'globus'  : globus   or FakeGlobus(),
                        'rhapsody': rhapsody or FakeRhapsody(),
                        'staging' : staging  or FakeStaging()}
        self.kwargs  = {}
        self.topo    = topo or {
            'hpc1'  : {'role': 'endpoint',
                       'plugins': {'globus': 1, 'rhapsody': 1, 'staging': 1}},
            'laptop': {'role': 'endpoint', 'plugins': {'lucid': 1}},
            'broker': {'role': 'broker', 'plugins': {'globus': 1, 'rhapsody': 1,
                                                     'staging': 1}}}

    def topology(self):
        return self.topo

    def get_plugin(self, endpoint, plugin, **kwargs):
        self.kwargs[plugin] = kwargs
        return self.clients[plugin]


def _run(rt, tmp_path, wells=('r01c01', 'r01c02'), **kw):
    return lf.LucidRun(rt, 'hpc1', list(wells), refresh_token='rt',
                       local_dir=str(tmp_path), poll=0.01, **kw)


# ------------------------------------------------------------------------------
# lucid_flow helpers
#
def test_candidate_endpoints_needs_all_plugins_and_endpoint_role():
    assert lf.candidate_endpoints(FakeRuntime().topology()) == ['hpc1']
    assert lf.candidate_endpoints({}) == []


def test_validate_wells_orders_and_rejects():
    assert lf.validate_wells(['r02c01', 'r01c12']) == ['r01c12', 'r02c01']
    with pytest.raises(lf.LucidError):
        lf.validate_wells([])
    with pytest.raises(lf.LucidError):
        lf.validate_wells(['r09c01'])


def test_get_analysis():
    assert lf.get_analysis('cropped_cells')['pipeline'] == 'cropped_cells.cppipe'
    disabled = next(a['id'] for a in lf.ANALYSES if not a['enabled'])
    with pytest.raises(lf.LucidError, match='not available'):
        lf.get_analysis(disabled)
    with pytest.raises(lf.LucidError, match='unknown'):
        lf.get_analysis('nope')


def test_transfer_refresh_token(tmp_path):
    f = tmp_path / 'tokens.json'
    f.write_text(json.dumps({'other_tokens': [
        {'resource_server': 'x', 'refresh_token': 'no'},
        {'resource_server': 'transfer.api.globus.org', 'refresh_token': 'yes'}]}))
    assert lf.transfer_refresh_token(str(f)) == 'yes'
    with pytest.raises(lf.LucidError):
        lf.transfer_refresh_token(str(tmp_path / 'missing.json'))


def test_kv():
    assert lf._kv('host=a files=3 junk\n') == {'host': 'a', 'files': '3'}


# ------------------------------------------------------------------------------
# LucidRun
#
def test_run_success(tmp_path):
    rt      = FakeRuntime()
    updates = []
    run     = _run(rt, tmp_path, on_update=updates.append)
    st      = run.run()

    assert st['status'] == 'done', st['error']
    assert all(p['state'] == 'done' for p in st['phases'].values())
    assert st['run_dir'] == f'/scratch/me/lucid-e2e/runs/{run.run_id}'
    assert st['target']  == f'/tmp_radical/{run.run_id}/'

    # stage-in: pipeline dir + two channels per well; stage-out: results dir
    g = rt.clients['globus']
    src, dst, items = g.transfers[0]
    assert (src, dst) == (lf.LUCID_COLLECTION, lf.NERSC_DTN)
    assert len(items) == 1 + 2 * 2
    assert g.transfers[1][:2] == (lf.NERSC_DTN, lf.LUCID_COLLECTION)
    assert g.transfers[1][2][0]['destination'] == st['target']

    assert st['stats']['compute'] == {'tasks': 2, 'done': 2, 'failed': 0,
                                      'nodes': 1, 'files': 84}
    assert st['tasks']['done'] == 2
    assert [p.split('/')[-1] for p in st['images']] == \
           ['montage.png', 'cell_00.png', 'cell_01.png']
    assert rt.kwargs['globus'] == {'refresh_token': 'rt',
                                   'client_id': lf.GLOBUS_CLIENT_ID}
    assert rt.kwargs['rhapsody'] == {'backends': ['dragon_v3']}
    assert all(c.closed for c in rt.clients.values())
    assert updates and updates[-1]['finished']


def test_run_partial_task_failure_continues(tmp_path):
    rt = FakeRuntime(rhapsody=FakeRhapsody(fail_names=['r01c02f01p05']))
    st = _run(rt, tmp_path).run()
    assert st['status'] == 'done'
    assert st['stats']['compute']['failed'] == 1
    assert list(st['failed_wells']) == ['r01c02']
    assert st['tasks']['failed'] == 1


def test_run_all_tasks_failed(tmp_path):
    rt = FakeRuntime(rhapsody=FakeRhapsody(fail_names=['cellprofiler -c -r']))
    st = _run(rt, tmp_path).run()
    assert st['status'] == 'failed'
    assert st['phases']['compute']['state'] == 'failed'
    assert 'all 2 tasks failed' in st['error']
    assert all(c.closed for c in rt.clients.values())


def test_run_transfer_failure_closes_sessions(tmp_path):
    rt = FakeRuntime(globus=FakeGlobus(fail=True))
    st = _run(rt, tmp_path).run()
    assert st['status'] == 'failed'
    assert st['phases']['stage_in']['state'] == 'failed'
    assert st['phases']['compute']['state'] == 'pending'
    assert all(c.closed for c in rt.clients.values())


def test_run_rejects_disabled_analysis(tmp_path):
    disabled = next(a['id'] for a in lf.ANALYSES if not a['enabled'])
    with pytest.raises(lf.LucidError):
        _run(FakeRuntime(), tmp_path, analysis=disabled)


# ------------------------------------------------------------------------------
# plugin
#
def _plugin(rt):
    app = FastAPI()
    app.state.endpoint_service = rt
    return PluginLucid(app)


def test_plugin_routes_and_ui():
    app = FastAPI()
    PluginLucid(app)
    pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    for route in ('config', 'submit', 'status', 'image'):
        assert any(route in p for p in pats), route
    assert PluginLucid.ui_config['custom_template'] is True


def test_plugin_not_on_broker():
    app = FastAPI()
    app.state.is_broker = True
    assert not PluginLucid.is_enabled(app)
    assert PluginLucid.is_enabled(FastAPI())


@pytest.mark.asyncio
async def test_plugin_config():
    cfg = await _plugin(FakeRuntime())._config()
    assert cfg['endpoints'] == ['hpc1']
    assert cfg['endpoint']  == 'hpc1'
    assert cfg['defaults']['collection'] == lf.LUCID_COLLECTION
    assert len(cfg['wells']) == 96
    assert any(not a['enabled'] for a in cfg['analyses'])


@pytest.mark.asyncio
async def test_plugin_submit_status_image(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')
    p = _plugin(FakeRuntime())
    assert p._status() == {'status': 'idle'}

    res = await p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']})
    p._thread.join(10)
    st = p._status()
    assert st['run_id'] == res['run_id']
    assert st['status'] == 'done'
    assert st['images'] == ['montage.png', 'cell_00.png', 'cell_01.png']

    img = p._image(res['run_id'], 'montage.png')
    assert base64.b64decode(img['data']) == PNG
    with pytest.raises(HTTPException):
        p._image(res['run_id'], '../../etc/passwd')
    with pytest.raises(HTTPException):
        p._image('other-run', 'montage.png')


@pytest.mark.asyncio
async def test_plugin_submit_validation(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')
    p = _plugin(FakeRuntime())
    for params in ({'wells': ['r01c01']},                        # no endpoint
                   {'endpoint': 'laptop', 'wells': ['r01c01']},  # lacks plugins
                   {'endpoint': 'hpc1', 'wells': []},            # no wells
                   {'endpoint': 'hpc1', 'wells': ['r01c01'],
                    'analysis': 'h2ax_foci'}):                   # disabled
        with pytest.raises(HTTPException) as e:
            await p._submit(params)
        assert e.value.status_code == 400


@pytest.mark.asyncio
async def test_plugin_rejects_concurrent_run(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')

    class SlowStaging(FakeStaging):
        def get(self, src, dst):
            time.sleep(0.3)
            super().get(src, dst)

    p = _plugin(FakeRuntime(staging=SlowStaging()))
    await p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']})
    with pytest.raises(HTTPException) as e:
        await p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']})
    assert e.value.status_code == 409
    await asyncio.to_thread(p._thread.join, 10)


# ------------------------------------------------------------------------------
# review round 1: bounded waits, cancel, input validation, races
#
def test_globus_inactive_fails_and_cancels(tmp_path):
    g  = FakeGlobus(status='INACTIVE')
    rt = FakeRuntime(globus=g)
    st = _run(rt, tmp_path).run()
    assert st['status'] == 'failed'
    assert 'inactive' in st['error']
    assert g.cancelled == ['t1']
    assert all(c.closed for c in rt.clients.values())


def test_globus_wait_times_out_and_cancels(tmp_path):
    g   = FakeGlobus(status='ACTIVE')
    run = _run(FakeRuntime(globus=g), tmp_path)
    g.submit_transfer('a', 'b', [{}])
    with pytest.raises(lf.LucidError, match='did not finish'):
        run._globus_wait(g, 't1', 'stage_in', timeout=0.05)
    assert g.cancelled == ['t1']


def test_cancel_stops_a_run(tmp_path):
    g   = FakeGlobus(status='ACTIVE')
    rt  = FakeRuntime(globus=g)
    run = _run(rt, tmp_path)
    t   = threading.Thread(target=run.run)
    t.start()
    time.sleep(0.1)
    run.cancel()
    t.join(5)
    st = run.snapshot()
    assert st['status'] == 'failed' and 'cancelled' in st['error']
    assert all(c.closed for c in rt.clients.values())


def test_preflight_fails_early_without_staging_access(tmp_path):
    rt = FakeRuntime(staging=FakeStaging(allowed=('/home',)))
    st = _run(rt, tmp_path).run()
    assert st['status'] == 'failed'
    assert st['phases']['preflight']['state'] == 'failed'
    assert st['phases']['stage_in']['state'] == 'pending'
    assert 'RADICAL_ORBIT_SCRATCH_BASE' in st['error']
    assert rt.clients['globus'].transfers == []


@pytest.mark.parametrize('kw', [
    {'images_path'  : '/x\nTXT\nrm -rf ~\nTXT'},     # heredoc break-out
    {'images_path'  : '/a/../../etc'},
    {'images_path'  : 'relative/path'},
    {'pipeline_path': '/p;id'},
    {'collection'   : 'not-a-uuid'},
])
def test_run_rejects_unsafe_inputs(tmp_path, kw):
    with pytest.raises(lf.LucidError):
        _run(FakeRuntime(), tmp_path, **kw)


def test_run_id_is_unique_and_matches_cleanup_pattern():
    ids = {lf.new_run_id() for _ in range(50)}
    assert len(ids) > 1
    assert all(lf.RUN_ID_RE.match(i) for i in ids)
    assert not lf.RUN_ID_RE.match('..')


@pytest.mark.asyncio
async def test_plugin_concurrent_submits_start_one_run(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')

    class SlowTopoRuntime(FakeRuntime):
        def topology(self):
            time.sleep(0.2)              # widen the window between checks
            return super().topology()

    p   = _plugin(SlowTopoRuntime())
    res = await asyncio.gather(
            p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']}),
            p._submit({'endpoint': 'hpc1', 'wells': ['r01c02']}),
            return_exceptions=True)
    ok  = [r for r in res if isinstance(r, dict)]
    err = [r for r in res if isinstance(r, HTTPException)]
    assert len(ok) == 1 and len(err) == 1 and err[0].status_code == 409
    await asyncio.to_thread(p._thread.join, 10)
    # the slot is free again once the run ended
    await p._submit({'endpoint': 'hpc1', 'wells': ['r01c03']})
    await asyncio.to_thread(p._thread.join, 10)


@pytest.mark.asyncio
async def test_plugin_failed_submit_frees_the_slot(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')
    p = _plugin(FakeRuntime())
    with pytest.raises(HTTPException):
        await p._submit({'endpoint': 'hpc1', 'wells': []})
    res = await p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']})
    await asyncio.to_thread(p._thread.join, 10)
    assert p._status()['run_id'] == res['run_id']


def test_plugin_ignores_stale_run_updates():
    p = _plugin(FakeRuntime())
    p._run   = type('R', (), {'run_id': 'current'})()
    p._state = {'run_id': 'current', 'status': 'running', 'images': []}
    p._on_update('old', {'run_id': 'old', 'status': 'done', 'images': []})
    assert p._status()['run_id'] == 'current'


@pytest.mark.asyncio
async def test_plugin_shutdown_cancels_run_and_removes_images(monkeypatch):
    monkeypatch.setattr(lf, 'transfer_refresh_token', lambda *a, **k: 'rt')
    p = _plugin(FakeRuntime(globus=FakeGlobus(status='ACTIVE')))
    await p._submit({'endpoint': 'hpc1', 'wells': ['r01c01']})
    imgdir = p._imgdir
    await p.shutdown()
    assert not p._thread.is_alive()
    assert p._status()['status'] == 'failed'
    assert not os.path.exists(imgdir)
