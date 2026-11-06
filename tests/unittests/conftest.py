import os
import pathlib

import pytest


# The real ORBIT config/state dir of the invoking user.  It may serve a live
# deployment, so the unit tests must never write there (issue #162).
_REAL_ORBIT_DIR = pathlib.Path.home() / '.radical' / 'orbit'

# How deep the leak check looks (psij/output/session.* is at depth 3): a
# live deployment's tree can be large, on a slow shared filesystem.
_SNAPSHOT_DEPTH = 3

# The empty HOME the unit tests run with (set by _isolate_orbit_dirs).
_test_home = None


def _snapshot(root: pathlib.Path, depth: int = _SNAPSHOT_DEPTH) -> set:
    """Paths below *root* (relative, at most *depth* levels), or an empty
    set if it is absent."""
    found = set()

    def _walk(path, rel, level):
        try:
            entries = list(os.scandir(path))
        except OSError:
            return
        for e in entries:
            sub = f'{rel}{e.name}'
            found.add(sub)
            if level < depth and e.is_dir(follow_symlinks=False):
                _walk(e.path, sub + '/', level + 1)

    if root.is_dir():
        found.add('.')
        _walk(root, '', 1)
    return found


def pytest_sessionstart(session):
    session.config._orbit_dir_before = _snapshot(_REAL_ORBIT_DIR)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Report writes under ``.radical/orbit`` that the redirects missed.

    Runs after the fixture teardown (``trylast``: after pytest's own
    ``sessionfinish``).  Checks the real ``~/.radical/orbit`` (paths the run
    added) and the test HOME's ``.radical/orbit`` (anything there is a
    home-derived default no fixture redirects yet).  Fails a passing run in
    CI (``$CI`` set); locally it only warns, since a live deployment on the
    same host may legitimately add files to the real directory meanwhile.
    """
    before = getattr(session.config, '_orbit_dir_before', None)
    added  = sorted(_snapshot(_REAL_ORBIT_DIR) - before) \
             if before is not None else []
    leaked = sorted(_snapshot(_test_home / '.radical' / 'orbit')) \
             if _test_home is not None else []
    lines  = [f'{_REAL_ORBIT_DIR}/{p}' for p in added] \
           + [f'<test HOME>/.radical/orbit/{p}' for p in leaked]
    if not lines:
        return
    shown = '\n  '.join(lines[:20]) + ('\n  ...' if len(lines) > 20 else '')
    msg   = (f"tests wrote {len(lines)} path(s) under .radical/orbit "
             f"(redirect the default in conftest.py, see issue #162):\n  {shown}")
    reporter = session.config.pluginmanager.get_plugin('terminalreporter')
    if reporter is not None:
        reporter.write_line(msg, red=True)
    if os.environ.get('CI') and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture(autouse=True, scope='session')
def _isolate_orbit_dirs(tmp_path_factory):
    """Point the home-derived module defaults under ``~/.radical/orbit`` at
    an empty test HOME: psij output/work dirs, tunnel rendezvous files,
    dispatcher/federation state and scratch roots, and the broker cert / key
    / token files.  Session-scoped, so it is in place before module-scoped
    fixtures build plugins; tests that need their own files still patch per
    test.
    """
    global _test_home
    from radical.orbit import (utils, tunnel, plugin_task_dispatcher,
                               plugin_federation)
    _test_home = tmp_path_factory.mktemp('home')
    base       = tmp_path_factory.mktemp('orbit-state')
    conf       = _test_home / '.radical' / 'orbit'      # never created
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('RADICAL_ORBIT_PSIJ_DIR', str(base / 'psij'))
        mp.setattr(utils, 'DEFAULT_DIR', conf)
        mp.setattr(utils, 'CERT_FILE',   conf / 'broker_cert.pem')
        mp.setattr(utils, 'KEY_FILE',    conf / 'broker_key.pem')
        mp.setattr(utils, 'TOKEN_FILE',  conf / 'broker.token')
        mp.setattr(tunnel, 'RELAY_BASE', base / 'tunnels')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_STATE_ROOT',
                   base / 'task_dispatcher' / 'state')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_SCRATCH_ROOT',
                   base / 'task_dispatcher' / 'scratch')
        mp.setattr(plugin_federation, '_DEFAULT_STATE_ROOT',
                   base / 'federation')
        yield


@pytest.fixture(autouse=True)
def _empty_home(monkeypatch):
    """Each unit test sees the empty test HOME, so none can write into -- or
    pass only because of files in -- the real home.  Per test, so tests
    outside this directory (e.g. integration tests in the same run) keep
    the real HOME."""
    monkeypatch.setenv('HOME', str(_test_home))


@pytest.fixture(autouse=True)
def _reset_batch_system_detection():
    """Every test starts and ends with a clear batch-system detection cache.

    ``detect_batch_system()`` memoises its result into a module global.
    Without this, the first unmocked detection (e.g. real SLURM on the test
    host) leaks into later tests and defeats their ``shutil.which`` mocks.
    """
    from radical.orbit import batch_system as _bs
    _bs.reset_detection()
    yield
    _bs.reset_detection()
