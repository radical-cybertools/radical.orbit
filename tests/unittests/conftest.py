import os
import pathlib
import shutil
import tempfile

import pytest


# The real ORBIT config/state dir of the invoking user.  It may serve a live
# deployment, so the suite must never write there (issue #162).  Resolved
# before HOME is replaced below.
_REAL_ORBIT_DIR = pathlib.Path.home() / '.radical' / 'orbit'

# The whole run sees an empty HOME (set before any radical.orbit module is
# imported, so home-derived module defaults land there too): no test can
# write into, or pass only because of files in, the real home directory.
_REAL_HOME = os.environ.get('HOME')
_TEST_HOME = tempfile.mkdtemp(prefix='orbit-test-home-')
os.environ['HOME'] = _TEST_HOME


def _snapshot(root: pathlib.Path) -> set:
    """All paths below *root* (relative), or an empty set if it is absent."""
    if not root.exists():
        return set()
    return {str(p.relative_to(root)) for p in root.rglob('*')} | {'.'}


def pytest_sessionstart(session):
    session.config._orbit_dir_before = _snapshot(_REAL_ORBIT_DIR)


def pytest_sessionfinish(session, exitstatus):
    """Report writes under ``.radical/orbit`` that the redirects missed.

    Two places are checked: the real ``~/.radical/orbit`` (paths the run
    added) and the test HOME's ``.radical/orbit`` (anything there is a
    home-derived default no fixture redirects yet).  Fails the run in CI
    (``$CI`` set); locally it only warns, since a live deployment on the
    same host may legitimately add files to the real directory meanwhile.
    """
    leaked = sorted(_snapshot(pathlib.Path(_TEST_HOME) / '.radical' / 'orbit'))
    if _REAL_HOME is not None:
        os.environ['HOME'] = _REAL_HOME
    shutil.rmtree(_TEST_HOME, ignore_errors=True)
    before = getattr(session.config, '_orbit_dir_before', None)
    added  = sorted(_snapshot(_REAL_ORBIT_DIR) - before) \
             if before is not None else []
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
    if os.environ.get('CI'):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture(autouse=True, scope='session')
def _isolate_orbit_dirs(tmp_path_factory):
    """Point every default under ``~/.radical/orbit`` that the code writes
    to at a temporary directory (psij output/work dirs, tunnel rendezvous
    files, dispatcher/federation state and scratch roots).

    Session-scoped, so it is in place before module- and session-scoped
    fixtures build plugins; tests that need their own directory still patch
    these per test.
    """
    from radical.orbit import tunnel, plugin_task_dispatcher, plugin_federation
    base = tmp_path_factory.mktemp('orbit-home')
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('RADICAL_ORBIT_PSIJ_DIR', str(base / 'psij'))
        mp.setattr(tunnel, 'RELAY_BASE', base / 'tunnels')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_STATE_ROOT',
                   base / 'task_dispatcher' / 'state')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_SCRATCH_ROOT',
                   base / 'task_dispatcher' / 'scratch')
        mp.setattr(plugin_federation, '_DEFAULT_STATE_ROOT',
                   base / 'federation')
        yield


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
