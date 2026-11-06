import os
import pathlib

import pytest


# The real ORBIT config/state dir of the invoking user.  It may serve a live
# deployment, so the unit tests must never write there (issue #162).
_REAL_ORBIT_DIR = pathlib.Path.home() / '.radical' / 'orbit'

# How deep the leak check looks for new paths (psij/output/session.* is at
# depth 3): a live deployment's tree can be large, on a slow shared
# filesystem.  Files at depth 1 (broker token / cert / key, globus.json) are
# also checked for changed content (size, mtime).
_SNAPSHOT_DEPTH = 3

# Ambient settings of a live deployment that would override the redirected
# files (they take precedence over the files in the lookups).
_AMBIENT_ENV = ('RADICAL_ORBIT_BROKER_URL',  'RADICAL_ORBIT_BROKER_TOKEN',
                'RADICAL_ORBIT_BROKER_CERT', 'RADICAL_ORBIT_BROKER_KEY')


def _snapshot(root: pathlib.Path, depth: int = _SNAPSHOT_DEPTH) -> dict:
    """Paths below *root* (relative, at most *depth* levels) -> ``None``,
    or ``(size, mtime_ns)`` for files at depth 1; empty if *root* is
    absent."""
    found = {}

    def _walk(path, rel, level):
        try:
            entries = list(os.scandir(path))
        except OSError:
            return
        for e in entries:
            sub   = f'{rel}{e.name}'
            isdir = e.is_dir(follow_symlinks=False)
            stamp = None
            if level == 1 and not isdir:
                try:
                    st    = e.stat(follow_symlinks=False)
                    stamp = (st.st_size, st.st_mtime_ns)
                except OSError:
                    pass
            found[sub] = stamp
            if level < depth and isdir:
                _walk(e.path, sub + '/', level + 1)

    if root.is_dir():
        found['.'] = None
        _walk(root, '', 1)
    return found


# Taken when pytest loads this conftest (before any of its tests run): a
# ``pytest_sessionstart`` here would not run when the conftest is loaded
# during collection (``pytest`` / ``pytest tests/``).
_ORBIT_DIR_BEFORE = _snapshot(_REAL_ORBIT_DIR)

# The empty HOME the unit tests run with (set by _isolate_orbit_dirs).
_test_home: 'pathlib.Path | None' = None
_leak_report: 'str | None' = None


def _ci() -> bool:
    return os.environ.get('CI', '').strip().lower() not in ('', '0', 'false',
                                                            'no', 'off')


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session):
    """Check for writes under ``.radical/orbit`` that the redirects missed.

    Runs after the fixture teardown (``trylast``: after pytest's own
    ``sessionfinish``).  Checks the real ``~/.radical/orbit`` (paths the run
    added, credential files it changed) and the test HOME's
    ``.radical/orbit`` (anything there is a home-derived default no fixture
    redirects yet).  Fails a passing run in CI (``$CI`` set); locally it
    only warns, since a live deployment on the same host may legitimately
    change the real directory meanwhile.  Reported at the end of the
    terminal summary.
    """
    global _leak_report
    after   = _snapshot(_REAL_ORBIT_DIR)
    changed = sorted(p for p, stamp in after.items()
                     if p not in _ORBIT_DIR_BEFORE
                     or (stamp is not None and stamp != _ORBIT_DIR_BEFORE[p]))
    leaked  = sorted(_snapshot(_test_home / '.radical' / 'orbit')) \
              if _test_home is not None else []
    lines   = [f'{_REAL_ORBIT_DIR}/{p}' for p in changed] \
            + [f'<test HOME>/.radical/orbit/{p}' for p in leaked]
    if not lines:
        return
    shown = '\n  '.join(lines[:20]) + ('\n  ...' if len(lines) > 20 else '')
    _leak_report = (f"tests wrote {len(lines)} path(s) under .radical/orbit "
                    f"(redirect the default in tests/unittests/conftest.py, "
                    f"see issue #162):\n  {shown}")
    if _ci() and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if _leak_report:
        terminalreporter.section('ORBIT home leak', sep='=', red=True)
        terminalreporter.write_line(_leak_report, red=True)


@pytest.fixture(autouse=True, scope='session')
def _isolate_orbit_dirs(tmp_path_factory):
    """Run the unit tests with an empty HOME, and point the home-derived
    module defaults (bound at import, from the real home) at a separate
    temp tree: psij output/work dirs, tunnel rendezvous files,
    dispatcher/federation state and scratch roots; the broker cert / key /
    token files resolve into the (empty) test HOME.  Ambient broker
    settings of a live deployment are cleared.

    Session-scoped, so it is in place before module-scoped fixtures build
    plugins; tests that need their own files still patch per test.  Run
    integration tests separately: they need the real HOME.
    """
    global _test_home
    from radical.orbit import (utils, tunnel, plugin_task_dispatcher,
                               plugin_federation)
    home       = tmp_path_factory.mktemp('home')
    state      = tmp_path_factory.mktemp('orbit-state')
    conf       = home / '.radical' / 'orbit'            # never created
    _test_home = home
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('HOME', str(home))
        for name in _AMBIENT_ENV:
            mp.delenv(name, raising=False)
        mp.setenv('RADICAL_ORBIT_PSIJ_DIR', str(state / 'psij'))
        mp.setattr(utils, 'DEFAULT_DIR', conf)
        mp.setattr(utils, 'CERT_FILE',   conf / 'broker_cert.pem')
        mp.setattr(utils, 'KEY_FILE',    conf / 'broker_key.pem')
        mp.setattr(utils, 'TOKEN_FILE',  conf / 'broker.token')
        mp.setattr(tunnel, 'RELAY_BASE', state / 'tunnels')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_STATE_ROOT',
                   state / 'task_dispatcher' / 'state')
        mp.setattr(plugin_task_dispatcher, '_DEFAULT_SCRATCH_ROOT',
                   state / 'task_dispatcher' / 'scratch')
        mp.setattr(plugin_federation, '_DEFAULT_STATE_ROOT',
                   state / 'federation')
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
