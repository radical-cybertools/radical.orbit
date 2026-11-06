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

# Ambient RADICAL_ORBIT_* settings of a live deployment (broker URL / token
# / cert, federation state, scratch base, ...) take precedence over the
# redirected defaults, so the run clears them -- all but these.
_KEEP_ENV = ('RADICAL_ORBIT_LOG_LVL',)


def _snapshot(root: pathlib.Path, depth: int = _SNAPSHOT_DEPTH) -> dict:
    """Paths below *root* (relative, at most *depth* levels) -> ``None``,
    or ``(size, mtime_ns)`` for files at depth 1; empty if *root* is
    absent.  Symlinks are followed (a quota'd home links e.g. ``psij`` to
    scratch), within the depth limit."""
    found = {}

    def _walk(path, rel, level):
        try:
            entries = list(os.scandir(path))
        except OSError:
            return
        for e in entries:
            sub   = f'{rel}{e.name}'
            isdir = e.is_dir()
            stamp = None
            if level == 1 and not isdir:
                try:
                    st    = e.stat()
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

_leak_report: 'str | None' = None


def _ci() -> bool:
    return os.environ.get('CI', '').strip().lower() not in ('', '0', 'false',
                                                            'no', 'off')


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session):
    """Check the real ``~/.radical/orbit`` for changes during the run.

    Runs after the fixture teardown (``trylast``: after pytest's own
    ``sessionfinish``): paths added or removed, credential files changed.
    Such a change means some code resolved the home directory before the
    test HOME was set, or bypassed it.  Fails a passing run in CI (``$CI``
    set); locally it only warns, since a live deployment on the same host
    may legitimately change the real directory meanwhile.  Reported at the
    end of the terminal summary.
    """
    global _leak_report
    after   = _snapshot(_REAL_ORBIT_DIR)
    changed = sorted(p for p, stamp in after.items()
                     if p not in _ORBIT_DIR_BEFORE
                     or (stamp is not None and stamp != _ORBIT_DIR_BEFORE[p]))
    changed += sorted(f'{p} (removed)' for p in _ORBIT_DIR_BEFORE
                      if p not in after)
    lines   = [f'{_REAL_ORBIT_DIR}/{p}' for p in changed]
    if not lines:
        return
    shown = '\n  '.join(lines[:20]) + ('\n  ...' if len(lines) > 20 else '')
    _leak_report = (f"{len(lines)} path(s) under {_REAL_ORBIT_DIR} changed "
                    f"during the run.  Unit tests must not write there "
                    f"(resolve home-derived defaults per call, see "
                    f"utils.orbit_dir and issue #162); a live deployment on "
                    f"this host may also change it:\n  {shown}")
    if _ci() and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter):
    if _leak_report:
        terminalreporter.section('ORBIT home leak', sep='=', red=True)
        terminalreporter.write_line(_leak_report, red=True)


@pytest.fixture(autouse=True, scope='session')
def _isolate_orbit_dirs(tmp_path_factory):
    """Run the unit tests with an empty HOME: every default under
    ``~/.radical/orbit`` resolves from the current HOME per call (see
    ``utils.orbit_dir``), so this alone keeps them out of the real home.
    The psij dir goes to a separate temp tree (its output is not a leak);
    ambient ``RADICAL_ORBIT_*`` settings of a live deployment are cleared.

    Session-scoped, so it is in place before module-scoped fixtures build
    plugins; tests that need their own files still patch per test.  Run
    integration tests separately: they need the real HOME.
    """
    home  = tmp_path_factory.mktemp('home')
    state = tmp_path_factory.mktemp('orbit-state')
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('HOME', str(home))
        for name in [n for n in os.environ if n.startswith('RADICAL_ORBIT_')
                     and n not in _KEEP_ENV]:
            mp.delenv(name)
        mp.setenv('RADICAL_ORBIT_PSIJ_DIR', str(state / 'psij'))
        yield


@pytest.fixture(autouse=True)
def _fresh_home(tmp_path_factory, monkeypatch):
    """Each test gets its own empty HOME (outside its ``tmp_path``), so a
    file one test leaves under ``~/.radical/orbit`` (a token, a cert)
    cannot change another's lookups.  Module- and session-scoped fixtures
    use the session one."""
    monkeypatch.setenv('HOME', str(tmp_path_factory.mktemp('home')))


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
