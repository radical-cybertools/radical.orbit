import pytest


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


@pytest.fixture(autouse=True)
def _no_submit_backoff(monkeypatch):
    """Submit retries resend at once in tests (no real backoff sleeps).

    Patched only when the module is loaded already: importing it here would
    pre-empt the rhapsody mocks some test modules install before import.
    """
    import sys
    prh = sys.modules.get('radical.orbit.plugin_rhapsody')
    if prh is not None:
        monkeypatch.setattr(prh, 'SUBMIT_BACKOFF', (0.0,))
