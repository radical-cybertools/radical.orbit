"""
Test that _watch_task runs concurrently and does not block subsequent submits.
This test uses the REAL rhapsody package (must be installed in venv).
Run with: python -m pytest tests/integration/test_rhapsody_concurrent.py -v -s
"""

import asyncio
import time
import pytest

try:
    import rhapsody as rh
    RHAPSODY_AVAILABLE = True
except ImportError:
    RHAPSODY_AVAILABLE = False


pytestmark = pytest.mark.skipif(
    not RHAPSODY_AVAILABLE,
    reason="rhapsody package not installed"
)


async def _make_session():
    b = rh.get_backend('concurrent')
    if hasattr(b, '__await__'):
        b = await b
    return rh.Session(backends=[b], uid=f"test.{id(b)}")


@pytest.mark.asyncio
async def test_concurrent_tasks_notify_independently():
    """
    Two tasks submitted to separate sessions should notify independently.
    Task 2 (short sleep) should finish and notify before task 1 (long sleep).
    """
    notifications = []

    async def _watch(session, task, label):
        await session.wait_tasks([task])
        notifications.append((label, time.monotonic()))

    s1 = await _make_session()
    s2 = await _make_session()

    t1 = rh.BaseTask(uid='t1', executable='/bin/sleep', arguments=['0.5'])
    t2 = rh.BaseTask(uid='t2', executable='/bin/sleep', arguments=['0.1'])

    await s1.submit_tasks([t1])
    await s2.submit_tasks([t2])

    start = time.monotonic()
    w1 = asyncio.ensure_future(_watch(s1, t1, 't1'))
    w2 = asyncio.ensure_future(_watch(s2, t2, 't2'))

    await asyncio.gather(w1, w2)
    elapsed = time.monotonic() - start

    # Both should finish within ~0.7s (not 0.6s if sequential)
    assert elapsed < 0.8, f"Tasks appear to have run sequentially: {elapsed:.2f}s"

    # t2 should have finished before t1
    labels = [n[0] for n in notifications]
    assert labels == ['t2', 't1'], f"Wrong order: {labels}"


@pytest.mark.asyncio
async def test_submit_returns_immediately_while_task_runs():
    """
    submit_tasks must return quickly (<<1s) even if task runs for 2s.
    The watcher should not block the submit path.
    """
    s = await _make_session()
    task = rh.BaseTask(uid='tb', executable='/bin/sleep', arguments=['2'])

    start = time.monotonic()
    await s.submit_tasks([task])
    elapsed = time.monotonic() - start

    # submit must return in well under 1 second
    assert elapsed < 0.5, f"submit_tasks blocked for {elapsed:.2f}s"

    # Cancel the running task to not hang the test
    if hasattr(s, 'close'):
        await s.close()


@pytest.mark.asyncio
async def test_watch_task_isolation_between_sessions():
    """
    Verifies that _watch_task for session 1 doesn't interfere with session 2.
    Both sessions should be completely independent.
    """
    from radical.orbit.plugin_rhapsody import RhapsodySession

    notify_calls = []

    async def make_rh_session(sid):
        sess = RhapsodySession(sid)
        mock_plugin = type('_P', (), {
            '_dispatch_notify': lambda self_, t, d: notify_calls.append((sid, t, d['uid']))
        })()
        sess._plugin = mock_plugin
        b = rh.get_backend('concurrent')
        if hasattr(b, '__await__'):
            b = await b
        sess._rh_session = rh.Session(backends=[b], uid=sid)
        sess._active = True
        return sess

    s1 = await make_rh_session('s1')
    s2 = await make_rh_session('s2')

    # Submit to each session — tasks have different durations
    r1 = await s1.submit_tasks([{'uid': 'ta', 'executable': '/bin/sleep', 'arguments': ['0.4']}])
    r2 = await s2.submit_tasks([{'uid': 'tb', 'executable': '/bin/echo', 'arguments': ['hi']}])

    # Give enough time for both to complete
    await asyncio.sleep(1.0)

    # Both sessions should have fired notifications
    assert any(c[0] == 's1' and c[2] == 'ta' for c in notify_calls), \
        f"Session s1 did not notify: {notify_calls}"
    assert any(c[0] == 's2' and c[2] == 'tb' for c in notify_calls), \
        f"Session s2 did not notify: {notify_calls}"

    await s1._rh_session.close()
    await s2._rh_session.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
