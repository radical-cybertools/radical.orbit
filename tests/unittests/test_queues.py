"""Tests for :class:`radical.orbit.queues.BoundedDropOldestQueue`."""

import asyncio

import pytest

from radical.orbit.queues import BoundedDropOldestQueue


def test_push_under_cap_keeps_order_no_drop():
    q = BoundedDropOldestQueue(3)
    for i in range(3):
        q.push(i)
    assert list(q.buf) == [0, 1, 2]
    assert q.dropped == 0
    assert q.wake.is_set()


def test_overflow_drops_oldest_and_counts():
    q = BoundedDropOldestQueue(2)
    for i in range(5):
        q.push(i)
    # deque(maxlen=2) keeps the newest two; three were evicted.
    assert list(q.buf) == [3, 4]
    assert q.dropped == 3


@pytest.mark.asyncio
async def test_drain_yields_then_waits():
    q = BoundedDropOldestQueue(4)
    seen = []

    async def consumer():
        async for item in q.drain():
            seen.append(item)
            if len(seen) == 3:
                return

    task = asyncio.ensure_future(consumer())
    q.push('a')
    q.push('b')
    await asyncio.sleep(0.01)
    assert seen == ['a', 'b']
    # After draining, the consumer waits on a cleared event.
    assert not q.wake.is_set()
    q.push('c')
    await asyncio.wait_for(task, timeout=1.0)
    assert seen == ['a', 'b', 'c']
