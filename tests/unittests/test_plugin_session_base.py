#!/usr/bin/env python

# pylint: disable=protected-access

import asyncio
import logging

import pytest

from radical.orbit.plugin_session_base import PluginSession


class _RecordingPlugin:
    """Captures notify() calls the way the real Plugin._dispatch_notify would."""

    def __init__(self):
        self.events = []

    def _dispatch_notify(self, topic, data):
        self.events.append((topic, data))


# ---------------------------------------------------------------------------
# notify()
# ---------------------------------------------------------------------------

def test_notify_forwards_to_plugin():
    sess = PluginSession("s1")
    plug = _RecordingPlugin()
    sess._plugin = plug

    sess.notify("topic", {"k": "v"})
    assert plug.events == [("topic", {"k": "v"})]


def test_notify_without_plugin_is_noop():
    sess = PluginSession("s1")            # no _plugin injected
    sess.notify("topic", {"k": "v"})      # must not raise


# ---------------------------------------------------------------------------
# start_status_poller
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_poller_notifies_on_change_then_stops_at_terminal():
    sess = PluginSession("s1")
    sess._plugin = _RecordingPlugin()

    items = {"j1": {"state": "RUNNING"}}

    async def fetch(key, item):
        # First poll flips to DONE (a change); after that item is terminal.
        item["state"] = "DONE"
        return item["state"]

    sess.start_status_poller(
        interval=0.01,
        items=lambda: items,
        is_terminal=lambda it: it["state"] == "DONE",
        fetch=fetch,
        to_payload=lambda key, item, result: {"job": key, "state": result},
        topic="job_status",
    )

    # The poller drives j1 to DONE (one notification) then exits (nothing left).
    await asyncio.sleep(0.1)
    assert sess._plugin.events == [("job_status", {"job": "j1", "state": "DONE"})]
    assert sess._status_poller_task.done()


@pytest.mark.asyncio
async def test_poller_stops_when_session_inactive():
    sess = PluginSession("s1")
    sess._plugin = _RecordingPlugin()
    items = {"j1": {"state": "RUNNING"}}

    async def fetch(key, item):
        return None  # never a change

    sess.start_status_poller(
        interval=0.01, items=lambda: items,
        is_terminal=lambda it: False, fetch=fetch,
        to_payload=lambda k, i, r: {}, topic="t")

    sess._active = False                  # simulate close
    await asyncio.sleep(0.05)
    assert sess._status_poller_task.done()
    assert sess._plugin.events == []


@pytest.mark.asyncio
async def test_poller_warns_after_consecutive_failures(caplog):
    sess = PluginSession("s1")
    sess._plugin = _RecordingPlugin()
    items = {"j1": {"state": "RUNNING"}}

    async def fetch(key, item):
        raise RuntimeError("token expired")

    logger = logging.getLogger("radical.orbit")
    logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger="radical.orbit"):
            sess.start_status_poller(
                interval=0.005, items=lambda: items,
                is_terminal=lambda it: False, fetch=fetch,
                to_payload=lambda k, i, r: {}, topic="t",
                name="poller", max_failures=3)
            await asyncio.sleep(0.1)
            sess.stop_status_poller()
    finally:
        logger.removeHandler(caplog.handler)

    assert "status poll failing" in caplog.text


@pytest.mark.asyncio
async def test_stop_status_poller_cancels():
    sess = PluginSession("s1")
    sess._plugin = _RecordingPlugin()
    items = {"j1": {"state": "RUNNING"}}

    task = sess.start_status_poller(
        interval=0.01, items=lambda: items,
        is_terminal=lambda it: False, fetch=lambda k, i: None,
        to_payload=lambda k, i, r: {}, topic="t")

    # Second call while running returns the same task (idempotent).
    assert sess.start_status_poller(
        interval=0.01, items=lambda: items,
        is_terminal=lambda it: False, fetch=lambda k, i: None,
        to_payload=lambda k, i, r: {}, topic="t") is task

    sess.stop_status_poller()
    await asyncio.sleep(0)
    assert task.cancelled() or task.done()


@pytest.mark.asyncio
async def test_close_stops_poller():
    sess = PluginSession("s1")
    sess._plugin = _RecordingPlugin()
    items = {"j1": {"state": "RUNNING"}}

    task = sess.start_status_poller(
        interval=0.01, items=lambda: items,
        is_terminal=lambda it: False, fetch=lambda k, i: None,
        to_payload=lambda k, i, r: {}, topic="t")

    await sess.close()
    await asyncio.sleep(0)
    assert task.cancelled() or task.done()
    assert sess.is_active is False
