#!/usr/bin/env python

# pylint: disable=protected-access

import asyncio
import logging

import pytest

from radical.orbit.plugin_session_base import PluginSession


class _RecordingPlugin:
    """Captures notify() calls the way the real Plugin._dispatch_notify would."""

    def __init__(self):
        self.events   = []
        self.sessions = []

    def _dispatch_notify(self, topic, data, session=None):
        self.events.append((topic, data))
        self.sessions.append(session)


# ---------------------------------------------------------------------------
# notify()
# ---------------------------------------------------------------------------

def test_notify_forwards_to_plugin():
    sess = PluginSession("s1")
    plug = _RecordingPlugin()
    sess._plugin = plug

    sess.notify("topic", {"k": "v"})
    assert plug.events   == [("topic", {"k": "v"})]
    assert plug.sessions == ["s1"]             # the event's session


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


# ---------------------------------------------------------------------------
# older overrides / shims without ``session``
# ---------------------------------------------------------------------------

def test_notify_falls_back_for_an_old_dispatch_override(caplog):
    """A plugin whose _dispatch_notify predates ``session`` still gets the
    notification (two-argument call, one warning)."""
    class _OldPlugin:
        def __init__(self):
            self.events = []

        def _dispatch_notify(self, topic, data):
            self.events.append((topic, data))

    sess = PluginSession("s1")
    sess._plugin = _OldPlugin()
    sess.notify("t", {"k": 1})
    sess.notify("t", {"k": 2})
    assert sess._plugin.events == [("t", {"k": 1}), ("t", {"k": 2})]
    assert sum("takes no 'session'" in r.message
               for r in caplog.records) == 1


def test_session_less_sends_keep_the_old_call_form():
    """A plugin-level (session-less) send calls an old send_notification
    override and an old host shim with their two/three arguments; a session
    send falls back to that form too."""
    import asyncio
    from fastapi import FastAPI
    from radical.orbit.plugin_base import Plugin

    class _Shim:
        def __init__(self):
            self.calls = []

        def send_notification(self, plugin, topic, data):   # old form
            self.calls.append((plugin, topic, data))

    class _Old(Plugin):
        plugin_name = 'old_notify_test'

        def __init__(self, app):
            super().__init__(app, 'old_notify_test')

    app = FastAPI()
    app.state.endpoint_service = _Shim()
    plugin = _Old(app)
    asyncio.run(plugin.send_notification('t', {'k': 1}))
    asyncio.run(plugin.send_notification('t', {'k': 2}, session='s1'))
    assert app.state.endpoint_service.calls == [
        ('old_notify_test', 't', {'k': 1}),
        ('old_notify_test', 't', {'k': 2})]

    sent = []

    class _OldSend(Plugin):
        plugin_name = 'old_send_test'

        def __init__(self, app):
            super().__init__(app, 'old_send_test')

        async def send_notification(self, topic, data):     # old override
            sent.append((topic, data))

    async def _go():
        p = _OldSend(FastAPI())
        p._dispatch_notify('t', {'k': 3})
        p._dispatch_notify('t', {'k': 4}, session='s1')
        await asyncio.sleep(0.01)

    asyncio.run(_go())
    assert sent == [('t', {'k': 3}), ('t', {'k': 4})]
