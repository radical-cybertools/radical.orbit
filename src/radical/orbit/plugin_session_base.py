"""
Base class for plugin sessions.

Provides common functionality for session lifecycle, state tracking,
and notification callbacks.
"""

import asyncio
import logging

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Mapping, \
                   Optional

if TYPE_CHECKING:
    from .plugin_base import Plugin

log = logging.getLogger('radical.orbit')

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


class PluginSession:
    """
    Base class for plugin sessions.

    Provides session ID / state tracking, a reusable status poller, and a
    notification path back to consumers.

    Sending notifications
    ---------------------
    A session notifies through its parent plugin, injected as ``_plugin``
    by ``Plugin._create_session``::

        def start_task(self, task_id):
            ...
            self.notify("task_status", {"task_id": task_id,
                                        "status":  "running"})

    ``notify`` forwards to ``self._plugin._dispatch_notify`` (which works from
    sync/async contexts and background threads); it is a no-op when the
    session has no plugin (e.g. in tests).  Notifications reach clients over
    SSE at the broker's ``/events`` endpoint as
    ``{endpoint, plugin, topic, data}``.
    """

    def __init__(self, sid: str):
        """
        Initialize a plugin session.

        Args:
            sid: The unique session ID.
        """
        self._sid: str = sid
        self._active: bool = True
        # Plugin reference, injected by Plugin._create_session(); use notify().
        self._plugin: Optional["Plugin"] = None
        self._status_poller_task: Optional[asyncio.Task] = None

    @property
    def sid(self) -> str:
        """Return the session ID."""
        return self._sid

    @property
    def is_active(self) -> bool:
        """Return whether the session is active."""
        return self._active

    def notify(self, topic: str, data: dict) -> None:
        """Send a notification to consumers via the parent plugin.

        A no-op when the session has no plugin (e.g. built directly in a
        test).  Safe from sync/async contexts and background threads.
        """
        if self._plugin is not None:
            self._plugin._dispatch_notify(topic, data)

    def start_status_poller(
        self, *,
        interval:    float,
        items:       Callable[[], Mapping[Any, Any]],
        is_terminal: Callable[[Any], bool],
        fetch:       Callable[[Any, Any], Awaitable[Optional[Any]]],
        to_payload:  Callable[[Any, Any, Any], dict],
        topic:       str,
        name:        Optional[str] = None,
        max_failures: int = 5,
    ) -> "asyncio.Task":
        """Start a background poller that emits ``topic`` notifications.

        A single reusable loop for the globus / iri / psij pattern: every
        ``interval`` seconds, poll each non-terminal item and notify on a
        state change, until the session goes inactive or nothing is left to
        watch.  Idempotent — a second call while one is running returns the
        live task.

        Args:
            interval:     Seconds between sweeps.
            items:        Returns the live ``{key: item}`` mapping to watch.
            is_terminal:  ``True`` when an item needs no further polling.
            fetch:        ``await fetch(key, item)`` polls one item; returns a
                          truthy *result* when the state changed (updating the
                          item in place), or ``None`` to skip notification.
            to_payload:   ``to_payload(key, item, result)`` builds the
                          notification ``data`` dict.
            topic:        Notification topic string.
            name:         Log label (defaults to ``topic``).
            max_failures: Log at WARNING once this many consecutive ``fetch``
                          failures accumulate — so a permanently-failing
                          poller (e.g. an expiring token) is visible.

        Returns:
            The background ``asyncio.Task``.
        """
        if self._status_poller_task is not None \
                and not self._status_poller_task.done():
            return self._status_poller_task

        label = name or topic

        async def _run() -> None:
            fails = 0
            while True:
                try:
                    await asyncio.sleep(interval)
                    if not self._active:
                        break
                    active = {k: v for k, v in dict(items()).items()
                              if not is_terminal(v)}
                    if not active:
                        break
                    for key, item in active.items():
                        try:
                            result = await fetch(key, item)
                        except Exception as exc:
                            fails += 1
                            if fails % max_failures == 0:
                                log.warning(
                                    "[%s] status poll failing (%d in a row): %s",
                                    label, fails, exc)
                            else:
                                log.debug("[%s] poll error for %s: %s",
                                          label, key, exc)
                            continue
                        fails = 0
                        if result is not None:
                            self.notify(topic, to_payload(key, item, result))
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    log.debug("[%s] poller loop error: %s", label, exc)

        self._status_poller_task = asyncio.create_task(_run())
        return self._status_poller_task

    def stop_status_poller(self) -> None:
        """Cancel the background status poller if one is running."""
        task = getattr(self, '_status_poller_task', None)
        if task is not None and not task.done():
            task.cancel()
        self._status_poller_task = None

    async def close(self) -> Dict[str, Any]:
        """
        Close this plugin session.

        Returns:
            An empty dictionary indicating successful closure.
        """
        self.stop_status_poller()
        self._active = False
        return {}

    def _check_active(self) -> None:
        """
        Check if the session is active.

        Raises:
            RuntimeError: If the session is closed.
        """
        if not self._active:
            raise RuntimeError("session is closed")
