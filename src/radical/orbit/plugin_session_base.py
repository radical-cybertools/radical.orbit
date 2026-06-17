"""
Base class for plugin sessions.

Provides common functionality for session lifecycle, state tracking,
and notification callbacks.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from .plugin_base import Plugin

__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


class PluginSession:
    """
    Base class for plugin sessions.

    Provides common functionality for all plugin sessions including:
    - Session ID management
    - Session state tracking
    - Echo service for testing
    - Session validation
    - Notification callbacks

    Sending Notifications
    ---------------------
    Sessions can send real-time notifications to connected clients via
    the `_plugin` reference.  The plugin is automatically injected by
    the parent Plugin when the session is created.

    Example usage in a session method::

        def start_task(self, task_id: str):
            # ... start the task ...

            if self._plugin:
                self._plugin._dispatch_notify("task_status", {
                    "task_id": task_id,
                    "status": "running",
                    "progress": 0
                })

    `_dispatch_notify`:
    - Takes two arguments: topic (str) and data (dict)
    - Works from both sync and async contexts
    - Works from background threads (uses thread-safe scheduling)
    - `_plugin` is None if the session was not created by a Plugin (e.g., in tests)

    Notifications are delivered to clients via SSE at the bridge's
    `/events` endpoint. The notification payload includes:
    - endpoint: Name of the endpoint that sent the notification
    - plugin: Name of the plugin that sent the notification
    - topic: The topic string passed to _dispatch_notify
    - data: The data dict passed to _dispatch_notify
    """

    def __init__(self, sid: str):
        """
        Initialize a plugin session.

        Args:
            sid: The unique session ID.
        """
        self._sid: str = sid
        self._active: bool = True
        # Plugin reference, injected by Plugin._create_session().
        # Use: self._plugin._dispatch_notify(topic, data)
        self._plugin: Optional["Plugin"] = None

    @property
    def sid(self) -> str:
        """Return the session ID."""
        return self._sid

    @property
    def is_active(self) -> bool:
        """Return whether the session is active."""
        return self._active

    async def close(self) -> Dict[str, Any]:
        """
        Close this plugin session.

        Returns:
            An empty dictionary indicating successful closure.
        """
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

