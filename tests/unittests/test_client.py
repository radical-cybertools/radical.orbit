"""Tests for the surviving client surface: the transport-agnostic
:class:`PluginClient` base every plugin ``client_class`` subclasses.

The broker-era ``BrokerClient``/``EndpointClient`` (HTTP + SSE) are gone — the
participant runtime (``EndpointRuntime``, tested in ``test_runtime.py``) is the
consumer now.  ``PluginClient`` itself stays: it rides an ``httpx``-shaped
transport (real HTTP for plugin ``TestClient`` tests; a runtime shim over the
WebSocket for the consumer).
"""

import pytest
from unittest.mock import MagicMock

from radical.orbit.client import PluginClient


# ---------------------------------------------------------------------------
# PluginClient — notification helpers
# ---------------------------------------------------------------------------

def test_plugin_client_register_notification_no_broker_raises():
    client = PluginClient(MagicMock(), "/base")
    with pytest.raises(RuntimeError, match="Missing endpoint tracking"):
        client.register_notification_callback(lambda e, p, t, d: None)


def test_plugin_client_register_notification_delegates_to_client():
    notif = MagicMock()
    client = PluginClient(MagicMock(), "/base",
                          broker_client=notif, endpoint_id="e1",
                          plugin_name="p1")
    cb = lambda e, p, t, d: None
    client.register_notification_callback(cb, topic="job_status")
    notif.register_callback.assert_called_once_with(
        endpoint_id="e1", plugin_name="p1", topic="job_status", callback=cb)


# ---------------------------------------------------------------------------
# PluginClient — session lifecycle
# ---------------------------------------------------------------------------

def test_plugin_client_register_session_stores_sid():
    mock_http = MagicMock()
    mock_http.post.return_value = MagicMock(
        is_error=False, json=lambda: {'sid': 'sid-abc'})
    client = PluginClient(mock_http, "/base",
                          endpoint_id="e1", plugin_name="p1")
    client.register_session()
    assert client.sid == "sid-abc"


def test_plugin_client_close_with_session_calls_unregister():
    mock_http = MagicMock()
    mock_http.post.return_value = MagicMock(is_error=False)
    client = PluginClient(mock_http, "/base",
                          broker_client=None, endpoint_id="e1", plugin_name="p1")
    client._sid = "sid-abc"
    client.close()
    # unregister_session should have been called (POST to unregister)
    mock_http.post.assert_called_once()


def test_plugin_client_touch_posts_the_touch_route():
    mock_http = MagicMock()
    mock_http.post.return_value = MagicMock(is_error=False)
    client = PluginClient(mock_http, "/base",
                          endpoint_id="e1", plugin_name="p1")
    with pytest.raises(Exception):
        client.touch()                      # no session yet
    client._sid = "sid-abc"
    client.touch()
    assert mock_http.post.call_args[0][0].endswith("touch/sid-abc")


def test_plugin_client_ttl_is_sent_in_the_legacy_form_too():
    '''Endpoints of the previous release accept ttl only with
    lifetime='ttl', and ttl=0 only as lifetime='persistent'.'''
    assert PluginClient._session_payload(None, None, 60) == \
        {'lifetime': 'ttl', 'ttl': 60}
    assert PluginClient._session_payload(None, None, 0) == \
        {'lifetime': 'persistent'}
    assert PluginClient._session_payload('s', None, None) == {'sid': 's'}
