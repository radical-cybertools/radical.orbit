import pytest
import httpx
from unittest.mock import MagicMock, patch

from radical.edge.client import BridgeClient, EdgeClient, PluginClient
from radical.edge.plugin_base import Plugin

@patch('httpx.Client.post')
@patch('httpx.Client.get')
def test_bridge_client(mock_get, mock_post):
    # Setup mock responses
    mock_post.return_value.is_error = False
    mock_post.return_value.json.return_value = {
        'data': {
            'edges': {
                'edge1': {'plugins': {}},
                'edge2': {'plugins': {}}
            }
        }
    }
    
    bc = BridgeClient(url="http://test")
    edges = bc.list_edges()
    assert edges == ['edge1', 'edge2']

    edge_client = bc.get_edge_client("edge1")
    assert edge_client._edge_id == "edge1"
    bc.close()

class DummyPluginClient(PluginClient):
    pass

class DummyPlugin(Plugin):
    plugin_name = "dummy"
    client_class = DummyPluginClient


@patch('httpx.Client.post')
def test_edge_client_get_plugin(mock_post):
    Plugin._registry["dummy"] = DummyPlugin
    
    mock_post.side_effect = [
        # First post is to edge/list
        MagicMock(is_error=False, json=lambda: {
            'data': {
                'edges': {
                    'edge1': {
                        'plugins': {
                            'dummy': {'namespace': '/edge1/dummy'}
                        }
                    }
                }
            }
        }),
        # Second post is to register_session
        MagicMock(is_error=False, json=lambda: {'sid': 'test_sid'})
    ]

    bc = BridgeClient(url="http://test")
    ec = bc.get_edge_client("edge1")
    
    plugin_client = ec.get_plugin("dummy")
    assert isinstance(plugin_client, DummyPluginClient)
    assert plugin_client.sid == "test_sid"
    
    # Test unregister behavior
    mock_post_unregister = MagicMock(is_error=False)
    mock_post.side_effect = [mock_post_unregister]
    plugin_client.unregister_session()
    assert plugin_client.sid is None


# ---------------------------------------------------------------------------
# BridgeClient — URL validation
# ---------------------------------------------------------------------------

def test_bridge_client_no_url_raises(tmp_path, monkeypatch):
    """No CLI arg, no env, no file at ``~/.radical/edge/bridge.url``
    → ValueError.  Redirect the resolver's file path to a tmp dir so
    we don't accidentally pick up the dev's own bridge.url."""
    from radical.edge import utils
    monkeypatch.delenv("RADICAL_BRIDGE_URL", raising=False)
    monkeypatch.setattr(utils, 'URL_FILE', tmp_path / 'bridge.url')

    with pytest.raises(ValueError, match="Bridge URL required"):
        BridgeClient()


# ---------------------------------------------------------------------------
# BridgeClient — callback registration
# ---------------------------------------------------------------------------

def test_register_callback_stores_and_starts_listener():
    bc = BridgeClient(url="http://test")
    calls = []
    cb = lambda e, p, t, d: calls.append((e, p, t, d))

    with patch.object(bc, '_ensure_listener'):
        bc.register_callback(callback=cb)

    assert (None, None, None) in bc._callbacks
    assert cb in bc._callbacks[(None, None, None)]
    bc.close()


def test_register_callback_with_filters():
    bc = BridgeClient(url="http://test")
    cb = lambda e, p, t, d: None

    with patch.object(bc, '_ensure_listener'):
        bc.register_callback(edge_id="hpc1", plugin_name="psij",
                             topic="job_status", callback=cb)

    assert ("hpc1", "psij", "job_status") in bc._callbacks
    bc.close()


def test_register_callback_none_callback_raises():
    bc = BridgeClient(url="http://test")
    with pytest.raises(ValueError, match="callback is required"):
        bc.register_callback(callback=None)
    bc.close()


def test_unregister_callback_removes_it():
    bc = BridgeClient(url="http://test")
    cb = lambda e, p, t, d: None

    with patch.object(bc, '_ensure_listener'):
        bc.register_callback(callback=cb)
        bc.unregister_callback(callback=cb)

    assert cb not in bc._callbacks.get((None, None, None), [])
    bc.close()


def test_register_topology_callback():
    bc = BridgeClient(url="http://test")
    cb = lambda edges: None

    with patch.object(bc, '_ensure_listener'):
        bc.register_topology_callback(cb)

    assert cb in bc._topology_callbacks
    bc.close()


def test_unregister_topology_callback():
    bc = BridgeClient(url="http://test")
    cb = lambda edges: None

    with patch.object(bc, '_ensure_listener'):
        bc.register_topology_callback(cb)
        bc.unregister_topology_callback(cb)

    assert cb not in bc._topology_callbacks
    bc.close()


# ---------------------------------------------------------------------------
# _dispatch_notification
# ---------------------------------------------------------------------------

def test_dispatch_notification_wildcard():
    bc = BridgeClient(url="http://test")
    received = []
    cb = lambda e, p, t, d: received.append((e, p, t, d))
    bc._callbacks[(None, None, None)] = [cb]

    bc._dispatch_notification("edge1", "psij", "job_status", {"x": 1})

    assert received == [("edge1", "psij", "job_status", {"x": 1})]
    bc.close()


def test_dispatch_notification_exact_match():
    bc = BridgeClient(url="http://test")
    received = []
    cb = lambda e, p, t, d: received.append(t)
    bc._callbacks[("edge1", "psij", "job_status")] = [cb]

    bc._dispatch_notification("edge1", "psij", "job_status", {})
    bc._dispatch_notification("edge2", "psij", "job_status", {})  # should not match

    assert received == ["job_status"]
    bc.close()


def test_dispatch_notification_no_match_no_crash():
    bc = BridgeClient(url="http://test")
    bc._callbacks[("edgeX", "plugin", "topic")] = [lambda e, p, t, d: None]
    # Different edge — should not raise, just no match
    bc._dispatch_notification("edge_other", "plugin", "topic", {})
    bc.close()


def test_dispatch_notification_callback_exception_logged():
    """Callback that raises should not propagate — logged as error."""
    bc = BridgeClient(url="http://test")
    def bad_cb(e, p, t, d): raise RuntimeError("oops")
    bc._callbacks[(None, None, None)] = [bad_cb]
    # Should not raise
    bc._dispatch_notification("e", "p", "t", {})
    bc.close()


# ---------------------------------------------------------------------------
# EdgeClient.list_plugins
# ---------------------------------------------------------------------------

@patch('httpx.Client.post')
def test_edge_client_list_plugins(mock_post):
    mock_post.return_value = MagicMock(
        is_error=False,
        json=lambda: {
            'data': {
                'edges': {
                    'edge1': {
                        'plugins': {
                            'sysinfo': {'namespace': '/edge1/sysinfo'},
                            'psij':    {'namespace': '/edge1/psij'},
                        }
                    }
                }
            }
        }
    )
    bc = BridgeClient(url="http://test")
    ec = bc.get_edge_client("edge1")
    plugins = ec.list_plugins()
    assert 'sysinfo' in plugins
    assert 'psij' in plugins
    bc.close()


# ---------------------------------------------------------------------------
# PluginClient — notification helpers
# ---------------------------------------------------------------------------

def test_plugin_client_register_notification_no_bridge_raises():
    client = PluginClient(MagicMock(), "/base")
    with pytest.raises(RuntimeError, match="Missing edge tracking"):
        client.register_notification_callback(lambda e, p, t, d: None)


def test_plugin_client_close_with_session_calls_unregister():
    mock_http = MagicMock()
    mock_http.post.return_value = MagicMock(is_error=False)
    client = PluginClient(mock_http, "/base",
                          bridge_client=None, edge_id="e1", plugin_name="p1")
    client._sid = "sid-abc"
    client.close()
    # unregister_session should have been called (POST to unregister)
    mock_http.post.assert_called_once()
