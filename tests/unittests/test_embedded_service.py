
import asyncio
import logging
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
import ssl
import time
import pytest
from unittest.mock import Mock

from radical.edge.service import EdgeService
import radical.edge as re
import radical.edge.service as service_mod
import radical.edge.utils as edge_utils

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)


@pytest.mark.asyncio
async def test_embedded_service_async_init():
    """Test async service initialization and plugin loading."""

    # Mock plugin class
    # Subclassing automatically registers it if plugin_name is set
    class MockPlugin(re.Plugin):
        plugin_name = "mock_plugin"
        session_class = Mock()
        def __init__(self, app):
            super().__init__(app, 'mock_plugin')

    # EdgeService now loads registered plugins automatically
    service = EdgeService(bridge_url="ws://localhost:0")

    assert 'mock_plugin' in service._plugins
    assert isinstance(service._plugins['mock_plugin'], MockPlugin)

    # Verify direct-dispatch route table has plugin routes
    assert any('/mock_plugin/' in pat.pattern
               for _, pat, _, _ in service._direct_routes)


@pytest.mark.asyncio
async def test_embedded_service_run_stop():
    """Test service run/stop cycle (integration-like but mocked ws)."""

    service = EdgeService(bridge_url="ws://localhost:0")

    # Create task for service.run()
    task = asyncio.create_task(service.run())

    # Let it start
    await asyncio.sleep(0.1)

    # Stop it
    service.stop()

    try:
        await asyncio.wait_for(task, timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Service did not stop in time")
    except asyncio.CancelledError:
        pass  # Expected on stop()
    except Exception:
        # Expected if mocked ws fails, but run() catches exceptions
        # run() swallows connection errors and retries.
        # But stop() should break the loop.
        pass


def test_embedded_service_sync_background():
    """Test synchronous background execution."""

    # Use a guaranteed-unresolvable host so the connect attempt fails
    # before any HTTP exchange — keeps the reconnect-backoff sleep
    # short enough that ``stop()`` propagates within the test's
    # join timeout.
    service = EdgeService(bridge_url="ws://no-such-host.invalid:1")

    # Start background thread
    service.start_background()

    time.sleep(0.1)

    assert service._thread.is_alive()

    service.stop()
    service._thread.join(timeout=5.0)

    assert not service._thread.is_alive()


@pytest.mark.asyncio
async def test_embedded_service_tls_hostname_fallback(monkeypatch, tmp_path, caplog):
    """Hostname verification failure logs a warning and retries without it."""

    cert = tmp_path / 'bridge_cert.pem'
    cert.write_text('dummy cert')

    monkeypatch.setattr(edge_utils, 'resolve_bridge_cert',
                        lambda cli=None: (cert, 'cli'))

    ssl_contexts = []

    class FakeSSLContext:
        def __init__(self):
            self.check_hostname = True
            self.verify_mode    = None
            self.loaded         = []

        def load_verify_locations(self, path):
            self.loaded.append(path)

    def fake_create_default_context():
        ctx = FakeSSLContext()
        ssl_contexts.append(ctx)
        return ctx

    monkeypatch.setattr(service_mod.ssl, 'create_default_context',
                        fake_create_default_context)

    service = EdgeService(bridge_url="https://bridge.example:443")

    class FakeWS:
        async def send(self, _msg):
            return None

        async def recv(self):
            await asyncio.sleep(60)

    class FakeConnect:
        def __init__(self, ssl_ctx):
            self._ssl_ctx = ssl_ctx

        async def __aenter__(self):
            if self._ssl_ctx.check_hostname:
                raise ssl.SSLCertVerificationError("Hostname mismatch")
            service._stop_event.set()
            return FakeWS()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(_url, **kwargs):
        return FakeConnect(kwargs['ssl'])

    monkeypatch.setattr(service_mod.websockets, 'connect', fake_connect)

    with caplog.at_level(logging.WARNING, logger="radical.edge"):
        await service.run()

    assert len(ssl_contexts) == 2
    assert ssl_contexts[0].check_hostname is True
    assert ssl_contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert ssl_contexts[0].loaded == [str(cert)]
    assert ssl_contexts[1].check_hostname is False
    assert ssl_contexts[1].verify_mode == ssl.CERT_REQUIRED
    assert ssl_contexts[1].loaded == [str(cert)]
    assert "TLS hostname validation failed" in caplog.text
    assert "Continuing with hostname validation disabled" in caplog.text

