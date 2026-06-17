
import asyncio
import logging
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
import ssl
import time
import pytest
from unittest.mock import Mock

from radical.orbit.service import EndpointService
import radical.orbit as re
import radical.orbit.service as service_mod
import radical.orbit.utils as endpoint_utils

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

    # EndpointService now loads registered plugins automatically
    service = EndpointService(bridge_url="ws://localhost:0")

    assert 'mock_plugin' in service._plugins
    assert isinstance(service._plugins['mock_plugin'], MockPlugin)

    # Verify direct-dispatch route table has plugin routes
    assert any('/mock_plugin/' in pat.pattern
               for _, pat, _, _ in service._direct_routes)


@pytest.mark.asyncio
async def test_embedded_service_run_stop():
    """Test service run/stop cycle (integration-like but mocked ws)."""

    service = EndpointService(bridge_url="ws://localhost:0")

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
    service = EndpointService(bridge_url="ws://no-such-host.invalid:1")

    # Start background thread
    service.start_background()

    time.sleep(0.1)

    assert service._thread.is_alive()

    service.stop()
    service._thread.join(timeout=5.0)

    assert not service._thread.is_alive()


@pytest.mark.asyncio
async def test_embedded_service_tls_hostname_fallback(monkeypatch, tmp_path, caplog):
    """Name/IP verification failure with a *pinned* cert logs a warning and
    retries with name validation disabled (development convenience)."""

    cert = tmp_path / 'bridge_cert.pem'
    cert.write_text('dummy cert')

    monkeypatch.setattr(endpoint_utils, 'resolve_bridge_cert',
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

    service = EndpointService(bridge_url="https://bridge.example:443")

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

    # The 'radical.orbit' logger is configured with propagate=False (see
    # logging_config.configure_logging), so caplog's root handler never
    # sees its records.  Attach caplog's handler to that logger directly
    # for the duration of the call instead of relying on propagation.
    re_logger = logging.getLogger("radical.orbit")
    re_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.WARNING, logger="radical.orbit"):
            await service.run()
    finally:
        re_logger.removeHandler(caplog.handler)

    assert len(ssl_contexts) == 2
    assert ssl_contexts[0].check_hostname is True
    assert ssl_contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert ssl_contexts[0].loaded == [str(cert)]
    assert ssl_contexts[1].check_hostname is False
    assert ssl_contexts[1].verify_mode == ssl.CERT_REQUIRED
    assert ssl_contexts[1].loaded == [str(cert)]
    assert "TLS name/IP validation failed" in caplog.text
    assert "name validation DISABLED" in caplog.text

