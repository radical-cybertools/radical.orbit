
import asyncio
import logging
# pylint: disable=protected-access,unused-import,unused-variable,not-callable,unused-argument
import time
import pytest
from unittest.mock import Mock

from radical.edge.service import EdgeService
import radical.edge as re

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



