"""
Integration tests for remote data backends: OrbitDataBackend → Broker →
Endpoint → Rhapsody plugin → rhapsody RedisDataBackend.

These tests require:
  - A running broker (RADICAL_ORBIT_BROKER_URL set or default localhost:8000)
  - A connected endpoint with the rhapsody plugin loaded (concurrent backend)
    and a rhapsody that provides data backends (``get_data_backend``)
  - ``redis-server`` on the endpoint's PATH

Run with:
  python -m pytest tests/integration/test_rhapsody_data_backend.py -v -s
"""

import asyncio
import os
import socket

import pytest

try:
    from rhapsody.backends.data import OrbitDataBackend
    from rhapsody.backends.execution.orbit import OrbitExecutionBackend
    DATA_BACKEND_AVAILABLE = True
except ImportError:
    DATA_BACKEND_AVAILABLE = False

try:
    from radical.orbit import EndpointRuntime
    ENDPOINT_AVAILABLE = True
except ImportError:
    ENDPOINT_AVAILABLE = False


def _get_broker_url():
    return os.environ.get('RADICAL_ORBIT_BROKER_URL', 'http://localhost:8000')


def _get_endpoint_name():
    """Discover the first connected endpoint, or skip."""
    try:
        rt   = EndpointRuntime(broker_url=_get_broker_url())
        rt.start(wait=True)
        eids = [n for n, info in rt.topology().items()
                if n != rt.name and info.get('role') not in ('broker', 'consumer',
                                                             'engine')]
        rt.stop()
    except Exception as e:
        pytest.skip(f"Cannot reach broker: {e}")

    if not eids:
        pytest.skip("No endpoints connected to broker")
    return eids[0]


def _redis_ping(descriptor: str) -> bytes:
    host, port = descriptor.rsplit(':', 1)
    with socket.create_connection((host, int(port)), timeout=5) as s:
        s.sendall(b'PING\r\n')
        return s.recv(64)


pytestmark = [
    pytest.mark.skipif(not DATA_BACKEND_AVAILABLE,
                       reason="rhapsody OrbitDataBackend not available"),
    pytest.mark.skipif(not ENDPOINT_AVAILABLE,
                       reason="radical.orbit not installed"),
]


@pytest.mark.asyncio
async def test_remote_redis_lifecycle():
    exe = OrbitExecutionBackend(broker_url=_get_broker_url(),
                                endpoint_name=_get_endpoint_name(),
                                backends=['concurrent'])
    data = OrbitDataBackend(exe, 'redis')
    try:
        await data.start()
        assert await data.ready()

        (endpoint,) = data.endpoints
        assert endpoint.kind == 'redis'
        # endpoint and test share a host here, so the store is reachable
        reply = await asyncio.to_thread(_redis_ping, endpoint.serialize())
        assert reply.startswith(b'+PONG')

        # a second store with the same name is refused, a new name works
        with pytest.raises(Exception):
            await OrbitDataBackend(exe, 'redis').start()
        other = await OrbitDataBackend(exe, 'redis', name='redis2').start()
        await other.shutdown()

        await data.shutdown()
        with pytest.raises(OSError):
            await asyncio.to_thread(_redis_ping, endpoint.serialize())
    finally:
        await data.shutdown()
        await exe.shutdown()
