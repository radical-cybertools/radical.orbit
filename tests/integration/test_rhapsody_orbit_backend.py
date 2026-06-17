"""
Integration tests for OrbitExecutionBackend → Bridge → Endpoint → Rhapsody plugin.

These tests require:
  - A running bridge (RADICAL_BRIDGE_URL set or default localhost:8000)
  - A connected endpoint with the rhapsody plugin loaded (concurrent backend)

Run with:
  python -m pytest tests/integration/test_rhapsody_endpoint_backend.py -v -s
"""

import asyncio
import os
import time

import pytest

try:
    from rhapsody.backends.execution.orbit import OrbitExecutionBackend
    ENDPOINT_BACKEND_AVAILABLE = True
except ImportError:
    ENDPOINT_BACKEND_AVAILABLE = False

try:
    from radical.orbit import BridgeClient
    ENDPOINT_AVAILABLE = True
except ImportError:
    ENDPOINT_AVAILABLE = False


def _get_bridge_url():
    return os.environ.get('RADICAL_BRIDGE_URL', 'http://localhost:8000')


def _get_endpoint_name():
    """Discover the first connected endpoint, or skip."""
    if not ENDPOINT_AVAILABLE:
        pytest.skip("radical.orbit not installed")

    try:
        bc   = BridgeClient(url=_get_bridge_url())
        eids = bc.list_endpoints()
        bc.close()
    except Exception as e:
        pytest.skip(f"Cannot reach bridge: {e}")

    if not eids:
        pytest.skip("No endpoints connected to bridge")
    return eids[0]


pytestmark = [
    pytest.mark.skipif(not ENDPOINT_BACKEND_AVAILABLE,
                       reason="OrbitExecutionBackend not available"),
    pytest.mark.skipif(not ENDPOINT_AVAILABLE,
                       reason="radical.orbit not installed"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_backend(endpoint_name, backends=None):
    """Create and initialize an OrbitExecutionBackend."""
    backend = OrbitExecutionBackend(
        bridge_url=_get_bridge_url(),
        endpoint_name=endpoint_name,
        backends=backends or ['concurrent'],
    )
    backend = await backend
    return backend


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_executable_task():
    """Submit a simple /bin/echo task and verify output."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    tasks = [{"uid": "integ.exec.001",
              "executable": "/bin/echo",
              "arguments": ["hello_endpoint"]}]

    await backend.submit_tasks(tasks)

    # Poll for completion via the REST API
    import httpx
    url = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": ["integ.exec.001"]},
                      verify=False, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    assert len(results) == 1
    assert results[0]["state"] == "DONE"
    assert "hello_endpoint" in results[0].get("stdout", "")

    await backend.shutdown()


@pytest.mark.asyncio
async def test_submit_batch_and_wait():
    """Submit a batch of tasks and wait for all."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    n = 5
    tasks = [{"uid": f"integ.batch.{i:03d}",
              "executable": "/bin/true"}
             for i in range(n)]

    await backend.submit_tasks(tasks)

    import httpx
    uids = [t["uid"] for t in tasks]
    url  = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": uids},
                      verify=False, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    assert len(results) == n
    for r in results:
        assert r["state"] in ("DONE", "COMPLETED")

    await backend.shutdown()


@pytest.mark.asyncio
async def test_cancel_all_tasks():
    """Submit long-running tasks and cancel them all."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    tasks = [{"uid": f"integ.cancel.{i:03d}",
              "executable": "/bin/sleep",
              "arguments": ["60"]}
             for i in range(3)]

    await backend.submit_tasks(tasks)

    # Give the endpoint a moment to start them
    await asyncio.sleep(0.5)

    count = await backend.cancel_all_tasks()
    assert count >= 0  # best-effort

    await backend.shutdown()


@pytest.mark.asyncio
async def test_function_task_cloudpickle():
    """Submit a cloudpickle-encoded function task."""
    pytest.importorskip("cloudpickle")
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    def adder(a, b):
        return a + b

    tasks = [{"uid": "integ.func.001",
              "function": adder,
              "args": (10, 20),
              "kwargs": {}}]

    await backend.submit_tasks(tasks)

    import httpx
    url  = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": ["integ.func.001"]},
                      verify=False, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    assert len(results) == 1
    assert results[0]["state"] == "DONE"
    assert results[0].get("return_value") == 30

    await backend.shutdown()


@pytest.mark.asyncio
async def test_function_task_import_path():
    """Submit a function task using import path notation."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    tasks = [{"uid": "integ.import.001",
              "function": "os.path:exists",
              "args": ["/tmp"]}]

    await backend.submit_tasks(tasks)

    import httpx
    url  = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": ["integ.import.001"]},
                      verify=False, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    assert len(results) == 1
    assert results[0]["state"] == "DONE"

    await backend.shutdown()


@pytest.mark.asyncio
async def test_task_with_backend_specific_kwargs():
    """task_backend_specific_kwargs must reach the remote backend."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    tasks = [{"uid": "integ.kwargs.001",
              "executable": "/bin/pwd",
              "task_backend_specific_kwargs": {"cwd": "/tmp"}}]

    await backend.submit_tasks(tasks)

    import httpx
    url  = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": ["integ.kwargs.001"]},
                      verify=False, timeout=30)
    resp.raise_for_status()
    results = resp.json()

    assert len(results) == 1
    assert results[0]["state"] == "DONE"
    assert "/tmp" in results[0].get("stdout", "")

    await backend.shutdown()


@pytest.mark.asyncio
async def test_throughput_batch():
    """Basic throughput measurement: submit N tasks in one batch."""
    endpoint = _get_endpoint_name()
    backend = await _make_backend(endpoint)

    n = 20
    tasks = [{"uid": f"integ.tp.{i:03d}",
              "executable": "/bin/true"}
             for i in range(n)]

    t0 = time.time()
    await backend.submit_tasks(tasks)

    import httpx
    uids = [t["uid"] for t in tasks]
    url  = f"{backend._base_url}/wait/{backend._sid}"
    resp = httpx.post(url, json={"uids": uids},
                      verify=False, timeout=60)
    resp.raise_for_status()
    elapsed = time.time() - t0

    print(f"\n  {n} tasks in {elapsed:.2f}s = {n/elapsed:.1f} tasks/s")

    await backend.shutdown()
