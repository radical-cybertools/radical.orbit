
# pylint: disable=protected-access,unused-import,unused-variable,unused-argument

import os
import base64
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from radical.edge.plugin_staging import PluginStaging, StagingSession


def test_plugin_staging_init():
    """Test plugin initialization and route registration."""
    app = FastAPI()
    plugin = PluginStaging(app)

    assert plugin.instance_name == 'staging'
    assert plugin.namespace == '/staging'

    route_pats = [p.pattern for _, p, _, _ in app.state.direct_routes]
    ns = plugin.namespace.lstrip('/')
    assert any(f'{ns}/put/' in p for p in route_pats)
    assert any(f'{ns}/get/' in p for p in route_pats)
    assert any(f'{ns}/list/' in p for p in route_pats)


def test_staging_session_put_creates_parent_dirs(tmp_path):
    """Test that put_file creates parent directories."""
    session = StagingSession("test-sid")

    # Create a deeply nested path
    nested_path = tmp_path / "a" / "b" / "c" / "file.txt"
    content = b"test content"
    content_b64 = base64.b64encode(content).decode('ascii')

    import asyncio
    result = asyncio.run(
        session.put_file(str(nested_path), content_b64)
    )

    assert result['path'] == str(nested_path)
    assert result['size'] == len(content)
    assert nested_path.exists()
    assert nested_path.read_bytes() == content


def test_staging_session_put_success(tmp_path):
    """Test successful file upload."""
    session = StagingSession("test-sid")

    target = tmp_path / "uploaded.txt"
    content = b"hello world"
    content_b64 = base64.b64encode(content).decode('ascii')

    import asyncio
    result = asyncio.run(
        session.put_file(str(target), content_b64)
    )

    assert result['path'] == str(target)
    assert result['size'] == len(content)
    assert target.read_bytes() == content


def test_staging_session_put_target_exists_raises(tmp_path):
    """Test that put_file raises FileExistsError if target exists."""
    session = StagingSession("test-sid")

    # Create existing file
    target = tmp_path / "existing.txt"
    target.write_text("already here")

    content_b64 = base64.b64encode(b"new content").decode('ascii')

    import asyncio
    with pytest.raises(FileExistsError) as exc_info:
        asyncio.run(
            session.put_file(str(target), content_b64)
        )

    assert "already exists" in str(exc_info.value)


def test_staging_session_put_relative_path_raises(tmp_path):
    """Test that put_file raises ValueError for relative paths."""
    session = StagingSession("test-sid")

    content_b64 = base64.b64encode(b"content").decode('ascii')

    import asyncio
    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            session.put_file("relative/path.txt", content_b64)
        )

    assert "absolute" in str(exc_info.value)


def test_staging_session_get_success(tmp_path):
    """Test successful file download."""
    session = StagingSession("test-sid")

    # Create source file
    source = tmp_path / "source.txt"
    content = b"file content here"
    source.write_bytes(content)

    import asyncio
    result = asyncio.run(
        session.get_file(str(source))
    )

    assert result['path'] == str(source)
    assert result['size'] == len(content)
    decoded = base64.b64decode(result['content'])
    assert decoded == content


def test_staging_session_get_not_found_raises(tmp_path):
    """Test that get_file raises FileNotFoundError if source doesn't exist."""
    session = StagingSession("test-sid")

    missing = tmp_path / "nonexistent.txt"

    import asyncio
    with pytest.raises(FileNotFoundError) as exc_info:
        asyncio.run(
            session.get_file(str(missing))
        )

    assert "not found" in str(exc_info.value)


def test_staging_session_get_relative_path_raises(tmp_path):
    """Test that get_file raises ValueError for relative paths."""
    session = StagingSession("test-sid")

    import asyncio
    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            session.get_file("relative/path.txt")
        )

    assert "absolute" in str(exc_info.value)


@pytest.mark.asyncio
async def test_put_endpoint(tmp_path):
    """Test PUT endpoint via HTTP."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Upload file
    target = tmp_path / "endpoint_test.txt"
    content = b"endpoint test content"
    content_b64 = base64.b64encode(content).decode('ascii')

    resp = client.post(f"{plugin.namespace}/put/{sid}", json={
        "filename": str(target),
        "content" : content_b64
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['path'] == str(target)
    assert data['size'] == len(content)
    assert target.read_bytes() == content


@pytest.mark.asyncio
async def test_put_endpoint_conflict(tmp_path):
    """Test PUT endpoint returns 409 if target exists."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    # Create existing file
    target = tmp_path / "existing.txt"
    target.write_text("existing")

    content_b64 = base64.b64encode(b"new").decode('ascii')

    resp = client.post(f"{plugin.namespace}/put/{sid}", json={
        "filename": str(target),
        "content" : content_b64
    })
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_get_endpoint(tmp_path):
    """Test GET endpoint via HTTP."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Create source file
    source = tmp_path / "source.txt"
    content = b"source content"
    source.write_bytes(content)

    resp = client.post(f"{plugin.namespace}/get/{sid}", json={
        "filename": str(source)
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['path'] == str(source)
    assert data['size'] == len(content)
    assert base64.b64decode(data['content']) == content


@pytest.mark.asyncio
async def test_get_endpoint_not_found(tmp_path):
    """Test GET endpoint returns 404 if source doesn't exist."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    missing = tmp_path / "missing.txt"

    resp = client.post(f"{plugin.namespace}/get/{sid}", json={
        "filename": str(missing)
    })
    assert resp.status_code == 404


def test_staging_session_list_success(tmp_path):
    """Test successful directory listing."""
    session = StagingSession("test-sid")

    # Create some files and directories
    (tmp_path / "file1.txt").write_text("content1")
    (tmp_path / "file2.txt").write_text("content2content2")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "nested.txt").write_text("nested")

    import asyncio
    result = asyncio.run(
        session.list_dir(str(tmp_path))
    )

    assert result['path'] == str(tmp_path)
    entries = result['entries']

    # Should have 3 entries: file1.txt, file2.txt, subdir
    assert len(entries) == 3

    # Check entries are sorted
    names = [e['name'] for e in entries]
    assert names == sorted(names)

    # Check file entry
    file1 = next(e for e in entries if e['name'] == 'file1.txt')
    assert file1['type'] == 'file'
    assert file1['size'] == 8  # len("content1")

    # Check directory entry
    subdir = next(e for e in entries if e['name'] == 'subdir')
    assert subdir['type'] == 'dir'
    assert subdir['size'] is None


def test_staging_session_list_empty_dir(tmp_path):
    """Test listing an empty directory."""
    session = StagingSession("test-sid")

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    import asyncio
    result = asyncio.run(
        session.list_dir(str(empty_dir))
    )

    assert result['path'] == str(empty_dir)
    assert result['entries'] == []


def test_staging_session_list_not_found_raises(tmp_path):
    """Test that list_dir raises FileNotFoundError if directory doesn't exist."""
    session = StagingSession("test-sid")

    missing = tmp_path / "nonexistent"

    import asyncio
    with pytest.raises(FileNotFoundError) as exc_info:
        asyncio.run(
            session.list_dir(str(missing))
        )

    assert "not found" in str(exc_info.value)


def test_staging_session_list_not_a_directory_raises(tmp_path):
    """Test that list_dir raises NotADirectoryError for files."""
    session = StagingSession("test-sid")

    # Create a file
    file_path = tmp_path / "file.txt"
    file_path.write_text("content")

    import asyncio
    with pytest.raises(NotADirectoryError) as exc_info:
        asyncio.run(
            session.list_dir(str(file_path))
        )

    assert "Not a directory" in str(exc_info.value)


def test_staging_session_list_relative_path_raises(tmp_path):
    """Test that list_dir raises ValueError for relative paths."""
    session = StagingSession("test-sid")

    import asyncio
    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            session.list_dir("relative/path")
        )

    assert "absolute" in str(exc_info.value)


@pytest.mark.asyncio
async def test_list_endpoint(tmp_path):
    """Test LIST endpoint via HTTP."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    assert resp.status_code == 200
    sid = resp.json()['sid']

    # Create some files
    (tmp_path / "a.txt").write_text("aaa")
    (tmp_path / "b.txt").write_text("bbbbb")
    (tmp_path / "subdir").mkdir()

    resp = client.post(f"{plugin.namespace}/list/{sid}", json={
        "path": str(tmp_path)
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data['path'] == str(tmp_path)
    assert len(data['entries']) == 3


@pytest.mark.asyncio
async def test_list_endpoint_not_found(tmp_path):
    """Test LIST endpoint returns 404 if directory doesn't exist."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    missing = tmp_path / "missing_dir"

    resp = client.post(f"{plugin.namespace}/list/{sid}", json={
        "path": str(missing)
    })
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_endpoint_not_a_directory(tmp_path):
    """Test LIST endpoint returns 400 if path is not a directory."""
    app = FastAPI()
    plugin = PluginStaging(app)
    client = TestClient(app)

    # Register session
    resp = client.post(f"{plugin.namespace}/register_session")
    sid = resp.json()['sid']

    # Create a file
    file_path = tmp_path / "file.txt"
    file_path.write_text("content")

    resp = client.post(f"{plugin.namespace}/list/{sid}", json={
        "path": str(file_path)
    })
    assert resp.status_code == 400
    assert "Not a directory" in resp.json()['detail']


# ---------------------------------------------------------------------------
# _validate_path
# ---------------------------------------------------------------------------

def test_validate_path_relative_raises():
    """Relative path must be rejected."""
    session = StagingSession("sid-val-1")
    with pytest.raises(ValueError, match="absolute"):
        session._validate_path("relative/path/file.txt")


def test_validate_path_outside_allowed_raises(tmp_path):
    """Path not under $HOME or /tmp must be rejected."""
    import tempfile, os
    session = StagingSession("sid-val-2")
    # Create a temporary dir that is NOT under HOME or /tmp
    # We'll manipulate _ALLOWED_BASES directly to test the logic
    original = StagingSession._ALLOWED_BASES[:]
    StagingSession._ALLOWED_BASES = ["/nonexistent/base"]
    try:
        with pytest.raises(ValueError, match="escapes"):
            session._validate_path(str(tmp_path / "file.txt"))
    finally:
        StagingSession._ALLOWED_BASES = original


def test_validate_path_valid_tmp(tmp_path):
    """Path within /tmp must be accepted."""
    import tempfile
    session = StagingSession("sid-val-3")
    # tmp_path is under /tmp on most systems; if not, patch _ALLOWED_BASES
    real_tmp = os.path.realpath('/tmp')
    real_path = os.path.realpath(str(tmp_path))
    if not real_path.startswith(real_tmp + os.sep) and real_path != real_tmp:
        # Patch to allow this path
        original = StagingSession._ALLOWED_BASES[:]
        StagingSession._ALLOWED_BASES = [os.path.dirname(real_path)]
        try:
            result = session._validate_path(str(tmp_path / "file.txt"))
            assert os.path.isabs(result)
        finally:
            StagingSession._ALLOWED_BASES = original
    else:
        result = session._validate_path(str(tmp_path / "file.txt"))
        assert os.path.isabs(result)


def test_check_target_not_exists_raises(tmp_path):
    """_check_target_not_exists must raise FileExistsError if file exists."""
    session = StagingSession("sid-val-4")
    existing = tmp_path / "existing.txt"
    existing.write_text("data")
    with pytest.raises(FileExistsError, match="already exists"):
        session._check_target_not_exists(str(existing))


def test_check_target_not_exists_ok(tmp_path):
    """_check_target_not_exists must not raise for a non-existent path."""
    session = StagingSession("sid-val-5")
    session._check_target_not_exists(str(tmp_path / "new_file.txt"))  # no raise


# ---------------------------------------------------------------------------
# StagingClient — no-session guard
# ---------------------------------------------------------------------------

def test_staging_client_put_no_session(tmp_path):
    """StagingClient.put() must raise if no session is active."""
    import httpx
    from radical.edge.plugin_staging import StagingClient
    http = httpx.Client(base_url="http://fake")
    client = StagingClient(http, "/staging")
    with pytest.raises(RuntimeError, match="session"):
        client.put(str(tmp_path / "src.txt"), str(tmp_path / "dst.txt"))


def test_staging_client_put_local_not_found(tmp_path):
    """StagingClient.put() raises FileNotFoundError for missing local file."""
    import httpx
    from radical.edge.plugin_staging import StagingClient
    http = httpx.Client(base_url="http://fake")
    client = StagingClient(http, "/staging")
    client._sid = "fake-sid"
    with pytest.raises(FileNotFoundError):
        client.put(str(tmp_path / "nonexistent.txt"), str(tmp_path / "dst.txt"))
