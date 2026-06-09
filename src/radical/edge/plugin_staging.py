
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import os
import base64
import binascii
import logging

from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from .plugin_base import Plugin
from .plugin_session_base import PluginSession
from .client import PluginClient

log = logging.getLogger("radical.edge")


class StagingSession(PluginSession):
    """
    Staging session (Service-side).

    Provides methods to transfer files between client and edge filesystems.
    """

    # Allowed base directories for file operations (resolved via realpath).
    # All requested paths must reside under one of these.
    _ALLOWED_BASES = [
        os.path.realpath(os.path.expanduser('~')),
        os.path.realpath('/tmp'),
    ]

    def __init__(self, sid: str):
        super().__init__(sid)

    def _validate_path(self, path: str) -> str:
        """Validate that path is absolute (or starts with ~) and within an
        allowed base directory.

        Returns the resolved (real) path.

        Raises:
            ValueError: If path is not absolute or escapes allowed directories.
        """
        path = os.path.expanduser(path)
        if not os.path.isabs(path):
            raise ValueError(f"Path must be absolute (or use ~): {path}")
        resolved = os.path.realpath(path)
        for base in self._ALLOWED_BASES:
            if resolved == base or resolved.startswith(base + os.sep):
                return resolved
        raise ValueError(
            f"Path escapes allowed directories: {path} "
            f"(resolves to {resolved})")

    def _ensure_parent_dirs(self, path: str) -> None:
        """Create parent directories if they don't exist."""
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
            log.info("[staging] Created parent directories: %s", parent)

    def _check_target_not_exists(self, path: str) -> None:
        """Raise FileExistsError if path already exists."""
        if os.path.exists(path):
            raise FileExistsError(f"Target already exists: {path}")

    async def put_file(self, filename: str, content_b64: str,
                       overwrite: bool = False) -> dict:
        """
        Write file content to the remote filesystem.

        Args:
            filename: Absolute path (or ~/...) on the edge filesystem
            content_b64: File content as base64-encoded string
            overwrite: If True, replace an existing file silently

        Returns:
            {"path": str, "size": int}

        Raises:
            FileExistsError: If target file already exists and overwrite is False
            PermissionError: If write permission denied
        """
        self._check_active()

        # Validate path is absolute and within allowed directories
        filename = self._validate_path(filename)

        # Check target doesn't exist (unless overwrite requested)
        if not overwrite:
            self._check_target_not_exists(filename)

        # Create parent directories
        self._ensure_parent_dirs(filename)

        # Decode and write content
        try:
            content = base64.b64decode(content_b64)
        except binascii.Error as e:
            raise ValueError(f"invalid base64 content: {e}") from e
        with open(filename, 'wb') as f:
            f.write(content)

        size = os.path.getsize(filename)
        log.info("[staging] PUT %s (%d bytes)", filename, size)

        return {"path": filename, "size": size}

    async def get_file(self, filename: str) -> dict:
        """
        Read file content from the remote filesystem.

        Args:
            filename: Absolute path (or ~/...) on the edge filesystem

        Returns:
            {"path": str, "size": int, "content": str (base64-encoded)}

        Raises:
            FileNotFoundError: If source file does not exist
            PermissionError: If read permission denied
        """
        self._check_active()

        # Validate path is absolute and within allowed directories
        filename = self._validate_path(filename)

        # Check file exists
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File not found: {filename}")

        if not os.path.isfile(filename):
            raise ValueError(f"Not a regular file: {filename}")

        # Read and encode content
        with open(filename, 'rb') as f:
            content = f.read()

        content_b64 = base64.b64encode(content).decode('ascii')
        size = len(content)
        log.info("[staging] GET %s (%d bytes)", filename, size)

        return {"path": filename, "size": size, "content": content_b64}

    async def list_dir(self, path: str) -> dict:
        """
        List contents of a directory on the remote filesystem.

        Args:
            path: Absolute path (or ~/...) to the directory

        Returns:
            {
                "path": str,
                "entries": [
                    {"name": str, "type": "file"|"dir", "size": int|None}
                ]
            }

        Raises:
            FileNotFoundError: If directory does not exist
            NotADirectoryError: If path is not a directory
            PermissionError: If read permission denied
        """
        self._check_active()

        # Validate path is absolute and within allowed directories
        path = self._validate_path(path)

        # Check path exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"Directory not found: {path}")

        if not os.path.isdir(path):
            raise NotADirectoryError(f"Not a directory: {path}")

        entries = []
        for name in sorted(os.listdir(path)):
            full_path = os.path.join(path, name)
            try:
                stat = os.stat(full_path)
                if os.path.isdir(full_path):
                    entries.append({
                        "name": name,
                        "type": "dir",
                        "size": None
                    })
                else:
                    entries.append({
                        "name": name,
                        "type": "file",
                        "size": stat.st_size
                    })
            except (PermissionError, OSError):
                # Skip entries we can't stat
                entries.append({
                    "name": name,
                    "type": "unknown",
                    "size": None
                })

        log.info("[staging] LIST %s (%d entries)", path, len(entries))

        return {"path": path, "entries": entries}


class StagingClient(PluginClient):
    """
    Client-side interface for the Staging plugin.
    """

    def put(self, src: str, tgt: str, overwrite: bool = False) -> dict:
        """
        Upload a local file to the remote edge filesystem.

        Args:
            src: Absolute path on the local (client) filesystem
            tgt: Absolute path (or ~/...) on the remote (edge) filesystem
            overwrite: If True, replace an existing remote file silently

        Returns:
            {"path": str, "size": int}

        Raises:
            FileNotFoundError: If local source file does not exist
            FileExistsError: If remote target file already exists and overwrite is False
            RuntimeError: If no active session
        """
        self._require_session()

        # Validate local source exists
        if not os.path.exists(src):
            raise FileNotFoundError(f"Local file not found: {src}")

        if not os.path.isfile(src):
            raise ValueError(f"Not a regular file: {src}")

        # Read and encode local file
        with open(src, 'rb') as f:
            content = f.read()

        content_b64 = base64.b64encode(content).decode('ascii')

        # Send to edge
        url = self._url(f"put/{self.sid}")
        resp = self._http.post(url, json={
            "filename" : tgt,
            "content"  : content_b64,
            "overwrite": overwrite
        })

        if resp.status_code == 409:
            raise FileExistsError(resp.json().get('detail', 'Target exists'))

        self._raise(resp, f"staging put {src!r} -> {tgt!r}")
        return resp.json()

    def get(self, src: str, tgt: str) -> dict:
        """
        Download a remote file to the local client filesystem.

        Args:
            src: Absolute path (or ~/...) on the remote (edge) filesystem
            tgt: Absolute path on the local (client) filesystem

        Returns:
            {"path": str, "size": int}

        Raises:
            FileNotFoundError: If remote source file does not exist
            FileExistsError: If local target file already exists (client-side)
            RuntimeError: If no active session
        """
        self._require_session()

        # Check local target doesn't exist
        if os.path.exists(tgt):
            raise FileExistsError(f"Local target already exists: {tgt}")

        # Request file from edge
        url = self._url(f"get/{self.sid}")
        resp = self._http.post(url, json={"filename": src})

        if resp.status_code == 404:
            raise FileNotFoundError(resp.json().get('detail', 'File not found'))

        self._raise(resp, f"staging get {src!r} -> {tgt!r}")
        data = resp.json()

        # Create parent directories for local target
        parent = os.path.dirname(tgt)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
            log.info("[staging] Created local parent directories: %s", parent)

        # Decode and write content
        try:
            content = base64.b64decode(data['content'])
        except binascii.Error as e:
            raise ValueError(f"invalid base64 in server response: {e}") from e
        with open(tgt, 'wb') as f:
            f.write(content)

        return {"path": tgt, "size": data['size']}

    def list(self, path: str) -> dict:
        """
        List contents of a directory on the remote edge filesystem.

        Args:
            path: Absolute path (or ~/...) on the remote (edge) filesystem

        Returns:
            {
                "path": str,
                "entries": [
                    {"name": str, "type": "file"|"dir", "size": int|None}
                ]
            }

        Raises:
            FileNotFoundError: If directory does not exist
            NotADirectoryError: If path is not a directory
            RuntimeError: If no active session
        """
        self._require_session()

        url = self._url(f"list/{self.sid}")
        resp = self._http.post(url, json={"path": path})

        if resp.status_code == 404:
            raise FileNotFoundError(resp.json().get('detail', 'Not found'))
        if resp.status_code == 400:
            detail = resp.json().get('detail', '')
            if 'Not a directory' in detail:
                raise NotADirectoryError(detail)
            raise ValueError(detail)

        self._raise(resp, f"staging list {path!r}")
        return resp.json()


class PluginStaging(Plugin):
    """
    Staging plugin for Radical Edge.

    Provides file transfer between client and edge filesystems.
    Supports put (upload) and get (download) operations.
    Parent directories are created automatically.
    Existing files are protected by default (overwrite=False).
    """

    plugin_name   = "staging"
    session_class = StagingSession
    client_class  = StagingClient
    version       = '0.0.1'

    ui_config = {
        "icon"       : "📁",
        "title"      : "File Staging",
        "description": "Transfer files between client and edge filesystems."
    }

    def __init__(self, app: FastAPI):
        """
        Initialize the Staging plugin.
        """
        super().__init__(app, 'staging')

        # Register routes
        self.add_route_post('put/{sid}', self.put_endpoint)
        self.add_route_post('get/{sid}', self.get_endpoint)
        self.add_route_post('list/{sid}', self.list_endpoint)

    async def put_endpoint(self, request: Request) -> dict:
        """
        Upload a file to the edge filesystem.

        Request body:
            {"filename": str, "content": str (base64)}

        Returns:
            {"path": str, "size": int}
        """
        sid  = request.path_params['sid']
        body = await request.json()

        filename    = body.get('filename')
        content_b64 = body.get('content')
        overwrite   = bool(body.get('overwrite', False))

        if not filename:
            raise HTTPException(status_code=400, detail="Missing 'filename'")
        if not content_b64:
            raise HTTPException(status_code=400, detail="Missing 'content'")

        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"Unknown session: {sid}")

        try:
            result = await session.put_file(filename, content_b64, overwrite=overwrite)
            return result
        except FileExistsError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def get_endpoint(self, request: Request) -> dict:
        """
        Download a file from the edge filesystem.

        Request body:
            {"filename": str}

        Returns:
            {"path": str, "size": int, "content": str (base64)}
        """
        sid  = request.path_params['sid']
        body = await request.json()

        filename = body.get('filename')

        if not filename:
            raise HTTPException(status_code=400, detail="Missing 'filename'")

        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"Unknown session: {sid}")

        try:
            result = await session.get_file(filename)
            return result
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def list_endpoint(self, request: Request) -> dict:
        """
        List contents of a directory on the edge filesystem.

        Request body:
            {"path": str}

        Returns:
            {"path": str, "entries": [{"name": str, "type": str, "size": int|null}]}
        """
        sid  = request.path_params['sid']
        body = await request.json()

        path = body.get('path')

        if not path:
            raise HTTPException(status_code=400, detail="Missing 'path'")

        session = self._sessions.get(sid)
        if not session:
            raise HTTPException(status_code=404, detail=f"Unknown session: {sid}")

        try:
            result = await session.list_dir(path)
            return result
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except NotADirectoryError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
