#!/usr/bin/env python3

import asyncio
import base64
import itertools
import json
import logging
import os
import re
import signal
import socket
import ssl

import msgpack
from radical.edge import _prof as rprof

from contextlib import asynccontextmanager
from typing  import Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi import Request, Response, HTTPException

from fastapi.responses       import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses     import StreamingResponse

from starlette.websockets    import WebSocketState

from radical.edge.bridge_plugin_host import BridgePluginHost

log = logging.getLogger("radical.edge.bridge")

BRIDGE_EDGE_NAME = 'bridge'

# Global shutdown event for graceful termination
shutdown_event = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    global bridge_plugin_host
    shutdown_event.clear()

    # Load bridge-hosted plugins if configured via CLI (stored on app.state)
    plugin_spec = getattr(app.state, '_bridge_plugins', None)
    if plugin_spec:
        names = [t.strip() for t in plugin_spec.split(',') if t.strip()]
        if names:
            async def _on_bridge_topology_changed():
                """Called when bridge plugins change (dynamic registration)."""
                if bridge_plugin_host:
                    endpoints['edges'][BRIDGE_EDGE_NAME] = \
                        bridge_plugin_host.get_topology_info()
                    _plugin_ui_module_js.update(
                        bridge_plugin_host.get_ui_modules())
                await broadcast_event('topology', endpoints)
                await broadcast_topology_to_edges()

            bridge_plugin_host = BridgePluginHost(
                names, broadcast_event, BRIDGE_EDGE_NAME,
                on_topology_changed=_on_bridge_topology_changed)
            endpoints['edges'][BRIDGE_EDGE_NAME] = \
                bridge_plugin_host.get_topology_info()
            _plugin_ui_module_js.update(bridge_plugin_host.get_ui_modules())
            log.info('[Bridge] Loaded bridge plugins: %s', names)

    async def _print_url():
        await asyncio.sleep(0.2)  # let uvicorn print its own startup line first
        print(f"[Bridge] URL: {endpoints['bridge'].get('url', 'unknown')}", flush=True)

    asyncio.ensure_future(_print_url())
    yield
    # Shutdown
    log.info("[Bridge] Shutting down...")
    shutdown_event.set()
    # Wake up all SSE clients
    for q in list(clients_sse):
        try:
            await q.put(None)
        except Exception as e:
            log.debug("[Bridge] SSE queue put failed during shutdown: %s", e)
    # Close WebSocket connections
    for edge_name, ws in list(edges.items()):
        try:
            await ws.close(code=1001, reason="Server shutting down")
        except Exception as e:
            log.debug("[Bridge] WebSocket close failed during shutdown: %s", e)
    edges.clear()
    log.info("[Bridge] Shutdown complete")


app = FastAPI(
    title="RADICAL Edge Bridge",
    lifespan=lifespan,
    description="""
RADICAL Edge Bridge - Reverse proxy connecting clients to HPC edge services.

## Overview

The Bridge acts as a public-facing reverse proxy that:
- Accepts HTTP requests from clients
- Forwards them to appropriate Edge services over WebSocket
- Returns responses back to clients
- Broadcasts real-time notifications via Server-Sent Events (SSE)

## Key Endpoints

- `GET /` - Web UI (Edge Explorer)
- `GET /events` - SSE stream for real-time updates
- `GET /edges` - List connected edges and their plugins
- `POST /edge/list` - Detailed edge/plugin topology
- `/{edge_name}/{plugin}/{path}` - Proxied requests to edge plugins
    """,
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# LUCID needs credentials; browsers reject credentials + wildcard origin,
# so we must list allowed origins explicitly.
origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://localhost",
    "https://localhost:8080",
    "https://dev-1.bv-brc.org",
]

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Centralized exception handlers
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle HTTP exceptions with consistent JSON format."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status_code": exc.status_code,
            "detail": exc.detail
        }
    )


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    """Handle ValueError as 400 Bad Request."""
    log.warning("[Bridge] ValueError: %s", exc)
    return JSONResponse(
        status_code=400,
        content={
            "error": True,
            "status_code": 400,
            "detail": str(exc)
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all handler for unexpected exceptions."""
    log.exception("[Bridge] Unhandled exception: %s", exc)
    # In production, don't expose internal error details
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "status_code": 500,
            "detail": "Internal server error"
        }
    )


edges: Dict[str, WebSocket] = {}
pending: Dict[str, tuple] = dict()  # req_id -> (future, edge_name)
pending_lock: asyncio.Lock = asyncio.Lock()

_bridge_prof    = rprof.Profiler('bridge', ns='radical.edge')
_bridge_req_ctr = itertools.count()

# {"bridge": {...},
#  "edges": {edge_name: {"plugins": {...}}}}
endpoints: Dict[str, Any] = {
    "bridge": {},  # URL will be set at startup
    "edges": {}
}

HEARTBEAT_INTERVAL = 20
REQUEST_TIMEOUT    = 45

clients_sse: set = set()

bridge_plugin_host: BridgePluginHost | None = None


async def broadcast_event(topic: str, data: dict):
    msg = json.dumps({"topic": topic, "data": data})
    formatted = f"data: {msg}\n\n"
    for q in list(clients_sse):
        await q.put(formatted)


async def broadcast_topology_to_edges():
    """Broadcast current topology to all connected edges via WebSocket."""
    # Build edge list (just names for simplicity)
    edge_list = {name: {"plugins": list(info.get("plugins", {}).keys())}
                 for name, info in endpoints.get("edges", {}).items()}
    msg = json.dumps({"type": "topology", "edges": edge_list})
    for edge_name, ws in list(edges.items()):
        try:
            if ws.client_state == WebSocketState.CONNECTED:
                await ws.send_text(msg)
        except Exception as e:
            log.warning("[Bridge] Failed to send topology to %s: %s", edge_name, e)

    # Also notify bridge-hosted plugins
    if bridge_plugin_host is not None:
        try:
            await bridge_plugin_host.on_topology_change(edge_list)
        except Exception as e:
            log.warning("[Bridge] Failed topology notify to bridge plugins: %s", e)


async def _send_to_edge(edge_name: str, message, binary: bool = False):

    ws = edges.get(edge_name)
    if not ws or ws.client_state != WebSocketState.CONNECTED:
        raise HTTPException(status_code=503, detail=f"Edge '{edge_name}' not connected")

    if binary:
        await ws.send_bytes(message)
    else:
        await ws.send_text(message)


@app.websocket("/register")
async def register(ws: WebSocket):

    await ws.accept()
    edge_name = None

    # Heartbeat
    async def pinger():
        elapsed = 0
        while not (shutdown_event.is_set()):
            try:
                await asyncio.sleep(1.0)
                elapsed += 1
            except asyncio.CancelledError:
                return
            if elapsed < HEARTBEAT_INTERVAL:
                continue
            elapsed = 0
            if ws.client_state != WebSocketState.CONNECTED:
                return
            try:
                await ws.send_text(json.dumps({"type": "ping"}))
            except Exception as e:
                log.exception("[Bridge] Ping failed for edge: %s", e)
                return

    ping_task = None
    try:

        # start the ping task - it will run as long as the endpoint is connected
        ping_task = asyncio.create_task(pinger())

        while not (shutdown_event.is_set()):
            try:
                raw = await asyncio.wait_for(ws.receive(), timeout=1.0)
            except asyncio.TimeoutError:
                continue  # Check shutdown_event again

            # Detect disconnect
            if raw.get("type") == "websocket.disconnect":
                raise WebSocketDisconnect(raw.get("code", 1000))

            # Binary frame (msgpack) or text frame (JSON)
            try:
                if raw.get("bytes"):
                    _bridge_prof.prof('bridge_deser',
                                      msg='msgpack:%d' % len(raw["bytes"]))
                    data = msgpack.unpackb(raw["bytes"], raw=False)
                else:
                    _bridge_prof.prof('bridge_deser',
                                      msg='json:%d' % len(raw.get("text", "")))
                    data = json.loads(raw.get("text", "{}"))
                _bridge_prof.prof('bridge_deser_done',
                                  uid=data.get('req_id', ''))
            except Exception:
                log.warning("[Bridge] Malformed message from edge '%s'",
                            edge_name or '(unregistered)')
                continue

            if data.get("type") == "pong":
                pass

            elif data.get("type") == "register":
                frame_edge_name = data.get("edge_name")

                if not frame_edge_name:
                    log.warning("[Bridge] Registration missing edge_name")
                    continue

                if frame_edge_name == BRIDGE_EDGE_NAME:
                    log.warning("[Bridge] Edge name '%s' is reserved",
                                frame_edge_name)
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": f"Edge name '{frame_edge_name}' is reserved"
                    }))
                    return

                if frame_edge_name in edges:
                    log.warning("[Bridge] Edge '%s' already connected.", frame_edge_name)
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "message": f"Edge '{frame_edge_name}' already used"
                    }))
                    return

                edge_name = frame_edge_name
                edges[edge_name] = ws
                log.info("[Bridge] Edge '%s' connected", edge_name)
                endpoints["edges"][edge_name] = {
                    "endpoint": data.get("endpoint", {}),
                    "plugins": {},
                }

                for pname, pdata in data.get("plugins", {}).items():
                    js_content = pdata.pop("ui_module", None)
                    if js_content:
                        _plugin_ui_module_js[pname] = js_content
                        log.info("[Bridge] Cached UI module for plugin '%s' from edge '%s'",
                                 pname, edge_name)
                    endpoints["edges"][edge_name]["plugins"][pname] = pdata

                plugin_names = list(endpoints["edges"][edge_name]["plugins"].keys())
                log.info("[Bridge] Edge '%s' registered  plugins=%s", edge_name, plugin_names)

                await broadcast_event("topology", endpoints)
                await broadcast_topology_to_edges()

            elif data.get("type") == "notification":
                await broadcast_event("notification", {
                    "edge": edge_name,
                    "plugin": data.get("plugin"),
                    "topic": data.get("topic"),
                    "data": data.get("data")
                })

            elif data.get("type") == "response":
                req_id = data.get("req_id")
                if not req_id:
                    log.warning("[Bridge] Response from '%s' missing req_id: %s",
                                edge_name, str(data)[:200])
                    continue
                async with pending_lock:
                    entry = pending.pop(req_id, None)

                if entry:
                    fut = entry[0]
                    if not fut.done():
                        fut.set_result(data)

            else:
                # ignore unknown frames
                log.debug("[Bridge] Unknown message type received: %s", data)


    except WebSocketDisconnect:
        pass

    except RuntimeError as e:
        # Starlette raises RuntimeError when WS is closed during receive
        if "not connected" in str(e).lower():
            log.debug("[Bridge] recv interrupted on disconnected edge '%s'",
                      edge_name or '(unknown)')
        else:
            log.exception("[Bridge] Edge connection error: %s", e)

    except Exception as e:
        log.exception("[Bridge] Edge connection error: %s", e)

    finally:

        log.info("[Bridge] Edge disconnected: %s", edge_name)
        if ping_task:
            ping_task.cancel()

        if edge_name:
            # Only unregister if this WS was the active one for the name
            # (Prevent rejected duplicates from killing the valid session)
            if edges.get(edge_name) == ws:
                # Remove from edges first so topology broadcast skips this WS
                del edges[edge_name]

                if edge_name in endpoints["edges"]:
                    log.info("[Bridge] Unregistering edge: %s", edge_name)
                    del endpoints["edges"][edge_name]
                    await broadcast_event("topology", endpoints)
                    await broadcast_topology_to_edges()
            else:
                log.info("[Bridge] Disconnected duplicate/inactive session for: %s", edge_name)

        # Fail in-flight requests for this edge only
        if edge_name:
            async with pending_lock:
                failed = [rid for rid, (fut, ename) in pending.items()
                          if ename == edge_name]
                for rid in failed:
                    fut, _ = pending.pop(rid)
                    if not fut.done():
                        fut.set_exception(
                            HTTPException(503, "Edge disconnected"))


def _strip_headers(request: Request) -> dict:

    to_strip = {"connection", "keep-alive", "proxy-authenticate",
                "proxy-authorization", "te", "trailers",
                "transfer-encoding", "upgrade"}

    ret = {k: v for k, v in request.headers.items()
                         if k.lower() not in to_strip}
    return ret


# some routes are handles here:

@app.get("/events", tags=["Events"])
async def sse_events(request: Request):
    """
    Server-Sent Events stream for real-time notifications.

    Returns a stream of events including:
    - `topology`: Edge/plugin registration changes
    - `notification`: Plugin-specific events (task status, job status, etc.)

    Event format:
    ```
    data: {"topic": "notification", "data": {...}}
    ```
    """
    q = asyncio.Queue()
    clients_sse.add(q)

    # optionally yield current syntax first so they get an immediate state sync
    await q.put(f"data: {json.dumps({'topic': 'topology', 'data': endpoints})}\n\n")

    async def event_generator():
        try:
            while not (shutdown_event.is_set()):
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                try:
                    # Use timeout to periodically check shutdown status
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    if msg is None:  # Shutdown sentinel
                        break
                    yield msg
                except asyncio.TimeoutError:
                    continue  # Check shutdown_event again
        except asyncio.CancelledError:
            log.debug("[Bridge] SSE client cancelled")
        except Exception as e:
            log.exception("[Bridge] SSE client error: %s", e)
        finally:
            clients_sse.discard(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/edge/list", tags=["Discovery"])
async def edge_list(request: Request):
    """
    Get detailed edge and plugin topology.

    Returns the full topology including:
    - Bridge URL
    - All connected edges
    - Plugins registered on each edge with their namespaces
    """
    return JSONResponse({"data": endpoints})


@app.get("/edges", tags=["Discovery"])
async def get_edges():
    """
    List all connected edges with their plugins.

    Returns a summary of connected edges including:
    - Edge names
    - Registered plugins per edge
    - Connection status
    """
    edge_list = []
    for edge_name, edge_data in endpoints.get("edges", {}).items():
        plugins = list(edge_data.get("plugins", {}).keys())
        connected = edge_name in edges or edge_name == BRIDGE_EDGE_NAME
        edge_list.append({
            "name": edge_name,
            "plugins": plugins,
            "connected": connected,
            "plugin_count": len(plugins)
        })
    return JSONResponse({
        "edges": edge_list,
        "total": len(edge_list)
    })


@app.post("/edge/disconnect/{edge_name}", tags=["Management"])
async def disconnect_edge(edge_name: str):
    """
    Shutdown an edge by sending a shutdown command and closing the connection.

    This will cause the edge service to terminate (not reconnect).
    """
    if edge_name == BRIDGE_EDGE_NAME:
        raise HTTPException(status_code=400,
                            detail="Cannot disconnect bridge-hosted plugins")

    if edge_name not in edges:
        raise HTTPException(status_code=404, detail=f"Edge '{edge_name}' not connected")

    ws = edges[edge_name]
    try:
        # Send shutdown command so edge doesn't reconnect
        await ws.send_text(json.dumps({"type": "shutdown", "reason": "Disconnected by user"}))
        await ws.close(code=1000)
    except Exception as e:
        log.warning("[Bridge] Error shutting down edge %s: %s", edge_name, e)

    return JSONResponse({"status": "shutdown", "edge": edge_name})


@app.post("/bridge/terminate", tags=["Management"])
async def terminate_bridge():
    """
    Terminate the bridge process.

    This will shut down the bridge but NOT terminate connected edges.
    Edges will detect the disconnection and may attempt to reconnect
    (to this or another bridge).
    """
    # Schedule shutdown after returning response
    async def delayed_shutdown():
        await asyncio.sleep(0.5)  # Give time for response to be sent
        log.info("[Bridge] Terminating via API request")
        # Send SIGTERM to self to trigger graceful uvicorn shutdown
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.create_task(delayed_shutdown())

    return JSONResponse({
        "status": "terminating",
        "message": "Bridge will terminate shortly. Edges will not be shut down."
    })


# ---------------------------------------------------------------------------
# Edge Submission (stub - PsiJ remote submission not yet implemented)
# ---------------------------------------------------------------------------

@app.post("/edge/submit", tags=["Edge Submission"])
async def submit_tunneled(request: Request):
    """
    Submit a new edge service to a remote resource via PsiJ.

    Expected JSON body:
    {
        "name": "edge-name",
        "resource": "hostname or URL",
        "executor": "slurm|pbs|lsf|local",
        "queue": "partition name",
        "account": "allocation/project",
        "duration": "seconds",
        "node_count": 1
    }

    NOTE: Not yet implemented - PsiJ does not support remote submission.
    """
    raise HTTPException(
        status_code=501,
        detail="Edge submission not implemented - PsiJ remote submission not yet available"
    )


@app.get("/edge/job/{job_id}", tags=["Edge Submission"])
async def get_edge_job_status(job_id: str):
    """
    Get status of a submitted edge job.

    NOTE: Not yet implemented - PsiJ does not support remote submission.
    """
    raise HTTPException(
        status_code=501,
        detail="Edge job status not implemented - PsiJ remote submission not yet available"
    )


@app.post("/edge/job/{job_id}/cancel", tags=["Edge Submission"])
async def cancel_edge_job(job_id: str):
    """
    Cancel a submitted edge job.

    NOTE: Not yet implemented - PsiJ does not support remote submission.
    """
    raise HTTPException(
        status_code=501,
        detail="Edge job cancellation not implemented - PsiJ remote submission not yet available"
    )


@app.get("/edge/jobs", tags=["Edge Submission"])
async def list_edge_jobs():
    """
    List all submitted edge jobs.

    NOTE: Not yet implemented - PsiJ does not support remote submission.
    """
    raise HTTPException(
        status_code=501,
        detail="Edge job listing not implemented - PsiJ remote submission not yet available"
    )


@app.get("/", tags=["UI"], include_in_schema=False)
async def root():
    # Try to find edge_explorer.html via importlib.resources (works with editable installs)
    html_path = None
    try:
        # Python 3.9+ with importlib.resources.files
        from importlib.resources import files
        data_dir = files('radical.edge').joinpath('data')
        candidate = data_dir.joinpath('edge_explorer.html')
        # For editable installs, this returns a Traversable that we can get the path from
        if hasattr(candidate, '__fspath__'):
            html_path = os.fspath(candidate)
        else:
            # For installed packages, we may need to extract to a temp file
            # But typically the path is directly accessible
            html_path = str(candidate)
        if not os.path.exists(html_path):
            html_path = None
    except Exception as e:
        log.debug("[Bridge] importlib.resources lookup failed: %s", e)

    # Fallback: try pkg_resources
    if not html_path:
        try:
            import pkg_resources
            html_path = pkg_resources.resource_filename('radical.edge', 'data/edge_explorer.html')
            if not os.path.exists(html_path):
                html_path = None
        except Exception as e:
            log.debug("[Bridge] pkg_resources lookup failed: %s", e)

    if html_path and os.path.exists(html_path):
        # Disable caching for development - ensures latest version is served
        return FileResponse(html_path, headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        })

    return Response(content="edge_explorer.html not found", status_code=404)


# JS content pushed by edges at registration time: plugin_name -> JS string.
_plugin_ui_module_js: Dict[str, str] = {}


@app.get("/plugins/{filename}", tags=["UI"], include_in_schema=False)
async def serve_plugin(filename: str):
    """Serve plugin UI modules — first from radical.edge's own plugins dir,
    then from paths declared via ui_module on registered plugin classes."""

    # Validate filename (only allow .js files with safe names)
    if not re.match(r'^[a-z_][a-z0-9_.]*\.js$', filename):
        raise HTTPException(status_code=404, detail="Invalid plugin filename")

    plugin_path = None

    # 1. Try radical.edge's own data/plugins/ via importlib.resources
    try:
        from importlib.resources import files
        data_dir = files('radical.edge').joinpath('data').joinpath('plugins')
        candidate = data_dir.joinpath(filename)
        candidate_path = os.fspath(candidate) if hasattr(candidate, '__fspath__') \
                         else str(candidate)
        if os.path.exists(candidate_path):
            plugin_path = candidate_path
    except Exception as e:
        log.debug("[Bridge] importlib.resources plugin lookup failed: %s", e)

    # Fallback: pkg_resources
    if not plugin_path:
        try:
            import pkg_resources
            candidate_path = pkg_resources.resource_filename(
                'radical.edge', f'data/plugins/{filename}'
            )
            if os.path.exists(candidate_path):
                plugin_path = candidate_path
        except Exception as e:
            log.debug("[Bridge] pkg_resources plugin lookup failed: %s", e)

    # 2. Try JS content pushed by edges at registration time
    if not plugin_path:
        plugin_name = filename[:-3]  # strip .js
        js_content = _plugin_ui_module_js.get(plugin_name)
        if js_content:
            return Response(
                js_content,
                media_type="application/javascript",
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0"
                }
            )
        log.warning("[Bridge] No UI module found for plugin '%s'", plugin_name)

    if plugin_path and os.path.exists(plugin_path):
        return FileResponse(
            plugin_path,
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )

    raise HTTPException(status_code=404, detail=f"Plugin '{filename}' not found")


# all other edge routes are forwarded
@app.api_route("/{full_path:path}",
               methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"],
               tags=["Proxy"],
               summary="Proxy requests to edge plugins")
async def proxy(full_path: str, request: Request):
    """
    Proxy HTTP requests to edge services.

    Path format: `/{edge_name}/{plugin_name}/{route}`

    The bridge forwards the request to the specified edge's plugin
    and returns the response.
    """

    # Parse edge_name from path: /edge_name/plugin/...
    parts = full_path.strip('/').split('/', 1)
    if not parts:
        raise HTTPException(status_code=404, detail="Invalid path")

    edge_name = parts[0]

    # Bridge-hosted plugins: dispatch locally, skip the WebSocket proxy path
    if edge_name == BRIDGE_EDGE_NAME and bridge_plugin_host is not None:
        forward_path = '/' + parts[1] if len(parts) > 1 else '/'
        return await bridge_plugin_host.handle_request(
            method       = request.method,
            path         = forward_path,
            headers      = dict(request.headers),
            body_bytes   = await request.body(),
            query_string = str(request.url.query) if request.url.query else '',
        )

    if edge_name not in edges:
        raise HTTPException(status_code=404, detail=f"Edge '{edge_name}' unknown")

    # Path to forward: /plugin/...
    if len(parts) > 1:
        forward_path = '/' + parts[1]
    else:
        forward_path = '/'

    # Prepare body (binary-safe)
    body_bytes = await request.body()
    body       = None
    is_binary  = False

    if body_bytes:
        # Cheap heuristic: if decodable, send as text; else binary WS frame
        try:
            body = body_bytes.decode("utf-8")
        except UnicodeDecodeError:
            is_binary = True

    # Use client-provided request ID or generate one
    req_id = (request.headers.get("x-request-id")
              or 'req.%06d' % next(_bridge_req_ctr))

    _bridge_prof.prof('bridge_recv', uid=req_id,
                      msg='%s %s' % (request.method, forward_path))
    _bridge_prof.prof('bridge_body_prep', uid=req_id,
                      msg=str(len(body_bytes)))

    # Query params handling
    if request.url.query:
        forward_path += f"?{request.url.query}"

    message = {
        "type"     : "request",
        "req_id"   : req_id,
        "method"   : request.method,
        "path"     : forward_path,
        "headers"  : _strip_headers(request),
        "is_binary": is_binary,
        "body"     : body_bytes if is_binary else body,  # raw bytes or text
    }

    fut = asyncio.get_running_loop().create_future()
    async with pending_lock:
        pending[req_id] = (fut, edge_name)

    try:
        _bridge_prof.prof('bridge_ser', uid=req_id)
        if is_binary:
            wire = msgpack.packb(message, use_bin_type=True)
        else:
            wire = json.dumps(message)
        _bridge_prof.prof('bridge_ser_done', uid=req_id, msg=str(len(wire)))

        _bridge_prof.prof('bridge_ws_send', uid=req_id)
        await _send_to_edge(edge_name, wire, binary=is_binary)
        _bridge_prof.prof('bridge_ws_sent', uid=req_id)

    except HTTPException:
        async with pending_lock:
            pending.pop(req_id, None)
        raise

    try:
        resp = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)

    except asyncio.TimeoutError as exc:
        async with pending_lock:
            pending.pop(req_id, None)
        raise HTTPException(status_code=504, detail="Upstream (edge) timeout") from exc

    _bridge_prof.prof('bridge_ws_recv', uid=req_id)

    status    = int(resp.get("status", 502))
    headers   = resp.get("headers") or {}
    resp_body = resp.get("body")

    _bridge_prof.prof('bridge_resp_ser', uid=req_id)

    if resp.get("is_binary"):
        try:
            raw = base64.b64decode(resp_body or b"")
        except Exception as e:
            log.exception("[Bridge] Failed to decode binary response: %s", e)
            raw = b""

        _bridge_prof.prof('bridge_reply', uid=req_id, state=str(status))
        return Response(content=raw, status_code=status, headers=headers)

    else:
        # If content-type hints JSON, send JSONResponse; else plain Response
        content = resp_body or ""
        ctype   = headers.get("content-type", "")

        if "application/json" in ctype:
            try:
                headers = {k.lower(): v for k, v in headers.items()
                                        if  k.lower() != "content-type"}
                # Body may already be parsed (raw JSON embed) or a string
                parsed = content if isinstance(content, (dict, list)) \
                    else json.loads(content)
                _bridge_prof.prof('bridge_reply', uid=req_id, state=str(status))
                return JSONResponse(content=parsed,
                                    status_code=status, headers=headers)

            except Exception as e:
                log.exception("[Bridge] Failed to parse JSON response: %s", e)

        _bridge_prof.prof('bridge_reply', uid=req_id, state=str(status))
        return Response(content=content, status_code=status, headers=headers)


def validate_ssl_config(certfile: str, keyfile: str) -> None:
    """Validate SSL certificate and key files. Exit on error."""

    if not certfile:
        log.error("[Bridge] SSL certificate required. Set RADICAL_BRIDGE_CERT.")
        exit(1)

    if not keyfile:
        log.error("[Bridge] SSL key required. Set RADICAL_BRIDGE_KEY.")
        exit(1)

    if not os.path.exists(certfile):
        log.error("[Bridge] Certificate file not found: %s", certfile)
        exit(1)

    if not os.path.exists(keyfile):
        log.error("[Bridge] Key file not found: %s", keyfile)
        exit(1)

    # Verify certificate and key are valid and match
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile, keyfile)
    except ssl.SSLError as e:
        log.error("[Bridge] Invalid SSL certificate/key: %s", e)
        exit(1)
    except Exception as e:
        log.error("[Bridge] Cannot load SSL certificate/key: %s", e)
        exit(1)

    log.info("[Bridge] SSL certificate validated: %s", certfile)


def main():

    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description='RADICAL Edge Bridge')
    parser.add_argument('--plugins', '-p', default='',
                        help='Comma-separated plugins to host on the bridge '
                             '(default: none). Use "all" for every registered '
                             'plugin. Prefix matching supported.')
    args = parser.parse_args()

    # Stash on app.state so the lifespan handler can pick it up
    app.state._bridge_plugins = args.plugins or ''

    # Custom log filter to suppress CancelledError during shutdown
    class ShutdownFilter(logging.Filter):
        def filter(self, record):
            # Suppress CancelledError messages during graceful shutdown
            msg = str(record.getMessage())
            if 'CancelledError' in msg:
                return False
            # Suppress "Exception in ASGI application" when it's a CancelledError
            if record.exc_info:
                exc = record.exc_info[1]
                if isinstance(exc, asyncio.CancelledError):
                    return False
            return True

    # Apply filter to uvicorn error logger
    logging.getLogger("uvicorn.error").addFilter(ShutdownFilter())

    # Uvicorn config
    host = "0.0.0.0"
    port = 8000
    ssl_certfile = os.environ.get('RADICAL_BRIDGE_CERT')
    ssl_keyfile  = os.environ.get('RADICAL_BRIDGE_KEY')

    # Validate SSL configuration - always required
    validate_ssl_config(ssl_certfile, ssl_keyfile)

    # Construct bridge URL based on config
    def _get_outbound_ip():
        """Return the IP this host uses for outbound internet connections."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('1.1.1.1', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return None

    if host == "0.0.0.0":
        fqdn = socket.getfqdn()
        if fqdn and fqdn not in ('localhost', 'localhost.localdomain') and '.' in fqdn:
            advertise_host = fqdn
        else:
            advertise_host = _get_outbound_ip() or socket.gethostname()
    else:
        advertise_host = host
    bridge_url = f"https://{advertise_host}:{port}/"

    endpoints["bridge"]["url"] = bridge_url

    uvicorn.run(app,
                host=host,
                port=port,
                reload=False,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                log_level="info",
                ws_max_size=10 * 1024 * 1024,       # 10 MB
                ws_per_message_deflate=True,        # compress WS frames
                timeout_graceful_shutdown=3)         # Force exit after 3s


if __name__ == "__main__":
    main()

