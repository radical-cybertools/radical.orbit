'''
XGFabric Plugin for Radical Edge.

Orchestrates CFDaAI workflows across multiple HPC clusters. Provides:
- Configuration management (load/save workflow configs)
- Workflow execution (start/stop/status)
- Real-time progress notifications via SSE

The plugin runs on a local edge and communicates with remote edges
(UCSB, Perlmutter) via the bridge.
'''

import asyncio
import csv
import dataclasses
import json
import logging
import os
import re
import shutil
import subprocess
import urllib.parse
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from fastapi import FastAPI, HTTPException, Request

from .plugin_session_base import PluginSession
from .plugin_base import Plugin
from .client import PluginClient

log = logging.getLogger("radical.edge")


# -----------------------------------------------------------------------------
# Configuration Dataclasses
# -----------------------------------------------------------------------------

@dataclass
class ResourceConfig:
    """Resource configuration — bridge connection and per-cluster scheduler settings."""
    name: str = "default"
    bridge_url: str = "https://localhost:8000"
    bridge_cert: Optional[str] = None
    # Per-edge overrides used when submitting pilots via psij.
    # Keys are edge names; values are dicts with any of:
    #   queue, account, duration, nodes, executor, workflow_path, custom_attributes
    cluster_configs: Dict[str, Dict] = field(default_factory=dict)


def dict_to_resource_config(d: Dict) -> 'ResourceConfig':
    """Convert dict to ResourceConfig, ignoring unknown fields."""

    valid = {f.name for f in dataclasses.fields(ResourceConfig)}
    return ResourceConfig(**{k: v for k, v in d.items() if k in valid})


@dataclass
class WorkflowConfig:
    """Workflow configuration — task templates and execution parameters."""
    name: str = "default"
    description: str = ""

    # Paths
    local_workspace: str = "/tmp/xgfabric_workspace"

    # CSPOT
    cspot_woof_url: str = "woof://128.111.45.61/davisstations/daviscupsout"
    cspot_limit: int = 10

    # Workflow
    num_simulations: int = 16
    batch_size: int = 4
    train_models: List[str] = field(default_factory=lambda: ["pcr", "pinn", "fno"])

    # Task templates
    # Simulation task: {workflow_path}, {sim_output_dir}, {wind_speed}, {sim_id}, {wind_dir}
    simulation_task: Optional[Dict] = None
    # Training tasks: model-name → task spec; placeholders: {workflow_path},
    #   {sensor_dir}, {sim_dir}, {output_dir}, {model}
    training_tasks: Dict[str, Dict] = field(default_factory=dict)
    # Evaluation task: {workflow_path}, {sensor_file}, {eval_output}
    evaluation_task: Optional[Dict] = None

    # Test/debug: skip CSPOT and generate synthetic sensor data
    mock_sensor_data: bool = False


def config_to_dict(cfg: WorkflowConfig) -> Dict:
    """Convert config to JSON-serializable dict."""
    return asdict(cfg)


def dict_to_config(d: Dict) -> WorkflowConfig:
    """Convert dict to WorkflowConfig, filtering unknown fields."""


    # Get valid field names from the dataclass
    valid_fields = {f.name for f in dataclasses.fields(WorkflowConfig)}

    # Filter to only valid fields
    filtered = {k: v for k, v in d.items() if k in valid_fields}

    # Convert string numbers to int where needed
    for int_field in ('cspot_limit', 'num_simulations', 'batch_size'):
        if int_field in filtered and isinstance(filtered[int_field], str):
            filtered[int_field] = int(filtered[int_field])

    return WorkflowConfig(**filtered)


# -----------------------------------------------------------------------------
# Workflow State
# -----------------------------------------------------------------------------

@dataclass
class ClusterStatus:
    """Status of a single cluster."""
    name: str
    edge_name: str
    cluster_type: str  # 'immediate' or 'allocate'
    has_gpu: bool = False
    online: bool = False
    tasks_running: int = 0
    pilot_job_id: Optional[str] = None
    pilot_status: Optional[str] = None  # 'pending', 'running', 'completed', 'failed'


@dataclass
class WorkflowState:
    """Runtime state of a workflow execution."""
    status: str = 'idle'  # idle, running, completed, failed
    phase: str = ''
    progress: int = 0
    message: str = ''
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error: Optional[str] = None
    active_cluster: Optional[str] = None
    completed_simulations: int = 0
    total_simulations: int = 0
    current_batch: int = 0
    total_batches: int = 0
    # Pilot job tracking
    pilot_jobs: Dict[str, str] = field(default_factory=dict)
    # Cluster status
    immediate_clusters: List[Dict] = field(default_factory=list)
    allocate_clusters: List[Dict] = field(default_factory=list)
    # Execution log (most recent first)
    log: List[Dict] = field(default_factory=list)
    # Config info
    config_name: Optional[str] = None
    config_dir: Optional[str] = None


# -----------------------------------------------------------------------------
# Session
# -----------------------------------------------------------------------------

class XGFabricSession(PluginSession):
    """
    XGFabric session - manages workflow configuration and execution.
    """

    def __init__(self, sid: str, workdir: Optional[str] = None, edge_name: Optional[str] = None,
                 bridge_url: Optional[str] = None, bridge_cert: Optional[str] = None):
        super().__init__(sid)
        default_workdir = os.environ.get('XGFABRIC_WORKDIR') or os.getcwd()
        self._workdir = Path(workdir or default_workdir)
        self._workdir.mkdir(parents=True, exist_ok=True)
        self._config_dir = self._workdir / 'configs'
        self._config_dir.mkdir(exist_ok=True)

        self._edge_name = edge_name or 'local'
        self._bridge_url = bridge_url
        self._bridge_cert = bridge_cert
        self._http        = httpx.AsyncClient(verify=self._verify())
        self._connected_edges: Dict[str, Any] = {}  # Cached connected edges
        self._current_config: Optional[WorkflowConfig] = None
        self._current_resource_config: Optional[ResourceConfig] = None
        self._state = WorkflowState()
        self._workflow_task: Optional[asyncio.Task] = None
        self._cancel_requested = False

        # Bridge client for communicating with other edges
        self._bc = None
        # Cache of resolved home dirs per edge (populated on first use)
        self._homedir_cache: Dict[str, str] = {}
        # Active rhapsody client + pending task UIDs for the current batch (for cleanup)
        self._pending_tasks: Optional[tuple] = None  # (rhapsody_client, set[uid])

    def _verify(self) -> Any:
        """Return SSL verification argument for httpx calls."""
        return self._bridge_cert if self._bridge_cert else False

    async def _http_get(self, url: str, **kwargs) -> Any:
        """Run httpx.get using the session AsyncClient."""
        return await self._http.get(url, **kwargs)

    async def _http_post(self, url: str, **kwargs) -> Any:
        """Run httpx.post using the session AsyncClient."""
        return await self._http.post(url, **kwargs)

    async def _resolve_path(self, edge_name: str, path: str) -> str:
        """Expand a leading '~' to the home directory on the remote edge."""
        if not path.startswith('~'):
            return path
        if edge_name not in self._homedir_cache:
            url = f"{self._bridge_url.rstrip('/')}/{edge_name}/sysinfo/homedir"
            try:
                resp = await self._http_get(url, timeout=5)
                self._homedir_cache[edge_name] = resp.json().get('homedir', '~')
            except Exception as e:
                log.warning("[XGFabric] _resolve_path(%s): failed — %s", edge_name, e)
                return path
        return path.replace('~', self._homedir_cache[edge_name], 1)

    def update_connected_edges(self, edges: Dict[str, Any]):
        """Update the cached list of connected edges."""
        log.debug("[XGFabric] Session %s: topology update — %d edges: %s",
                  self._sid, len(edges), list(edges.keys()))
        self._connected_edges = edges

    # -------------------------------------------------------------------------
    # Config Directory Management
    # -------------------------------------------------------------------------

    async def get_config_dir(self) -> Dict:
        """Get current config directory."""
        return {'path': str(self._config_dir.parent)}

    async def set_config_dir(self, path: str) -> Dict:
        """Set config directory."""
        new_dir = Path(path)
        if not new_dir.exists():
            raise HTTPException(status_code=400, detail=f"Directory not found: {path}")
        self._workdir = new_dir
        self._config_dir = new_dir / 'configs'
        self._config_dir.mkdir(exist_ok=True)
        return {'path': str(self._workdir), 'status': 'ok'}

    # -------------------------------------------------------------------------
    # Config Management
    # -------------------------------------------------------------------------

    async def list_configs(self) -> List[Dict]:
        """List all saved configurations."""
        configs = []
        for f in self._config_dir.glob('*.json'):
            try:
                with open(f) as fp:
                    data = json.load(fp)
                    configs.append({
                        'name': f.stem,
                        'description': data.get('description', ''),
                        'modified': datetime.fromtimestamp(f.stat().st_mtime).isoformat()
                    })
            except Exception as e:
                log.warning("Failed to read config %s: %s", f, e)
        return sorted(configs, key=lambda x: x['name'])

    async def load_config(self, name: str) -> Dict:
        """Load a workflow config by name, path, or builtin alias ('default', 'test')."""
        _builtins = {
            'default': 'xgfabric_workflow_default.json',
            'test':    'xgfabric_workflow_test.json',
        }
        if name in _builtins:
            return self._load_builtin_config(_builtins[name])

        p = Path(name)
        if p.is_absolute() or p.exists():
            config_file = p if p.suffix else p.with_suffix('.json')
        else:
            config_file = self._config_dir / (name if name.endswith('.json') else f'{name}.json')
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")

        with open(config_file) as f:
            data = json.load(f)
        self._current_config = dict_to_config(data)
        return data

    async def save_config(self, data: Dict) -> Dict:
        """Save a configuration."""
        name = data.get('name', 'default')
        if not name:
            raise HTTPException(status_code=400, detail="Config name is required")

        # Convert to WorkflowConfig (filters out UI-only fields) and back to dict
        self._current_config = dict_to_config(data)
        clean_data = config_to_dict(self._current_config)

        config_file = self._config_dir / f'{name}.json'
        with open(config_file, 'w') as f:
            json.dump(clean_data, f, indent=2)

        return {'status': 'saved', 'name': name}

    async def delete_config(self, name: str) -> Dict:
        """Delete a configuration."""
        config_file = self._config_dir / f'{name}.json'
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        config_file.unlink()
        return {'status': 'deleted', 'name': name}

    @staticmethod
    def _load_builtin_config(filename: str) -> Dict:
        """Load a built-in config JSON from the package data directory."""
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        with open(os.path.join(data_dir, filename)) as f:
            return json.load(f)

    # -------------------------------------------------------------------------
    # Workflow Control
    # -------------------------------------------------------------------------

    async def get_status(self) -> Dict:
        """Get current workflow status including cluster info."""
        # Always update config_dir
        self._state.config_dir = str(self._workdir)
        # Update config name if loaded
        if self._current_config:
            self._state.config_name = self._current_config.name

        # Query connected edges from bridge (if not running a workflow)
        if self._state.status != 'running':
            immediate, allocate = await self._get_connected_edges()
            log.info("[XGFabric] get_status: immediate=%s  allocate=%s",
                     [c['name'] for c in immediate],
                     [c['name'] for c in allocate])
            self._state.immediate_clusters = immediate
            self._state.allocate_clusters  = allocate
        else:
            log.info("[XGFabric] get_status: workflow running — skipping cluster refresh "
                     "(immediate=%s  allocate=%s)",
                     [c['name'] for c in self._state.immediate_clusters],
                     [c['name'] for c in self._state.allocate_clusters])

        return asdict(self._state)

    async def _get_connected_edges(self) -> tuple[List[Dict], List[Dict]]:
        """Return (immediate, allocate) cluster lists from cache or bridge query.

        Edges with the queue_info plugin AND a working scheduler go into
        allocate_clusters; all others go into immediate_clusters.
        """
        rc = self._current_resource_config
        def _cluster(edge_name: str) -> Dict:
            base = {'name': edge_name, 'edge_name': edge_name,
                    'has_gpu': False, 'online': True, 'tasks_running': 0}
            if rc:
                base.update(rc.cluster_configs.get(edge_name, {}))
            return base

        async def _classify(edges_info: Dict) -> tuple[List[Dict], List[Dict]]:
            """Classify edges_info dict into (immediate, allocate)."""
            immediate, allocate = [], []
            for edge_name, edge_info in edges_info.items():
                plugins = edge_info.get('plugins', [])
                # ucsb edges are always immediate (no batch scheduler available)
                if 'ucsb' in edge_name:
                    log.info("[XGFabric]   %s -> immediate (ucsb)", edge_name)
                    immediate.append(_cluster(edge_name))
                elif 'queue_info' in plugins:
                    log.info("[XGFabric]   %s -> allocate (queue_info enabled)", edge_name)
                    allocate.append(_cluster(edge_name))
                else:
                    log.info("[XGFabric]   %s -> immediate (no scheduler)", edge_name)
                    immediate.append(_cluster(edge_name))
            return immediate, allocate

        # Use cached topology if available (populated by on_topology_change)
        if self._connected_edges:
            log.info("[XGFabric] _get_connected_edges: using cached topology (%d edges)",
                     len(self._connected_edges))
            return await _classify(self._connected_edges)

        # Fallback: query bridge for full plugin info and classify the same way
        log.info("[XGFabric] _get_connected_edges: no cached topology — querying bridge "
                 "(bridge_url=%s)", self._bridge_url)
        if not self._bridge_url:
            return [], []

        try:

            resp = await self._http_post(
                f"{self._bridge_url.rstrip('/')}/edge/list", timeout=5)
            data       = resp.json().get('data', {})
            edges_info = {name: {'plugins': list(info.get('plugins', {}).keys())}
                          for name, info in data.get('edges', {}).items()}
            log.info("[XGFabric] _get_connected_edges: bridge returned %d edges: %s",
                     len(edges_info), list(edges_info.keys()))
            return await _classify(edges_info)

        except Exception as e:
            log.info("[XGFabric] _get_connected_edges: bridge query failed — %s", e)
            return [], []

    async def start_workflow(self, workflow: str = '__default__',
                             resource: str = '__default__') -> Dict:
        """Start workflow execution."""
        log.info("[XGFabric] start_workflow: workflow=%s  resource=%s  status=%s",
                 workflow, resource, self._state.status)
        if self._state.status == 'running':
            raise HTTPException(status_code=409, detail="Workflow already running")

        # Load workflow config (handles 'default', 'test', and user configs)
        self._current_config = dict_to_config(await self.load_config(workflow))

        # Load resource config
        self._current_resource_config = dict_to_resource_config(
            await self._load_resource_config(resource))

        cfg = self._current_config
        self._state = WorkflowState(
            status='running',
            phase='initializing',
            start_time=datetime.now(timezone.utc).isoformat(),
            config_name=cfg.name,
            config_dir=str(self._workdir),
            total_simulations=cfg.num_simulations,
            total_batches=(cfg.num_simulations + cfg.batch_size - 1) // cfg.batch_size,
            immediate_clusters=self._state.immediate_clusters,
            allocate_clusters=self._state.allocate_clusters,
        )
        self._cancel_requested = False

        # Start workflow in background
        self._workflow_task = asyncio.create_task(self._run_workflow())

        return {'status': 'started', 'config': cfg.name}

    async def _load_resource_config(self, name: str) -> Dict:
        """Load a resource config by name or builtin alias ('default', 'test')."""
        _builtins = {
            'default':    'xgfabric_resource_default.json',
            '__default__': 'xgfabric_resource_default.json',
            'test':        'xgfabric_resource_test.json',
            '__test__':    'xgfabric_resource_test.json',
        }
        if name in _builtins:
            return self._load_builtin_config(_builtins[name])

        p = Path(name)
        config_file = (p if p.suffix else p.with_suffix('.json')) \
            if (p.is_absolute() or p.exists()) \
            else self._config_dir / (name if name.endswith('.json') else f'{name}.json')
        if not config_file.exists():
            raise HTTPException(status_code=404, detail=f"Resource config '{name}' not found")
        with open(config_file) as f:
            return json.load(f)

    async def stop_workflow(self) -> Dict:
        """Stop running workflow."""
        if self._state.status != 'running':
            raise HTTPException(status_code=409, detail="No workflow running")

        self._cancel_requested = True
        self._state.message = "Cancellation requested..."

        if self._workflow_task:
            self._workflow_task.cancel()
            try:
                await self._workflow_task
            except asyncio.CancelledError:
                pass

        return {'status': 'stopped'}

    # -------------------------------------------------------------------------
    # Workflow Execution
    # -------------------------------------------------------------------------

    async def _run_workflow(self):
        """Execute the complete workflow."""
        try:
            await self._execute_workflow()
            self._state.status = 'completed'
            self._state.phase = 'done'
            self._state.message = 'Workflow completed successfully'
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            self._notify_state()

        except asyncio.CancelledError:
            self._state.status = 'failed'
            self._state.error = 'Workflow cancelled by user'
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            await self._cleanup_on_failure()
            self._notify_state()

        except Exception as e:
            log.exception("Workflow failed: %s", e)
            self._state.status = 'failed'
            self._state.error = str(e)
            self._state.end_time = datetime.now(timezone.utc).isoformat()
            await self._cleanup_on_failure()
            self._notify_state()

    async def _execute_workflow(self):
        """Main workflow execution logic."""
        if not self._current_config:
            raise RuntimeError("No active workflow configuration")
        cfg = self._current_config

        rc = self._current_resource_config
        log.info("[XGFabric] _execute_workflow: starting — session.bridge_url=%s",
                 self._bridge_url)

        # Always prefer the live bridge URL from the session over the resource config —
        # the saved URL may be stale (e.g. localhost vs public IP).
        bridge_url  = self._bridge_url  or (rc.bridge_url  if rc else 'https://localhost:8000')
        bridge_cert = self._bridge_cert or (rc.bridge_cert if rc else None)
        log.info("[XGFabric] _execute_workflow: effective bridge_url=%s  cert=%s",
                 bridge_url, bridge_cert)
        self._update_state('connecting', 'Connecting to bridge...')
        from .client import BridgeClient
        self._bc = BridgeClient(url=bridge_url, cert=bridge_cert)

        # Discover which clusters are connected right now (always live, ignores config)
        self._update_state('verifying', 'Verifying edges...')
        immediate_list, allocate_list = await self._get_connected_edges()
        log.info("[XGFabric] _execute_workflow: immediate=%s  allocate=%s",
                 [c['name'] for c in immediate_list],
                 [c['name'] for c in allocate_list])

        immediate = immediate_list[0] if immediate_list else None
        allocate  = allocate_list[0]  if allocate_list  else None

        if not immediate:
            raise RuntimeError(
                f"No immediate cluster is connected "
                f"(online immediate: {[c['edge_name'] for c in immediate_list]}, "
                f"online allocate: {[c['edge_name'] for c in allocate_list]})"
            )

        # Phase 1: Data acquisition
        self._update_state('data_acquisition', 'Fetching sensor data from CSPOT...')
        workspace = Path(cfg.local_workspace)
        workspace.mkdir(parents=True, exist_ok=True)
        sensor_csv = await self._acquire_sensor_data(workspace)

        # Phase 2: Submit pilot (async)
        if allocate:
            self._update_state('pilot_submit', f"Submitting pilot job to {allocate['name']}...")
            pilot_id = await self._submit_pilot(allocate, bridge_url)
            self._state.pilot_jobs[allocate['name']] = pilot_id

        # Phase 3: Stage data and run simulations
        self._update_state('staging', f"Staging data to {immediate['name']}...")
        await self._stage_sensor_data(immediate, sensor_csv)

        self._update_state('simulations', f"Running simulations on {immediate['name']}...")
        self._state.total_simulations = cfg.num_simulations
        sim_results = await self._run_simulations(immediate, sensor_csv, allocate)

        # Phase 4: Migration decision
        self._update_state('migration_check', 'Checking for GPU cluster...')
        active_cluster = immediate
        if allocate and await self._is_edge_online(allocate):
            self._update_state('migration', f"Migrating to {allocate['name']}...")
            await self._migrate_data(immediate, allocate, sim_results)
            active_cluster = allocate

        self._state.active_cluster = active_cluster['name']

        # Phase 5: Training
        self._update_state('training', f"Running ML training on {active_cluster['name']}...")
        await self._run_training(active_cluster, sim_results)

        # Phase 6: Evaluation
        self._update_state('evaluation', f"Running evaluation on {active_cluster['name']}...")
        await self._run_evaluation(active_cluster)

        # Done
        self._state.progress = 100

    def _update_state(self, phase: str, message: str, progress: Optional[int] = None):
        """Update workflow state, add log entry, and send notification."""
        self._state.phase = phase
        self._state.message = message
        if progress is not None:
            self._state.progress = progress
        self._add_log(message)
        self._notify_state()

    def _add_log(self, message: str, level: str = 'info'):
        """Add entry to execution log (most recent first, max 50 entries)."""
        entry = {
            'time':    datetime.now(timezone.utc).strftime('%H:%M:%S'),
            'level':   level,
            'message': message,
        }
        self._state.log.insert(0, entry)
        if len(self._state.log) > 50:
            self._state.log = self._state.log[:50]

    def _log_task_error(self, label: str, t: dict, task_spec: Optional[Dict] = None):
        """Log a task failure immediately and notify clients."""
        state     = t.get('state', '?')
        exit_code = t.get('exit_code')
        exception = (t.get('exception') or '').strip()
        stdout    = (t.get('stdout')    or '').strip()
        stderr    = (t.get('stderr')    or '').strip()

        msg = f"FAILED {label}: state={state} exit={exit_code}"
        if task_spec:
            cmd = task_spec.get('executable', '')
            args = ' '.join(str(a) for a in task_spec.get('arguments', []))
            msg += f" | cmd: {cmd} {args}"[:120]
        if exception:
            msg += f" | exception: {exception[:200]}"
        if stderr:
            msg += f" | stderr: {stderr[:200]}"
        if stdout and not stderr:
            msg += f" | stdout: {stdout[:200]}"

        log.warning("[XGFabric] %s", msg)
        self._add_log(msg, level='error')
        self._state.error = msg
        self._notify_state()

    def _notify_state(self):
        """Send state notification via SSE."""
        log.info("[XGFabric] _notify_state: phase=%s  status=%s  immediate=%s  allocate=%s",
                 self._state.phase, self._state.status,
                 [c['name'] for c in self._state.immediate_clusters],
                 [c['name'] for c in self._state.allocate_clusters])
        if self._plugin:
            self._plugin._dispatch_notify('workflow_status', asdict(self._state))

    async def _is_edge_online(self, cluster: Dict) -> bool:
        """Check if cluster's child edge is online."""
        if not self._bc:
            raise RuntimeError("No active bridge connection")
        edge_name = cluster.get('child_edge_name') or cluster['edge_name']
        edges = await asyncio.to_thread(self._bc.list_edges)
        return edge_name in edges

    def _get_plugin(self, cluster: Dict, plugin_name: str) -> Any:
        """Get plugin client for a cluster."""
        if not self._bc:
            raise RuntimeError("No active bridge connection")
        edge_name = cluster.get('child_edge_name') or cluster['edge_name']
        ec = self._bc.get_edge_client(edge_name)
        return ec.get_plugin(plugin_name)

    # -------------------------------------------------------------------------
    # Task rendering
    # -------------------------------------------------------------------------

    def _render_task(self, template: Dict, **subs) -> Dict:
        """Substitute {placeholder} values in a task template dict."""
        return {
            "executable": template["executable"].format_map(subs),
            "arguments":  [str(a).format_map(subs)
                           for a in template.get("arguments", [])],
        }

    # -------------------------------------------------------------------------
    # Data Acquisition
    # -------------------------------------------------------------------------

    async def _acquire_sensor_data(self, workspace: Path) -> Path:
        """Fetch sensor data from CSPOT (or generate mock data for testing)."""
        if not self._current_config:
            raise RuntimeError("No active workflow configuration")
        cfg = self._current_config
        output_dir = workspace / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "sensor_out.csv"

        if cfg.mock_sensor_data:
            log.info("[XGFabric] _acquire_sensor_data: mock mode — writing synthetic CSV")
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['dt', 'windspeed', 'windavg', 'winddir'])
                writer.writeheader()
                for i in range(max(cfg.num_simulations, 8)):
                    writer.writerow({'dt': f'2024-01-01T{i:02d}:00:00+00:00',
                                     'windspeed': 2.0 + i * 0.5,
                                     'windavg':   2.0 + i * 0.5,
                                     'winddir':   90 + i * 5})
            self._update_state('data_acquisition', 'Mock sensor data ready (test mode)', 10)
            return output_file

        # Find senspot-get
        senspot_path = self._find_senspot_get()

        # Validate CSPOT URL
        parsed = urllib.parse.urlparse(cfg.cspot_woof_url)
        if parsed.scheme not in ('http', 'https', 'woof', ''):
            raise ValueError(f"Invalid cspot_woof_url scheme: {cfg.cspot_woof_url}")

        # Fetch latest sequence number
        cmd = [senspot_path, '-W', cfg.cspot_woof_url]
        result = await asyncio.to_thread(
            subprocess.run, cmd, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(f"senspot-get failed: {result.stderr}")

        match = re.search(r'seq_no:\s+(\d+)', result.stdout)
        if not match:
            raise RuntimeError("Could not parse sequence number from CSPOT")
        latest_seq = int(match.group(1))

        # Collect data backwards
        records = []
        current_seq = latest_seq
        limit = cfg.cspot_limit
        skipped = 0

        log.info("[XGFabric] _acquire_sensor_data: latest_seq=%d  limit=%d  woof=%s",
                 latest_seq, limit, cfg.cspot_woof_url)

        while len(records) < limit and current_seq >= 0:
            if self._cancel_requested:
                raise asyncio.CancelledError()

            cmd = [senspot_path, '-W', cfg.cspot_woof_url, '-S', str(current_seq)]
            result = await asyncio.to_thread(
                subprocess.run, cmd, capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                match = re.search(r'time:\s+([\d.]+)', output)
                if match:
                    timestamp = float(match.group(1))
                    dt = datetime.fromtimestamp(timestamp, timezone.utc)
                    parts = output.split()
                    data  = parts[0].split(':') if parts else []
                    if len(data) >= 3:
                        ws = float(data[0])
                        wa = float(data[1])
                        wd = float(data[2])
                        if ws > 50:  # mph to m/s
                            ws *= 0.44704
                            wa *= 0.44704
                        records.append({
                            'dt': dt.isoformat(),
                            'windspeed': ws,
                            'windavg': wa,
                            'winddir': wd
                        })
                    else:
                        skipped += 1
                        log.info("[XGFabric] seq %d: not enough fields (%d): %s",
                                 current_seq, len(data), output[:80])
                else:
                    skipped += 1
                    log.info("[XGFabric] seq %d: no 'time:' in output: %s",
                             current_seq, output[:80])
            else:
                skipped += 1
                log.info("[XGFabric] seq %d: returncode=%d  stderr=%s",
                         current_seq, result.returncode, result.stderr.strip()[:80])

            current_seq -= 1

            # Log progress every 10 iterations
            if (latest_seq - current_seq) % 10 == 0:
                log.info("[XGFabric] _acquire_sensor_data: seq=%d  records=%d/%d  skipped=%d",
                         current_seq, len(records), limit, skipped)

            # Update progress
            progress = int(len(records) / limit * 10)  # 0-10% for data acquisition
            self._update_state('data_acquisition',
                               f'Fetched {len(records)}/{limit} sensor records',
                               progress)

        if not records:
            raise RuntimeError("No records fetched from CSPOT")

        # Write CSV
        with open(output_file, 'w') as f:
            f.write("dt,windspeed,windavg,winddir\n")
            for r in records:
                f.write(f"{r['dt']},{r['windspeed']},{r['windavg']},{r['winddir']}\n")

        return output_file

    def _find_senspot_get(self) -> str:
        """Find senspot-get binary."""
        if os.environ.get('SENSPOT_PATH'):
            path = os.environ['SENSPOT_PATH']
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path

        which_path = shutil.which('senspot-get')
        if which_path:
            return which_path

        home = os.path.expanduser('~')
        candidates = [
            f"{home}/bin/senspot-get",
            f"{home}/common/cspot/build/bin/senspot-get",
            "/global/common/software/m5290/cspot/build/bin/senspot-get",
        ]
        for path in candidates:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path

        raise FileNotFoundError("senspot-get not found")

    # -------------------------------------------------------------------------
    # Pilot Job
    # -------------------------------------------------------------------------

    async def _submit_pilot(self, cluster: Dict, bridge_url: str) -> str:
        """Submit pilot job to spawn child edge."""
        if not self._bc:
            raise RuntimeError("No active bridge connection")
        ec = self._bc.get_edge_client(cluster['edge_name'])
        psij: Any = await asyncio.to_thread(ec.get_plugin, 'psij')

        args = ["--url", bridge_url, "--name", cluster['edge_name'] + ".1"]

        edge_svc      = self._app.state.edge_service
        plugin_filter = edge_svc._plugin_filter
        args += ["-p", ",".join(plugin_filter)]

        pilot_spec = {
            "executable": "radical-edge-service.sh",
            "arguments": args,
            "attributes": {
                "queue_name": cluster.get('queue', 'regular'),
                "account": cluster.get('account', ''),
                "duration": str(cluster.get('duration', 3600)),
                "node_count": cluster.get('nodes', 1),
            }
        }

        executor = cluster.get('executor') or await self._discover_executor(ec)
        result = await asyncio.to_thread(psij.submit_job, pilot_spec, executor)
        return result['job_id']

    async def _discover_executor(self, ec) -> str:
        """Ask the remote edge's queue_info plugin which scheduler it uses.

        Returns the matching PsiJ executor name. Falls back to 'slurm' (the
        historical default) if the edge has no queue_info plugin or the
        query fails.
        """
        try:
            qi = await asyncio.to_thread(ec.get_plugin, 'queue_info')
            backend = await asyncio.to_thread(qi.backend)
        except Exception as exc:
            log.info("[XGFabric] _discover_executor: probe failed (%s) — "
                     "defaulting to slurm", exc)
            return 'slurm'
        # backend names align with PsiJ executor names; 'none' falls back too.
        if backend in ('slurm', 'pbs', 'lsf', 'cobalt'):
            return backend
        return 'slurm'

    # -------------------------------------------------------------------------
    # Data Staging
    # -------------------------------------------------------------------------

    async def _stage_sensor_data(self, cluster: Dict, sensor_csv: Path):
        """Stage sensor data to cluster."""
        staging = await asyncio.to_thread(self._get_plugin, cluster, 'staging')
        workflow_path = await self._resolve_path(cluster['edge_name'], cluster['workflow_path'])
        remote_path = f"{workflow_path}/data/sensor_out.csv"
        await asyncio.to_thread(staging.put, str(sensor_csv), remote_path, overwrite=True)

    async def _migrate_data(self, source: Dict, dest: Dict, sim_results: List[str]):
        """Migrate simulation results between clusters."""
        if not sim_results:
            return

        if not self._current_config:
            raise RuntimeError("No active workflow configuration")
        source_staging = await asyncio.to_thread(self._get_plugin, source, 'staging')
        dest_staging   = await asyncio.to_thread(self._get_plugin, dest,   'staging')

        staging_dir = Path(self._current_config.local_workspace) / "staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        dest_workflow = await self._resolve_path(dest['edge_name'], dest['workflow_path'])

        for i, remote_path in enumerate(sim_results):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            filename = Path(remote_path).name
            local_path = staging_dir / filename

            await asyncio.to_thread(source_staging.get, remote_path, str(local_path))
            await asyncio.to_thread(dest_staging.put,   str(local_path), f"{dest_workflow}/simulations/{filename}")

            progress = 50 + int((i + 1) / len(sim_results) * 10)
            self._update_state('migration',
                               f'Migrated {i+1}/{len(sim_results)} files',
                               progress)

    # -------------------------------------------------------------------------
    # Simulations
    # -------------------------------------------------------------------------

    async def _run_simulations(self, cluster: Dict, sensor_csv: Path,
                               allocate: Optional[Dict]) -> List[str]:
        """Run CFD simulations on cluster.

        Polls Rhapsody wait_tasks (timeout=5s) so the event loop stays free.
        If the allocate cluster comes online mid-batch the current batch is
        aborted and we return early so the caller can migrate.
        """
        cfg = self._current_config
        if not cfg:
            raise RuntimeError("No active workflow configuration")
        params = self._generate_sim_params(sensor_csv, cfg.num_simulations)

        workflow_path  = await self._resolve_path(cluster['edge_name'], cluster['workflow_path'])
        sim_output_dir = f"{workflow_path}/simulations"
        rhapsody       = await asyncio.to_thread(self._get_plugin, cluster, 'rhapsody')

        if not cfg.simulation_task:
            raise RuntimeError("Config missing 'simulation_task' — cannot run simulations")

        # Build tasks, track (ws, sim_id, wd) per index for result-path assembly
        tasks       = []
        task_params = []   # parallel to tasks: (wind_speed_str, sim_id_str, wind_dir_str)
        for wind_speed, wind_dir, sim_id in params:
            task = self._render_task(cfg.simulation_task,
                                     workflow_path=workflow_path,
                                     sim_output_dir=sim_output_dir,
                                     wind_speed=str(wind_speed),
                                     sim_id=str(sim_id),
                                     wind_dir=str(wind_dir))
            tasks.append(task)
            task_params.append((str(wind_speed), str(sim_id), str(wind_dir)))

        completed_results = []
        total_batches     = (len(tasks) + cfg.batch_size - 1) // cfg.batch_size
        self._state.total_batches = total_batches

        TERMINAL = {'COMPLETED', 'FAILED', 'CANCELED', 'CANCELLED', 'ERROR'}

        abort_for_pilot = False
        for batch_num, i in enumerate(range(0, len(tasks), cfg.batch_size)):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            batch        = tasks[i:i + cfg.batch_size]
            batch_params = task_params[i:i + cfg.batch_size]

            self._state.current_batch = batch_num + 1
            self._update_state('simulations',
                               f'Running batch {batch_num+1}/{total_batches}...',
                               15 + int(batch_num / total_batches * 30))

            submitted = await asyncio.to_thread(rhapsody.submit_tasks, batch)

            # uid → expected result path (known at submission time)
            uid_to_result: dict = {}
            for j, t in enumerate(submitted):
                uid = t['uid']
                ws, sim_idx, wd = batch_params[j]
                uid_to_result[uid] = f"{sim_output_dir}/sim_{sim_idx}_ws_{ws}_wd_{wd}.csv"

            pending = set(uid_to_result)
            self._pending_tasks = (rhapsody, pending)

            while pending:
                if self._cancel_requested:
                    raise asyncio.CancelledError()

                # Pilot came online — abort remaining tasks and migrate.
                if allocate and await self._is_edge_online(allocate):
                    log.info("[XGFabric] Pilot online mid-batch (%d pending) "
                             "— aborting to migrate", len(pending))
                    abort_for_pilot = True
                    break

                # Poll for up to 5s; returns current state of all requested tasks.
                try:
                    results = await asyncio.to_thread(
                        rhapsody.wait_tasks, list(pending), 5.0)
                except Exception as e:
                    log.warning("[XGFabric] wait_tasks error: %s — retrying", e)
                    await asyncio.sleep(1)
                    continue

                for t in results:
                    uid   = t.get('uid')
                    state = str(t.get('state') or '').upper()
                    if uid not in pending:
                        continue
                    if not any(s in state for s in TERMINAL):
                        continue  # still running — will be included in next poll

                    pending.discard(uid)
                    exit_code = t.get('exit_code')
                    task_ok   = (state == 'COMPLETED') and exit_code in (None, 0)
                    if task_ok:
                        completed_results.append(uid_to_result[uid])
                    else:
                        j = next((idx for idx, sub in enumerate(submitted)
                                  if sub.get('uid') == uid), None)
                        spec = batch[j] if j is not None else None
                        self._log_task_error(f"sim {uid}", t, spec)

                    self._state.completed_simulations += 1
                    if task_ok:
                        self._notify_state()

            self._pending_tasks = None
            if abort_for_pilot:
                break

        if not completed_results:
            raise RuntimeError(
                f"All {len(tasks)} simulations failed — cannot proceed to training")

        return completed_results

    def _generate_sim_params(self, sensor_csv: Path, num_sims: int) -> List:
        """Generate simulation parameters from sensor data."""

        wind_speeds = []
        with open(sensor_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ws = float(row['windspeed'])
                if 0.5 < ws < 30:
                    wind_speeds.append(ws)

        if not wind_speeds:
            raise RuntimeError("No valid wind speeds in sensor data")

        ws_min, ws_max = min(wind_speeds), max(wind_speeds)
        step = (ws_max - ws_min) / max(num_sims - 1, 1)
        return [(round(ws_min + i * step, 2), 0, i) for i in range(num_sims)]

    # -------------------------------------------------------------------------
    # Training
    # -------------------------------------------------------------------------

    async def _run_training(self, cluster: Dict, sim_results: List[str]):
        """Run ML training on cluster."""
        cfg = self._current_config
        if not cfg:
            raise RuntimeError("No active workflow configuration")
        workflow_path = await self._resolve_path(cluster['edge_name'], cluster['workflow_path'])
        sensor_dir = f"{workflow_path}/data"
        sim_dir = f"{workflow_path}/simulations"
        output_dir = f"{workflow_path}/models"

        rhapsody = await asyncio.to_thread(self._get_plugin, cluster, 'rhapsody')

        subs = dict(workflow_path=workflow_path, sensor_dir=sensor_dir,
                    sim_dir=sim_dir, output_dir=output_dir)

        for i, model in enumerate(cfg.train_models):
            if self._cancel_requested:
                raise asyncio.CancelledError()

            if model not in cfg.training_tasks:
                log.warning("[XGFabric] No training_task defined for model '%s' — skipping", model)
                continue

            progress = 60 + int((i + 1) / len(cfg.train_models) * 25)
            self._update_state('training', f'Training {model.upper()} model...', progress)

            task = self._render_task(cfg.training_tasks[model], model=model, **subs)
            submitted = await asyncio.to_thread(rhapsody.submit_tasks, [task])
            results = await asyncio.to_thread(rhapsody.wait_tasks, [submitted[0]['uid']])
            t = results[0] if results else {}
            if t.get('exit_code') not in (None, 0) or \
                    str(t.get('state', '')).upper() != 'COMPLETED':
                self._log_task_error(f"training/{model}", t, task)
                raise RuntimeError(f"Training {model} failed (exit={t.get('exit_code')})")

    # -------------------------------------------------------------------------
    # Evaluation
    # -------------------------------------------------------------------------

    async def _run_evaluation(self, cluster: Dict):
        """Run evaluation metrics computation."""
        if not self._current_config:
            raise RuntimeError("No active workflow configuration")
        cfg = self._current_config

        workflow_path = await self._resolve_path(cluster['edge_name'], cluster['workflow_path'])
        sensor_file = f"{workflow_path}/data/sensor_out.csv"
        eval_output = f"{workflow_path}/evaluation"
        rhapsody = await asyncio.to_thread(self._get_plugin, cluster, 'rhapsody')

        if not cfg.evaluation_task:
            raise RuntimeError("Config missing 'evaluation_task' — cannot run evaluation")

        task = self._render_task(cfg.evaluation_task,
                                 workflow_path=workflow_path,
                                 sensor_file=sensor_file,
                                 eval_output=eval_output)
        self._update_state('evaluation', 'Computing metrics...', 90)
        submitted = await asyncio.to_thread(rhapsody.submit_tasks, [task])
        results   = await asyncio.to_thread(rhapsody.wait_tasks, [submitted[0]['uid']])
        t = results[0] if results else {}
        if t.get('exit_code') not in (None, 0) or \
                str(t.get('state', '')).upper() != 'COMPLETED':
            self._log_task_error("evaluation", t, task)
            raise RuntimeError(f"Evaluation failed (exit={t.get('exit_code')})")

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    async def _cleanup_on_failure(self):
        """Clean up resources on failure."""
        # Cancel any pending rhapsody tasks from the current batch
        if self._pending_tasks:
            rhapsody, pending = self._pending_tasks
            self._pending_tasks = None
            for uid in list(pending):
                try:
                    await asyncio.to_thread(rhapsody.cancel_task, uid)
                    log.info("[XGFabric] Cancelled task %s", uid)
                except Exception as e:
                    log.warning("[XGFabric] Failed to cancel task %s: %s", uid, e)

        # Cancel pilot jobs
        for cluster_name, pilot_id in self._state.pilot_jobs.items():
            try:
                # Find the cluster config
                cfg = self._current_config
                if not cfg:
                    raise RuntimeError("No active workflow configuration")
                if not self._bc:
                    raise RuntimeError("No active bridge connection")
                all_clusters = (self._state.immediate_clusters +
                                self._state.allocate_clusters)
                for c in all_clusters:
                    if c.get('name') == cluster_name:
                        ec   = self._bc.get_edge_client(c['edge_name'])
                        psij = await asyncio.to_thread(ec.get_plugin, 'psij')
                        await asyncio.to_thread(psij.cancel_job, pilot_id)
                        log.info("Cancelled pilot job %s", pilot_id)
                        break
            except Exception as e:
                log.warning("Failed to cancel pilot %s: %s", pilot_id, e)

    async def close(self) -> dict:
        """Close the session."""
        if self._workflow_task and not self._workflow_task.done():
            self._cancel_requested = True
            self._workflow_task.cancel()
        if self._bc:
            try:
                self._bc.close()
            except Exception as e:
                log.exception("[XGFabric] Error closing bridge client: %s", e)
        await self._http.aclose()
        return await super().close()


# -----------------------------------------------------------------------------
# Client
# -----------------------------------------------------------------------------

class XGFabricClient(PluginClient):
    """Client-side interface for the XGFabric plugin."""

    def get_workdir(self) -> Dict:
        """Get current working directory."""
        resp = self._http.get(self._url(f"workdir/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def set_workdir(self, path: str) -> Dict:
        """Set working directory."""
        resp = self._http.post(self._url(f"workdir/{self.sid}"), json={'path': path})
        self._raise(resp)
        return resp.json()

    def list_configs(self) -> List[Dict]:
        """List all saved configurations."""
        resp = self._http.get(self._url(f"configs/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def load_config(self, name: str) -> Dict:
        """Load a configuration by name."""
        resp = self._http.get(self._url(f"config/{self.sid}/{name}"))
        self._raise(resp)
        return resp.json()

    def save_config(self, config: Dict) -> Dict:
        """Save a configuration."""
        resp = self._http.post(self._url(f"config/{self.sid}"), json=config)
        self._raise(resp)
        return resp.json()

    def delete_config(self, name: str) -> Dict:
        """Delete a configuration."""
        resp = self._http.post(self._url(f"config/{self.sid}/{name}/delete"))
        self._raise(resp)
        return resp.json()

    def get_default_config(self) -> Dict:
        """Get default configuration template."""
        resp = self._http.get(self._url(f"config/{self.sid}/default"))
        self._raise(resp)
        return resp.json()

    def get_test_config(self) -> Dict:
        """Get test configuration template (stub tasks, no CSPOT required)."""
        resp = self._http.get(self._url(f"config/{self.sid}/test"))
        self._raise(resp)
        return resp.json()

    def get_status(self) -> Dict:
        """Get current workflow status."""
        resp = self._http.get(self._url(f"status/{self.sid}"))
        self._raise(resp)
        return resp.json()

    def start_workflow(self, workflow: str = '__default__',
                       resource: str = '__default__') -> Dict:
        """Start workflow execution."""
        resp = self._http.post(self._url(f"start/{self.sid}"),
                               json={'workflow': workflow, 'resource': resource})
        self._raise(resp)
        return resp.json()

    def stop_workflow(self) -> Dict:
        """Stop running workflow."""
        resp = self._http.post(self._url(f"stop/{self.sid}"))
        self._raise(resp)
        return resp.json()


# -----------------------------------------------------------------------------
# Plugin
# -----------------------------------------------------------------------------

class PluginXGFabric(Plugin):
    """
    XGFabric plugin for Radical Edge.

    Orchestrates CFDaAI workflows across multiple HPC clusters.
    Provides configuration management and workflow execution via REST API.
    """

    plugin_name = "xgfabric"
    session_class = XGFabricSession
    client_class = XGFabricClient
    version = '0.1.0'

    ui_config = {
        "icon":            "🌊",
        "title":           "XGFabric Workflow",
        "description":     "CFDaAI workflow orchestrator for HPC clusters.",
        "custom_template": True,
    }

    @classmethod
    def is_enabled(cls, app: FastAPI) -> bool:
        """XGFabric loads on edge nodes (login or compute) — not on the bridge."""
        return not getattr(app.state, 'is_bridge', False)

    def __init__(self, app: FastAPI, workdir: Optional[str] = None):
        super().__init__(app, 'xgfabric')

        self._workdir = workdir or os.environ.get('XGFABRIC_WORKDIR') or os.getcwd()
        self._connected_edges: Dict[str, Any] = {}  # Cache of connected edges

        # Config directory endpoints
        self.add_route_get('workdir/{sid}', self.get_workdir)
        self.add_route_post('workdir/{sid}', self.set_workdir)

        # Config endpoints
        self.add_route_get('configs/{sid}', self.list_configs)
        self.add_route_get('config/{sid}/default', self.get_default_config)
        self.add_route_get('config/{sid}/test', self.get_test_config)
        self.add_route_get('config/{sid}/{name}', self.load_config)
        self.add_route_post('config/{sid}', self.save_config)
        self.add_route_post('config/{sid}/{name}/delete', self.delete_config)

        # Workflow endpoints
        self.add_route_get('status/{sid}', self.get_status)
        self.add_route_post('start/{sid}', self.start_workflow)
        self.add_route_post('stop/{sid}', self.stop_workflow)

    def _create_session(self, sid: str, **_) -> XGFabricSession:
        """Create session with workdir, edge name, and bridge connection info."""
        edge_name = getattr(self._app.state, 'edge_name', 'local')

        # Get bridge URL from edge service.  Cert path comes from the
        # shared resolver (CLI > env > file) — we ignore CLI here since
        # this is the bridge-internal session-creation path.
        from . import utils
        edge_service = getattr(self._app.state, 'edge_service', None)
        bridge_url   = getattr(edge_service, '_bridge_url', None) if edge_service else None
        try:
            cert_path, _ = utils.resolve_bridge_cert()
            bridge_cert  = str(cert_path)
        except (ValueError, FileNotFoundError):
            bridge_cert  = None

        log.info("[XGFabric] _create_session: sid=%s  edge=%s  bridge_url=%s  cached_edges=%s",
                 sid, edge_name, bridge_url, list(self._connected_edges.keys()))

        # Use super() so the base class injects the _notify callback into the session
        session = super()._create_session(sid,
                      workdir=self._workdir, edge_name=edge_name,
                      bridge_url=bridge_url, bridge_cert=bridge_cert)
        if not isinstance(session, XGFabricSession):
            raise RuntimeError(f"Expected XGFabricSession, got {type(session).__name__}")

        # Seed session with current topology so get_status() classifies edges correctly
        if self._connected_edges:
            session.update_connected_edges(self._connected_edges)

        return session

    async def on_topology_change(self, edges: dict):
        """Handle topology updates from the bridge."""
        prev  = set(self._connected_edges or {})
        curr  = set(edges)
        self._connected_edges = edges

        for name in curr - prev:
            plugins = list(edges[name].get('plugins', []))
            log.info("[XGFabric] Edge connected: %s  plugins=%s", name, plugins)
        for name in prev - curr:
            log.info("[XGFabric] Edge disconnected: %s", name)

        # Update all active sessions and push updated cluster list to clients
        for session in self._sessions.values():
            if isinstance(session, XGFabricSession):
                session.update_connected_edges(edges)
                if session._plugin:
                    session._notify_state()

    # -- Route handlers -------------------------------------------------------

    async def get_workdir(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.get_config_dir)

    async def set_workdir(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        path = data.get('path', '')
        return await self._forward(sid, XGFabricSession.set_config_dir, path=path)

    async def list_configs(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.list_configs)

    async def get_default_config(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.load_config, name='default')

    async def get_test_config(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.load_config, name='test')

    async def load_config(self, request: Request) -> dict:
        sid = request.path_params['sid']
        name = request.path_params['name']
        return await self._forward(sid, XGFabricSession.load_config, name=name)

    async def save_config(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        return await self._forward(sid, XGFabricSession.save_config, data=data)

    async def delete_config(self, request: Request) -> dict:
        sid = request.path_params['sid']
        name = request.path_params['name']
        return await self._forward(sid, XGFabricSession.delete_config, name=name)

    async def get_status(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.get_status)

    async def start_workflow(self, request: Request) -> dict:
        sid = request.path_params['sid']
        data = await request.json()
        # Accept both new-style {workflow, resource} and legacy {config_name} from explorer
        workflow = data.get('workflow') or data.get('config_name') or '__default__'
        resource = data.get('resource', '__default__')
        return await self._forward(sid, XGFabricSession.start_workflow,
                                   workflow=workflow, resource=resource)

    async def stop_workflow(self, request: Request) -> dict:
        sid = request.path_params['sid']
        return await self._forward(sid, XGFabricSession.stop_workflow)
