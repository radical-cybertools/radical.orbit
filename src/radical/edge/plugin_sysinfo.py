
__author__    = 'Radical Development Team'
__email__     = 'radical@radical-project.org'
__copyright__ = 'Copyright 2024, RADICAL@Rutgers'
__license__   = 'MIT'


import getpass
import glob
import os
import re
import time
import threading
import psutil
import socket
import logging
import platform
import subprocess
import json

from typing import Dict, List, Any

from fastapi import FastAPI
from starlette.requests import Request

from .plugin_base   import Plugin
from .client        import PluginClient

log = logging.getLogger("radical.edge")


class SysInfoProvider:
    """
    Helper class to gather system information using psutil and standard tools.
    """

    def __init__(self):
        self._cpu_model = None
        self._gpu_info  = None

    def start_prefetch(self):
        """
        Start a background thread to prefetch hardware detection.

        This lazily fills the detection cache so later queries are faster.
        """
        def _prefetch():
            try:
                self._ensure_detected()
            except Exception:
                pass  # Silently ignore prefetch failures

        thread = threading.Thread(target=_prefetch, daemon=True)
        thread.start()

    def _ensure_detected(self):
        """Run hardware detection once on first use."""
        if self._cpu_model is None:
            self._cpu_model = self._detect_cpu_model()
        if self._gpu_info is None:
            self._gpu_info = self._detect_gpus()

    def _detect_cpu_model(self) -> str:
        """Parse /proc/cpuinfo or platform info for CPU model name."""
        try:
            if platform.system() == "Linux":
                with open("/proc/cpuinfo", "r") as f:
                    for line in f:
                        if "model name" in line:
                            return line.split(":")[1].strip()
            return platform.processor()
        except Exception:
            return "Unknown"

    def _detect_disk_type(self, device: str) -> str:
        """Detect storage type (SSD/HDD) via /sys/block on Linux."""
        try:
            dev_name = os.path.basename(device)

            # Handle different device naming patterns
            # NVMe: nvme0n1p1 → nvme0n1
            # MMC: mmcblk0p1 → mmcblk0
            # Standard: sda1 → sda

            if dev_name.startswith('nvme'):
                # NVMe devices: nvme0n1p1 → nvme0n1
                base_dev = re.sub(r'p\d+$', '', dev_name)
            elif dev_name.startswith('mmcblk'):
                # MMC devices: mmcblk0p1 → mmcblk0
                base_dev = re.sub(r'p\d+$', '', dev_name)
            else:
                # Standard devices: sda1 → sda
                base_dev = re.sub(r'\d+$', '', dev_name)

            rot_path = f"/sys/block/{base_dev}/queue/rotational"
            if os.path.exists(rot_path):
                with open(rot_path, "r") as f:
                    rot = f.read().strip()
                    if rot == "0":
                        return "ssd"
                    elif rot == "1":
                        return "hdd"
        except Exception:
            pass
        return "unknown"

    def _detect_gpus(self) -> List[Dict[str, Any]]:
        """
        Detect available GPUs (NVIDIA, AMD, Intel) in parallel.
        Each detector runs in its own thread with a 5 s timeout so a
        slow or absent tool does not block the others.
        """
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        gpus: List[Dict[str, Any]] = []
        detectors = [
            self._detect_nvidia_gpus,
            self._detect_amd_gpus,
            self._detect_intel_gpus,
        ]
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = [ex.submit(fn) for fn in detectors]
            for future in futures:
                try:
                    gpus.extend(future.result(timeout=5))
                except (FuturesTimeout, Exception):
                    pass
        return gpus

    def _detect_nvidia_gpus(self) -> List[Dict[str, Any]]:
        """Query nvidia-smi for NVIDIA GPUs."""
        nvidia_smi = os.environ.get("NVIDIA_SMI_PATH", "nvidia-smi")
        try:
            # Check availability
            subprocess.run([nvidia_smi, "-L"],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL,
                           check=True, timeout=5)

            # Query static info
            cmd = [
                nvidia_smi,
                "--query-gpu=index,name,driver_version,uuid",
                "--format=csv,noheader,nounits"
            ]
            ret = subprocess.check_output(cmd, text=True, timeout=5)
            gpus = []
            for line in ret.strip().splitlines():
                if not line.strip(): continue
                parts = [x.strip() for x in line.split(',')]
                if len(parts) >= 4:
                    idx, name, driver, uuid = parts[:4]
                    gpus.append({
                        "id": idx,  # specific ID for internal mapping
                        "index": int(idx),
                        "name": name,
                        "driver_version": driver,
                        "uuid": uuid,
                        "vendor": "NVIDIA"
                    })
            return gpus

        except (FileNotFoundError, subprocess.CalledProcessError):
            return []

    def _detect_amd_gpus(self) -> List[Dict[str, Any]]:
        """Query rocm-smi for AMD GPUs."""
        rocm_smi = os.environ.get("ROCM_SMI_PATH", "rocm-smi")
        try:
            # Check availability: rocm-smi --json returns full info
            # Note: --json flag support varies by version, but modern rocm-smi has it.
            cmd = [rocm_smi, "--showproductname", "--showdriverversion", "--showuniqueid", "--json"]
            ret = subprocess.check_output(cmd, text=True, timeout=5)
            data = json.loads(ret)

            gpus = []
            # rocm-smi json keys are typically "card0", "card1", etc.
            for card_key, info in data.items():
                if not card_key.startswith("card"): continue
                idx = int(card_key.replace("card", ""))

                gpus.append({
                    "id": card_key,  # Use card0 as ID
                    "index": idx,
                    "name": info.get("Card Series", "Unknown AMD GPU"),
                    "driver_version": info.get("Driver version", "Unknown"),
                    "uuid": info.get("Unique ID", ""),
                    "vendor": "AMD"
                })
            return gpus

        except (FileNotFoundError, subprocess.CalledProcessError, ImportError, Exception):
            return []

    def _detect_intel_gpus(self) -> List[Dict[str, Any]]:
        """Detect Intel GPUs via sysfs."""
        try:
            gpus = []

            # Intel PCI vendor ID
            INTEL_VENDOR_ID = "0x8086"

            # Scan /sys/class/drm/card* for Intel GPUs
            for card_path in glob.glob("/sys/class/drm/card[0-9]"):
                try:
                    # Read vendor ID
                    vendor_path = os.path.join(card_path, "device/vendor")
                    if not os.path.exists(vendor_path):
                        continue

                    with open(vendor_path, 'r') as f:
                        vendor_id = f.read().strip()

                    if vendor_id != INTEL_VENDOR_ID:
                        continue

                    # This is an Intel GPU
                    card_name = os.path.basename(card_path)
                    card_idx = int(card_name.replace("card", ""))

                    # Try to get device name from various sources
                    device_name = "Intel GPU"

                    # Try device/label
                    label_path = os.path.join(card_path, "device/label")
                    if os.path.exists(label_path):
                        with open(label_path, 'r') as f:
                            device_name = f.read().strip()
                    else:
                        # Try to parse from modalias or device ID
                        device_path = os.path.join(card_path, "device/device")
                        if os.path.exists(device_path):
                            with open(device_path, 'r') as f:
                                device_id = f.read().strip()
                                # Basic mapping for common Intel GPUs (incomplete)
                                if "0x46" in device_id:
                                    device_name = "Intel Iris Xe Graphics"
                                elif "0x9a" in device_id:
                                    device_name = "Intel Iris Xe Graphics"
                                elif "0x19" in device_id:
                                    device_name = "Intel UHD Graphics"

                    # Try to get driver version
                    driver_version = "Unknown"
                    try:
                        driver_path = os.path.join(card_path, "device/driver/module/version")
                        if os.path.exists(driver_path):
                            with open(driver_path, 'r') as f:
                                driver_version = f.read().strip()
                    except Exception:
                        pass

                    gpus.append({
                        "id": card_name,
                        "index": card_idx,
                        "name": device_name,
                        "driver_version": driver_version,
                        "uuid": "",  # Intel doesn't provide UUID via sysfs easily
                        "vendor": "Intel"
                    })

                except Exception:
                    continue

            return gpus

        except Exception:
            return []

    def _get_gpu_metrics(self, static_gpus: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Gather dynamic metrics for detected GPUs."""

        nvidia_gpus = [g for g in static_gpus if g['vendor'] == 'NVIDIA']
        amd_gpus    = [g for g in static_gpus if g['vendor'] == 'AMD']
        intel_gpus  = [g for g in static_gpus if g['vendor'] == 'Intel']

        metrics = []

        # NVIDIA Metrics
        if nvidia_gpus:
            try:
                # Add memory.free to query
                cmd = [
                    "nvidia-smi",
                    "--query-gpu=index,utilization.gpu,utilization.memory,memory.total,memory.used,memory.free",
                    "--format=csv,noheader,nounits"
                ]
                ret = subprocess.check_output(cmd, text=True)

                # Parse
                n_metrics = {}
                for line in ret.strip().splitlines():
                    if not line.strip(): continue
                    parts = [x.strip() for x in line.split(',')]
                    idx = int(parts[0])
                    n_metrics[idx] = {
                        "util_gpu": float(parts[1]),
                        "util_mem": float(parts[2]),
                        "mem_total": int(parts[3]),
                        "mem_used": int(parts[4]),
                        "mem_free": int(parts[5])
                    }

                # Merge
                for g in nvidia_gpus:
                    idx = g['index']
                    m = n_metrics.get(idx, {})
                    metrics.append({**g, **m})

            except Exception:
                metrics.extend(nvidia_gpus)

        # AMD Metrics
        if amd_gpus:
            try:
                # rocm-smi --showuse --showmeminfo vram --json
                cmd = ["rocm-smi", "--showuse", "--showmeminfo", "vram", "--json"]
                ret = subprocess.check_output(cmd, text=True)
                data = json.loads(ret)

                for g in amd_gpus:
                    card = g['id']  # card0
                    info = data.get(card, {})

                    # Parse utilization
                    try:
                        util_gpu = float(info.get("GPU use (%)", 0))
                    except Exception:
                        util_gpu = 0.0

                    # Parse memory (Assuming Bytes usually, check output carefully in prod)
                    # rocm-smi JSON output format varies.
                    try:
                        mem_tot = int(info.get("VRAM Total Memory (B)", 0))
                        mem_used = int(info.get("VRAM Total Used Memory (B)", 0))

                        metrics.append({
                            **g,
                            "util_gpu": util_gpu,
                            "util_mem": 0.0,
                            "mem_total": mem_tot // (1024 * 1024),
                            "mem_used": mem_used // (1024 * 1024),
                            "mem_free": (mem_tot - mem_used) // (1024 * 1024)
                        })
                    except Exception:
                        metrics.append(g)

            except Exception:
                metrics.extend(amd_gpus)

        # Intel Metrics
        # Intel GPUs don't have standard CLI tool like nvidia-smi
        # Return static info only for now
        if intel_gpus:
            metrics.extend(intel_gpus)

        return metrics

    def get_metrics(self) -> Dict[str, Any]:
        """Collect current system metrics."""

        self._ensure_detected()

        # --- System ---
        boot_time = psutil.boot_time()
        uptime = time.time() - boot_time
        unet = platform.uname()

        # ``getpass.getuser()`` checks the standard env vars (LOGNAME /
        # USER / LNAME / USERNAME) before falling back to pwd lookup —
        # right answer in the common case, no exception on rootless
        # container weirdness where pwd lookup might fail.
        try:    user = getpass.getuser()
        except Exception:
            user = ''

        metrics = {
            "system": {
                "hostname": socket.gethostname(),
                "user":     user,
                "uptime":   uptime,
                "kernel":   f"{unet.system} {unet.release}",
                "arch":     unet.machine,
            }
        }

        # --- CPU ---
        # psutil.cpu_freq() might fail on some container envs
        freq = None
        try:
            freq_struct = psutil.cpu_freq()
            freq = freq_struct.current if freq_struct else None
        except Exception:
            pass

        metrics["cpu"] = {
            "model": self._cpu_model,
            "vendor": platform.processor(),  # Simplistic
            "cores_physical": psutil.cpu_count(logical=False) or 0,
            "cores_logical": psutil.cpu_count(logical=True) or 0,
            "percent": psutil.cpu_percent(interval=None),  # Non-blocking (requires previous call for accuracy)
            "load_avg": list(os.getloadavg()) if hasattr(os, "getloadavg") else [],
            "freq_mhz": freq
        }

        # --- Memory ---
        mem = psutil.virtual_memory()
        metrics["memory"] = {
            "total": mem.total,
            "available": mem.available,
            "percent": mem.percent,
            "used": mem.used
        }

        # --- Disk ---
        # Shared/network filesystem types common on HPC systems
        NETWORK_FSTYPES = {
            # Standard network filesystems
            "nfs", "nfs4", "cifs", "smb", "smbfs",
            # Parallel/HPC filesystems
            "lustre", "gpfs", "beegfs", "pvfs2", "orangefs",
            "glusterfs", "cephfs", "afs",
            # Cray-specific
            "dvs",
        }

        disks = []
        for part in psutil.disk_partitions(all=False):
            # Skip pseudo filesystems if possible (handled by all=False usually)
            if "sqsh" in part.opts or "snap" in part.mountpoint:
                continue

            try:
                usage = psutil.disk_usage(part.mountpoint)

                # Heuristic for filesystem type vs disk type
                fstype = part.fstype
                disk_type = "unknown"

                if fstype in NETWORK_FSTYPES:
                    disk_type = "shared"
                elif part.device.startswith("/dev/"):
                    sub_type = self._detect_disk_type(part.device)
                    disk_type = f"local {sub_type}"

                disks.append({
                    "mount": part.mountpoint,
                    "device": part.device,
                    "fstype": fstype,
                    "type": disk_type,
                    "total": usage.total,
                    "used": usage.used,
                    "percent": usage.percent
                })
            except PermissionError:
                continue

        metrics["disks"] = disks

        # --- Network ---
        net_io = psutil.net_io_counters(pernic=True)
        net_addrs = psutil.net_if_addrs()
        net_stats = psutil.net_if_stats()
        networks = []

        for iface, addrs in net_addrs.items():
            # Skip loopback
            if iface == "lo":
                continue

            # Verify UP state
            if iface in net_stats and not net_stats[iface].isup:
                continue

            ip_addr = None
            mac_addr = None

            for addr in addrs:
                if addr.family == socket.AF_INET:
                    ip_addr = addr.address
                elif addr.family == psutil.AF_LINK:
                    mac_addr = addr.address

            if not ip_addr:
                continue

            io = net_io.get(iface)
            speed = net_stats[iface].speed if iface in net_stats else 0

            networks.append({
                "interface": iface,
                "ip": ip_addr,
                "mac": mac_addr,
                "rx_bytes": io.bytes_recv if io else 0,
                "tx_bytes": io.bytes_sent if io else 0,
                "speed_mbps": speed
            })

        metrics["network"] = networks

        # --- GPU ---
        metrics["gpus"] = self._get_gpu_metrics(self._gpu_info)

        return metrics


from .plugin_session_base import PluginSession


class SysInfoSession(PluginSession):
    """
    SysInfo session (Service-side).

    Provides methods to gather system metrics.
    """
    def __init__(self, sid: str, provider: SysInfoProvider):
        super().__init__(sid)
        self._provider = provider

    async def get_metrics(self) -> dict:
        """
        Return current system metrics.
        """
        self._check_active()
        return self._provider.get_metrics()



class SysInfoClient(PluginClient):
    """
    Client-side interface for the SysInfo plugin.
    """

    def homedir(self) -> str:
        """Return the home directory of the edge-side process.

        No session is required.
        """
        resp = self._http.get(self._url('homedir'))
        self._raise(resp, 'homedir')
        return resp.json()['homedir']

    def host_role(self) -> dict:
        """Return ``{'role', 'scheduler', 'job_id'}`` for the edge host.

        ``role`` is one of ``'bridge'``, ``'login'`` or ``'compute'``.
        ``scheduler`` is ``'slurm' | 'pbs' | 'lsf' | None`` and ``job_id``
        is the allocation id (``None`` outside an allocation).

        No session is required.
        """
        resp = self._http.get(self._url('host_role'))
        self._raise(resp, 'host_role')
        return resp.json()

    def get_metrics(self) -> dict:
        """
        Return current system metrics.
        """
        self._require_session()

        url = self._url(f"metrics/{self.sid}")
        resp = self._http.get(url)
        self._raise(resp)
        return resp.json()


class PluginSysInfo(Plugin):
    """
    SysInfo plugin for Radical Edge.

    Provides system hardware configuration and resource utilization metrics.
    """

    plugin_name = "sysinfo"
    session_class = SysInfoSession
    client_class = SysInfoClient
    version = '0.0.1'

    ui_config = {
        "icon": "🖥️",
        "title": "System Info",
        "description": "Live CPU, memory, disk, network and GPU metrics.",
        "refresh_button": True,
        "monitors": [{
            "id": "metrics",
            "title": "System Metrics",
            "type": "metrics",
            "css_class": "sysinfo-content",
            "auto_load": "metrics/{sid}"
        }]
    }

    def __init__(self, app: FastAPI):
        """
        Initialize the SysInfo plugin.
        """
        super().__init__(app, 'sysinfo')

        self._provider = SysInfoProvider()

        # Start background prefetch for hardware detection
        self._provider.start_prefetch()

        # Register routes
        self.add_route_get('homedir',        self.homedir_endpoint)
        self.add_route_get('host_role',      self.host_role_endpoint)
        self.add_route_get('metrics/{sid}',  self.get_metrics_endpoint)

    def _create_session(self, sid: str, **kwargs) -> SysInfoSession:
        """
        Custom session creation to pass the provider.
        """
        return SysInfoSession(sid, self._provider)

    async def homedir_endpoint(self, request: Request) -> dict:
        """Return the home directory of the edge-side process."""
        return {'homedir': os.path.expanduser('~')}

    async def host_role_endpoint(self, request: Request) -> dict:
        """Return the role of the host this edge runs on.

        Returned fields:

        - ``role``          — ``bridge`` / ``login`` / ``compute`` /
                              ``standalone``.
        - ``scheduler``     — the batch system's full name (e.g.
                              ``'slurm'``, ``'pbs'``, ``'pbs-aurora'``,
                              ``'none'``); may be a site-specific
                              subclass identifier.
        - ``psij_executor`` — the corresponding PsiJ executor name
                              (``'slurm'``, ``'pbs'``, ``'local'``).
                              Use this when actually submitting via
                              PsiJ; ``scheduler`` may be more specific.
        - ``job_id``        — current allocation id on compute nodes,
                              ``None`` everywhere else.

        Detection logic lives in :func:`utils.host_role`; this route
        is just a wire surface for it.
        """
        from .utils import host_role
        return host_role(self._app)

    async def get_metrics_endpoint(self, request: Request) -> dict:
        """
        Return current system metrics for the specified session.
        """
        sid = request.path_params['sid']
        return await self._forward(sid, SysInfoSession.get_metrics)

