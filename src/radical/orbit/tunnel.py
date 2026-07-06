"""SSH tunnel spawning helpers.

Two directions are supported, selected per-target by the caller:

* **Forward** (compute -> login): the endpoint service running inside a
  batch job opens an outbound SSH tunnel back to the submitting login
  node.  Used on sites that allow outbound compute -> login SSH and
  block the reverse direction (Aurora, Perlmutter).
  Spawned by :func:`spawn_tunnel`::

      ssh -L <port>:<broker_host>:<broker_port> <login_host> -N

* **Reverse** (login -> compute): the parent endpoint running on the
  login node opens an SSH connection to the compute node, asking
  ``sshd`` there to listen on a remote port that forwards back to
  the broker.  Used on sites that allow login -> compute SSH and
  block compute -> login (Odo).  Spawned by :func:`spawn_reverse_tunnel`::

      ssh -R 0:<broker_host>:<broker_port> <compute_host> -N

In both cases the resulting port (forward: local on compute; reverse:
remote on compute, allocated by sshd) is written to a rendezvous file
``~/.radical/orbit/tunnels/<endpoint_name>.port`` on the shared filesystem
so the *consumer* (always the child endpoint on the compute node) can
read the same path regardless of which side spawned the SSH.
"""

import json
import logging
import os
import pathlib
import re
import socket
import subprocess
import threading
import time

log = logging.getLogger('radical.orbit')


RELAY_BASE = pathlib.Path.home() / '.radical' / 'orbit' / 'tunnels'

# Matches sshd's "Allocated port N for remote forward to ..." line that
# OpenSSH 7.6+ prints on stderr when ``-R 0:host:port`` is used.
_ALLOCATED_PORT_RE = re.compile(r'Allocated port (\d+) for remote forward')


def relay_dir() -> pathlib.Path:
    """Return (and create) the rendezvous directory on the shared fs."""
    RELAY_BASE.mkdir(parents=True, exist_ok=True)
    return RELAY_BASE


def _rendezvous_path(endpoint_name: str, suffix: str) -> pathlib.Path:
    """Return the ``<endpoint_name>.<suffix>`` rendezvous file path."""
    return relay_dir() / f'{endpoint_name}.{suffix}'


def rendezvous_read(endpoint_name: str, suffix: str = 'port') -> 'int | None':
    """Return the integer in the ``.port`` (or ``.pid``) rendezvous file.

    Returns ``None`` when the file is absent or does not hold an integer.
    """
    try:
        return int(_rendezvous_path(endpoint_name, suffix).read_text().strip())
    except (ValueError, OSError):
        return None


def rendezvous_wait(endpoint_name: str) -> 'dict | None':
    """Return the child's ``.req`` payload dict if present, else ``None``.

    A ``readdir`` on the parent directory precedes the read to defeat NFSv3
    negative-lookup caching: after a first miss the cached ENOENT can keep
    reporting the file absent for tens of seconds even once the child has
    written it on the shared filesystem (observed on ODO), and a readdir
    forces fresh directory attributes.

    Raises:
        ValueError: the ``.req`` file exists but is not valid JSON.
        OSError:    the ``.req`` file exists but could not be read.
    """
    req_file = _rendezvous_path(endpoint_name, 'req')
    try:
        present = req_file.name in set(os.listdir(str(req_file.parent)))
    except OSError:
        present = False
    if not present:
        return None
    return json.loads(req_file.read_text())


def rendezvous_clear(endpoint_name: str) -> None:
    """Remove any stale ``.port``/``.pid``/``.req`` rendezvous files."""
    for suffix in ('port', 'pid', 'req'):
        _rendezvous_path(endpoint_name, suffix).unlink(missing_ok=True)


def _pick_free_local_port() -> int:
    """Bind to port 0 on loopback and immediately release to learn a free port.

    There's a small TOCTOU window between this returning and SSH binding
    the port; in practice nothing else races for the same port on a
    compute node and SSH binds within milliseconds.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _wait_for_listener(port: int, proc, timeout: float,
                       log_lines: list) -> None:
    """Block until ``127.0.0.1:port`` accepts a TCP connection.

    Raises :class:`RuntimeError` if *proc* exits before the listener
    comes up, or if *timeout* seconds elapse first.  *log_lines* is the
    list being filled by the stderr-drain thread; its tail is included
    in the error message.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            tail = '\n'.join(log_lines[-20:])
            raise RuntimeError(
                f"SSH tunnel exited (rc={proc.returncode}) before listener "
                f"came up\nSSH output (last 20 lines):\n{tail}")
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.3)
    tail = '\n'.join(log_lines[-20:])
    raise RuntimeError(
        f"SSH tunnel listener on 127.0.0.1:{port} did not come up within "
        f"{timeout:.0f}s\nSSH output (last 20 lines):\n{tail}")


def _start_stderr_drain(proc, log_lines: list) -> threading.Thread:
    """Start a daemon thread that drains *proc.stderr* into *log_lines*.

    Without this the SSH process blocks once the stderr pipe fills.
    """
    def _drain():
        try:
            for raw in proc.stderr:
                log_lines.append(raw.decode('utf-8', errors='replace').rstrip())
        except (OSError, ValueError):
            pass
    t = threading.Thread(target=_drain, daemon=True)
    t.start()
    return t


def spawn_tunnel(login_host: str, broker_host: str, broker_port: int,
                 endpoint_name: str, listen_timeout: float = 15.0) -> tuple:
    """Open a compute -> login ssh -L tunnel and return ``(proc, port)``.

    The port is pre-picked locally and passed to ``ssh -L``; the SSH
    process runs in a new session so it survives the caller's lifetime.
    Rendezvous files ``<endpoint_name>.port`` and ``<endpoint_name>.pid`` are
    written under :func:`relay_dir`.

    Args:
        login_host:     Host to SSH *to* (the submitting login node).
        broker_host:    Broker hostname (the destination of the forward).
        broker_port:    Broker port.
        endpoint_name:      Used in log messages and rendezvous file names.
        listen_timeout: Seconds to wait for the local listener to come up.

    Returns:
        ``(proc, port)`` — the :class:`subprocess.Popen` instance and the
        local port the tunnel is listening on.

    Raises:
        RuntimeError: SSH exited before the listener came up, or the
            listener didn't open within *listen_timeout* seconds.
    """
    port = _pick_free_local_port()
    forward = f'{port}:{broker_host}:{broker_port}'

    ssh_cmd = [
        'ssh', '-N',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'BatchMode=yes',
        '-o', 'ServerAliveInterval=10',
        '-o', 'ServerAliveCountMax=3',
        '-o', 'ExitOnForwardFailure=yes',
        '-L', forward,
        login_host,
    ]
    log.info("[tunnel] Spawning: %s", ' '.join(ssh_cmd))

    proc = subprocess.Popen(
        ssh_cmd,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    log_lines: list = []
    _start_stderr_drain(proc, log_lines)
    _wait_for_listener(port, proc, listen_timeout, log_lines)

    log.info("[tunnel] SSH listener active on 127.0.0.1:%d for endpoint %r",
             port, endpoint_name)

    _rendezvous_path(endpoint_name, 'port').write_text(str(port))
    _rendezvous_path(endpoint_name, 'pid').write_text(str(proc.pid))

    return proc, port


def cleanup_tunnel(proc, endpoint_name: str = '') -> None:
    """Terminate an SSH tunnel process cleanly."""
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    if endpoint_name:
        log.info("[tunnel] Terminated SSH process for endpoint %r", endpoint_name)


def _parse_allocated_port(proc, log_lines: list, timeout: float) -> int:
    """Wait for OpenSSH to print "Allocated port N" on stderr.

    Starts the shared stderr-drain thread and scans the lines it
    accumulates for the allocated-port line until *timeout* seconds
    elapse or the SSH process exits.  Raises :class:`RuntimeError` on
    timeout or premature SSH exit.
    """
    _start_stderr_drain(proc, log_lines)
    deadline = time.monotonic() + timeout
    scanned  = 0
    while time.monotonic() < deadline:
        while scanned < len(log_lines):
            m = _ALLOCATED_PORT_RE.search(log_lines[scanned])
            scanned += 1
            if m:
                return int(m.group(1))
        if proc.poll() is not None:
            tail = '\n'.join(log_lines[-20:])
            raise RuntimeError(
                f"SSH reverse tunnel exited (rc={proc.returncode}) before "
                f"allocating a port\nSSH output (last 20 lines):\n{tail}")
        time.sleep(0.1)
    tail = '\n'.join(log_lines[-20:])
    raise RuntimeError(
        f"SSH reverse tunnel did not allocate a port within {timeout:.0f}s\n"
        f"SSH output (last 20 lines):\n{tail}")


def spawn_reverse_tunnel(compute_host: str, broker_host: str, broker_port: int,
                         endpoint_name: str, allocate_timeout: float = 30.0) -> tuple:
    """Open a login -> compute ssh -R tunnel and return ``(proc, port)``.

    The remote sshd allocates a free port (``-R 0:...``) and prints it
    on stderr.  We parse that line, drop it into the rendezvous file,
    and continue draining stderr in a daemon thread.

    Args:
        compute_host:     Compute node hostname to SSH *to* (the child's host).
        broker_host:      Broker hostname (the destination of the forward).
        broker_port:      Broker port.
        endpoint_name:        Used in log messages and rendezvous file names.
        allocate_timeout: Seconds to wait for "Allocated port N" on stderr.

    Returns:
        ``(proc, port)`` — the :class:`subprocess.Popen` instance and the
        remote port that sshd listens on for the child to connect to.

    Raises:
        RuntimeError: SSH exited before allocating a port, or the
            allocated-port line didn't appear within *allocate_timeout*.
    """
    forward = f'0:{broker_host}:{broker_port}'

    ssh_cmd = [
        'ssh', '-N',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'BatchMode=yes',
        '-o', 'ServerAliveInterval=10',
        '-o', 'ServerAliveCountMax=3',
        '-o', 'ExitOnForwardFailure=yes',
        # ``-v`` makes OpenSSH print "Allocated port N for remote forward"
        # on stderr, which is the only way to discover the port sshd
        # picked.  Without -v that line is suppressed.
        '-v',
        '-R', forward,
        compute_host,
    ]
    log.info("[tunnel] Spawning reverse: %s", ' '.join(ssh_cmd))

    proc = subprocess.Popen(
        ssh_cmd,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    # ``_parse_allocated_port`` starts the stderr-drain thread itself and
    # keeps it running, so the SSH process never blocks on a full pipe.
    log_lines: list = []
    port = _parse_allocated_port(proc, log_lines, allocate_timeout)

    log.info("[tunnel] Reverse SSH allocated remote port %d on %s for endpoint %r",
             port, compute_host, endpoint_name)

    _rendezvous_path(endpoint_name, 'port').write_text(str(port))
    _rendezvous_path(endpoint_name, 'pid').write_text(str(proc.pid))

    return proc, port
