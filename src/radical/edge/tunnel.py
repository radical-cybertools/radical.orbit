"""SSH tunnel spawning helpers.

Two directions are supported, selected per-target by the caller:

* **Forward** (compute -> login): the edge service running inside a
  batch job opens an outbound SSH tunnel back to the submitting login
  node.  Used on sites that allow outbound compute -> login SSH and
  block the reverse direction (Aurora, Perlmutter).
  Spawned by :func:`spawn_tunnel`::

      ssh -L <port>:<bridge_host>:<bridge_port> <login_host> -N

* **Reverse** (login -> compute): the parent edge running on the
  login node opens an SSH connection to the compute node, asking
  ``sshd`` there to listen on a remote port that forwards back to
  the bridge.  Used on sites that allow login -> compute SSH and
  block compute -> login (Odo).  Spawned by :func:`spawn_reverse_tunnel`::

      ssh -R 0:<bridge_host>:<bridge_port> <compute_host> -N

In both cases the resulting port (forward: local on compute; reverse:
remote on compute, allocated by sshd) is written to a rendezvous file
``~/.radical/edge/tunnels/<edge_name>.port`` on the shared filesystem
so the *consumer* (always the child edge on the compute node) can
read the same path regardless of which side spawned the SSH.
"""

import logging
import pathlib
import socket
import subprocess
import threading
import time

log = logging.getLogger('radical.edge')


RELAY_BASE = pathlib.Path.home() / '.radical' / 'edge' / 'tunnels'


def relay_dir() -> pathlib.Path:
    """Return (and create) the rendezvous directory on the shared fs."""
    RELAY_BASE.mkdir(parents=True, exist_ok=True)
    return RELAY_BASE


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


def spawn_tunnel(login_host: str, bridge_host: str, bridge_port: int,
                 edge_name: str, listen_timeout: float = 15.0) -> tuple:
    """Open a compute -> login ssh -L tunnel and return ``(proc, port)``.

    The port is pre-picked locally and passed to ``ssh -L``; the SSH
    process runs in a new session so it survives the caller's lifetime.
    Rendezvous files ``<edge_name>.port`` and ``<edge_name>.pid`` are
    written under :func:`relay_dir`.

    Args:
        login_host:     Host to SSH *to* (the submitting login node).
        bridge_host:    Bridge hostname (the destination of the forward).
        bridge_port:    Bridge port.
        edge_name:      Used in log messages and rendezvous file names.
        listen_timeout: Seconds to wait for the local listener to come up.

    Returns:
        ``(proc, port)`` — the :class:`subprocess.Popen` instance and the
        local port the tunnel is listening on.

    Raises:
        RuntimeError: SSH exited before the listener came up, or the
            listener didn't open within *listen_timeout* seconds.
    """
    port = _pick_free_local_port()
    forward = f'{port}:{bridge_host}:{bridge_port}'

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

    log.info("[tunnel] SSH listener active on 127.0.0.1:%d for edge %r",
             port, edge_name)

    rdir = relay_dir()
    (rdir / f'{edge_name}.port').write_text(str(port))
    (rdir / f'{edge_name}.pid').write_text(str(proc.pid))

    return proc, port


def cleanup_tunnel(proc, edge_name: str = '') -> None:
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
    if edge_name:
        log.info("[tunnel] Terminated SSH process for edge %r", edge_name)


# ─────────────────────────────────────────────────────────────────────────────
# Reverse tunnel (login -> compute)
# ─────────────────────────────────────────────────────────────────────────────

import re

# Matches sshd's "Allocated port N for remote forward to ..." line that
# OpenSSH 7.6+ prints on stderr when ``-R 0:host:port`` is used.
_ALLOCATED_PORT_RE = re.compile(r'Allocated port (\d+) for remote forward')


def _parse_allocated_port(proc, log_lines: list, timeout: float) -> int:
    """Wait for OpenSSH to print "Allocated port N" on stderr.

    Drains stderr line-by-line in the calling thread (separate stderr
    drain not started yet, since we need to inspect the lines here).
    Raises :class:`RuntimeError` on timeout or premature SSH exit.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.stderr is None:
            raise RuntimeError("SSH stderr unavailable; cannot parse allocated port")
        line = proc.stderr.readline()
        if not line:
            if proc.poll() is not None:
                tail = '\n'.join(log_lines[-20:])
                raise RuntimeError(
                    f"SSH reverse tunnel exited (rc={proc.returncode}) "
                    f"before allocating a port\nSSH output (last 20 lines):"
                    f"\n{tail}")
            time.sleep(0.05)
            continue
        text = line.decode('utf-8', errors='replace').rstrip()
        log_lines.append(text)
        m = _ALLOCATED_PORT_RE.search(text)
        if m:
            return int(m.group(1))
    tail = '\n'.join(log_lines[-20:])
    raise RuntimeError(
        f"SSH reverse tunnel did not allocate a port within {timeout:.0f}s\n"
        f"SSH output (last 20 lines):\n{tail}")


def spawn_reverse_tunnel(compute_host: str, bridge_host: str, bridge_port: int,
                         edge_name: str, allocate_timeout: float = 30.0) -> tuple:
    """Open a login -> compute ssh -R tunnel and return ``(proc, port)``.

    The remote sshd allocates a free port (``-R 0:...``) and prints it
    on stderr.  We parse that line, drop it into the rendezvous file,
    and continue draining stderr in a daemon thread.

    Args:
        compute_host:     Compute node hostname to SSH *to* (the child's host).
        bridge_host:      Bridge hostname (the destination of the forward).
        bridge_port:      Bridge port.
        edge_name:        Used in log messages and rendezvous file names.
        allocate_timeout: Seconds to wait for "Allocated port N" on stderr.

    Returns:
        ``(proc, port)`` — the :class:`subprocess.Popen` instance and the
        remote port that sshd listens on for the child to connect to.

    Raises:
        RuntimeError: SSH exited before allocating a port, or the
            allocated-port line didn't appear within *allocate_timeout*.
    """
    forward = f'0:{bridge_host}:{bridge_port}'

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

    log_lines: list = []
    port = _parse_allocated_port(proc, log_lines, allocate_timeout)
    # Hand stderr off to a background drain thread so the SSH process
    # doesn't block writing to a full pipe later in its life.
    _start_stderr_drain(proc, log_lines)

    log.info("[tunnel] Reverse SSH allocated remote port %d on %s for edge %r",
             port, compute_host, edge_name)

    rdir = relay_dir()
    (rdir / f'{edge_name}.port').write_text(str(port))
    (rdir / f'{edge_name}.pid').write_text(str(proc.pid))

    return proc, port
