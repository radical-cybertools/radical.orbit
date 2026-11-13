"""Small, broadly-used helpers shared across the radical.orbit package.

House rule: only stateless, no-side-effect-at-import helpers belong here.
Anything with state, threads, side effects, or non-trivial domain logic
belongs in its own module.
"""

import hmac
import os
import socket
import ssl
import stat
import sys

from pathlib import Path
from typing  import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
#  Broker connection config (consumer side: endpoint / client / cert+key reads).
#
#  Env vars:
#    RADICAL_ORBIT_BROKER_URL  — broker URL for endpoints / clients to connect to
#    RADICAL_ORBIT_BROKER_CERT — TLS cert path (broker serves with it; endpoints /
#                          clients verify against it)
#    RADICAL_ORBIT_BROKER_KEY  — TLS key path (broker only)
#
#  Fallback files (placed by the operator — the ``~/.radical/orbit`` config
#  is read-only for the software; nothing below is ever auto-written):
#    ~/.radical/orbit/broker_cert.pem
#    ~/.radical/orbit/broker_key.pem
#    ~/.radical/orbit/broker.token
#
#  Precedence (cert / key / token): CLI arg > env var > file > error.
#  The URL has *no* file fallback — it always comes explicitly, via API /
#  CLI arg or $RADICAL_ORBIT_BROKER_URL (a stale on-disk URL was a recurring
#  source of confusion; certs and tokens are stable material, URLs are not).
#
#  The broker process itself does NOT consume a URL — it derives its
#  advertised URL from its own (host, port).
# ─────────────────────────────────────────────────────────────────────────────

# The defaults under ``~/.radical/orbit`` are resolved per call, from the
# current home directory: use the accessors below.  These module attributes
# are override hooks (``None``: derived); a per-file override wins over a
# ``DEFAULT_DIR`` override, which wins over the home directory.
DEFAULT_DIR  = None
CERT_FILE    = None
KEY_FILE     = None
TOKEN_FILE   = None


def orbit_dir() -> Path:
    """The ORBIT config/state root, ``~/.radical/orbit`` (per call)."""
    if DEFAULT_DIR is not None:
        return Path(DEFAULT_DIR).expanduser()
    return Path.home() / '.radical' / 'orbit'


def allowed_bases() -> list:
    """The roots a user-supplied file path may live under (staging,
    federation scratch): ``~`` and ``/tmp``, both through ``realpath`` (a
    platform where ``/tmp`` is a symlink, ``/private/tmp``, must not reject
    every path).  Per call."""
    return [os.path.realpath(os.path.expanduser('~')),
            os.path.realpath('/tmp')]


def cert_file() -> Path:
    """Default broker TLS cert file."""
    return Path(CERT_FILE).expanduser() if CERT_FILE is not None \
        else orbit_dir() / 'broker_cert.pem'


def key_file() -> Path:
    """Default broker TLS key file."""
    return Path(KEY_FILE).expanduser() if KEY_FILE is not None \
        else orbit_dir() / 'broker_key.pem'


def token_file() -> Path:
    """Default broker ingress token file."""
    return Path(TOKEN_FILE).expanduser() if TOKEN_FILE is not None \
        else orbit_dir() / 'broker.token'


ENV_URL      = 'RADICAL_ORBIT_BROKER_URL'
ENV_CERT     = 'RADICAL_ORBIT_BROKER_CERT'
ENV_KEY      = 'RADICAL_ORBIT_BROKER_KEY'
ENV_TOKEN    = 'RADICAL_ORBIT_BROKER_TOKEN'


def _env(name: str) -> str:
    """Read env var *name*, stripped of surrounding whitespace."""
    return os.environ.get(name, '').strip()


# WebSocket permessage-deflate (RFC 7692), negotiated per connection: frames
# are compressed only when the dialling side offers it and the accepting side
# accepts it, so mixed settings interoperate.  Deflate dominates large
# payloads on fast links (1 MiB put/get p50 ~100 ms with, 16-19 ms without,
# on Perlmutter), so a runtime offers it in ``auto`` mode only when the
# broker looks remote (TCP connect time >= a threshold).
ENV_WS_COMPRESSION        = 'RADICAL_ORBIT_WS_COMPRESSION'
ENV_WS_COMPRESSION_RTT_MS = 'RADICAL_ORBIT_WS_COMPRESSION_RTT_MS'
WS_COMPRESSION_RTT_MS     = 10.0

_WS_MODES = {'1': 'on',  'true':  'on',  'on':  'on',  'yes': 'on',
             '0': 'off', 'false': 'off', 'off': 'off', 'no':  'off',
             'auto': 'auto'}


def resolve_ws_compression(value: Any = None, default: str = 'auto') -> str:
    """Return ``'on'`` / ``'off'`` / ``'auto'`` for the deflate switch.

    Explicit *value* > ``$RADICAL_ORBIT_WS_COMPRESSION`` > *default*.
    *value* may be a bool (``True`` = on, ``False`` = off), a mode string, or
    ``None`` (not set).  An unrecognised value raises ``ValueError``.
    """
    if isinstance(value, bool):
        return 'on' if value else 'off'
    src, raw = ('setting', value) if value is not None \
        else (f'${ENV_WS_COMPRESSION}', _env(ENV_WS_COMPRESSION))
    raw = str(raw).strip().lower()
    if not raw:
        return default
    if raw not in _WS_MODES:
        raise ValueError(f"ws_compression {src}={raw!r}: expected on/off/auto "
                         f"(or 1/true/yes, 0/false/no)")
    return _WS_MODES[raw]


def resolve_ws_compression_rtt_ms(value: Optional[float] = None) -> float:
    """``auto`` threshold (ms): explicit > env > ``WS_COMPRESSION_RTT_MS``."""
    if value is not None:
        return float(value)
    raw = _env(ENV_WS_COMPRESSION_RTT_MS)
    if not raw:
        return WS_COMPRESSION_RTT_MS
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"${ENV_WS_COMPRESSION_RTT_MS}={raw!r}: expected a "
                         f"number of milliseconds") from None


# The WebSocket keepalive deadline (s): a socket whose pong is this late is
# closed, on the broker (uvicorn ``ws_ping_timeout``) and in every runtime
# (``websockets`` ``ping_timeout``).  One variable for both ends: a broker
# whose loop stalls longer than this under load drops every connection at
# once (a broker hosting hundreds of in-process participants, e.g. one per
# DT service session), so a deployment raises it on both sides together.
ENV_PING_TIMEOUT = 'RADICAL_ORBIT_PING_TIMEOUT'
PING_TIMEOUT     = 3.0
PING_TIMEOUT_UNSET = object()   # "not set": the variable, then the default


def resolve_ping_timeout(value: Any = PING_TIMEOUT_UNSET) -> Optional[float]:
    """Keepalive deadline (s): explicit > ``$RADICAL_ORBIT_PING_TIMEOUT`` >
    ``PING_TIMEOUT``.  An explicit ``None`` disables the deadline, as uvicorn
    and ``websockets`` take it.  A value that is not a positive number raises
    ``ValueError``."""
    if value is None:
        return None
    if value is PING_TIMEOUT_UNSET:
        src, raw = f'${ENV_PING_TIMEOUT}', _env(ENV_PING_TIMEOUT)
        if not raw:
            return PING_TIMEOUT
    else:
        src, raw = 'setting', value
    try:
        timeout = float(raw)
    except (TypeError, ValueError):
        timeout = -1.0
    if not timeout > 0:
        raise ValueError(f"ping_timeout {src}={raw!r}: expected a positive "
                         f"number of seconds")
    return timeout


# Cookie the browser/SSE path carries (set by the broker's POST /auth).  The
# token never lives in a query string, only this HttpOnly cookie or a
# request header.
AUTH_COOKIE  = 'orbit_broker_token'


def _outbound_ipv4() -> Optional[str]:
    """Return the IPv4 address this host uses for outbound traffic.

    Uses the standard "open a UDP socket to a public IP and read the
    local end" trick — no packets are actually sent.  Returns ``None``
    on any failure (no network, IPv6-only, restricted egress).
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('1.1.1.1', 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return None


def public_url_forms(host: str, port: int, *,
                     scheme: str = 'https') -> List[str]:
    """Return URLs to advertise for a broker listening on ``(host, port)``.

    For wildcard binds (``''`` / ``0.0.0.0`` / ``::``), returns up to
    two forms:

      * ``<scheme>://<fqdn>:<port>``  — DNS-resolvable canonical form.
      * ``<scheme>://<ipv4>:<port>``  — fallback for hosts where FQDN
                                         isn't routable from the client.

    For specific binds (any IP or hostname), returns a single form
    using that host literally — including loopback (``127.0.0.1``)
    and IPv6 (which is bracket-wrapped for URL embedding).

    Always returns at least one form.
    """
    if host not in ('', '0.0.0.0', '::'):
        # Specific bind — advertise it as-is, bracketing IPv6 literals.
        host_in_url = f'[{host}]' if ':' in host else host
        return [f'{scheme}://{host_in_url}:{port}']

    forms = []
    fqdn  = socket.getfqdn()
    if fqdn and fqdn not in ('localhost', 'localhost.localdomain') \
            and '.' in fqdn:
        forms.append(f'{scheme}://{fqdn}:{port}')

    ipv4 = _outbound_ipv4()
    if ipv4:
        ipv4_url = f'{scheme}://{ipv4}:{port}'
        if ipv4_url not in forms:
            forms.append(ipv4_url)

    if not forms:
        # Last-ditch fallback so callers always get something printable.
        forms.append(f'{scheme}://{socket.gethostname() or "localhost"}:{port}')

    return forms


def resolve_broker_url(cli: Optional[str] = None) -> Tuple[str, str]:
    """Resolve the broker URL for a *consumer* (endpoint / client).

    Precedence: CLI arg > ``$RADICAL_ORBIT_BROKER_URL``.  There is
    deliberately no file fallback — a URL is volatile (hosts and ports
    change) and always passed explicitly.

    Returns ``(url, source)`` with source one of ``'cli'`` / ``'env'``.
    Raises ``ValueError`` if no source resolves.

    The broker process itself does *not* call this — it derives its
    advertised URL from its own ``(host, port)``.
    """
    if cli:
        return cli.strip().rstrip('/'), 'cli'
    env_url = _env(ENV_URL)
    if env_url:
        return env_url.rstrip('/'), 'env'
    raise ValueError(f"Broker URL required (no CLI arg, ${ENV_URL} unset)")


def _resolve_path_value(cli: Optional[str], env_var: str,
                        file_path: Path) -> Tuple[Optional[Path], str]:
    """CLI > env > file precedence for a filesystem path.

    ``~`` is expanded: the shell does not expand it after ``--cert=`` /
    ``--key=`` or inside env vars, so the tool must.
    """
    if cli:
        return Path(cli).expanduser(), 'cli'
    env_val = os.environ.get(env_var, '').strip()
    if env_val:
        return Path(env_val).expanduser(), 'env'
    if file_path.exists():
        return file_path, 'file'
    return None, ''


TLS_RECIPE = '''\
To create a self-signed cert/key pair at the default location:

  mkdir -p ~/.radical/orbit
  openssl req -x509 -newkey rsa:4096 -nodes \\
      -keyout ~/.radical/orbit/broker_key.pem \\
      -out    ~/.radical/orbit/broker_cert.pem \\
      -days 365 -subj "/CN=$(hostname -f)"
  chmod 600 ~/.radical/orbit/broker_key.pem
'''


def resolve_broker_cert(cli: Optional[str] = None) -> Tuple[Path, str]:
    """Resolve the TLS cert path.

    Validates the file is loadable as a CA cert
    (``ssl.create_default_context().load_verify_locations``).
    Brokers that need to pair cert + key call :func:`resolve_broker_key`
    with the resolved cert path; that does the server-side pairing
    via ``load_cert_chain``.

    Precedence: CLI arg > ``$RADICAL_ORBIT_BROKER_CERT`` >
    ``~/.radical/orbit/broker_cert.pem``.

    Returns ``(path, source)``.  Raises ``ValueError`` if no source
    yields a path, ``FileNotFoundError`` if the path does not exist,
    ``ssl.SSLError`` if the file is not a valid cert.
    """
    path, source = _resolve_path_value(cli, ENV_CERT, cert_file())
    if path is None:
        raise ValueError(f"TLS cert required (no CLI arg, ${ENV_CERT} unset, "
                         f"no file at {cert_file()})")
    if not path.exists():
        raise FileNotFoundError(f"TLS cert not found: {path}")
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(str(path))
    return path, source


def resolve_broker_key(cli: Optional[str] = None, *,
                       cert: Optional[Path] = None
                       ) -> Tuple[Path, str]:
    """Resolve the TLS key path (broker-only).

    Enforces mode ``0o600`` — refuses to start if the file is more
    permissive (the broker's TLS private key must not be world-readable).

    If *cert* is supplied, validates that the cert/key pair loads as
    a server-side ``SSLContext``.

    Returns ``(path, source)``.
    """
    path, source = _resolve_path_value(cli, ENV_KEY, key_file())
    if path is None:
        raise ValueError(
            f"TLS key required for role='broker' (no CLI arg, "
            f"${ENV_KEY} unset, no file at {key_file()})")
    if not path.exists():
        raise FileNotFoundError(f"TLS key not found: {path}")

    # Mode check: refuse open keys.  Look at the actual file mode bits;
    # ignore type bits.  ``S_IRWXG | S_IRWXO`` covers any group/other
    # read/write/execute permission.
    mode = path.stat().st_mode & 0o777
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError(
            f"TLS key file is too permissive (mode {oct(mode)}): {path} — "
            f"must be 0o600 or stricter")

    if cert is not None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(cert), str(path))

    return path, source


# ─────────────────────────────────────────────────────────────────────────────
#  Broker ingress auth token.
#
#  A shared bearer token gates the broker's HTTP ingress and the endpoint
#  ``/register`` handshake.  It is the deployment-scoped credential (same trust
#  model as the cert): clients/endpoints present it, the broker verifies it.
#
#    Env var:  RADICAL_ORBIT_BROKER_TOKEN
#    File:     ~/.radical/orbit/broker.token   (mode 0600, operator-placed)
#
#  Precedence (both sides): CLI > env > file.  A missing token is *not* an
#  error on the consumer side — the broker may run with auth off.  The broker
#  *requires* a token when auth is enabled (``auth=False`` / ``--no-auth`` is
#  the explicit local-dev escape hatch); nothing is ever generated or written
#  by code — ``TOKEN_RECIPE`` below is the one-time operator step.
# ─────────────────────────────────────────────────────────────────────────────

TOKEN_RECIPE = '''\
To create a shared ingress token at the default location:

  mkdir -p ~/.radical/orbit
  python3 -c "import secrets; print(secrets.token_urlsafe(32))" \\
      > ~/.radical/orbit/broker.token
  chmod 600 ~/.radical/orbit/broker.token
'''


def _read_token_file(path: Optional[Path] = None) -> Optional[str]:
    """Read the token file, stripped.  ``None`` if absent/empty.

    Only ``FileNotFoundError`` is swallowed: a present-but-unreadable file
    (e.g. ``PermissionError``) is a real misconfiguration that should
    surface loudly rather than be masked as "no token".
    """
    if path is None:
        path = token_file()
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        return None
    return text or None


def resolve_broker_token(cli: Optional[str] = None) -> Tuple[Optional[str], str]:
    """Resolve the broker token for a *consumer* (endpoint / client).

    Precedence: CLI arg > ``$RADICAL_ORBIT_BROKER_TOKEN`` >
    ``~/.radical/orbit/broker.token``.  Returns ``(token, source)`` with source
    one of ``'cli'`` / ``'env'`` / ``'file'``, or ``(None, '')`` when none is
    configured (a missing token is not fatal — the broker may have auth off).
    """
    if cli:
        return cli.strip(), 'cli'
    env_tok = _env(ENV_TOKEN)
    if env_tok:
        return env_tok, 'env'
    file_tok = _read_token_file()
    if file_tok:
        return file_tok, 'file'
    return None, ''


def tokens_match(provided: Any, expected: Any) -> bool:
    """Constant-time token comparison; ``False`` if either side is empty or
    not a string (e.g. a non-string token in a JSON ``/register`` payload,
    which would otherwise raise ``TypeError`` in ``hmac.compare_digest``)."""
    if not isinstance(provided, str) or not isinstance(expected, str):
        return False
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided, expected)


# Host roles returned by :func:`host_role`; plugins name them in
# ``Plugin.default_roles``.
HOST_ROLES = ('broker', 'login', 'compute', 'standalone')


def host_role(app: Any) -> Dict[str, Any]:
    """Classify the host that *app* is running on.

    Single source of truth for role / scheduler / executor detection
    across the codebase.  Returns a dict with these fields:

    - ``role``           — one of :data:`HOST_ROLES`.
    - ``scheduler``      — the detected batch system's full name (e.g.
                           ``'slurm'``, ``'pbs'``, ``'pbs-aurora'``,
                           ``'none'``).
    - ``psij_executor``  — the corresponding PsiJ executor name
                           (``'slurm'`` / ``'pbs'`` / ``'local'``).
    - ``job_id``         — current allocation id on compute nodes,
                           ``None`` everywhere else.
    - ``python_version`` — the host's Python interpreter version as
                           ``'<major>.<minor>.<micro>'``.  Consumed by
                           remote-execution backends (e.g. rhapsody's
                           Endpoint backend) to gate cloudpickle-based
                           function-task submission against version
                           skew between client and endpoint.

    Args:
        app: a FastAPI application.  ``app.state.is_broker`` (when
             present and truthy) marks the host as a broker.
    """
    from .batch_system import detect_batch_system
    bs       = detect_batch_system()
    in_alloc = bs.in_allocation()
    if   getattr(app.state, 'is_broker', False): role = 'broker'
    elif in_alloc:                               role = 'compute'
    elif bs.name == 'none':                      role = 'standalone'
    else:                                        role = 'login'
    return {
        'role'          : role,
        'scheduler'     : bs.name,
        'psij_executor' : bs.psij_executor,
        'job_id'        : bs.job_id() if in_alloc else None,
        'python_version': '%d.%d.%d' % (sys.version_info.major,
                                         sys.version_info.minor,
                                         sys.version_info.micro),
    }
