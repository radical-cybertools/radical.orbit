"""Small, broadly-used helpers shared across the radical.orbit package.

House rule: only stateless, no-side-effect-at-import helpers belong here.
Anything with state, threads, side effects, or non-trivial domain logic
belongs in its own module.
"""

import hmac
import os
import secrets
import socket
import ssl
import stat
import sys
import tempfile

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
#  Fallback files (placed by the operator; never auto-written from env):
#    ~/.radical/orbit/broker.url
#    ~/.radical/orbit/broker_cert.pem
#    ~/.radical/orbit/broker_key.pem
#
#  Precedence (consumer side): CLI arg > env var > file > error.  Each env var
#  and file also silently falls back to its ``BRIDGE`` predecessor when the
#  ``BROKER`` name is unset/absent, so deployments configured before the rename
#  keep working; only the ``BROKER`` names are ever written.
#
#  The broker process itself does NOT consume a URL — it derives its
#  advertised URL from its own (host, port).  ``broker.url`` is a write-
#  side artefact only: broker writes; endpoints / clients read.
#  Cert / key files are never written by code — operator places them.
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_DIR  = Path.home() / '.radical' / 'orbit'
URL_FILE     = DEFAULT_DIR / 'broker.url'
CERT_FILE    = DEFAULT_DIR / 'broker_cert.pem'
KEY_FILE     = DEFAULT_DIR / 'broker_key.pem'
TOKEN_FILE   = DEFAULT_DIR / 'broker.token'

# Predecessor file names — read as a fallback when the broker-named file is
# absent, so deployments configured before the rename keep working.
URL_FILE_LEGACY   = DEFAULT_DIR / 'bridge.url'
CERT_FILE_LEGACY  = DEFAULT_DIR / 'bridge_cert.pem'
KEY_FILE_LEGACY   = DEFAULT_DIR / 'bridge_key.pem'
TOKEN_FILE_LEGACY = DEFAULT_DIR / 'bridge.token'

ENV_URL      = 'RADICAL_ORBIT_BROKER_URL'
ENV_CERT     = 'RADICAL_ORBIT_BROKER_CERT'
ENV_KEY      = 'RADICAL_ORBIT_BROKER_KEY'
ENV_TOKEN    = 'RADICAL_ORBIT_BROKER_TOKEN'
ENV_NO_AUTH  = 'RADICAL_ORBIT_BROKER_NO_AUTH'

# Predecessor env vars — read as a silent fallback when the broker-named var
# is unset.
ENV_URL_LEGACY     = 'RADICAL_ORBIT_BRIDGE_URL'
ENV_CERT_LEGACY    = 'RADICAL_ORBIT_BRIDGE_CERT'
ENV_KEY_LEGACY     = 'RADICAL_ORBIT_BRIDGE_KEY'
ENV_TOKEN_LEGACY   = 'RADICAL_ORBIT_BRIDGE_TOKEN'
ENV_NO_AUTH_LEGACY = 'RADICAL_ORBIT_BRIDGE_NO_AUTH'


def _env(new: str, legacy: str) -> str:
    """Read env var *new*, falling back silently to its *legacy* predecessor."""
    return (os.environ.get(new, '') or os.environ.get(legacy, '')).strip()


# Cookie the browser/SSE path carries (set by the broker's POST /auth).  The
# token never lives in a query string, only this HttpOnly cookie or a
# request header.
AUTH_COOKIE  = 'orbit_broker_token'


def _read_url_file(path: Optional[Path] = None) -> Optional[str]:
    """Read a URL file, stripped of surrounding whitespace/newlines.

    Resolves ``path`` from the module-level ``URL_FILE`` at call time
    (not at def time) so tests that monkeypatch ``URL_FILE`` see the
    redirected location.
    """
    if path is None:
        path = URL_FILE
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        if path == URL_FILE and URL_FILE_LEGACY.exists():
            try:
                text = URL_FILE_LEGACY.read_text().strip()
            except FileNotFoundError:
                return None
        else:
            return None
    return text or None


def write_broker_url_file(url: str, path: Optional[Path] = None) -> None:
    """Write *url* to *path* atomically (tmp + os.replace).

    Creates parent directories as needed.  Mode 0644.  This is the only
    auto-write of any of the three broker config files; cert and key are
    always operator-placed.
    """
    if path is None:
        path = URL_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.broker.url.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(url.rstrip() + '\n')
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except Exception:
        try:    os.unlink(tmp)
        except FileNotFoundError: pass
        raise


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

    Precedence: CLI arg > ``$RADICAL_ORBIT_BROKER_URL`` > ``~/.radical/orbit/broker.url``.

    Returns ``(url, source)`` with source one of
    ``'cli'`` / ``'env'`` / ``'file'``.  Raises ``ValueError`` if no
    source resolves.

    The broker process itself does *not* call this — it derives its
    advertised URL from its own ``(host, port)``.
    """
    if cli:
        return cli.strip().rstrip('/'), 'cli'
    env_url = _env(ENV_URL, ENV_URL_LEGACY)
    if env_url:
        return env_url.rstrip('/'), 'env'
    file_url = _read_url_file()
    if file_url:
        return file_url.rstrip('/'), 'file'
    raise ValueError(f"Broker URL required (no CLI arg, ${ENV_URL} unset, "
                     f"no file at {URL_FILE})")


def _resolve_path_value(cli: Optional[str], env_var: str,
                        file_path: Path, *,
                        env_legacy: Optional[str]  = None,
                        file_legacy: Optional[Path] = None
                        ) -> Tuple[Optional[Path], str]:
    """CLI > env > file precedence for a filesystem path.

    ``~`` is expanded: the shell does not expand it after ``--cert=`` /
    ``--key=`` or inside env vars, so the tool must.  *env_legacy* /
    *file_legacy* are the pre-rename predecessor names, consulted as a
    silent fallback when the primary is unset/absent.
    """
    if cli:
        return Path(cli).expanduser(), 'cli'
    env_val = os.environ.get(env_var, '').strip()
    if not env_val and env_legacy:
        env_val = os.environ.get(env_legacy, '').strip()
    if env_val:
        return Path(env_val).expanduser(), 'env'
    if file_path.exists():
        return file_path, 'file'
    if file_legacy is not None and file_legacy.exists():
        return file_legacy, 'file'
    return None, ''


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
    path, source = _resolve_path_value(cli, ENV_CERT, CERT_FILE,
                                       env_legacy=ENV_CERT_LEGACY,
                                       file_legacy=CERT_FILE_LEGACY)
    if path is None:
        raise ValueError(f"TLS cert required (no CLI arg, ${ENV_CERT} unset, "
                         f"no file at {CERT_FILE})")
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
    path, source = _resolve_path_value(cli, ENV_KEY, KEY_FILE,
                                       env_legacy=ENV_KEY_LEGACY,
                                       file_legacy=KEY_FILE_LEGACY)
    if path is None:
        raise ValueError(
            f"TLS key required for role='broker' (no CLI arg, "
            f"${ENV_KEY} unset, no file at {KEY_FILE})")
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
#    File:     ~/.radical/orbit/broker.token   (mode 0600)
#    Disable:  RADICAL_ORBIT_BROKER_NO_AUTH=1  (escape hatch for local dev)
#
#  Consumer precedence (endpoint / client): CLI > env > file.  A missing token
#  is *not* an error on the consumer side — the broker may run with auth off.
#  The broker itself uses ``ensure_broker_token``, which generates and writes a
#  token when none is configured.  Both the env var and the file fall back to
#  their pre-rename ``BRIDGE`` predecessors when the ``BROKER`` name is
#  unset/absent, so an existing token keeps authenticating; only the
#  ``broker.token`` file is ever written.
# ─────────────────────────────────────────────────────────────────────────────

def _read_token_file(path: Optional[Path] = None) -> Optional[str]:
    """Read the token file, stripped.  ``None`` if absent/empty.

    Only ``FileNotFoundError`` is swallowed (consistent with ``_read_url_file``):
    a present-but-unreadable file (e.g. ``PermissionError``) is a real
    misconfiguration that should surface loudly rather than be masked as
    "no token" — which would otherwise trigger a regenerate-then-crash-on-write.
    """
    if path is None:
        path = TOKEN_FILE
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        if path == TOKEN_FILE and TOKEN_FILE_LEGACY.exists():
            try:
                text = TOKEN_FILE_LEGACY.read_text().strip()
            except FileNotFoundError:
                return None
        else:
            return None
    return text or None


def write_broker_token_file(token: str, path: Optional[Path] = None) -> None:
    """Write *token* to *path* atomically, mode 0600 (private — like the key)."""
    if path is None:
        path = TOKEN_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.broker.token.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(token.rstrip() + '\n')
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except Exception:
        try:    os.unlink(tmp)
        except FileNotFoundError: pass
        raise


def resolve_broker_token(cli: Optional[str] = None) -> Tuple[Optional[str], str]:
    """Resolve the broker token for a *consumer* (endpoint / client).

    Precedence: CLI arg > ``$RADICAL_ORBIT_BROKER_TOKEN`` >
    ``~/.radical/orbit/broker.token``.  Returns ``(token, source)`` with source
    one of ``'cli'`` / ``'env'`` / ``'file'``, or ``(None, '')`` when none is
    configured (a missing token is not fatal — the broker may have auth off).
    """
    if cli:
        return cli.strip(), 'cli'
    env_tok = _env(ENV_TOKEN, ENV_TOKEN_LEGACY)
    if env_tok:
        return env_tok, 'env'
    file_tok = _read_token_file()
    if file_tok:
        return file_tok, 'file'
    return None, ''


def auth_disabled(cli_no_auth: bool = False) -> bool:
    """Whether broker ingress auth is disabled (the ``--no-auth`` escape hatch).

    True when *cli_no_auth* is set or ``$RADICAL_ORBIT_BROKER_NO_AUTH`` is a
    truthy value (``1`` / ``true`` / ``yes``).
    """
    if cli_no_auth:
        return True
    return _env(ENV_NO_AUTH, ENV_NO_AUTH_LEGACY).lower() in ('1', 'true', 'yes')


def ensure_broker_token(cli: Optional[str] = None) -> Tuple[str, str]:
    """Resolve the broker token for the *broker* itself, generating if absent.

    Precedence: CLI > env > file; if none resolves, generate a fresh
    URL-safe token, write it to ``~/.radical/orbit/broker.token`` (mode 0600),
    and return it.  Returns ``(token, source)`` with source one of
    ``'cli'`` / ``'env'`` / ``'file'`` / ``'generated'``.
    """
    token, source = resolve_broker_token(cli)
    if token:
        return token, source
    token = secrets.token_urlsafe(32)
    write_broker_token_file(token)
    return token, 'generated'


def tokens_match(provided: Any, expected: Any) -> bool:
    """Constant-time token comparison; ``False`` if either side is empty or
    not a string (e.g. a non-string token in a JSON ``/register`` payload,
    which would otherwise raise ``TypeError`` in ``hmac.compare_digest``)."""
    if not isinstance(provided, str) or not isinstance(expected, str):
        return False
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided, expected)


def host_role(app: Any) -> Dict[str, Any]:
    """Classify the host that *app* is running on.

    Single source of truth for role / scheduler / executor detection
    across the codebase.  Returns a dict with these fields:

    - ``role``           — one of ``'broker'`` / ``'login'`` /
                           ``'compute'`` / ``'standalone'``.
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
