"""In-process ORBIT broker for consumer clients.

``EmbeddedBroker`` runs the full :class:`~radical.orbit.broker.Broker`
(routing loop, hosted plugins, gateway) on a daemon thread inside the
client process, so a single-client setup needs no standalone broker
deployment.  Endpoints connect to the advertised URL exactly as they
would to a standalone broker.

The embedded broker is a *regular* broker: the operator-placed
cert / key / token under ``~/.radical/orbit`` (or their env-var
redirects) are required, auth is on by default, and nothing is ever
written to the config directory.

The listen socket is bound *before* the ``Broker`` is constructed:
uvicorn runs the FastAPI lifespan — which hands the broker's advertised
URL to hosted plugins — before it would bind sockets itself, so the
port must be final at construction time.  The pre-bound socket is then
handed to ``uvicorn.Server.run(sockets=[...])``.
"""

import errno
import logging
import socket
import threading
import time

from typing import Optional

from .broker import Broker, BrokerTuning

log = logging.getLogger("radical.orbit.embedded")


class EmbeddedBroker:
    """A full ORBIT broker served on a daemon thread inside this process."""

    _DEFAULT_PORT = 8000     # parity with the standalone broker; tests redirect

    def __init__(self,
                 cert:    Optional[str] = None,
                 key:     Optional[str] = None,
                 token:   Optional[str] = None,
                 host:    str           = '0.0.0.0',
                 port:    Optional[int] = None,
                 plugins: str           = '',
                 auth:    bool          = True,
                 gateway: bool          = True,
                 tuning:  Optional[BrokerTuning] = None):
        self._cert    = cert
        self._key     = key
        self._token   = token
        self._host    = host
        self._port    = port
        self._plugins = plugins
        self._auth    = auth
        self._gateway = gateway
        self._tuning  = tuning

        self._broker:  Optional[Broker]           = None
        self._server                              = None   # uvicorn.Server
        self._thread:  Optional[threading.Thread] = None
        self._sock:    Optional[socket.socket]    = None
        self._stopped: bool                       = False

    # ── public API ─────────────────────────────────────────────────────

    @property
    def broker(self) -> Optional[Broker]:
        return self._broker

    @property
    def url(self) -> Optional[str]:
        return self._broker.url if self._broker else None

    def start(self, timeout: float = 15.0) -> str:
        """Bind, construct the broker, serve on a daemon thread.

        Returns the advertised broker URL once the server accepts
        connections.  On any failure the pre-bound socket and the server
        thread are cleaned up before the exception propagates.
        """
        import uvicorn

        self._sock = self._bind()
        port       = self._sock.getsockname()[1]

        try:
            self._broker = Broker(cert=self._cert, key=self._key,
                                  host=self._host, port=port,
                                  plugins=self._plugins, token=self._token,
                                  auth=self._auth, gateway=self._gateway,
                                  tuning=self._tuning)

            # Quieter than run(): an embedded broker must not spam the
            # client's output (and the startup banner is skipped — run() is
            # never called).
            config = uvicorn.Config(self._broker.app,
                                    host=self._host, port=port,
                                    **{**self._broker._uvicorn_kwargs(),
                                       'log_level': 'warning'})
            self._server = uvicorn.Server(config)
            self._thread = threading.Thread(target=self._server.run,
                                            kwargs={'sockets': [self._sock]},
                                            name='orbit-embedded-broker',
                                            daemon=True)
            self._thread.start()
        except Exception:
            self._sock.close()
            self._sock = None
            raise

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._server.started:
                log.info("[Embedded] broker serving at %s", self._broker.url)
                return self._broker.url
            if not self._thread.is_alive():
                break                      # serve() returned — startup failed
            time.sleep(0.02)

        # Timeout or early thread death: tear down, nothing may leak.
        self._server.should_exit = True
        self._thread.join(5.0)
        self._sock.close()
        self._sock = None
        raise RuntimeError(
            f"embedded broker failed to start on {self._host}:{port} "
            f"within {timeout}s — see the radical.orbit log for the "
            f"uvicorn/broker error")

    def stop(self, timeout: float = 10.0) -> None:
        """Shut the server down and join its thread.  Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout)
            if self._thread.is_alive():
                log.warning("[Embedded] broker thread did not stop within "
                            "%.1fs; abandoning (daemon)", timeout)

    # ── internals ──────────────────────────────────────────────────────

    def _bind(self) -> socket.socket:
        """Pre-bind the listen socket (no ``listen()`` — uvicorn's job).

        An explicit ``port`` binds exactly (``OSError`` propagates); the
        default tries ``_DEFAULT_PORT`` and falls back to an ephemeral
        port with a warning when it is taken.
        """
        def _try(port: int) -> socket.socket:
            # IPv6 literals ('::', '::1', …) need AF_INET6.
            family = socket.AF_INET6 if ':' in self._host else socket.AF_INET
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((self._host, port))
            except OSError:
                sock.close()
                raise
            return sock

        if self._port is not None:
            return _try(self._port)
        try:
            return _try(self._DEFAULT_PORT)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            log.warning("[Embedded] port %d in use; falling back to an "
                        "ephemeral port", self._DEFAULT_PORT)
            return _try(0)
