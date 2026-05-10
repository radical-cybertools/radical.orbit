"""HTTP client factory with stale-keepalive retry.

NAT / firewall devices on the path between client and bridge will
silently RST idle TCP connections.  httpx then surfaces this on the
next reuse as ``RemoteProtocolError("Server disconnected without
sending a response.")`` (or ``ReadError`` if the RST lands mid-handshake).

The wrapping transport here retries the request once: in this failure
mode the request was never delivered, so retry is safe; httpcore's
pool discards the dead connection on the next attempt and opens a
fresh one.  Steady-state cost is zero — only failed reuses pay extra.

Usage::

    from .http_utils import make_http_client, make_async_http_client
    self._http = make_http_client(base_url=..., verify=..., timeout=...)
"""

import httpx


class RetryTransport(httpx.HTTPTransport):
    def handle_request(self, request):
        try:
            return super().handle_request(request)
        except (httpx.RemoteProtocolError, httpx.ReadError):
            return super().handle_request(request)


class RetryAsyncTransport(httpx.AsyncHTTPTransport):
    async def handle_async_request(self, request):
        try:
            return await super().handle_async_request(request)
        except (httpx.RemoteProtocolError, httpx.ReadError):
            return await super().handle_async_request(request)


def make_http_client(*, verify=True, cert=None, **client_kwargs):
    return httpx.Client(
        transport=RetryTransport(verify=verify, cert=cert),
        **client_kwargs)


def make_async_http_client(*, verify=True, cert=None, **client_kwargs):
    return httpx.AsyncClient(
        transport=RetryAsyncTransport(verify=verify, cert=cert),
        **client_kwargs)
