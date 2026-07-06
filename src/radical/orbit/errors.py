"""Canonical error envelope + exception-to-status mapping for ORBIT.

Every boundary error-producer (endpoint runtime, broker, gateway, plugins)
synthesizes its error bodies through the helpers here so they all carry one
shape on the wire::

    {"error": true, "status_code": <int>, "detail": "<str>"}

This is a strict superset of the older ``{"detail": ...}`` body — ``detail``
stays present at every site, so existing consumers (``client.py`` and external
HTTP callers) keep working; ``error`` and ``status_code`` are additive.
"""

import json

from typing import Dict, Optional


# ── exception → HTTP status ────────────────────────────────────────────────
# Boundary handlers translate a raised stdlib exception into an HTTP status via
# this map (walking the type's MRO); anything unmapped is a 500.  Keep the
# staging client's status→exception reversal in mind: FileExistsError→409,
# FileNotFoundError→404, PermissionError→403, ValueError/NotADirectoryError→400
# must stay exactly as below.
EXC_STATUS: Dict[type, int] = {
    FileNotFoundError:  404,
    FileExistsError:    409,
    PermissionError:    403,
    NotADirectoryError: 400,
    IsADirectoryError:  400,
    ValueError:         400,
    TimeoutError:       504,
}


def status_for(exc: BaseException) -> int:
    """Return the HTTP status for *exc*, walking its MRO against ``EXC_STATUS``.

    Falls back to 500 for any exception type not covered by the map.
    """
    for cls in type(exc).__mro__:
        status = EXC_STATUS.get(cls)
        if status is not None:
            return status
    return 500


def error_dict(status: int, detail: str) -> dict:
    """Return the canonical error envelope as a dict."""
    return {"error": True, "status_code": status, "detail": detail}


def error_body(status: int, detail: str) -> bytes:
    """Return the canonical error envelope as JSON-encoded bytes."""
    return json.dumps(error_dict(status, detail)).encode()


def http_exception(exc: BaseException, status: Optional[int] = None):
    """Wrap *exc* in a ``fastapi.HTTPException`` with a mapped status.

    Convenience for plugin handlers: uses *status* when given, else
    :func:`status_for`; the detail is ``str(exc)``.
    """
    from fastapi import HTTPException
    return HTTPException(status or status_for(exc), str(exc))
