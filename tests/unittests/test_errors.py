"""Tests for :mod:`radical.orbit.errors` — the canonical error envelope."""

import json

import pytest

from radical.orbit import errors


# ---------------------------------------------------------------------------
# status_for: direct map, MRO lookup, default
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("exc, status", [
    (FileNotFoundError('x'),  404),
    (FileExistsError('x'),    409),
    (PermissionError('x'),    403),
    (NotADirectoryError('x'), 400),
    (IsADirectoryError('x'),  400),
    (ValueError('x'),         400),
    (TimeoutError('x'),       504),
])
def test_status_for_direct_map(exc, status):
    assert errors.status_for(exc) == status


def test_status_for_subclass_mro_lookup():
    # A subclass not itself in the map resolves via its MRO to the mapped base.
    class MyValueError(ValueError):
        pass

    assert errors.status_for(MyValueError('boom')) == 400


def test_status_for_default_is_500():
    assert errors.status_for(RuntimeError('unmapped')) == 500
    assert errors.status_for(KeyError('unmapped'))     == 500


# ---------------------------------------------------------------------------
# builders
# ---------------------------------------------------------------------------

def test_error_dict_shape():
    d = errors.error_dict(404, 'not here')
    assert d == {"error": True, "status_code": 404, "detail": "not here"}


def test_error_body_is_json_bytes():
    body = errors.error_body(503, 'at cap')
    assert isinstance(body, bytes)
    assert json.loads(body) == {"error": True, "status_code": 503,
                                "detail": "at cap"}


# ---------------------------------------------------------------------------
# http_exception convenience
# ---------------------------------------------------------------------------

def test_http_exception_maps_status_and_detail():
    exc = errors.http_exception(FileExistsError('dup'))
    assert exc.status_code == 409
    assert exc.detail      == 'dup'


def test_http_exception_explicit_status_overrides_map():
    exc = errors.http_exception(ValueError('bad'), status=422)
    assert exc.status_code == 422
    assert exc.detail      == 'bad'
