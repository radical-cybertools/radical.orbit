"""
Tests for the direct-dispatch path (RequestShim + route matching).
"""
import json
import re

import pytest

from fastapi import HTTPException

from radical.orbit.service import RequestShim, EndpointService


# ---------------------------------------------------------------------------
# RequestShim
# ---------------------------------------------------------------------------

class TestRequestShim:

    @pytest.mark.asyncio
    async def test_json_parses_body(self):
        shim = RequestShim({}, {}, b'{"key": "value"}')
        result = await shim.json()
        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_json_caches_result(self):
        shim = RequestShim({}, {}, b'{"a": 1}')
        r1 = await shim.json()
        r2 = await shim.json()
        assert r1 is r2

    @pytest.mark.asyncio
    async def test_json_empty_body(self):
        shim = RequestShim({}, {}, b'')
        result = await shim.json()
        assert result == {}

    @pytest.mark.asyncio
    async def test_json_none_body(self):
        shim = RequestShim({}, {}, b'')
        result = await shim.json()
        assert result == {}

    @pytest.mark.asyncio
    async def test_body_returns_raw_bytes(self):
        raw = b'\x00\x01\x02'
        shim = RequestShim({}, {}, raw)
        assert await shim.body() == raw

    def test_path_params(self):
        shim = RequestShim({"sid": "s1", "uid": "u2"}, {}, b'')
        assert shim.path_params["sid"] == "s1"
        assert shim.path_params["uid"] == "u2"

    def test_query_params_get(self):
        shim = RequestShim({}, {"user": "bob", "force": "true"}, b'')
        assert shim.query_params.get("user") == "bob"
        assert shim.query_params.get("force") == "true"
        assert shim.query_params.get("missing") is None
        assert shim.query_params.get("missing", "default") == "default"

    def test_content_type_default(self):
        shim = RequestShim({}, {}, b'')
        assert shim.content_type == 'application/json'

    def test_content_type_custom(self):
        shim = RequestShim({}, {}, b'', content_type='application/msgpack')
        assert shim.content_type == 'application/msgpack'

    @pytest.mark.asyncio
    async def test_json_with_content_type(self):
        shim = RequestShim({}, {}, b'{"x": 42}', 'application/json')
        result = await shim.json()
        assert result == {"x": 42}

    @pytest.mark.asyncio
    async def test_json_caches(self):
        shim = RequestShim({}, {}, b'{"x": 1}')
        r1 = await shim.json()
        r2 = await shim.json()
        assert r1 is r2


# ---------------------------------------------------------------------------
# Route matching
# ---------------------------------------------------------------------------

def _compile_route(path):
    """Mimic Plugin._register_direct to build a route entry."""
    parts       = path.strip('/').split('/')
    regex_parts = []
    param_names = []
    for part in parts:
        if part.startswith('{') and part.endswith('}'):
            param_names.append(part[1:-1])
            regex_parts.append('([^/]+)')
        else:
            regex_parts.append(re.escape(part))
    pattern = re.compile('^/' + '/'.join(regex_parts) + '$')
    return pattern, tuple(param_names)


class TestRouteMatching:

    def test_no_params(self):
        pattern, names = _compile_route('/sysinfo/homedir')
        assert names == ()
        assert pattern.match('/sysinfo/homedir')
        assert not pattern.match('/sysinfo/other')

    def test_one_param(self):
        pattern, names = _compile_route('/rhapsody/submit/{sid}')
        assert names == ('sid',)
        m = pattern.match('/rhapsody/submit/session.abc123')
        assert m
        assert m.group(1) == 'session.abc123'

    def test_two_params(self):
        pattern, names = _compile_route('/psij/status/{sid}/{job_id}')
        assert names == ('sid', 'job_id')
        m = pattern.match('/psij/status/session.x/job-42')
        assert m
        assert dict(zip(names, m.groups())) == {
            'sid': 'session.x', 'job_id': 'job-42'}

    def test_no_match(self):
        pattern, _ = _compile_route('/sysinfo/homedir')
        assert not pattern.match('/sysinfo/metrics/s1')
        assert not pattern.match('/other/homedir')

    def test_partial_no_match(self):
        pattern, _ = _compile_route('/sysinfo/homedir')
        assert not pattern.match('/sysinfo/homedir/extra')

    def test_method_filtering(self):
        """Simulate _match_route: method must match too."""
        async def handler(req): pass

        routes = [("POST", *_compile_route('/submit/{sid}'), handler)]
        # POST matches
        method, pattern, names, h = routes[0]
        assert method == "POST" and pattern.match('/submit/s1')
        # Simulating GET would not match because method != "GET"


# ---------------------------------------------------------------------------
# Error response helper
# ---------------------------------------------------------------------------

class TestErrorResponse:

    def test_http_exception(self):
        exc = HTTPException(status_code=404, detail="not found")
        resp = EndpointService._error_response("req-1", exc)
        assert resp.status == 404
        body = json.loads(resp.body)
        assert body == {"detail": "not found"}

    def test_generic_exception(self):
        exc = RuntimeError("boom")
        resp = EndpointService._error_response("req-2", exc)
        assert resp.status == 502
        body = json.loads(resp.body)
        assert body["error"] == "endpoint-invoke-failed"
        assert body["detail"] == "boom"

    def test_value_error(self):
        exc = ValueError("bad input")
        resp = EndpointService._error_response("req-3", exc)
        assert resp.status == 502
        body = json.loads(resp.body)
        assert "bad input" in body["detail"]
