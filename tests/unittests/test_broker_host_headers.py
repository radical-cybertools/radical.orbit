"""Header-case handling on the broker plugin host.

HTTP headers are case-insensitive, but only the gateway path goes through
starlette's normalisation -- an in-process caller (BrokerCaller) hands its
header dict to ``handle_request`` verbatim.  A client sending
``Content-Type: application/msgpack`` must still get its body parsed as
msgpack, not fed to ``json.loads`` (where the first msgpack byte dies as
invalid utf-8).
"""

import re

import msgpack
import pytest

from radical.orbit.broker_plugin_host import BrokerPluginHost


def _host_with_echo_route():
    async def noop_broadcast(*_a, **_k):
        return None

    host = BrokerPluginHost(plugin_names=[], broadcast_fn=noop_broadcast)

    async def echo(request):
        return {'echoed': await request.json()}

    host._direct_routes.append(
        ('POST', re.compile(r'^/echo$'), (), echo))
    return host


@pytest.mark.asyncio
async def test_msgpack_body_with_capitalized_content_type():
    host = _host_with_echo_route()
    payload = {'tasks': [{'uid': 'task.000000', 'pool': 'local'}]}

    resp = await host.handle_request(
        'POST', '/echo',
        headers={'Content-Type': 'application/msgpack'},
        body_bytes=msgpack.packb(payload, use_bin_type=True))

    import json
    assert json.loads(resp.body)['echoed'] == payload


@pytest.mark.asyncio
async def test_json_body_stays_the_default():
    host = _host_with_echo_route()

    resp = await host.handle_request(
        'POST', '/echo', headers={},
        body_bytes=b'{"a": 1}')

    import json
    assert json.loads(resp.body)['echoed'] == {'a': 1}
