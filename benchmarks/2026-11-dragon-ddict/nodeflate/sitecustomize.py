"""Diagnostic only (stage k1nd): with $ORBIT_BENCH_NO_DEFLATE=1 on
PYTHONPATH, ORBIT's websockets.connect() offers no permessage-deflate, so
neither the client->broker nor the endpoint->broker leg compresses.  ORBIT
itself hard-wires compression='deflate' (runtime.py) and the broker accepts
it (broker.py, ws_per_message_deflate=True)."""
import os
if os.environ.get('ORBIT_BENCH_NO_DEFLATE') == '1':
    import websockets
    _connect = websockets.connect

    def connect(*args, **kwargs):
        kwargs['compression'] = None
        return _connect(*args, **kwargs)

    websockets.connect = connect
