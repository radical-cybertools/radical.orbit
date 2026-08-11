#!/usr/bin/env python3

'''Consume the tutorial `math` plugin (see docs/source/tutorial_plugin.rst).

Start a broker and an endpoint that serves the plugin first:

    # Terminal 1
    ./bin/radical-orbit-broker.py

    # Terminal 2
    ./bin/radical-orbit-endpoint-wrapper.sh --plugins default,math

    # Terminal 3
    python examples/example_math.py
'''

import time

from radical.orbit import EndpointRuntime


def on_result(endpoint, plugin, topic, data):
    print(f"  notification: {endpoint}/{plugin} {topic}: "
          f"{data['op']}({data['a']}, {data['b']}) = {data['result']}")


def main():

    rt = EndpointRuntime()
    rt.start(wait=True)

    # Find an endpoint that serves the math plugin
    eids = [e_name for e_name, e_info in rt.topology().items()
            if e_info and 'math' in (e_info.get('plugins') or {})]
    if not eids:
        print("no endpoint serves the 'math' plugin - start one with "
              "'--plugins default,math'")
        rt.stop()
        return

    math = rt.get_plugin(eids[0], 'math')   # also registers a session
    math.register_notification_callback(on_result, topic='result')

    print(f"add(3, 4): {math.add(3, 4)}")
    print(f"sub(3, 4): {math.sub(3, 4)}")
    print(f"mul(3, 4): {math.mul(3, 4)}")
    print(f"div(3, 4): {math.div(3, 4)}")

    try:
        math.div(1, 0)
    except RuntimeError as e:
        print(f"div(1, 0) failed as expected: {e}")

    hist = math.history()
    print(f"{hist['count']} operations recorded in this session")

    time.sleep(1)   # let the last notification arrive
    math.close()
    rt.stop()


if __name__ == '__main__':
    main()
