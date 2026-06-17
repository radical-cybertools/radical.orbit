
try:
    from radical.prof import Profiler
except ImportError:
    class Profiler:
        def __init__(self, name, ns=None): pass
        def prof(self, *args, **kwargs):   pass
        def close(self):                   pass
