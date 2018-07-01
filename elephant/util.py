import contextlib
import time

class AccessDenied(Exception):
    pass

@contextlib.contextmanager
def stopwatch(p, s):
    t0 = time.perf_counter_ns()
    yield
    t1 = time.perf_counter_ns()
    p(s + f"{(t1 - t0) / 1e6:10.6f} ms")

