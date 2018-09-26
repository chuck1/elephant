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

async def encode(o):
     
    if hasattr(o, "__encode__"):
        return await o.__encode__()
   
    if isinstance(o, dict):
        return {k: await encode(v) for k, v in o.items()}

    if isinstance(o, (list, tuple)):
        return [await encode(v) for v in o]

    return o
 

