import contextlib
import io
import pprint
import time

import bson.json_util

def lines(func, *args):

    f = io.StringIO()

    with contextlib.redirect_stdout(f):
        func(*args)

    s = f.getvalue()

    for l in s.split('\n'):
        yield l

class AccessDenied(Exception):
    pass

@contextlib.contextmanager
def stopwatch(p, s):
    t0 = time.perf_counter_ns()
    yield
    t1 = time.perf_counter_ns()
    p(s + f"{(t1 - t0) / 1e6:10.6f} ms")

async def encode(h, user, mode, o):
     
    if hasattr(o, "__encode__"):
        try:
            a = await o.__encode__(h, user, mode)
        except Exception as e:
            print(repr(e))
            print(repr(o))
            raise

        try:
            bson.json_util.dumps(a)
        except:
            print(f"{o!r} __encode__ did not produce bson encodable object")
            pprint.pprint(a)
            raise

        return a
   
    if isinstance(o, dict):
        return {k: await encode(h, user, mode, v) for k, v in o.items()}

    if isinstance(o, (list, tuple)):
        return [await encode(h, user, mode, v) for v in o]


    # check
    try:
        bson.json_util.dumps(o)
    except:
        print(f'could not encode {type(o)} {o!r}')
        raise


    return o
 

