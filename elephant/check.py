
def countup():
    i = 0
    while True:
        yield i
        i += 1

def check(o, f, address=[]):

    if isinstance(o, dict):
        for k, v in o.items():
            check(v, f, address + [k])

    elif isinstance(o, (list, tuple)):
        for k, v in zip(countup(), o):
            check(v, f, address + [k])

    else:
        try:
            #bson.json_util.dumps(o)
            f(o)
        except Exception as e:
            print("failed", repr(f))
            print(repr(o))
            print(address)
            raise
 
