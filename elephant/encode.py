import bson


class Encoder:

    def encode(self, o):
        
        if isinstance(o, dict):
            return {k: self.encode(v) for k, v in o.items()}

        if isinstance(o, (tuple, list)):
            return [self.encode(v) for v in o]
        
        if isinstance(o, (int, str, float)): return o

        try:
            bson.json_util.dumps(o)
            return o
        except Exception as e:
            try:
                return o.__encode__()
            except:
                print(f"cannot bson encode {o!r}")
                print(repr(e))
                raise Exception("cannot bson encode and no __encode__ method")

def encode(o):
    return Encoder().encode(o)


