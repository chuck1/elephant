import bson
import elephant.util

class DocRef:
    @classmethod
    async def decode(cls, h, args):
        return DocRef(*args)

    def __init__(self, _id, ref=None):
        if not isinstance(_id, bson.objectid.ObjectId): raise TypeError()
        if not ((ref is None) or isinstance(ref, (str, bson.objectid.ObjectId))): raise TypeError()
        self._id = _id
        self.ref = ref
    async def __encode__(self):
        return {"DocRef": [self._id, self.ref]}
        

