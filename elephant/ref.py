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

    async def __encode__(self, h, user, mode):
        args = [self._id, self.ref]
        return {"DocRef": await elephant.util.encode(h, user, mode, args)}
       
    def __eq__(self, other):
        if other is None: return False
        if not isinstance(other, DocRef): return False
        if self._id != other._id: return False
        if self.ref != other.ref: return False
        return True
 

