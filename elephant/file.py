import logging
import aardvark
import bson
import elephant.util

logger = logging.getLogger(__name__)

class _AArray:
    """
    not for use with local or global
    for use with regular mongo documents
    """
    def __init__(self, coll, d):
        self.coll = coll
        self.d = d

    def __getitem__(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v

        self.coll.update_one({'_id': self.d['_id']}, {'$set': {k, v}})

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        else:
            return default

    def to_array(self):
        return self.d

class File:
    def __init__(self, e, d):
        self.e = e
        self.d = d

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        else:
            return default

    async def valid_group(self, docs_0):
        pass

    def _commits(self, ref = None):

        if self.d.get('_root', False): return

        def _find(commit_id):
            if '_temp' in self.d:

                if not "commits" in self.d["_temp"]:
                    logger.warning(f"{self!r} has not field '_temp.commits'")

                for c in self.d["_temp"]["commits"]:
                    if c["_id"] == commit_id:
                        return c
            
            return self.e.coll.commits.find_one({'_id': commit_id})

        ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            id0 = self.d["_elephant"]["refs"][ref]

            # only different between this and global_.File
            #ref = self.e.coll.refs.find_one({'name': ref})
            #id0 = ref["commit_id"]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])



    def commits(self, ref = None):
        return reversed(list(self._commits(ref)))

    async def to_array(self):
        return dict(self.d)

    async def temp_to_array(self):
        return dict(self.d['_temp'])

    def list_connected(self, user, query=None):
        yield from self.list_upstream(user, query)
        yield from self.list_downstream(user, query)

    def list_upstream(self, user, query=None):
        yield
        return
 
    def list_downstream(self, user, query=None):
        yield
        return
 
