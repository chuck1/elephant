import logging
import aardvark
import bson
import elephant.util

logger = logging.getLogger(__name__)

class DEP_AArray:
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

class Doc:
    def __init__(self, e, d, _d, is_subobject=False):
        self.e = e
        self.d = d
        self._d = _d
        self.is_subobject = is_subobject

    async def __encode__(self):
        args = [dict(self.d), self.is_subobject]
        return {'Document': await elephant.util.encode(args)}

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        else:
            return default

    async def clean_encode(self):
        return await elephant.util.encode(aardvark.util.clean(self.d))

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

    async def temp_to_array(self):
        return await elephant.util.encode(dict(self.d['_temp']))

    def list_connected(self, user, query=None):
        yield from self.list_upstream(user, query)
        yield from self.list_downstream(user, query)

    async def list_upstream(self, user, query=None):
        yield
        return
 
    async def list_downstream(self, user, query=None):
        yield
        return
 







