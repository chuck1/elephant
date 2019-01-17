import copy
import logging
import pprint

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
    async def __encode__(self, h, user, mode):
        args = [copy.deepcopy(self.d), self.is_subobject]
        return {'Document': await elephant.util.encode(h, user, mode, args)}

    def __init__(self, e, d, _d, is_subobject=False):
        self.e = e
        self.d = d
        self._d = _d
        self.is_subobject = is_subobject

    def __copy__(self):
        return self.__class__(
                self.e, 
                copy.deepcopy(self.d),
                copy.deepcopy(self._d),
                is_subobject=self.is_subobject,
                )

    def __deepcopy__(self, memo):
        return self.__class__(
                self.e, 
                copy.deepcopy(self.d, memo),
                copy.deepcopy(self._d, memo),
                self.is_subobject,
                )

    async def creator_id(self):

        if "_temp" not in self.d:

            commits = await self.temp_commits()
            return commits[0].user

            pprint.pprint(self.d)
            raise Exception("no _temp")

        return self.d["_temp"]["commits"][0].user

    async def creator(self):
        if self.d.get('_root'):
            logger.info("has field _root!")
            pprint.pprint(self.d)
            return None

        user_id = await self.creator_id()
        user = await self.e.h.e_users._find_one_by_id("master", user_id)
        assert user is not None
        return user
 
    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        else:
            return default

    async def clean_encode(self, user):
        return await elephant.util.encode(
                self.h, user, elephant.EncodeMode.DATABASE, aardvark.util.clean(self.d))

    async def valid_group(self, docs_0):
        pass

    async def _commits(self, ref = None):
        # yield commits in order

        if self.d.get('_root', False): return

        async def _temp_commits():
            if '_temp' in self.d:
                if "commits" in self.d["_temp"]:
                    return self.d["_temp"]["commits"]
            
            return await self._temp_commits()

        def _find(commits, commit_id):
            for c in commits:
                if c._id == commit_id:
                    return c
            return
            raise Exception(f'{commit_id} {commits}')

        #############################

        commits = await _temp_commits()

        ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            id0 = self.d["_elephant"]["refs"][ref]

            # only different between this and global_.File
            #ref = self.e.coll.refs.find_one({'name': ref})
            #id0 = ref["commit_id"]

        c0 = _find(commits, id0)

        while c0:
            yield c0
            
            c0 = _find(commits, c0.parent)

    async def commits(self, ref = None):
        return reversed([_ async for _ in self._commits(ref)])

    async def temp_to_array(self, user):
        return await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, dict(self.d['_temp']))

    def list_connected(self, user, query=None):
        yield from self.list_upstream(user, query)
        yield from self.list_downstream(user, query)

    async def list_upstream(self, user, query=None):
        yield
        return
 
    async def list_downstream(self, user, query=None):
        yield
        return
 
    def clear_temp(self):
        if "_temp" in self.d:
            del self.d["_temp"]






