import copy
import json
import time
import hashlib
import datetime
import pprint
import bson.json_util
import crayons
import aardvark
import logging

import networkx as nx

import elephant.check
import elephant.util
import elephant.doc
import elephant.ref

logger = logging.getLogger(__name__)
logger_mongo = logging.getLogger(__name__ + "-mongo")


class _User:
    def __init__(self):
        self.d = {}

class Doc(elephant.doc.Doc):
    def __init__(self, e, d, _d, *args, **kwargs):
        super().__init__(e, d, _d, *args, **kwargs)

    def freeze(self):
        if '_root' in self.d: return

        if isinstance(self.d['_elephant']['ref'], bson.objectid.ObjectId):
            return elephant.ref.DocRef(
                    self.d['_id'],
                    self.d['_elephant']['ref'],
                    )
        else:
            return elephant.ref.DocRef(
                    self.d['_id'],
                    self.d['_elephant']['refs'][self.d['_elephant']['ref']],
                    )

    @classmethod
    async def get_test_document(cls, b0={}):
        b = {"test_field": str(time.time())}
        b.update(b0)
        return b

    def valid(self):
        pass

    async def check_0(self):
        # checks before _id is available
        pass

    async def check(self):
        creator = await self.creator()
        assert creator

        self.d["_temp"]

        self.d["_temp"]["commits"]

        # used in the read_permissions pipe
        self.d["_temp"]["commits"][0].user


    async def has_read_permission(self, user0):
        if '_root' in self.d: 
            logger.info(f'Permission denied: root document')
            return False

        if hasattr(self.e, 'h'):
            if user0 == self.e.h.root_user:
                return True

        if user0 is None: 
            logger.info(f'Permission denied: user0 is None')
            return False

        user1 = await self.creator()

        assert user1


        if user0.freeze() == user1.freeze():
            logger.debug(f"Permission granted: {user0} == {user1}")
            return True
        else:

            #ref_0 = await user0.freeze().__encode__(None, None, None)
            #if ref_0 == user1.freeze():

            logger.info(f"Permission denied:  {user0.freeze()} != {user1.freeze()}")
            return False


    async def has_write_permission(self, user):
        if hasattr(self.e, 'h'):
            if user == self.e.h.root_user:
                return True
        return user.freeze() == (await self.creator()).freeze()

    def commit0(self):
        if self.d.get('_root', False): return
        return next(self.commits())

    def _commit0(self):
        try:
            commit0 = next(self.e.coll.commits.find({"file": self.d["_id"]}).sort([('time', 1)]))
        except StopIteration:
            print(crayons.red('no commits'))
            pprint.pprint(self.d)
            raise
 
            item = aardvark.util.clean(self.d)
            diffs = list(aardvark.diff({}, item))
            commit_id = self.e._create_commit_1(self.d['_id'], None, diffs)
            ref = 'master'
            e = {
                    "ref": ref,
                    "refs": {ref: commit_id},
                    }
            res = self.e.coll.files.update_one({'_id': self.d['_id']}, {'$set': {'_elephant': e}})
            self.d['_elephant'] = e
        
            commit0 = self.e.coll.commits.find_one({'_id': commit_id})

        return commit0

    def _assert_elephant(self):
        if '_elephant' not in self.d:
            if self.d.get('_root'): return
            print(crayons.red('no field _elephant'))

            commit0 = self._commit0()

            if '_elephant' not in self.d:

                ref = 'master'
                self.d['_elephant'] = {
                        "ref": ref,
                        "refs": {ref: commit0['_id']},
                        }
    
                res = self.e.coll.files.update_one(
                        {'_id': self.d['_id']}, {'$set': {'_elephant': self.d['_elephant']}})
    
                print(res.modified_count)

    def put(self, user):
        return self.e.put(user, self.d["_elephant"]["ref"], self.d["_id"], self.d)

    async def delete(self, user):
        self.d["hide"] = True
        await self.put(user)

    async def update_temp(self, user):
        """
        update self.d["_temp"] with calculated values to be stored in the database for querying
        """
        self.d["_temp"] = {}

        self.d["_temp"]["commits"] = await self.temp_commits()

    async def temp_commits(self):

        return list(elephant.commit.CommitLocal(
                _["_id"],
                _["time"],
                _["user"],
                _["parent"],
                _["file"],
                _["changes"],
                ) for _ in self.e.coll.commits.find({"file": self.d["_id"]}))

    async def temp_messages(self):
        return
        yield

    async def checkout(self, user, ref):

        path = await self.e.get_path(ref)

        a = await self.e.apply_path(path, {})

        a['_id'] = self.d['_id']
        

        a['_elephant'] = {
                'ref': ref,

                # we are not changing the definition of any of our refs
                # we are just changing this document to reflect a particular commit
                'refs': self.d['_elephant']['refs'],
                }
        

        self.d = a

        await self.update_temp(user)

class Query(Doc):

    async def check_0(self):
        await super().check_0()
        assert "title" in self.d
        assert "query0" in self.d

    async def check(self):
        await super().check()
        assert "title" in self.d
        assert "query0" in self.d

    @classmethod
    async def get_test_document(self, b0={}):
        b1 = {"title": "test", "query0": "{}"}
        b1.update(b0)
        return await super().get_test_document(b1)


