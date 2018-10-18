import copy
import contextlib
import itertools
import pprint
import datetime
import json
import time
import hashlib
import logging
import time

import bson
import crayons

import aardvark
import aardvark.util
import elephant.util
import elephant.doc

logger = logging.getLogger(__name__)
logger_mongo = logging.getLogger(__name__ + "-mongo")

class Temp: pass

class File(elephant.doc.Doc):
    def __init__(self, e, d, _d, *args):
        super().__init__(e, d, _d, *args)
        assert isinstance(d, dict)
        self.temp = Temp()

    @classmethod
    async def get_test_document(cls, b0={}):
        b = {"test_field": str(time.time())}
        b.update(b0)
        return b

    def freeze(self):
        return elephant.ref.DocRef(self.d["_id"])

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        return default

    def get(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v
        updates = {'$set': {k: v}}
        self.e.coll.files.update_one({'_id': self.d['_id']}, updates)

    def valid(self):
        pass

    async def check_0(self):
        pass

    async def check(self):
        creator = await self.creator()

        self.d["_temp"]
        self.d["_temp"]["first_commit"]

        # used in the read_permissions pipe
        self.d["_temp"]["first_commit"]["user"]

    async def update_temp(self, user):
        """
        recalculate _temp fields
        _temp fields store data in the database for querying
        this should be done before a document is written, not when it is read.

        fields:
          first_commit
        """
	
        self.d["_temp"] = {}
 
        await self.temp_commits()

    def _commits_1(self):

        pipe = [
                {'$addFields': {'files1': '$files'}},
                {'$unwind': '$files1'},
                {'$match': {'$expr': {'$eq': ["$files1.file_id", self.d["_id"]]}}},
                {'$project': {'files1': 0}},
                ]

        return list(self.e.coll.commits.aggregate(pipe))

    async def temp_commits(self):

        pipe = [
                {'$addFields': {'files1': '$files'}},
                {'$unwind': '$files1'},
                {'$match': {'$expr': {'$eq': ["$files1.file_id", self.d["_id"]]}}},
                {'$project': {'files1': 0}},
                ]
 
        commits = list(self.e.coll.commits.aggregate(pipe))

        self.d["_temp"]["commits"] = commits

        #self.d["_temp"]["last_commit"]  = self.d['_temp']['commits'][-1]

        self.d["_temp"]["first_commit"] = commits[0]

        del commits

    async def delete(self):
        self.e.coll.files.delete_one({'_id': self.d["_id"]})

    def update(self, updates):
        self.e.coll.files.update_one({"_id": self.d['_id']}, updates)

    async def put(self, user=None):
        return await self.e.put(user, self.d["_id"], self.d)

    def _commits(self, ref):
        def _find(commit_id):
            for c in self.d["_temp"]["commits"]:
                if c["_id"] == commit_id:
                    return c

        #ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            ref = self.e.coll.refs.find_one({'name': ref})
            id0 = ref["commit_id"]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])

    async def has_read_permission(self, user):
        if user is None: 
            logger.info("read denied: user is None")
            return False
        
        if hasattr(self.e, 'h'):
            if user == self.e.h.root_user:
                logger.debug("read allowed: user is root_user")
                return True
        
        creator = await self.creator()
        
        logger.debug(f"user    = {user} {user.freeze()}")
        logger.debug(f"creator = {creator} {creator.freeze()}")

        b = user.freeze() == creator.freeze()

        if b:
            logger.debug("read allowed: user is creator")

        return b

    async def has_write_permission(self, user):
        if user is None:
            logger.info("write permission denied: user is None")
            return False

        if hasattr(self.e, 'h'):
            if user == self.e.h.root_user:
                logger.info("write permission allowed: user is root")
                return True

        creator = await self.creator()
        if user.freeze() == creator.freeze():
            logger.info("write permission allowed: user is creator")
            return True

        logger.info("write permission denied: user is not root or creator")
        logger.info(f"user = {user}")
        logger.info(f"user = {user}")
        return False

    async def creator_id(self):

        return self.d["_temp"]["commits"][0]["user"]

        if "_temp" in self.d:
            if "first_commit" in self.d["_temp"]:
                return self.d["_temp"]["first_commit"]["user"]

        commits = self.commits1()

        try:
            commit0 = next(commits)
        except StopIteration:
            print(crayons.red('no commits'))
            return

        my_id = bson.objectid.ObjectId("5b05b7a26c38a525cfd3e569")
        if 'user' not in commit0:
            print(crayons.red('no user'))
            pprint.pprint(commit0)
            #commit0['user'] = my_id
            #self.e.coll.commits.update_one({'_id': commit0['_id']}, 
            #{'$set': {'user': commit0['user']}})

        user_id = commit0['user']

    async def creator(self):
        user_id = await self.creator_id()
        user = await self.e.h.e_users._find_one("master", {"_id": user_id})
        assert user is not None
        return user
 
    def commits1(self):
        return self.e.coll.commits.find({"files.file_id": self.d["_id"]}).sort([('time', 1)])

    async def temp_messages(self):
        return
        yield
 

  
class Engine(elephant.Engine):
    """
    This implements the collection-wide commit concept
    
    The items we are tracking shall be called files.
    The database contains the following collections

    * files
    * commits
    * refs

    File structure shall be

        {
            # these key-value pairs make up the traditional content of a mongo item.
            # they are stored at the root of the item.
            # to elephant, this information is temporary, it can be automatically created based on version history
            # it is used for convenient access of a particular state of the item

            "_id": "123",
            "key1": "value1",
            "key2": "value2",
            
            # heres where the magic happends

            "_elephant": {
                "commit_id": "123",
            }
        }
    
    Ref structure shall be

        {
            "_id": "123",
            "name": "master",
            "commit_id": "123",
        }

    Commit structure shall be

        {
            "_id": "123",
            "parent": "122",
            "files": [
                {
                    "file_id": "123",
                    "changes": [] # list of aardvark diffs
                },
            ]
        }

    Note that commit ids are not mongo ids because commits are not items.
    Commit its will be managed by elephant.

    """
    def __init__(self, coll, ref_name, e_queries):
        self.coll = coll

        self.e_queries = e_queries

        self.ref_name = ref_name
        self._cache = {}

    async def pre_put_new(self, _): return _

    async def get_test_object(self, user, b0={}):
        b = await self._doc_class.get_test_document(b0)
        o = await self.put(user, None, b)
        return o

    async def create_indices(self):
        pass

    async def delete_test_documents(self):
        
        res = self.coll.files.delete_many({'test_field': {'$exists': True}})

        logger.info(f'{self.coll.files.name:34} deleted: {res.deleted_count}')

    def _pipe_read_permission(self, user):
        
        yield {'$match': {'_temp.first_commit.user': user.d["_id"]}}

    def pipe0(self, user):
        # for mongo aggregate
        
        for _ in self._pipe_read_permission(user): yield _

    def pipe1(self, sort=None):
        # for mongo aggregate

        if sort:
            yield {'$sort': bson.son.SON(sort)}

    async def object_or_id(self, o):

        if isinstance(o, bson.objectid.ObjectId):
            return o, await self._find_one_by_id(o)
        else:
            return o.d['_id'], o
 
    async def check(self):
        logger.warning(f'check collection {self.coll.name}')

        # delete test docs
        res = self.coll.files.delete_many({'test_field': {'$exists': True}})

        logger.warning(f'deleted {res.deleted_count} test documents')

        i = 0
        for d in self.coll.files.find():
            d1 = await self._factory(d)
            logger.debug(f'{d1}')
            await d1.check()
            i += 1
          
        logger.warning(f'checked {i} documents')

        logger.warning('check commits')

        for c in self.coll.commits.find():
            assert 'user' in c
            assert 'time' in c
            assert isinstance(c['time'], datetime.datetime)

    def ref(self):
        ref = self.coll.refs.find_one({'name': self.ref_name})

        if ref is not None: return ref

        ref = {
                'name': self.ref_name,
                'commit_id': None,
                }

        res = self.coll.refs.insert_one(ref)

        return ref

    def file_changes(self, file_id, diffs):
        diffs_array = [d.to_array() for d in diffs]
        return {
                'file_id': file_id,
                'changes': diffs_array,
                }

    def _create_commit(self, files_changes, user):

        ref = self.ref()
        
        assert user is not None

        commit = {
                'time': datetime.datetime.utcnow(),
                'parent': ref['commit_id'],
                'files': files_changes,
                'user': user.d["_id"],
                }
        
        res = self.coll.commits.insert_one(commit)
        
        commit['_id'] = res.inserted_id

        self.coll.refs.update_one(
                {'_id': ref['_id']}, 
                {'$set': {'commit_id': res.inserted_id}})

        return commit

    async def put_new(self, user, doc_new_0):

        doc_new_0 = await self.pre_put_new(doc_new_0)

        doc_new_0 = aardvark.util.clean(doc_new_0)

        # check before any database operations
        f0 = await self._factory(copy.deepcopy(doc_new_0))
        await f0.check_0()

        # need file id to create commit
        doc_new_encoded = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, doc_new_0)

        res = self.coll.files.insert_one(doc_new_encoded)

        file_id = res.inserted_id

        diffs = list(aardvark.diff(
                {}, 
                doc_new_encoded,
                ))

        commit = self._create_commit([self.file_changes(file_id, diffs)], user)

        # save ancestors
        item1 = dict(doc_new_0)
        item1["_id"] = file_id

        f = await self._factory(item1)

        await f.update_temp(user)

        update = {'$set': {
            '_elephant': {"commit_id": commit['_id']},
            '_temp':     await f.temp_to_array(user),
            }}

        logger.info(f'update = {update}')

        res = self.coll.files.update_one({'_id': file_id}, update)

        assert res.modified_count == 1

        await f.check()

        return f

    async def put(self, user, file_id, doc_new_0):

        #doc_new_0 = await elephant.util.encode(doc_new_0)

        if file_id is None:
            return await self.put_new(user, doc_new_0)

        doc_new_clean = aardvark.util.clean(doc_new_0)

        # check before any database operations
        _ = await self._factory(copy.deepcopy(doc_new_clean))
        await _.check_0()

        # get existing document
        obj_old = await self._find_one_by_id(file_id, check=False)

        doc_new_encoded = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, doc_new_clean)

        diffs = list(aardvark.diff(
                await obj_old.clean_encode(user),
                doc_new_encoded,
                ))

        # construct new object
        _ = copy.deepcopy(obj_old._d)
        _.update(doc_new_encoded)
        obj_new = await self._factory(_)

        await obj_new.update_temp(user)

        if not diffs:

            if obj_old.d.get("_temp", {}) != obj_new.d.get("_temp", {}):

                update = {'$set': {"_temp": await obj_new.temp_to_array(user)}}

                res = self.coll.files.update_one({'_id': file_id}, update)
            
            return obj_new

        if not (await obj_old.has_write_permission(user)):
            raise elephant.util.AccessDenied()

        commit = self._create_commit([self.file_changes(file_id, diffs)], user)
        
        update = aardvark.util.diffs_to_update(diffs, doc_new_encoded)

        if '$set' not in update:
            update['$set'] = {}

        update['$set']['_elephant.commit_id'] = commit["_id"]
        update['$set']['_temp'] = await obj_new.temp_to_array(user)

        logger.info("update:")
        logger.info(repr(update))

        res = self.coll.files.update_one({'_id': file_id}, update)

        self._cache[file_id] = obj_new

        await obj_new.check()

        return obj_new

    async def _find_one_by_id(self, _id, check=True):
        return await self._find_one({"_id": _id}, check=check)

    async def find_one_by_id(self, user, _id, check=True):
        return await self.find_one(user, {"_id": _id}, check=check)

    async def find_one_by_ref(self, user, ref):
        if not isinstance(ref, elephant.ref.DocRef): raise TypeError()
        return await self.find_one_by_id(user, ref._id)

    async def _find_one(self, query, pipe0=[], pipe1=[], check=True):
        """
        do not check permissions
        """

        pipe = pipe0 + [{'$match': query}] + pipe1

        c = self.coll.files.aggregate(pipe)

        try:
            d = next(c)
        except StopIteration:
            return None

        d1 = await self._factory(d)

        assert d1 is not None

        if check:
            try:
                await d1.check()
            except Exception as e:
                logging.error(crayons.red(f"query: {query}"))
                logging.error(crayons.red(f"pipe0: {pipe0}"))
                logging.error(crayons.red(f"pipe1: {pipe1}"))
                logging.error(crayons.red(f"{self!r}: check failed for {d!r}: {e!r}"))
                raise
                #await d1.update_temp(self.h.root_user)
                #await d1.check()

        return d1

    async def find_one(self, user, query, pipe0=[], pipe1=[], check=True):

        query = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, query)

        f = await self._find_one(query, check=check)

        if f is None: return None

        if not (await f.has_read_permission(user)): raise elephant.util.AccessDenied()

        return f

    async def _add_commits(self, user, files):
        files_ids = [f["_id"] for f in files]

        commits = list(self.coll.commits.find({"files.file_id": {"$in": files_ids}}))
        
        for f in files:
            if "_temp" not in f:
                f["_temp"] = {}
            
            commits1 = [c for c in commits if f["_id"] in [l["file_id"] for l in c["files"]]]
            
            assert commits1

            f["_temp"]["commits"] = commits1

            f1 = await self._factory(f)
            #f1.update_temp()

            if not (await f1.has_read_permission(user)): continue

            yield f1

    async def _find(self, query, pipe0=[], pipe1=[], check=True):

        pipe = [
            {'$match': query},
            ]
        pipe = pipe0 + pipe + pipe1

        pipe = await elephant.util.encode(self.h, None, elephant.EncodeMode.DATABASE, pipe)

        with elephant.util.stopwatch(logger_mongo.debug, "aggregate "):
            c = self.coll.files.aggregate(pipe, allowDiskUse=True)

        for d in c:

            if "_temp" not in d:
                raise Exception(f"document {d!r} has no _temp field")

            d1 = await self._factory(d)

            if check:
                await d1.check()

            yield d1

    async def find(self, user, query, pipe0=[], pipe1=[], check=True):
        
        async for d in self._find(query, pipe0, pipe1, check=check):

            if await d.has_read_permission(user):
                yield d
            else:
                logger.warning(crayons.yellow(f'permission denied for {d.d.get("title", None)}'))

    async def aggregate(self, user, pipeline_generator):

        pipe = list(pipeline_generator())

        c = self.coll.files.aggregate(pipe, allowDiskUse=True)
        
        for d in c:
            d1 = self._factory(d)
            if await d1.has_read_permission(user):
                yield d1
      

