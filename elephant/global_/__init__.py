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
import elephant.commit

logger = logging.getLogger(__name__)
logger_mongo = logging.getLogger(__name__ + "-mongo")

class Temp: pass

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
            # to elephant, this information is temporary, it can be automatically 
            # created based on version history
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
    def __init__(self, coll, ref_name, e_queries=None):
        self.coll = coll

        self.e_queries = e_queries

        self.ref_name = ref_name
        self._cache = {}

        self._doc_class = elephant.global_.doc.Doc

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
        # TODO does not work for root_user
        yield {'$match': {'_temp.commits.0.CommitGlobal.2': user.d["_id"]}}

    def pipe0_no_permissions(self, user):
        yield {"$match": {"hide": {"$not": {"$eq": True}}}}

    def pipe0(self, user):

        for _ in self.pipe0_no_permissions(user): yield _
        
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

        pipe0 - None or line of aggregate stages. If None, self.pipe0() will be used.
        """

        pipe = pipe0 + [{'$match': query}] + pipe1

        logger.info("pipe")
        for stage in pipe:
            logger.info(f"    {stage}")

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
                #await d1.check()

        return d1

    async def find_one(self, user, query, pipe0=None, pipe1=[], check=True):

        if pipe0 is None: pipe0 = list(self.pipe0_no_permissions(user))

        query = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, query)

        f = await self._find_one(query, pipe0=pipe0, check=check)

        if f is None: return None

        if not (await f.has_read_permission(user)):
            raise elephant.util.AccessDenied()

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

            if not (await f1.has_read_permission(user)): continue

            yield f1

    async def _find(self, query, pipe0=None, pipe1=[], check=True):

        if pipe0 is None: pipe0 = list(self.pipe0_no_permissions(None))

        pipe = [
            {'$match': query},
            ]
        pipe = pipe0 + pipe + pipe1

        logger.info('pipe')
        for p in pipe:
            logger.info(f'  {p!r}')

        pipe = await elephant.util.encode(self.h, None, elephant.EncodeMode.DATABASE, pipe)

        with elephant.util.stopwatch(logger_mongo.debug, "aggregate "):
            c = self.coll.files.aggregate(pipe, allowDiskUse=True)

        for d in c:

            #if "_temp" not in d:
            #    raise Exception(f"document {d!r} has no _temp field")

            d1 = await self._factory(d)

            if check:
                await d1.check()

            yield d1

    async def find(self, user, query, pipe0=[], pipe1=[], check=True):
        
        logger.info(f'user = {user.d["_id"]!r}')

        async for d in self._find(query, pipe0, pipe1, check=check):
            b = user.d["_id"] == d._d["_temp"]["commits"][0]["CommitGlobal"][2]
            logger.info(f"user is creator = {b}")

            if await d.has_read_permission(user):
                yield d
            else:
                logger.warning(crayons.yellow(f'permission denied for {d.d.get("title", None)}'))

    async def aggregate(self, user, pipeline_generator):

        pipe = list(pipeline_generator())

        c = self.coll.files.aggregate(pipe, allowDiskUse=True)
        
        for d in c:
            d1 = await self._factory(d)
            if await d1.has_read_permission(user):
                yield d1
      


