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

logger = logging.getLogger(__name__)
logger_mongo = logging.getLogger(__name__ + "-mongo")
           

class Engine(elephant.Engine):
    """
    This implements the per-item version concept

    Item structure shall be

        {
            # these key-value pairs make up the traditional content of a mongo item.
            # they are stored at the root of the item.
            # to elephant, this information is temporary, it can be automatically 
            # created based on version history
            # it is used for convenient access of a particular state of the item

            "_id": <bson.objectid.ObjectId>,

            # this section is basically a 'working directory'
            # it reflects the state of the document at the commit refered to by the
            # '_elephant.ref' field below

            "key1": "value1",
            "key2": "value2",
            
            # heres where the magic happends

            "_elephant": {
                "ref": "master"
                "refs": {
                    "master": "<commit id>"
                },
                ]
            }
        }
    
    Commits shall be

        {
            "_id": <commit id>,
            "file_id": 
            "user": 
            "time": 
            "changes": [
                # list of aardvark json-ized diffs
            ]
        }

    Note that commit ids are not mongo ids because commits are not items.
    Commit its will be managed by elephant.

    """
    def __init__(self, coll, e_queries=None):
        super().__init__()
        self.coll = coll
        self.e_queries = e_queries

        # the commit graph for the docs in this collection
        self.commit_graph = nx.DiGraph()

        self._doc_class = elephant.local_.doc.Doc

    async def create_indices(self):
        pass

    async def get_test_object(self, user, b0={}):
        b = await self._doc_class.get_test_document(b0)
        o = await self.put(user, "master", None, b)
        return o

    async def check(self):
        logger.info(f'check collection {self.coll.name}')

        # delete test docs
        res = self.coll.files.delete_many({'test_field': {'$exists': True}})

        logger.info(f'deleted {res.deleted_count} test documents')

        i = 0
        for d in self.coll.files.find():
            d1 = await self._factory(d)
            d1.check()
            i += 1
        if i == 0:
            logger.info(f'checked {i} documents')

        logger.info('check commits')
        my_id = bson.objectid.ObjectId("5b05b7a26c38a525cfd3e569")
        for c in self.coll.commits.find():
            assert 'user' in c
            assert 'time' in c
            assert isinstance(c['time'], datetime.datetime)

    def _commit_path(self, c0, c1):

        

        paths_ = nx.shortest_simple_paths(g,
                c0['_id'],
                c1['_id'],
                )


    def _create_commit_1(self, file_id, parent, diffs):
        diffs_array = [d.to_array() for d in diffs]
        
        commit = {
                'file': file_id,
                'parent': parent,
                'changes': diffs_array,
                'time': datetime.datetime.utcnow(),
                }
 
        res = self.coll.commits.insert_one(commit)

        return res.inserted_id

    def _create_commit(self, file_id, parent, diffs, user):
        diffs_array = [d.to_array() for d in diffs]
        
        elephant.check.check(diffs_array, bson.json_util.dumps)

        commit = {
                'file':    file_id,
                'parent':  parent,
                'changes': diffs_array,
                'time':    datetime.datetime.utcnow(),
                'user':    user.d["_id"],
                }
 
        res = self.coll.commits.insert_one(commit)

        return res.inserted_id

    async def pre_put_new(self, d_0):
        """
        override to add data to new documents before they are created
        this is called before clean, so underscore-escaped fields will have no effect
        """
        return d_0

    async def _put_new(self, user, ref, doc_new_0):
        assert isinstance(doc_new_0, dict)

        doc_new_0 = await self.pre_put_new(doc_new_0)

        doc_new_0 = aardvark.util.clean(doc_new_0)

        # check before any database operations
        f0 = await self._factory(copy.deepcopy(doc_new_0), False)
        await f0.check_0()

        doc_new_encoded = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, doc_new_0)

        # calculate diffs
        diffs = list(aardvark.diff({}, doc_new_encoded))

        commit_id = self._create_commit(None, None, diffs, user)

        logger.debug(f"new document commit: {commit_id}")

        el = {
                "ref": ref,
                "refs": {ref: commit_id},
                }

        doc_new_0['_elephant'] = el

        doc_new_encoded["_elephant"] = el

        # insert into database

        res = self.coll.files.insert_one(doc_new_encoded)

        # update the commit in the database with the inserted id

        self.coll.commits.update_one(
                {"_id": commit_id}, {"$set": {"file": res.inserted_id}})

        doc_new_0['_id'] = res.inserted_id

        o = await self._factory(doc_new_0, False)
  
        await o.update_temp(user)

        # check
        await o.check_0()
        await o.check()

        # update database with temp
        update = {"$set": {}}
        update['$set']['_temp'] = await elephant.util.encode(
                self.h, 
                user, 
                elephant.EncodeMode.DATABASE,
                o.d["_temp"])

        res1 = self.coll.files.update_one({'_id': res.inserted_id}, update)
        assert res1.modified_count == 1

        return o 

    async def put(self, user, ref, _id, data_new_0):
        if _id is None:
            return await self._put_new(user, ref, data_new_0)

        assert isinstance(data_new_0, dict)

        data_new_0 = aardvark.util.clean(data_new_0)

        doc_new_encoded = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, data_new_0)

        doc_old_0 = await self._find_one_by_id(ref, _id)

        #await doc_old_0.check()

        if doc_old_0 is None:
            raise Exception(f"Error updating document. Document not found ref = {ref} id = {_id}")

        # check before any database operations
        doc_new_1 = copy.deepcopy(doc_old_0.d)
        doc_new_1.update(data_new_0)
        f0 = await self._factory(doc_new_1, False)
        await f0.check_0()

        # check permissions
        # TODO use query to do this
 
        if not (await doc_old_0.has_write_permission(user)):
            raise otter.AccessDenied()

        # calculate diff
        # for doc_0, use _d which is the undecoded data
        # for doc_1, encode the data
        # this way we are compaing what will be stored in the DB
  
        doc_old_1 = aardvark.util.clean(doc_old_0._d)

        el0 = doc_old_0.d['_elephant']
        el1 = dict(el0)

        assert ref == el0['ref']

        diffs = list(aardvark.diff(doc_old_1, doc_new_encoded))

        # update the old document object

        #_ = aardvark.util.clean(doc_old_0.d)

        #elephant.check.check(_, copy.deepcopy)
        #copy.deepcopy(_)

        #aardvark.apply(_, diffs)

        #doc_old_0.d.update(_)

        doc_old_0.d.update(data_new_0)

        # might not have _temp field, next line would fail if not
        await doc_old_0.update_temp(user)

        # make sure new data passes checks
        await doc_old_0.check()

        # update temp

        temp_old = dict(doc_old_0.d.get("_temp", {}))

        if not diffs:
            logger.info("diffs is empty")

            try:
                await doc_old_0.update_temp(user)
             
            except Exception as e:
                logger.error(f"failed to update_temp for previous version: {e!r}")
                raise
                
                #update = {'$set': {"_temp": await doc_old_0.temp_to_array(user)}}
                #res = self.coll.files.update_one({'_id': _id}, update)

            else:

                if doc_old_0.d.get("_temp", {}) != temp_old:
                    logger.info('document unchanged but temp change')
                    update = {'$set': {"_temp": await doc_old_0.temp_to_array(user)}}
                    res = self.coll.files.update_one({'_id': _id}, update)
                
                return doc_old_0


        # create commit

        parent = el0['refs'][ref]
        commit_id = self._create_commit(_id, parent, diffs, user)

        # point ref to new commit id
        
        el1['refs'][ref] = commit_id

        logger.info(f"{ref}: {parent} -> {commit_id}")

        # update database

        update = aardvark.util.diffs_to_update(diffs, doc_new_encoded)
        
        if '$set' not in update:
            update['$set'] = {}

        update['$set']['_elephant'] = el1

        update['$set']['_temp'] = await elephant.util.encode(
                self.h, 
                user, 
                elephant.EncodeMode.DATABASE,
                doc_old_0.d["_temp"])

        elephant.check.check(update, bson.json_util.dumps)

        pprint.pprint(update)

        res = self.coll.files.update_one({'_id': _id}, update)

        assert res.modified_count == 1

        # update the document object

        doc_old_0.d["_elephant"] = el1

        return doc_old_0

    def _assert_elephant(self, f, f0):
        if '_elephant' not in f:

            if f.get('_root', False): return            

            print(crayons.red('local doc has no _elephant field'))
            pprint.pprint(f)
            raise Exception()

            ref = 'master'
            f['_elephant'] = {
                    "ref": ref,
                    "refs": {ref: f0.commit0()['_id']},
                    }
            self.coll.files.update_one({'_id': f['_id']}, {'$set': {'_elephant': f['_elephant']}})

        assert f['_elephant']

    async def delete_test_documents(self):
        
        res = self.coll.files.delete_many({'test_field': {'$exists': True}})

        logger.info(f'{self.coll.files.name:34} deleted: {res.deleted_count}')

    async def get_path(self, c_id_0):
        # assumes that c_id_0 is older than c_id_1

        c_0 = self.coll.commits.find_one({"_id": c_id_0})

        assert c_0 is not None
  
        path = [c_0]

        # move backward in time to first commit
        while True:
            if path[-1]["parent"] is None: break
            c = self.coll.commits.find_one({"_id": path[-1]["parent"]})
            path.append(c)

        path = list(reversed(path))

        return path

    async def apply_path(self, path, a):
        
        for c in path:
            diffs = aardvark.parse_diffs(c['changes'])
            a = aardvark.apply(a, diffs)

        return a

    async def _find_one_by_id(self, ref, _id):
        return await self._find_one(ref, {"_id": _id})

    async def find_one_by_id(self, user, ref, _id):
        return await self.find_one(user, ref, {"_id": _id})

    async def find_one_by_ref(self, user, ref):
        if not isinstance(ref, elephant.ref.DocRef): raise TypeError()
        return await self.find_one_by_id(user, ref.ref, ref._id)

    async def find_one(self, user, ref, q, temp=True):

        q = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, q)

        d = await self._find_one(ref, q)

        if d is None: return 

        # needed if reference is to older version. see second method in _find_one function
        # but have option to not get temp if this is to be a subobject
        if temp:
            await d.update_temp(user)
        else:
            d.clear_temp()

        #await d.check()

        if not await d.has_read_permission(user):
            raise Exception("Access Denied")

        return d

    async def _find_one(self, ref, filt={}):

        breakpoint()

        # hide hidden docs
        if "hide" in filt: raise Exception('query contains reserved field "hide"')
        filt["hide"] = {"$not": {"$eq": True}}

        logger.info('query')
        for line in elephant.util.lines(pprint.pprint, filt):
            logger.info(f'  {line}')

        f = self.coll.files.find_one(filt)

        logger.debug(f'f = {f!r}')

        if f is None: return None

        logger.info('encoded')
        for line in elephant.util.lines(pprint.pprint, f):
            logger.info(f'  {line}')
        

        f0 = await self._factory(f, False)
 
        # TODO do we really need this?
        if f.get('_root', False): return f0

        self._assert_elephant(f, f0)
        
        if (ref is None) or (ref == f['_elephant']['ref']) or (ref == f["_elephant"]["refs"][f["_elephant"]["ref"]]):

            logger.info('ref matches. return')
            
            #commits = list(self.coll.commits.find({"file": f["_id"]}))
            #f["_temp"] = {}
            #f["_temp"]["commits"] = commits
    
            return f0

        else:
            
            #await f0.update_temp(user)

            #print('commits')
            #for c in f0.d['_temp']['commits']:
            #    print(f'  {c}')

            c_id_1 = f["_elephant"]["refs"][f["_elephant"]["ref"]]

            path = await self.get_path(ref)

            a = await self.apply_path(path, {})

            print('created:')
            pprint.pprint(a)

            a['_id'] = f['_id']

            if '_elephant' not in a:
                a['_elephant'] = {}

            a['_elephant']['ref'] = ref
            a['_elephant']['refs'] = f['_elephant']['refs']

            f2 = await self._factory(a)

            return f2

    def pipe0_no_permissions(self, user):
        yield {"$match": {"hide": {"$not": {"$eq": True}}}}

    def pipe0(self, user):
        yield from self.pipe0_no_permissions(user)

    def pipe1(self, sort=None):
        return 
        yield

    async def _find(self, query={}, pipe0=None, pipe1=[], check=True):

        if pipe0 is None: pipe0 = list(self.pipe0_no_permissions(None))

        assert isinstance(pipe0, list)
        assert isinstance(pipe1, list)

        pipe = pipe0 + [{'$match': query}] + pipe1

        logger.debug('local find pipeline')
        for _ in pipe:
            logger.debug(f'  {_!r}')

        pipe = await elephant.util.encode(self.h, None, elephant.EncodeMode.DATABASE, pipe)

        with elephant.util.stopwatch(logger_mongo.debug, "aggregate "):
            c = self.coll.files.aggregate(pipe)

        for d in c:

            yield await self._factory(d, False)

    async def find(self, user, query, pipe0=[], pipe1=[]):
        assert isinstance(pipe0, list)
        assert isinstance(pipe1, list)

        query = await elephant.util.encode(self.h, user, elephant.EncodeMode.DATABASE, query)

        async for d in self._find(query, pipe0, pipe1):

            if await d.has_read_permission(user):
                yield d

    async def aggregate(self, user, pipeline_generator):

        pipe = list(pipeline_generator())

        c = self.coll.files.aggregate(pipe, allowDiskUse=True)
        
        for d in c:
            d1 = await self._factory(d)
            if await d1.has_read_permission(user):
                yield d1





