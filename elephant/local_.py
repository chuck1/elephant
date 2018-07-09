import json
import time
import hashlib
import datetime
import pprint
import bson.json_util
import crayons
import aardvark
import logging

import elephant.util
import elephant.file

logger = logging.getLogger(__name__)

class _User:
    def __init__(self):
        self.d = {}

class File(elephant.file.File):
    def __init__(self, e, d):
        super(File, self).__init__(e, d)

    def valid(self):
        pass

    def check(self):
        self.creator()

    def has_read_permission(self, user):
        return user.d["_id"] == self.creator()

    def has_write_permission(self, user):
        return user.d["_id"] == self.creator()

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

    def creator(self):
        if self.d.get('_root'): return None

        self._assert_elephant()

        commits = self.commits()

        my_id = bson.objectid.ObjectId("5b05b7a26c38a525cfd3e569")
        my_user = _User()
        my_user.d['_id'] = my_id

        try:
            commit0 = next(commits)
        except StopIteration:
            print(crayons.red('no commits'))

            try:
                commit1 = next(self.e.coll.commits.find({"file": self.d["_id"]}).sort([('time', 1)]))
                print('actually there are commits')
                raise Exception()
            except StopIteration:
                pass

            print(self.e)
            item = aardvark.util.clean(self.d)
            diffs = list(aardvark.diff({}, item))
            commit_id = self.e._create_commit(self.d['_id'], None, diffs, my_user)
            ref = 'master'
            item['_elephant'] = {
                    "ref": ref,
                    "refs": {ref: commit_id},
                    }
            res = self.coll.files.update_one(
                    {'_id': self.d['_id']}, {'$set': {'_elephant': item['_elephant']}})
            return

        if 'user' not in commit0:
            commit0 = self.e.coll.commits.find_one({'_id': commit0['_id']})
            print(crayons.yellow('local: no user in commit'))
            if 'user' not in commit0:
                print(crayons.red('local: no user'))
                pprint.pprint(commit0)
                commit0['user'] = my_id
                res = self.e.coll.commits.update_one({'_id': commit0['_id']}, 
                        {'$set': {'user': commit0['user']}})
                print(res.modified_count)
                res = self.e.coll.commits.find_one({'_id': commit0['_id']})
                pprint.pprint(res)
                assert 'user' in res
            else:
                self.update_temp()
                self.put('master', None)

        return commit0['user']
 
    def put(self, ref, user):
        return self.e.put(ref, self.d["_id"], self.d, user)

    #def commits(self):
    #    return self.e.coll.commits.find({"file": self.d["_id"]}).sort([('time', 1)])
       
    def delete(self):
        self.e.coll.files.delete_one({'_id': self.d["_id"]})

    def update_temp(self):
        """
        update self.d["_temp"] with calculated values to be stored in the database for querying
        """
        commits = list(self.e.coll.commits.find({"file": self.d["_id"]}))
        
        self.d["_temp"] = {}

        self.d["_temp"]["commits"] = commits

    async def temp_messages(self):
        return
        yield

class Engine:
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
        self.coll = coll
        self.e_queries = e_queries

    async def create_indices(self):
        pass

    async def check(self):
        logger.info(f'check collection {self.coll.name}')

        # delete test docs
        res = self.coll.files.delete_many({'test_field': {'$exists': True}})

        logger.info(f'deleted {res.deleted_count} test documents')

        i = 0
        for d in self.coll.files.find():
            d1 = self._factory(d)
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

    def _factory(self, d):
        return File(self, d)

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
        
        commit = {
                'file': file_id,
                'parent': parent,
                'changes': diffs_array,
                'time': datetime.datetime.utcnow(),
                'user': user.d["_id"],
                }
 
        res = self.coll.commits.insert_one(commit)

        return res.inserted_id

    def _put_new(self, ref, item, user):
        diffs = list(aardvark.diff({}, item))

        commit_id = self._create_commit(None, None, diffs, user)

        item1 = aardvark.util.clean(item)

        item1['_elephant'] = {
                "ref": ref,
                "refs": {ref: commit_id},
                }

        res = self.coll.files.insert_one(item1)

        self.coll.commits.update_one(
                {"_id": commit_id}, {"$set": {"file": res.inserted_id}})

        item1['_id'] = res.inserted_id

        return self._factory(item1)

    def put(self, ref, _id, doc_new_0, user):

        if _id is None:
            return self._put_new(ref, doc_new_0, user)

        doc_old_0 = self.coll.files.find_one({'_id': _id})

        doc_old_1 = aardvark.util.clean(doc_old_0)
        doc_new_1 = aardvark.util.clean(doc_new_0)

        el0 = doc_old_0['_elephant']
        el1 = dict(el0)

        assert ref == el0['ref']

        diffs = list(aardvark.diff(doc_old_1, doc_new_1))

        logger.info('diffs')
        for d in diffs:
            logger.info(repr(d))

        if not diffs:
            d = self._factory(doc_new_0)
            d.update_temp()

            if doc_old_0.get("_temp", {}) != d.d["_temp"]:
                logger.info('update temp')
                update = {'$set': {}}
                update['$set']['_temp'] = d.d["_temp"]
                res = self.coll.files.update_one({'_id': _id}, update)
            
            return

        parent = el0['refs'][ref]
 
        d = self.get_content('master', user, {'_id': _id})

        if not d.has_write_permission(user):
            raise otter.AccessDenied()

        commit_id = self._create_commit(_id, parent, diffs, user)
        
        el1['refs'][ref] = commit_id

        update = aardvark.util.diffs_to_update(diffs, doc_new_1)
        
        if '$set' not in update:
            update['$set'] = {}

        update['$set']['_elephant'] = el1

        res = self.coll.files.update_one({'_id': _id}, update)

        return res

    def _find_one(self, ref, q):
        return self.get_content(ref, None, q)

    def find_one(self, user, ref, q):
        return self.get_content(ref, user, q)

    def _assert_elephant(self, f, f0):
        if '_elephant' not in f:

            if f.get('_root', False): return            

            print(crayons.red('local doc has no _elephant field'))
            raise Exception()

            ref = 'master'
            f['_elephant'] = {
                    "ref": ref,
                    "refs": {ref: f0.commit0()['_id']},
                    }
            self.coll.files.update_one({'_id': f['_id']}, {'$set': {'_elephant': f['_elephant']}})

        assert f['_elephant']

    def get_content(self, ref, user, filt):
        f = self.coll.files.find_one(filt)
        if f is None: return None

        f0 = self._factory(f)

        if f.get('_root', False): return f0

        self._assert_elephant(f, f0)

        
        if ref == f['_elephant']['ref']:
            pass
        elif ref == f["_elephant"]["refs"][f["_elephant"]["ref"]]:
            pass
        else:
            Exception(f'ref {ref} does not match {f["_elephant"]["ref"]} or {f["_elephant"]["refs"][f["_elephant"]["ref"]]}')

        commits = list(self.coll.commits.find({"file": f["_id"]}))
        
        f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self._factory(f)

    def find(self, user, filt):
        for d in self.coll.files.find(filt):
            d1 = self._factory(d)
 
            if not d1.has_read_permission(user):
                continue
 
            yield d1





