import itertools
import datetime
import json
import time
import hashlib
import bson
import logging

import aardvark
import elephant.util

logger = logging.getLogger(__name__)

class File:
    def __init__(self, e, d):
        self.e = e
        self.d = d

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        return default

    def __getitem__(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v
        updates = {'$set': {k: v}}
        self.e.coll.files.update_one({'_id': self.d['_id']}, updates)

    def valid(self):
        pass

    def check(self):
        self.creator()

    def update_temp(self):
        """
        update self.d["_temp"] with calculated values to be stored in the database for querying
        """
        pass

    def delete(self):
        self.e.coll.files.delete_one({'_id': self.d["_id"]})

    def update(self, updates):
        self.e.coll.files.update_one({"_id": self.d['_id']}, updates)

    def put(self, user):
        return self.e.put(self.d["_id"], self.d, user)

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

    def commits(self, ref):
        return reversed(list(self._commits(ref)))

    def has_read_permission(self, user):
        if user is None: return False
        return user.d["_id"] == self.creator()

    def has_write_permission(self, user):
        if user is None: return False
        return user.d["_id"] == self.creator()

    def creator(self):
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

        return commit0['user']
 
    def commits1(self):
        return self.e.coll.commits.find({"files.file_id": self.d["_id"]}).sort([('time', 1)])
 
def breakpoint(): import pdb; pdb.set_trace();

  
class Global:
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

    def pipe1(self, sort=None):
        # for mongo aggregate
        if sort is not None:
            return [
                    {'$sort': bson.son.SON(sort)}
                    ]
        else:
            return []
 
    def _factory(self, d):
        return File(self, d)
    
    def check(self):
        logger.info('check documents')
        i = 0
        for d in self.coll.files.find():
            d1 = self._factory(d)
            d1.check()
            i += 1
        if i == 0:
           print(f'check {self.coll}')
           print(f'checked {i} documents')

        logger.info('check commits')
        my_id = bson.objectid.ObjectId("5b05b7a26c38a525cfd3e569")
        for c in self.coll.commits.find():
            #if 'user' not in c:
            #    pprint.pprint(c)
            #    logger.error('commit does not have field user')
            #    self.coll.commits.update_one({"_id": c["_id"]}, {"$set": {'user': my_id}})
            assert 'user' in c

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

        self.coll.refs.update_one({'_id': ref['_id']}, {'$set': {'commit_id': res.inserted_id}})

        return commit

    def put_new(self, item, user):
        item = elephant.util.clean_document(item)

        # need file id to create commit
        res = self.coll.files.insert_one(item)

        file_id = res.inserted_id

        diffs = list(aardvark.diff({}, item))
        
        commit = self._create_commit([self.file_changes(file_id, diffs)], user)

        # save ancestors
        item1 = dict(item)
        item1["_id"] = file_id
        item1["_temp"] = {}

        f = self._factory(item1)

        f.update_temp()

        self.coll.files.update_one({'_id': file_id}, {'$set': {
            '_elephant': {"commit_id": commit['_id']},
            '_temp': f.d["_temp"],
            }})

        return res

    def put(self, file_id, item, user):

        if file_id is None:
            return self.put_new(item, user)

        item = elephant.util.clean_document(item)

        f = self.get_content({"_id": file_id})
        item0 = dict(f.d)


        item1 = elephant.util.clean_document(item0)

        diffs = list(aardvark.diff(item1, item))

        aardvark.apply(f.d, diffs)

        f.update_temp()

        if not diffs:
            if item0.get("temp", {}) != f.d["_temp"]:
                update = {'$set': {}}
                update['$set']['_temp'] = f.d["_temp"]
                res = self.coll.files.update_one({'_id': file_id}, update)
            
            return

        if not f.has_write_permission(user):
            raise elephant.util.AccessDenied()

        commit = self._create_commit([self.file_changes(file_id, diffs)], user)
        
        update = elephant.util.diffs_to_update(diffs, item)

        if '$set' not in update:
            update['$set'] = {}

        update['$set']['_elephant.commit_id'] = commit["_id"]
        update['$set']['_temp'] = f.d["_temp"]

        res = self.coll.files.update_one({'_id': file_id}, update)

        self._cache[file_id] = f

        return res

    def get_file_by_id(self, _id):
        #if _id in self._cache:
        #    return self._cache[_id]
        
        f = self.get_content({"_id": _id})

        self._cache[_id] = f

        return f

    def get_content(self, filt):
        f = self.coll.files.find_one(filt)
        
        if f is None: return

        commits = list(self.coll.commits.find({"files.file_id": f['_id']}))
        
        assert commits

        if "_temp" not in f:
            f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self._factory(f)

    def _add_commits(self, user, files):
        files_ids = [f["_id"] for f in files]

        commits = list(self.coll.commits.find({"files.file_id": {"$in": files_ids}}))
        
        for f in files:
            if "_temp" not in f:
                f["_temp"] = {}
            
            commits1 = [c for c in commits if f["_id"] in [l["file_id"] for l in c["files"]]]
            
            if not commits1:
                #print(f["_id"])
                #for c in commits:
                #    print([l["file_id"] for l in c["files"]])
                print(f"didnt find any commits for {f}")
            
            assert commits1

            f["_temp"]["commits"] = commits1

            f1 = self._factory(f)
            #f1.update_temp()

            if not f1.has_read_permission(user): continue

            yield f1

    def _find(self, query, pipe1=[]):
        pipe = [
            {'$match': query},
            ]
        pipe += pipe1

        c = self.coll.files.aggregate(pipe)

        for d in c:
            yield self._factory(d)

    def find(self, user, query, pipe1=[]):
        n = 30
        
        pipe = [
            {'$match': query},
            ]
        pipe += pipe1

        c = self.coll.files.aggregate(pipe)

        files0 = []
        files1 = []

        for i in range(n):
            try:
                files0.append(next(c))
            except StopIteration:
                break
        
        yield list(self._add_commits(user, files0))

        for f in c:
            files1.append(f)
        
        if files1:
            yield list(self._add_commits(user, files1))









