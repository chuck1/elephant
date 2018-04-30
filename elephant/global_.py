import json
import time
import hashlib
import bson

import aardvark

import elephant.util

class File:
    def __init__(self, e, d):
        self.e = e
        self.d = d

    def _commits(self, ref):
        def _find(commit_id):
            for c in self.d["_temp"]["commits"]:
                if c["_id"] == commit_id:
                    return c

        #ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            ref = self.e.db.refs.find_one({'name': ref})
            id0 = ref["commit_id"]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])

    def commits(self, ref):
        return reversed(list(self._commits(ref)))

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
    def __init__(self, db, ref_name, file_factory = None):
        self.db = db
        self.ref_name = ref_name
    
    def _factory(self, d):
        return File(self, d)
    
    def ref(self):
        ref = self.db.refs.find_one({'name': self.ref_name})

        if ref is not None: return ref

        ref = {
                'name': self.ref_name,
                'commit_id': None,
                }

        res = self.db.refs.insert_one(ref)

        return ref

    def file_changes(self, file_id, diffs):
        diffs_array = [d.to_array() for d in diffs]
        return {
                'file_id': file_id,
                'changes': diffs_array,
                }

    def _create_commit(self, files_changes):

        ref = self.ref()
        
        commit = {
                'time': time.time(),
                'parent': ref['commit_id'],
                'files': files_changes,
                }
        
        res = self.db.commits.insert_one(commit)
        
        commit['_id'] = res.inserted_id

        self.db.refs.update_one({'_id': ref['_id']}, {'$set': {'commit_id': res.inserted_id}})

        return commit

    def _put_new(self, item):

        # need file id to create commit
        res = self.db.files.insert_one(item)
        file_id = res.inserted_id

        diffs = list(aardvark.diff({}, item))
        
        commit = self._create_commit([self.file_changes(file_id, diffs)])

        self.db.files.update_one({'_id': file_id}, {'$set': {'_elephant': {"commit_id": commit['_id']}}})

        return res

    def put(self, file_id, item):

        item = dict(item)

        if '_temp' in item:
            del item['_temp']

        if file_id is None:
            return self._put_new(item)

        item0 = self.db.files.find_one({'_id': file_id})

        el0 = item0['_elephant']

        el1 = dict(el0)

        item1 = dict(item0)
        del item1['_id']
        del item1['_elephant']

        diffs = list(aardvark.diff(item1, item))
        
        commit = self._create_commit([self.file_changes(file_id, diffs)])
        
        update = elephant.util.diffs_to_update(diffs, item)
        
        update['$set']['_elephant'] = {'commit_id': commit['_id']}

        res = self.db.files.update_one({'_id': file_id}, update)

        return res

    def get_content(self, filt):
        f = self.db.files.find_one(filt)
        
        if f is None: return
        
        commits = list(self.db.commits.find({"file": f["_id"]}))
        
        f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self._factory(f)

    def find(self, filt):
        return self.db.files.find(filt)










