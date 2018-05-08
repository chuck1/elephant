import datetime
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

    def __getitem__(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v
        updates = {'$set': {k: v}}
        self.e.db.files.update_one({'_id': self.d['_id']}, updates)

    def update_temp(self):
        """
        update self.d["_temp"] with calculated values to be stored in the database for querying
        """
        pass

    def update(self, updates):
        self.e.db.files.update_one({"_id": self.d['_id']}, updates)

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

def breakpoint(): import pdb; pdb.set_trace();

def clean_document(d0):
    d1 = dict(d0)

    keys_to_delete = [k for k in d1.keys() if k.startswith("_")]

    for k in keys_to_delete:
        del d1[k]

    return d1
   
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
        self._cache = {}
    
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
                'time': datetime.datetime.utcnow(),
                'parent': ref['commit_id'],
                'files': files_changes,
                }
        
        res = self.db.commits.insert_one(commit)
        
        commit['_id'] = res.inserted_id

        self.db.refs.update_one({'_id': ref['_id']}, {'$set': {'commit_id': res.inserted_id}})

        return commit

    def put_new(self, item):
        item = clean_document(item)

        # need file id to create commit
        res = self.db.files.insert_one(item)

        file_id = res.inserted_id

        diffs = list(aardvark.diff({}, item))
        
        commit = self._create_commit([self.file_changes(file_id, diffs)])

        # save ancestors
        item1 = dict(item)
        item1["_id"] = file_id
        item1["_temp"] = {}

        f = self._factory(item1)

        f.update_temp()

        self.db.files.update_one({'_id': file_id}, {'$set': {
            '_elephant': {"commit_id": commit['_id']},
            '_temp': f.d["_temp"],
            }})

        return res

    def put(self, file_id, item):

        if file_id is None:
            return self.put_new(item)

        item = clean_document(item)

        f = self.get_content({'_id': file_id})
        item0 = dict(f.d)

        #el0 = item0['_elephant']
        #el1 = dict(el0)

        item1 = clean_document(item0)

        diffs = list(aardvark.diff(item1, item))

        aardvark.apply(f.d, diffs)
        f.update_temp()
        
        commit = self._create_commit([self.file_changes(file_id, diffs)])
        
        update = elephant.util.diffs_to_update(diffs, item)

        update['$set']['_elephant.commit_id'] = commit["_id"]
        update['$set']['_temp'] = f.d["_temp"]

        res = self.db.files.update_one({'_id': file_id}, update)

        return res

    def get_file_by_id(self, _id):
        if _id in self._cache:
            return self._cache[_id]
        
        f = self.get_content({"_id": _id})

        self._cache[_id] = f

        return f

    def get_content(self, filt):
        f = self.db.files.find_one(filt)
        
        if f is None: return

        commits = list(self.db.commits.find({"files.file_id": f['_id']}))
        
        assert commits

        if "_temp" not in f:
            f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self._factory(f)

    def find(self, filt):

        files = list(self.db.files.find(filt))
        
        files_ids = [f["_id"] for f in files]

        commits = list(self.db.commits.find({"files.file_id": {"$in": files_ids}}))
        
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
            yield f1










