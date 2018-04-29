import json
import time
import hashlib
import datetime
import pprint
import bson.json_util

import aardvark

import elephant.util

class File:
    def __init__(self, d):
        self.d = d

    def _commits(self, ref = None):
        def _find(commit_id):
            for c in self.d["_temp"]["commits"]:
                if c["_id"] == commit_id:
                    return c

        ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            id0 = self.d["_elephant"]["refs"][ref]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])

    def commits(self, ref = None):
        return reversed(list(self._commits()))

        

class Local:
    """
    This implements the per-item version concept

    Item structure shall be

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
                "ref": "master"
                "refs": {
                    "master": "<commit id>"
                },
                "commits": {
                    "<commit id>": {
                        "id": "<commit id>",
                        "changes": [
                            # list of aardvark json-ized diffs
                        ]
                    }
                ]
            }
        }
    

    Note that commit ids are not mongo ids because commits are not items.
    Commit its will be managed by elephant.

    """
    def __init__(self, db, file_class = None):
        self.db = db
        self.file_class = file_class or File

    def _create_commit(self, file_id, parent, diffs):
        diffs_array = [d.to_array() for d in diffs]
        
        commit = {
                'file': file_id,
                'parent': parent,
                'changes': diffs_array,
                'time': datetime.datetime.utcnow(),
                }
 
        res = self.db.commits.insert_one(commit)

        return res.inserted_id

    def _put_new(self, ref, item):
        diffs = list(aardvark.diff({}, item))

        commit_id = self._create_commit(None, None, diffs)

        item1 = dict(item)

        item1['_elephant'] = {
                "ref": ref,
                "refs": {ref: commit_id},
                }

        res = self.db.files.insert_one(item1)

        self.db.commits.update_one({"_id": commit_id}, {"$set": {"file": res.inserted_id}})

        return res

    def put(self, ref, _id, item):
        # dont want to track _id or _elephant
        for k in ['_id', '_elephant']:
            if k in item: del item[k]

        if _id is None:
            return self._put_new(ref, item)

        item0 = self.db.files.find_one({'_id': _id})

        el0 = item0['_elephant']
        el1 = dict(el0)

        assert ref == item0['_elephant']['ref']

        item1 = dict(item0)
        del item1['_id']
        del item1['_elephant']

        diffs = list(aardvark.diff(item1, item))
        
        parent = el0['refs'][ref]
 
        commit_id = self._create_commit(_id, parent, diffs)
        
        el1['refs'][ref] = commit_id

        update = elephant.util.diffs_to_update(diffs, item)
        
        update['$set']['_elephant'] = el1

        res = self.db.files.update_one({'_id': _id}, update)

        return res

    def get_content(self, ref, filt):
        f = self.db.files.find_one(filt)

        assert ref == f['_elephant']['ref']

        commits = list(self.db.commits.find({"file": f["_id"]}))
        
        f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self.file_class(f)






