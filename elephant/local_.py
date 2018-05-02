import json
import time
import hashlib
import datetime
import pprint
import bson.json_util

import aardvark

import elephant.util
import elephant.file

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
    def __init__(self, db):
        self.db = db

    def _factory(self, d):
        return elephant.file.File(self, d)

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
        if f is None: return None
        assert f['_elephant']
        
        if ref == f['_elephant']['ref']:
            pass
        elif ref == f["_elephant"]["refs"][f["_elephant"]["ref"]]:
            pass
        else:
            Exception(f'ref {ref} does not match {f["_elephant"]["ref"]} or {f["_elephant"]["refs"][f["_elephant"]["ref"]]}')

        commits = list(self.db.commits.find({"file": f["_id"]}))
        
        f["_temp"] = {}

        f["_temp"]["commits"] = commits

        return self._factory(f)

    def find(self, filt):
        return [self._factory(d) for d in self.db.files.find(filt)]





