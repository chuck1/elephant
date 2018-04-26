import datetime
import json
import time
import hashlib

import aardvark

def breakpoint(): import pdb; pdb.set_trace();

class DatabaseGlobal:
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
    def __init__(self, db, ref_name):
        self.db = db
        self.ref_name = ref_name

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
        
        # remove all keys that start with "_"
        # starts with "_" is used to identify fields controlled by elephant and should not be set by the user
        keys_to_delete = [k for k in item.keys() if k.startswith("_")]
        for k in keys_to_delete:
            del item[k]

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
        
        update = diffs_to_update(diffs, item)
        
        update['$set']['_elephant'] = {'commit_id': commit['_id']}

        res = self.db.files.update_one({'_id': file_id}, update)

        return res

    def get_content(self, filt):
        f = self.db.files.find_one(filt)
        if f is None: return
        return f

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
                print("didnt find any commits!")
            

            f["_temp"]["commits"] = commits1

            yield f

def diffs_keys_set(diffs):
    for d in diffs:
        if len(d.address.lines) > 1:
            yield d.address.lines[0].key

        if isinstance(d, aardvark.OperationRemove):
            continue
        
        yield d.address.lines[0].key

def diffs_keys_unset(diffs):
    for d in diffs:
        if isinstance(d, aardvark.OperationRemove):
            if len(d.address.lines) == 1:
                yield d.address.lines[0].key

def diffs_to_update(diffs, item):

    update_unset = dict((k, "") for k in diffs_keys_unset(diffs))

    update = {
            '$set': dict((k, item[k]) for k in diffs_keys_set(diffs)),
            }

    if update_unset:
        update['$unset'] = update_unset

    return update




