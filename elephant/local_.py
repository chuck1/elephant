import json
import time
import hashlib
import datetime
import pprint
import bson.json_util
import crayons
import aardvark

import elephant.util
import elephant.file

class File(elephant.file.File):
    def __init__(self, e, d):
        super(File, self).__init__(e, d)

    def creator(self):
        commits = self.commits()

        my_id = bson.objectid.ObjectId("5b05b7a26c38a525cfd3e569")

        try:
            commit0 = next(commits)
        except StopIteration:
            print(crayons.red('no commits'))
            print(self.e)
            item = elephant.util.clean_document(self.d)
            diffs = list(aardvark.diff({}, item))
            commit_id = self.e._create_commit(self.d['_id'], None, diffs, my_id)
            ref = 'master'
            item['_elephant'] = {
                    "ref": ref,
                    "refs": {ref: commit_id},
                    }
            return

        if 'user' not in commit0:
            print(crayons.red('no user'))
            pprint.pprint(commit0)
            commit0['user'] = my_id
            self.e.coll.commits.update_one({'_id': commit0['_id']}, 
                    {'$set': {'user': commit0['user']}})

        return commit0['user']
 
    def commits(self):
        return self.e.coll.commits.find({"file": self.d["_id"]}).sort([('time', 1)])
       
class Engine:
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
    def __init__(self, coll, e_queries=None):
        self.coll = coll
        self.e_queries = e_queries

    def check(self):
        i = 0
        for d in self.coll.files.find():
            d1 = self._factory(d)
            d1.creator()
            i += 1
        if i == 0:
            print(f'check {self.coll}')
            print(f'checked {i} documents')

    def _factory(self, d):
        return File(self, d)

    def _create_commit(self, file_id, parent, diffs, user_id):
        diffs_array = [d.to_array() for d in diffs]
        
        commit = {
                'file': file_id,
                'parent': parent,
                'changes': diffs_array,
                'time': datetime.datetime.utcnow(),
                'user': user_id,
                }
 
        res = self.coll.commits.insert_one(commit)

        return res.inserted_id

    def _put_new(self, ref, item, user_id):
        diffs = list(aardvark.diff({}, item))

        commit_id = self._create_commit(None, None, diffs, user_id)

        item1 = elephant.util.clean_document(item)

        item1['_elephant'] = {
                "ref": ref,
                "refs": {ref: commit_id},
                }

        res = self.coll.files.insert_one(item1)

        self.coll.commits.update_one({"_id": commit_id}, {"$set": {"file": res.inserted_id}})

        return res

    def put(self, ref, _id, item, user_id):

        if _id is None:
            return self._put_new(ref, item, user_id)

        item1 = elephant.util.clean_document(item)

        item0 = self.coll.files.find_one({'_id': _id})

        el0 = item0['_elephant']
        el1 = dict(el0)

        assert ref == item0['_elephant']['ref']

        item1 = dict(item0)
        del item1['_id']
        del item1['_elephant']

        diffs = list(aardvark.diff(item1, item))
        
        parent = el0['refs'][ref]
 
        commit_id = self._create_commit(_id, parent, diffs, user_id)
        
        el1['refs'][ref] = commit_id

        update = elephant.util.diffs_to_update(diffs, item)
        
        update['$set']['_elephant'] = el1

        res = self.coll.files.update_one({'_id': _id}, update)

        return res

    def get_content(self, ref, filt):
        f = self.coll.files.find_one(filt)
        if f is None: return None
        assert f['_elephant']
        
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

    def find(self, filt):
        return [self._factory(d) for d in self.coll.files.find(filt)]





