import json
import time
import hashlib

import bson.json_util

import aardvark

class CollectionLocal:
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
    def __init__(self, collection):
        self.collection = collection

    def _create_commit(self, parent, diffs):
        diffs_array = [d.to_array() for d in diffs]
        
        commit = {
                'parent': parent,
                'changes': diffs_array,
                'time': time.time(),
                }
        
        m = hashlib.md5()
        
        #s = json.dumps(commit)
        s = bson.json_util.dumps(commit)

        m.update(s.encode())

        h = m.hexdigest()

        commit['id'] = h

        return h, commit

    def _put_new(self, ref, item):
            diffs = list(aardvark.diff({}, item))

            h, commit = self._create_commit(None, diffs)

            item1 = dict(item)

            item1['_elephant'] = {
                    "ref": ref,
                    "refs": {ref: h},
                    "commits": {
                        h: commit,
                        },
                    }
    
            res = self.collection.insert_one(item1)

            return res

    def put(self, ref, _id, item):
        # dont want to track _id or _elephant
        for k in ['_id', '_elephant']:
            if k in item: del item[k]

        if _id is None:
            return self._put_new(ref, item)

        item0 = self.collection.find_one({'_id': _id})

        el0 = item0['_elephant']
        el1 = dict(el0)

        assert ref == item0['_elephant']['ref']

        item1 = dict(item0)
        del item1['_id']
        del item1['_elephant']

        diffs = list(aardvark.diff(item1, item))
        
        parent = el0['refs'][ref]
 
        h, commit = self._create_commit(parent, diffs)
        
        el1['commits'][h] = commit

        el1['refs'][ref] = h

        update = diffs_to_update(diffs, item)
        
        update['$set']['_elephant'] = el1

        res = self.collection.update_one({'_id': _id}, update)

        return res

    def get_content(self, ref, file_id):
        f = self.collection.find_one({'_id': file_id})

        assert ref == f['_elephant']['ref']

        del f['_id']
        del f['_elephant']
        return f


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




