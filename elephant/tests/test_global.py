import time
import pprint

import pymongo
import pytest

import elephant.global_

def breakpoint(): import pdb; pdb.set_trace();

class User:
    def __init__(self):
        self.d = {}

@pytest.mark.parametrize('a,b', [
    ({'a': 1}, {'a': 2}),
    ({'a': 1}, {'a': 1, 'b': 2}),
    ({'a': 1}, {}),
    ])
def test_1(client, database, a, b):
    print()

    db = database
   
    user = User()
    user.d["_id"] = db.users.insert_one({}).inserted_id
 
    e = elephant.global_.Global(
            db, 
            "master",
            #elephant.file.Engine(db.queries),
            None
            )
    
    res = e.put(None, a, user)

    _id = res.inserted_id

    res = e.put(_id, b, user)
    
    item = e.get_content({'_id': _id})
    
    print(item)

    print('commits')
    for c in item.d["_temp"]["commits"]:
        pprint.pprint(c)

    print('commits')
    for c in item.commits("master"):
        pprint.pprint(c)

        item.commits(c["_id"])

    


