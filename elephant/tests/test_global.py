import time
import pprint

import pymongo
import pytest

import elephant.global_

def breakpoint(): import pdb; pdb.set_trace();

@pytest.mark.parametrize('a,b', [
    ({'a': 1}, {'a': 2}),
    ({'a': 1}, {'a': 1, 'b': 2}),
    ({'a': 1}, {}),
    ])
def test_1(client, database, a, b):
    print()

    db = database
    
    cl = elephant.global_.Global(db, "master")
    
    res = cl.put(None, a)

    _id = res.inserted_id

    res = cl.put(_id, b)
    
    item = cl.get_content({'_id': _id})
    
    print(item)

    print('commits')
    for c in item.d["_temp"]["commits"]:
        pprint.pprint(c)

    print('commits')
    for c in item.commits("master"):
        pprint.pprint(c)

        item.commits(c["_id"])



