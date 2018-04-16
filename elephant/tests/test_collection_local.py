import time

import pymongo
import pytest

import elephant.collection_local

def breakpoint(): import pdb; pdb.set_trace();

@pytest.mark.parametrize('a,b', [
    ({'a': 1}, {'a': 2}),
    ({'a': 1}, {'a': 1, 'b': 2}),
    ({'a': 1}, {}),
    ])
def test_1(a, b):

    client = pymongo.MongoClient()

    db_name = 'test_' + str(int(time.time()))

    db = client[db_name]
    
    cl = elephant.collection_local.CollectionLocal(db.test)
    
    res = cl.put("master", None, a)

    _id = res.inserted_id

    res = cl.put("master", _id, b)
    
    print(res)

    item = db.test.find_one({'_id': _id})


