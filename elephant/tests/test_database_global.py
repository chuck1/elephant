import time

import pymongo
import pytest

import elephant.database_global

def breakpoint(): import pdb; pdb.set_trace();

@pytest.mark.parametrize('a,b', [
    ({'a': 1}, {'a': 2}),
    ({'a': 1}, {'a': 1, 'b': 2}),
    ({'a': 1}, {}),
    ])
def test_1(client, database, a, b):
    print()

    db = database
    
    cl = elephant.database_global.DatabaseGlobal(db, "master")
    
    res = cl.put(None, a)

    _id = res.inserted_id

    res = cl.put(_id, b)
    
    item = db.files.find_one({'_id': _id})

    item1 = dict(item)
    del item1['_id']
    del item1['_elephant']
    
    assert item1 == b

    print(item)


