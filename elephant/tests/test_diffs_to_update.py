import pytest

import aardvark
import elephant.util


def _test(database, a, b):
    a = dict(a)
    b = dict(b)
    print(a)
    print(b)

    diffs = list(aardvark.diff(a, b))

    print(diffs)

    coll = database.test

    res = coll.insert_one(a)
    
    u = elephant.util.diffs_to_update(diffs, b)

    print(u)
    
    coll.update_one({'_id': res.inserted_id}, u)

    i = coll.find_one({'_id': res.inserted_id})
    del i['_id']
    assert i == b


@pytest.mark.parametrize("a, b", [
    ({'a': 1}, {'b': 1}),
    ({'c': {'a': 1}}, {'c': {'b': 1}}),
    ({'c': {'a': 1}}, {'c': {'a': 2}}),
    (
        {'c': {'a': 1}}, 
        {'c': {'a': 2, 'b': 1}}),
    ({'c': 1}, {'c': 2}),
    ])
def test_1(database, a, b):
    _test(database, a, b)
    _test(database, b, a)


