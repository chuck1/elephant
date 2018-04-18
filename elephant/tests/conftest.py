import time

import pytest
import pymongo

@pytest.fixture(scope="module")
def client():
    return pymongo.MongoClient()

@pytest.fixture(scope="module")
def database(client):
    db_name = 'test_' + str(int(time.time()))
    yield client[db_name]
    print('delete database')
    client.drop_database(db_name)


