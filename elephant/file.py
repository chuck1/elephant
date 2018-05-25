import logging
import aardvark
import bson
import elephant.util

logger = logging.getLogger(__name__)

class _AArray:
    """
    not for use with local or global
    for use with regular mongo documents
    """
    def __init__(self, coll, d):
        self.coll = coll
        self.d = d

    def __getitem__(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v

        self.coll.update_one({'_id': self.d['_id']}, {'$set': {k, v}})

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        else:
            return default

    def to_array(self):
        return self.d

class Engine:
    """
    simple engine for querying a collection and returning _AArray objects
    """
    def __init__(self, coll):
        self.coll = coll

    def _factory(self, d):
        return _AArray(self.coll, d)

    def put_new(self, doc):
        doc = elephant.util.clean_document(doc)
        res = self.coll.insert_one(doc)
        return res

    def put(self, doc_id, doc):
        if doc_id is None:
            return self.put_new(doc)

        doc0 = self.coll.find_one({'_id': doc_id})
        doc0 = elephant.util.clean_document(doc0)

        doc1 = elephant.util.clean_document(doc)

        diffs = list(aardvark.diff(doc0, doc1))
        
        update = elephant.util.diffs_to_update(diffs, doc)

        logger.info(str(update))
        if not update:
            logger.info("updates is empty")
            return

        res = self.coll.update_one({'_id': doc_id}, update)

    def find(self, filt):
        for d in self.coll.find(filt):
            yield self._factory(d)

    def find_one(self, filt):
        d = self.coll.find_one(filt)
        if d is None: return None
        return self._factory(d)

class File:
    def __init__(self, e, d):
        self.e = e
        self.d = d

    def _commits(self, ref = None):
        def _find(commit_id):
            for c in self.d["_temp"]["commits"]:
                if c["_id"] == commit_id:
                    return c

        ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            id0 = self.d["_elephant"]["refs"][ref]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])

    def commits(self, ref = None):
        return reversed(list(self._commits()))


