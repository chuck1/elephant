import time
import logging
import async_patterns
import elephant.doc
import otter.subobjects

logger = logging.getLogger(__name__)

class Temp: pass

class Doc(elephant.doc.Doc):
    def __init__(self, e, d, _d, *args, **kwargs):
        super().__init__(e, d, _d, *args, **kwargs)
        assert isinstance(d, dict)
        self.temp = Temp()

    @classmethod
    async def get_test_document(cls, b0={}):
        b = {"test_field": str(time.time())}
        b.update(b0)
        return b

    async def update_post(self, user, diffs, ):

        # TODO implement per key
        #for diff in diffs:
        #    k = diff.address.lines[0].key

        # search the database for docs that list this as a trigger
            
        # TODO just do tasks for now but should be ALL engines

        query = {"_temp.listeners": {"$elemMatch": {"Listener.0": self.freeze()}}}
        logger.info(f'docs listening to {self!r}:')
        async for doc in self.e.find(user, query, check=False):
            logger.info(f'  {doc}')
            await doc.put(user)

    async def listening_to_self(self, user):
        query = {"_temp.listeners": {"$elemMatch": {"Listener.0": self.freeze()}}}
        #query = {"_temp.listeners": {"$elemMatch": {"Listener": [self.freeze()]}}}
        #query = {"_temp.listeners": {"$elemMatch": {"Listener": {"$exists": True}}}}
        #query = {"_temp.listeners": {"$exists": True}}
        async for doc in self.e.find(user, query):
            yield doc

    def freeze(self):
        return elephant.ref.DocRef(self.d["_id"])

    def get(self, k, default):
        if k in self.d:
            return self.d[k]
        return default

    def get(self, k):
        return self.d[k]

    def __setitem__(self, k, v):
        self.d[k] = v
        updates = {'$set': {k: v}}
        self.e.coll.files.update_one({'_id': self.d['_id']}, updates)

    def valid(self):
        pass

    async def check_0(self):
        for tag in self.d.get("tags", []):
            if not isinstance(tag, otter.subobjects.tag.Tag):
                raise TypeError(f"expected subobject.tag.Tag not {type(tag)}")

    async def check(self):
        creator = await self.creator()

        self.d["_temp"]

        self.d["_temp"]["commits"]

        # used in the read_permissions pipe
        self.d["_temp"]["commits"][0].user

    async def update_temp(self, user):
        """
        recalculate _temp fields
        _temp fields store data in the database for querying
        this should be done before a document is written, not when it is read.

        fields:
          first_commit
        """
	
        self.d["_temp"] = {}
 
        commits = await self.temp_commits()

        self.d["_temp"]["commits"] = commits

    async def temp_commits(self):

        pipe = [
                {'$addFields': {'files1': '$files'}},
                {'$unwind': '$files1'},
                {'$match': {'$expr': {'$eq': ["$files1.file_id", self.d["_id"]]}}},
                {'$project': {'files1': 0}},
                ]
 
        return list(elephant.commit.CommitGlobal(
                _["_id"],
                _["time"],
                _["user"],
                _["parent"],
                _["files"],
                ) for _ in self.e.coll.commits.aggregate(pipe))

    async def delete(self, user):
        self.d["hide"] = True
        await self.put(user)

    def update(self, updates):
        self.e.coll.files.update_one({"_id": self.d['_id']}, updates)

    async def put(self, user=None):

        # temp needs to reflect current state
        #await self.update_temp(user)

        ret = await self.e.put(user, self.d["_id"], self.d)
      
        #assert ret is self
 
        return ret

    def _commits(self, ref):
        def _find(commit_id):
            for c in self.d["_temp"]["commits"]:

                assert isinstance(c, elephant.commit.Commit)

                if c["_id"] == commit_id:
                    return c

        #ref = ref or self.d["_elephant"]["ref"]

        if isinstance(ref, bson.objectid.ObjectId):
            id0 = ref
        else:
            ref = self.e.coll.refs.find_one({'name': ref})
            id0 = ref["commit_id"]

        c0 = _find(id0)

        while c0:
            yield c0
            
            c0 = _find(c0["parent"])

    async def has_read_permission(self, user):
        if user is None: 
            logger.info("read denied: user is None")
            return False
        
        if hasattr(self.e, 'h'):
            if user == self.e.h.root_user:
                logger.debug("read allowed: user is root_user")
                return True
        
        creator = await self.creator()
        
        logger.debug(f"user    = {user} {user.freeze()}")
        logger.debug(f"creator = {creator} {creator.freeze()}")

        b = user.freeze() == creator.freeze()

        if b:
            logger.debug("read allowed: user is creator")
        else:
            logger.info("read denied: user is not creator")

        return b

    async def has_write_permission(self, user):
        if user is None:
            logger.info("write permission denied: user is None")
            return False

        if hasattr(self.e, 'h'):
            if user == self.e.h.root_user:
                logger.info("write permission allowed: user is root")
                return True

        creator = await self.creator()
        if user.freeze() == creator.freeze():
            logger.info("write permission allowed: user is creator")
            return True

        logger.info("write permission denied: user is not root or creator")
        logger.info(f"user = {user}")
        logger.info(f"user = {user}")
        return False


    def commits1(self):
        return self.e.coll.commits.find({"files.file_id": self.d["_id"]}).sort([('time', 1)])

    async def temp_messages(self):
        return
        yield

    def __repr__(self):
        return f'{self.__class__.__name__}({self.d["_id"]})'

