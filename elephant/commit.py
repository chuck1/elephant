
import elephant.util

class Commit:
    @classmethod
    async def decode(cls, h, args):
        return Commit(**args)

    def __init__(self, _id, time, user, parent, files):
        self._id = _id
        self.time = time
        self.user = user
        self.parent = parent
        self.files = files

    async def __encode__(self, h, user, mode):
        args = [
            self._id,
            self.time,
            self.user,
            self.parent,
            self.files,
            ]
        return {"Commit": await elephant.util.encode(h, user, mode, args)}

class CommitLocal:
    @classmethod
    async def decode(cls, h, args):
        return Commit(**args)

    def __init__(self, _id, time, user, parent, file_, changes):
        self._id = _id
        self.time = time
        self.user = user
        self.parent = parent
        self.file_ = file_
        self.changes = changes

    async def __encode__(self, h, user, mode):
        args = [
            self._id,
            self.time,
            self.user,
            self.parent,
            self.file_,
            self.changes,
            ]
        return {"Commit": await elephant.util.encode(h, user, mode, args)}

