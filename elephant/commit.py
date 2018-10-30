
import elephant.util

class CommitGlobal:
    @classmethod
    async def decode(cls, h, args):
        _id, time, user, parent, files = args
        return cls(
                await h.decode(_id),
                await h.decode(time),
                await h.decode(user),
                await h.decode(parent),
                files,
                )

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
        return {"CommitGlobal": await elephant.util.encode(h, user, mode, args)}

class CommitLocal:
    @classmethod
    async def decode(cls, h, args):
        _id, time, user, parent, file_, changes = args
        return cls(
                await h.decode(_id),
                await h.decode(time),
                await h.decode(user),
                await h.decode(parent),
                await h.decode(file_),
                changes,
                )

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
            ]
        args = await elephant.util.encode(h, user, mode, args)
        # we must not encode the changes because they contain old versions of data structures
        # that are not compatible with current program
        args.append(self.changes)
        return {"CommitLocal": args}

