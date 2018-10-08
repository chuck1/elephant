




class Engine:

    async def _factory(self, *args):
        d = args[0]
        return self._doc_class(self, await self.h.decode(d), d, *args[1:])







