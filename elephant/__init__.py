




class Engine:

    async def _factory(self, d, **kwargs):
        return self._doc_class(self, await self.h.decode(d), d, **kwargs)









