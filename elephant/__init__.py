import enum
import logging

logger = logging.getLogger(__name__)

class EncodeMode(enum.Enum):
    CLIENT   = 0
    DATABASE = 1

class Engine:

    async def _factory(self, *args):

        d = args[0]

        try:
            o = self._doc_class(self, await self.h.decode(d), d, *args[1:])
        except Exception as e:
            logger.error("failed to create {self._doc_class}")
            raise

        o.h = self.h

        return o







