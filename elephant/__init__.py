import enum
import logging
import aardvark

logger = logging.getLogger(__name__)

def compare(a, b):
    for diff in aardvark.diff(a, b):
        logger.info(f'{diff}')

        if isinstance(diff, aardvark.OperationReplace):
            if diff.b == []:
                raise Exception()

class EncodeMode(enum.Enum):
    CLIENT   = 0
    DATABASE = 1

class Engine:

    def __init__(self):
        # a cache of document objects
        self._cache = {}


    async def _factory(self, encoded, is_subobject, *args, skip_cache=False, ):
       
        assert isinstance(encoded, dict)
        assert isinstance(is_subobject, bool)

        decoded = await self.h.decode(encoded)

        o = self._doc_class(self, decoded, encoded, is_subobject, *args)

        if not skip_cache:
            if "_id" in decoded:
                if decoded["_id"] in self._cache:
    
                    if len(args) > 0:
                        breakpoint()
                        raise Exception()
    
                    #logger.warning(f'cached object found {decoded["_id"]}')
    
                    doc_0 = self._cache[decoded["_id"]]
    
                    if not is_subobject:
    
                        #logger.warning(f'return cached object')
    
                        # overwrite data in existing
                        logger.info('overwriting data in existing object')
                        
                        #doc_0.d = decoded
                        #doc_0._d = encoded

                        compare(doc_0.d, decoded)

                        doc_0.d.update(decoded)
                        doc_0._d.update(encoded)
    
                        return doc_0

                else:
                    self._cache[decoded["_id"]] = o

        o.h = self.h

        return o







