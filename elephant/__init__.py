import enum
import logging
import aardvark

logger = logging.getLogger(__name__)


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
                        logger.info(f'overwriting data in existing object {encoded["_id"]}')
                        
                        #doc_0.d = decoded
                        #doc_0._d = encoded

                        diffs_0 = list(aardvark.diff(doc_0.d, decoded))
                        diffs_1 = list(aardvark.diff(doc_0._d, encoded))

                        for diff in diffs_0:
                            logger.info(f'  {type(diff)} {[a.key for a in diff.address.lines]}')
                    
                            if isinstance(diff, aardvark.OperationReplace):
                                if diff.b == []:
                                    raise Exception(f'{diff}')
                    

                        # TODO might need way to apply in-place
                        doc_0.d  = aardvark.apply(doc_0.d,  diffs_0) #.update(decoded)
                        doc_0._d = aardvark.apply(doc_0._d, diffs_1) #.update(encoded)

                        for diff in diffs_0:
                            if isinstance(diff, aardvark.OperationRemove):
                                if len(diff.address.lines) == 1:
                                    k = diff.address.lines[0].key
                                    if k in doc_0.d:
                                        raise Exception(f'{k!r} should not be in d. diff = {diff}')
    
                        return doc_0

                else:
                    self._cache[decoded["_id"]] = o

        o.h = self.h

        return o







