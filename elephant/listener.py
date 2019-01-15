
import elephant.ref

class Listener:

    @classmethod
    async def decode(cls, h, args):
        args = await h.decode(args)
        return cls(*args)

    def __init__(self, ref):
        if not isinstance(ref, elephant.ref.DocRef):
            raise TypeError(f'expected DocRef not {type(ref)}')
        self.ref = ref

    async def __encode__(self, h, user, mode):
        args = [self.ref]
        return {self.__class__.__name__: await elephant.util.encode(h, user, mode, args)}
       
    def __repr__(self):
        return f'{self.__class__.__name__}({self.ref})'
 

