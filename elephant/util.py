
import aardvark

def clean_document(d0):
    d1 = dict(d0)

    keys_to_delete = [k for k in d1.keys() if k.startswith("_")]

    for k in keys_to_delete:
        del d1[k]

    return d1
 
def diffs_keys_set(diffs):
    for d in diffs:
        if len(d.address.lines) > 1:
            yield d.address.lines[0].key

        if isinstance(d, aardvark.OperationRemove):
            continue
        
        yield d.address.lines[0].key

def diffs_keys_unset(diffs):
    for d in diffs:
        if isinstance(d, aardvark.OperationRemove):
            if len(d.address.lines) == 1:
                yield d.address.lines[0].key

def diffs_to_update(diffs, item):

    update_unset = dict((k, "") for k in diffs_keys_unset(diffs))

    update = {
            '$set': dict((k, item[k]) for k in diffs_keys_set(diffs)),
            }

    if update_unset:
        update['$unset'] = update_unset

    return update


