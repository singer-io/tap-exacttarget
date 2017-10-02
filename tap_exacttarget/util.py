import datetime
import suds


def partition_all(collection, chunk_size):
    return (collection[index:(index + chunk_size)]
            for index
            in range(0, len(collection), chunk_size))


def sudsobj_to_dict(obj):
    if isinstance(obj, list):
        return [sudsobj_to_dict(item) for item in obj]

    if not isinstance(obj, suds.sudsobject.Object):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')

        return obj

    to_return = {}

    for key in obj.__keylist__:
        to_return[key] = sudsobj_to_dict(getattr(obj, key))

    return to_return
