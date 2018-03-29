import datetime
import suds


def partition_all(collection, chunk_size):
    to_yield = []

    for item in collection:
        to_yield.append(item)

        if len(to_yield) >= chunk_size:
            yield to_yield
            to_yield = []

    yield to_yield


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
