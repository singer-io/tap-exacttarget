import backoff
import socket
import functools
import singer
from singer import metadata

from funcy import project

from tap_exacttarget.util import sudsobj_to_dict

LOGGER = singer.get_logger()


def _get_catalog_schema(catalog):
    return catalog.get('schema', {}).get('properties')

# decorator for retrying on error
def exacttarget_error_handling(fnc):
    @backoff.on_exception(backoff.expo, (socket.timeout, ConnectionError), max_tries=5, factor=2)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

class DataAccessObject():

    def __init__(self, config, state, auth_stub, catalog):
        self.config = config.copy()
        self.state = state.copy()
        self.catalog = catalog
        self.auth_stub = auth_stub

    @classmethod
    def matches_catalog(cls, catalog):
        return catalog.get('stream') == cls.TABLE

    def generate_catalog(self):
        cls = self.__class__

        mdata = metadata.new()

        # use 'get_standard_metadata' with primary key, replication key and replication method
        mdata = metadata.get_standard_metadata(schema=self.SCHEMA,
                                               key_properties=self.KEY_PROPERTIES,
                                               valid_replication_keys=self.REPLICATION_KEYS if self.REPLICATION_KEYS else None,
                                               replication_method=self.REPLICATION_METHOD)

        mdata_map = metadata.to_map(mdata)

        # make 'automatic' inclusion for replication keys
        for replication_key in self.REPLICATION_KEYS:
            mdata_map[('properties', replication_key)]['inclusion'] = 'automatic'

        return [{
            'tap_stream_id': cls.TABLE,
            'stream': cls.TABLE,
            'key_properties': cls.KEY_PROPERTIES,
            'schema': cls.SCHEMA,
            'metadata': metadata.to_list(mdata_map)
        }]

    def filter_keys_and_parse(self, obj):
        to_return = sudsobj_to_dict(obj)

        return self.parse_object(to_return)

    def get_catalog_keys(self):
        return list(
            self.catalog.get('schema', {}).get('properties', {}).keys())

    def parse_object(self, obj):
        return project(obj, self.get_catalog_keys())

    def write_schema(self):
        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=self.catalog.get('key_properties'))

    def sync(self):
        mdata = metadata.to_map(self.catalog['metadata'])
        if not metadata.get(mdata, (), 'selected'):
            LOGGER.info('{} is not marked as selected, skipping.'
                        .format(self.catalog.get('stream')))
            return None

        LOGGER.info('Syncing stream {} with accessor {}'
                    .format(self.catalog.get('tap_stream_id'),
                            self.__class__.__name__))

        self.write_schema()

        return self.sync_data()

    # OVERRIDE THESE TO IMPLEMENT A NEW DAO:

    SCHEMA = None
    TABLE = None
    KEY_PROPERTIES = None
    REPLICATION_KEYS = []
    REPLICATION_METHOD = None

    def sync_data(self):  # pylint: disable=no-self-use
        raise RuntimeError('sync_data is not implemented!')
