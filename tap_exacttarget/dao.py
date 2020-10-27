import singer
from singer import metadata

from funcy import project

from tap_exacttarget.util import sudsobj_to_dict

LOGGER = singer.get_logger()


def _get_catalog_schema(catalog):
    return catalog.get('schema', {}).get('properties')

SENSITIVE_PROPERTIES = ('EmailAddress', 'SubscriberKey', 'Addresses', 'Attributes')

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
        metadata.write(mdata, (), 'inclusion', 'available')
        for prop in cls.SCHEMA['properties']: # pylint:disable=unsubscriptable-object
            metadata.write(mdata, ('properties', prop), 'inclusion', 'available')

        return [{
            'tap_stream_id': cls.TABLE,
            'stream': cls.TABLE,
            'key_properties': cls.KEY_PROPERTIES,
            'schema': cls.SCHEMA,
            'metadata': metadata.to_list(mdata)
        }]

    def filter_keys_and_parse(self, obj):
        to_return = sudsobj_to_dict(obj)

        return self.parse_object(to_return)

    def get_catalog_keys(self):
        return list(
            self.catalog.get('schema', {}).get('properties', {}).keys())

    def parse_object(self, obj):
        return project(obj, self.get_catalog_keys())

    def remove_sensitive_data(self, record):
        # remove personally identifiable data if option is set to true
        # check the list above to see properties that we don't record
        record_parsed = {}
        if self.config.get('remove_personally_identifiable_data', False):
            for key, value in record.items():
                record_parsed = {key: value for key, value in record.items() if key not in SENSITIVE_PROPERTIES}
            return record_parsed
        return record

    def write_schema(self):
        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=self.catalog.get('key_properties'))

    def sync(self):
        if not self.catalog['schema'].get('selected', False):
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

    def sync_data(self):  # pylint: disable=no-self-use
        raise RuntimeError('sync_data is not implemented!')
