from tap_exacttarget.util import sudsobj_to_dict


class DataAccessObject(object):

    def __init__(self, config, state, auth_stub, catalog):
        self.config = config
        self.state = state
        self.auth_stub = auth_stub
        self.catalog = catalog

    def generate_catalog(self):
        cls = self.__class__

        return [{
            'tap_stream_id': cls.TABLE,
            'stream': cls.TABLE,
            'key_properties': cls.KEY_PROPERTIES,
            'schema': cls.SCHEMA,
            'replication_key': 'ModifiedDate'
        }]

    def _is_selected(self, catalog_entry):
        return ((catalog_entry.get('inclusion') == 'automatic') or
                (catalog_entry.get('inclusion') == 'available' and
                 catalog_entry.get('selected') is True))

    def select_keys_with_catalog(self, obj):
        # by default, use all the keys
        selected_keys = self.__class__.SCHEMA.get('properties').keys()

        if self.catalog is not None:
            selected_keys = \
                filter(self._is_selected, self.catalog).keys()

        return {key: obj[key]
                for key in selected_keys}

    def filter_keys_and_parse(self, obj):
        to_return = sudsobj_to_dict(obj)
        to_return = self.select_keys_with_catalog(to_return)

        return self.parse_object(to_return)

    # OVERRIDE THESE TO IMPLEMENT A NEW DAO:

    SCHEMA = None
    TABLE = None
    KEY_PROPERTIES = None

    @classmethod
    def parse_object(self, obj):
        raise RuntimeError('parse_object is not implemented!')

    @classmethod
    def sync(self):
        raise RuntimeError('sync is not implemented!')
