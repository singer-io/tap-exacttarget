import FuelSDK
import singer

from funcy import set_in, update_in, merge

from tap_exacttarget.client import request, request_from_cursor
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.util import sudsobj_to_dict

LOGGER = singer.get_logger()  # noqa


def _merge_in(collection, path, new_item):
    return update_in(collection, path, lambda x: merge(x, [new_item]))


def _convert_extension_datatype(datatype):
    if datatype in ['Boolean']:
        return 'bool'
    elif datatype in ['Decimal', 'Number']:
        return 'number'

    return 'string'


def _convert_data_extension_to_catalog(extension):
    return {
        field.get('Name'): {
            'type': _convert_extension_datatype(field.get('ValueType')),
            'description': field.get('Description'),
            'inclusion': 'available',
        }
        for field in extension.get('Fields')
    }


def _get_tap_stream_id(extension):
    extension_name = extension.CustomerKey
    return 'data_extension.{}'.format(extension_name)


def _get_extension_name_from_tap_stream_id(tap_stream_id):
    return tap_stream_id.split('.')[1]


class DataExtensionDataAccessObject(DataAccessObject):

    @classmethod
    def matches_catalog(cls, catalog):
        return 'data_extension.' in catalog.get('stream')

    def _get_extensions(self):
        result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub,
            props=['CustomerKey', 'Name'])

        to_return = {}

        for extension in result:
            extension_name = str(extension.Name)
            customer_key = str(extension.CustomerKey)

            to_return[customer_key] = {
                'tap_stream_id': 'data_extension.{}'.format(customer_key),
                'stream': 'data_extension.{}'.format(extension_name),
                'key_properties': ['_CustomObjectKey'],
                'schema': {
                    'type': 'object',
                    'inclusion': 'available',
                    'selected': False,
                    'properties': {
                        '_CustomObjectKey': {
                            'type': 'string',
                            'description': ('Hidden auto-incrementing primary '
                                            'key for data extension rows.'),
                        },
                        'CategoryID': {
                            'type': 'integer',
                            'description': ('Specifies the identifier of the '
                                            'folder. (Taken from the parent '
                                            'data extension.)')
                        }
                    }
                },
                'replication_key': 'ModifiedDate',
            }

        return to_return

    def _get_fields(self, extensions):
        to_return = extensions.copy()

        result = request(
            'DataExtensionField',
            FuelSDK.ET_DataExtension_Column,
            self.auth_stub)

        for field in result:
            extension_id = field.DataExtension.CustomerKey
            field = sudsobj_to_dict(field)
            field_name = field['Name']

            if field.get('IsPrimaryKey'):
                to_return = _merge_in(
                    to_return,
                    [extension_id, 'key_properties'],
                    field_name)

            field_schema = {
                'type': [
                    'null',
                    _convert_extension_datatype(str(field.get('FieldType')))
                ],
                'description': str(field.get('Description')),
            }

            to_return = set_in(
                to_return,
                [extension_id, 'schema', 'properties', field_name],
                field_schema)

        return to_return

    def generate_catalog(self):
        # get all the data extensions by requesting all the fields
        extensions_catalog = self._get_extensions()

        extensions_catalog_with_fields = self._get_fields(extensions_catalog)

        return extensions_catalog_with_fields.values()

    def parse_object(self, obj):
        properties = obj.get('Properties', {}).get('Property', {})
        to_return = {}

        for prop in properties:
            to_return[prop['Name']] = prop['Value']

        return to_return

    def sync_data(self):
        tap_stream_id = self.catalog.get('tap_stream_id')
        table = self.catalog.get('stream')
        (_, customer_key) = tap_stream_id.split('.', 1)

        cursor = FuelSDK.ET_DataExtension_Row()
        cursor.auth_stub = self.auth_stub
        cursor.CustomerKey = customer_key

        keys = self.get_catalog_keys()
        keys.remove('CategoryID')

        cursor.props = keys

        result = request_from_cursor('DataExtensionObject', cursor)

        parent_result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub,
            search_filter={
                'Property': 'CustomerKey',
                'SimpleOperator': 'equals',
                'Value': customer_key,
            },
            props=['CustomerKey', 'CategoryID'])

        parent_extension = next(parent_result)

        for row in result:
            row = self.filter_keys_and_parse(row)
            row['CategoryID'] = parent_extension.CategoryID

            singer.write_records(table, [row])
