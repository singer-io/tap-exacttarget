import FuelSDK
import singer

from funcy import set_in, update_in, merge

from tap_exacttarget.client import request, request_from_cursor
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import sudsobj_to_dict

LOGGER = singer.get_logger()  # noqa


def _merge_in(collection, path, new_item):
    return update_in(collection, path, lambda x: merge(x, [new_item]))


def _convert_extension_datatype(datatype):
    if datatype == 'Boolean':
        return 'boolean'
    elif datatype == 'Decimal':
        return 'number'
    elif datatype == 'Number':
        return 'integer'

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
                    'properties': {
                        '_CustomObjectKey': {
                            'type': ['null', 'string'],
                            'description': ('Hidden auto-incrementing primary '
                                            'key for data extension rows.'),
                        },
                        'CategoryID': {
                            'type': ['null', 'integer'],
                            'description': ('Specifies the identifier of the '
                                            'folder. (Taken from the parent '
                                            'data extension.)')
                        }
                    }
                },
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

    def filter_keys_and_parse(self, obj):
        to_return = self.parse_object(sudsobj_to_dict(obj))

        obj_schema = self.catalog['schema']['properties']

        for k, v in to_return.items():
            field_schema = obj_schema.get(k, {})

            # sometimes data extension fields have type integer or
            # number, but come back as strings from the API. we need
            # to explicitly cast them.
            if v is None:
                pass

            elif 'integer' in field_schema.get('type'):
                to_return[k] = int(v)

            elif 'number' in field_schema.get('type'):
                to_return[k] = float(v)

            elif ('boolean' in field_schema.get('type') and
                  isinstance(to_return[k], str)):
                # Extension bools can come through as a number of values, see:
                # https://help.salesforce.com/articleView?id=mc_es_data_extension_data_types.htm&type=5
                # In practice, looks like they come through as either "True"
                # or "False", but for completeness I have included the other
                # possible values.
                if str(to_return[k]).lower() in [1, "1", "y", "yes", "true"]:
                    to_return[k] = True
                elif str(to_return[k]).lower() in [0, "0", "n", "no", "false"]:
                    to_return[k] = False
                else:
                    LOGGER.warn('Could not infer boolean value from {}'
                                .format(to_return[k]))
                    to_return[k] = None

        return to_return

    def _replicate(self, customer_key, keys,
                   parent_category_id, table,
                   partial=False, start=None,
                   end=None, unit=None, replication_key=None):
        if partial:
            LOGGER.info("Fetching {} from {} to {}"
                        .format(table, start, end))

        cursor = FuelSDK.ET_DataExtension_Row()
        cursor.auth_stub = self.auth_stub
        cursor.CustomerKey = customer_key
        cursor.props = keys

        if partial:
            cursor.search_filter = get_date_page(replication_key,
                                                 start,
                                                 unit)

        result = request_from_cursor('DataExtensionObject', cursor)

        for row in result:
            row = self.filter_keys_and_parse(row)
            row['CategoryID'] = parent_category_id

            self.state = incorporate(self.state,
                                     table,
                                     replication_key,
                                     row.get(replication_key))

            singer.write_records(table, [row])

        if partial:
            self.state = incorporate(self.state,
                                     table,
                                     replication_key,
                                     start)

            save_state(self.state)

    def sync_data(self):
        tap_stream_id = self.catalog.get('tap_stream_id')
        table = self.catalog.get('stream')
        (_, customer_key) = tap_stream_id.split('.', 1)

        keys = self.get_catalog_keys()
        keys.remove('CategoryID')

        replication_key = None

        start = get_last_record_value_for_table(self.state, table)

        if start is None:
            start = self.config.get('start_date')

        for key in ['ModifiedDate', 'JoinDate']:
            if key in keys:
                replication_key = key

        pagination_unit = self.config.get(
            'pagination__data_extension_interval_unit', 'days')
        pagination_quantity = self.config.get(
            'pagination__data_extension_interval_quantity', 7)

        unit = {pagination_unit: int(pagination_quantity)}

        end = increment_date(start, unit)

        parent_result = None
        parent_extension = None
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
        parent_category_id = parent_extension.CategoryID

        while before_now(start) or replication_key is None:
            self._replicate(
                customer_key,
                keys,
                parent_category_id,
                table,
                partial=(replication_key is not None),
                start=start,
                end=end,
                unit=unit,
                replication_key=replication_key)

            if replication_key is None:
                return

            self.state = incorporate(self.state,
                                     table,
                                     replication_key,
                                     start)

            save_state(self.state)

            start = end
            end = increment_date(start, unit)
