import FuelSDK
import copy
import singer

from funcy import set_in, update_in, merge

from tap_exacttarget.client import request, request_from_cursor
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import sudsobj_to_dict
from tap_exacttarget.fuel_overrides import TapExacttarget__ET_DataExtension_Row, \
    TapExacttarget__ET_DataExtension_Column

LOGGER = singer.get_logger()  # noqa

# add 'new_item' in 'collection' (dict) at the 'path'
# update_in({"a": {}}, ["a", "cnt"], 1) -> {"a": {"cnt": 1}}
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


def _get_tap_stream_id(extension):
    extension_name = extension.CustomerKey
    return 'data_extension.{}'.format(extension_name)


def _get_extension_name_from_tap_stream_id(tap_stream_id):
    return tap_stream_id.split('.')[1]


class DataExtensionDataAccessObject(DataAccessObject):

    @classmethod
    def matches_catalog(cls, catalog):
        return 'data_extension.' in catalog.get('stream')

    # get list of all the data extensions created by the user
    @exacttarget_error_handling
    def _get_extensions(self):
        result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub,
            props=['CustomerKey', 'Name'],
            batch_size=self.batch_size
        )

        to_return = {}

        for extension in result:
            extension_name = str(extension.Name)
            customer_key = str(extension.CustomerKey)

            # create a basic catalog dict for all data extensions
            to_return[customer_key] = {
                'tap_stream_id': 'data_extension.{}'.format(customer_key),
                'stream': 'data_extension.{}'.format(extension_name),
                'key_properties': ['_CustomObjectKey'],
                'schema': {
                    'type': 'object',
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
                'metadata': [
                    {
                        'breadcrumb': (),
                        'metadata': {
                            'inclusion':'available',
                            'forced-replication-method': 'FULL_TABLE',
                            'table-key-properties': ['_CustomObjectKey'],
                            'valid-replication-keys': []
                        }
                    },
                    {
                        'breadcrumb': ('properties', '_CustomObjectKey'),
                        'metadata': {'inclusion':'automatic'}
                    },
                    {
                        'breadcrumb': ('properties', 'CategoryID'),
                        'metadata': {'inclusion':'available'}
                    }
                ]
            }

        return to_return

    # get all the fields in all the data extensions
    @exacttarget_error_handling
    def _get_fields(self, extensions): # pylint: disable=too-many-branches
        to_return = extensions.copy()

        result = request(
            'DataExtensionField',
            # use custom class to apply 'batch_size'
            TapExacttarget__ET_DataExtension_Column,
            self.auth_stub,
            batch_size=self.batch_size)

        # iterate through all the fields and determine if it is primary key
        # or replication key and update the catalog file accordingly:
        #   is_primary_key:
        #       update catalog file by appending that field in 'table-key-properties'
        #   is_replication_key:
        #       update value of 'forced-replication-method' as INCREMENTAL
        #       update catalog file by appending that field in 'valid-replication-keys'
        #   add 'AUTOMATIC' replication method for both primary and replication keys
        for field in result:
            is_replication_key = False
            is_primary_key = False
            # the date extension in which the fields is present
            extension_id = field.DataExtension.CustomerKey
            field = sudsobj_to_dict(field)
            field_name = field['Name']

            if field.get('IsPrimaryKey'):
                # add primary key in 'key_properties' list
                is_primary_key = True
                to_return = _merge_in(
                    to_return,
                    [extension_id, 'key_properties'],
                    field_name)

            if field_name in ['ModifiedDate', 'JoinDate']:
                is_replication_key = True

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

            # add primary key in 'table-key-properties'
            if is_primary_key:
                for mdata in to_return[extension_id]['metadata']:
                    if not mdata.get('breadcrumb'):
                        mdata.get('metadata').get('table-key-properties').append(field_name)

            # add replication key in 'valid-replication-keys'
            # and change 'forced-replication-method' to INCREMENTAL
            if is_replication_key:
                for mdata in to_return[extension_id]['metadata']:
                    if not mdata.get('breadcrumb'):
                        mdata.get('metadata')['forced-replication-method'] = "INCREMENTAL"
                        mdata.get('metadata').get('valid-replication-keys').append(field_name)

            # These fields are defaulted into the schema, do not add to metadata again.
            if field_name not in {'_CustomObjectKey', 'CategoryID'}:
                # if primary of replication key, then mark it as automatic
                if is_primary_key or is_replication_key:
                    to_return[extension_id]['metadata'].append({
                        'breadcrumb': ('properties', field_name),
                        'metadata': {'inclusion': 'automatic'}
                    })
                else:
                    to_return[extension_id]['metadata'].append({
                        'breadcrumb': ('properties', field_name),
                        'metadata': {'inclusion': 'available'}
                    })

        # the structure of 'to_return' is like:
        # {
        #     'de1': {
        #         'tap_stream_id': 'data_extension.de1',
        #         'stream': 'data_extension.de1',
        #         'key_properties': ['_CustomObjectKey'],
        #         'schema': {
        #             'type': 'object',
        #             'properties': {...}
        #         },
        #         'metadata': [...]
        #     },
        #    'de2': {
        #         'tap_stream_id': 'data_extension.de2',
        #         'stream': 'data_extension.de2',
        #         'key_properties': ['_CustomObjectKey'],
        #         'schema': {
        #             'type': 'object',
        #             'properties': {...}
        #         },
        #         'metadata': [...]
        #     }
        # }

        # loop through all the data extension catalog in 'to_return'
        # and remove empty 'valid-replication-keys' present in metadata
        for catalog in to_return.values():
            for mdata in catalog.get('metadata'):
                if not mdata.get('breadcrumb'):
                    if not mdata.get('metadata').get('valid-replication-keys'):
                        del mdata.get('metadata')['valid-replication-keys']
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

    @exacttarget_error_handling
    def _replicate(self, customer_key, keys,
                   parent_category_id, table,
                   partial=False, start=None,
                   end=None, unit=None, replication_key=None):
        if partial:
            LOGGER.info("Fetching {} from {} to {}"
                        .format(table, start, end))

        # use custom class to apply 'batch_size'
        cursor = TapExacttarget__ET_DataExtension_Row()
        cursor.auth_stub = self.auth_stub
        cursor.CustomerKey = customer_key
        cursor.props = keys
        cursor.options = {"BatchSize": self.batch_size}

        if partial:
            cursor.search_filter = get_date_page(replication_key,
                                                 start,
                                                 unit)

        result = request_from_cursor('DataExtensionObject', cursor,
                                     batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for row in result:
            row = self.filter_keys_and_parse(row)
            row['CategoryID'] = parent_category_id

            self.state = incorporate(self.state,
                                     table,
                                     replication_key,
                                     row.get(replication_key))

            self.write_records_with_transform(row, catalog_copy, table)

        if partial:
            self.state = incorporate(self.state,
                                     table,
                                     replication_key,
                                     start)

            save_state(self.state)

    @exacttarget_error_handling
    def sync_data(self):
        tap_stream_id = self.catalog.get('tap_stream_id')
        table = self.catalog.get('stream')
        (_, customer_key) = tap_stream_id.split('.', 1)

        keys = self.get_catalog_keys()

        keys.remove('CategoryID')

        replication_key = None

        # pass config to return start date if not bookmark is found
        start = get_last_record_value_for_table(self.state, table, self.config)

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
            props=['CustomerKey', 'CategoryID'],
            batch_size=self.batch_size)

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
