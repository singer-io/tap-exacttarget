import FuelSDK

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject


def _convert_extension_datatype(datatype):
    if datatype == 'xsdboolean':
        return 'bool'
    elif datatype == 'xsddouble':
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


class DataExtensionDataAccessObject(DataAccessObject):

    def generate_catalog(self):
        # get all the data extensions
        result = request(
            'DataExtension',
            FuelSDK.ET_DataExtension,
            self.auth_stub)

        to_return = []

        for extension in result:
            # NOTE: using this as the key means that, if the name of
            # the extension changes, replication will stop working.
            # maybe this is not a good idea.
            extension_name = extension.get('Name')
            tap_stream_id = 'data_extension.{}'.format(extension_name)

            to_return.append({
                'tap_stream_id': tap_stream_id,
                'stream': extension_name,
                'key_properties': ['ObjectID'],
                'schema': _convert_data_extension_to_catalog(extension),
                'replication_key': 'ModifiedDate'
            })

        return to_return
