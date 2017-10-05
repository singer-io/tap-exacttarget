import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state


LOGGER = singer.get_logger()


class FolderDataAccessObject(DataAccessObject):

    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'AllowChildren': {
                'type': 'boolean',
                'description': ('Specifies whether a data folder can have '
                                'child data folders.'),
            },
            'ContentType': {
                'type': 'string',
                'description': ('Defines the type of content contained '
                                'within a folder.'),
            },
            'CreatedDate': {
                'type': 'string',
                'description': ('Read-only date and time of the object\'s '
                                'creation.')
            },
            'CustomerKey': {
                'type': ['null', 'string'],
                'description': ('User-supplied unique identifier for an '
                                'object within an object type (corresponds '
                                'to the external key assigned to an object '
                                'in the user interface).'),
            },
            'Description': {
                'type': ['null', 'string'],
                'description': ('Describes and provides information regarding '
                                'the object.'),
            },
            'ID': ID_FIELD,
            'ModifiedDate': {
                'type': ['null', 'string'],
                'description': ('Indicates the last time object information '
                                'was modified.')
            },
            'Name': {
                'type': ['null', 'string'],
                'description': 'Name of the object or property.',
            },
            'ObjectID': {
                'type': ['null', 'string'],
                'description': ('System-controlled, read-only text string '
                                'identifier for object.'),
            },
            'ParentFolder': {
                'type': ['null', 'integer'],
                'description': ('Specifies the parent folder for a data '
                                'folder.'),
            },
            'PartnerProperties': CUSTOM_PROPERTY_LIST,
            'Type': {
                'type': ['null', 'string'],
                'description': ('Indicates type of specific list. Valid '
                                'values include Public, Private, Salesforce, '
                                'GlobalUnsubscribe, and Master.')
            }
        }
    }

    TABLE = 'folder'
    KEY_PROPERTIES = ['ObjectID']

    def parse_object(self, row):
        to_return = row.copy()

        to_return['ParentFolder'] = to_return.get('ParentFolder', {}).get('ID')

        return super(FolderDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Folder

        search_filter = None
        retrieve_all_since = self.state.get('bookmarks', {}).get(table)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Folder',
                         selector,
                         self.auth_stub,
                         search_filter)

        for folder in stream:
            folder = self.filter_keys_and_parse(folder)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     folder.get('ModifiedDate'))

            singer.write_records(table, [folder])

        save_state(self.state)
