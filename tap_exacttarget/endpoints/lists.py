import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state


LOGGER = singer.get_logger()


class ListDataAccessObject(DataAccessObject):

    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'Category': {
                'type': 'integer',
                'description': 'ID of the folder that an item is located in.',
            },
            'CreatedDate': {
                'type': 'string',
                'description': ('Read-only date and time of the object\'s '
                                'creation.')
            },
            'ID': ID_FIELD,
            'ModifiedDate': {
                'type': ['null', 'string'],
                'description': ('Indicates the last time object information '
                                'was modified.')
            },
            'ObjectID': {
                'type': ['null', 'string'],
                'description': ('System-controlled, read-only text string '
                                'identifier for object.'),
            },
            'PartnerProperties': CUSTOM_PROPERTY_LIST,
            'ListClassification': {
                'type': ['null', 'string'],
                'description': ('Specifies the classification for a list.'),
            },
            'ListName': {
                'type': ['null', 'string'],
                'description': 'Name of a specific list.',
            },
            'Description': {
                'type': ['null', 'string'],
                'description': ('Describes and provides information regarding '
                                'the object.'),
            },
            'SendClassification': {
                'type': ['null', 'string'],
                'description': ('Indicates the send classification to use '
                                'as part of a send definition.'),
            },
            'Type': {
                'type': ['null', 'string'],
                'description': ('Indicates type of specific list. Valid '
                                'values include Public, Private, Salesforce, '
                                'GlobalUnsubscribe, and Master.')
            }
        }
    }

    TABLE = 'list'
    KEY_PROPERTIES = ['ObjectID']

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_List

        search_filter = None
        retrieve_all_since = self.state.get('bookmarks', {}).get(table)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('List',
                         selector,
                         self.auth_stub,
                         search_filter)

        for _list in stream:
            _list = self.filter_keys_and_parse(_list)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     _list.get('ModifiedDate'))

            singer.write_records(table, [_list])

        save_state(self.state)
