import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, OBJECT_ID_FIELD, DESCRIPTION_FIELD, \
    MODIFIED_DATE_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class ListDataAccessObject(DataAccessObject):

    SCHEMA = with_properties({
        'Category': {
            'type': ['null', 'integer'],
            'description': 'ID of the folder that an item is located in.',
        },
        'CreatedDate': CREATED_DATE_FIELD,
        'ID': ID_FIELD,
        'ModifiedDate': MODIFIED_DATE_FIELD,
        'ObjectID': OBJECT_ID_FIELD,
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'ListClassification': {
            'type': ['null', 'string'],
            'description': ('Specifies the classification for a list.'),
        },
        'ListName': {
            'type': ['null', 'string'],
            'description': 'Name of a specific list.',
        },
        'Description': DESCRIPTION_FIELD,
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
    })

    TABLE = 'list'
    KEY_PROPERTIES = ['ID']

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_List

        search_filter = None
        retrieve_all_since = get_last_record_value_for_table(self.state, table)

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
