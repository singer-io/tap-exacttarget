import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, CUSTOMER_KEY_FIELD, MODIFIED_DATE_FIELD, \
    DESCRIPTION_FIELD, OBJECT_ID_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class FolderDataAccessObject(DataAccessObject):

    SCHEMA = with_properties({
        'AllowChildren': {
            'type': ['null', 'boolean'],
            'description': ('Specifies whether a data folder can have '
                            'child data folders.'),
        },
        'ContentType': {
            'type': ['null', 'string'],
            'description': ('Defines the type of content contained '
                            'within a folder.'),
        },
        'CreatedDate': CREATED_DATE_FIELD,
        'CustomerKey': CUSTOMER_KEY_FIELD,
        'Description': DESCRIPTION_FIELD,
        'ID': ID_FIELD,
        'ModifiedDate': MODIFIED_DATE_FIELD,
        'Name': {
            'type': ['null', 'string'],
            'description': 'Name of the object or property.',
        },
        'ObjectID': OBJECT_ID_FIELD,
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
    })

    TABLE = 'folder'
    KEY_PROPERTIES = ['ID']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['ParentFolder'] = to_return.get('ParentFolder', {}).get('ID')

        return super(FolderDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Folder

        search_filter = None

        retrieve_all_since = get_last_record_value_for_table(self.state, table)

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
