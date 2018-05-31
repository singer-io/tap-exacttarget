import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, MODIFIED_DATE_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table

LOGGER = singer.get_logger()


class SendDataAccessObject(DataAccessObject):
    SCHEMA = with_properties({
        'CreatedDate': CREATED_DATE_FIELD,
        'EmailID': {
            'type': ['null', 'integer'],
            'description': ('Specifies the ID of an email message '
                            'associated with a send.'),
        },
        'EmailName': {
            'type': ['null', 'string'],
            'description': ('Specifies the name of an email message '
                            'associated with a send.'),
        },
        'FromAddress': {
            'type': ['null', 'string'],
            'description': ('Indicates From address associated with a '
                            'object. Deprecated for email send '
                            'definitions and triggered send '
                            'definitions.'),
        },
        'FromName': {
            'type': ['null', 'string'],
            'description': ('Specifies the default email message From '
                            'Name. Deprecated for email send '
                            'definitions and triggered send '
                            'definitions.'),
        },
        'ID': ID_FIELD,
        'IsAlwaysOn': {
            'type': ['null', 'boolean'],
            'description': ('Indicates whether the request can be '
                            'performed while the system is is '
                            'maintenance mode. A value of true '
                            'indicates the system will process the '
                            'request.'),
        },
        'IsMultipart': {
            'type': ['null', 'boolean'],
            'description': ('Indicates whether the email is sent with '
                            'Multipart/MIME enabled.'),
        },
        'ModifiedDate': MODIFIED_DATE_FIELD,
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'SendDate': {
            'type': ['null', 'string'],
            'format': 'date-time',
            'description': ('Indicates the date on which a send '
                            'occurred. Set this value to have a CST '
                            '(Central Standard Time) value.'),
        },
        'SentDate': {
            'type': ['null', 'string'],
            'format': 'date-time',
            'description': ('Indicates date on which a send took '
                            'place.'),
        },
        'Status': {
            'type': ['null', 'string'],
            'description': ('Defines status of object. Status of an '
                            'address.'),
        },
        'Subject': {
            'type': ['null', 'string'],
            'description': ('Contains subject area information for '
                            'a message.'),
        }
    })

    TABLE = 'send'
    KEY_PROPERTIES = ['ID']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')

        return super(SendDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Send

        search_filter = None
        retrieve_all_since = get_last_record_value_for_table(self.state, table)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Send',
                         selector,
                         self.auth_stub,
                         search_filter)

        for send in stream:
            send = self.filter_keys_and_parse(send)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     send.get('ModifiedDate'))

            singer.write_records(table, [send])

        save_state(self.state)
