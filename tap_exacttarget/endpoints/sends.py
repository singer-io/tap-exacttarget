import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state

LOGGER = singer.get_logger()


class SendDataAccessObject(DataAccessObject):
    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'CreatedDate': {
                'type': 'string',
                'format': 'date-time',
                'description': ('Read-only date and time of the object\'s '
                                'creation.'),
            },
            'EmailID': {
                'type': 'integer',
                'description': ('Specifies the ID of an email message '
                                'associated with a send.'),
            },
            'EmailName': {
                'type': 'string',
                'description': ('Specifies the name of an email message '
                                'associated with a send.'),
            },
            'FromAddress': {
                'type': 'string',
                'description': ('Indicates From address associated with a '
                                'object. Deprecated for email send '
                                'definitions and triggered send '
                                'definitions.'),
            },
            'FromName': {
                'type': 'string',
                'description': ('Specifies the default email message From '
                                'Name. Deprecated for email send '
                                'definitions and triggered send '
                                'definitions.'),
            },
            'ID': ID_FIELD,
            'IsAlwaysOn': {
                'type': 'boolean',
                'description': ('Indicates whether the request can be '
                                'performed while the system is is '
                                'maintenance mode. A value of true '
                                'indicates the system will process the '
                                'request.'),
            },
            'IsMultipart': {
                'type': 'boolean',
                'description': ('Indicates whether the email is sent with '
                                'Multipart/MIME enabled.'),
            },
            'ModifiedDate': {
                'type': 'string',
                'format': 'date-time',
                'description': ('Indicates the last time object '
                                'information was modified.'),
            },
            'PartnerProperties': CUSTOM_PROPERTY_LIST,
            'SendDate': {
                'type': 'string',
                'format': 'date-time',
                'description': ('Indicates the date on which a send '
                                'occurred. Set this value to have a CST '
                                '(Central Standard Time) value.'),
            },
            'SentDate': {
                'type': 'string',
                'format': 'date-time',
                'description': ('Indicates date on which a send took '
                                'place.'),
            },
            'Status': {
                'type': 'string',
                'description': ('Defines status of object. Status of an '
                                'address.'),
            },
            'Subject': {
                'type': 'string',
                'description': ('Contains subject area information for '
                                'a message.'),
            }
        }
    }

    TABLE = 'send'
    KEY_PROPERTIES = ['SendID', 'SendType', 'SubscriberKey', 'SendDate']

    def parse_object(self, row):
        to_return = row.copy()

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')

        return super(SendDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Send

        search_filter = None
        retrieve_all_since = self.state.get('bookmarks', {}).get(table)

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
