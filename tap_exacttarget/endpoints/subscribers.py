import FuelSDK
import singer

from funcy import get_in

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import CUSTOM_PROPERTY_LIST, ID_FIELD
from tap_exacttarget.state import incorporate, save_state
from tap_exacttarget.util import partition_all, sudsobj_to_dict


LOGGER = singer.get_logger()

SCHEMA = {
    'type': 'object',
    'inclusion': 'available',
    'selected': False,
    'properties': {
        'Addresses': {
            'type': 'array',
            'description': ('Indicates addresses belonging to a subscriber, '
                            'used to create, retrieve, update or delete an '
                            'email or SMS Address for a given subscriber.'),
            'items': {
                'type': 'object',
                'properties': {
                    'Address': {'type': 'string'},
                    'AddressType': {'type': 'string'},
                    'AddressStatus': {'type': 'string'}
                }
            }
        },
        'Attributes': CUSTOM_PROPERTY_LIST,
        'CorrelationID': {
            'type': ['null', 'string'],
            'description': ('Identifies correlation of objects across '
                            'several requests.'),
        },
        'CreatedDate': {
            'type': 'string',
            'description': 'Read-only date and time of the object\'s creation.'
        },
        'CustomerKey': {
            'type': ['null', 'string'],
            'description': ('User-supplied unique identifier for an object '
                            'within an object type (corresponds to the '
                            'external key assigned to an object in the user '
                            'interface).'),
        },
        'EmailAddress': {
            'type': ['null', 'string'],
            'description': ('Contains the email address for a subscriber. '
                            'Indicates the data extension field contains '
                            'email address data.'),
        },
        'EmailTypePreference': {
            'type': ['null', 'string'],
            'description': 'The format in which email should be sent'
        },
        'ID': ID_FIELD,
        'ListIDs': {
            'type': 'array',
            'description': 'Defines list IDs a subscriber resides on.',
            'items': {
                'type': 'string'
            }
        },
        'Locale': {
            'type': ['null', 'string'],
            'description': ('Contains the locale information for an Account. '
                            'If no location is set, Locale defaults to en-US '
                            '(English in United States).'),
        },
        'ModifiedDate': {
            'type': ['null', 'string'],
            'description': ('Indicates the last time object information was '
                            'modified.')
        },
        'ObjectID': {
            'type': ['null', 'string'],
            'description': ('System-controlled, read-only text string '
                            'identifier for object.'),
        },
        'PartnerKey': {
            'type': ['null', 'string'],
            'description': ('Unique identifier provided by partner for an '
                            'object, accessible only via API.'),
        },
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'PartnerType': {
            'type': ['null', 'string'],
            'description': 'Defines partner associated with a subscriber.'
        },
        'PrimaryEmailAddress': {
            'type': ['null', 'string'],
            'description': 'Indicates primary email address for a subscriber.'
        },
        'PrimarySMSAddress': {
            'type': ['null', 'string'],
            'description': ('Indicates primary SMS address for a subscriber. '
                            'Used to create and update SMS Address for a '
                            'given subscriber.'),
        },
        'PrimarySMSPublicationStatus': {
            'type': ['null', 'string'],
            'description': 'Indicates the subscriber\'s modality status.',
        },
        'Status': {
            'type': 'string',
            'description': 'Defines status of object. Status of an address.',
        },
        'SubscriberKey': {
            'type': 'string',
            'description': 'Identification of a specific subscriber.',
        },
        'SubscriberTypeDefinition': {
            'type': ['null', 'string'],
            'description': ('Specifies if a subscriber resides in an '
                            'integration, such as Salesforce or Microsoft '
                            'Dynamics CRM'),
        },
        'UnsubscribedDate': {
            'type': ['null', 'string'],
            'description': ('Represents date subscriber unsubscribed '
                            'from a list.'),
        }
    }
}


class SubscriberDataAccessObject(DataAccessObject):

    SCHEMA = SCHEMA
    TABLE = 'subscriber'
    KEY_PROPERTIES = ['ObjectID']

    def parse_object(self, obj):
        to_return = obj.copy()

        if 'ListIDs' in to_return:
            to_return['ListIDs'] = [_list.get('ObjectID')
                                    for _list in to_return.get('Lists', [])]

        if 'Lists' in to_return:
            del to_return['Lists']

        if to_return.get('Addresses') is None:
            to_return['Addresses'] = []

        if to_return.get('PartnerProperties') is None:
            to_return['PartnerProperties'] = []

        return super(SubscriberDataAccessObject, self).parse_object(obj)

    def sync_data(self):
        pass

    def _pull_subscribers_batch(self, subscriber_keys):
        table = self.__class__.TABLE
        stream = request('Subscriber', FuelSDK.ET_Subscriber, self.auth_stub, {
            'Property': 'SubscriberKey',
            'SimpleOperator': 'IN',
            'Value': subscriber_keys
        })

        for subscriber in stream:
            subscriber = self.filter_keys_and_parse(subscriber)

            singer.write_records(table, [subscriber])
