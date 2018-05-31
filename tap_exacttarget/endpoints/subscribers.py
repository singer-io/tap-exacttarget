import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import CUSTOM_PROPERTY_LIST, ID_FIELD, \
    CREATED_DATE_FIELD, CUSTOMER_KEY_FIELD, OBJECT_ID_FIELD, \
    SUBSCRIBER_KEY_FIELD, MODIFIED_DATE_FIELD, with_properties


LOGGER = singer.get_logger()

SCHEMA = with_properties({
    'Addresses': {
        'type': 'array',
        'description': ('Indicates addresses belonging to a subscriber, '
                        'used to create, retrieve, update or delete an '
                        'email or SMS Address for a given subscriber.'),
        'items': {
            'type': 'object',
            'properties': {
                'Address': {'type': ['null', 'string']},
                'AddressType': {'type': ['null', 'string']},
                'AddressStatus': {'type': ['null', 'string']}
            }
        }
    },
    'Attributes': CUSTOM_PROPERTY_LIST,
    'CreatedDate': CREATED_DATE_FIELD,
    'CustomerKey': CUSTOMER_KEY_FIELD,
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
            'type': ['null', 'string']
        }
    },
    'Locale': {
        'type': ['null', 'string'],
        'description': ('Contains the locale information for an Account. '
                        'If no location is set, Locale defaults to en-US '
                        '(English in United States).'),
    },
    'ModifiedDate': MODIFIED_DATE_FIELD,
    'ObjectID': OBJECT_ID_FIELD,
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
        'type': ['null', 'string'],
        'description': 'Defines status of object. Status of an address.',
    },
    'SubscriberKey': SUBSCRIBER_KEY_FIELD,
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
})


class SubscriberDataAccessObject(DataAccessObject):

    SCHEMA = SCHEMA
    TABLE = 'subscriber'
    KEY_PROPERTIES = ['ID']

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

    def pull_subscribers_batch(self, subscriber_keys):
        if not subscriber_keys:
            return

        table = self.__class__.TABLE
        _filter = {}

        if len(subscriber_keys) == 1:
            _filter = {
                'Property': 'SubscriberKey',
                'SimpleOperator': 'equals',
                'Value': subscriber_keys[0]
            }

        elif len(subscriber_keys) > 1:
            _filter = {
                'Property': 'SubscriberKey',
                'SimpleOperator': 'IN',
                'Value': subscriber_keys
            }
        else:
            LOGGER.info('Got empty set of subscriber keys, moving on')
            return

        stream = request(
            'Subscriber', FuelSDK.ET_Subscriber, self.auth_stub, _filter)

        for subscriber in stream:
            subscriber = self.filter_keys_and_parse(subscriber)

            singer.write_records(table, [subscriber])
