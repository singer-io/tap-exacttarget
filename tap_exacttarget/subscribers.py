import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import CUSTOM_PROPERTY_LIST, ID_FIELD
from tap_exacttarget.state import incorporate, save_state
from tap_exacttarget.util import partition_all


SCHEMA = {
    'type': 'object',
    'properties': {
        'Addresses': {
            'type': 'array',
            'inclusion': 'available',
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
            'type': 'string',
            'inclusion': 'available',
            'description': ('Identifies correlation of objects across '
                            'several requests.'),
        },
        'CreatedDate': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Read-only date and time of the object\'s creation.'
        },
        'CustomerKey': {
            'type': 'string',
            'inclusion': 'automatic',
            'description': ('User-supplied unique identifier for an object '
                            'within an object type (corresponds to the '
                            'external key assigned to an object in the user '
                            'interface).'),
        },
        'EmailAddress': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Contains the email address for a subscriber. '
                            'Indicates the data extension field contains '
                            'email address data.'),
        },
        'EmailTypePreference': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'The format in which email should be sent'
        },
        'ID': ID_FIELD,
        'ListIDs': {
            'type': 'array',
            'inclusion': 'available',
            'description': 'Defines list IDs a subscriber resides on.',
            'items': {
                'type': 'string'
            }
        },
        'Locale': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Contains the locale information for an Account. '
                            'If no location is set, Locale defaults to en-US '
                            '(English in United States).'),
        },
        'ModifiedDate': {
            'type': ['string', 'null'],
            'inclusion': 'automatic',
            'description': ('Indicates the last time object information was '
                            'modified.')
        },
        'ObjectID': {
            'type': 'string',
            'inclusion': 'automatic',
            'description': ('System-controlled, read-only text string '
                            'identifier for object.'),
        },
        'ObjectState': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Reserved for future use.'
        },
        'PartnerKey': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Unique identifier provided by partner for an '
                            'object, accessible only via API.'),
        },
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'PartnerType': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Defines partner associated with a subscriber.'
        },
        'PrimaryEmailAddress': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Indicates primary email address for a subscriber.'
        },
        'PrimarySMSAddress': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Indicates primary SMS address for a subscriber. '
                            'Used to create and update SMS Address for a '
                            'given subscriber.'),
        },
        'PrimarySMSPublicationStatus': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Indicates the subscriber\'s modality status.',
        },
        'Status': {
            'type': 'string',
            'inclusion': 'available',
            'description': 'Defines status of object. Status of an address.',
        },
        'SubscriberKey': {
            'type': 'string',
            'inclusion': 'automatic',
            'description': 'Identification of a specific subscriber.',
        },
        'SubscriberTypeDefinition': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Specifies if a subscriber resides in an '
                            'integration, such as Salesforce or Microsoft '
                            'Dynamics CRM'),
        },
        'UnsubscribedDate': {
            'type': 'string',
            'inclusion': 'available',
            'description': ('Represents date subscriber unsubscribed '
                            'from a list.'),
        }
    }
}


def _get_list_subscriber_filter(_list, retrieve_all_since):
    list_filter = {
        'Property': 'ListID',
        'SimpleOperator': 'equals',
        'Value': _list.get('ListID'),
    }

    full_filter = None

    if retrieve_all_since:
        full_filter = {
            'LogicalOperator': 'AND',
            'LeftOperand': list_filter,
            'RightOperand': {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since,
            }
        }
    else:
        full_filter = list_filter

    return full_filter


def _get_subscriber_key(list_subscriber):
    list_subscriber.get('SubscriberKey')


class SubscriberDataAccessObject(DataAccessObject):

    SCHEMA = SCHEMA
    TABLE = 'subscriber'
    KEY_PROPERTIES = ['ObjectID']

    def parse_object(self, obj):
        to_return = obj.copy()

        if 'ListIDs' in to_return:
            to_return['ListIDs'] = [_list.get('ObjectID')
                                    for _list in to_return.get('Lists')]

        return to_return

    def sync(self):
        all_subscribers_list = self._get_all_subscribers_list()

        self._pull_list_subscribers(all_subscribers_list)

    def _get_all_subscribers_list(self):
        """
        Find the 'All Subscribers' list via the SOAP API, and return it.
        """
        result = request('List', FuelSDK.ET_List, self.auth_stub, {
            'Property': 'ListName',
            'SimpleOperator': 'equals',
            'Value': 'All Subscribers',
        })

        lists = list(result)

        if len(lists) != 1:
            msg = ('Found {} all subscriber lists, expected one!'
                   .format(len(lists)))
            raise RuntimeError(msg)

        return lists[0]

    def _pull_list_subscribers(self, all_subscribers_list):
        """
        Pull all the ListSubscribers for a given List, and then pull the
        associated Subscribers. Persist the ListSubscribers and Subscribers.
        """
        retrieve_all_since = self.state.get('subscriber')

        stream = request('ListSubscriber',
                         FuelSDK.ET_List_Subscriber,
                         self.auth_stub,
                         _get_list_subscriber_filter(
                             all_subscribers_list,
                             retrieve_all_since))

        batch_size = 100

        for list_subscribers_batch in partition_all(stream, batch_size):
            subscriber_keys = list(map(
                _get_subscriber_key, list_subscribers_batch))

            self._pull_subscribers_batch(subscriber_keys)

    def _pull_subscribers_batch(self, subscriber_keys):
        table = self.__class__.TABLE
        stream = request('Subscriber', FuelSDK.ET_Subscriber, self.auth_stub, {
            'Property': 'SubscriberKey',
            'SimpleOperator': 'IN',
            'Value': subscriber_keys
        })

        for subscriber in stream:
            subscriber = self.filter_keys_and_parse(subscriber)
            state = incorporate(self.state,
                                table,
                                'ModifiedDate',
                                subscriber.get('ModifiedDate'))

            singer.write_records(table, [subscriber])

        save_state(state)
