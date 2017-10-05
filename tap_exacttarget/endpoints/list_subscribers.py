import FuelSDK
import singer

from funcy import get_in

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.endpoints.subscribers import SubscriberDataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST
from tap_exacttarget.state import incorporate, save_state
from tap_exacttarget.util import partition_all, sudsobj_to_dict


LOGGER = singer.get_logger()


def _get_subscriber_key(list_subscriber):
    return list_subscriber.SubscriberKey


def _get_list_subscriber_filter(_list, retrieve_all_since):
    list_filter = {
        'Property': 'ListID',
        'SimpleOperator': 'equals',
        'Value': _list.get('ID'),
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


class ListSubscriberDataAccessObject(DataAccessObject):
    SCHEMA = {
        'type': 'object',
        'inclusion': 'available',
        'selected': False,
        'properties': {
            'ID': ID_FIELD,
            'CreatedDate': {
                'type': 'string',
                'description': ('Read-only date and time of the object\'s '
                                'creation.')
            },
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
            'ListID': {
                'type': ['null', 'integer'],
                'description': ('Defines identification for a list the '
                                'subscriber resides on.'),
            },
            'Status': {
                'type': 'string',
                'description': ('Defines status of object. Status of '
                                'an address.'),
            },
            'SubscriberKey': {
                'type': 'string',
                'description': 'Identification of a specific subscriber.',
            },
        },
    }
    TABLE = 'list_subscriber'
    KEY_PROPERTIES = ['ObjectID']

    def __init__(self, config, state, auth_stub, catalog,
                 replicate_subscriber=False):
        super(ListSubscriberDataAccessObject, self).__init__(
            config, state, auth_stub, catalog)

        self.replicate_subscriber = replicate_subscriber

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

        return sudsobj_to_dict(lists[0])

    def sync_data(self):
        table = self.__class__.TABLE
        subscriber_dao = SubscriberDataAccessObject(
            self.config,
            self.state,
            self.auth_stub,
            self.catalog)

        all_subscribers_list = self._get_all_subscribers_list()

        retrieve_all_since = get_in(self.state, ['bookmarks', 'subscriber'])

        stream = request('ListSubscriber',
                         FuelSDK.ET_List_Subscriber,
                         self.auth_stub,
                         _get_list_subscriber_filter(
                             all_subscribers_list,
                             retrieve_all_since))

        batch_size = 100

        for list_subscribers_batch in partition_all(list(stream), batch_size):
            for list_subscriber in list_subscribers_batch:
                list_subscriber = self.filter_keys_and_parse(list_subscriber)

                if list_subscriber.get('ModifiedDate'):
                    self.state = incorporate(
                        self.state,
                        table,
                        'ModifiedDate',
                        list_subscriber.get('ModifiedDate'))

                singer.write_records(table, [list_subscriber])

            if self.replicate_subscriber:
                subscriber_keys = list(map(
                    _get_subscriber_key, list_subscribers_batch))

                subscriber_dao._pull_subscribers_batch(subscriber_keys)

        save_state(self.state)
