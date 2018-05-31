import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.endpoints.subscribers import SubscriberDataAccessObject
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, OBJECT_ID_FIELD, MODIFIED_DATE_FIELD, \
    SUBSCRIBER_KEY_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import partition_all, sudsobj_to_dict


LOGGER = singer.get_logger()


def _get_subscriber_key(list_subscriber):
    return list_subscriber.SubscriberKey


def _get_list_subscriber_filter(_list, start, unit):
    return {
        'LogicalOperator': 'AND',
        'LeftOperand': {
            'Property': 'ListID',
            'SimpleOperator': 'equals',
            'Value': _list.get('ID'),
        },
        'RightOperand': get_date_page('ModifiedDate', start, unit)
    }


class ListSubscriberDataAccessObject(DataAccessObject):
    SCHEMA = with_properties({
        'ID': ID_FIELD,
        'CreatedDate': CREATED_DATE_FIELD,
        'ModifiedDate': MODIFIED_DATE_FIELD,
        'ObjectID': OBJECT_ID_FIELD,
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'ListID': {
            'type': ['null', 'integer'],
            'description': ('Defines identification for a list the '
                            'subscriber resides on.'),
        },
        'Status': {
            'type': ['null', 'string'],
            'description': ('Defines status of object. Status of '
                            'an address.'),
        },
        'SubscriberKey': SUBSCRIBER_KEY_FIELD,
    })

    TABLE = 'list_subscriber'
    KEY_PROPERTIES = ['SubscriberKey', 'ListID']

    def __init__(self, config, state, auth_stub, catalog):
        super(ListSubscriberDataAccessObject, self).__init__(
            config, state, auth_stub, catalog)

        self.replicate_subscriber = False
        self.subscriber_catalog = None

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
            self.subscriber_catalog)

        start = get_last_record_value_for_table(self.state, table)

        if start is None:
            start = self.config.get('start_date')

        pagination_unit = self.config.get(
            'pagination__list_subscriber_interval_unit', 'days')
        pagination_quantity = self.config.get(
            'pagination__list_subsctiber_interval_quantity', 1)

        unit = {pagination_unit: int(pagination_quantity)}

        end = increment_date(start, unit)

        all_subscribers_list = self._get_all_subscribers_list()

        while before_now(start):
            stream = request('ListSubscriber',
                             FuelSDK.ET_List_Subscriber,
                             self.auth_stub,
                             _get_list_subscriber_filter(
                                 all_subscribers_list,
                                 start, unit))

            batch_size = 100

            if self.replicate_subscriber:
                subscriber_dao.write_schema()

            for list_subscribers_batch in partition_all(stream, batch_size):
                for list_subscriber in list_subscribers_batch:
                    list_subscriber = self.filter_keys_and_parse(
                        list_subscriber)

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

                    subscriber_dao.pull_subscribers_batch(subscriber_keys)

            save_state(self.state)

            start = end
            end = increment_date(start, unit)
