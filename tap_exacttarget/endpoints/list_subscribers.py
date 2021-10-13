import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.endpoints.subscribers import SubscriberDataAccessObject
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import partition_all, sudsobj_to_dict


LOGGER = singer.get_logger()


def _get_subscriber_key(list_subscriber):
    # return the 'SubscriberKey' of the subscriber
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

    TABLE = 'list_subscriber'
    KEY_PROPERTIES = ['SubscriberKey', 'ListID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def __init__(self, config, state, auth_stub, catalog):
        super().__init__(
            config, state, auth_stub, catalog)

        self.replicate_subscriber = False
        self.subscriber_catalog = None

    @exacttarget_error_handling
    def _get_all_subscribers_list(self):
        """
        Find the 'All Subscribers' list via the SOAP API, and return it.
        """
        result = request('List', FuelSDK.ET_List, self.auth_stub, {
            'Property': 'ListName',
            'SimpleOperator': 'equals',
            'Value': 'All Subscribers',
        }, batch_size=self.batch_size)

        lists = list(result)

        if len(lists) != 1:
            msg = ('Found {} all subscriber lists, expected one!'
                   .format(len(lists)))
            raise RuntimeError(msg)

        return sudsobj_to_dict(lists[0])

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        subscriber_dao = SubscriberDataAccessObject(
            self.config,
            self.state,
            self.auth_stub,
            self.subscriber_catalog)

        # pass config to return start date if not bookmark is found
        start = get_last_record_value_for_table(self.state, table, self.config)

        pagination_unit = self.config.get(
            'pagination__list_subscriber_interval_unit', 'days')
        pagination_quantity = self.config.get(
            'pagination__list_subscriber_interval_quantity', 1)

        unit = {pagination_unit: int(pagination_quantity)}

        end = increment_date(start, unit)

        all_subscribers_list = self._get_all_subscribers_list()

        while before_now(start):
            stream = request('ListSubscriber',
                             FuelSDK.ET_List_Subscriber,
                             self.auth_stub,
                             _get_list_subscriber_filter(
                                 all_subscribers_list,
                                 start, unit),
                             batch_size=self.batch_size)

            batch_size = 100

            if self.replicate_subscriber:
                subscriber_dao.write_schema()

            catalog_copy = copy.deepcopy(self.catalog)

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

                    self.write_records_with_transform(list_subscriber, catalog_copy, table)

                if self.replicate_subscriber:
                    # make the list of subscriber keys
                    subscriber_keys = list(map(
                        _get_subscriber_key, list_subscribers_batch))

                    # pass the list of 'subscriber_keys' to fetch subscriber details
                    subscriber_dao.pull_subscribers_batch(subscriber_keys)

            save_state(self.state)

            start = end
            end = increment_date(start, unit)
