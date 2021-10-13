import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)

LOGGER = singer.get_logger()

class SubscriberDataAccessObject(DataAccessObject):

    TABLE = 'subscriber'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

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

        return super().parse_object(obj)

    def sync_data(self):
        pass

    # fetch subscriber records based in the 'subscriber_keys' provided
    @exacttarget_error_handling
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
            'Subscriber', FuelSDK.ET_Subscriber, self.auth_stub, _filter, batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for subscriber in stream:
            subscriber = self.filter_keys_and_parse(subscriber)

            self.write_records_with_transform(subscriber, catalog_copy, table)
