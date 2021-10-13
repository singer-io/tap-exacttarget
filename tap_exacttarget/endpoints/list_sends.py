import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)

LOGGER = singer.get_logger()


class ListSendDataAccessObject(DataAccessObject):

    TABLE = 'list_send'
    KEY_PROPERTIES = ['ListID', 'SendID']
    REPLICATION_METHOD = 'FULL_TABLE'

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['ListID'] = to_return.get('List', {}).get('ID')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_ListSend

        # making this endpoint as FULL_TABLE, as 'ModifiedDate' is not retrievable as discussed
        # here: https://salesforce.stackexchange.com/questions/354332/not-getting-modifieddate-for-listsend-endpoint
        stream = request('ListSend',
                         selector,
                         self.auth_stub,
                         batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for list_send in stream:
            list_send = self.filter_keys_and_parse(list_send)

            self.write_records_with_transform(list_send, catalog_copy, table)
