import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class FolderDataAccessObject(DataAccessObject):

    TABLE = 'folder'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['ParentFolder'] = to_return.get('ParentFolder', {}).get('ID')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Folder

        search_filter = None

        # pass config to return start date if not bookmark is found
        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Folder',
                         selector,
                         self.auth_stub,
                         search_filter,
                         batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for folder in stream:
            folder = self.filter_keys_and_parse(folder)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     folder.get('ModifiedDate'))

            self.write_records_with_transform(folder, catalog_copy, table)

        save_state(self.state)
