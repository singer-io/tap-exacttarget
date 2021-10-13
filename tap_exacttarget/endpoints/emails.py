import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table

LOGGER = singer.get_logger()


class EmailDataAccessObject(DataAccessObject):

    TABLE = 'email'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def parse_object(self, obj):
        to_return = obj.copy()
        content_area_ids = []

        for content_area in to_return.get('ContentAreas', []):
            content_area_ids.append(content_area.get('ID'))

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')
        to_return['ContentAreaIDs'] = content_area_ids

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Email

        search_filter = None
        # pass config to return start date if not bookmark is found
        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Email',
                         selector,
                         self.auth_stub,
                         search_filter,
                         batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for email in stream:
            email = self.filter_keys_and_parse(email)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     email.get('ModifiedDate'))

            self.write_records_with_transform(email, catalog_copy, table)

        save_state(self.state)
