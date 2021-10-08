import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject

LOGGER = singer.get_logger()


class CampaignDataAccessObject(DataAccessObject):

    TABLE = 'campaign'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'FULL_TABLE'

    def sync_data(self):
        cursor = request(
            'Campaign',
            FuelSDK.ET_Campaign,
            self.auth_stub)

        catalog_copy = copy.deepcopy(self.catalog)

        for campaign in cursor:
            campaign = self.filter_keys_and_parse(campaign)

            self.write_records_with_transform(campaign, catalog_copy, self.TABLE)
