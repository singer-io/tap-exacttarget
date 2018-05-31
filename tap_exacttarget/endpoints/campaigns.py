import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import with_properties

LOGGER = singer.get_logger()


class CampaignDataAccessObject(DataAccessObject):

    SCHEMA = with_properties({
        'id': {
            'type': ['null', 'string'],
        },
        'createdDate': {
            'type': ['null', 'string'],
        },
        'modifiedDate': {
            'type': ['null', 'string'],
        },
        'name': {
            'type': ['null', 'string'],
        },
        'description': {
            'type': ['null', 'string'],
        },
        'campaignCode': {
            'type': ['null', 'string'],
        },
        'color': {
            'type': ['null', 'string'],
        }
    })

    TABLE = 'campaign'
    KEY_PROPERTIES = ['id']

    def sync_data(self):
        cursor = request(
            'Campaign',
            FuelSDK.ET_Campaign,
            self.auth_stub)

        for campaign in cursor:
            campaign = self.filter_keys_and_parse(campaign)

            singer.write_records(self.__class__.TABLE, [campaign])
