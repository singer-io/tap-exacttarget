import FuelSDK
import copy
import singer
from singer import Transformer, metadata
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
    REPLICATION_METHOD = 'FULL_TABLE'

    def sync_data(self):
        cursor = request(
            'Campaign',
            FuelSDK.ET_Campaign,
            self.auth_stub)

        catalog_copy = copy.deepcopy(self.catalog)

        for campaign in cursor:
            campaign = self.filter_keys_and_parse(campaign)

            with Transformer() as transformer:
                for rec in [campaign]:
                    rec = transformer.transform(rec, catalog_copy.get('schema'), metadata.to_map(catalog_copy.get('metadata')))
                    singer.write_record(self.__class__.TABLE, rec)
