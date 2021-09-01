import FuelSDK
import copy
import singer
from singer import Transformer, metadata

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import ID_FIELD, CUSTOM_PROPERTY_LIST, \
    CREATED_DATE_FIELD, CUSTOMER_KEY_FIELD, OBJECT_ID_FIELD, \
    MODIFIED_DATE_FIELD, with_properties

LOGGER = singer.get_logger()


class ListSendDataAccessObject(DataAccessObject):
    SCHEMA = with_properties({
        'CreatedDate': CREATED_DATE_FIELD,
        'CustomerKey': CUSTOMER_KEY_FIELD,
        'ExistingUndeliverables': {
            'type': ['null', 'integer'],
            'description': ('Indicates whether bounces occurred on previous '
                            'send.'),
        },
        'ExistingUnsubscribes': {
            'type': ['null', 'integer'],
            'description': ('Indicates whether unsubscriptions occurred on '
                            'previous send.'),
        },
        'ForwardedEmails': {
            'type': ['null', 'integer'],
            'description': ('Number of emails forwarded for a send.'),
        },
        'HardBounces': {
            'type': ['null', 'integer'],
            'description': ('Indicates number of hard bounces associated '
                            'with a send.'),
        },
        'InvalidAddresses': {
            'type': ['null', 'integer'],
            'description': ('Specifies the number of invalid addresses '
                            'associated with a send.'),
        },
        'ListID': {
            'type': ['null', 'integer'],
            'description': 'List associated with the send.',
        },
        'ID': ID_FIELD,
        'MissingAddresses': {
            'type': ['null', 'integer'],
            'description': ('Specifies number of missing addresses '
                            'encountered within a send.'),
        },
        'ModifiedDate': MODIFIED_DATE_FIELD,
        'NumberDelivered': {
            'type': ['null', 'integer'],
            'description': ('Number of sent emails that did not bounce.'),
        },
        'NumberSent': {
            'type': ['null', 'integer'],
            'description': ('Number of emails actually sent as part of an '
                            'email send. This number reflects all of the sent '
                            'messages and may include bounced messages.'),
        },
        'ObjectID': OBJECT_ID_FIELD,
        'OtherBounces': {
            'type': ['null', 'integer'],
            'description': 'Specifies number of Other-type bounces in a send.',
        },
        'PartnerProperties': CUSTOM_PROPERTY_LIST,
        'SendID': {
            'type': ['null', 'integer'],
            'description': 'Contains identifier for a specific send.',
        },
        'SoftBounces': {
            'type': ['null', 'integer'],
            'description': ('Indicates number of soft bounces associated with '
                            'a specific send.'),
        },
        'UniqueClicks': {
            'type': ['null', 'integer'],
            'description': 'Indicates number of unique clicks on message.',
        },
        'UniqueOpens': {
            'type': ['null', 'integer'],
            'description': ('Indicates number of unique opens resulting from '
                            'a triggered send.'),
        },
        'Unsubscribes': {
            'type': ['null', 'integer'],
            'description': ('Indicates the number of unsubscribe events '
                            'associated with a send.'),
        },
    })

    TABLE = 'list_send'
    KEY_PROPERTIES = ['ListID', 'SendID']
    REPLICATION_METHOD = 'FULL_TABLE'

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['ListID'] = to_return.get('List', {}).get('ID')

        return super(ListSendDataAccessObject, self).parse_object(to_return)

    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_ListSend

        # making this endpoint as FULL_TABLE, as 'ModifiedDate' is not retrievable as discussed
        # here: https://salesforce.stackexchange.com/questions/354332/not-getting-modifieddate-for-listsend-endpoint
        stream = request('ListSend',
                         selector,
                         self.auth_stub)

        catalog_copy = copy.deepcopy(self.catalog)

        for list_send in stream:
            list_send = self.filter_keys_and_parse(list_send)

            with Transformer() as transformer:
                for rec in [list_send]:
                    rec = transformer.transform(rec, catalog_copy.get('schema'), metadata.to_map(catalog_copy.get('metadata')))
                    singer.write_record(table, rec)
