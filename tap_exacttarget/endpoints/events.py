import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.schemas import SUBSCRIBER_KEY_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state


LOGGER = singer.get_logger()


class EventDataAccessObject(DataAccessObject):
    SCHEMA = with_properties({
        'SendID': {
            'type': 'integer',
            'description': 'Contains identifier for a specific send.',
        },
        'EventDate': {
            'type': 'string',
            'format': 'datetime',
            'description': 'Date when a tracking event occurred.',
        },
        'EventType': {
            'type': 'string',
            'description': 'The type of tracking event',
        },
        'SubscriberKey': SUBSCRIBER_KEY_FIELD,
    })

    TABLE = 'event'
    KEY_PROPERTIES = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']

    def sync_data(self):
        table = self.__class__.TABLE
        endpoints = {
            'sent': FuelSDK.ET_SentEvent,
            'click': FuelSDK.ET_ClickEvent,
            'open': FuelSDK.ET_OpenEvent,
            'bounce': FuelSDK.ET_BounceEvent,
            'unsub': FuelSDK.ET_UnsubEvent
        }

        for event_name, selector in endpoints.items():
            search_filter = None
            retrieve_all_since = self.state.get('event', {}).get(event_name)

            if retrieve_all_since is not None:
                search_filter = {
                    'Property': 'EventDate',
                    'SimpleOperator': 'greaterThan',
                    'Value': retrieve_all_since
                }

            stream = request(event_name,
                             selector,
                             self.auth_stub,
                             search_filter)

            for event in stream:
                event = self.filter_keys_and_parse(event)

                self.state = incorporate(self.state,
                                         event_name,
                                         'EventDate',
                                         event.get('EventDate'))

                singer.write_records(table, [event])

        save_state(self.state)
