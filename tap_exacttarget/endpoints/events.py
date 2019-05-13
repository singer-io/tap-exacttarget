import FuelSDK
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import DataAccessObject
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.schemas import SUBSCRIBER_KEY_FIELD, with_properties
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class EventDataAccessObject(DataAccessObject):
    SCHEMA = with_properties({
        'SendID': {
            'type': ['null', 'integer'],
            'description': 'Contains identifier for a specific send.',
        },
        'EventDate': {
            'type': ['null', 'string'],
            'format': 'datetime',
            'description': 'Date when a tracking event occurred.',
        },
        'EventType': {
            'type': ['null', 'string'],
            'description': 'The type of tracking event',
        },
        'BatchID': {
            'type': ['null','integer'],
            'description': 'Ties triggered send sent events to other events (like clicks and opens that occur at a later date and time)',
        },
        'CorrelationID': {
            'type': ['null','string'],
            'description': 'Identifies correlation of objects across several requests.',
        },
        'URL': {
            'type': ['null','string'],
            'description': 'URL that was clicked.',
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

            start = get_last_record_value_for_table(self.state, event_name)

            if start is None:
                start = self.config.get('start_date')

            if start is None:
                raise RuntimeError('start_date not defined!')

            pagination_unit = self.config.get(
                'pagination__{}_interval_unit'.format(event_name), 'minutes')
            pagination_quantity = self.config.get(
                'pagination__{}_interval_quantity'.format(event_name), 10)

            unit = {pagination_unit: int(pagination_quantity)}

            end = increment_date(start, unit)

            while before_now(start):
                LOGGER.info("Fetching {} from {} to {}"
                            .format(event_name, start, end))

                search_filter = get_date_page('EventDate', start, unit)

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

                self.state = incorporate(self.state,
                                         event_name,
                                         'EventDate',
                                         start)

                save_state(self.state)

                start = end
                end = increment_date(start, unit)
