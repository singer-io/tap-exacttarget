import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class EventDataAccessObject(DataAccessObject):

    TABLE = 'event'
    KEY_PROPERTIES = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['EventDate']

    @exacttarget_error_handling
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

            # pass config to return start date if not bookmark is found
            start = get_last_record_value_for_table(self.state, event_name, self.config)

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
                                 search_filter,
                                 batch_size=self.batch_size)

                catalog_copy = copy.deepcopy(self.catalog)

                for event in stream:
                    event = self.filter_keys_and_parse(event)

                    self.state = incorporate(self.state,
                                             event_name,
                                             'EventDate',
                                             event.get('EventDate'))

                    if event.get('SubscriberKey') is None:
                        LOGGER.info("SubscriberKey is NULL so ignoring {} record with SendID: {} and EventDate: {}"
                                    .format(event_name,
                                            event.get('SendID'),
                                            event.get('EventDate')))
                        continue

                    self.write_records_with_transform(event, catalog_copy, table)

                self.state = incorporate(self.state,
                                         event_name,
                                         'EventDate',
                                         start)

                save_state(self.state)

                start = end
                end = increment_date(start, unit)
