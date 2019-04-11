from dateutil.parser import parse

import datetime
import singer

from voluptuous import Schema, Required

from tap_exacttarget.pagination import DATE_FORMAT, decrement_date

LOGGER = singer.get_logger()

STATE_SCHEMA = Schema({
    Required('bookmarks'): {
        str: {
            Required('last_record'): str,
            Required('field'): str,
        }
    }
})


def get_last_record_value_for_table(state, table):
    raw = state.get('bookmarks', {}) \
               .get(table, {}) \
               .get('last_record')

    if raw is None:
        return None

    interval_unit = self.config.get('state__bookmark_interval_unit', 'days')
    interval = self.config.get('state__bookmark_interval', 1)

    unit = {interval_unit: int(interval)}

    return decrement_date(state, unit)


def incorporate(state, table, field, value):
    if value is None:
        return state

    new_state = state.copy()

    parsed = parse(value).strftime("%Y-%m-%dT%H:%M:%SZ")

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if(new_state['bookmarks'].get(table, {}).get('last_record') is None or
       new_state['bookmarks'].get(table, {}).get('last_record') < value):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_record': parsed,
        }

    return new_state


def save_state(state):
    if not state:
        return

    STATE_SCHEMA(state)

    LOGGER.info('Updating state.')

    singer.write_state(state)
