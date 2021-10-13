from dateutil.parser import parse

import datetime
import singer

from voluptuous import Schema, Required

from tap_exacttarget.pagination import DATE_FORMAT

LOGGER = singer.get_logger()

STATE_SCHEMA = Schema({
    Required('bookmarks'): {
        str: {
            Required('last_record'): str,
            Required('field'): str,
        }
    }
})


# get the start date / bookmark date for the stream
# 'config': to return start date if no bookmark is found
def get_last_record_value_for_table(state, table, config):
    raw = state.get('bookmarks', {}) \
               .get(table, {}) \
               .get('last_record')

    # return 'start date' from 'config' if no bookmark found in state file
    if raw is None:
        return config['start_date']

    date_obj = datetime.datetime.strptime(raw, DATE_FORMAT)
    date_obj = date_obj - datetime.timedelta(days=1)

    return date_obj.strftime(DATE_FORMAT)

# updated the state file with the provided value
def incorporate(state, table, field, value):
    if value is None:
        return state

    new_state = state.copy()

    parsed = parse(value).strftime("%Y-%m-%dT%H:%M:%SZ")

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    # used 'parsed' value in second condition below instead of original 'value'
    # because for data extensions bookmark value is coming in the format
    # 'dd/mm/yyyy hh:mm:ss am/pm' and the bookmark in the state file
    # is saved in 'yyyy-mm-ddThh:mm:ssZ'
    # Value in STATE file: 2021-08-31T18:00:00Z
    # Replication key value from data: 8/24/2021 6:00:00 PM
    # Replication key value from data 'parsed': 2021-08-24T18:00:00Z
    if(new_state['bookmarks'].get(table, {}).get('last_record') is None or
       new_state['bookmarks'].get(table, {}).get('last_record') < parsed):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_record': parsed,
        }

    return new_state

# save the state
def save_state(state):
    if not state:
        return

    STATE_SCHEMA(state)

    LOGGER.info('Updating state.')

    singer.write_state(state)
