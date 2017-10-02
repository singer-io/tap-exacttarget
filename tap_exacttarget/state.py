import json
import singer

from voluptuous import Schema, Required

logger = singer.get_logger()

STATE_SCHEMA = Schema({
    Required('bookmarks'): {
        str: {
            Required('last_record'): str,
            Required('field'): str,
        }
    }
})


def incorporate(state, table, field, value):
    new_state = state.copy()

    if 'bookmarks' not in new_state:
        new_state['bookmarks'] = {}

    if(new_state['bookmarks'].get(table, {}).get('last_record') is None or
       new_state['bookmarks'].get(table, {}).get('last_record') < value):
        new_state['bookmarks'][table] = {
            'field': field,
            'last_record': value,
        }

    return new_state


def save_state(state):
    global STATE_SCHEMA

    STATE_SCHEMA(state)

    logger.info('Updating state.')

    singer.write_state(state)


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as f:
            return json.load(f)
    except:
        logger.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError
