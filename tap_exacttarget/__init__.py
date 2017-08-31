#!/usr/bin/env python3

import FuelSDK

import suds

import argparse
import datetime
import json

import singer
import tap_exacttarget.schemas


logger = singer.get_logger()

table_mapping = {
    'campaign': FuelSDK.ET_Campaign,
    'event': FuelSDK.ET_SentEvent,
}

event_endpoints = {
    'sent': FuelSDK.ET_SentEvent,
    'click': FuelSDK.ET_ClickEvent,
    'open': FuelSDK.ET_OpenEvent,
    'bounce': FuelSDK.ET_BounceEvent,
    'unsub': FuelSDK.ET_UnsubEvent
}


def get_auth_stub(config):
    logger.info("Generating auth stub...")

    auth_stub = FuelSDK.ET_Client(
        params={
            'clientid': config['client_id'],
            'clientsecret': config['client_secret']
        })

    logger.info("Success.")

    return auth_stub


def request(name, selector, auth_stub, search_filter=None):
    cursor = selector()
    cursor.auth_stub = auth_stub

    if search_filter is not None:
        cursor.search_filter = search_filter

    logger.info(
        "Making RETRIEVE call to '{}' endpoint with filter {}."
        .format(name, search_filter))

    response = cursor.get()

    for item in response.results:
        yield item

    while response.more_results:
        logger.info("Getting more results from '{}' endpoint".format(name))

        response = response.getMoreResults()

        for item in response.results:
            yield item

    logger.info("Done retrieving results from '{}' endpoint".format(name))


def sync_events(config, state, auth_stub):
    table = 'event'

    singer.write_schema(
        table,
        tap_exacttarget.schemas.event,
        key_properties=['SendID', 'EventType', 'SubscriberKey', 'EventDate'])

    for event_name, selector in event_endpoints.items():
        search_filter = None
        retrieve_all_since = state.get('event', {}).get(event_name)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'EventDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request(event_name, selector, auth_stub, search_filter)

        for event in stream:
            event = parse_event(event)

            if state.get(table) is None:
                state[table] = {}

            if state.get(table).get(event_name) is None or \
               event.get('EventDate') > state.get(table).get(event_name):
                state[table][event_name] = event.get('EventDate')

            singer.write_records(table, [event])

    singer.write_state(state)


def parse_event(event):
    event = sudsobj_to_dict(event)

    return {
        "SendID": event.get("SendID"),
        "EventDate": event.get("EventDate"),
        "EventType": event.get("EventType"),
        "SubscriberKey": event.get("SubscriberKey")
    }


def sync_sends(config, state, auth_stub):
    table = 'send'
    selector = FuelSDK.ET_Send

    singer.write_schema(
        table,
        tap_exacttarget.schemas.send,
        key_properties=['ID'])

    search_filter = None
    retrieve_all_since = state.get(table)

    if retrieve_all_since is not None:
        search_filter = {
            'Property': 'ModifiedDate',
            'SimpleOperator': 'greaterThan',
            'Value': retrieve_all_since
        }

    stream = request(table, selector, auth_stub, search_filter)

    for send in stream:
        send = parse_send(send)

        if table not in state or send.get('ModifiedDate') > state[table]:
            state[table] = send.get('ModifiedDate')

        singer.write_records(table, [send])

    singer.write_state(state)


def parse_send(send):
    send = sudsobj_to_dict(send)

    return {
        "CreatedDate": send.get("CreatedDate"),
        "EmailName": str(send.get("EmailName")),
        "FromAddress": str(send.get("FromAddress")),
        "FromName": str(send.get("FromName")),
        "ID": send.get("ID"),
        "IsAlwaysOn": send.get("IsAlwaysOn"),
        "IsMultipart": send.get("IsMultipart"),
        "ModifiedDate": send.get("ModifiedDate"),
        "PartnerProperties": send.get("PartnerProperties"),
        "SendDate": send.get("SendDate"),
        "SentDate": send.get("SentDate"),
        "Status": send.get("Status"),
        "Subject": send.get("Subject"),
    }


def sudsobj_to_dict(obj):
    if isinstance(obj, list):
        return [sudsobj_to_dict(item) for item in obj]

    if not isinstance(obj, suds.sudsobject.Object):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')

        return obj

    to_return = {}

    for key in obj.__keylist__:
        to_return[key] = sudsobj_to_dict(getattr(obj, key))

    return to_return


def validate_config(config):
    required_keys = ['client_id', 'client_secret']
    missing_keys = []
    null_keys = []
    has_errors = False

    for required_key in required_keys:
        if required_key not in config:
            missing_keys.append(required_key)

        elif config.get(required_key) is None:
            null_keys.append(required_key)

    if len(missing_keys) > 0:
        logger.fatal("Config is missing keys: {}"
                     .format(", ".join(missing_keys)))
        has_errors = True

    if len(null_keys) > 0:
        logger.fatal("Config has null keys: {}"
                     .format(", ".join(null_keys)))
        has_errors = True

    if has_errors:
        raise RuntimeError


def load_config(filename):
    config = {}

    try:
        with open(filename) as f:
            config = json.load(f)
    except:
        logger.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    validate_config(config)

    return config


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as f:
            return json.load(f)
    except:
        logger.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError


def do_sync(args):
    logger.info("Starting sync.")

    config = load_config(args.config)
    state = load_state(args.state)

    auth_stub = get_auth_stub(config)

    sync_events(config, state, auth_stub)
    sync_sends(config, state, auth_stub)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    try:
        do_sync(args)
    except RuntimeError:
        logger.fatal("Run failed.")
        exit(1)


if __name__ == '__main__':
    main()
