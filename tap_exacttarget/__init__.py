#!/usr/bin/env python3

import FuelSDK

import argparse
import json

import singer
import tap_exacttarget.schemas
from tap_exacttarget.client import request
from tap_exacttarget.state import load_state, save_state, incorporate
from tap_exacttarget.util import sudsobj_to_dict

from tap_exacttarget.data_extensions import DataExtensionDataAccessObject
from tap_exacttarget.subscribers import SubscriberDataAccessObject


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
            state = incorporate(state, table, 'EventDate',
                                event.get('EventDate'))

            singer.write_records(table, [event])

    save_state(state)


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

        state = incorporate(state, table, 'ModifiedDate',
                            send.get('ModifiedDate'))

        singer.write_records(table, [send])

    save_state(state)


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


def load_catalog(filename):
    catalog = {}

    try:
        with open(filename) as f:
            catalog = json.load(f)
    except:
        logger.fatal("Failed to decode catalog file. Is it valid json?")
        raise RuntimeError

    return catalog


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


AVAILABLE_STREAMS = [
    DataExtensionDataAccessObject,
    SubscriberDataAccessObject
]


def do_discover(args):
    logger.info("Starting discovery.")

    config = load_config(args.config)
    state = load_state(args.state)

    auth_stub = get_auth_stub(config)

    catalog = []

    for available_stream in AVAILABLE_STREAMS:
        stream = available_stream(config, state, auth_stub, None)

        catalog += stream.generate_catalog()

    print(json.dumps({'streams': catalog}))


def do_sync(args):
    logger.info("Starting sync.")

    config = load_config(args.config)
    state = load_state(args.state)
    catalog = load_catalog(args.catalog)

    auth_stub = get_auth_stub(config)

    for available_stream in AVAILABLE_STREAMS:
        stream_catalogs = [stream_catalog for stream_catalog in catalog
                           if (stream_catalog.get('stream') ==
                               available_stream.TABLE)]

        for stream_catalog in stream_catalogs:
            stream = available_stream(config, state, auth_stub, stream_catalog)
            stream.sync()

    sync_events(config, state, auth_stub)
    sync_sends(config, state, auth_stub)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')
    parser.add_argument(
        '-C', '--catalog', help='Catalog file with fields selected')

    parser.add_argument(
        '-d', '--discover',
        help='Build a catalog from the underlying schema',
        action='store_true')

    args = parser.parse_args()

    try:
        if args.discover:
            do_discover(args)
        else:
            do_sync(args)
    except RuntimeError as e:
        logger.error(str(e))
        logger.fatal("Run failed.")
        exit(1)


if __name__ == '__main__':
    main()
