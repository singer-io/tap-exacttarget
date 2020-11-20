#!/usr/bin/env python3

import argparse
import json

import singer
from singer import utils
from singer import metadata

from tap_exacttarget.state import save_state

from tap_exacttarget.client import get_auth_stub

from tap_exacttarget.endpoints.campaigns \
    import CampaignDataAccessObject
from tap_exacttarget.endpoints.content_areas \
    import ContentAreaDataAccessObject
from tap_exacttarget.endpoints.data_extensions \
    import DataExtensionDataAccessObject
from tap_exacttarget.endpoints.emails import EmailDataAccessObject
from tap_exacttarget.endpoints.events \
    import ClickEventDataAccessObject, SentEventDataAccessObject, OpenEventDataAccessObject, \
        BounceEventDataAccessObject, UnsubEventDataAccessObject
from tap_exacttarget.endpoints.folders import FolderDataAccessObject
from tap_exacttarget.endpoints.lists import ListDataAccessObject
from tap_exacttarget.endpoints.list_sends import ListSendDataAccessObject
from tap_exacttarget.endpoints.list_subscribers \
    import ListSubscriberDataAccessObject
from tap_exacttarget.endpoints.sends import SendDataAccessObject
from tap_exacttarget.endpoints.subscribers import SubscriberDataAccessObject


LOGGER = singer.get_logger()  # noqa

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret'
]


AVAILABLE_STREAM_ACCESSORS = [
    CampaignDataAccessObject,
    ContentAreaDataAccessObject,
    DataExtensionDataAccessObject,
    EmailDataAccessObject,
    ClickEventDataAccessObject, 
    SentEventDataAccessObject,
    OpenEventDataAccessObject,
    BounceEventDataAccessObject,
    UnsubEventDataAccessObject,
    FolderDataAccessObject,
    ListDataAccessObject,
    ListSendDataAccessObject,
    ListSubscriberDataAccessObject,
    SendDataAccessObject,
    SubscriberDataAccessObject,
]


def do_discover(args):
    LOGGER.info("Starting discovery.")

    config = args.config
    state = args.state

    auth_stub = get_auth_stub(config)

    catalog = []

    for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
        # option to sync data extension
        if not config.get('discover_data_extension', False) and \
            available_stream_accessor is DataExtensionDataAccessObject:
            continue
        stream_accessor = available_stream_accessor(
            config, state, auth_stub, None)

        catalog += stream_accessor.generate_catalog()

    print(json.dumps({'streams': catalog}, indent=4))


def _is_selected(catalog_entry):
    mdata = metadata.to_map(catalog_entry['metadata'])
    return catalog_entry['schema'].get('selected', False)

def do_sync(args):
    LOGGER.info("Starting sync.")

    config = args.config
    state = args.state
    catalog = args.properties

    success = True

    auth_stub = get_auth_stub(config)

    stream_accessors = []

    subscriber_selected = False
    subscriber_catalog = None
    listsend_selected = False
    listsend_catalog = None

    list_subscriber_selected = False
    send_selected = False

    for stream_catalog in catalog.get('streams'):
        stream_accessor = None
        
        if not _is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.get('stream')))
            continue

        if SubscriberDataAccessObject.matches_catalog(stream_catalog):
            subscriber_selected = True
            subscriber_catalog = stream_catalog
            LOGGER.info("'subscriber' selected, will replicate via "
                        "'list_subscriber'")
            continue

        if ListSendDataAccessObject.matches_catalog(stream_catalog):
            listsend_selected = True
            listsend_catalog = stream_catalog
            LOGGER.info("'list send' selected, will replicate via "
                        "'send'")
            continue

        if ListSubscriberDataAccessObject.matches_catalog(stream_catalog):
            list_subscriber_selected = True

        if SendDataAccessObject.matches_catalog(stream_catalog):
            send_selected = True

        for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
            if available_stream_accessor.matches_catalog(stream_catalog):
                stream_accessors.append(available_stream_accessor(
                    config, state, auth_stub, stream_catalog))

                break

    if subscriber_selected and not list_subscriber_selected:
        LOGGER.fatal('Cannot replicate `subscriber` without '
                     '`list_subscriber`. Please select `list_subscriber` '
                     'and try again.')
        exit(1)

    if listsend_selected and not send_selected:
        LOGGER.fatal('Cannot replicate `list_send` without '
                     '`send`. Please select `send` '
                     'and try again.')
        exit(1)

    for stream_accessor in stream_accessors:
        if isinstance(stream_accessor, ListSubscriberDataAccessObject) and \
           subscriber_selected:
            stream_accessor.replicate_subscriber = True
            stream_accessor.subscriber_catalog = subscriber_catalog

        if isinstance(stream_accessor, SendDataAccessObject) and \
           listsend_selected:
            stream_accessor.replicate_listsend = True
            stream_accessor.listsend_catalog = listsend_catalog

        try:
            stream_accessor.state = state
            stream_accessor.sync()
            state = stream_accessor.state

        except Exception as e:
            LOGGER.exception(e)
            LOGGER.error('Failed to sync endpoint, moving on!')
            success = False

    save_state(state)

    return success


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    success = True

    if args.discover:
        do_discover(args)
    elif args.properties:
        success = do_sync(args)
    else:
        LOGGER.info("No properties were selected")

    if success:
        LOGGER.info("Completed successfully, exiting.")
        exit(0)
    else:
        LOGGER.info("Run failed, exiting.")
        exit(1)

if __name__ == '__main__':
    main()
