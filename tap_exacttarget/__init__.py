#!/usr/bin/env python3

import argparse
import json

import sys

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
from tap_exacttarget.endpoints.events import EventDataAccessObject
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
    EventDataAccessObject,
    FolderDataAccessObject,
    ListDataAccessObject,
    ListSendDataAccessObject,
    ListSubscriberDataAccessObject,
    SendDataAccessObject,
    SubscriberDataAccessObject,
]

# run discover mode
def do_discover(args):
    LOGGER.info("Starting discovery.")

    config = args.config
    state = args.state

    auth_stub = get_auth_stub(config)

    catalog = []

    for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
        stream_accessor = available_stream_accessor(
            config, state, auth_stub, None)

        catalog += stream_accessor.generate_catalog()

    print(json.dumps({'streams': catalog}, indent=4))

# check if the stream is selected or not
def _is_selected(catalog_entry):
    mdata = metadata.to_map(catalog_entry['metadata'])
    return singer.should_sync_field(metadata.get(mdata, (), 'inclusion'),
                                    metadata.get(mdata, (), 'selected'),
                                    default=False)

# run sync mode
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
    list_subscriber_selected = False

    for stream_catalog in catalog.get('streams'):
        stream_accessor = None

        if not _is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.get('stream')))
            continue

        # for 'subscriber' stream if it is selected, add values for 'subscriber_catalog' and
        # 'subscriber_selected', and it will replicated via 'list_subscribers' stream
        # The 'subscribers' stream is the child stream of 'list_subscribers'
        # When we sync 'list_subscribers', it makes the list of subscriber's
        # 'SubscriberKey' that were returned as part of 'list_subscribers' records
        # and pass that list to 'subscribers' stream and thus 'subscribers' stream
        # will only sync records of subscribers that are present in the list.
        # Hence, for different start dates the 'SubscriberKey' list will differ and
        # thus 'subscribers' records will also be different for different start dates.
        if SubscriberDataAccessObject.matches_catalog(stream_catalog):
            subscriber_selected = True
            subscriber_catalog = stream_catalog
            LOGGER.info("'subscriber' selected, will replicate via "
                        "'list_subscriber'")
            continue

        if ListSubscriberDataAccessObject.matches_catalog(stream_catalog):
            list_subscriber_selected = True

        for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
            if available_stream_accessor.matches_catalog(stream_catalog):
                stream_accessors.append(available_stream_accessor(
                    config, state, auth_stub, stream_catalog))

                break

    # do not replicate 'subscriber' stream without selecting 'list_subscriber' stream
    if subscriber_selected and not list_subscriber_selected:
        LOGGER.fatal('Cannot replicate `subscriber` without '
                     '`list_subscriber`. Please select `list_subscriber` '
                     'and try again.')
        sys.exit(1)

    for stream_accessor in stream_accessors:
        if isinstance(stream_accessor, ListSubscriberDataAccessObject) and \
           subscriber_selected:
            stream_accessor.replicate_subscriber = True
            stream_accessor.subscriber_catalog = subscriber_catalog

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
        sys.exit(0)
    else:
        LOGGER.info("Run failed, exiting.")
        sys.exit(1)

if __name__ == '__main__':
    main()
