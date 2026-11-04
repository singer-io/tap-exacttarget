from typing import Dict, List, Tuple

import singer

from tap_exacttarget.discover_dataextensionobj import (
    DATA_EXTENSION_STREAM_PREFIX,
    discover_dao_streams,
)
from tap_exacttarget.exceptions import (
    IncompatibleFieldSelectionError,
    MarketingCloudPermissionFailure,
    MarketingCloudSoapApiException,
)
from tap_exacttarget.streams import STREAMS

LOGGER = singer.get_logger()


def _discover_dao_streams_if_selected(client, selected_ids: List[str]) -> Tuple[Dict, List[Tuple[str, str]]]:
    """Discover Data Extension streams only if at least one is selected,
    skipping the (potentially expensive) discover_dao_streams() call
    entirely otherwise. If the credentials lack access to Data Extensions,
    degrade gracefully: mark only the selected Data Extension stream(s) as
    failed instead of raising and aborting the sync for every other,
    already-accessible stream.
    """
    selected_dao_ids = [sid for sid in selected_ids if sid.startswith(DATA_EXTENSION_STREAM_PREFIX)]
    if not selected_dao_ids:
        return {}, []

    try:
        return discover_dao_streams(client=client), []
    except MarketingCloudPermissionFailure as err:
        LOGGER.warning(
            "Skipping Data Extension stream(s) %s: credentials lack access "
            "to Data Extensions. Error: '%s'",
            ", ".join(selected_dao_ids),
            str(err),
        )
        failed_streams = []
        for stream_id in selected_dao_ids:
            LOGGER.info("Stream Failed to sync %s", stream_id)
            failed_streams.append((stream_id, err))
        return {}, failed_streams


def sync(client, catalog: singer.Catalog, state: Dict):
    """Performs sync for selected streams."""
    selected_streams = list(catalog.get_selected_streams(state))
    selected_ids = [_s.tap_stream_id for _s in selected_streams]

    doa_streams, failed_streams = _discover_dao_streams_if_selected(client, selected_ids)
    STREAMS.update(doa_streams)

    sync_subscribers = False
    subscribers_obj = None

    if "subscribers" in selected_ids and "list_subscribers" in selected_ids:
        sync_subscribers = True
        for item in selected_streams:
            if item.tap_stream_id == "subscribers":
                tap_stream_id = item.tap_stream_id
                stream_schema = item.schema.to_dict()
                stream_metadata = singer.metadata.to_map(item.metadata)
                subscribers_obj = STREAMS[tap_stream_id](stream_metadata, stream_schema, client)
                singer.write_schema(
                    subscribers_obj.tap_stream_id,
                    stream_schema,
                    subscribers_obj.key_properties,
                    subscribers_obj.replication_key,
                )
    elif "subscribers" in selected_ids:
        LOGGER.info(
            "Stream Failed to sync subscribers, error: Select list_subscribers stream to enable sync for subscribers"
        )
        failed_streams.append(
            (
                "subscribers",
                "Stream Failed to sync subscribers, error: Select list_subscribers stream to enable sync for subscribers",
            )
        )
    for stream in selected_streams:
        tap_stream_id = stream.tap_stream_id
        stream_schema = stream.schema.to_dict()

        stream_metadata = singer.metadata.to_map(stream.metadata)

        if tap_stream_id == "subscribers":
            continue

        if tap_stream_id not in STREAMS:
            # Selected Data Extension stream whose discovery was skipped
            # above because the credentials lack access to Data Extensions
            # (already recorded in failed_streams); nothing else to do here.
            continue

        stream_obj = STREAMS[tap_stream_id](stream_metadata, stream_schema, client)

        if tap_stream_id == "list_subscribers":
            stream_obj.sync_subscribers = sync_subscribers
            stream_obj.subscribers_obj = subscribers_obj

        LOGGER.info("Starting sync for stream: %s", tap_stream_id)
        state = singer.set_currently_syncing(state, tap_stream_id)
        singer.write_state(state)
        singer.write_schema(
            tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key
        )
        try:
            with singer.Transformer() as transformer:
                state = stream_obj.sync(
                    state=state,
                    schema=stream_schema,
                    stream_metadata=stream_metadata,
                    transformer=transformer,
                )

        except (IncompatibleFieldSelectionError, MarketingCloudSoapApiException) as err:
            LOGGER.info("Stream Failed to sync %s", tap_stream_id)
            failed_streams.append((tap_stream_id, err))

        singer.write_state(state)

    LOGGER.info("Sync Completed, stream(s) failed: %s", len(failed_streams))
    if failed_streams:
        for stream, err_cause in failed_streams:
            LOGGER.fatal("Stream Failed %s, Reason: %s", stream, err_cause)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
