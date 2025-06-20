from typing import Dict

import singer

from tap_exacttarget.discover_dataextentionobj import discover_dao_streams
from tap_exacttarget.exceptions import IncompatibleFieldSelectionError
from tap_exacttarget.streams import STREAMS

LOGGER = singer.get_logger()


def sync(client, catalog: singer.Catalog, state: Dict):
    """Performs sync for selected streams."""
    doa_streams = discover_dao_streams(client=client)
    STREAMS.update(doa_streams)
    failed_streams = []
    selected_streams = list(catalog.get_selected_streams(state))

    selected_ids = [_s.tap_stream_id for _s in selected_streams]
    sync_subscribers = False
    subscribers_obj = None

    if "subscribers" in selected_ids:
        if "list_subscribers" in selected_ids:
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
        else:
            LOGGER.info(
                "Stream Failed to sync subscribers, error: Select list_subscribers stream to enable sync for subscribers"
            )
            failed_streams.append(
                "subscribers",
                "Stream Failed to sync subscribers, error: Select list_subscribers stream to enable sync for subscribers",
            )

    with singer.Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()

            stream_metadata = singer.metadata.to_map(stream.metadata)

            if tap_stream_id == "subscribers":
                continue
            stream_obj = STREAMS[tap_stream_id](stream_metadata, stream_schema, client)

            if tap_stream_id == "list_subscribers":
                stream_obj.sync_subscribers = sync_subscribers
                stream_obj.subscribers_obj = subscribers_obj

            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)
            singer.write_schema(tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key)
            try:
                state = stream_obj.sync(
                    state=state, schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer
                )

            except IncompatibleFieldSelectionError as err:
                LOGGER.info("Stream Failed to sync %s, error: %s", tap_stream_id, err)
                failed_streams.append((tap_stream_id, err))

            singer.write_state(state)
    if failed_streams:
        for stream, err_cause in failed_streams:
            LOGGER.fatal("Stream Failed %s, Reason: %s", stream, err_cause)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
