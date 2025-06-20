import json
from typing import Dict
import os
from singer import get_logger
from singer.catalog import Catalog
from .discover_dataextentionobj import discover_dao_streams
from tap_exacttarget.streams import STREAMS
from tap_exacttarget.client import Client

LOGGER = get_logger()


def get_abs_path(path: str) -> str:
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def discover(config: Dict = None, client: Client = None):
    """Performs Discovery for tap-exacttarget."""
    LOGGER.info("Starting Discovery")
    doa_streams = discover_dao_streams(client=client)
    streams = []
    for stream_name, stream in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream.tap_stream_id,
                "schema": schema,
                "metadata": stream.get_metadata(schema),
            }
        )
    for stream_name, stream in doa_streams.items():
        streams.append(
            {
                "stream": stream.stream,
                "tap_stream_id": stream.tap_stream_id,
                "schema": stream.schema,
                "metadata": stream.get_metadata(stream.schema),
            }
        )
    LOGGER.info("Discovery Completed")
    return Catalog.from_dict({"streams": streams})
