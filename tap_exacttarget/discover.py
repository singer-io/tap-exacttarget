import json
import os
from typing import Dict, Type

from requests.exceptions import HTTPError
from singer import get_logger
from singer.catalog import Catalog

from tap_exacttarget.client import Client
from tap_exacttarget.exceptions import MarketingCloudPermissionFailure
from tap_exacttarget.streams import STREAMS
from tap_exacttarget.streams.abstracts import BaseStream

from .discover_dataextensionobj import discover_dao_streams

LOGGER = get_logger()


def get_abs_path(path: str) -> str:
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def check_stream_access(client: Client, stream_name: str, stream_class: Type[BaseStream]) -> bool:
    """Probe a stream endpoint and return whether the credentials can read it.

    - Streams backed by the REST hub API (e.g. `campaigns`) are probed with a
      first-page request; a 403 response means the stream is excluded.
    - All other (SOAP) streams are probed with a minimal, unfiltered Retrieve
      request for a single known-valid field (the stream's first key
      property); a SOAP "API Permission Failed" fault means the stream is
      excluded.
    - Any other error (connectivity issues, unexpected SOAP faults/transport
      errors, generic API errors) is intentionally NOT caught here and
      propagates, aborting discovery. These don't have a well-defined
      "exclude vs. include" interpretation, so failing fast is safer than
      guessing.
    """
    try:
        endpoint = getattr(stream_class, "endpoint", None)
        if endpoint:
            client.get_rest(endpoint, params={})
        else:
            probe_field = stream_class.key_properties[0]
            client.retrieve_request(stream_class.object_ref, [probe_field])
            if stream_name == "list_subscribers":
                client.retrieve_request("LIST", ["ID"])
        return True
    except MarketingCloudPermissionFailure as err:
        LOGGER.warning(
            "Excluding unauthorized stream '%s' from catalog. Error: '%s'",
            stream_name,
            str(err),
        )
        return False
    except HTTPError as err:
        if err.response is not None and err.response.status_code == 403:
            LOGGER.warning(
                "Excluding unauthorized stream '%s' from catalog. Error: '%s'",
                stream_name,
                str(err),
            )
            return False
        raise


def _apply_access_checks(client: Client, streams: Dict[str, Type[BaseStream]]) -> Dict[str, Type[BaseStream]]:
    """Return the subset of `streams` the credentials are authorized to read.

    Child streams (those with a `parent_tap_stream_id`) are excluded whenever
    their parent is excluded, since their sync depends on the parent stream,
    in addition to being individually access-checked for their own object.
    """
    accessible: Dict[str, Type[BaseStream]] = {}
    excluded = []

    top_level = {name: cls for name, cls in streams.items() if not cls.parent_tap_stream_id}
    children = {name: cls for name, cls in streams.items() if cls.parent_tap_stream_id}

    for stream_name, stream_class in top_level.items():
        if check_stream_access(client, stream_name, stream_class):
            accessible[stream_name] = stream_class
        else:
            excluded.append(stream_name)

    for stream_name, stream_class in children.items():
        if stream_class.parent_tap_stream_id not in accessible:
            LOGGER.warning(
                "Excluding stream '%s' from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                stream_class.parent_tap_stream_id,
            )
            excluded.append(stream_name)
        elif check_stream_access(client, stream_name, stream_class):
            accessible[stream_name] = stream_class
        else:
            excluded.append(stream_name)

    if excluded:
        LOGGER.warning("Unauthorized streams excluded from catalog: %s", ", ".join(excluded))

    return accessible


def _discover_dao_streams_safe(client: Client) -> Dict:
    """Discover Data Extension streams, excluding them entirely (with a
    warning) if the credentials lack access to Data Extensions, instead of
    aborting discovery for every other stream."""
    try:
        return discover_dao_streams(client=client)
    except MarketingCloudPermissionFailure as err:
        LOGGER.warning(
            "Excluding Data Extension streams from catalog: credentials lack "
            "access to Data Extensions. Error: '%s'",
            str(err),
        )
        return {}


def discover(client: Client = None):
    """Performs Discovery for tap-exacttarget."""
    LOGGER.info("Starting Discovery")
    doa_streams = _discover_dao_streams_safe(client=client)
    accessible_streams = _apply_access_checks(client, STREAMS)

    if not accessible_streams and not doa_streams:
        raise MarketingCloudPermissionFailure(
            "The credentials do not have 'read' access to any supported streams."
        )

    streams = []
    for stream_name, stream in accessible_streams.items():
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
