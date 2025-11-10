from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class Link(FullTableStream):
    """Class for List stream."""

    client: Client

    stream = "link"
    tap_stream_id = "link"
    object_ref = "Link"
    key_properties = ["ID"]
    # repl_keys = ["ModifiedDate"]
