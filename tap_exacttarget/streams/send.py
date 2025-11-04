from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class Sends(IncrementalStream):
    """Class for Sends stream."""

    client: Client

    stream = "send"
    tap_stream_id = "send"
    object_ref = "Send"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
