from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class ETList(IncrementalStream):
    """Class for collections stream."""

    client: Client

    stream = "list"
    tap_stream_id = "list"
    object_ref = "List"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
