from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class ContentArea(IncrementalStream):
    """Class for collections stream."""

    client: Client

    stream = "content_area"
    tap_stream_id = "content_area"
    object_ref = "ContentArea"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
