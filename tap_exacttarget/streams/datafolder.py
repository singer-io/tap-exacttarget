from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class DataFolder(IncrementalStream):
    """Class for datafolder stream."""

    client: Client

    stream = "datafolder"
    tap_stream_id = "datafolder"
    object_ref = "DataFolder"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
    config_start_key = "start_date"
