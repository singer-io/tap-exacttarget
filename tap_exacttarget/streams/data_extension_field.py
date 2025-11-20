from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class DataExtensionField(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "data_extension_field"
    tap_stream_id = "data_extension_field"
    object_ref = "DataExtensionField"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
