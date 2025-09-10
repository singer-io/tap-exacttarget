from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class DataFolder(FullTableStream):
    """Class for collections stream."""

    client: Client

    stream = "datafolder"
    tap_stream_id = "datafolder"
    object_ref = "DataFolder"
    key_properties = ["ID"]
