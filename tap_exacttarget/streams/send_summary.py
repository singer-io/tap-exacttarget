from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class SendSummary(FullTableStream):
    """Class for SendSummary stream."""

    client: Client

    stream = "send_summary"
    tap_stream_id = "send_summary"
    object_ref = "SendSummary"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
