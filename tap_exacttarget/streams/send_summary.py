from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class SendSummary(IncrementalStream):
    """Class for SendSummary stream."""

    # https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/sendsummary.html
    # Using CreatedDate, unable to find any record i.e updated
    # document suggests retrieve-only object which may never be updated

    client: Client

    stream = "send_summary"
    tap_stream_id = "send_summary"
    object_ref = "SendSummary"
    key_properties = ["SendID"]
    replication_key = "CreatedDate"
    valid_replication_keys = ["CreatedDate"]
