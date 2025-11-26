from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class LinkSend(FullTableStream):
    """Class for Link Send stream."""
    # https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/linksend.html

    client: Client

    stream = "link_send"
    tap_stream_id = "link_send"
    object_ref = "LinkSend"
    key_properties = ["ID"]
    # replication_key = "ModifiedDate"
    # valid_replication_keys = ["ModifiedDate"]
