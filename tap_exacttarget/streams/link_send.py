from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class LinkSend(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "link_send"
    tap_stream_id = "link_send"
    object_ref = "LinkSend"
    key_properties = ["ID"]
    # repl_keys = ["ModifiedDate"]
