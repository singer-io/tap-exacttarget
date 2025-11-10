from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class Automation(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "automation"
    tap_stream_id = "automation"
    object_ref = "Automation"
    key_properties = ["ID"]
    # repl_keys = ["ModifiedDate"]
