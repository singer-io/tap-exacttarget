from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class AutomationInstance(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "automation_instance"
    tap_stream_id = "automation_instance"
    object_ref = "AutomationInstance"
    key_properties = ["ID"]
    # repl_keys = ["ModifiedDate"]
