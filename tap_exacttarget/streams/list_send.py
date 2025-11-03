from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class ListSend(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "list_send"
    tap_stream_id = "list_send"
    object_ref = "ListSend"
    key_properties = ["SendID", "ListID"]

    def transform_record(self, obj: Dict):
        obj = super().transform_record(obj)
        obj["ListID"] = obj.get("List", {}).get("ID")
        return obj
