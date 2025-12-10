from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class ClickEvent(IncrementalStream):
    """Class for Click Event stream."""

    client: Client

    stream = "clickevent"
    tap_stream_id = "clickevent"
    object_ref = "ClickEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]

    def transform_record(self, obj):
        obj = super().transform_record(obj)
        if obj['SubscriberKey'] is not None:
            return obj
