from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class UnsubEvent(IncrementalStream):
    """Class for Unsub Event stream."""

    client: Client

    stream = "unsubevent"
    tap_stream_id = "unsubevent"
    object_ref = "UnsubEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]
