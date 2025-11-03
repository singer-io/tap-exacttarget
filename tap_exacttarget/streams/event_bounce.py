from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class BounceEvent(IncrementalStream):
    """Class for Bounce Event stream."""

    client: Client

    stream = "bounceevent"
    tap_stream_id = "bounceevent"
    object_ref = "BounceEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]
