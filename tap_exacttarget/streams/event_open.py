from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class OpenEvent(IncrementalStream):
    """Class for Open Event stream."""

    client: Client

    stream = "openevent"
    tap_stream_id = "openevent"
    object_ref = "OpenEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]
