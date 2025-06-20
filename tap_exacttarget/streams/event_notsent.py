from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class NotSentEvent(IncrementalStream):
    """class for collections stream."""

    client : Client

    stream = "notsentevent"
    tap_stream_id = "notsentevent"
    object_ref = "NotSentEvent"
    key_properties = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]