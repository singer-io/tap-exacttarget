from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class SentEvent(IncrementalStream):
    """class for collections stream."""

    client : Client

    stream = "sentevent"
    tap_stream_id = "sentevent"
    object_ref = "SentEvent"
    key_properties = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]