from datetime import datetime
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream, strptime_to_cst, fixed_cst
from singer import get_logger, write_record

LOGGER = get_logger()


class BounceEvent(IncrementalStream):
    """Class for Bounce Event stream."""

    client: Client

    stream = "bounceevent"
    tap_stream_id = "bounceevent"
    object_ref = "BounceEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]

    def transform_record(self, obj):
        obj = super().transform_record(obj)
        if obj['SubscriberKey'] is not None:
            return obj
