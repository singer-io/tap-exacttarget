from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream
from singer import get_logger

LOGGER = get_logger()


class DataExtension(IncrementalStream):
    """Class for List Send stream."""

    client: Client

    stream = "data_extension"
    tap_stream_id = "data_extension"
    object_ref = "DataExtension"
    key_properties = ["CustomerKey"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]

    def get_query_fields(self, *args, **kwargs):
        """Filter Query fields."""
        q_fields = self.get_available_fields()
        if "IsPlatformObject" in q_fields:
            q_fields.remove("IsPlatformObject")
        LOGGER.info("Objtype: %s fields: %s", self.object_ref, q_fields)
        return q_fields

    def transform_record(self, obj):
        obj = super().transform_record(obj)
        obj['Template'] = obj.get('Template', {}).get('CustomerKey')
        obj['SendableDataExtensionField'] = obj.get('SendableDataExtensionField', {}).get('Name')
        obj['SendableSubscriberField'] = obj.get('SendableSubscriberField', {}).get('Name')

        return obj
