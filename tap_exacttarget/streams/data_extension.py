from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class DataExtension(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "data_extension"
    tap_stream_id = "data_extension"
    object_ref = "DataExtension"
    key_properties = ["ID"]

    # repl_keys = ["ModifiedDate"]
    def get_query_fields(self, *args, **kwargs):
        """Filter Query fields."""
        q_fields = self.get_available_fields()
        if "IsPlatformObject" in q_fields:
            q_fields.remove("IsPlatformObject")
        return q_fields