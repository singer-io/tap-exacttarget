from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class Email(IncrementalStream):
    """class for collections stream."""

    client: Client

    stream = "email"
    tap_stream_id = "email"
    object_ref = "Email"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]

    def transform_record(self, obj):
        obj = super().transform_record(obj)

        content_area_ids = []

        for content_area in obj.get("ContentAreas", []):
            content_area_ids.append(content_area.get("ID"))

        obj["EmailID"] = obj.get("Email", {}).get("ID")
        obj["ContentAreaIDs"] = content_area_ids

        return obj
