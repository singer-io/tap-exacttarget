from singer import Transformer, get_logger, write_record

from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Subscribers(FullTableStream):
    """Class for collections stream."""

    client: Client

    stream = "subscribers"
    tap_stream_id = "subscribers"
    object_ref = "Subscriber"
    key_properties = ["ID"]

    def transform_record(self, obj):
        obj = super().transform_record(obj)

        if obj["Lists"]:
            obj["ListIDs"] = [_list.get("ObjectID") for _list in obj.get("Lists", [])]
        return obj

    def filter_records(self, parent_id_list):
        """Queries for records."""
        query_fields = self.get_query_fields(self.metadata, self.schema)

        if len(parent_id_list) == 1:
            search_filter = self.client.create_simple_filter("SubscriberKey", "equals", parent_id_list[0])
        else:
            search_filter = self.client.create_simple_filter("SubscriberKey", "IN", parent_id_list)

        next_page = True
        request_id = None

        while next_page:

            response = self.client.retrieve_request(
                self.object_ref, query_fields, request_id=request_id, search_filter=search_filter
            )
            raw_records = response["Results"]
            request_id = response["RequestID"]

            if response["OverallStatus"] != "MoreDataAvailable":
                if "Error" in response["OverallStatus"]:
                    LOGGER.info("Req Failed: %s %s %s", self.object_ref, query_fields, response["OverallStatus"])
                next_page = False

            for rec in raw_records:
                yield self.transform_record(rec)

    def sync_ids(self, subs_key_list):
        transformer = Transformer()
        for record in self.filter_records(subs_key_list):
            transformed_record = transformer.transform(record, self.schema, self.metadata)
            write_record(self.tap_stream_id, transformed_record)

    def sync(self, state, schema, stream_metadata, transformer):
        return state
