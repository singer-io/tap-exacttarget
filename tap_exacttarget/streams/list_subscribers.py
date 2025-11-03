from typing import Dict
from itertools import islice

from singer import get_logger
from singer.utils import now

from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream

from .subscribers import Subscribers

LOGGER = get_logger()


class ListSubscribe(IncrementalStream):
    """Class for List Subscribers stream."""

    client: Client

    stream = "list_subscribers"
    tap_stream_id = "list_subscribers"
    object_ref = "ListSubscriber"
    key_properties = ["SubscriberKey", "ListID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
    sync_subscribers = False
    subscribers_obj: Subscribers = None

    def fetch_subscribers_batch(self, subs_ids):
        """Creates Batch of 100 to fetch subscriber profile."""
        subs_ids = iter(subs_ids)
        while True:
            chunk = list(islice(subs_ids, 100))
            if not chunk:
                break
            if self.subscribers_obj:
                LOGGER.info("fetching batch with %s rec", len(chunk))
                self.subscribers_obj.sync_ids(chunk)

    def get_list_id(self):
        """Returns the ID of the "All Subscribers" List."""
        all_sub_filter = self.client.create_simple_filter("ListName", "equals", "All Subscribers")
        response = self.client.retrieve_request(
            "LIST", ["ID", "ListName"], search_filter=all_sub_filter
        )
        if len(response["Results"]) > 1:
            raise RuntimeError("Unexpected Number of All Subscribers list ")
        return response["Results"][0]["ID"]

    def get_records(self, start_date, stream_metadata: Dict, schema: Dict):
        """Performs get / retrieve for ListSubscribers."""

        query_fields = self.get_query_fields(stream_metadata, schema)
        list_id = self.get_list_id()
        sub_id_filter = self.client.create_simple_filter("ListID", "equals", list_id)
        subscriber_keys = []

        for start_dt, end_dt in self.create_date_windows(
            start_date, now(), self.client.date_window
        ):
            subscriber_keys = []
            s_filter = self.client.create_simple_filter(
                self.replication_key, "greaterThanOrEqual", date_value=start_dt
            )
            t_filter = self.client.create_simple_filter(
                self.replication_key, "lessThanOrEqual", date_value=end_dt
            )
            c_filter = self.client.create_complex_filter(s_filter, "AND", t_filter)
            final_filter = self.client.create_complex_filter(c_filter, "AND", sub_id_filter)

            next_page = True
            request_id = None
            while next_page:

                response = self.client.retrieve_request(
                    self.object_ref, query_fields, request_id=request_id, search_filter=final_filter
                )
                raw_records = response["Results"]
                request_id = response["RequestID"]

                if response["OverallStatus"] != "MoreDataAvailable":
                    if "Error" in response["OverallStatus"]:
                        LOGGER.info(
                            "Req Failed: %s %s %s",
                            self.object_ref,
                            query_fields,
                            response["OverallStatus"],
                        )
                    next_page = False

                for rec in raw_records:
                    trx_rec = self.transform_record(rec)
                    subscriber_keys.append(trx_rec["SubscriberKey"])
                    yield trx_rec
                if subscriber_keys:
                    self.fetch_subscribers_batch(subscriber_keys)
