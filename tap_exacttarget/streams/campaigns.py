from typing import Dict

from singer import Transformer, get_logger, write_record

from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Campaigns(FullTableStream):
    """Class for Campaigns stream."""

    client: Client

    stream = "campaigns"
    tap_stream_id = "campaigns"
    object_ref = "Campaigns"
    key_properties = ["ID"]
    endpoint = "hub/v1/campaigns/"

    def get_records(self, stream_metadata, schema):

        next_page = True
        pagesize = 2
        params = {}
        while next_page:

            response = self.client.get_rest(self.endpoint, params=params)
            raw_records = response["items"]
            page_num = response["page"]

            if len(raw_records) < pagesize:
                next_page = False

            yield from raw_records
            params["$page"] = page_num + 1

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""

        for record in self.get_records(stream_metadata, schema):
            transformed_record = transformer.transform(record, schema, stream_metadata)
            write_record(self.tap_stream_id, transformed_record)
        return state
