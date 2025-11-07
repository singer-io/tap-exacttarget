from typing import Dict

from singer import Transformer, get_logger, write_record
from singer.transform import SchemaMismatch
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Campaigns(FullTableStream):
    """Class for Campaigns stream."""

    client: Client

    stream = "campaigns"
    tap_stream_id = "campaigns"
    object_ref = "Campaigns"
    key_properties = ["id"]
    endpoint = "hub/v1/campaigns/"

    def get_records(self, stream_metadata: Dict, schema: Dict):

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

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        records_processed, schema_mismatch_count = 0, 0
        for record in self.get_records(stream_metadata, schema):
            try:
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                records_processed += 1
            except SchemaMismatch as ex:
                schema_mismatch_count += 1
                LOGGER.warning(
                    "Schema mismatch for record in stream %s: %s", self.tap_stream_id, str(ex)
                )

        LOGGER.info(
            "Stream %s sync complete: %d records processed, %d schema mismatch errors",
            self.tap_stream_id,
            records_processed,
            schema_mismatch_count,
        )
        return state
