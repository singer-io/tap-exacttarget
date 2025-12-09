from datetime import datetime
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream, strptime_to_cst, fixed_cst
from singer import get_logger, write_record

LOGGER = get_logger()


class OpenEvent(IncrementalStream):
    """Class for Open Event stream."""

    client: Client

    stream = "openevent"
    tap_stream_id = "openevent"
    object_ref = "OpenEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]

    def sync(self, state, schema, stream_metadata, transformer):
        """Sync implementation for incremental streams."""

        current_max_bookmark_date = bookmark_date_utc = strptime_to_cst(self.get_bookmark(state))
        records_processed = 0

        for record in self.get_records(bookmark_date_utc, stream_metadata, schema):
            if record['SubscriberKey'] is None:
                continue

            record_timestamp = None

            if isinstance(record[self.replication_key], datetime):
                record_timestamp = record[self.replication_key].replace(tzinfo=fixed_cst)

            if record[self.replication_key]:
                record_timestamp = strptime_to_cst(record[self.replication_key])
                record[self.replication_key] = record_timestamp.isoformat()
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                records_processed += 1

            if record_timestamp:
                current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)

        LOGGER.info("Stream %s sync complete: %d records processed", self.tap_stream_id, records_processed)

        state = self.write_bookmark(state, value=current_max_bookmark_date.isoformat(timespec='microseconds'))
        return state
