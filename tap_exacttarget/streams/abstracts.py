import json
from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Tuple

import dateutil.parser
from singer import Transformer, get_bookmark, get_logger, write_bookmark, write_record
from singer.metadata import get_standard_metadata, to_list, to_map, write
from singer.transform import SchemaMismatch
from singer.utils import now
from zeep.helpers import serialize_object

LOGGER = get_logger()


# https://help.salesforce.com/s/articleView?id=mktg.mc_server_timezone_changes.htm&type=5
# Must use -6 to avoid DST impact
fixed_cst = timezone(timedelta(hours=-6))


def strptime_to_cst(dtimestr):
    """Converts a datetime string to a datetime object in (CST)."""
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=fixed_cst)
    else:
        return d_object.astimezone(tz=fixed_cst)


class CustomDTParser(json.JSONEncoder):
    """Handles parsing for datetime, date, and time objects."""

    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, time):
            return o.isoformat()
        return super().default(o)


class BaseStream(ABC):
    """Base stream class."""

    schema = None

    @property
    @abstractmethod
    def stream(self) -> str:
        """Display Name of the stream."""

    @property
    @abstractmethod
    def object_ref(self) -> str:
        """Property required for interacting with soap api."""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in order to allow for sources
        that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """Defines the replication key for incremental sync mode of a stream."""

    @property
    @abstractmethod
    def valid_replication_keys(self) -> Tuple[str, str]:
        """Defines the replication key for incremental sync mode of a stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the default replication strategy of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    @abstractmethod
    def parent_tap_stream_id(self) -> str:
        """Indicates the parent of the stream."""
        return ""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if stream is selected by default."""
        return False

    @abstractmethod
    def get_records(self, *args, **kwargs):
        """Interacts with api client interaction and pagination."""

    @abstractmethod
    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Performs sync for the stream."""

    def __init__(self, metadata, schema, client) -> None:
        self.client = client
        self.metadata = metadata
        self.schema = schema
        self.stream_metadata = metadata.get(()) or {}

    def get_available_fields(self):
        """Provides selectable fields for each stream."""
        is_retrievable = []
        obj_defs = self.client.describe_request(self.object_ref)
        schema = obj_defs["ObjectDefinition"][0]
        for prop in schema["Properties"]:
            if prop["IsRetrievable"]:
                is_retrievable.append(prop["Name"])
        return is_retrievable

    def get_query_fields(self, *args, **kwargs):
        """Filter Query fields."""
        return self.get_available_fields()

    def transform_record(self, obj):
        """serialize_object Converts Soap class obj to python repr json.dumps converts to string
        json.loads converts to dictionary which can be used by transformer."""
        return json.loads(json.dumps(serialize_object(obj), cls=CustomDTParser))

    @classmethod
    def get_metadata(cls, schema) -> Dict[str, str]:
        """Returns a `dict` for generating stream metadata."""
        stream_metadata = get_standard_metadata(
            **{
                "schema": schema,
                "key_properties": cls.key_properties,
                "valid_replication_keys": cls.valid_replication_keys,
                "replication_method": cls.replication_method or cls.forced_replication_method,
            }
        )
        stream_metadata = to_map(stream_metadata)

        if cls.parent_tap_stream_id:
            stream_metadata = write(
                stream_metadata, (), "parent-tap-stream-id", cls.parent_tap_stream_id
            )

        if cls.valid_replication_keys:
            for key in cls.valid_replication_keys:
                stream_metadata = write(
                    stream_metadata, ("properties", key), "inclusion", "automatic"
                )
        stream_metadata = to_list(stream_metadata)
        return stream_metadata


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    parent_tap_stream_id = None

    def __init__(self, metadata, schema, client):
        super().__init__(metadata, schema, client)
        self.replication_key = self.stream_metadata.get("replication-key") or self.replication_key

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark."""
        return get_bookmark(
            state,
            self.tap_stream_id,
            key or self.replication_key,
            self.client.config.get("start_date", False),
        )

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.write_bookmark."""
        return write_bookmark(state, self.tap_stream_id, key or self.replication_key, value)

    def create_date_windows(self, start_date, end_date, interval_days) -> List:
        """Creates Date Window for given date range."""
        if isinstance(start_date, str):
            start_date = strptime_to_cst(start_date)
        if isinstance(end_date, str):
            end_date = strptime_to_cst(end_date)

        if start_date >= end_date:
            return [(end_date, start_date)]
        if interval_days <= 0:
            raise ValueError("date_window value must be positive")

        export_batches = []
        current_start = start_date
        interval_delta = timedelta(days=interval_days)

        while current_start < end_date:
            current_end = current_start + interval_delta
            current_end = end_date if current_end > end_date else current_end
            export_batches.append((current_start, current_end))
            current_start = current_end

            if current_start >= end_date:
                break
        return export_batches

    def get_records(self, start_date, stream_metadata, schema):
        """Performs Pagination and query building."""

        query_fields = self.get_query_fields(stream_metadata, schema)
        for start_dt, end_dt in self.create_date_windows(
            start_date, now().astimezone(tz=fixed_cst), self.client.date_window
        ):
            start_date = self.client.create_simple_filter(
                self.replication_key, "greaterThanOrEqual", date_value=start_dt
            )
            end_date = self.client.create_simple_filter(
                self.replication_key, "lessThanOrEqual", date_value=end_dt
            )
            date_range = self.client.create_complex_filter(start_date, "AND", end_date)

            next_page = True
            request_id = None
            while next_page:

                response = self.client.retrieve_request(
                    self.object_ref, query_fields, request_id=request_id, search_filter=date_range
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
                    yield self.transform_record(rec)

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Sync implementation for incremental streams."""

        current_max_bookmark_date = bookmark_date_utc = strptime_to_cst(self.get_bookmark(state))
        records_processed, schema_mismatch_count = 0, 0

        for record in self.get_records(bookmark_date_utc, stream_metadata, schema):
            record_timestamp = None
            try:
                if isinstance(record[self.replication_key], datetime):
                    record_timestamp = record[self.replication_key].replace(tzinfo=fixed_cst)
                elif record[self.replication_key]:
                    record_timestamp = strptime_to_cst(record[self.replication_key])
                # Add specific handling for schema mismatch errors
                try:
                    transformed_record = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed_record)
                    records_processed += 1
                except SchemaMismatch as ex:
                    schema_mismatch_count += 1
                    LOGGER.warning(
                        "Schema mismatch for record in stream %s: %s", self.tap_stream_id, str(ex)
                    )
                    continue
            except KeyError as err:
                LOGGER.info("Missing replication key value skipping %s", err)

            if record_timestamp:
                current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)

        LOGGER.info(
            "Stream %s sync complete: %d records processed, %d schema mismatch errors",
            self.tap_stream_id,
            records_processed,
            schema_mismatch_count,
        )

        state = self.write_bookmark(state, value=current_max_bookmark_date.isoformat())
        return state


class FullTableStream(BaseStream):
    """Base Class for Fulltable Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None
    parent_tap_stream_id = None

    def get_records(self, stream_metadata, schema):
        """Performs Pagination and querybuilding."""

        query_fields = self.get_query_fields(stream_metadata, schema)
        next_page, request_id = True, None

        while next_page:
            response = self.client.retrieve_request(
                self.object_ref, query_fields, request_id=request_id
            )
            raw_records, request_id = response["Results"], response["RequestID"]

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
                yield self.transform_record(rec)

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Generic Sync implementation for Fulltable streams."""
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
