from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List
from datetime import timedelta, datetime, timezone, date
import json
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    write_bookmark,
    write_record,
)
from singer.metadata import get_standard_metadata, to_list, to_map, write
from singer.utils import strftime, strptime_to_utc, now
from zeep.helpers import serialize_object

LOGGER = get_logger()


class CustomDTParser(json.JSONEncoder):
    """
    handles parsing for datetime objects
    """

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    schema = None

    @property
    @abstractmethod
    def stream(self) -> str:
        """Display Name of the stream."""

    @property
    @abstractmethod
    def object_ref(self) -> str:
        """Property required for interacting with fuelsdk"""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def valid_replication_keys(self) -> Tuple[str, str]:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if a node in the schema should be replicated, if a user
        has not expressed any opinion on whether or not to replicate it."""
        return False

    @abstractmethod
    def get_records(self, *args, **kwargs):
        """Interacts with api client interaction and pagination."""

    @abstractmethod
    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - schema (dict): Schema of the stream
         - transformer (object): A Object of the singer.transformer class.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#replication-method
        """

    def __init__(self, metadata, schema, client) -> None:
        self.client = client
        self.metadata = metadata
        self.schema = schema
        self.stream_metadata = metadata.get(()) or {}

    def get_available_fields(self):
        """
        provides selectable fields for each stream
        """
        is_retrievable = []
        obj_defs = self.client.describe_request(self.object_ref)
        schema = obj_defs["ObjectDefinition"][0]
        for prop in schema["Properties"]:
            if prop["IsRetrievable"]:
                is_retrievable.append(prop["Name"])
        return is_retrievable

    def get_query_fields(self, *args, **kwargs):
        """
        Filter Query fields
        """
        return self.get_available_fields()

    def transform_record(self, obj):
        """
        serialize_object Converts Soap class obj to python repr
        json.dumps converts to string
        json.loads converts to dictionary which can be used by transformer
        """
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
        if cls.valid_replication_keys is not None:
            for key in cls.valid_replication_keys:
                stream_metadata = write(stream_metadata, ("properties", key), "inclusion", "automatic")
        stream_metadata = to_list(stream_metadata)
        return stream_metadata


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"

    def __init__(self, metadata, schema, client):
        super().__init__(metadata, schema, client)
        self.replication_key = self.stream_metadata.get("replication-key") or self.replication_key

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark"""
        return get_bookmark(
            state, self.tap_stream_id, key or self.replication_key, self.client.config.get("start_date", False)
        )

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.write_bookmark"""
        return write_bookmark(state, self.tap_stream_id, key or self.replication_key, value)

    def get_records(self, start_date, stream_metadata, schema):
        """Performs Pagination and querybuilding."""

        query_fields = self.get_query_fields(stream_metadata, schema)

        for start_dt, end_dt in self.create_date_windows(start_date, now(), self.client.date_window):
            s_filter = self.client.create_simple_filter(self.replication_key, "greaterThanOrEqual", date_value=start_dt)
            t_filter = self.client.create_simple_filter(self.replication_key, "lessThanOrEqual", date_value=end_dt)
            c_filter = self.client.create_complex_filter(s_filter, "AND", t_filter)

            next_page = True
            request_id = None
            while next_page:

                response = self.client.retrieve_request(
                    self.object_ref, query_fields, request_id=request_id, search_filter=c_filter
                )
                raw_records = response["Results"]
                request_id = response["RequestID"]

                if response["OverallStatus"] != "MoreDataAvailable":
                    if "Error" in response["OverallStatus"]:
                        LOGGER.info("Req Failed: %s %s %s", self.object_ref, query_fields, response["OverallStatus"])
                    next_page = False

                for rec in raw_records:
                    yield self.transform_record(rec)

    def create_date_windows(self, start_date, end_date, interval_days) -> List:
        """Creates Date Window for given date range"""
        if isinstance(start_date, str):
            start_date = strptime_to_utc(start_date)
        if isinstance(end_date, str):
            end_date = strptime_to_utc(end_date)

        if start_date >= end_date:
            return [end_date, start_date]
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

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Generic Sync impl. for incremental streams"""

        current_max_bookmark_date = bookmark_date_utc = strptime_to_utc(self.get_bookmark(state))

        for record in self.get_records(bookmark_date_utc, stream_metadata, schema):
            record_timestamp = None
            try:
                if isinstance(record[self.replication_key], datetime):
                    record_timestamp = record[self.replication_key].replace(tzinfo=timezone.utc)
                elif record[self.replication_key]:
                    record_timestamp = strptime_to_utc(record[self.replication_key])

                write_record(self.tap_stream_id, transformer.transform(record, schema, stream_metadata))
            except KeyError as _:
                LOGGER.info("%s Record did not have replication key value skipping", record)

            if record_timestamp:
                current_max_bookmark_date = max(current_max_bookmark_date, record_timestamp)
            state = self.write_bookmark(state, value=strftime(current_max_bookmark_date))
        return state


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None

    def get_records(self, stream_metadata, schema):
        """Performs Pagination and querybuilding."""

        query_fields = self.get_query_fields(stream_metadata, schema)
        next_page, request_id = True, None

        while next_page:
            response = self.client.retrieve_request(self.object_ref, query_fields, request_id=request_id)
            raw_records, request_id = response["Results"], response["RequestID"]

            if response["OverallStatus"] != "MoreDataAvailable":
                if "Error" in response["OverallStatus"]:
                    LOGGER.info("Req Failed: %s %s %s", self.object_ref, query_fields, response["OverallStatus"])
                next_page = False

            for rec in raw_records:
                yield self.transform_record(rec)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Generic Sync impl. for incremental streams"""

        for record in self.get_records(stream_metadata, schema):
            transformed_record = transformer.transform(record, schema, stream_metadata)
            write_record(self.tap_stream_id, transformed_record)
        return state
