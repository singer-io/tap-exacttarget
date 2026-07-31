import runpy
import importlib
from datetime import datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import tap_exacttarget
from singer.transform import SchemaMismatch

from tap_exacttarget.client import Client
from tap_exacttarget.discover import discover
from tap_exacttarget.exceptions import MarketingCloudError, MarketingCloudSoapApiException
from tap_exacttarget.streams.abstracts import (
    BaseStream,
    FullTableStream,
    IncrementalStream,
    fixed_cst,
    strptime_to_cst,
)
from tap_exacttarget.streams import STREAMS
from tap_exacttarget.streams.campaigns import Campaigns
from tap_exacttarget.streams.datafolder import DataFolder
from tap_exacttarget.streams.email import Email
from tap_exacttarget.streams.event_bounce import BounceEvent
from tap_exacttarget.streams.event_click import ClickEvent
from tap_exacttarget.streams.event_notsent import NotSentEvent
from tap_exacttarget.streams.event_open import OpenEvent
from tap_exacttarget.streams.event_sent import SentEvent
from tap_exacttarget.streams.event_unsub import UnsubEvent
from tap_exacttarget.streams.list_send import ListSend
from tap_exacttarget.streams.list_subscribers import ListSubscribers
from tap_exacttarget.streams.send import Sends
from tap_exacttarget.streams.subscriber import Subscribers


sync_module = importlib.import_module("tap_exacttarget.sync")


def _metadata(extra=None):
    return {(): extra or {}}


class ProbeIncrementalStream(IncrementalStream):
    stream = "probe_incremental"
    tap_stream_id = "probe_incremental"
    object_ref = "ProbeIncremental"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]

    def get_records(self, *args, **kwargs):
        return []

    def sync(self, *args, **kwargs):
        return {}


class ProbeIncrementalWithParent(ProbeIncrementalStream):
    parent_tap_stream_id = "parent_stream"


class ProbePropertyStream(ProbeIncrementalStream):
    @property
    def parent_tap_stream_id(self):
        return super().parent_tap_stream_id

    @property
    def selected_by_default(self):
        return super().selected_by_default


class ProbeFullTableStream(FullTableStream):
    stream = "probe_full"
    tap_stream_id = "probe_full"
    object_ref = "ProbeFull"
    key_properties = ["ID"]

    def get_records(self, *args, **kwargs):
        return []

    def sync(self, *args, **kwargs):
        return {}


class TestEntryPointsAndDiscovery(TestCase):
    @patch("singer.utils.parse_args")
    @patch("tap_exacttarget.client.Client")
    @patch("tap_exacttarget.discover.discover")
    @patch("tap_exacttarget.sync.sync")
    def test_module_main_block_executes(self, mock_sync, mock_discover, mock_client, mock_parse_args):
        mock_parse_args.return_value = SimpleNamespace(
            config={"tenant_subdomain": "t", "client_id": "c", "client_secret": "s", "start_date": "2024-01-01T00:00:00Z"},
            discover=True,
            catalog=None,
            state=None,
        )
        mock_discover.return_value = MagicMock()

        runpy.run_path(tap_exacttarget.__file__, run_name="__main__")

        self.assertTrue(mock_client.called)
        self.assertTrue(mock_discover.called)
        self.assertTrue(mock_discover.return_value.dump.called)

    @patch("tap_exacttarget.utils.parse_args")
    @patch("tap_exacttarget.Client")
    @patch("tap_exacttarget.discover")
    @patch("tap_exacttarget.sync")
    def test_main_dispatches_to_discover(self, mock_sync, mock_discover, mock_client, mock_parse_args):
        args = SimpleNamespace(
            config={"tenant_subdomain": "t", "client_id": "c", "client_secret": "s", "start_date": "2024-01-01T00:00:00Z"},
            discover=True,
            catalog=None,
            state=None,
        )
        mock_parse_args.return_value = args
        mock_discover.return_value = MagicMock()

        tap_exacttarget.main()

        mock_client.assert_called_once_with(args.config)
        mock_discover.assert_called_once()
        mock_discover.return_value.dump.assert_called_once()
        mock_sync.assert_not_called()

    @patch("tap_exacttarget.utils.parse_args")
    @patch("tap_exacttarget.Client")
    @patch("tap_exacttarget.discover")
    @patch("tap_exacttarget.sync")
    def test_main_dispatches_to_sync(self, mock_sync, mock_discover, mock_client, mock_parse_args):
        args = SimpleNamespace(
            config={"tenant_subdomain": "t", "client_id": "c", "client_secret": "s", "start_date": "2024-01-01T00:00:00Z"},
            discover=False,
            catalog=object(),
            state={"bookmarks": {}},
        )
        mock_parse_args.return_value = args

        tap_exacttarget.main()

        mock_client.assert_called_once_with(args.config)
        mock_discover.assert_not_called()
        mock_sync.assert_called_once_with(mock_client.return_value, args.catalog, args.state)

    @patch("tap_exacttarget.discover.discover_dao_streams", return_value={})
    def test_discover_builds_catalog_from_schema_files(self, mock_discover_dao_streams):
        catalog = discover(client=None)

        self.assertEqual(len(catalog.streams), len(STREAMS))
        mock_discover_dao_streams.assert_called_once_with(client=None)

    @patch("tap_exacttarget.discover.discover_dao_streams")
    def test_discover_includes_dynamic_streams(self, mock_discover_dao_streams):
        dynamic_stream = SimpleNamespace(
            stream="dynamic_stream",
            tap_stream_id="dynamic_stream",
            schema={"properties": {}},
            get_metadata=lambda schema: [],
        )
        mock_discover_dao_streams.return_value = {"dynamic_stream": dynamic_stream}

        catalog = discover(client=None)

        self.assertTrue(any(stream.tap_stream_id == "dynamic_stream" for stream in catalog.streams))

    def test_client_describe_request(self):
        from .base_test import BaseClientTest

        base = BaseClientTest()
        base.setUp()
        try:
            response = base.client_instance.describe_request("Subscriber")

            self.assertIsNotNone(response)
            base.mock_soap_client.service.Describe.assert_called_once()
        finally:
            base.tearDown()

    def test_client_raise_for_error_generic_branch(self):
        client = Client.__new__(Client)

        with self.assertRaises(Exception):
            Client.raise_for_error(client, {"OverallStatus": "Error: Something unexpected"})


class TestAbstractStreamHelpers(TestCase):
    def setUp(self):
        self.client = MagicMock(
            config={"start_date": "2024-01-01T00:00:00Z"},
        )

    def test_base_property_defaults(self):
        stream = ProbePropertyStream(_metadata(), {"properties": {}}, self.client)

        self.assertEqual(BaseStream.parent_tap_stream_id.fget(stream), "")
        self.assertFalse(BaseStream.selected_by_default.fget(stream))

    def test_available_fields_and_query_fields(self):
        self.client.describe_request.return_value = {
            "ObjectDefinition": [
                {
                    "Properties": [
                        {"Name": "A", "IsRetrievable": True},
                        {"Name": "B", "IsRetrievable": False},
                    ]
                }
            ]
        }
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)

        self.assertEqual(stream.get_available_fields(), ["A"])
        self.assertEqual(stream.get_query_fields(), ["A"])

    def test_transform_record_serializes_datetime_values(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        payload = {
            "Top": datetime(2024, 1, 1, 12, 30),
            "Nested": {"At": datetime(2024, 1, 2, 8, 15)},
        }

        result = stream.transform_record(payload)

        self.assertEqual(result["Top"], "2024-01-01T12:30:00")
        self.assertEqual(result["Nested"]["At"], "2024-01-02T08:15:00")

    def test_get_metadata_includes_parent_and_replication_key(self):
        schema = {"properties": {"ModifiedDate": {"type": ["null", "string"]}}}
        metadata = ProbeIncrementalWithParent.get_metadata(schema)

        self.assertTrue(metadata)
        self.assertIsInstance(metadata, list)

    def test_create_date_windows_handles_strings_and_invalid_ranges(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)

        windows = stream.create_date_windows(
            "2024-01-01T00:00:00Z", "2024-01-05T00:00:00Z", 2
        )
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][0], strptime_to_cst("2024-01-01T00:00:00Z"))
        self.assertEqual(windows[1][1], strptime_to_cst("2024-01-05T00:00:00Z"))

        reversed_window = stream.create_date_windows(
            "2024-01-05T00:00:00Z", "2024-01-01T00:00:00Z", 2
        )
        self.assertEqual(
            reversed_window,
            [(strptime_to_cst("2024-01-01T00:00:00Z"), strptime_to_cst("2024-01-05T00:00:00Z"))],
        )

        with self.assertRaises(ValueError):
            stream.create_date_windows("2024-01-01T00:00:00Z", "2024-01-05T00:00:00Z", 0)

    def test_bookmark_helpers_delegate_to_singer(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        state = {}

        with patch("tap_exacttarget.streams.abstracts.get_bookmark", return_value="2024-01-01T00:00:00Z") as mock_get_bookmark, patch(
            "tap_exacttarget.streams.abstracts.write_bookmark", return_value={"bookmark": True}
        ) as mock_write_bookmark:
            self.assertEqual(IncrementalStream.get_bookmark(stream, state), "2024-01-01T00:00:00Z")
            self.assertEqual(
                IncrementalStream.write_bookmark(stream, state, value="2024-01-01T00:00:00Z"),
                {"bookmark": True},
            )

        mock_get_bookmark.assert_called_once()
        mock_write_bookmark.assert_called_once()

    def test_build_search_filter_with_and_without_nulls(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        start_dt = datetime(2024, 1, 1, tzinfo=fixed_cst)
        end_dt = datetime(2024, 1, 2, tzinfo=fixed_cst)

        self.client.create_simple_filter.reset_mock()
        self.client.create_complex_filter.reset_mock()
        stream._build_search_filter(start_dt, end_dt, include_null=False)
        self.assertEqual(self.client.create_simple_filter.call_count, 2)
        self.assertEqual(self.client.create_complex_filter.call_count, 1)

        self.client.create_simple_filter.reset_mock()
        self.client.create_complex_filter.reset_mock()
        stream._build_search_filter(start_dt, end_dt, include_null=True)
        self.assertEqual(self.client.create_simple_filter.call_count, 3)
        self.assertEqual(self.client.create_complex_filter.call_count, 2)

    def test_fetch_paginated_records_retries_without_null_filter(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        self.client.retrieve_request.side_effect = [
            MarketingCloudError("Value cannot be null"),
            {"Results": [{"ModifiedDate": "2024-01-01T00:00:00Z"}], "RequestID": "req", "OverallStatus": "OK"},
        ]
        stream._build_search_filter = MagicMock(return_value="retry-filter")

        records = list(
            IncrementalStream._fetch_paginated_records(
                stream, ["ModifiedDate"], "initial-filter", "start", "end", include_null=True
            )
        )

        self.assertEqual(records[0]["ModifiedDate"], "2024-01-01T00:00:00Z")
        stream._build_search_filter.assert_called_once_with("start", "end", include_null=False)

    def test_fetch_paginated_records_raises_when_retry_is_not_allowed(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        self.client.retrieve_request.side_effect = MarketingCloudError("boom")

        with self.assertRaises(MarketingCloudError):
            list(
                IncrementalStream._fetch_paginated_records(
                    stream, ["ModifiedDate"], "filter", "start", "end", include_null=False
                )
            )

    def test_fetch_paginated_records_logs_error_status(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        self.client.retrieve_request.return_value = {
            "Results": [{"ModifiedDate": "2024-01-01T00:00:00Z"}],
            "RequestID": "req",
            "OverallStatus": "Error: Something went wrong",
        }

        records = list(
            IncrementalStream._fetch_paginated_records(
                stream, ["ModifiedDate"], "filter", "start", "end", include_null=False
            )
        )

        self.assertEqual(len(records), 1)

    def test_get_records_and_sync_flow(self):
        stream = ProbeIncrementalStream(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ModifiedDate"])
        stream.create_date_windows = MagicMock(return_value=[("start-1", "end-1"), ("start-2", "end-2")])
        stream._build_search_filter = MagicMock(side_effect=["filter-1", "filter-2"])
        stream._fetch_paginated_records = MagicMock(
            side_effect=[iter([{"ModifiedDate": "2024-01-01T00:00:00Z"}]), iter([{"ModifiedDate": "2024-01-02T00:00:00Z"}])]
        )

        records = list(
            IncrementalStream.get_records(stream, "2024-01-01T00:00:00Z", {}, {"properties": {}})
        )

        self.assertEqual([record["ModifiedDate"] for record in records], ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"])
        self.assertEqual(stream.query_fields, ["ModifiedDate"])

        stream.get_bookmark = MagicMock(return_value="2024-01-01T00:00:00Z")
        stream.get_records = MagicMock(
            return_value=[
                {"ModifiedDate": datetime(2024, 1, 2, 10, 0, tzinfo=fixed_cst)},
                {"ModifiedDate": "2024-01-03T09:30:00Z"},
            ]
        )
        stream.write_bookmark = MagicMock(return_value={"state": "updated"})

        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, schema, metadata: record

        with patch("tap_exacttarget.streams.abstracts.write_record") as mock_write_record, patch(
            "tap_exacttarget.streams.abstracts.strptime_to_cst",
            side_effect=lambda value: value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00")),
        ):
            state = IncrementalStream.sync(stream, {}, {"properties": {}}, {}, transformer)

        self.assertEqual(state, {"state": "updated"})
        self.assertEqual(mock_write_record.call_count, 2)
        stream.write_bookmark.assert_called_once()

    def test_full_table_stream_get_records_and_sync(self):
        stream = ProbeFullTableStream(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ID"])
        self.client.retrieve_request.side_effect = [
            {"Results": [{"ID": 1}], "RequestID": "r1", "OverallStatus": "MoreDataAvailable"},
            {"Results": [{"ID": 2}], "RequestID": "r2", "OverallStatus": "OK"},
        ]

        records = list(FullTableStream.get_records(stream, {}, {}))
        self.assertEqual([record["ID"] for record in records], [1, 2])

        stream.get_records = MagicMock(return_value=[{"ID": 3}, {"ID": 4}])
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, schema, metadata: record

        with patch("tap_exacttarget.streams.abstracts.write_record") as mock_write_record:
            state = FullTableStream.sync(stream, {"keep": True}, {"properties": {}}, {}, transformer)

        self.assertEqual(state, {"keep": True})
        self.assertEqual(mock_write_record.call_count, 2)

    def test_full_table_stream_logs_error_status(self):
        stream = ProbeFullTableStream(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ID"])
        self.client.retrieve_request.return_value = {
            "Results": [{"ID": 1}],
            "RequestID": "r1",
            "OverallStatus": "Error: Bad thing",
        }

        records = list(FullTableStream.get_records(stream, {}, {}))

        self.assertEqual([record["ID"] for record in records], [1])


class TestStreamTransforms(TestCase):
    def setUp(self):
        self.client = MagicMock(config={"start_date": "2024-01-01T00:00:00Z"})

    def _instance(self, stream_cls):
        return stream_cls(_metadata(), {"properties": {}}, self.client)

    def test_simple_transformers(self):
        cases = [
            (DataFolder, {"ParentFolder": {"ID": 42}}, {"ParentFolder": 42}),
            (Email, {"Email": {"ID": 7}, "ContentAreas": [{"ID": 1}, {"ID": 2}]}, {"EmailID": 7, "ContentAreaIDs": [1, 2]}),
            (BounceEvent, {"SubscriberKey": "sub"}, {"SubscriberKey": "sub"}),
            (ClickEvent, {"SubscriberKey": "sub"}, {"SubscriberKey": "sub"}),
            (NotSentEvent, {"SubscriberKey": "sub"}, {"SubscriberKey": "sub"}),
            (OpenEvent, {"SubscriberKey": "sub"}, {"SubscriberKey": "sub"}),
            (SentEvent, {"SubscriberKey": "sub", "PartnerProperties": [{"Name": "Country", "Value": "US"}]}, {"SubscriberKey": "sub", "Country": "US"}),
            (UnsubEvent, {"SubscriberKey": "sub", "List": {"ID": 88}}, {"SubscriberKey": "sub", "ListID": 88}),
            (ListSend, {"List": {"ID": 99}}, {"ListID": 99}),
            (Sends, {"Email": {"ID": 11}}, {"EmailID": 11}),
            (Subscribers, {"Lists": [{"ObjectID": "l1"}, {"ObjectID": "l2"}]}, {"ListIDs": ["l1", "l2"]}),
        ]

        for stream_cls, payload, expected in cases:
            with self.subTest(stream_cls=stream_cls.__name__):
                stream = self._instance(stream_cls)
                result = stream.transform_record(payload)
                for key, value in expected.items():
                    self.assertEqual(result[key], value)

    def test_campaigns_get_records_and_sync(self):
        stream = Campaigns(_metadata(), {"properties": {}}, self.client)
        self.client.get_rest.side_effect = [
            {"items": [{"id": index} for index in range(1, 26)], "page": 1},
            {"items": [{"id": 2}], "page": 2},
        ]

        records = list(stream.get_records({}, {}))
        self.assertEqual(records[0]["id"], 1)
        self.assertEqual(records[-1]["id"], 2)

        stream.get_records = MagicMock(return_value=[{"id": 3}, {"id": 4}])
        transformer = MagicMock()
        transformer.transform.side_effect = [
            {"id": 3},
            SchemaMismatch([Mock(tostr=Mock(return_value="bad schema"))]),
        ]

        with patch("tap_exacttarget.streams.campaigns.write_record") as mock_write_record:
            state = stream.sync({}, {"properties": {}}, {}, transformer)

        self.assertEqual(state, {})
        self.assertEqual(mock_write_record.call_count, 1)

    def test_list_subscribers_fetch_and_get_list_id_and_get_records(self):
        stream = ListSubscribers(_metadata(), {"properties": {}}, self.client)
        stream.subscribers_obj = SimpleNamespace(sync_ids=MagicMock())

        stream.fetch_subscribers_batch(list(range(101)))
        self.assertEqual(stream.subscribers_obj.sync_ids.call_count, 2)

        self.client.create_simple_filter.return_value = "list-filter"
        self.client.retrieve_request.return_value = {
            "Results": [{"ID": 123, "ListName": "All Subscribers"}],
            "RequestID": "req",
            "OverallStatus": "OK",
        }
        self.assertEqual(stream.get_list_id(), 123)

        stream.get_query_fields = MagicMock(return_value=["ModifiedDate"])
        stream.get_list_id = MagicMock(return_value=123)
        stream.create_date_windows = MagicMock(return_value=[("start", "end")])
        stream.transform_record = MagicMock(side_effect=lambda record: record)
        stream.fetch_subscribers_batch = MagicMock()
        self.client.retrieve_request.return_value = {
            "Results": [{"SubscriberKey": "a"}, {"SubscriberKey": "b"}],
            "RequestID": "req-2",
            "OverallStatus": "OK",
        }

        records = list(stream.get_records("2024-01-01T00:00:00Z", {}, {"properties": {}}))
        self.assertEqual([record["SubscriberKey"] for record in records], ["a", "b"])
        stream.fetch_subscribers_batch.assert_called_once_with(["a", "b"])

    def test_list_subscribers_get_list_id_raises_on_multiple_matches(self):
        stream = ListSubscribers(_metadata(), {"properties": {}}, self.client)
        self.client.create_simple_filter.return_value = "list-filter"
        self.client.retrieve_request.return_value = {
            "Results": [{"ID": 1}, {"ID": 2}],
            "RequestID": "req",
            "OverallStatus": "OK",
        }

        with self.assertRaises(RuntimeError):
            stream.get_list_id()

    def test_list_subscribers_logs_error_status(self):
        stream = ListSubscribers(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ModifiedDate"])
        stream.get_list_id = MagicMock(return_value=123)
        stream.create_date_windows = MagicMock(return_value=[("start", "end")])
        stream.transform_record = MagicMock(side_effect=lambda record: record)
        stream.fetch_subscribers_batch = MagicMock()
        self.client.retrieve_request.return_value = {
            "Results": [{"SubscriberKey": "a"}],
            "RequestID": "req-2",
            "OverallStatus": "Error: Something bad",
        }

        records = list(stream.get_records("2024-01-01T00:00:00Z", {}, {"properties": {}}))
        self.assertEqual([record["SubscriberKey"] for record in records], ["a"])

    def test_subscriber_filter_sync_and_return_state(self):
        stream = Subscribers(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ID"])
        stream.transform_record = MagicMock(side_effect=lambda record: record)

        self.client.create_simple_filter.return_value = "simple-filter"
        self.client.retrieve_request.return_value = {
            "Results": [{"ID": 1}],
            "RequestID": "req",
            "OverallStatus": "OK",
        }

        single_results = list(stream.filter_records(["subscriber-1"]))
        self.assertEqual([record["ID"] for record in single_results], [1])
        self.client.create_simple_filter.assert_called_with("SubscriberKey", "equals", "subscriber-1")

        self.client.create_simple_filter.reset_mock()
        self.client.retrieve_request.reset_mock()

        multiple_results = list(stream.filter_records(["a", "b"]))
        self.assertEqual([record["ID"] for record in multiple_results], [1])
        self.client.create_simple_filter.assert_called_with("SubscriberKey", "IN", ["a", "b"])

        stream.filter_records = MagicMock(return_value=[{"ID": 1}])
        state = stream.sync({"ok": True}, {"properties": {}}, {}, MagicMock())
        self.assertEqual(state, {"ok": True})

        stream.filter_records = MagicMock(return_value=[{"ID": 2}])
        with patch("tap_exacttarget.streams.subscriber.Transformer") as mock_transformer_cls, patch(
            "tap_exacttarget.streams.subscriber.write_record"
        ) as mock_write_record:
            mock_transformer = MagicMock()
            mock_transformer.transform.side_effect = lambda record, schema, metadata: record
            mock_transformer_cls.return_value = mock_transformer
            stream.client.log_search_filter = True
            stream.sync_ids(["id-1", "id-2"])

        self.assertTrue(stream.client.log_search_filter)
        mock_write_record.assert_called()

    def test_subscriber_filter_records_logs_error_status(self):
        stream = Subscribers(_metadata(), {"properties": {}}, self.client)
        stream.get_query_fields = MagicMock(return_value=["ID"])
        stream.transform_record = MagicMock(side_effect=lambda record: record)
        self.client.create_simple_filter.return_value = "simple-filter"
        self.client.retrieve_request.return_value = {
            "Results": [{"ID": 9}],
            "RequestID": "req",
            "OverallStatus": "Error: Bad subscriber query",
        }

        records = list(stream.filter_records(["subscriber-1"]))
        self.assertEqual([record["ID"] for record in records], [9])


class TestSyncOrchestration(TestCase):
    @patch("tap_exacttarget.sync.discover_dao_streams", return_value={})
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_handles_subscribers_and_list_subscribers(self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema, mock_discover_dao_streams):
        state = {"bookmarks": {}}
        schema = SimpleNamespace(to_dict=lambda: {"properties": {}})
        subscribers_item = SimpleNamespace(tap_stream_id="subscribers", schema=schema, metadata=[], replication_key=None)
        list_subscribers_item = SimpleNamespace(tap_stream_id="list_subscribers", schema=schema, metadata=[], replication_key="ModifiedDate")

        class CatalogMock:
            def get_selected_streams(self, _state):
                return [subscribers_item, list_subscribers_item]

        subscribers_stream = MagicMock(key_properties=["ID"], replication_key=None, tap_stream_id="subscribers")
        list_subscribers_stream = MagicMock(key_properties=["SubscriberKey", "ListID"], replication_key="ModifiedDate", tap_stream_id="list_subscribers")
        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer
        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state

        with patch.dict(
            sync_module.STREAMS,
            {"subscribers": Mock(return_value=subscribers_stream), "list_subscribers": Mock(return_value=list_subscribers_stream)},
            clear=False,
        ):
            sync_module.sync(MagicMock(), CatalogMock(), state)

        self.assertTrue(list_subscribers_stream.sync_subscribers)
        self.assertIsNotNone(list_subscribers_stream.subscribers_obj)
        mock_write_schema.assert_any_call("subscribers", {"properties": {}}, ["ID"], None)

    @patch("tap_exacttarget.sync.discover_dao_streams", return_value={})
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_flags_missing_list_subscribers(self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema, mock_discover_dao_streams):
        state = {"bookmarks": {}}
        schema = SimpleNamespace(to_dict=lambda: {"properties": {}})
        subscribers_item = SimpleNamespace(tap_stream_id="subscribers", schema=schema, metadata=[], replication_key=None)

        class CatalogMock:
            def get_selected_streams(self, _state):
                return [subscribers_item]

        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer
        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state

        with patch.dict(sync_module.STREAMS, {}, clear=False):
            sync_module.sync(MagicMock(), CatalogMock(), state)

        mock_write_schema.assert_not_called()

    @patch("tap_exacttarget.sync.discover_dao_streams", return_value={})
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.set_currently_syncing")
    @patch("tap_exacttarget.sync.singer.metadata.to_map", side_effect=lambda metadata: metadata)
    @patch("tap_exacttarget.sync.singer.Transformer")
    def test_sync_catches_stream_failures(self, mock_transformer_cls, mock_to_map, mock_set_currently_syncing, mock_write_state, mock_write_schema, mock_discover_dao_streams):
        state = {"bookmarks": {}}
        schema = SimpleNamespace(to_dict=lambda: {"properties": {}})
        stream_item = SimpleNamespace(tap_stream_id="campaigns", schema=schema, metadata=[], replication_key=None)

        class CatalogMock:
            def get_selected_streams(self, _state):
                return [stream_item]

        failing_stream = MagicMock(key_properties=["id"], replication_key=None, tap_stream_id="campaigns")
        failing_stream.sync.side_effect = MarketingCloudSoapApiException("boom")
        mock_transformer = MagicMock()
        mock_transformer.__enter__.return_value = mock_transformer
        mock_transformer_cls.return_value = mock_transformer
        mock_set_currently_syncing.side_effect = lambda current_state, syncing: current_state

        with patch.dict(sync_module.STREAMS, {"campaigns": Mock(return_value=failing_stream)}, clear=False):
            sync_module.sync(MagicMock(), CatalogMock(), state)

        self.assertEqual(mock_write_schema.call_count, 1)
