import unittest
from unittest.mock import patch
from datetime import datetime, date, time, timezone, timedelta
import json

from tap_exacttarget.streams.abstracts import strptime_to_cst, CustomDTParser, fixed_cst
from tap_exacttarget.streams.list_subscribers import ListSubscribe
from .base_test import BaseClientTest


class TestStrptimeToCST(BaseClientTest):
    def test_naive_datetime(self):
        """Test conversion of naive datetime string to CST."""
        dt_str = "2025-11-03 15:30:00"
        result = strptime_to_cst(dt_str)

        self.assertEqual(result.tzinfo, fixed_cst)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.year, 2025)

    def test_default_falls_back_to_super(self):
        """Test that non-date/time objects fall back to the parent JSONEncoder."""

        class Dummy:
            pass

        dummy = Dummy()
        encoder = CustomDTParser()

        # json.JSONEncoder.default raises TypeError by default for unknown objects
        with self.assertRaises(TypeError):
            encoder.default(dummy)

    def test_utc_datetime(self):
        """Test conversion from UTC to CST."""
        dt_str = "2025-11-03T21:30:00+00:00"  # 21:30 UTC = 15:30 CST
        result = strptime_to_cst(dt_str)

        self.assertEqual(result.utcoffset(), timedelta(hours=-6))
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo, fixed_cst)

    def test_other_timezone(self):
        """Test conversion from another timezone (EST) to CST."""
        dt_str = "2025-11-03T16:30:00-05:00"  # 16:30 EST = 15:30 CST
        result = strptime_to_cst(dt_str)

        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo, fixed_cst)


class TestCustomDTParser(unittest.TestCase):
    def test_datetime_serialization(self):
        """Test JSON encoding of datetime object."""
        dt = datetime(2025, 11, 3, 15, 30, tzinfo=timezone.utc)
        json_str = json.dumps({"dt": dt}, cls=CustomDTParser)
        self.assertIn("2025-11-03T15:30:00+00:00", json_str)

    def test_date_serialization(self):
        """Test JSON encoding of date object."""
        d = date(2025, 11, 3)
        json_str = json.dumps({"date": d}, cls=CustomDTParser)
        self.assertIn("2025-11-03", json_str)

    def test_time_serialization(self):
        """Test JSON encoding of time object."""
        t = time(15, 30, 0)
        json_str = json.dumps({"time": t}, cls=CustomDTParser)
        self.assertIn("15:30:00", json_str)

    def test_fallback_non_datetime(self):
        """Test fallback to default JSONEncoder behavior."""
        data = {"x": 123}
        json_str = json.dumps(data, cls=CustomDTParser)
        self.assertEqual(json_str, '{"x": 123}')

    @patch("tap_exacttarget.streams.list_subscribers.LOGGER")
    def test_fetch_subscribers_batch_calls_sync_ids_in_batches(self, mock_logger):
        """Test that fetch_subscribers_batch processes IDs in 100-sized batches."""

        class DummyObj:
            def __init__(self):
                self.calls = []

            def sync_ids(self, ids):
                self.calls.append(ids)

        obj = ListSubscribe({}, {}, {})
        obj.subscribers_obj = DummyObj()

        # Create 250 subscriber IDs → should produce 3 batches (100, 100, 50)
        subs = list(range(250))
        obj.fetch_subscribers_batch(subs)

        # Verify correct batching
        assert len(obj.subscribers_obj.calls) == 3
        assert len(obj.subscribers_obj.calls[0]) == 100
        assert len(obj.subscribers_obj.calls[1]) == 100
        assert len(obj.subscribers_obj.calls[2]) == 50

        # Verify logging called for each batch
        assert mock_logger.info.call_count == 3
