"""Mocked integration test: verify the configured start_date is used as the
sync's lower date bound when no bookmark exists yet in state."""

import unittest
from unittest.mock import patch

from tap_exacttarget.discover import discover
from tap_exacttarget.sync import sync

from .base import ExactTargetMockedBaseTest, MockClient, describe_fields, find_filter_date_value

LARGE_DATE_WINDOW = 20000


class StartDateMockedIntegrationTest(ExactTargetMockedBaseTest, unittest.TestCase):
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.streams.abstracts.write_record")
    def test_start_date_used_as_query_lower_bound_when_no_bookmark(
        self, mock_write_record, _mock_write_schema, _mock_write_state
    ):
        # A naive (no "Z"/offset) date string is used so strptime_to_cst()
        # doesn't shift the date via a UTC->CST conversion, keeping the
        # date assertion below exact.
        client = MockClient(
            self.base_config(start_date="2024-02-10T00:00:00", date_window=LARGE_DATE_WINDOW)
        )
        client.describe_responses["ContentArea"] = describe_fields("ID", "ModifiedDate", "Name")
        client.retrieve_responses["ContentArea"] = {
            "Results": [{"ID": 1, "ModifiedDate": "2024-02-15T00:00:00", "Name": "only"}],
            "RequestID": "req-1",
            "OverallStatus": "OK",
        }

        state = {}  # No bookmark for content_area yet.
        catalog = self.select_streams(discover(client), {"content_area"})
        sync(client, catalog, state)

        search_filter = client.retrieve_calls[-1]["search_filter"]
        lower_bound = find_filter_date_value(search_filter, "greaterThanOrEqual")
        self.assertIsNotNone(lower_bound)
        self.assertEqual((lower_bound.year, lower_bound.month, lower_bound.day), (2024, 2, 10))

        self.assertEqual(mock_write_record.call_count, 1)
        new_bookmark = state["bookmarks"]["content_area"]["ModifiedDate"]
        self.assertTrue(new_bookmark.startswith("2024-02-15T00:00:00"))


if __name__ == "__main__":
    unittest.main()
