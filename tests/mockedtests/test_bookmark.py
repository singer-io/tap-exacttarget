"""Mocked integration test: verify an existing bookmark is used as the
sync's lower date bound, and advances to the latest record's replication
key value after a successful sync."""

import unittest
from unittest.mock import patch

from tap_exacttarget.discover import discover
from tap_exacttarget.sync import sync

from .base import ExactTargetMockedBaseTest, MockClient, describe_fields, find_filter_date_value

LARGE_DATE_WINDOW = 20000


class BookmarkMockedIntegrationTest(ExactTargetMockedBaseTest, unittest.TestCase):
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.streams.abstracts.write_record")
    def test_bookmark_used_as_query_lower_bound_and_advances_after_sync(
        self, mock_write_record, _mock_write_schema, _mock_write_state
    ):
        # start_date is deliberately much older than the bookmark, so that
        # a passing test proves the bookmark (not the config start_date)
        # was used as the query's lower bound. Naive (no "Z"/offset) date
        # strings are used throughout so strptime_to_cst() doesn't shift the
        # date via a UTC->CST conversion, keeping date assertions exact.
        client = MockClient(
            self.base_config(start_date="2015-01-01T00:00:00", date_window=LARGE_DATE_WINDOW)
        )
        client.describe_responses["ContentArea"] = describe_fields("ID", "ModifiedDate", "Name")
        client.retrieve_responses["ContentArea"] = {
            "Results": [
                {"ID": 1, "ModifiedDate": "2024-01-05T00:00:00", "Name": "first"},
                {"ID": 2, "ModifiedDate": "2024-01-20T00:00:00", "Name": "second"},
            ],
            "RequestID": "req-1",
            "OverallStatus": "OK",
        }

        bookmark_value = "2024-01-02T00:00:00"
        state = {"bookmarks": {"content_area": {"ModifiedDate": bookmark_value}}}

        catalog = self.select_streams(discover(client), {"content_area"})
        sync(client, catalog, state)

        # The query's lower bound should reflect the bookmark, not the
        # (much older) config start_date.
        search_filter = client.retrieve_calls[-1]["search_filter"]
        lower_bound = find_filter_date_value(search_filter, "greaterThanOrEqual")
        self.assertIsNotNone(lower_bound)
        self.assertEqual(lower_bound.year, 2024)
        self.assertEqual(lower_bound.month, 1)
        self.assertEqual(lower_bound.day, 2)

        # Both records are written (the mock doesn't filter server-side;
        # it's the search_filter construction above that is being tested).
        self.assertEqual(mock_write_record.call_count, 2)

        # The bookmark advances to the latest record's ModifiedDate seen.
        new_bookmark = state["bookmarks"]["content_area"]["ModifiedDate"]
        self.assertTrue(new_bookmark.startswith("2024-01-20T00:00:00"))


if __name__ == "__main__":
    unittest.main()
