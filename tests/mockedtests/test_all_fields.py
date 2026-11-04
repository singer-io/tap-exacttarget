"""Mocked integration test: verify all available fields of a record are
written through discover() + sync() against a mocked Client."""

import unittest
from unittest.mock import patch

from tap_exacttarget.discover import discover
from tap_exacttarget.sync import sync

from .base import ExactTargetMockedBaseTest, MockClient, describe_fields

# A large date_window guarantees create_date_windows() produces exactly one
# window (covering the whole start_date -> now range), so the mocked,
# always-identical retrieve_request response is only fetched (and its
# records written) once per test, regardless of how much time has elapsed
# since the fixed start_date used below.
LARGE_DATE_WINDOW = 20000


class AllFieldsMockedIntegrationTest(ExactTargetMockedBaseTest, unittest.TestCase):
    @patch("tap_exacttarget.sync.singer.write_state")
    @patch("tap_exacttarget.sync.singer.write_schema")
    @patch("tap_exacttarget.streams.abstracts.write_record")
    def test_all_available_fields_are_written(
        self, mock_write_record, _mock_write_schema, _mock_write_state
    ):
        client = MockClient(self.base_config(date_window=LARGE_DATE_WINDOW))
        client.describe_responses["ContentArea"] = describe_fields(
            "ID", "ModifiedDate", "Name", "Content", "CategoryID"
        )
        client.retrieve_responses["ContentArea"] = {
            "Results": [
                {
                    "ID": 101,
                    "ModifiedDate": "2024-01-12T00:00:00",
                    "Name": "Welcome Email Content",
                    "Content": "<html>Hello</html>",
                    "CategoryID": 55,
                }
            ],
            "RequestID": "req-1",
            "OverallStatus": "OK",
        }

        catalog = self.select_streams(discover(client), {"content_area"})
        sync(client, catalog, {})

        self.assertEqual(mock_write_record.call_count, 1)
        written_stream, written_record = mock_write_record.call_args.args[:2]
        self.assertEqual(written_stream, "content_area")
        self.assertEqual(written_record["ID"], 101)
        self.assertEqual(written_record["Name"], "Welcome Email Content")
        self.assertEqual(written_record["Content"], "<html>Hello</html>")
        self.assertEqual(written_record["CategoryID"], 55)
        # Singer's Transformer normalizes date-times to UTC, so only the
        # date portion (not the CST-offset time-of-day) is asserted here.
        self.assertTrue(written_record["ModifiedDate"].startswith("2024-01-12"))


if __name__ == "__main__":
    unittest.main()
