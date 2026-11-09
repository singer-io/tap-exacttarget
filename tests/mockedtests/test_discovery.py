"""Mocked integration test: discover() end-to-end against a mocked Client."""

import unittest

from singer import metadata

from tap_exacttarget.discover import discover
from tap_exacttarget.streams import STREAMS

from .base import ExactTargetMockedBaseTest, MockClient


class DiscoveryMockedIntegrationTest(ExactTargetMockedBaseTest, unittest.TestCase):
    def test_discovery_returns_every_registered_stream_with_correct_metadata(self):
        client = MockClient(self.base_config())

        catalog = discover(client)

        discovered = {entry.tap_stream_id: entry for entry in catalog.streams}
        self.assertEqual(set(discovered), set(STREAMS))

        for stream_name, stream_cls in STREAMS.items():
            entry = discovered[stream_name]
            mdata = metadata.to_map(entry.metadata)
            root = mdata.get((), {})

            expected_method = stream_cls.replication_method or stream_cls.forced_replication_method
            self.assertEqual(
                root.get("forced-replication-method"), expected_method, stream_name
            )
            self.assertEqual(
                set(root.get("table-key-properties", [])),
                set(stream_cls.key_properties),
                stream_name,
            )

            if stream_cls.valid_replication_keys:
                for key in stream_cls.valid_replication_keys:
                    self.assertEqual(
                        metadata.get(mdata, ("properties", key), "inclusion"),
                        "automatic",
                        f"{stream_name}.{key}",
                    )

    def test_discovery_excludes_unauthorized_streams(self):
        client = MockClient(self.base_config())
        client.deny_object_types.add("ContentArea")

        catalog = discover(client)
        discovered_ids = {entry.tap_stream_id for entry in catalog.streams}

        self.assertNotIn("content_area", discovered_ids)
        self.assertIn("campaigns", discovered_ids)
        self.assertIn("email", discovered_ids)

    def test_discovery_excludes_unauthorized_rest_stream(self):
        client = MockClient(self.base_config())
        client.deny_endpoints.add("hub/v1/campaigns/")

        catalog = discover(client)
        discovered_ids = {entry.tap_stream_id for entry in catalog.streams}

        self.assertNotIn("campaigns", discovered_ids)
        self.assertIn("content_area", discovered_ids)


if __name__ == "__main__":
    unittest.main()
