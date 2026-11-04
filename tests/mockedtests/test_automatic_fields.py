"""Mocked integration test: verify key/replication properties are marked
'automatic' (and other fields 'available') in discovered catalog metadata."""

import unittest

from singer import metadata

from tap_exacttarget.discover import discover

from .base import ExactTargetMockedBaseTest, MockClient


class AutomaticFieldsMockedIntegrationTest(ExactTargetMockedBaseTest, unittest.TestCase):
    def test_incremental_stream_key_and_replication_properties_are_automatic(self):
        client = MockClient(self.base_config())
        catalog = discover(client)
        entry = self.get_stream(catalog, "content_area")
        mdata = metadata.to_map(entry.metadata)

        self.assertEqual(metadata.get(mdata, ("properties", "ID"), "inclusion"), "automatic")
        self.assertEqual(
            metadata.get(mdata, ("properties", "ModifiedDate"), "inclusion"), "automatic"
        )
        self.assertEqual(metadata.get(mdata, ("properties", "Name"), "inclusion"), "available")

    def test_full_table_stream_key_properties_are_automatic(self):
        client = MockClient(self.base_config())
        catalog = discover(client)
        entry = self.get_stream(catalog, "campaigns")
        mdata = metadata.to_map(entry.metadata)

        self.assertEqual(metadata.get(mdata, ("properties", "id"), "inclusion"), "automatic")


if __name__ == "__main__":
    unittest.main()
