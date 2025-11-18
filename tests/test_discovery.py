"""Test tap discovery mode and metadata."""
import re
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import ExactTargetBaseTest


class ExacttargetDiscoveryTest(DiscoveryTest, ExactTargetBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_exacttarget_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_stream_naming(self):
        """Verify stream names follow naming convention (lowercase alphas and underscores)."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                self.assertEqual(re.fullmatch(r"[a-z0-9_]+", stream).group(0), stream,
                                    logging=f"verify {stream} is only lower case alpha "
                                            f"or underscore characters")
