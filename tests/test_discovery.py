"""Test tap discovery mode and metadata."""

from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from .base import ExactTargetBaseTest


class ExacttargetDiscoveryTest(DiscoveryTest, ExactTargetBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_exacttarget_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()