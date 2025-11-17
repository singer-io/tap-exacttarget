from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import ExactTargetBaseTest


class ExactTargetStartDateTest(StartDateTest, ExactTargetBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_exacttarget_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # No data available for streams
            "subscribers",
            "campaigns",
            "content_area",
            "email",
            "list_subscribers",
            "bounceevent",
            "list",
            "notsentevent",
            "unsubevent",
            # Unsupported Full-Table Streams
            "list_send",
            "datafolder"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2025-06-05T00:00:00.000000-06:00"

    @property
    def start_date_2(self):
        return "2025-06-05T03:00:00.000000-06:00"
