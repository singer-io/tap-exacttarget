from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import ExactTargetBaseTest

KNOWN_MISSING_FIELDS = {}


class ExactTargetAllFields(AllFieldsTest, ExactTargetBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    @staticmethod
    def name():
        return "tap_tester_exacttarget_all_fields_test"

    MISSING_FIELDS =  {
            "datafolder": {"Type"},
            "openevent": {"URL"},
            "sentevent": {"URL"},
            "unsubevent": {"URL"},
            "send": {"EmailID"},
        }

    def streams_to_test(self):
        streams_to_exclude = {
            # No data available for streams
            "subscribers",
            "campaigns",
            "content_area",
            "email",
            "list_subscribers",
            "bounceevent",
            "notsentevent"
        }
        return self.expected_stream_names().difference(streams_to_exclude)
