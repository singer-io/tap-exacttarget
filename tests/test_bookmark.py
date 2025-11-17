from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import ExactTargetBaseTest


class ExactTargetBookMarkTest(BookmarkTest, ExactTargetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    initial_bookmarks = {"bookmarks": {}}

    @staticmethod
    def name():
        return "tap_tester_exacttarget_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # No data available for streams
            "subscribers",
            "campaigns",
            "content_area",
            "email",
            "list_subscribers",
            "bounceevent",
            "notsentevent",
            # Unsupported Full-Table Streams
            "list_send",
            "datafolder"

        }
        return self.expected_stream_names().difference(streams_to_exclude)
