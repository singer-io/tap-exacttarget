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
            # No data available for streams(data_extension)
            "data_extension_cloudpages_dataextension",
            "data_extension_pi_abandoned_cart_event",
            "data_extension__mobilelineprofile",
            "data_extension__pushaddress",
            "data_extension_pi_content",
            "data_extension_igo_purchases",
            "data_extension_pi_contentviews",
            "data_extension_mobilelineorphancontact",
            "data_extension_pi_session_ends",
            "data_extension__mobileaddressapplication",
            "data_extension_einstein_mc_predictive_scores",
            "data_extension__mobileaddress",
            "data_extension_expressionbuilderattributes",
            "data_extension_pi_abandoned_cart_items",
            "data_extension_snowflaketest",
            "data_extension_snowflake_poc1_reference",
            "data_extension__mobilelineaddress",
            "data_extension_pi_contentattribs",
            "data_extension_igo_productattribs",
            "data_extension_igo_views",
            "data_extension_igo_profiles",
            "data_extension__pushtag",
            "data_extension_igo_products",
            "data_extension_pi_sessions",
            "data_extension__mobilelineaddresscontact",
            "data_extension__chatmessagingsubscription",
            "data_extension__mobilelinesubscription",
            "data_extension__mobilesubscription",
            "data_extension__mobilelineprofileattribute",
            # Following DataExtension streams are failing due to error: Invalid column name '_CustomObjectKey'.
            "data_extension__mobileaddress",
            "data_extension__mobilesubscription",
            "data_extension__mobilelineaddresscontact",
            "data_extension__mobilelineaddress",
            "data_extension__mobilelineprofile",
            "data_extension__mobilelineprofileattribute",
            "data_extension__mobilelinesubscription",
            # Following DataExtension streams are failing due to error: Invalid column name '_ModifiedDate'.
            "data_extension_mobilelineorphancontact",

            # Unsupported Full-Table Streams
            "data_extension_snowflake_poc1",
            "data_extension_snowflake_vot2_dex_welcome_order_added",
            "data_extension_tcx_snowflaketest_newslettersubscribers",
            "list_send",
            "datafolder"

        }
        return self.expected_stream_names().difference(streams_to_exclude)
