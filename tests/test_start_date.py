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
            #Following DataExtension streams are failing due to error: Invalid column name '_ModifiedDate'.
            "data_extension_mobilelineorphancontact",
            # Unsupported Full-Table Streams
            "data_extension_snowflake_poc1",
            "data_extension_snowflake_vot2_dex_welcome_order_added",
            "data_extension_tcx_snowflaketest_newslettersubscribers",
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
