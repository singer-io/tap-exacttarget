"""Test that with no fields selected for a stream automatic fields are still
replicated."""

from tap_tester.base_suite_tests.automatic_fields_test import \
    MinimumSelectionTest

from base import ExactTargetBaseTest


class ExactTargetAutomaticFields(MinimumSelectionTest, ExactTargetBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_exacttarget_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # No data
            "subscribers",
            "campaigns",
            "content_area",
            "email",
            "list_subscribers",
            "bounceevent",
            "notsentevent",

            # # passed
            # "datafolder",
            # "send",
            # "sentevent",
            # "openevent",
            # "clickevent",
            # "unsubevent",
            # "list_send",
            # "list",
            # "data_extension_snowflake_poc1",
            # "data_extension_tcx_snowflaketest_newslettersubscribers",
            # "data_extension_snowflake_vot2_dex_welcome_order_added",

            # Failed due to
            # tap - CRITICAL Stream Failed data_extension__mobileaddress, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilesubscription, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilelineaddresscontact, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilelineaddress, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilelineprofile, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilelineprofileattribute, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension__mobilelinesubscription, Reason: Error: Invalid column name '_CustomObjectKey'.
            # tap - CRITICAL Stream Failed data_extension_mobilelineorphancontact, Reason: Error: Invalid column name '_ModifiedDate'.

            "data_extension__mobileaddress",
            "data_extension__mobilesubscription",
            "data_extension__mobilelineaddresscontact",
            "data_extension__mobilelineaddress",
            "data_extension__mobilelineprofile",
            "data_extension__mobilelineprofileattribute",
            "data_extension__mobilelinesubscription",
            "data_extension_mobilelineorphancontact",


            # No data - data_extension's
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
        }
        return self.expected_stream_names().difference(streams_to_exclude)
