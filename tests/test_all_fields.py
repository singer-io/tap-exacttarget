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
            # No data
            "subscribers",
            "campaigns",
            "content_area",
            "email",
            "list_subscribers",
            "bounceevent",
            "notsentevent",

            # Failed (Bad Streams)
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
