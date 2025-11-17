import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.base_suite_tests.base_case import BaseCase
from tap_tester.logger import LOGGER


class ExactTargetBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2015-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-exacttarget"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.exacttarget"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "bounceevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            },

            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "clickevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            },

            "content_area": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__chatmessagingsubscription": {
                cls.PRIMARY_KEYS: {"_ChannelId", "_ChannelType", "_CustomObjectKey", "_MobileNumber"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_ModifiedDate", "_CreatedDate"},
            },

            "data_extension__mobileaddress": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_CreatedDate", "_ModifiedDate"},
            },

            "data_extension__mobileaddressapplication": {
                cls.PRIMARY_KEYS: {"_ContactID", "_CustomObjectKey", "_MobileApplicationID", "_MobileNumber"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_CreatedDate"},
            },

            "data_extension__mobilelineaddress": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__mobilelineaddresscontact": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__mobilelineprofile": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__mobilelineprofileattribute": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__mobilelinesubscription": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension__mobilesubscription": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_CreatedDate", "_ModifiedDate"},
            },

            "data_extension__pushaddress": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_ModifiedDate", "_CreatedDate"},
            },

            "data_extension__pushtag": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_ModifiedDate", "_CreatedDate"},
            },

            "data_extension_cloudpages_dataextension": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "data_extension_einstein_mc_predictive_scores": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "email_address"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_expressionbuilderattributes": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "ID", "Category"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_igo_products": {
                cls.PRIMARY_KEYS: {"Uuid", "_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_igo_productattribs": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "attribName", "attributeValueIndex", "sku"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_igo_profiles": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "user_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_igo_purchases": {
                cls.PRIMARY_KEYS: {"Sku", "Timestamp", "_CustomObjectKey", "user_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_igo_views": {
                cls.PRIMARY_KEYS: {"Timestamp", "_CustomObjectKey", "user_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_mobilelineorphancontact": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"_ModifiedDate", "_CreatedDate"},
            },

            "data_extension_pi_abandoned_cart_event": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "cart_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_abandoned_cart_items": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "cart_id", "sku"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_content": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "uuid"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_contentattribs": {
                cls.PRIMARY_KEYS: {"AttribName", "_CustomObjectKey", "attributeValueIndex", "content_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_contentviews": {
                cls.PRIMARY_KEYS: {"Timestamp", "_CustomObjectKey", "user_Id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_session_ends": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "session_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_pi_sessions": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "session_id", "user_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_snowflake_poc1": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "email_address"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_snowflake_poc1_reference": {
                cls.PRIMARY_KEYS: {"_CustomObjectKey", "email_address"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_snowflake_vot2_dex_welcome_order_added": {
                cls.PRIMARY_KEYS: {"EventId", "_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_snowflaketest": {
                cls.PRIMARY_KEYS: {"SubscriberKey", "_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "data_extension_tcx_snowflaketest_newslettersubscribers": {
                cls.PRIMARY_KEYS: {"CampaignCode", "Locale", "SubscriberKey", "_CustomObjectKey"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "datafolder": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "email": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "list": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "list_send": {
                cls.PRIMARY_KEYS: {"SendID", "ListID"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "list_subscribers": {
                cls.PRIMARY_KEYS: {"SubscriberKey", "ListID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "notsentevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            },

            "openevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            },

            "send": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },

            "sentevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            },

            "subscribers": {
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },

            "unsubevent": {
                cls.PRIMARY_KEYS: {"SendID", "EventType", "SubscriberKey", "EventDate"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"EventDate"},
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "client_id": "TAP_EXACTTARGET_CLIENT_ID",
            "client_secret": "TAP_EXACTTARGET_CLIENT_SECRET",
            "tenant_subdomain": "TAP_EXACTTARGET_TENANT_SUBDOMAIN",
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = False):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2015-07-01T00:00:00Z",
            "date_window": 365
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value
