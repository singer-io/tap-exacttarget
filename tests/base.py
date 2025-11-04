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


class ExacttargetBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2019-01-01T00:00:00Z"

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
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE
            },
            "content_area":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
            "datafolder":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
            "email":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
            "list_send":{
                cls.PRIMARY_KEYS: {"ListID", "SendID"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
            },
            "list_subscribers":{
                cls.PRIMARY_KEYS: {"SubscriberKey", "ListID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
             "list":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
            "send":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            },
            "subscribers":{
                cls.PRIMARY_KEYS: {"ID"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"ModifiedDate"},
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "client_id": "TAP_EXACTTARGET_CLIENT_ID",
            "client_secret": "TAP_EXACTTARGET_CLIENT_SECRET",
            "refresh_token": "TAP_EXACTTARGET_TENANT_SUBDOMAIN",
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {"start_date": "2022-07-01T00:00:00Z"}
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value