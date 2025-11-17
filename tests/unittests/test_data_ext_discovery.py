import unittest
from unittest.mock import Mock, patch

from tap_exacttarget.discover_dataextensionobj import (
    detect_field_schema,
    discover_fields,
    discover_dao_streams,
)
from tap_exacttarget.exceptions import MarketingCloudError
from tap_exacttarget.streams import DataExtensionObjectFt, DataExtensionObjectInc
from .base_test import BaseClientTest


class TestDetectFieldSchema(BaseClientTest):
    """Tests for detect_field_schema function."""

    def test_boolean_field(self):
        field = {"FieldType": "Boolean"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "boolean"]})

    def test_decimal_field(self):
        field = {"FieldType": "Decimal"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "number"], "format": "singer.decimal"})

    def test_number_field(self):
        field = {"FieldType": "Number"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "integer"]})

    def test_email_address_field(self):
        field = {"FieldType": "EmailAddress"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"]})

    def test_text_field(self):
        field = {"FieldType": "Text"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"]})

    def test_date_field(self):
        field = {"FieldType": "Date"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"], "format": "date-time"})

    def test_unknown_field_type_defaults_to_string(self):
        field = {"FieldType": "UnknownType"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"]})

    def test_phone_field_defaults_to_string(self):
        field = {"FieldType": "Phone"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"]})

    def test_locale_field_defaults_to_string(self):
        field = {"FieldType": "Locale"}
        result = detect_field_schema(field)
        self.assertEqual(result, {"type": ["null", "string"]})


class TestDiscoverFields(unittest.TestCase):
    """Tests for discover_fields function."""

    def test_single_page_response_with_primary_key(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "EmailAddress",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "EmailAddress",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
                {
                    "Name": "FirstName",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Text",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertIn("ext-001", result)
        self.assertEqual(result["ext-001"]["key_properties"], ["EmailAddress"])
        self.assertEqual(result["ext-001"]["valid_replication_keys"], [])
        self.assertIn("EmailAddress", result["ext-001"]["properties"])
        self.assertIn("FirstName", result["ext-001"]["properties"])

    def test_modified_date_as_replication_key(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Id",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
                {
                    "Name": "ModifiedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertEqual(result["ext-001"]["valid_replication_keys"], ["ModifiedDate"])

    def test_join_date_as_replication_key(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Id",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-002"},
                },
                {
                    "Name": "JoinDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-002"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertEqual(result["ext-002"]["valid_replication_keys"], ["JoinDate"])

    def test_underscore_modified_date_as_replication_key(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Id",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-003"},
                },
                {
                    "Name": "_ModifiedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-003"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertEqual(result["ext-003"]["valid_replication_keys"], ["_ModifiedDate"])

    def test_underscore_created_date_as_replication_key(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Id",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-004"},
                },
                {
                    "Name": "_CreatedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-004"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertEqual(result["ext-004"]["valid_replication_keys"], ["_CreatedDate"])

    def test_all_replication_keys_present(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Id",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-005"},
                },
                {
                    "Name": "ModifiedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-005"},
                },
                {
                    "Name": "JoinDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-005"},
                },
                {
                    "Name": "_ModifiedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-005"},
                },
                {
                    "Name": "_CreatedDate",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-005"},
                },
            ],
        }

        result = discover_fields(mock_client)

        repl_keys = result["ext-005"]["valid_replication_keys"]
        self.assertIn("ModifiedDate", repl_keys)
        self.assertIn("JoinDate", repl_keys)
        self.assertIn("_ModifiedDate", repl_keys)
        self.assertIn("_CreatedDate", repl_keys)
        self.assertEqual(len(repl_keys), 4)

    def test_multiple_primary_keys(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "EmailAddress",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "EmailAddress",
                    "DataExtension": {"CustomerKey": "ext-006"},
                },
                {
                    "Name": "SubscriberKey",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Text",
                    "DataExtension": {"CustomerKey": "ext-006"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertIn("EmailAddress", result["ext-006"]["key_properties"])
        self.assertIn("SubscriberKey", result["ext-006"]["key_properties"])
        self.assertEqual(len(result["ext-006"]["key_properties"]), 2)

    def test_multiple_pages_response(self):
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            {
                "RequestID": "req-123",
                "OverallStatus": "MoreDataAvailable",
                "Results": [
                    {
                        "Name": "Field1",
                        "IsRequired": False,
                        "IsPrimaryKey": True,
                        "FieldType": "Text",
                        "DataExtension": {"CustomerKey": "ext-001"},
                    }
                ],
            },
            {
                "RequestID": "req-456",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "Field2",
                        "IsRequired": False,
                        "IsPrimaryKey": False,
                        "FieldType": "Number",
                        "DataExtension": {"CustomerKey": "ext-001"},
                    }
                ],
            },
        ]

        result = discover_fields(mock_client)

        self.assertEqual(mock_client.retrieve_request.call_count, 2)
        self.assertIn("Field1", result["ext-001"]["properties"])
        self.assertIn("Field2", result["ext-001"]["properties"])

    def test_multiple_data_extensions(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Email",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "EmailAddress",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
                {
                    "Name": "UserId",
                    "IsRequired": True,
                    "IsPrimaryKey": True,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-002"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertEqual(len(result), 2)
        self.assertIn("ext-001", result)
        self.assertIn("ext-002", result)

    def test_all_field_types_mapped_correctly(self):
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "BoolField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Boolean",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
                {
                    "Name": "DecimalField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Decimal",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
                {
                    "Name": "NumberField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Number",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
                {
                    "Name": "EmailField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "EmailAddress",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
                {
                    "Name": "TextField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Text",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
                {
                    "Name": "DateField",
                    "IsRequired": False,
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-007"},
                },
            ],
        }

        result = discover_fields(mock_client)

        props = result["ext-007"]["properties"]
        self.assertEqual(props["BoolField"]["type"], ["null", "boolean"])
        self.assertEqual(props["DecimalField"]["type"], ["null", "number"])
        self.assertEqual(props["NumberField"]["type"], ["null", "integer"])
        self.assertEqual(props["EmailField"]["type"], ["null", "string"])
        self.assertEqual(props["TextField"]["type"], ["null", "string"])
        self.assertEqual(props["DateField"]["type"], ["null", "string"])


class TestDiscoverDaoStreams(unittest.TestCase):
    """Tests for discover_dao_streams function."""

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_incremental_stream_with_modified_date(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-001": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["ModifiedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "ModifiedDate": {"type": ["null", "string"], "format": "date-time"},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-001", "Name": "TestExtension", "CategoryID": 100}
            ],
        }

        result = discover_dao_streams(mock_client)

        self.assertIn("data_extension_testextension", result)
        stream_class = result["data_extension_testextension"]
        self.assertEqual(stream_class.replication_key, "ModifiedDate")
        self.assertEqual(stream_class.valid_replication_keys, ["ModifiedDate"])
        self.assertTrue(issubclass(stream_class, DataExtensionObjectInc))

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_incremental_stream_with_join_date(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-002": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["JoinDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "JoinDate": {"type": ["null", "string"], "format": "date-time"},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-002", "Name": "SubscriberExt", "CategoryID": 200}
            ],
        }

        result = discover_dao_streams(mock_client)

        stream_class = result["data_extension_subscriberext"]
        self.assertEqual(stream_class.replication_key, "JoinDate")
        self.assertEqual(stream_class.valid_replication_keys, ["JoinDate"])

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_incremental_stream_with_underscore_modified_date(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-003": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["_ModifiedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "_ModifiedDate": {"type": ["null", "string"], "format": "date-time"},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-003", "Name": "SystemExt", "CategoryID": 300}
            ],
        }

        result = discover_dao_streams(mock_client)

        stream_class = result["data_extension_systemext"]
        self.assertEqual(stream_class.replication_key, "_ModifiedDate")
        self.assertEqual(stream_class.valid_replication_keys, ["_ModifiedDate"])

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_incremental_stream_with_underscore_created_date(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-004": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["_CreatedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "_CreatedDate": {"type": ["null", "string"], "format": "date-time"},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-004", "Name": "CreatedExt", "CategoryID": 400}
            ],
        }

        result = discover_dao_streams(mock_client)

        stream_class = result["data_extension_createdext"]
        self.assertEqual(stream_class.replication_key, "_CreatedDate")
        self.assertEqual(stream_class.valid_replication_keys, ["_CreatedDate"])

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_replication_key_priority_modified_date_first(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-005": {
                "key_properties": ["Id"],
                "valid_replication_keys": [
                    "_CreatedDate",
                    "JoinDate",
                    "ModifiedDate",
                    "_ModifiedDate",
                ],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "ModifiedDate": {"type": ["null", "string"]},
                    "JoinDate": {"type": ["null", "string"]},
                    "_ModifiedDate": {"type": ["null", "string"]},
                    "_CreatedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-005", "Name": "TestExt", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext"]
        self.assertEqual(stream_class.replication_key, "ModifiedDate")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_replication_key_priority_join_date_second(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-006": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["_CreatedDate", "JoinDate", "_ModifiedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "JoinDate": {"type": ["null", "string"]},
                    "_ModifiedDate": {"type": ["null", "string"]},
                    "_CreatedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-006", "Name": "TestExt2", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext2"]
        self.assertEqual(stream_class.replication_key, "JoinDate")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_replication_key_priority_underscore_modified_third(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-007": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["_CreatedDate", "_ModifiedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "_ModifiedDate": {"type": ["null", "string"]},
                    "_CreatedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-007", "Name": "TestExt3", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext3"]
        self.assertEqual(stream_class.replication_key, "_ModifiedDate")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_replication_key_priority_underscore_created_last(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-008": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["_CreatedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "_CreatedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-008", "Name": "TestExt4", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext4"]
        self.assertEqual(stream_class.replication_key, "_CreatedDate")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_full_table_stream_no_replication_key(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-009": {
                "key_properties": ["Id"],
                "valid_replication_keys": [],
                "properties": {"Id": {"type": ["null", "integer"]}},
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-009", "Name": "StaticExtension", "CategoryID": 500}
            ],
        }

        result = discover_dao_streams(mock_client)

        stream_class = result["data_extension_staticextension"]
        self.assertIsNone(stream_class.replication_key)
        self.assertEqual(stream_class.valid_replication_keys, [])
        self.assertTrue(issubclass(stream_class, DataExtensionObjectFt))

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_key_properties_includes_custom_object_key(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-010": {
                "key_properties": ["Id", "Email"],
                "valid_replication_keys": [],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "Email": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-010", "Name": "ContactExt", "CategoryID": 100}
            ],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_contactext"]

        self.assertIn("_CustomObjectKey", stream_class.key_properties)
        self.assertIn("Id", stream_class.key_properties)
        self.assertIn("Email", stream_class.key_properties)

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_key_properties_are_sorted_alphabetically(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-011": {
                "key_properties": ["Zebra", "Apple", "Middle", "Banana"],
                "valid_replication_keys": [],
                "properties": {},
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-011", "Name": "TestExt", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext"]

        expected = ["Apple", "Banana", "Middle", "Zebra", "_CustomObjectKey"]
        self.assertEqual(stream_class.key_properties, sorted(expected))

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_key_properties_duplicates_removed(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-012": {
                "key_properties": ["Id", "Id", "Email"],
                "valid_replication_keys": [],
                "properties": {},
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-012", "Name": "TestExt", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext"]

        self.assertEqual(len(stream_class.key_properties), len(set(stream_class.key_properties)))
        self.assertIn("Id", stream_class.key_properties)
        self.assertIn("Email", stream_class.key_properties)
        self.assertIn("_CustomObjectKey", stream_class.key_properties)

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_stream_name_sanitization(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-013": {
                "key_properties": [],
                "valid_replication_keys": [],
                "properties": {},
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "CustomerKey": "customer-key-013",
                    "Name": "Test Extension!@# 123",
                    "CategoryID": 100,
                }
            ],
        }

        result = discover_dao_streams(mock_client)

        self.assertIn("data_extension_test extension!@# 123", result)
        stream_class = result["data_extension_test extension!@# 123"]
        self.assertEqual(stream_class.__name__, "DataExtensionObjStreamtestextension123")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_multiple_data_extensions(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "key-001": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["ModifiedDate"],
                "properties": {"Id": {"type": ["null", "integer"]}},
            },
            "key-002": {
                "key_properties": ["UserId"],
                "valid_replication_keys": [],
                "properties": {"UserId": {"type": ["null", "integer"]}},
            },
            "key-003": {
                "key_properties": ["Email"],
                "valid_replication_keys": ["JoinDate"],
                "properties": {"Email": {"type": ["null", "string"]}},
            },
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "key-001", "Name": "Extension1", "CategoryID": 100},
                {"CustomerKey": "key-002", "Name": "Extension2", "CategoryID": 200},
                {"CustomerKey": "key-003", "Name": "Extension3", "CategoryID": 300},
            ],
        }

        result = discover_dao_streams(mock_client)

        self.assertEqual(len(result), 3)
        self.assertIn("data_extension_Extension1", result)
        self.assertIn("data_extension_Extension2", result)
        self.assertIn("data_extension_Extension3", result)


        stream1 = result["data_extension_extension1"]
        self.assertEqual(stream1.replication_key, "ModifiedDate")
        self.assertTrue(issubclass(stream1, DataExtensionObjectInc))

        stream2 = result["data_extension_extension2"]
        self.assertIsNone(stream2.replication_key)
        self.assertTrue(issubclass(stream2, DataExtensionObjectFt))

        stream3 = result["data_extension_extension3"]
        self.assertEqual(stream3.replication_key, "JoinDate")
        self.assertTrue(issubclass(stream3, DataExtensionObjectInc))

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_multiple_pages(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "key-001": {
                "key_properties": ["Id"],
                "valid_replication_keys": [],
                "properties": {"Id": {"type": ["null", "integer"]}},
            },
            "key-002": {
                "key_properties": ["UserId"],
                "valid_replication_keys": [],
                "properties": {"UserId": {"type": ["null", "integer"]}},
            },
        }

        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            {
                "RequestID": "req-123",
                "OverallStatus": "MoreDataAvailable",
                "Results": [{"CustomerKey": "key-001", "Name": "Extension1", "CategoryID": 100}],
            },
            {
                "RequestID": "req-456",
                "OverallStatus": "OK",
                "Results": [{"CustomerKey": "key-002", "Name": "Extension2", "CategoryID": 200}],
            },
        ]

        result = discover_dao_streams(mock_client)

        self.assertEqual(len(result), 2)
        self.assertIn("data_extension_extension1", result)
        self.assertIn("data_extension_extension2", result)

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_error_status_raises_exception(self, mock_discover_fields):
        mock_discover_fields.return_value = {}

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "Error: Unauthorized",
            "Results": [],
        }

        with self.assertRaises(MarketingCloudError) as context:
            discover_dao_streams(mock_client)

        self.assertIn("Request failed with status", str(context.exception))
        self.assertIn("Unable to discover Streams", str(context.exception))

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_schema_includes_custom_object_key_and_category_id(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-014": {
                "key_properties": [],
                "valid_replication_keys": [],
                "properties": {
                    "Field1": {"type": ["null", "string"]},
                    "Field2": {"type": ["null", "integer"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-014", "Name": "TestExt", "CategoryID": 500}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_testext"]

        self.assertIn("_CustomObjectKey", stream_class.schema["properties"])
        self.assertIn("CategoryID", stream_class.schema["properties"])
        self.assertIn("Field1", stream_class.schema["properties"])
        self.assertIn("Field2", stream_class.schema["properties"])

        self.assertEqual(stream_class.schema["properties"]["_CustomObjectKey"]["type"], ["string"])
        self.assertEqual(
            stream_class.schema["properties"]["CategoryID"]["type"], ["null", "integer"]
        )

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_stream_attributes_set_correctly(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-015": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["ModifiedDate", "JoinDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "ModifiedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-015", "Name": "MyExtension", "CategoryID": 999}
            ],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_myextension"]

        self.assertEqual(stream_class.stream, "data_extension_myextension")
        self.assertEqual(stream_class.tap_stream_id, "data_extension_myextension")
        self.assertEqual(stream_class.object_ref, "DataExtensionObject[MyExtension]")
        self.assertEqual(stream_class.customer_key, "customer-key-015")
        self.assertEqual(stream_class.category_id, 999)
        self.assertEqual(stream_class.valid_replication_keys, ["ModifiedDate", "JoinDate"])

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_empty_fields_creates_minimal_schema(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-016": {
                "key_properties": [],
                "valid_replication_keys": [],
                "properties": {},
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [{"CustomerKey": "customer-key-016", "Name": "EmptyExt", "CategoryID": 100}],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_emptyext"]

        self.assertIn("_CustomObjectKey", stream_class.schema["properties"])
        self.assertIn("CategoryID", stream_class.schema["properties"])
        self.assertEqual(len(stream_class.schema["properties"]), 2)

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_data_extension_with_all_field_types(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-017": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["ModifiedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "IsActive": {"type": ["null", "boolean"]},
                    "Price": {"type": ["null", "number"], "format": "singer.decimal"},
                    "Quantity": {"type": ["null", "integer"]},
                    "Email": {"type": ["null", "string"]},
                    "Description": {"type": ["null", "string"]},
                    "ModifiedDate": {"type": ["null", "string"], "format": "date-time"},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-017", "Name": "CompleteExt", "CategoryID": 100}
            ],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_completeext"]

        props = stream_class.schema["properties"]
        self.assertEqual(props["Id"]["type"], ["null", "integer"])
        self.assertEqual(props["IsActive"]["type"], ["null", "boolean"])
        self.assertEqual(props["Price"]["type"], ["null", "number"])
        self.assertEqual(props["Price"]["format"], "singer.decimal")
        self.assertEqual(props["Quantity"]["type"], ["null", "integer"])
        self.assertEqual(props["Email"]["type"], ["null", "string"])
        self.assertEqual(props["Description"]["type"], ["null", "string"])
        self.assertEqual(props["ModifiedDate"]["type"], ["null", "string"])
        self.assertEqual(props["ModifiedDate"]["format"], "date-time")

    @patch("tap_exacttarget.discover_dataextensionobj.discover_fields")
    def test_valid_replication_keys_preserved_in_stream(self, mock_discover_fields):
        mock_discover_fields.return_value = {
            "customer-key-018": {
                "key_properties": ["Id"],
                "valid_replication_keys": ["ModifiedDate", "JoinDate", "_CreatedDate"],
                "properties": {
                    "Id": {"type": ["null", "integer"]},
                    "ModifiedDate": {"type": ["null", "string"]},
                    "JoinDate": {"type": ["null", "string"]},
                    "_CreatedDate": {"type": ["null", "string"]},
                },
            }
        }

        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {"CustomerKey": "customer-key-018", "Name": "MultiKeyExt", "CategoryID": 100}
            ],
        }

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_multikeyext"]


        self.assertIn("ModifiedDate", stream_class.valid_replication_keys)
        self.assertIn("JoinDate", stream_class.valid_replication_keys)
        self.assertIn("_CreatedDate", stream_class.valid_replication_keys)
        self.assertEqual(len(stream_class.valid_replication_keys), 3)

        self.assertEqual(stream_class.replication_key, "ModifiedDate")
