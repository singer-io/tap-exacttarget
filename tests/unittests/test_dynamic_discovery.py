import unittest
from unittest.mock import Mock
from tap_exacttarget.discover_dataextensionobj import (
    detect_field_schema,
    discover_fields,
    discover_dao_streams,
    DataExtensionObjectFt,
    DataExtensionObjectInc,
    MarketingCloudError,
)


class TestDiscoverFields(unittest.TestCase):
    """Test field discovery with pagination and key detection."""

    def test_boolean_field_mapping(self):
        """Test Boolean field type is correctly mapped to boolean."""
        field = {"FieldType": "Boolean"}
        schema = detect_field_schema(field)
        self.assertEqual(schema["type"], ["null", "boolean"])
        self.assertNotIn("format", schema)

    def test_decimal_field_with_format(self):
        """Test Decimal field includes both type and format."""
        field = {"FieldType": "Decimal"}
        schema = detect_field_schema(field)
        self.assertEqual(schema["type"], ["null", "number"])
        self.assertEqual(schema["format"], "singer.decimal")

    def test_date_field_with_format(self):
        """Test Date field maps to string with date-time format."""
        field = {"FieldType": "Date"}
        schema = detect_field_schema(field)
        self.assertEqual(schema["type"], ["null", "string"])
        self.assertEqual(schema["format"], "date-time")

    def test_unknown_field_defaults_to_string(self):
        """Test unknown field types default to string."""
        field = {"FieldType": "UnknownType"}
        schema = detect_field_schema(field)
        self.assertEqual(schema["type"], ["null", "string"])
        self.assertNotIn("format", schema)

    def test_single_page_field_discovery(self):
        """Test field discovery with single page of results."""
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "Email",
                    "IsPrimaryKey": True,
                    "FieldType": "EmailAddress",
                    "DataExtension": {"CustomerKey": "ext-001"},
                }
            ],
        }

        result = discover_fields(mock_client)

        mock_client.retrieve_request.assert_called_with(
            "DataExtensionField",
            ["Name", "IsRequired", "IsPrimaryKey", "FieldType", "DataExtension.CustomerKey"],
            request_id=None,
        )

        self.assertIn("ext-001", result)
        self.assertIn("Email", result["ext-001"]["key_properties"])
        self.assertEqual(result["ext-001"]["properties"]["Email"]["type"], ["null", "string"])

    def test_multi_page_field_discovery(self):
        """Test field discovery handles pagination correctly."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            {
                "RequestID": "req-1",
                "OverallStatus": "MoreDataAvailable",
                "Results": [
                    {
                        "Name": "Field1",
                        "IsPrimaryKey": False,
                        "FieldType": "Text",
                        "DataExtension": {"CustomerKey": "ext-001"},
                    }
                ],
            },
            {
                "RequestID": "req-2",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "Field2",
                        "IsPrimaryKey": True,
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
        self.assertIn("Field2", result["ext-001"]["key_properties"])

    def test_replication_key_detection(self):
        """Test that valid replication keys are correctly identified."""
        mock_client = Mock()
        mock_client.retrieve_request.return_value = {
            "RequestID": "req-123",
            "OverallStatus": "OK",
            "Results": [
                {
                    "Name": "ModifiedDate",
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
                {
                    "Name": "_CreatedDate",
                    "IsPrimaryKey": False,
                    "FieldType": "Date",
                    "DataExtension": {"CustomerKey": "ext-001"},
                },
            ],
        }

        result = discover_fields(mock_client)

        self.assertIn("ModifiedDate", result["ext-001"]["valid_replication_keys"])
        self.assertIn("_CreatedDate", result["ext-001"]["valid_replication_keys"])


class TestDiscoverDaoStreams(unittest.TestCase):
    """Test data extension stream discovery and class creation."""

    def test_full_table_stream_creation(self):
        """Test creating full table stream when no replication keys exist."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            # Fields discovery
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "Email",
                        "IsPrimaryKey": True,
                        "FieldType": "EmailAddress",
                        "DataExtension": {"CustomerKey": "customer-key-1"},
                    }
                ],
            },
            # DataExtension discovery
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {"CustomerKey": "customer-key-1", "Name": "TestExtension", "CategoryID": 100}
                ],
            },
        ]

        result = discover_dao_streams(mock_client)

        stream_id = "data_extension_testextension"
        self.assertIn(stream_id, result)
        stream_class = result[stream_id]
        self.assertTrue(issubclass(stream_class, DataExtensionObjectFt))
        self.assertEqual(stream_class.customer_key, "customer-key-1")
        self.assertIsNone(stream_class.replication_key)

    def test_incremental_stream_creation(self):
        """Test creating incremental stream with replication key."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            # Fields discovery
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "ID",
                        "IsPrimaryKey": True,
                        "FieldType": "Number",
                        "DataExtension": {"CustomerKey": "customer-key-2"},
                    },
                    {
                        "Name": "ModifiedDate",
                        "IsPrimaryKey": False,
                        "FieldType": "Date",
                        "DataExtension": {"CustomerKey": "customer-key-2"},
                    },
                ],
            },
            # DataExtension discovery
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "CustomerKey": "customer-key-2",
                        "Name": "IncrementalExtension",
                        "CategoryID": 200,
                    }
                ],
            },
        ]

        result = discover_dao_streams(mock_client)

        stream_id = "data_extension_incrementalextension"
        self.assertIn(stream_id, result)
        stream_class = result[stream_id]
        self.assertTrue(issubclass(stream_class, DataExtensionObjectInc))
        self.assertEqual(stream_class.replication_key, "ModifiedDate")

    def test_replication_key_priority_order(self):
        """Test replication key selection follows priority: ModifiedDate > JoinDate > _ModifiedDate > _CreatedDate"""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            # Fields discovery with multiple replication keys
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "_CreatedDate",
                        "IsPrimaryKey": False,
                        "FieldType": "Date",
                        "DataExtension": {"CustomerKey": "customer-key-3"},
                    },
                    {
                        "Name": "JoinDate",
                        "IsPrimaryKey": False,
                        "FieldType": "Date",
                        "DataExtension": {"CustomerKey": "customer-key-3"},
                    },
                ],
            },
            # DataExtension discovery
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {"CustomerKey": "customer-key-3", "Name": "PriorityTest", "CategoryID": 300}
                ],
            },
        ]

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_prioritytest"]
        # JoinDate should be chosen over _CreatedDate
        self.assertEqual(stream_class.replication_key, "JoinDate")

    def test_event_replication_key_priority(self):
        """Test that EventDatetime/EventDate are preferred over other replication keys."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            # Fields discovery with event and regular replication keys
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "ModifiedDate",
                        "IsPrimaryKey": False,
                        "FieldType": "Date",
                        "DataExtension": {"CustomerKey": "customer-key-6"},
                    },
                    {
                        "Name": "EventDatetime",
                        "IsPrimaryKey": False,
                        "FieldType": "Date",
                        "DataExtension": {"CustomerKey": "customer-key-6"},
                    },
                ],
            },
            # DataExtension discovery
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {"CustomerKey": "customer-key-6", "Name": "EventPriorityTest", "CategoryID": 600}
                ],
            },
        ]

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_eventprioritytest"]
        # EventDatetime should be chosen over ModifiedDate
        self.assertEqual(stream_class.replication_key, "EventDatetime")


    def test_event_replication_key_case_insensitivity(self):
        """Test select_replication_key is case-insensitive and preserves original key casing for returned value.

        Verifies behavior for both all-lowercase and camelCase key names in a single test.
        """
        # lowercase keys
        mock_client_lower = Mock()
        mock_client_lower.retrieve_request.side_effect = [
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {"Name": "modifieddate", "IsPrimaryKey": False, "FieldType": "Date", "DataExtension": {"CustomerKey": "customer-key-7"}},
                    {"Name": "eventdatetime", "IsPrimaryKey": False, "FieldType": "Date", "DataExtension": {"CustomerKey": "customer-key-7"}},
                ],
            },
            {"RequestID": "req-de", "OverallStatus": "OK", "Results": [{"CustomerKey": "customer-key-7", "Name": "EventCaseTestLower", "CategoryID": 700}]},
        ]

        result_lower = discover_dao_streams(mock_client_lower)
        stream_class_lower = result_lower["data_extension_eventcasetestlower"]
        self.assertEqual(stream_class_lower.replication_key, "eventdatetime")

        # camelCase keys
        mock_client_camel = Mock()
        mock_client_camel.retrieve_request.side_effect = [
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {"Name": "ModifiedDate", "IsPrimaryKey": False, "FieldType": "Date", "DataExtension": {"CustomerKey": "customer-key-8"}},
                    {"Name": "eventDatetime", "IsPrimaryKey": False, "FieldType": "Date", "DataExtension": {"CustomerKey": "customer-key-8"}},
                ],
            },
            {"RequestID": "req-de", "OverallStatus": "OK", "Results": [{"CustomerKey": "customer-key-8", "Name": "EventCaseTestCamel", "CategoryID": 800}]},
        ]

        result_camel = discover_dao_streams(mock_client_camel)
        stream_class_camel = result_camel["data_extension_eventcasetestcamel"]
        self.assertEqual(stream_class_camel.replication_key, "eventDatetime")

    def test_special_characters_in_stream_name(self):
        """Test stream names with special characters are sanitized for class names."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            {"RequestID": "req-fields", "OverallStatus": "OK", "Results": []},
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "CustomerKey": "customer-key-4",
                        "Name": "Test-Extension_With$Special!Chars",
                        "CategoryID": 400,
                    }
                ],
            },
        ]

        result = discover_dao_streams(mock_client)

        stream_id = "data_extension_test_extension_with_special_chars"
        self.assertIn(stream_id, result)

        # Verify the class was created successfully despite special chars
        self.assertIsNotNone(result[stream_id])
        stream_class = result[stream_id]
        self.assertTrue(stream_class.__name__.isidentifier())
        self.assertEqual(
            stream_class.__name__, "DataExtensionObjStreamtestextensionwithspecialchars"
        )

    def test_error_status_raises_exception(self):
        """Test that error status in DataExtension discovery raises MarketingCloudError."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            # Fields discovery succeeds
            {"RequestID": "req-fields", "OverallStatus": "OK", "Results": []},
            # DataExtension discovery fails
            {"RequestID": "req-de", "OverallStatus": "Error", "Results": []},
        ]

        with self.assertRaises(MarketingCloudError) as context:
            discover_dao_streams(mock_client)

        self.assertIn("Request failed with status: Error", str(context.exception))

    def test_schema_structure_completeness(self):
        """Test that generated schema includes all required fields."""
        mock_client = Mock()
        mock_client.retrieve_request.side_effect = [
            {
                "RequestID": "req-fields",
                "OverallStatus": "OK",
                "Results": [
                    {
                        "Name": "CustomField",
                        "IsPrimaryKey": False,
                        "FieldType": "Text",
                        "DataExtension": {"CustomerKey": "customer-key-5"},
                    }
                ],
            },
            {
                "RequestID": "req-de",
                "OverallStatus": "OK",
                "Results": [
                    {"CustomerKey": "customer-key-5", "Name": "SchemaTest", "CategoryID": 500}
                ],
            },
        ]

        result = discover_dao_streams(mock_client)
        stream_class = result["data_extension_schematest"]
        schema = stream_class.schema

        # Verify schema structure
        self.assertEqual(schema["type"], "object")
        self.assertIn("_CustomObjectKey", schema["properties"])
        self.assertIn("CategoryID", schema["properties"])
        self.assertIn("CustomField", schema["properties"])
        self.assertEqual(schema["properties"]["CategoryID"]["type"], ["null", "integer"])
