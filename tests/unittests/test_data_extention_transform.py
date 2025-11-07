import unittest
from unittest.mock import patch
from tap_exacttarget.streams.dataextensionobjects import DataExtensionObjectBase


class TestDataExtensionObjectTransform(unittest.TestCase):
    """Focused tests for transform_record() behavior."""

    def setUp(self):
        self.obj = DataExtensionObjectBase()
        self.obj.category_id = 999
        self.obj.schema = {
            "properties": {
                "Id": {"type": ["integer"]},
                "Price": {"type": ["number"]},
                "IsActive": {"type": ["boolean"]},
                "Name": {"type": ["string"]},
                "ExtraField": {"type": ["string"]},
                "CategoryID": {"type": ["integer"]},
            }
        }

    def test_get_query_fields_filters_selected_fields(self):
        """Test that get_query_fields returns correct fields based on metadata."""
        stream_metadata = {
            ("properties", "Id"): {"selected": True},
            ("properties", "Name"): {"selected": False},
            ("properties", "IsActive"): {"selected": True},
            ("properties", "CategoryID"): {"selected": True},
        }

        result = self.obj.get_query_fields(stream_metadata, self.obj.schema)

        self.assertIn("Id", result)
        self.assertIn("IsActive", result)

        # CategoryID is Excluded by default
        self.assertNotIn("CategoryID", result)
        self.assertNotIn("Name", result)

    def test_get_query_fields_inclusion_automatic(self):
        """Test that fields with inclusion=automatic are included."""
        stream_metadata = {
            ("properties", "Id"): {"inclusion": "automatic"},
            ("properties", "Name"): {"selected": False},
        }

        result = self.obj.get_query_fields(stream_metadata, self.obj.schema)
        self.assertEqual(result, ["Id"])

    def test_transform_integer_and_float_conversion(self):
        """Should convert string numbers to int and float types."""
        obj = {
            "Properties": {
                "Property": [{"Name": "Id", "Value": "10"}, {"Name": "Price", "Value": "49.95"}]
            }
        }

        result = self.obj.transform_record(obj)
        self.assertEqual(result["Id"], 10)
        self.assertIsInstance(result["Id"], int)
        self.assertAlmostEqual(result["Price"], 49.95)
        self.assertIsInstance(result["Price"], float)

    def test_transform_boolean_true_false_strings(self):
        """Should correctly interpret truthy and falsy string values."""
        obj = {"Properties": {"Property": [{"Name": "IsActive", "Value": "True"}]}}
        result = self.obj.transform_record(obj)
        self.assertTrue(result["IsActive"])

        obj["Properties"]["Property"][0]["Value"] = "no"
        result = self.obj.transform_record(obj)
        self.assertFalse(result["IsActive"])

    @patch("tap_exacttarget.streams.dataextensionobjects.LOGGER")
    def test_transform_invalid_boolean_value_logs_warning(self, mock_logger):
        """Should log warning and set None for invalid boolean value."""
        obj = {"Properties": {"Property": [{"Name": "IsActive", "Value": "maybe"}]}}

        result = self.obj.transform_record(obj)
        mock_logger.warning.assert_called_once()
        self.assertIsNone(result["IsActive"])

    def test_transform_preserves_none_values(self):
        """Should preserve None values directly."""
        obj = {"Properties": {"Property": [{"Name": "Name", "Value": None}]}}
        result = self.obj.transform_record(obj)
        self.assertIsNone(result["Name"])

    @patch.object(
        DataExtensionObjectBase,
        "schema",
        new_callable=lambda: {"properties": {"properties": {"Id": {"type": ["integer"]}}}},
    )
    def test_transform_unknown_field_ignored(self, mock_schema):
        """Should ignore fields not defined in schema."""
        obj = {
            "Properties": {
                "Property": [{"Name": "UnknownField", "Value": "123"}, {"Name": "Id", "Value": "5"}]
            }
        }

        result = self.obj.transform_record(obj)
        self.assertIn("UnknownField", result)
        self.assertEqual(result["Id"], 5)

    def test_transform_mixed_types_in_one_record(self):
        """Should correctly convert mixed-type properties."""
        obj = {
            "Properties": {
                "Property": [
                    {"Name": "Id", "Value": "1"},
                    {"Name": "Price", "Value": "5.5"},
                    {"Name": "IsActive", "Value": "yes"},
                    {"Name": "Name", "Value": "Widget"},
                ]
            }
        }

        result = self.obj.transform_record(obj)
        self.assertEqual(result["Id"], 1)
        self.assertAlmostEqual(result["Price"], 5.5)
        self.assertTrue(result["IsActive"])
        self.assertEqual(result["Name"], "Widget")

    def test_transform_adds_category_id(self):
        """Should always add CategoryID to result."""
        obj = {"Properties": {"Property": [{"Name": "Id", "Value": "77"}]}}

        result = self.obj.transform_record(obj)
        self.assertIn("CategoryID", result)
        self.assertEqual(result["CategoryID"], 999)
