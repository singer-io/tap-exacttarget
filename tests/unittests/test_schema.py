import unittest
import tap_exacttarget.dao as dao
from unittest import mock

class TestSchema(unittest.TestCase):

    @mock.patch("tap_exacttarget.dao.get_abs_path")
    @mock.patch("singer.utils.load_json")
    def test_load_schema(self, mocked_load_json, mocked_get_abs_path):
        field_schema = {
            "type": "object",
            "properties": {
                "field": {
                    "type": ["null", "string"]
                }
            }
        }

        # mock singer.utils.load_json and return 'field_schema'
        mocked_load_json.return_value = field_schema.copy()

        # call actual function
        schema = dao.load_schema("test")

        # verify if the 'schema' is same as 'field_schema'
        self.assertEquals(schema, field_schema)

    @mock.patch("tap_exacttarget.dao.get_abs_path")
    @mock.patch("singer.utils.load_json")
    def test_load_schema_references(self, mocked_load_json, mocked_get_abs_path):
        field_schema = {
            "type": "object",
            "properties": {
                "field1": {
                    "type": ["null", "string"]
                },
                "field2": {
                    "type": ["null", "string"]
                }
            }
        }

        # mock singer.utils.load_json and return 'field_schema'
        mocked_load_json.return_value = field_schema.copy()

        # call the actual function
        schema = dao.load_schema_references()

        # make data for assertion
        expected_schema = {}
        expected_schema["definations.json"] = field_schema

        # verify if the 'schema' is same as 'field_schema'
        self.assertEquals(schema, expected_schema)
