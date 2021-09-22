import unittest
from unittest import mock
import tap_exacttarget.client as _client

class Mockresponse:
    def __init__(self, json):
        self.results = json
        self.more_results = False

class TestGetResponseItems(unittest.TestCase):

    @mock.patch("tap_exacttarget.client.LOGGER.info")
    def test_result_without_count(self, mocked_logger):
        json = [{
            "key1": "value1",
            "key2": "value2"
        },{
            "key3": "value3",
            "key4": "value4"
        }]
        items = _client._get_response_items(Mockresponse(json), "TestDataAccessObject")
        self.assertEquals(items, json)
        mocked_logger.assert_called_with("Got %s results from %s endpoint.", 2, "TestDataAccessObject")

    @mock.patch("tap_exacttarget.client.LOGGER.info")
    def test_result_with_count(self, mocked_logger):
        json = {
            "count": 2,
            "items": [{
                    "key1": "value1",
                    "key2": "value2"
                },{
                    "key3": "value3",
                    "key4": "value4"
                }
            ]
        }
        items = _client._get_response_items(Mockresponse(json), "TestDataAccessObject")
        self.assertEquals(items, json.get("items"))
        mocked_logger.assert_called_with("Got %s results from %s endpoint.", 2, "TestDataAccessObject")
