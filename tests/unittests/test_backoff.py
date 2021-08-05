import unittest
import FuelSDK
import socket
import time
import singer
from unittest import mock
from tap_exacttarget.endpoints.emails import EmailDataAccessObject

class Mockresponse:
    def __init__(self, status, json):
        self.status = status
        self.results = json
        self.more_results = False

def get_response(status, json={}):
    return Mockresponse(status, json)

@mock.patch("FuelSDK.ET_Email.get")
@mock.patch("time.sleep")
class TestConnectionResetError(unittest.TestCase):

    def test_connection_reset_error_occurred(self, mocked_sleep, mocked_get):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        email = EmailDataAccessObject({}, {}, None, {})
        try:
            email.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred(self, mocked_write_records, mocked_sleep, mocked_get):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        email = EmailDataAccessObject({}, {}, None, {})
        email.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

@mock.patch("FuelSDK.ET_Email.get")
@mock.patch("time.sleep")
class TestSocketTimeoutError(unittest.TestCase):

    def test_socket_timeout_error_occurred(self, mocked_sleep, mocked_get):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        email = EmailDataAccessObject({}, {}, None, {})
        try:
            email.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred(self, mocked_write_records, mocked_sleep, mocked_get):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        email = EmailDataAccessObject({}, {}, None, {})
        email.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)
