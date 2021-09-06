import unittest
import socket
from unittest import mock
from tap_exacttarget.endpoints import (
    campaigns, content_areas, data_extensions,
    emails, events, folders, list_sends,
    list_subscribers, lists, sends, subscribers)

class Mockresponse:
    def __init__(self, status, json):
        self.status = status
        self.results = json
        self.more_results = False

def get_response(status, json={}):
    return Mockresponse(status, json)

@mock.patch("time.sleep")
class TestConnectionResetError(unittest.TestCase):

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__content_area(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__content_area(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    def test_connection_reset_error_occurred__campaign(self, mocked_get_rest, mocked_sleep):
        mocked_get_rest.side_effect = socket.error(104, 'Connection reset by peer')
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get_rest.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__campaign(self, mocked_write_records, mocked_get_rest, mocked_sleep):
        mocked_get_rest.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__data_extension(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {
            "stream": "data_extention.e1",
            "tap_stream_id": "data_extention.e1",
            "schema": {
                "properties": {
                    "id": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "CategoryID": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            }})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__data_extension_get_extensions(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_extensions()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Column.get")
    def test_connection_reset_error_occurred__data_extension_get_fields(self, mocked_data_ext_column, mocked_sleep):
        mocked_data_ext_column.side_effect = socket.error(104, 'Connection reset by peer')
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_fields([])
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Row.get")
    def test_connection_reset_error_occurred__data_extension_replicate(self, mocked_data_ext_column, mocked_sleep):
        mocked_data_ext_column.side_effect = socket.error(104, 'Connection reset by peer')
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._replicate(None, None, None, None)
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__email(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__email(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__events(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = events.EventDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__folder(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__folder(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list_send(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__list_send(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("tap_exacttarget.endpoints.list_subscribers.ListSubscriberDataAccessObject._get_all_subscribers_list")
    def test_connection_reset_error_occurred__list_subscriber(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_all_subscribers_list()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_no_connection_reset_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        json = {
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }
        mocked_get.side_effect = [get_response(True, [json])]
        obj = list_subscribers.ListSubscriberDataAccessObject({}, {}, None, {})
        actual = obj._get_all_subscribers_list()
        # verify if the record was returned as response
        self.assertEquals(actual, json)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = lists.ListDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__list(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = lists.ListDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__sends(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = sends.SendDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__sends(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = sends.SendDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__subscriber(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        try:
            obj.pull_subscribers_batch(['sub1'])
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_connection_reset_error_occurred__subscriber(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        obj.pull_subscribers_batch(['sub1'])
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

@mock.patch("time.sleep")
class TestSocketTimeoutError(unittest.TestCase):

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__content_area(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__content_area(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    def test_socket_timeout_error_occurred__campaign(self, mocked_get_rest, mocked_sleep):
        mocked_get_rest.side_effect = socket.timeout("The read operation timed out")
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get_rest.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__campaign(self, mocked_write_records, mocked_get_rest, mocked_sleep):
        mocked_get_rest.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__data_extension(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {
            "stream": "data_extention.e1",
            "tap_stream_id": "data_extention.e1",
            "schema": {
                "properties": {
                    "id": {
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    "CategoryID": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            }})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__data_extension_get_extensions(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_extensions()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Column.get")
    def test_socket_timeout_error_occurred__data_extension_get_fields(self, mocked_data_ext_column, mocked_sleep):
        mocked_data_ext_column.side_effect = socket.timeout("The read operation timed out")
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_fields([])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Row.get")
    def test_socket_timeout_error_occurred__data_extension_replicate(self, mocked_data_ext_column, mocked_sleep):
        mocked_data_ext_column.side_effect = socket.timeout("The read operation timed out")
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._replicate(None, None, None, None)
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__email(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__email(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__events(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = events.EventDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__folder(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__folder(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list_send(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__list_send(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("tap_exacttarget.endpoints.list_subscribers.ListSubscriberDataAccessObject._get_all_subscribers_list")
    def test_socket_timeout_error_occurred__list_subscriber(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            obj._get_all_subscribers_list()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_no_socket_timeout_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        json = {
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }
        mocked_get.side_effect = [get_response(True, [json])]
        obj = list_subscribers.ListSubscriberDataAccessObject({}, {}, None, {})
        actual = obj._get_all_subscribers_list()
        # verify if the record was returned as response
        self.assertEquals(actual, json)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = lists.ListDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__list(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = lists.ListDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__sends(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = sends.SendDataAccessObject({}, {}, None, {})
        try:
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__sends(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = sends.SendDataAccessObject({}, {}, None, {})
        obj.sync_data()
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__subscriber(self, mocked_get, mocked_sleep):
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        try:
            obj.pull_subscribers_batch(['sub1'])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("singer.write_records")
    def test_no_socket_timeout_error_occurred__subscriber(self, mocked_write_records, mocked_get, mocked_sleep):
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        obj.pull_subscribers_batch(['sub1'])
        # verify if 'singer.write_records' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)
