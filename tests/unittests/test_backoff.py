import unittest
import socket
from unittest import mock
from tap_exacttarget.endpoints import (
    campaigns, content_areas, data_extensions,
    emails, events, folders, list_sends,
    list_subscribers, lists, sends, subscribers)

# prepare mock response
class Mockresponse:
    def __init__(self, status, json):
        self.status = status
        self.results = json
        self.more_results = False

# get mock response
def get_response(status, json={}):
    return Mockresponse(status, json)

@mock.patch("time.sleep")
class TestConnectionResetError(unittest.TestCase):
    """
        Tests for verifying that the backoff is working as expected for 'ConnectionResetError'
    """

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__content_area(self, mocked_get, mocked_sleep):
        # mocked 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'ContentAreaDataAccessObject'
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__content_area(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ContentAreaDataAccessObject'
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    def test_connection_reset_error_occurred__campaign(self, mocked_get_rest, mocked_sleep):
        # mock 'get' and raise error
        mocked_get_rest.side_effect = socket.error(104, 'Connection reset by peer')
        # # make the object of 'CampaignDataAccessObject'
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get_rest.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__campaign(self, mocked_write_records, mocked_get_rest, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get_rest.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'CampaignDataAccessObject'
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__data_extension(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {
            # dummy catalog file
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
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__data_extension_get_extensions(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._get_extensions()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Column.get")
    def test_connection_reset_error_occurred__data_extension_get_fields(self, mocked_data_ext_column, mocked_sleep):
        # mock 'get' and raise error
        mocked_data_ext_column.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._get_fields([])
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Row.get")
    def test_connection_reset_error_occurred__data_extension_replicate(self, mocked_data_ext_column, mocked_sleep):
        # mock 'get' and raise error
        mocked_data_ext_column.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._replicate(None, None, None, None)
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__email(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'EmailDataAccessObject'
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        try:
            # call function
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__email(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'EmailDataAccessObject'
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__events(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'EventDataAccessObject'
        obj = events.EventDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__folder(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'FolderDataAccessObject'
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__folder(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'FolderDataAccessObject'
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list_send(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'ListSendDataAccessObject'
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__list_send(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ListSendDataAccessObject'
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("tap_exacttarget.endpoints.list_subscribers.ListSubscriberDataAccessObject._get_all_subscribers_list")
    def test_connection_reset_error_occurred__list_subscriber(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
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
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [json])]
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({}, {}, None, {})
        # call function
        actual = obj._get_all_subscribers_list()
        # verify if the record was returned as response
        self.assertEquals(actual, json)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__list(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'ListDataAccessObject'
        obj = lists.ListDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__list(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ListDataAccessObject'
        obj = lists.ListDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__sends(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'SendDataAccessObject'
        obj = sends.SendDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__sends(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'SendDataAccessObject'
        obj = sends.SendDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_connection_reset_error_occurred__subscriber(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.error(104, 'Connection reset by peer')
        # make the object of 'SubscriberDataAccessObject'
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        try:
            # call function
            obj.pull_subscribers_batch(['sub1'])
        except ConnectionError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_connection_reset_error_occurred__subscriber(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'SubscriberDataAccessObject'
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        # call function
        obj.pull_subscribers_batch(['sub1'])
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

@mock.patch("time.sleep")
class TestSocketTimeoutError(unittest.TestCase):
    """
        Tests for verifying that the backoff is working as expected for 'socket.timeout' error
    """

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__content_area(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'ContentAreaDataAccessObject'
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__content_area(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ContentAreaDataAccessObject'
        obj = content_areas.ContentAreaDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    def test_socket_timeout_error_occurred__campaign(self, mocked_get_rest, mocked_sleep):
        # mock 'get' and raise error
        mocked_get_rest.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'CampaignDataAccessObject'
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get_rest.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupportRest.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__campaign(self, mocked_write_records, mocked_get_rest, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get_rest.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'CampaignDataAccessObject'
        obj = campaigns.CampaignDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__data_extension(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {
            # dummy catalog file
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
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__data_extension_get_extensions(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._get_extensions()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Column.get")
    def test_socket_timeout_error_occurred__data_extension_get_fields(self, mocked_data_ext_column, mocked_sleep):
        # mock 'get' and raise error
        mocked_data_ext_column.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._get_fields([])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.objects.ET_DataExtension_Row.get")
    def test_socket_timeout_error_occurred__data_extension_replicate(self, mocked_data_ext_column, mocked_sleep):
        # mock 'get' and raise error
        mocked_data_ext_column.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'DataExtensionDataAccessObject'
        obj = data_extensions.DataExtensionDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
            obj._replicate(None, None, None, None)
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_data_ext_column.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__email(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # # make the object of 'EmailDataAccessObject'
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__email(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'EmailDataAccessObject'
        obj = emails.EmailDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__events(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'EventDataAccessObject'
        obj = events.EventDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__folder(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'FolderDataAccessObject'
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__folder(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'FolderDataAccessObject'
        obj = folders.FolderDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list_send(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'ListSendDataAccessObject'
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__list_send(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ListSendDataAccessObject'
        obj = list_sends.ListSendDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("tap_exacttarget.endpoints.list_subscribers.ListSubscriberDataAccessObject._get_all_subscribers_list")
    def test_socket_timeout_error_occurred__list_subscriber(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list_subscriber__get_all_subscribers_list(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({"start_date": "2020-01-01T00:00:00Z"}, {}, None, {})
        try:
            # call function
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
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [json])]
        # make the object of 'ListSubscriberDataAccessObject'
        obj = list_subscribers.ListSubscriberDataAccessObject({}, {}, None, {})
        # call function
        actual = obj._get_all_subscribers_list()
        # verify if the record was returned as response
        self.assertEquals(actual, json)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__list(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'ListDataAccessObject'
        obj = lists.ListDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__list(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'ListDataAccessObject'
        obj = lists.ListDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__sends(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'SendDataAccessObject'
        obj = sends.SendDataAccessObject({}, {}, None, {})
        try:
            # call sync
            obj.sync_data()
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__sends(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'SendDataAccessObject'
        obj = sends.SendDataAccessObject({}, {}, None, {})
        # call sync
        obj.sync_data()
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    def test_socket_timeout_error_occurred__subscriber(self, mocked_get, mocked_sleep):
        # mock 'get' and raise error
        mocked_get.side_effect = socket.timeout("The read operation timed out")
        # make the object of 'SubscriberDataAccessObject'
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        try:
            # call function
            obj.pull_subscribers_batch(['sub1'])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEquals(mocked_get.call_count, 5)

    @mock.patch("FuelSDK.rest.ET_GetSupport.get")
    @mock.patch("tap_exacttarget.dao.DataAccessObject.write_records_with_transform")
    def test_no_socket_timeout_error_occurred__subscriber(self, mocked_write_records, mocked_get, mocked_sleep):
        # mock 'get' and return the dummy data
        mocked_get.side_effect = [get_response(True, [{
            "CategoryID": 12345,
            "ContentCheckStatus": "Not Checked",
            "CreatedDate": "2021-01-01T00:00:00Z",
            "EmailType": "Normal"
        }])]
        # make the object of 'SubscriberDataAccessObject'
        obj = subscribers.SubscriberDataAccessObject({}, {}, None, {})
        # call function
        obj.pull_subscribers_batch(['sub1'])
        # verify if 'tap_exacttarget.dao.DataAccessObject.write_records_with_transform' was called
        # once as there is only one record
        self.assertEquals(mocked_write_records.call_count, 1)
