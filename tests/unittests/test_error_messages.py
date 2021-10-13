from unittest import mock
import unittest
import tap_exacttarget.client as _client
import FuelSDK
import time
import requests

class Mockresponse:
    def __init__(self, json, headers=None):
        self.text = json
        self.headers = headers

    def json(self):
        return self.text

def get_response(json={}):
    return Mockresponse(json)

class TestErrorMessages(unittest.TestCase):

    @mock.patch("requests.post")
    def test_error_1(self, mocked_post_request):
        json = {
            "error": "Client authentication failed."}
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "tenant_subdomain" is not provided, error will be raised after call from v1
            self.assertEquals(
                str(e),
                "Unable to validate App Keys(ClientID/ClientSecret) provided: " + str(json) + ". Please check your 'client_id', 'client_secret' or try adding the 'tenant_subdomain'.")

    @mock.patch("requests.post")
    def test_error_2(self, mocked_post_request):
        json = {
            "error": "Client authentication failed."}
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "tenant_subdomain" is provided, error will be raised after call from v2
            self.assertEquals(
                str(e),
                "Unable to validate App Keys(ClientID/ClientSecret) provided: " + str(json) + ". Please check your 'client_id', 'client_secret' or 'tenant_subdomain'.")

    @mock.patch("requests.post")
    def test_error_3(self, mocked_post_request):
        json = {
            "error": "Client authentication failed."}
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "",
            "client_secret": "",
            "tenant_subdomain": "",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "client_secret" and "client_id" is not provided and
            # "tenant_subdomain" is not provided, error will be raised after call from v1
            self.assertEquals(
                str(e),
                "clientid or clientsecret is null: clientid and clientsecret must be passed when instantiating ET_Client or must be provided in environment variables / config file. Please check your 'client_id', 'client_secret' or try adding the 'tenant_subdomain'.")

    @mock.patch("requests.post")
    def test_error_4(self, mocked_post_request):
        json = {
            "error": "Client authentication failed."}
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "",
            "client_secret": "",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "client_secret" and "client_id" is not provided and
            # "tenant_subdomain" is provided, error will be raised after call from v2
            self.assertEquals(
                str(e),
                "clientid or clientsecret is null: clientid and clientsecret must be passed when instantiating ET_Client or must be provided in environment variables / config file. Please check your 'client_id', 'client_secret' or 'tenant_subdomain'.")

    @mock.patch("FuelSDK.ET_Client")
    def test_error_5(self, mocked_ET_Client):
        mocked_ET_Client.side_effect = requests.exceptions.ConnectionError("Connection Error")
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "tenant_subdomain" is not provided, error will be raised after call from v1
            self.assertEquals(
                str(e),
                "Connection Error. Please check your 'client_id', 'client_secret' or try adding the 'tenant_subdomain'.")

    @mock.patch("FuelSDK.ET_Client")
    def test_error_6(self, mocked_ET_Client):
        mocked_ET_Client.side_effect = requests.exceptions.ConnectionError("Connection Error")
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        try:
            _client.get_auth_stub(config)
        except Exception as e:
            # as "tenant_subdomain" is provided, error will be raised after call from v2
            self.assertEquals(
                str(e),
                "Connection Error. Please check your 'client_id', 'client_secret' or 'tenant_subdomain'.")

    @mock.patch("requests.post")
    @mock.patch("tap_exacttarget.client.LOGGER.info")
    def test_no_error_1(self, mocked_logger, mocked_post_request):
        json = {
            "accessToken": "access_token_123",
            "expiresIn": time.time() + 3600,
            "legacyToken": "legacyToken_123"
        }
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        _client.get_auth_stub(config)
        # as "tenant_subdomain" is not provided, auth_stub will be generated from v1
        mocked_logger.assert_called_with("Success.")

    @mock.patch("requests.post")
    @mock.patch("tap_exacttarget.client.LOGGER.info")
    def test_no_error_2(self, mocked_logger, mocked_post_request):
        json = {
            "access_token": "access_token_123",
            "expires_in": time.time() + 3600,
            "rest_instance_url": "aaa",
            "soap_instance_url": "bbb"
        }
        mocked_post_request.return_value = get_response(json)
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2019-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        _client.get_auth_stub(config)
        # as "tenant_subdomain" is provided, auth_stub will be generated from v2
        mocked_logger.assert_called_with("Success.")
