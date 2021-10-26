import unittest
from unittest import mock
import tap_exacttarget.client as _client

# mock soap client
class SOAP_Client:
    test_timeout = None
    def set_options(self, *args, **kwargs):
        self.test_timeout = kwargs.get("timeout")

# mock 'ET_Client' class
class Mocked_ET_Client:
    soap_client = None
    def __init__(self, *args, **kwargs):
        self.soap_client = SOAP_Client()

@mock.patch("FuelSDK.ET_Client")
@mock.patch("suds.transport.https.HttpAuthenticated.__init__")
class TestTimeoutVlaue(unittest.TestCase):

    def test_timeout_no_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that timeout was set to default timeout when no value is passed in config
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500
        })

        # verify the timeout was called with default timeout
        mocked_HttpAuthenticated.assert_called_with(timeout=900.0)

    def test_timeout_integer_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that integer timeout value passed in the config file was respected
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500,
            "request_timeout": 100,
        })

        # verify the timeout was called with the timeout set in the config file
        mocked_HttpAuthenticated.assert_called_with(timeout=100.0)

    def test_timeout_string_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that string timeout value passed in the config file was respected
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500,
            "request_timeout": "100",
        })

        # verify the timeout was called with the timeout set in the config file
        mocked_HttpAuthenticated.assert_called_with(timeout=100.0)

    def test_timeout_empty_string_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that timeout was set to default timeout when empty string was passed in the config file
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500,
            "request_timeout": "",
        })

        # verify the timeout was called with default timeout
        mocked_HttpAuthenticated.assert_called_with(timeout=900.0)

    def test_timeout_0_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that timeout was set to default timeout when empty 0 was passed in the config file
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500,
            "request_timeout": 0.0,
        })

        # verify the timeout was called with default timeout
        mocked_HttpAuthenticated.assert_called_with(timeout=900.0)

    def test_timeout_string_0_value(self, mocked_HttpAuthenticated, mocked_ET_Client):
        """
            Verify that timeout was set to default timeout when string 0 was passed in the config file
        """
        # mock 'FuelSDK.ET_Client'
        mocked_ET_Client.side_effect = Mocked_ET_Client
        mocked_HttpAuthenticated.return_value = None

        # function call
        _client.get_auth_stub({
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "batch_size": 2500,
            "request_timeout": "0.0",
        })

        # verify the timeout was called with default timeout
        mocked_HttpAuthenticated.assert_called_with(timeout=900.0)
