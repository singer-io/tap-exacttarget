"""Unit tests for Client initialization and configuration logic."""

from unittest.mock import MagicMock, patch
from tap_exacttarget.client import Client, DEFAULT_TIMEOUT, DEFAULT_BATCH_SIZE, DEFAULT_DATE_WINDOW
from .base_test import BaseClientTest


class TestClientInitialization(BaseClientTest):
    """Tests for Client initialization and configuration parsing."""

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_init_with_valid_config_creates_client_successfully(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test successful client initialization with valid config."""
        mock_post.return_value = self.mock_oauth_response
        mock_soap = MagicMock()
        mock_zeep_client.return_value = mock_soap

        client = Client(self.mock_config)

        # Verify all config values parsed correctly
        assert client.timeout == 60
        assert client.date_window == 15.0
        assert client.batch_size == 1000
        assert client.client_id == "test-client-id"
        assert client.client_secret == "test-client-secret"
        assert client.soap_client is not None

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    @patch("tap_exacttarget.client.LOGGER")
    def test_invalid_batch_size_defaults_to_default_value(
        self, mock_logger, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test client initialization with invalid batch_size falls back to default."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        self.mock_config["batch_size"] = "not-a-number"

        client = Client(self.mock_config)

        # Verify fallback to DEFAULT_BATCH_SIZE
        assert client.batch_size == DEFAULT_BATCH_SIZE
        # Verify logging occurred
        mock_logger.info.assert_any_call(
            "invalid value received for batch_size, fallback to default %s", DEFAULT_BATCH_SIZE
        )

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    @patch("tap_exacttarget.client.LOGGER")
    def test_invalid_date_window_defaults_to_default_value(
        self, mock_logger, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test client initialization with invalid date_window falls back to default."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        self.mock_config["date_window"] = "not-a-number"

        client = Client(self.mock_config)

        # Verify fallback to DEFAULT_DATE_WINDOW
        assert client.date_window == DEFAULT_DATE_WINDOW
        # Verify logging occurred
        mock_logger.info.assert_any_call(
            "invalid value received for date_window, fallback to default %s", DEFAULT_DATE_WINDOW
        )

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_missing_optional_config_uses_defaults(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test that missing optional config values use defaults."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        del self.mock_config["request_timeout"]
        del self.mock_config["date_window"]
        del self.mock_config["batch_size"]

        client = Client(self.mock_config)

        # Verify defaults are used
        assert client.timeout == DEFAULT_TIMEOUT
        assert client.date_window == DEFAULT_DATE_WINDOW
        assert client.batch_size == DEFAULT_BATCH_SIZE

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_wsdl_and_auth_urls_are_constructed_correctly(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test that WSDL and auth URLs are constructed correctly from subdomain."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        client = Client(self.mock_config)

        # Verify URL construction
        assert (
            client.wsdl_uri == "https://test-subdomain.soap.marketingcloudapis.com/etframework.wsdl"
        )
        assert client.auth_url == "https://test-subdomain.auth.marketingcloudapis.com/v2/token"
        assert client.rest_url == "https://test-subdomain.rest.marketingcloudapis.com/"

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_initialize_soap_client_called_during_init(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test that SOAP client initialization is called with correct parameters."""
        mock_post.return_value = self.mock_oauth_response
        mock_soap = MagicMock()
        mock_zeep_client.return_value = mock_soap

        client = Client(self.mock_config)

        # Verify zeep.Client was called with wsdl and transport
        mock_zeep_client.assert_called_once()
        call_kwargs = mock_zeep_client.call_args[1]
        assert "wsdl" in call_kwargs
        assert call_kwargs["wsdl"] == client.wsdl_uri
        assert "transport" in call_kwargs

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_default_soap_headers_are_set_after_initialization(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test that default SOAP headers with OAuth token are set."""
        mock_post.return_value = self.mock_oauth_response
        mock_soap = MagicMock()
        mock_zeep_client.return_value = mock_soap

        Client(self.mock_config)

        # Verify set_default_soapheaders was called with OAuth header
        mock_soap.set_default_soapheaders.assert_called_once()
        call_args = mock_soap.set_default_soapheaders.call_args[0][0]
        assert len(call_args) == 1  # One header element

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_timeout_parsed_correctly_from_config(
        self, mock_zeep_client, mock_session, mock_transport, mock_post
    ):
        """Test that timeout value is correctly parsed as integer from string config."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        self.mock_config["request_timeout"] = "120"
        client = Client(self.mock_config)

        # Verify timeout is parsed as int
        assert client.timeout == 120
        assert isinstance(client.timeout, int)

    @patch("tap_exacttarget.client.requests.post")
    @patch("tap_exacttarget.client.Transport")
    @patch("tap_exacttarget.client.Session")
    @patch("tap_exacttarget.client.client.Client")
    def test_transport_initialized_with_correct_timeouts(
        self, mock_zeep_client, mock_session_class, mock_transport_class, mock_post
    ):
        """Test that Transport is initialized with correct timeout values."""
        mock_post.return_value = self.mock_oauth_response
        mock_zeep_client.return_value = MagicMock()

        Client(self.mock_config)

        # Verify Transport was called with timeout=300 and operation_timeout=300
        mock_transport_class.assert_called_once()
        call_kwargs = mock_transport_class.call_args[1]
        assert call_kwargs["timeout"] == 300
        assert call_kwargs["operation_timeout"] == 300

    def test_context_manager_enter_returns_self(self):
        """Test that __enter__ returns the client instance."""
        result = self.client_instance.__enter__()
        assert result is self.client_instance

    def test_context_manager_exit_completes_without_error(self):
        """Test that __exit__ completes without raising exceptions."""
        # Should not raise any exception
        self.client_instance.__exit__(None, None, None)
        assert True
