from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
from .base_test import BaseClientTest


class TestRestOperations(BaseClientTest):
    """Tests for REST API GET operations."""

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_successful_response(self, mock_get):
        """Test successful REST API GET request returns JSON data."""
        mock_response = Mock()
        mock_response.json.return_value = {"items": [{"id": 1}, {"id": 2}]}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "rest-token"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        result = self.client_instance.get_rest("data/v1/customobjects", {"page": 1})

        # Verify result matches response JSON
        assert result == {"items": [{"id": 1}, {"id": 2}]}

        mock_get.assert_called_once()

        mock_response.raise_for_status.assert_called_once()

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_includes_bearer_token_in_header(self, mock_get):
        """Test that REST request includes Bearer token in Authorization header."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "bearer-token-123"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        self.client_instance.get_rest("data/v1/endpoint", {})

        # Verify Authorization header
        call_kwargs = mock_get.call_args[1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"]["Authorization"] == "Bearer bearer-token-123"

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_constructs_correct_url(self, mock_get):
        """Test that full URL is constructed from rest_url and endpoint."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "token"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)
        self.client_instance.rest_url = "https://test.rest.marketingcloudapis.com/"

        self.client_instance.get_rest("data/v1/test", {})

        call_args = mock_get.call_args[0]
        assert call_args[0] == "https://test.rest.marketingcloudapis.com/data/v1/test"

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_handles_http_error_gracefully(self, mock_get):
        """Test that HTTP errors in REST requests are properly raised."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "token"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        with self.assertRaises(HTTPError) as exc_info:
            self.client_instance.get_rest("data/v1/notfound", {})

        assert "404 Not Found" in str(exc_info.exception)

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_passes_correct_params_to_requests(self, mock_get):
        """Test that query parameters are correctly passed to requests.get."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "token"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        params = {"page": 2, "limit": 50, "filter": "active"}
        self.client_instance.get_rest("data/v1/items", params)

        # Verify params passed correctly
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"] == params

    @patch("tap_exacttarget.client.requests.get")
    def test_get_rest_uses_configured_timeout(self, mock_get):
        """Test that REST request uses the configured timeout value."""
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        self.client_instance._Client__access_token = "token"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)
        self.client_instance.timeout = 90

        self.client_instance.get_rest("data/v1/endpoint", {})

        # Verify timeout parameter
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["timeout"] == 90
