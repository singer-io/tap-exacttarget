"""Unit tests for OAuth token management and expiry handling."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from requests.exceptions import HTTPError, RequestException
from .base_test import BaseClientTest


class TestTokenManagement(BaseClientTest):
    """Tests for OAuth token caching, expiry validation, and refresh logic."""

    @patch("tap_exacttarget.client.datetime")
    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_retrieved_and_cached_successfully(self, mock_post, mock_datetime_module):
        """Test that access token is retrieved, cached, and expiry time set correctly."""
        fixed_time = datetime(2025, 9, 4, 3, 0, 0)
        mock_datetime_module.now.return_value = fixed_time

        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "new-token-123", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        self.client_instance._Client__access_token = None
        self.client_instance.token_expiry_time = None

        token = self.client_instance.access_token

        assert token == "new-token-123"
        assert self.client_instance._Client__access_token == "new-token-123"

        # Verify expiry time is set correctly (expires_in - 500 seconds)
        expected_expiry = fixed_time + timedelta(seconds=(3600 - 500))
        assert self.client_instance.token_expiry_time == expected_expiry

        # Verify API was called once
        mock_post.assert_called_once()

    @patch("tap_exacttarget.client.datetime")
    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_refreshed_when_expired(self, mock_post, mock_datetime_module):
        """Test that expired access token triggers refresh."""
        fixed_time = datetime(2025, 9, 4, 3, 0, 0)
        mock_datetime_module.now.return_value = fixed_time

        mock_response = Mock()
        mock_response.json.return_value = {
            "access_token": "refreshed-token-456",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # Set expired token
        self.client_instance._Client__access_token = "old-expired-token"
        self.client_instance.token_expiry_time = fixed_time - timedelta(minutes=10)

        token = self.client_instance.access_token

        # Verify new token was retrieved
        assert token == "refreshed-token-456"
        assert self.client_instance._Client__access_token == "refreshed-token-456"
        mock_post.assert_called_once()

    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_not_refreshed_when_valid(self, mock_post):
        """Test that valid token is reused without API call."""
        self.client_instance._Client__access_token = "valid-token-789"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        token = self.client_instance.access_token

        # Verify same token is returned
        assert token == "valid-token-789"

        # Verify no API call was made
        mock_post.assert_not_called()

    def test_is_token_expired_returns_true_when_no_token(self):
        """Test that is_token_expired returns True when token is None."""
        self.client_instance._Client__access_token = None
        self.client_instance.token_expiry_time = None

        assert self.client_instance.is_token_expired() is True

    @patch("tap_exacttarget.client.datetime")
    def test_is_token_expired_returns_false_when_still_valid(self, mock_datetime_module):
        """Test that is_token_expired returns False for valid token beyond buffer."""
        fixed_time = datetime(2025, 9, 4, 3, 0, 0)
        mock_datetime_module.now.return_value = fixed_time

        self.client_instance._Client__access_token = "valid-token"

        self.client_instance.token_expiry_time = fixed_time + timedelta(hours=1)
        assert self.client_instance.is_token_expired() is False

    @patch("tap_exacttarget.client.datetime")
    def test_token_expiry_buffer_5_minutes(self, mock_datetime_module):
        """Test that token is considered expired within 5-minute buffer."""
        fixed_time = datetime(2025, 9, 4, 3, 0, 0)
        mock_datetime_module.now.return_value = fixed_time

        self.client_instance._Client__access_token = "expiring-token"
        self.client_instance.token_expiry_time = fixed_time + timedelta(minutes=3)

        assert self.client_instance.is_token_expired() is True

    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_request_handles_http_errors(self, mock_post):
        """Test that HTTP errors during token request are properly raised."""
        mock_post.side_effect = HTTPError("401 Unauthorized")

        self.client_instance._Client__access_token = None
        self.client_instance.token_expiry_time = None

        with self.assertRaises(HTTPError) as exc_info:
            _ = self.client_instance.access_token

        assert "401 Unauthorized" in str(exc_info.exception)

    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_request_raises_exception_on_failure(self, mock_post):
        """Test that generic exceptions during token request are propagated."""
        mock_post.side_effect = RequestException("Network error")

        self.client_instance._Client__access_token = None
        self.client_instance.token_expiry_time = None

        with self.assertRaises(RequestException) as exc_info:
            _ = self.client_instance.access_token

        assert "Network error" in str(exc_info.exception)

    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_request_includes_correct_payload(self, mock_post):
        """Test that token request includes correct client credentials."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test-token", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        self.client_instance._Client__access_token = None
        self.client_instance.token_expiry_time = None

        _ = self.client_instance.access_token

        call_kwargs = mock_post.call_args[1]
        payload = call_kwargs["json"]
        assert payload["client_id"] == self.client_instance.client_id
        assert payload["client_secret"] == self.client_instance.client_secret
        assert payload["grant_type"] == "client_credentials"

    @patch("tap_exacttarget.client.requests.post")
    def test_access_token_request_uses_configured_timeout(self, mock_post):
        """Test that token request uses the configured timeout value."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test-token", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        self.client_instance._Client__access_token = None
        self.client_instance.timeout = 75

        _ = self.client_instance.access_token

        # Verify timeout was passed
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["timeout"] == 75
