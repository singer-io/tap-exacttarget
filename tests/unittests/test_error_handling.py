from requests.exceptions import ConnectionError, RequestException, HTTPError, Timeout
from unittest.mock import Mock, patch
from zeep.exceptions import TransportError, Fault
from tap_exacttarget.exceptions import (
    IncompatibleFieldSelectionError,
    MarketingCloudPermissionFailure,
    MarketingCloudSoapApiException,
)
from tap_exacttarget.client import is_non_retryable_http_error
from .base_test import BaseClientTest


class TestErrorHandling(BaseClientTest):
    """Tests for error detection and mapping to custom exceptions."""

    def _run_backoff_test(self, exception_type, message="Test Error"):
        """Helper to test that backoff retries occur for given exception."""
        mock_retrieve = self.client_instance.soap_client.service.Retrieve
        mock_retrieve.side_effect = exception_type(message)

        with self.assertRaises(exception_type) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        self.assertEqual(mock_retrieve.call_count, 5)
        self.assertIn(message, str(exc_info.exception))

    def test_backoff_connection_error(self):
        """Test retry behavior for ConnectionError."""
        self._run_backoff_test(ConnectionError, "Connection failed")

    def test_backoff_timeout(self):
        """Test retry behavior for Timeout."""
        self._run_backoff_test(Timeout, "Timeout occurred")

    def test_backoff_http_error(self):
        """Test retry behavior for HTTPError."""
        self._run_backoff_test(HTTPError, "HTTP Error occurred")

    def test_backoff_request_exception(self):
        """Test retry behavior for RequestException."""
        self._run_backoff_test(RequestException, "Generic request error")

    def test_raises_incompatible_field_selection_error_on_invalid_column_name(self):
        """Test that 'Invalid column name' errors raise IncompatibleFieldSelectionError."""
        response = {"OverallStatus": "Error: Invalid column name 'NonExistentColumn'"}

        with self.assertRaises(IncompatibleFieldSelectionError) as exc_info:
            self.client_instance.raise_for_error(response)

        assert "Invalid column name" in str(exc_info.exception)

    def test_raises_incompatible_field_selection_error_on_request_property_error(self):
        """Test that 'Request Property' errors raise IncompatibleFieldSelectionError."""
        response = {"OverallStatus": "Error: The Request Property(s) InvalidField is not supported"}

        with self.assertRaises(IncompatibleFieldSelectionError) as exc_info:
            self.client_instance.raise_for_error(response)

        assert "Request Property" in str(exc_info.exception)

    def test_raises_marketing_cloud_permission_failure_on_permission_error(self):
        """Test that 'API Permission Failed' errors raise MarketingCloudPermissionFailure."""
        response = {"OverallStatus": "Error: API Permission Failed - Insufficient privileges"}

        with self.assertRaises(MarketingCloudPermissionFailure) as exc_info:
            self.client_instance.raise_for_error(response)

        assert "API Permission Failed" in str(exc_info.exception)

    def test_no_error_for_valid_overall_status(self):
        """Test that valid 'OK' response status does not raise any error."""
        response = {"OverallStatus": "OK"}

        try:
            self.client_instance.raise_for_error(response)
            success = True
        except Exception:
            success = False

        assert success is True

    def test_no_error_for_success_status(self):
        """Test that other success statuses don't raise errors."""
        response = {"OverallStatus": "MoreDataAvailable"}

        # Should not raise any exception
        try:
            self.client_instance.raise_for_error(response)
            success = True
        except Exception:
            success = False

        assert success is True

    def test_retrieve_request_wraps_fault_exception_in_custom_error(self):
        """Test that SOAP Fault exceptions are wrapped in MarketingCloudSoapApiException."""
        fault_message = "Server.InternalError: An internal error occurred"
        self.client_instance.soap_client.service.Retrieve.side_effect = Fault(fault_message)

        with self.assertRaises(MarketingCloudSoapApiException) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        # Verify custom exception contains descriptive message
        assert "SOAP Fault or Transport Error" in str(exc_info.exception)
        # Verify original exception is preserved as cause
        assert isinstance(exc_info.exception.__cause__, Fault)

    def test_retrieve_request_wraps_transport_error_in_custom_error(self):
        """Test that TransportError exceptions are wrapped in MarketingCloudSoapApiException."""
        transport_message = "HTTP Error 503: Service Unavailable"
        self.client_instance.soap_client.service.Retrieve.side_effect = TransportError(
            transport_message
        )

        with self.assertRaises(MarketingCloudSoapApiException) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        assert "SOAP Fault or Transport Error" in str(exc_info.exception)
        assert isinstance(exc_info.exception.__cause__, TransportError)

    def test_retrieve_request_raises_marketing_cloud_error_unchanged(self):
        """Test that MarketingCloudError exceptions are re-raised as-is."""
        from tap_exacttarget.exceptions import MarketingCloudError

        original_error = MarketingCloudError("Custom marketing cloud error")
        self.client_instance.soap_client.service.Retrieve.side_effect = original_error

        with self.assertRaises(MarketingCloudError) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        # Should be the same exception, not wrapped
        assert exc_info.exception is original_error

    def test_non_retryable_http_error_returns_true_for_403(self):
        response = Mock()
        response.status_code = 403
        err = HTTPError("403 Forbidden", response=response)

        assert is_non_retryable_http_error(err) is True

    def test_non_retryable_http_error_returns_false_for_non_403(self):
        response = Mock()
        response.status_code = 500
        err = HTTPError("500 Internal Server Error", response=response)

        assert is_non_retryable_http_error(err) is False

    def test_get_rest_does_not_retry_403(self):
        response = Mock()
        response.status_code = 403
        response.raise_for_status.side_effect = HTTPError("403 Forbidden", response=response)

        with patch("tap_exacttarget.client.requests.get", return_value=response) as mock_get:
            with self.assertRaises(HTTPError):
                self.client_instance.get_rest("hub/v1/campaigns/", params={})

        assert mock_get.call_count == 1
