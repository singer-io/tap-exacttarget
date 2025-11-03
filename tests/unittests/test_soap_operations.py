from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from zeep.exceptions import TransportError, Fault
from tap_exacttarget.exceptions import MarketingCloudSoapApiException
from .base_test import BaseClientTest


class TestSoapOperations(BaseClientTest):
    """Tests for SOAP service operations (Retrieve and Describe)."""

    def test_retrieve_request_calls_soap_service_correctly(self):
        """Test that retrieve_request constructs RetrieveRequest and calls SOAP service."""
        # Mock the types
        mock_retrieve_request_type = Mock()
        mock_retrieve_request_instance = Mock()
        mock_retrieve_request_type.return_value = mock_retrieve_request_instance

        mock_retrieve_options_type = Mock()
        mock_retrieve_options_instance = Mock()
        mock_retrieve_options_type.return_value = mock_retrieve_options_instance

        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        mock_response = {"OverallStatus": "OK", "Results": [{"Key": "Value"}]}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        response = self.client_instance.retrieve_request(
            object_type="Subscriber", properties=["SubscriberKey", "EmailAddress"]
        )

        # Verify response returned
        assert response == mock_response

        # Verify RetrieveRequest was created with correct parameters
        mock_retrieve_request_type.assert_called_once()
        call_kwargs = mock_retrieve_request_type.call_args[1]
        assert call_kwargs["ObjectType"] == "Subscriber"
        assert call_kwargs["Properties"] == ["SubscriberKey", "EmailAddress"]
        assert call_kwargs["Options"] == mock_retrieve_options_instance

        # Verify SOAP service was called
        self.client_instance.soap_client.service.Retrieve.assert_called_once()

    def test_retrieve_request_includes_filter_when_provided(self):
        """Test that search filter is included in retrieve request when provided."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_request_instance = Mock()
        mock_retrieve_request_type.return_value = mock_retrieve_request_instance

        mock_retrieve_options_type = Mock()
        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        mock_filter = Mock(name="TestFilter")
        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(
            object_type="Subscriber", properties=["SubscriberKey"], search_filter=mock_filter
        )

        # Verify Filter was set on the request instance
        assert mock_retrieve_request_instance.Filter == mock_filter

    def test_retrieve_request_includes_continue_request_id(self):
        """Test that continuation request ID is set for pagination."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_request_instance = Mock()
        mock_retrieve_request_type.return_value = mock_retrieve_request_instance

        mock_retrieve_options_type = Mock()
        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(
            object_type="Subscriber",
            properties=["SubscriberKey"],
            request_id="continuation-request-123",
        )

        # Verify ContinueRequest was set
        assert mock_retrieve_request_instance.ContinueRequest == "continuation-request-123"

    def test_retrieve_request_sets_batch_size_in_options(self):
        """Test that batch size is correctly set in RetrieveOptions."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_options_type = Mock()
        mock_retrieve_options_instance = Mock()
        mock_retrieve_options_type.return_value = mock_retrieve_options_instance

        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        self.client_instance.batch_size = 1234

        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(
            object_type="Subscriber", properties=["SubscriberKey"]
        )

        # Verify RetrieveOptions called with correct batch size
        mock_retrieve_options_type.assert_called_once_with(BatchSize=1234, IncludeObjects=True)

    def test_retrieve_request_sets_include_objects_true(self):
        """Test that IncludeObjects is always set to True in options."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_options_type = Mock()

        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(object_type="DataExtension", properties=["Name"])

        # Verify IncludeObjects=True
        call_kwargs = mock_retrieve_options_type.call_args[1]
        assert call_kwargs["IncludeObjects"] is True

    def test_retrieve_request_raises_custom_exceptions_for_faults(self):
        """Test that SOAP Fault exceptions are wrapped in MarketingCloudSoapApiException."""
        self.client_instance.soap_client.service.Retrieve.side_effect = Fault("SOAP Fault occurred")

        with self.assertRaises(MarketingCloudSoapApiException) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        # Verify custom exception message
        assert "SOAP Fault or Transport Error" in str(exc_info.exception)
        # Verify original exception is chained
        assert exc_info.exception.__cause__ is not None

    def test_retrieve_request_raises_custom_exceptions_for_transport_errors(self):
        """Test that TransportError exceptions are wrapped in MarketingCloudSoapApiException."""
        self.client_instance.soap_client.service.Retrieve.side_effect = TransportError(
            "Connection failed"
        )

        with self.assertRaises(MarketingCloudSoapApiException) as exc_info:
            self.client_instance.retrieve_request(
                object_type="Subscriber", properties=["SubscriberKey"]
            )

        assert "SOAP Fault or Transport Error" in str(exc_info.exception)

    def test_retrieve_request_updates_oauth_header_before_call(self):
        """Test that OAuth header is refreshed with current access token before each call."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_options_type = Mock()

        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        # Set a specific token
        self.client_instance._Client__access_token = "specific-token-xyz"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(
            object_type="Subscriber", properties=["SubscriberKey"]
        )

        # Verify set_default_soapheaders was called (OAuth header update)
        assert self.client_instance.soap_client.set_default_soapheaders.called

    @patch("tap_exacttarget.client.LOGGER")
    def test_retrieve_request_logs_object_type_and_fields(self, mock_logger):
        """Test that retrieve request logs object type and properties."""
        mock_retrieve_request_type = Mock()
        mock_retrieve_options_type = Mock()

        self.client_instance.soap_client.get_type.side_effect = [
            mock_retrieve_request_type,
            mock_retrieve_options_type,
        ]

        mock_response = {"OverallStatus": "OK", "Results": []}
        self.client_instance.soap_client.service.Retrieve.return_value = mock_response

        self.client_instance.retrieve_request(
            object_type="DataExtension", properties=["Name", "CustomerKey"]
        )

        # Verify logging occurred with object type and fields
        mock_logger.info.assert_any_call(
            "Objtype: %s fields: %s", "DataExtension", ["Name", "CustomerKey"]
        )

    def test_describe_request_builds_correct_object_definition(self):
        """Test that describe_request builds ObjectDefinitionRequest correctly."""
        mock_obj_defn_request_type = Mock()
        mock_obj_defn_instance = Mock()
        mock_obj_defn_request_type.return_value = mock_obj_defn_instance

        mock_array_obj_defn_type = Mock()
        mock_array_instance = Mock()
        mock_array_obj_defn_type.return_value = mock_array_instance

        self.client_instance.soap_client.get_type.side_effect = [
            mock_obj_defn_request_type,
            mock_array_obj_defn_type,
        ]

        mock_response = {"ObjectDefinition": {"ObjectType": "Subscriber"}}
        self.client_instance.soap_client.service.Describe.return_value = mock_response

        response = self.client_instance.describe_request("Subscriber")

        # Verify ObjectDefinitionRequest created with correct ObjectType
        mock_obj_defn_request_type.assert_called_once_with(ObjectType="Subscriber")

        # Verify array created with the request
        mock_array_obj_defn_type.assert_called_once()
        call_kwargs = mock_array_obj_defn_type.call_args[1]
        assert call_kwargs["ObjectDefinitionRequest"] == [mock_obj_defn_instance]

        # Verify response returned
        assert response == mock_response

    def test_describe_request_uses_access_token_in_header(self):
        """Test that describe request updates OAuth header before calling service."""
        mock_obj_defn_request_type = Mock()
        mock_array_obj_defn_type = Mock()

        self.client_instance.soap_client.get_type.side_effect = [
            mock_obj_defn_request_type,
            mock_array_obj_defn_type,
        ]

        # Set specific token
        self.client_instance._Client__access_token = "describe-token-abc"
        self.client_instance.token_expiry_time = datetime.now() + timedelta(hours=1)

        mock_response = {"ObjectDefinition": {}}
        self.client_instance.soap_client.service.Describe.return_value = mock_response

        self.client_instance.describe_request("DataExtension")
        assert self.client_instance.soap_client.set_default_soapheaders.called
