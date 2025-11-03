# test_base.py
"""Shared setup for Client unit tests (unittest version)."""

import unittest
from unittest.mock import Mock, MagicMock, patch
from tap_exacttarget.client import Client


class BaseClientTest(unittest.TestCase):
    """Base test class providing common mocks and client setup."""

    def setUp(self):
        self.mock_config = {
            "tenant_subdomain": "test-subdomain",
            "client_id": "test-client-id",
            "start_date": "2024-01-01T00:00:00Z",
            "client_secret": "test-client-secret",
            "request_timeout": "60",
            "date_window": "15",
            "batch_size": "1000",
        }

        self.mock_soap_client = MagicMock()
        self.mock_soap_client.get_type.side_effect = lambda name: MagicMock()
        self.mock_soap_client.service.Retrieve = MagicMock()
        self.mock_soap_client.service.Describe = MagicMock()
        self.mock_soap_client.set_default_soapheaders = MagicMock()

        self.mock_oauth_response = Mock()
        self.mock_oauth_response.json.return_value = {
            "access_token": "test-access-token",
            "expires_in": 3600,
        }
        self.mock_oauth_response.raise_for_status = Mock()

        self.patches = [
            patch("tap_exacttarget.client.Transport"),
            patch("tap_exacttarget.client.Session"),
            patch("tap_exacttarget.client.requests.post", return_value=self.mock_oauth_response),
            patch("tap_exacttarget.client.client.Client", return_value=self.mock_soap_client),
        ]
        for p in self.patches:
            p.start()

        self.client_instance = Client(self.mock_config)

    def tearDown(self):
        """Stop all active patches."""
        for p in self.patches:
            p.stop()
