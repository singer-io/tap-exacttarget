"""Unit tests for discovery-time unauthorized stream exclusion.

Mirrors the pattern used by other taps: probe each stream, catch
permission-denied responses and exclude the stream from the catalog instead
of aborting discovery entirely.
"""
import unittest
from unittest.mock import MagicMock, Mock, patch

from requests.exceptions import HTTPError

from tap_exacttarget.discover import (
    _apply_access_checks,
    _discover_dao_streams_safe,
    check_stream_access,
    discover,
)
from tap_exacttarget.exceptions import MarketingCloudError, MarketingCloudPermissionFailure


def make_stream_class(object_ref="SomeObject", key_properties=None,
                       parent_tap_stream_id=None, endpoint=None):
    """Builds a lightweight stand-in for a stream class under test."""
    attrs = {
        "object_ref": object_ref,
        "key_properties": key_properties or ["ID"],
        "parent_tap_stream_id": parent_tap_stream_id,
        "tap_stream_id": object_ref,
    }
    if endpoint is not None:
        attrs["endpoint"] = endpoint
    return type("StreamStub", (), attrs)


def make_http_error(status_code):
    response = Mock()
    response.status_code = status_code
    return HTTPError(response=response)


class TestCheckStreamAccessSoap(unittest.TestCase):
    """SOAP-backed streams are probed via Client.retrieve_request."""

    def test_accessible_stream_returns_true(self):
        client = MagicMock()
        client.retrieve_request.return_value = {"OverallStatus": "OK", "Results": []}
        stream_class = make_stream_class(object_ref="Email", key_properties=["ID"])

        self.assertTrue(check_stream_access(client, "email", stream_class))
        client.retrieve_request.assert_called_once_with("Email", ["ID"], batch_size=1)

    def test_permission_denied_excludes_stream(self):
        client = MagicMock()
        client.retrieve_request.side_effect = MarketingCloudPermissionFailure(
            "Error: API Permission Failed - Insufficient privileges"
        )
        stream_class = make_stream_class(object_ref="Email", key_properties=["ID"])

        self.assertFalse(check_stream_access(client, "email", stream_class))

    def test_other_marketing_cloud_error_propagates(self):
        """Non-permission errors must not be swallowed; discovery should abort."""
        client = MagicMock()
        client.retrieve_request.side_effect = MarketingCloudError("Error: Something else broke")
        stream_class = make_stream_class(object_ref="Email", key_properties=["ID"])

        with self.assertRaises(MarketingCloudError):
            check_stream_access(client, "email", stream_class)


class TestCheckStreamAccessRest(unittest.TestCase):
    """REST-backed streams (e.g. campaigns) are probed via Client.get_rest."""

    def test_accessible_stream_returns_true(self):
        client = MagicMock()
        client.get_rest.return_value = {"items": [], "page": 1}
        stream_class = make_stream_class(endpoint="hub/v1/campaigns/")

        self.assertTrue(check_stream_access(client, "campaigns", stream_class))
        client.get_rest.assert_called_once_with("hub/v1/campaigns/", params={})

    def test_403_excludes_stream(self):
        client = MagicMock()
        client.get_rest.side_effect = make_http_error(403)
        stream_class = make_stream_class(endpoint="hub/v1/campaigns/")

        self.assertFalse(check_stream_access(client, "campaigns", stream_class))

    def test_non_403_http_error_propagates(self):
        client = MagicMock()
        client.get_rest.side_effect = make_http_error(500)
        stream_class = make_stream_class(endpoint="hub/v1/campaigns/")

        with self.assertRaises(HTTPError):
            check_stream_access(client, "campaigns", stream_class)


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for the discovery-wide access-check orchestration."""

    def test_all_accessible(self):
        client = MagicMock()
        client.retrieve_request.return_value = {"OverallStatus": "OK", "Results": []}
        streams = {
            "list": make_stream_class(object_ref="List", key_properties=["ID"]),
            "email": make_stream_class(object_ref="Email", key_properties=["ID"]),
        }

        result = _apply_access_checks(client, streams)

        self.assertEqual(set(result.keys()), {"list", "email"})

    def test_excludes_unauthorized_top_level_stream(self):
        client = MagicMock()

        def side_effect(object_ref, _fields, batch_size=None):
            if object_ref == "Email":
                raise MarketingCloudPermissionFailure("Error: API Permission Failed")
            return {"OverallStatus": "OK", "Results": []}

        client.retrieve_request.side_effect = side_effect
        streams = {
            "list": make_stream_class(object_ref="List", key_properties=["ID"]),
            "email": make_stream_class(object_ref="Email", key_properties=["ID"]),
        }

        result = _apply_access_checks(client, streams)

        self.assertEqual(set(result.keys()), {"list"})

    def test_excludes_child_when_parent_excluded_without_probing_child(self):
        client = MagicMock()
        client.retrieve_request.side_effect = MarketingCloudPermissionFailure(
            "Error: API Permission Failed"
        )
        streams = {
            "list_subscribers": make_stream_class(
                object_ref="ListSubscriber", key_properties=["SubscriberKey"]
            ),
            "subscribers": make_stream_class(
                object_ref="Subscriber",
                key_properties=["ID"],
                parent_tap_stream_id="list_subscribers",
            ),
        }

        result = _apply_access_checks(client, streams)

        self.assertEqual(result, {})
        # Only the parent should have been probed; the child is pruned without
        # issuing a separate request once its parent is known to be excluded.
        client.retrieve_request.assert_called_once_with(
            "ListSubscriber", ["SubscriberKey"], batch_size=1
        )

    def test_child_individually_excluded_even_if_parent_accessible(self):
        client = MagicMock()

        def side_effect(object_ref, _fields, batch_size=None):
            if object_ref == "Subscriber":
                raise MarketingCloudPermissionFailure("Error: API Permission Failed")
            return {"OverallStatus": "OK", "Results": []}

        client.retrieve_request.side_effect = side_effect
        streams = {
            "list_subscribers": make_stream_class(
                object_ref="ListSubscriber", key_properties=["SubscriberKey"]
            ),
            "subscribers": make_stream_class(
                object_ref="Subscriber",
                key_properties=["ID"],
                parent_tap_stream_id="list_subscribers",
            ),
        }

        result = _apply_access_checks(client, streams)

        self.assertEqual(set(result.keys()), {"list_subscribers"})

    def test_list_subscribers_performs_additional_list_probe_with_single_batch(self):
        client = MagicMock()
        client.retrieve_request.return_value = {"OverallStatus": "OK", "Results": []}
        stream_class = make_stream_class(object_ref="ListSubscriber", key_properties=["SubscriberKey"])

        self.assertTrue(check_stream_access(client, "list_subscribers", stream_class))
        self.assertEqual(
            client.retrieve_request.call_args_list,
            [
                unittest.mock.call("ListSubscriber", ["SubscriberKey"], batch_size=1),
                unittest.mock.call("LIST", ["ID"], batch_size=1),
            ],
        )

    def test_no_streams_accessible_returns_empty_dict(self):
        client = MagicMock()
        client.retrieve_request.side_effect = MarketingCloudPermissionFailure(
            "Error: API Permission Failed"
        )
        streams = {"email": make_stream_class(object_ref="Email", key_properties=["ID"])}

        result = _apply_access_checks(client, streams)

        self.assertEqual(result, {})


class TestDiscoverDaoStreamsSafe(unittest.TestCase):
    """Data Extension discovery should degrade gracefully on permission failure."""

    @patch("tap_exacttarget.discover.discover_dao_streams")
    def test_returns_streams_on_success(self, mock_discover_dao):
        mock_discover_dao.return_value = {"data_extension_foo": object()}
        client = MagicMock()

        result = _discover_dao_streams_safe(client)

        self.assertEqual(result, {"data_extension_foo": mock_discover_dao.return_value["data_extension_foo"]})

    @patch("tap_exacttarget.discover.discover_dao_streams")
    def test_permission_failure_returns_empty_dict(self, mock_discover_dao):
        mock_discover_dao.side_effect = MarketingCloudPermissionFailure(
            "Error: API Permission Failed"
        )
        client = MagicMock()

        result = _discover_dao_streams_safe(client)

        self.assertEqual(result, {})

    @patch("tap_exacttarget.discover.discover_dao_streams")
    def test_other_errors_propagate(self, mock_discover_dao):
        mock_discover_dao.side_effect = MarketingCloudError("Error: Something else broke")
        client = MagicMock()

        with self.assertRaises(MarketingCloudError):
            _discover_dao_streams_safe(client)


class TestDiscoverRaisesWhenNothingAccessible(unittest.TestCase):
    """discover() should fail loudly if the credentials can read nothing at all."""

    @patch("tap_exacttarget.discover._discover_dao_streams_safe", return_value={})
    @patch("tap_exacttarget.discover._apply_access_checks", return_value={})
    def test_raises_permission_failure(self, _mock_apply, _mock_dao):
        client = MagicMock()

        with self.assertRaises(MarketingCloudPermissionFailure):
            discover(client)


if __name__ == "__main__":
    unittest.main()
