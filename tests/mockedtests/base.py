"""Base helpers for tap-exacttarget mocked integration tests.

These tests exercise the real `discover()` and `sync()` entry points
end-to-end against a fully mocked `Client` stand-in (`MockClient` below),
instead of either hitting the live ExactTarget/Marketing Cloud API (like
tests/*.py, which use tap_tester and require live credentials) or mocking
at the HTTP/SOAP transport layer (like tests/unittests/*.py). This gives
fast, credential-free coverage of the tap's discovery and sync behavior
(catalog metadata, field selection, bookmarking, start_date handling).
"""

from typing import Dict, List, Optional
from unittest.mock import Mock

from requests.exceptions import HTTPError
from singer import metadata

from tap_exacttarget.exceptions import MarketingCloudPermissionFailure

DEFAULT_RETRIEVE_RESPONSE = {
    "Results": [],
    "RequestID": "mock-request-id",
    "OverallStatus": "OK",
}

DEFAULT_REST_RESPONSE = {"items": [], "page": 1}


class MockClient:
    """A minimal stand-in for `tap_exacttarget.client.Client`.

    Implements just the surface area `discover()`/`sync()`/the stream
    classes call: `retrieve_request`, `get_rest`, `describe_request`,
    `create_simple_filter`, `create_complex_filter`, plus the
    `config`/`date_window` attributes.

    By default every call succeeds and returns no data, so discovery's
    access-checks consider every stream accessible and syncing any
    not-explicitly-configured stream completes immediately with zero
    records. Tests set `retrieve_responses`/`rest_responses`/
    `describe_responses` (keyed by object_ref/endpoint) to supply canned
    data for the stream(s) they're exercising, and `deny_object_types`/
    `deny_endpoints` to simulate a 403/permission-denied stream.
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {
            "tenant_subdomain": "test-subdomain",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "start_date": "2015-01-01T00:00:00Z",
        }
        self.date_window = float(self.config.get("date_window") or 30)

        # object_ref/endpoint -> canned response, returned for every call.
        self.retrieve_responses: Dict[str, Dict] = {}
        self.rest_responses: Dict[str, Dict] = {}
        self.describe_responses: Dict[str, Dict] = {}

        # object_refs/endpoints that should behave as though the
        # credentials are not authorized to read them.
        self.deny_object_types: set = set()
        self.deny_endpoints: set = set()

        # Records every call for assertions (e.g. verifying the
        # search_filter used to query a stream).
        self.retrieve_calls: List[Dict] = []
        self.rest_calls: List[Dict] = []

    # -- SOAP-style API ---------------------------------------------------
    def retrieve_request(
        self,
        object_type,
        properties,
        request_id=None,
        search_filter=None,
        batch_size=None,
    ):
        self.retrieve_calls.append(
            {
                "object_type": object_type,
                "properties": properties,
                "request_id": request_id,
                "search_filter": search_filter,
                "batch_size": batch_size,
            }
        )
        if object_type in self.deny_object_types:
            raise MarketingCloudPermissionFailure(f"Access denied for object type '{object_type}'")
        return self.retrieve_responses.get(object_type, DEFAULT_RETRIEVE_RESPONSE)

    def describe_request(self, object_ref):
        return self.describe_responses.get(object_ref, {"ObjectDefinition": [{"Properties": []}]})

    def create_simple_filter(self, property_name, operator, value=None, date_value=None):
        return {
            "Property": property_name,
            "SimpleOperator": operator,
            "Value": value,
            "DateValue": date_value,
        }

    def create_complex_filter(self, left_operand, logical_operator, right_operand):
        return {
            "LeftOperand": left_operand,
            "LogicalOperator": logical_operator,
            "RightOperand": right_operand,
        }

    # -- REST-style API ----------------------------------------------------
    def get_rest(self, endpoint, params=None):
        self.rest_calls.append({"endpoint": endpoint, "params": params})
        if endpoint in self.deny_endpoints:
            response = Mock(status_code=403)
            raise HTTPError(response=response)
        return self.rest_responses.get(endpoint, DEFAULT_REST_RESPONSE)


def describe_fields(*field_names: str) -> Dict:
    """Builds a `describe_request()`-shaped response marking each named
    field retrievable, for use with `MockClient.describe_responses`."""
    return {
        "ObjectDefinition": [
            {"Properties": [{"Name": name, "IsRetrievable": True} for name in field_names]}
        ]
    }


def find_filter_date_value(filter_obj, operator):
    """Recursively searches a (possibly nested Simple/Complex) filter dict
    built by `MockClient.create_simple_filter`/`create_complex_filter` for a
    SimpleFilterPart with the given `operator`, returning its `DateValue`.

    Returns None if no matching filter part is found.
    """
    if not isinstance(filter_obj, dict):
        return None
    if filter_obj.get("SimpleOperator") == operator:
        return filter_obj.get("DateValue")
    for key in ("LeftOperand", "RightOperand"):
        found = find_filter_date_value(filter_obj.get(key), operator)
        if found is not None:
            return found
    return None


class ExactTargetMockedBaseTest:
    """Shared helpers for tap-exacttarget mocked integration tests."""

    @staticmethod
    def base_config(start_date="2015-01-01T00:00:00Z", **overrides):
        config = {
            "tenant_subdomain": "test-subdomain",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "start_date": start_date,
        }
        config.update(overrides)
        return config

    @staticmethod
    def select_streams(catalog, stream_ids):
        """Marks only the given tap_stream_ids as selected in a discovered
        catalog, mirroring what a real connection's catalog selection does
        before sync."""
        for stream in catalog.streams:
            mdata = metadata.to_map(stream.metadata)
            root = dict(mdata.get((), {}))
            root["selected"] = stream.tap_stream_id in stream_ids
            mdata[()] = root
            stream.metadata = metadata.to_list(mdata)
        return catalog

    @staticmethod
    def get_stream(catalog, tap_stream_id):
        for stream in catalog.streams:
            if stream.tap_stream_id == tap_stream_id:
                return stream
        raise AssertionError(f"Stream '{tap_stream_id}' not found in catalog")
