from datetime import datetime, timedelta

import backoff
import requests
from requests import Session
from requests.exceptions import ConnectionError, RequestException, HTTPError, Timeout
from singer import get_logger
from zeep import client, xsd
from zeep.transports import Transport
from zeep.exceptions import TransportError, Fault

from tap_exacttarget.exceptions import (
    IncompatibleFieldSelectionError,
    MarketingCloudError,
    MarketingCloudPermissionFailure,
    MarketingCloudSoapApiException,
)

LOGGER = get_logger()
DEFAULT_DATE_WINDOW = 30
DEFAULT_BATCH_SIZE = 2500
DEFAULT_TIMEOUT = 300


class Client:

    batch_size = 2500
    log_search_filter = True

    oauth_header = xsd.Element(
        "{http://exacttarget.com}fueloauth",
        xsd.ComplexType([xsd.Element("_value_1", xsd.String(), nillable=False)]),
    )

    def __init__(self, config: dict):
        LOGGER.info("WebService Client initialization Started.")

        self.config = config
        subdomain = config["tenant_subdomain"]
        client_id = config["client_id"]
        client_secret = config["client_secret"]
        self.timeout = int(config.get("request_timeout") or DEFAULT_TIMEOUT)

        try:
            self.date_window = float(config.get("date_window") or DEFAULT_DATE_WINDOW)
        except ValueError:
            self.date_window = DEFAULT_DATE_WINDOW
            LOGGER.info(
                "invalid value received for batch_size, fallback to default %s", DEFAULT_DATE_WINDOW
            )

        try:
            self.batch_size = int(config.get("batch_size") or DEFAULT_BATCH_SIZE)
        except ValueError:
            self.batch_size = DEFAULT_BATCH_SIZE
            LOGGER.info(
                "invalid value received for batch_size, fallback to default %s", DEFAULT_BATCH_SIZE
            )

        self.wsdl_uri = f"https://{subdomain}.soap.marketingcloudapis.com/etframework.wsdl"
        self.auth_url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"
        self.rest_url = f"https://{subdomain}.rest.marketingcloudapis.com/"

        self.__access_token = None
        self.token_expiry_time = None

        self.client_id = client_id
        self.client_secret = client_secret
        self.soap_client = self.initalize_soap_client()

        oauth_value = self.oauth_header(self.access_token)
        self.soap_client.set_default_soapheaders([oauth_value])
        LOGGER.info("WebService Client initialization Complete.")

    @backoff.on_exception(
        backoff.expo, (ConnectionError, Timeout, RequestException), max_tries=6, max_time=300
    )
    def initalize_soap_client(self):
        """Performs WebService Client init & handles Rerty."""

        session = Session()
        transport = Transport(session=session, timeout=300, operation_timeout=300)
        soap_client = client.Client(wsdl=self.wsdl_uri, transport=transport)
        return soap_client

    def is_token_expired(self):
        """Checks for token expiry."""
        if self.__access_token is None:
            return True

        if self.token_expiry_time and (
            datetime.now() + timedelta(minutes=5) < self.token_expiry_time
        ):
            return False

        return True

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @property
    def access_token(self):
        """Provides existing token if valid, if expired will refresh it."""
        if self.is_token_expired() or self.__access_token is None:
            LOGGER.info("Access token expired or not set, requesting new token.")
            payload = {"client_id": self.client_id}
            payload["client_secret"] = self.client_secret
            payload["grant_type"] = "client_credentials"
            try:

                response = requests.post(self.auth_url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                self.__access_token = data["access_token"]
                self.token_expiry_time = datetime.now() + timedelta(
                    seconds=(data["expires_in"] - 500)
                )

            except Exception as err:
                raise err
        return self.__access_token

    def create_simple_filter(self, property_name: str, operator: str, value=None, date_value=None):
        """Creates a filter object, handles case for date_value."""
        simple_part_filter = self.soap_client.get_type("ns0:SimpleFilterPart")
        _filter = simple_part_filter(Property=property_name, SimpleOperator=operator)
        if not value and date_value:
            _filter.DateValue = date_value
        elif value:
            _filter.Value = [value] if not isinstance(value, list) else value
        return _filter

    def create_complex_filter(self, left_operand, logical_operator: str, right_operand):
        """Create a ComplexFilterPart for combining multiple filter conditions.

        Args:
            left_operand: Left filter condition (SimpleFilterPart or ComplexFilterPart)
            logical_operator (str): Logical operator ('AND' or 'OR')
            right_operand: Right filter condition (SimpleFilterPart or ComplexFilterPart)

        Returns:
            ComplexFilterPart object

        Example:
            # Create individual simple filters
            filter1 = client.create_simple_filter('Status', 'equals', 'Active')
            filter2 = client.create_simple_filter('EmailAddress', 'isNotNull', '')

            # Combine with AND
            complex_filter = client.create_complex_filter(filter1, 'AND', filter2)

            # More complex example with nested conditions
            filter3 = client.create_simple_filter('SubscriberKey', 'greaterThan', '1000')
            nested_complex = client.create_complex_filter(complex_filter, 'OR', filter3)
        """
        complex_part_filter = self.soap_client.get_type("ns0:ComplexFilterPart")
        return complex_part_filter(
            LeftOperand=left_operand, LogicalOperator=logical_operator, RightOperand=right_operand
        )

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, Timeout, HTTPError, RequestException, TransportError, Fault),
        max_tries=5,
        max_time=300,
    )
    def retrieve_request(
        self, object_type: str, properties: list, request_id=None, search_filter=None
    ):
        retrieve_req = self.soap_client.get_type("ns0:RetrieveRequest")
        retrieve_opts = self.soap_client.get_type("ns0:RetrieveOptions")

        retrieve_options = retrieve_opts(
            BatchSize=self.batch_size,
            # This ensures all the Inherited APIObject fields are available
            IncludeObjects=True,
        )

        retrieve_request_obj = retrieve_req(
            ObjectType=object_type, Properties=properties, Options=retrieve_options
        )

        if search_filter:
            retrieve_request_obj.Filter = search_filter

        if request_id:
            retrieve_request_obj.ContinueRequest = request_id

        oauth_value = self.oauth_header(self.access_token)
        self.soap_client.set_default_soapheaders([oauth_value])
        LOGGER.info("Objtype: %s fields: %s", object_type, properties)

        try:
            response = self.soap_client.service.Retrieve(RetrieveRequest=retrieve_request_obj)
            self.raise_for_error(response)
            return response
        except (MarketingCloudError, TransportError, Fault) as err:
            if self.log_search_filter:
                LOGGER.info("Filter: %s", search_filter)
            if isinstance(err, Fault) or isinstance(err, TransportError):
                raise MarketingCloudSoapApiException(
                    f"SOAP Fault or Transport Error: {err}"
                ) from err
            raise err

    def raise_for_error(self, response):
        """Handles basic request failure."""
        if "Error: The Request Property(s)" in response["OverallStatus"]:
            raise IncompatibleFieldSelectionError(response["OverallStatus"])

        if "Error: Invalid column name" in response["OverallStatus"]:
            raise IncompatibleFieldSelectionError(response["OverallStatus"])

        if "Error: API Permission Failed" in response["OverallStatus"]:
            raise MarketingCloudPermissionFailure(response["OverallStatus"])

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, Timeout, HTTPError, RequestException),
        max_tries=5,
        max_time=300,
    )
    def get_rest(self, endpoint, params):

        headers = {"Authorization": f"Bearer {self.access_token}"}
        final_url = f"{self.rest_url}{endpoint}"
        response = requests.get(final_url, headers=headers, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def describe_request(self, object_type):
        """Queries schema definition for ET Objects."""

        obj_defn_reqs = self.soap_client.get_type("ns0:ObjectDefinitionRequest")
        arr_obj_defn_reqs = self.soap_client.get_type("ns0:ArrayOfObjectDefinitionRequest")

        obj_def = obj_defn_reqs(ObjectType=object_type)
        obj_def_array = arr_obj_defn_reqs(ObjectDefinitionRequest=[obj_def])

        oauth_value = self.oauth_header(self.access_token)
        self.soap_client.set_default_soapheaders([oauth_value])

        return self.soap_client.service.Describe(obj_def_array)
