import time
import traceback

import FuelSDK
import singer

from suds.transport.https import HttpAuthenticated
from tap_exacttarget.fuel_overrides import tap_exacttarget__getMoreResults

LOGGER = singer.get_logger()


def _get_response_items(response):
    items = response.results

    if 'count' in response.results:
        LOGGER.info('Got {} results.'.format(response.results.get('count')))
        items = response.results.get('items')

    return items


class RetryDecorator(object):
    retry_count = 1
    min_retry_delay_seconds = 5
    max_retry_delay_seconds = 600

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        retry_number = 0

        while retry_number < self.retry_count:
            try:
                return self.func(*args, **kwargs)
            except Exception as e:
                LOGGER.error(
                    u"Error reading data from API on try {}".format(
                        retry_number + 1))
                LOGGER.error(traceback.format_exc())

                retry_number += 1
                retry_delay = min(
                    self.min_retry_delay_seconds * retry_number * retry_number,
                    self.max_retry_delay_seconds)
                time.sleep(retry_delay)
                continue

        raise RuntimeError("Maximum number of retries reached")


@RetryDecorator
def _get_data_from_cursor(cursor):
    response = cursor.get()
    if not response.status:
        raise RuntimeError("Request failed with '{}'"
                           .format(response.message))
    return response


@RetryDecorator
def _get_more_results(cursor, batch_size):
    response = tap_exacttarget__getMoreResults(
        cursor, batch_size=batch_size)
    if not response.status:
        raise RuntimeError("Request failed with '{}'"
                           .format(response.message))
    return response


__all__ = ['change_retry_count',
           'change_min_retry_delay_seconds',
           'change_max_retry_delay_seconds',
           'get_auth_stub',
           'request',
           'request_from_cursor']


# PUBLIC FUNCTIONS
def change_retry_count(new_retry_count):
    RetryDecorator.retry_count = new_retry_count


def change_min_retry_delay_seconds(new_min_retry_delay_seconds):
    RetryDecorator.min_retry_delay_seconds = new_min_retry_delay_seconds


def change_max_retry_delay_seconds(new_max_retry_delay_seconds):
    RetryDecorator.max_retry_delay_seconds = new_max_retry_delay_seconds


def get_auth_stub(config):
    """
    Given a config dict in the format:

        {'clientid': ... your ET client ID ...,
         'clientsecret': ... your ET client secret ...}

    ... return an auth stub to be used when making requests.
    """
    LOGGER.info("Generating auth stub...")

    params = {
        'clientid': config['client_id'],
        'clientsecret': config['client_secret']
        }

    if config.get('tenant_subdomain'):
        # For S10+ accounts: https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/your-subdomain-tenant-specific-endpoints.htm

        params['authenticationurl'] = ('https://{}.auth.marketingcloudapis.com/v1/requestToken'
                                       .format(config['tenant_subdomain']))
        LOGGER.info("Authentication URL is: %s", params['authenticationurl'])
        params['soapendpoint'] = ('https://{}.soap.marketingcloudapis.com/Service.asmx'
                                  .format(config['tenant_subdomain']))

    # First try V1
    try:
        LOGGER.info('Trying to authenticate using V1 endpoint')
        params['useOAuth2Authentication'] = "False"
        auth_stub = FuelSDK.ET_Client(params=params)
        transport = HttpAuthenticated(timeout=int(config.get('request_timeout', 900)))
        auth_stub.soap_client.set_options(
            transport=transport)
        LOGGER.info("Success.")
        return auth_stub
    except Exception as e:
        LOGGER.info('Failed to auth using V1 endpoint')
        if not config.get('tenant_subdomain'):
            LOGGER.warning('No tenant_subdomain found, will not attempt to auth with V2 endpoint')
            raise e

    # Next try V2
    # Move to OAuth2: https://help.salesforce.com/articleView?id=mc_rn_january_2019_platform_ip_remove_legacy_package_create_ability.htm&type=5
    try:
        LOGGER.info('Trying to authenticate using V2 endpoint')
        params['useOAuth2Authentication'] = "True"
        params['authenticationurl'] = ('https://{}.auth.marketingcloudapis.com'
                                       .format(config['tenant_subdomain']))
        LOGGER.info("Authentication URL is: %s", params['authenticationurl'])
        auth_stub = FuelSDK.ET_Client(params=params)
        transport = HttpAuthenticated(timeout=int(config.get('request_timeout', 900)))
        auth_stub.soap_client.set_options(
            transport=transport)
    except Exception as e:
        LOGGER.info('Failed to auth using V2 endpoint')
        raise e

    LOGGER.info("Success.")
    return auth_stub


def request(name, selector, auth_stub, search_filter=None, props=None, batch_size=2500):
    """
    Given an object name (`name`), used for logging purposes only,
      a `selector`, for example FuelSDK.ET_ClickEvent,
      an `auth_stub`, generated by `get_auth_stub`,
      an optional `search_filter`,
      and an optional set of `props` (properties), which specifies the fields
        to be returned from this object,

    ... request data from the ExactTarget API using FuelSDK. This function
    returns a generator that will yield all the records returned by the
    request.

    Example `search_filter`:

        {'Property': 'CustomerKey',
         'SimpleOperator': 'equals',
         'Value': 'abcdef'}

    For more on search filters, see:
      https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/using_complex_filter_parts.htm
    """
    cursor = selector()
    cursor.auth_stub = auth_stub

    if props is not None:
        cursor.props = props

    if search_filter is not None:
        cursor.search_filter = search_filter

        LOGGER.info(
            "Making RETRIEVE call to '{}' endpoint with filters '{}'."
            .format(name, search_filter))

    else:
        LOGGER.info(
            "Making RETRIEVE call to '{}' endpoint with no filters."
            .format(name))

    return request_from_cursor(name, cursor, batch_size)


def request_from_cursor(name, cursor, batch_size):
    """
    Given an object name (`name`), used for logging purposes only, and a
    `cursor` provided by FuelSDK, return a generator that yields all the
    items in that cursor.

    Primarily used internally by `request`, but can be used if cursors have
    to be customized. See tap_exacttarget.endpoints.data_extensions for
    an example.
    """
    response = _get_data_from_cursor(cursor)
    for item in _get_response_items(response):
        yield item

    while response.more_results:
        LOGGER.info("Getting more results from '{}' endpoint".format(name))

        response = _get_more_results(cursor, batch_size)

        LOGGER.info("Fetched {} results from '{}' endpoint".format(
            len(response.results), name))

        for item in _get_response_items(response):
            yield item

    LOGGER.info("Done retrieving results from '{}' endpoint".format(name))
