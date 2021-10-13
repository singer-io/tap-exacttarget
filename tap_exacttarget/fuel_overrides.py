
import FuelSDK

"""
This module overrides classes and methods deep inside of the FuelSDK module.
We need to do this in order to specify the batch size for the getMoreRequests()
method call. This is necessary because the default, 2500 records, consumes too
much memory to be run in Stitch. If the Stitch memory limits are raised, or if
there is a more native way to specify this configuration, then this tap should
be modified to call into the getMoreRequests() method in the FuelSDK module.

See: https://github.com/singer-io/tap-exacttarget/issues/25
"""


class TapExacttarget__ET_Continue(FuelSDK.rest.ET_Constructor):
    def __init__(self, auth_stub, request_id, batch_size):
        auth_stub.refresh_token()

        ws_continueRequest = auth_stub.soap_client.factory.create('RetrieveRequest')
        ws_continueRequest.ContinueRequest = request_id

        # tap-exacttarget override: set batch size here
        ws_continueRequest.Options.BatchSize = batch_size

        response = auth_stub.soap_client.service.Retrieve(ws_continueRequest)

        if response is not None:
            super().__init__(response)

def tap_exacttarget__getMoreResults(cursor, batch_size=2500):
    obj = TapExacttarget__ET_Continue(cursor.auth_stub, cursor.last_request_id, batch_size)
    if obj is not None:
        cursor.last_request_id = obj.request_id

    return obj

# extend 'get' from 'ET_DataExtension_Row' and add 'options' parameter to set 'batch_size'
class TapExacttarget__ET_DataExtension_Row(FuelSDK.ET_DataExtension_Row):

    def get(self):
        self.getName()
        '''
        if props and props.is_a? Array then
            @props = props
        end
        '''

        if self.props is not None and type(self.props) is dict: # pylint:disable=unidiomatic-typecheck
            self.props = self.props.keys()

        '''
        if filter and filter.is_a? Hash then
            @filter = filter
        end
        '''

        # add 'options' parameter to set 'batch_size'
        obj = FuelSDK.ET_Get(self.auth_stub, "DataExtensionObject[{0}]".format(self.Name), self.props, self.search_filter, self.options)
        self.last_request_id = obj.request_id

        return obj

# extend 'get' from 'ET_DataExtension_Column' and add 'options' parameter to set 'batch_size'
class TapExacttarget__ET_DataExtension_Column(FuelSDK.ET_DataExtension_Column):

    def get(self):
        '''
        if props and props.is_a? Array then
            @props = props
        end
        '''

        if self.props is not None and type(self.props) is dict: # pylint:disable=unidiomatic-typecheck
            self.props = self.props.keys()

        '''
        if filter and filter.is_a? Hash then
            @filter = filter
        end
        '''

        '''
        fixCustomerKey = False
        if filter and filter.is_a? Hash then
            @filter = filter
            if @filter.has_key?("Property") && @filter["Property"] == "CustomerKey" then
                @filter["Property"]  = "DataExtension.CustomerKey"
                fixCustomerKey = true
            end
        end
        '''

        # add 'options' parameter to set 'batch_size'
        obj = FuelSDK.ET_Get(self.auth_stub, self.obj, self.props, self.search_filter, self.options)
        self.last_request_id = obj.request_id

        '''
        if fixCustomerKey then
            @filter["Property"] = "CustomerKey"
        end
        '''

        return obj
