
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
            super(TapExacttarget__ET_Continue, self).__init__(response)

def tap_exacttarget__getMoreResults(cursor, batch_size=2500):
    obj = TapExacttarget__ET_Continue(cursor.auth_stub, cursor.last_request_id, batch_size)
    if obj is not None:
        cursor.last_request_id = obj.request_id

    return obj
