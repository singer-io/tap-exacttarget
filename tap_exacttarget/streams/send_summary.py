from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class SendSummary(FullTableStream):
    """Class for SendSummary stream."""

    # https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/sendsummary.html

    # Error
    # singer.transform.SchemaMismatch: Errors during transform
    #     ID: data does not match {'type': 'integer'}

    # Supported Fields
    # INFO Objtype: SendSummary fields: ['Client.ID', 'AccountID', 'SendID', 'DeliveredTime', 'CreatedDate', 'ModifiedDate', 'CustomerKey', 'PartnerKey', 'AccountName', 'AccountEmail', 'IsTestAccount', 'TotalSent', 'Transactional', 'NonTransactional']
    # ID or ObjectID missing in supported fields, might be related to a permission issue

    client: Client

    stream = "send_summary"
    tap_stream_id = "send_summary"
    object_ref = "SendSummary"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
