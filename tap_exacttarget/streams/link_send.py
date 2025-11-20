from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class LinkSend(IncrementalStream):
    """Class for Link Send stream."""
    # https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/linksend.html

    # Error:
    # singer.transform.SchemaMismatch: Errors during transform
    #     IDLong: data does not match {'type': 'integer'} -- Changed to ID and added null

    # Works Well as full table but not incremental
    # Supported Fields
    # INFO Objtype: LinkSend fields: ['ID', 'SendID', 'PartnerKey', 'Client.ID', 'Client.PartnerClientKey', 'Link.ID', 'Link.PartnerKey', 'Link.TotalClicks', 'Link.UniqueClicks', 'Link.URL', 'Link.Alias']
    # Error
    #     raise MarketingCloudError(response["OverallStatus"])
    # tap_exacttarget.exceptions.MarketingCloudError: Error: The Filter Property 'ModifiedDate' is not a retrievable property.

    client: Client

    stream = "link_send"
    tap_stream_id = "link_send"
    object_ref = "LinkSend"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]
