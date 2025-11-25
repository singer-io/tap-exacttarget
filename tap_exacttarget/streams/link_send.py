from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class LinkSend(FullTableStream):
    """Class for Link Send stream."""
    # https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/linksend.html

    # Works Well as full table but not incremental
    # Supported Fields
    # INFO Objtype: LinkSend fields: ['ID', 'SendID', 'PartnerKey', 'Client.ID', 'Client.PartnerClientKey', 'Link.ID', 'Link.PartnerKey', 'Link.TotalClicks', 'Link.UniqueClicks', 'Link.URL', 'Link.Alias']
    # Error
    #     raise MarketingCloudError(response["OverallStatus"])
    # tap_exacttarget.exceptions.MarketingCloudError: Error: The Filter Property 'ModifiedDate' is not a retrievable property.

    # Can be implemented as psuedo incremental but no valid records available with a ModifiedDate thus making implementation challenging

    client: Client

    stream = "link_send"
    tap_stream_id = "link_send"
    object_ref = "LinkSend"
    key_properties = ["ID"]
    # replication_key = "ModifiedDate"
    # valid_replication_keys = ["ModifiedDate"]
