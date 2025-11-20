from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class Automation(FullTableStream):
    """Class for Automation stream."""

        # INFO Objtype: Automation fields: ['ObjectID', 'Name', 'Description', 'Schedule.ID', 'CustomerKey', 'Client.ID', 'IsActive', 'CreatedDate',
    # 'Client.CreatedBy', 'ModifiedDate', 'Client.ModifiedBy', 'Status', 'Client.EnterpriseID']

    # ID not retrievable
    # ObjectID is retrievable but also null
    # Single Record with almost all null values
    # Null values for replication key(expected), unable to test pagination etc "ModifiedDate": null,
    # no records with any valiid replication key value


    client: Client

    stream = "automation"
    tap_stream_id = "automation"
    object_ref = "Automation"
    key_properties = ["ObjectID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]

