from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class AutomationInstance(FullTableStream):
    """Class for List Send stream."""

    # INFO OBj: AutomationInstance Non Retrievable fields ['AutomationID', 'StatusMessage', 'StatusLastUpdate',
    # 'TaskInstances', 'StartTime', 'CompletedTime', 'Schedule', 'AutomationTasks', 'IsActive', 'AutomationSource', 'Status',
    # 'Notifications', 'ScheduledTime', 'AutomationType', 'UpdateModified', 'LastRunInstanceID', 'CreatedBy', 'CategoryID',
    # 'LastRunTime', 'LastSaveDate', 'ModifiedBy', 'RecurrenceID', 'LastSavedBy', 'InteractionObjectID', 'Name',
    #  'Description', 'Keyword', 'Client', 'PartnerKey', 'PartnerProperties', 'CreatedDate', 'ModifiedDate', 'ID',
    # 'CustomerKey', 'Owner', 'CorrelationID', 'ObjectState', 'IsPlatformObject']


    # INFO OBj: AutomationInstance Retrievable fields ['ObjectID']

    # Only 1 field is accessable

    client: Client

    stream = "automation_instance"
    tap_stream_id = "automation_instance"
    object_ref = "AutomationInstance"
    key_properties = ["ID"]
    replication_key = "ModifiedDate"
    valid_replication_keys = ["ModifiedDate"]