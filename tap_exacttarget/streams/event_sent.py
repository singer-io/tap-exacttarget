import re
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import IncrementalStream


class SentEvent(IncrementalStream):
    """Class for Sent Event stream."""

    client: Client

    stream = "sentevent"
    tap_stream_id = "sentevent"
    object_ref = "SentEvent"
    key_properties = ["SendID", "EventType", "SubscriberKey", "EventDate"]
    replication_key = "EventDate"
    valid_replication_keys = ["EventDate"]

    # Reserved properties that should not be overwritten by PartnerProperties
    RESERVED_PROPERTIES = {
        "SendID", "EventType", "SubscriberKey", "EventDate",
        "BatchID", "ListID", "PartnerProperties", "PartnerKey",
        "SubscriberID", "TriggeredSendDefinitionObjectID"
    }
    
    # Regex pattern for validating property names (compiled once for performance)
    NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')

    def transform_record(self, obj):
        obj = super().transform_record(obj)

        # Validate and process PartnerProperties
        partner_properties = obj.get('PartnerProperties')
        if partner_properties and isinstance(partner_properties, list):
            for item in partner_properties:
                # Validate item structure
                if not isinstance(item, dict):
                    continue
                
                # Ensure both Name and Value keys exist
                name = item.get("Name")
                value = item.get("Value")
                
                # Skip if name is None or empty string
                if name is None or name == '':
                    continue
                
                # Sanitize property name - only allow alphanumeric, underscore, and dash
                # Convert to string in case it's not already
                name_str = str(name)
                if not self.NAME_PATTERN.match(name_str):
                    # Skip properties with invalid characters
                    continue
                
                # Prevent overwriting reserved properties
                if name_str in self.RESERVED_PROPERTIES:
                    continue
                
                # Add the property with a prefix to avoid conflicts
                obj[f"partner_{name_str}"] = value

        if obj['SubscriberKey'] is not None:
            return obj
