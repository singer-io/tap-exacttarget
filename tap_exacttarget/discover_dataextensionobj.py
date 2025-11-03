from singer import get_logger

from tap_exacttarget.client import Client
from tap_exacttarget.exceptions import MarketingCloudError
from tap_exacttarget.streams import DataExtensionObjectFt, DataExtensionObjectInc

LOGGER = get_logger()

field_type_mapping = {
    "Boolean": "boolean",
    "Decimal": "number",
    "Number": "integer",
    "EmailAddress": "string",
    "Text": "string",
    "Date": "string",
}

field_format = {"Decimal": "singer.decimal", "Date": "date-time"}


def detect_field_schema(field):

    schema = {}
    field_type = ["null"]

    field_type.append(field_type_mapping.get(field["FieldType"], "string"))
    schema["type"] = field_type

    if field["FieldType"] in field_format.keys():
        schema["format"] = field_format.get(field["FieldType"])

    return schema


def discover_fields(client: Client):

    has_data, req_id = True, None
    doa_fields, field_info = [], {}

    query_fields = ["Name", "IsRequired", "IsPrimaryKey", "FieldType", "DataExtension.CustomerKey"]
    LOGGER.info("Discovering fields from DataExtensionObjects")
    while has_data:
        response = client.retrieve_request("DataExtensionField", query_fields, request_id=req_id)
        req_id = response["RequestID"]
        if response["OverallStatus"] != "MoreDataAvailable":
            has_data = False
        doa_fields.extend(response["Results"])

    for field in doa_fields:
        stream_id = field["DataExtension"]["CustomerKey"]
        field_name = field["Name"]
        stream_field_data = field_info.setdefault(
            stream_id, {"key_properties": [], "valid_replication_keys": [], "properties": {}}
        )

        if field["IsPrimaryKey"]:
            stream_field_data["key_properties"].append(field_name)

        if field_name in {"ModifiedDate", "JoinDate", "_ModifiedDate", "_CreatedDate"}:
            stream_field_data["valid_replication_keys"].append(field_name)

        stream_field_data["properties"][field_name] = detect_field_schema(field)

    LOGGER.info("Finished processing DataExtensionFields count: %s", len(doa_fields))
    return field_info


def discover_dao_streams(client: Client):
    discovered_fields = discover_fields(client)
    LOGGER.info("Fetching DataExtension objects...")

    obj_ref = "DataExtension"
    fields = ["CustomerKey", "Name", "CategoryID"]
    request_id = None
    discovered_streams = {}
    has_more_data = True

    while has_more_data:
        response = client.retrieve_request(obj_ref, fields, request_id=request_id)
        request_id = response["RequestID"]
        status = response["OverallStatus"]

        if "Error" in status:
            raise MarketingCloudError(
                f"Request failed with status: {status}, Unable to discover Streams"
            )

        results = response["Results"]
        LOGGER.info("discovered %s DataExtensions", len(results))

        for item in results:
            customer_key = item["CustomerKey"]
            category_id = item["CategoryID"]

            stream_name = item["Name"]
            stream_id = f"data_extension.{stream_name}"
            stream_fields = discovered_fields.get(customer_key, {})

            key_props = ["_CustomObjectKey"] + stream_fields.get("key_properties", [])
            repl_keys = stream_fields.get("valid_replication_keys", [])
            props = stream_fields.get("properties", {})

            schema = {
                "type": "object",
                "properties": {
                    "_CustomObjectKey": {"type": ["string"]},
                    "CategoryID": {"type": ["null", "integer"]},
                    **props,
                },
            }

            # Modified Date is the preferred replication key
            # Maintaining original sequence of the key picking order
            replication_key = next(
                (
                    key
                    for key in ["ModifiedDate", "JoinDate", "_ModifiedDate", "_CreatedDate"]
                    if key in repl_keys
                ),
                None,
            )

            # mightContainUnexpectedSpecialChars
            name_suffix = stream_name.strip().lower()
            name_suffix = "".join(c for c in name_suffix if c.isalnum())
            class_name = f"DataExtensionObjStream{name_suffix}"

            # prevents unpredictable order of key properties
            sorted_key_props = sorted(list(set(key_props)))

            base_class = DataExtensionObjectFt if not repl_keys else DataExtensionObjectInc
            stream_class = type(
                class_name,
                (base_class,),
                {
                    "stream": stream_id,
                    "tap_stream_id": stream_id,
                    "object_ref": f"DataExtensionObject[{stream_name}]",
                    "key_properties": sorted_key_props,
                    "replication_key": replication_key,
                    "valid_replication_keys": repl_keys,
                    "schema": schema,
                    "customer_key": customer_key,
                    "category_id": category_id,
                },
            )

            discovered_streams[stream_id] = stream_class

        if status != "MoreDataAvailable":
            has_more_data = False

    return discovered_streams
