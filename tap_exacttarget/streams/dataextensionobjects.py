from typing import Dict

from singer import get_logger

from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream, IncrementalStream

LOGGER = get_logger()

TRUTHY_VALUES = {1, "1", "y", "yes", "true"}
FALSY_VALUES = {0, "0", "n", "no", "false"}


class DataExtensionObjectBase:
    """Base class for Data Extension Objects."""

    client: Client
    schema = None
    customer_key = None
    category_id = None

    def get_query_fields(self, stream_metadata: Dict, schema: Dict):
        """Filter fields to query from metadata."""

        available_fields = schema.get("properties", {}).keys()
        selected_fields = []

        for key, item in stream_metadata.items():
            if item.get("selected") or item.get("inclusion") == "automatic":
                if isinstance(key, tuple) and len(key) == 2:
                    selected_fields.append(key[1])

        query_fields = []
        for field in selected_fields:
            if field in available_fields and field != "CategoryID":
                query_fields.append(field)

        return query_fields

    def transform_record(self, obj: Dict):

        obj_schema = self.schema["properties"]
        properties = obj["Properties"]["Property"]
        to_return = {}

        for prop in properties:
            to_return[prop["Name"]] = prop["Value"]

        for k, v in to_return.items():
            if v is None:
                to_return[k] = None
                continue
            field_schema = obj_schema.get(k, {})

            if "integer" in field_schema.get("type"):
                to_return[k] = int(v)

            elif "number" in field_schema.get("type"):
                to_return[k] = float(v)

            elif "boolean" in field_schema.get("type") and isinstance(to_return[k], str):
                # Extension bools can come through as a number of values, see:
                # https://help.salesforce.com/articleView?id=mc_es_data_extension_data_types.htm&type=5
                # In practice, looks like they come through as either "True"
                # or "False", but for completeness I have included the other
                # possible values.
                if str(to_return[k]).lower() in TRUTHY_VALUES:
                    to_return[k] = True
                elif str(to_return[k]).lower() in FALSY_VALUES:
                    to_return[k] = False
                else:
                    LOGGER.warning("Could not infer boolean value from %s", to_return[k])
                    to_return[k] = None

        to_return["CategoryID"] = self.category_id
        return to_return


class DataExtensionObjectInc(DataExtensionObjectBase, IncrementalStream):
    """Encapsulates DataExtension Incremental."""


class DataExtensionObjectFt(DataExtensionObjectBase, FullTableStream):
    """Encapsulates DataExtension FullTable."""
