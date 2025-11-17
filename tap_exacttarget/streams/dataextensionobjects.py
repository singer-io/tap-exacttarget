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
    object_ref = None

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
        LOGGER.info("Objtype: %s fields: %s", self.object_ref, query_fields)
        return query_fields

    def transform_record(self, obj: Dict):

        obj_schema = self.schema["properties"]
        properties = obj["Properties"]["Property"]

        to_return = {p["Name"]: (None if p["Value"] is None else p["Value"]) for p in properties}

        for k, v in to_return.items():
            field_schema = obj_schema.get(k)
            if not field_schema:
                continue

            if "integer" in field_schema.get("type"):
                to_return[k] = int(v)
            elif "number" in field_schema.get("type"):
                to_return[k] = float(v)

            if "boolean" in field_schema.get("type") and isinstance(to_return[k], str):
                val = str(v).lower()
                if val in TRUTHY_VALUES | FALSY_VALUES:
                    to_return[k] = val in TRUTHY_VALUES
                else:
                    LOGGER.warning("Could not infer boolean value from %s", to_return[k])
                    to_return[k] = None

        to_return["CategoryID"] = self.category_id
        return to_return


class DataExtensionObjectInc(DataExtensionObjectBase, IncrementalStream):
    """Encapsulates DataExtension Incremental."""


class DataExtensionObjectFt(DataExtensionObjectBase, FullTableStream):
    """Encapsulates DataExtension FullTable."""
