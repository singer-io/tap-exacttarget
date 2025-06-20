from datetime import datetime, timezone, timedelta
from typing import Dict, List
from singer import Transformer, get_logger, write_record
from singer.utils import now, strftime, strptime_to_utc

from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream, IncrementalStream

LOGGER = get_logger()


class DataExtentionObjectBase:

    client: Client
    schema = None
    customer_key = None
    category_id = None

    def get_query_fields(self, stream_metadata: Dict, schema: List):

        available = schema["properties"].keys()
        selected = []
        query_fields = []

        for key, item in stream_metadata.items():
            if item.get("inclusion") == "automatic" or item.get("selected"):
                if len(key) == 2:
                    selected.append(key[1])

        for key in selected:
            if key not in available:
                continue
            else:
                query_fields.append(key)

        # Cannot be queried, fetched from parent obj
        if "CategoryID" in query_fields:
            query_fields.remove("CategoryID")

        return query_fields

    def transform_record(self, obj):
        LOGGER.info("record for tx %s", obj)
        obj_schema = self.schema["properties"]
        properties = obj["Properties"]["Property"]
        to_return = {}

        for prop in properties:
            to_return[prop["Name"]] = prop["Value"]

        for k, v in to_return.items():
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
                if str(to_return[k]).lower() in [1, "1", "y", "yes", "true"]:
                    to_return[k] = True
                elif str(to_return[k]).lower() in [0, "0", "n", "no", "false"]:
                    to_return[k] = False
                else:
                    LOGGER.warning("Could not infer boolean value from %s", to_return[k])
                    to_return[k] = None

        to_return["CategoryID"] = self.category_id
        return to_return


class DataExtentionObjectInc(DataExtentionObjectBase, IncrementalStream):
    """Encapsulates DataExtention Incremental"""


class DataExtentionObjectFt(DataExtentionObjectBase, FullTableStream):
    """Encapsulates DataExtention FullTable"""
