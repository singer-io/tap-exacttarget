import backoff
import socket
import functools
import requests
import singer
import os
import urllib
from singer import metadata, Transformer, utils

from funcy import project

from util import sudsobj_to_dict

LOGGER = singer.get_logger()


def _get_catalog_schema(catalog):
    return catalog.get("schema", {}).get("properties")


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# function to load the fields in the 'definitions' which contains the reference fields
def load_schema_references():
    shared_schema_path = get_abs_path("schemas/definitions.json")

    refs = {}
    # load json from the path
    refs["definitions.json"] = utils.load_json(shared_schema_path)

    return refs


# function to load schema from json file
def load_schema(stream):
    path = get_abs_path("schemas/{}s.json".format(stream))
    # load json from the path
    schema = utils.load_json(path)

    return schema


# boolean function to check if the error is 'timeout' error or not
def is_timeout_error(e):
    """
    This function checks whether the URLError contains 'timed out' substring and return boolean
    values accordingly, to decide whether to backoff or not.
    """
    # retry if the error string contains 'timed out'
    if str(e).__contains__("timed out"):
        return False
    return True


# decorator for retrying on error
def exacttarget_error_handling(fnc):
    @backoff.on_exception(
        backoff.expo,
        urllib.error.URLError,  # backoff 'timeout' error for SOAP API
        giveup=is_timeout_error,
        max_tries=5,
        factor=2,
    )
    @backoff.on_exception(
        backoff.expo,
        (socket.timeout, ConnectionError, requests.Timeout),
        max_tries=5,
        factor=2,
    )
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)

    return wrapper


class DataAccessObject:

    def __init__(self, config, state, auth_stub, catalog):
        self.config = config.copy()
        self.state = state.copy()
        self.catalog = catalog
        self.auth_stub = auth_stub
        # initialize batch size
        self.batch_size = int(self.config.get("batch_size", 2500))

    @classmethod
    def matches_catalog(cls, catalog):
        return catalog.get("stream") == cls.TABLE

    # generate schema and metadata for adding in catalog file
    def generate_catalog(self):
        cls = self.__class__

        # get the reference schemas
        refs = load_schema_references()
        # resolve the schema reference and make final schema
        schema = singer.resolve_schema_references(load_schema(cls.TABLE), refs)
        mdata = metadata.new()

        # use 'get_standard_metadata' with primary key, replication key and replication method
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.KEY_PROPERTIES,
            valid_replication_keys=(
                self.REPLICATION_KEYS if self.REPLICATION_KEYS else None
            ),
            replication_method=self.REPLICATION_METHOD,
        )

        mdata_map = metadata.to_map(mdata)
        # make 'automatic' inclusion for replication keys
        for replication_key in self.REPLICATION_KEYS:
            mdata_map[("properties", replication_key)]["inclusion"] = "automatic"

        return [
            {
                "tap_stream_id": cls.TABLE,
                "stream": cls.TABLE,
                "key_properties": cls.KEY_PROPERTIES,
                "schema": schema,
                "metadata": metadata.to_list(mdata_map),
            }
        ]

    # convert suds object to dictionary
    def filter_keys_and_parse(self, obj):
        to_return = sudsobj_to_dict(obj)

        return self.parse_object(to_return)

    # get the list for keys present in the schema
    def get_catalog_keys(self):
        return list(self.catalog.get("schema", {}).get("properties", {}).keys())

    def parse_object(self, obj):
        return project(obj, self.get_catalog_keys())

    # a function to write records by applying transformation
    @staticmethod
    def write_records_with_transform(record, catalog, table):
        with Transformer() as transformer:
            rec = transformer.transform(
                record, catalog.get("schema"), metadata.to_map(catalog.get("metadata"))
            )
            singer.write_record(table, rec)

    def write_schema(self):
        singer.write_schema(
            self.catalog.get("stream"),
            self.catalog.get("schema"),
            key_properties=self.catalog.get("key_properties"),
        )

    # main 'sync' function
    def sync(self):

        self.write_schema()

        return self.sync_data()

    # OVERRIDE THESE TO IMPLEMENT A NEW DAO:

    TABLE = None
    KEY_PROPERTIES = None
    REPLICATION_KEYS = []
    REPLICATION_METHOD = None

    # function to be overridden by the respective stream files and implement sync
    def sync_data(self):  # pylint: disable=no-self-use
        raise RuntimeError("sync_data is not implemented!")
