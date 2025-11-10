from typing import Dict
from tap_exacttarget.client import Client
from tap_exacttarget.streams.abstracts import FullTableStream


class AccountUser(FullTableStream):
    """Class for List Send stream."""

    client: Client

    stream = "account_user"
    tap_stream_id = "account_user"
    object_ref = "AccountUser"
    key_properties = ["ID"]
    # repl_keys = ["ModifiedDate"]
