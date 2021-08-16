import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
import unittest
from datetime import datetime as dt
import time

class ExactTargetBase(unittest.TestCase):
    START_DATE = ""
    DATETIME_FMT = {
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    }

    def name(self):
        return "tap_tester_exacttarget_base"

    def tap_name(self):
        return "tap-exacttarget"

    def setUp(self):
        required_env = {
            "TAP_EXACTTARGET_CLIENT_ID",
            "TAP_EXACTTARGET_CLIENT_SECRET",
            "TAP_EXACTTARGET_TENANT_SUBDOMAIN",
            "TAP_EXACTTARGET_V2_CLIENT_ID",
            "TAP_EXACTTARGET_V2_CLIENT_SECRET",
            "TAP_EXACTTARGET_V2_TENANT_SUBDOMAIN",
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.exacttarget"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date': '2014-01-01T00:00:00Z',
            'client_id': os.getenv('TAP_EXACTTARGET_CLIENT_ID'),
            'tenant_subdomain': os.getenv('TAP_EXACTTARGET_TENANT_SUBDOMAIN')
        }
        if original:
            return return_value

        # Reassign start date
        return_value["start_date"] = self.START_DATE
        return return_value

    def expected_metadata(self):
        # Note: Custom streams failed on our account with an error on
        # `_CustomObjectKey` not being valid
        return {
            "campaign": {
                "pk": ["id"],
                "replication": "FULL"
            },
            "content_area":{
                "pk": ["ID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "email":{
                "pk": ["ID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "event": {
                "pk": ["SendID", "EventType", "SubscriberKey", "EventDate"],
                "replication": "INCREMENTAL",
                "replication-key": ["EventDate"]
            },
            "folder":{
                "pk": ["ID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "list":{
                "pk": ["ID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "list_send":{
                "pk": ["ListID", "SendID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "list_subscriber":{
                "pk": ["SubscriberKey", "ListID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "send":{
                "pk": ["ID"],
                "replication": "INCREMENTAL",
                "replication-key": ["ModifiedDate"]
            },
            "subscriber":{
                "pk": ["ID"],
                "replication": "FULL"
            } 
        }

    def streams_to_select(self):
        return set(self.expected_metadata().keys()) - {'event', 'list_subscriber', 'subscriber'}

    def expected_primary_keys(self):
        return {table: properties.get("pk", set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        return {table: properties.get("replication", set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_start_date_keys(self):
        return {table: properties.get("replication-key", set())
                for table, properties
                in self.expected_metadata().items()}

    def select_found_catalogs(self, conn_id, found_catalogs, only_streams=None):
        selected = []
        for catalog in found_catalogs:
            if only_streams and catalog["tap_stream_id"] not in only_streams:
                continue
            schema = menagerie.select_catalog(conn_id, catalog)

            selected.append({
                "key_properties": catalog.get("key_properties"),
                "schema": schema,
                "tap_stream_id": catalog.get("tap_stream_id"),
                "replication_method": catalog.get("replication_method"),
                "replication_key": catalog.get("replication_key"),
            })

        for catalog_entry in selected:
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                {"annotated-schema": catalog_entry['schema']}
            )

    def run_and_verify_sync(self, conn_id):
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.streams_to_select(), self.expected_primary_keys())

        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def dt_to_ts(self, dtime):
        for date_format in self.DATETIME_FMT:
            try:
                date_stripped = int(time.mktime(dt.strptime(dtime, date_format).timetuple()))
                return date_stripped
            except ValueError:
                continue

    def is_incremental(self, stream):
        return self.expected_metadata()[stream]["replication"] == "INCREMENTAL"
