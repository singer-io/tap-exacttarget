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
            'start_date': '2019-01-01T00:00:00Z',
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
                "primary-key": {"id"},
                "replication": "FULL"
            },
            "content_area":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "email":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "event": {
                "primary-key": {"SendID", "EventType", "SubscriberKey", "EventDate"},
                "replication": "INCREMENTAL",
                "replication-key": {"EventDate"},
            },
            "folder":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "list":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "list_send":{
                "primary-key": {"ListID", "SendID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "list_subscriber":{
                "primary-key": {"SubscriberKey", "ListID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "send":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            },
            "subscriber":{
                "primary-key": {"ID"},
                "replication": "INCREMENTAL",
                "replication-key": {"ModifiedDate"},
            } 
        }

    def streams_to_select(self):
        return set(self.expected_metadata().keys()) - {'event', 'list_send'}

    def expected_replication_keys(self):
        return {table: properties.get("replication-key", set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_primary_keys(self):
        return {table: properties.get("primary-key", set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        return {table: properties.get("replication", set())
                for table, properties
                in self.expected_metadata().items()}

    def select_found_catalogs(self, conn_id, catalogs, only_streams=None, deselect_all_fields: bool = False, non_selected_props=[]):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            if only_streams and catalog["tap_stream_id"] not in only_streams:
                continue
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = non_selected_props if not deselect_all_fields else []
            if deselect_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {})
                non_selected_properties = non_selected_properties.keys()
            additional_md = []

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, additional_md=additional_md,
                non_selected_fields=non_selected_properties
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
