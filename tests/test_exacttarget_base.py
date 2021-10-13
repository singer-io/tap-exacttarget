
import datetime
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
import unittest
import pdb
import json
import requests


class ExactTargetBase(unittest.TestCase):

    def name(self):
        return "tap_tester_exacttarget_base"

    def tap_name(self):
        return "tap-exacttarget"

    def setUp(self):
        required_env = {
            "client_id": "TAP_EXACTTARGET_CLIENT_ID",
            "client_secret": "TAP_EXACTTARGET_CLIENT_SECRET",
        }
        missing_envs = [v for v in required_env.values() if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.exacttarget"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self):
        yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        return {
            'start_date': yesterday.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'client_id': os.getenv('TAP_EXACTTARGET_CLIENT_ID')
        }

    def streams_to_select(self):
        # Note: Custom streams failed on our account with an error on
        # `_CustomObjectKey` not being valid
        return ["campaign",
                "content_area",
                "email",
                "event",
                "folder",
                "list",
                "list_send",
                "list_subscriber",
                "send",
                "subscriber"]

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

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=self.streams_to_select())
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)


