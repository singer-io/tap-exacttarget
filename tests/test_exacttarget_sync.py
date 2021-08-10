from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os

class ExactTargetSync(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_sync_v1"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props.pop('tenant_subdomain')
        return props

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=self.streams_to_select())

        sync_record_count = self.run_and_verify_sync(conn_id)

        for stream in self.streams_to_select():
            self.assertGreater(sync_record_count.get(stream, 0), 0)

class ExactTargetSync2(ExactTargetSync):
    def name(self):
        return "tap_tester_exacttarget_sync_v1_with_subdomain"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['tenant_subdomain'] = os.getenv('TAP_EXACTTARGET_TENANT_SUBDOMAIN')
        return props


class ExactTargetSync3(ExactTargetSync):
    def name(self):
        return "tap_tester_exacttarget_sync_v2_with_subdomain"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_V2_CLIENT_SECRET')
        }

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['client_id'] = os.getenv('TAP_EXACTTARGET_V2_CLIENT_ID')
        props['tenant_subdomain'] = os.getenv('TAP_EXACTTARGET_V2_TENANT_SUBDOMAIN')
        return props
