from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os

class ExactTargetSync(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_sync"

    def test_run(self):
        self.run_test('2014-01-01T00:00:00Z', self.streams_to_select() - {'event', 'list_subscriber', 'subscriber'})

    def run_test(self, start_date, expected_streams):
        self.START_DATE = start_date
        conn_id = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        sync_record_count = self.run_and_verify_sync(conn_id)

        for stream in expected_streams:
            self.assertGreater(sync_record_count.get(stream, 0), 0)
