from base import ExactTargetBase
import datetime
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetFutureDate(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_future_date"

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        expected_streams = self.streams_to_select()

        future_date = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(days=1), "%Y-%m-%dT00:00:00Z")

        state = {'bookmarks': dict()}
        replication_keys = self.expected_replication_keys()
        for stream in expected_streams:
            if self.is_incremental(stream):
                state['bookmarks'][stream] = dict()
                state['bookmarks'][stream]['field'] = next(iter(replication_keys[stream]))
                state['bookmarks'][stream]['last_record'] = future_date

        # set state for running sync mode
        menagerie.set_state(conn_id, state)

        runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        # run sync mode
        self.run_and_verify_sync(conn_id)

        # get the state after running sync mode
        latest_state = menagerie.get_state(conn_id)

        # verify state passed before sync and
        # state got after sync are same
        self.assertEquals(latest_state, state)
