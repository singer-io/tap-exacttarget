import datetime
from dateutil.parser import parse
from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import datetime
import dateutil.parser
import pytz

class ExactTargetBookmarks(ExactTargetBase):
    def name(self):
        return "tap_tester_exacttarget_bookmarks"

    def convert_state_to_utc(self, date_str):
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def test_run(self):
        self.run_test(self.streams_to_select() - {'data_extension.test 1'}, '2019-01-01T00:00:00Z')
        self.run_test({'data_extension.test 1'}, '2021-08-01T00:00:00Z')

    def run_test(self, streams, start_date):
        self.START_DATE = start_date

        conn_id = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id)

        expected_streams = streams

        found_catalogs_1 = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs_1, only_streams=expected_streams)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State
        ##########################################################################

        new_state = {'bookmarks': dict()}
        replication_keys = self.expected_replication_keys()
        for stream in expected_streams:
            if self.is_incremental(stream):
                new_state['bookmarks'][stream] = dict()
                new_state['bookmarks'][stream]['field'] = next(iter(replication_keys[stream]))
                new_state['bookmarks'][stream]['last_record'] = '2021-08-23T00:00:00Z' if stream == 'data_extension.test 1' else '2019-01-14T00:00:00Z'

        # Set state for next sync
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in expected_streams:
            # skip "subscriber" stream as replication key is not retrievable
            if stream == "subscriber":
                continue

            with self.subTest(stream=stream):
                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)

                if self.is_incremental(stream):

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(self.expected_replication_keys()[stream]))
                    first_bookmark_value = first_bookmark_key_value.get('last_record')
                    second_bookmark_value = second_bookmark_key_value.get('last_record')
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_key_value.get('last_record'))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_key_value.get('last_record'))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test


                    for record in second_sync_messages:

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = parse(replication_key_value).strftime("%Y-%m-%dT%H:%M:%SZ")
                        self.assertLessEqual(replication_key_value_parsed, second_bookmark_value_utc,
                                             msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = record.get(replication_key)
                        replication_key_value_parsed = parse(replication_key_value).strftime("%Y-%m-%dT%H:%M:%SZ")
                        self.assertLessEqual(replication_key_value_parsed, first_bookmark_value_utc,
                                             msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.")

                    # Verify the number of records in the 2nd sync is less then or equal to the first
                    self.assertLessEqual(second_sync_count, first_sync_count)

                else:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
