from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import json
import datetime

class FullReplicationTest(ExactTargetBase):
    """Test tap gets all records for streams with full replication"""

    def name(self):
        return "tap_tester_exacttarget_full_replication"

    def test_run(self):
        conn_id_1 = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id_1)

        # Select streams
        found_catalogs = menagerie.get_catalogs(conn_id_1)
        full_streams = {key for key, value in self.expected_replication_method().items()
                        if value == "FULL_TABLE"}
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('stream_name') in full_streams]
        self.select_found_catalogs(conn_id_1, our_catalogs, full_streams)

        # Run a sync job
        first_sync_record_count = self.run_and_verify_sync(conn_id_1)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), full_streams)

        first_sync_state = menagerie.get_state(conn_id_1)

        # Get the set of records from a first sync
        first_sync_records = runner.get_records_from_target_output()

        # set future start date, which validates that stream is syncing 'FULL_TABLE'
        self.START_DATE = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(days=1), "%Y-%m-%dT00:00:00Z")

        conn_id_2 = connections.ensure_connection(self, original_properties=False)
        runner.run_check_mode(self, conn_id_2)

        found_catalogs = menagerie.get_catalogs(conn_id_2)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('stream_name') in full_streams]
        self.select_found_catalogs(conn_id_2, our_catalogs, full_streams)

        # Run a second sync job
        second_sync_record_count = self.run_and_verify_sync(conn_id_2)

        # Get the set of records from a second sync
        second_sync_records = runner.get_records_from_target_output()

        for stream in full_streams:
            with self.subTest(stream=stream):

                # verify there is no bookmark values from state
                state_value = first_sync_state.get("bookmarks", {}).get(stream)
                self.assertIsNone(state_value)

                # verify that there is more than 1 record of data - setup necessary
                self.assertGreater(first_sync_record_count.get(stream, 0), 1,
                                   msg="Data is not set up to be able to test full sync")

                # verify that you get the same or more data the 2nd time around
                self.assertGreaterEqual(
                    second_sync_record_count.get(stream, 0),
                    first_sync_record_count.get(stream, 0),
                    msg="second syc did not have more records, full sync not verified")

                # [set(message['data']) for message in messages['messages']
                #                    if message['action'] == 'upsert'][0]
                # verify all data from 1st sync included in 2nd sync
                first_data = [record["data"] for record
                              in first_sync_records.get(stream, {}).get("messages", {"data": {}})]
                second_data = [record["data"] for record
                               in second_sync_records.get(stream, {}).get("messages", {"data": {}})]

                same_records = 0
                for first_record in first_data:
                    first_value = json.dumps(first_record, sort_keys=True)

                    for compare_record in second_data:
                        compare_value = json.dumps(compare_record, sort_keys=True)

                        if first_value == compare_value:
                            second_data.remove(compare_record)
                            same_records += 1
                            break

                self.assertEqual(len(first_data), same_records,
                                 msg="Not all data from the first sync was in the second sync")
