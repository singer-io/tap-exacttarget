from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetPagination(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_pagination"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['batch_size'] = '1'
        return props

    def test_run(self):
        page_size = 1
        expected_streams = self.streams_to_select()

        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        sync_record_count = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect information for assertions from sync based on expected values
                record_count_sync = sync_record_count.get(stream, 0)
                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                       for message in synced_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                sync_messages = synced_records.get(stream, {'messages': []}).get('messages')

                # verify records are more than page size so multiple page is working
                self.assertGreater(record_count_sync, page_size)

                if record_count_sync > page_size:
                    primary_keys_list_1 = primary_keys_list[:page_size]
                    primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                    primary_keys_page_1 = set(primary_keys_list_1)
                    primary_keys_page_2 = set(primary_keys_list_2)

                    # Verify by private keys that data is unique for page
                    self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))

                # Verify we did not duplicate any records across pages
                records_pks_set = {tuple([message.get('data').get(primary_key)
                                          for primary_key in expected_primary_keys])
                                   for message in sync_messages}
                records_pks_list = [tuple([message.get('data').get(primary_key)
                                           for primary_key in expected_primary_keys])
                                    for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg=f"We have duplicate records for {stream}")
