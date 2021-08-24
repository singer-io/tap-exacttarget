from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetAllFields(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_all_fields"

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        expected_streams = self.streams_to_select()

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_found_catalogs(conn_id, found_catalogs, only_streams=expected_streams)

        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('stream_name') in expected_streams]

        # grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # # expected values
                # expected_automatic_keys = self.expected_automatic_fields().get(stream)
                # # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)
                actual_all_keys = [set(message['data'].keys()) for message in messages['messages']
                                       if message['action'] == 'upsert'][0]

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # remove some fields as data cannot be generated / retrieved
                if stream == 'list':
                    expected_all_keys.remove('SendClassification') # not retrievable
                    expected_all_keys.remove('PartnerProperties') # not retrievable
                elif stream == 'subscriber':
                    expected_all_keys.remove('CustomerKey') # not retrievable
                    expected_all_keys.remove('PartnerType') # not retrievable
                    expected_all_keys.remove('UnsubscribedDate') 
                    expected_all_keys.remove('PrimarySMSAddress') # not retrievable
                    expected_all_keys.remove('PrimaryEmailAddress') # not retrievable
                    expected_all_keys.remove('PartnerProperties') # not retrievable
                    expected_all_keys.remove('SubscriberTypeDefinition') # not retrievable
                    expected_all_keys.remove('Addresses') # not retrievable
                    expected_all_keys.remove('ListIDs')
                    expected_all_keys.remove('Locale') # not retrievable
                    expected_all_keys.remove('PrimarySMSPublicationStatus') # not retrievable
                    expected_all_keys.remove('ModifiedDate') # not retrievable
                elif stream == 'list_send':
                    expected_all_keys.remove('CreatedDate') # not retrievable
                    expected_all_keys.remove('CustomerKey') # not retrievable
                    expected_all_keys.remove('ID')
                    expected_all_keys.remove('PartnerProperties') # not retrievable
                    expected_all_keys.remove('ModifiedDate') # not retrievable
                elif stream == 'folder':
                    expected_all_keys.remove('Type')
                    expected_all_keys.remove('PartnerProperties')
                elif stream == 'email':
                    expected_all_keys.remove('__AdditionalEmailAttribute1') # not retrievable
                    expected_all_keys.remove('__AdditionalEmailAttribute3') # not retrievable
                    expected_all_keys.remove('SyncTextWithHTML') # not retrievable
                    expected_all_keys.remove('PartnerProperties') # not retrievable
                    expected_all_keys.remove('__AdditionalEmailAttribute5') # not retrievable
                    expected_all_keys.remove('ClonedFromID')
                    expected_all_keys.remove('__AdditionalEmailAttribute4') # not retrievable
                    expected_all_keys.remove('__AdditionalEmailAttribute2') # not retrievable
                elif stream == 'content_area':
                    # most of them are included in the 'Content' data
                    expected_all_keys.remove('BackgroundColor') # not retrievable
                    expected_all_keys.remove('Cellpadding') # not retrievable
                    expected_all_keys.remove('HasFontSize') # not retrievable
                    expected_all_keys.remove('BorderColor') # not retrievable
                    expected_all_keys.remove('BorderWidth') # not retrievable
                    expected_all_keys.remove('Width') # not retrievable
                    expected_all_keys.remove('IsLocked') # not retrievable
                    expected_all_keys.remove('Cellspacing') # not retrievable
                    expected_all_keys.remove('FontFamily') # not retrievable

                self.assertSetEqual(expected_all_keys, actual_all_keys)
