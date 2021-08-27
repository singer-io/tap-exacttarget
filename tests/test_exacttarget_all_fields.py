from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetAllFields(ExactTargetBase):

    # Note: some fields are not retrievable as discussed below
    #   https://salesforce.stackexchange.com/questions/354332/not-getting-modifieddate-for-listsend-endpoint
    #   so we have to remove them
    fields_to_remove = {
        'list': [
            'SendClassification', # not retrievable
            'PartnerProperties'], # not retrievable
        'subscriber': [
            'CustomerKey', # not retrievable
            'PartnerType', # not retrievable
            'UnsubscribedDate',
            'PrimarySMSAddress', # not retrievable
            'PrimaryEmailAddress', # not retrievable
            'PartnerProperties', # not retrievable
            'SubscriberTypeDefinition', # not retrievable
            'Addresses', # not retrievable
            'ListIDs',
            'Locale', # not retrievable
            'PrimarySMSPublicationStatus', # not retrievable
            'ModifiedDate'], # not retrievable
        'list_send': [
            'CreatedDate', # not retrievable
            'CustomerKey', # not retrievable
            'ID',
            'PartnerProperties', # not retrievable
            'ModifiedDate'], # not retrievable
        'folder': [
            'Type',
            'PartnerProperties'],
        'email': [
            '__AdditionalEmailAttribute1', # not retrievable
            '__AdditionalEmailAttribute3', # not retrievable
            'SyncTextWithHTML', # not retrievable
            'PartnerProperties', # not retrievable
            '__AdditionalEmailAttribute5', # not retrievable
            'ClonedFromID',
            '__AdditionalEmailAttribute4', # not retrievable
            '__AdditionalEmailAttribute2'], # not retrievable
        'content_area': [
            # most of them are included in the 'Content' data
            'BackgroundColor', # not retrievable
            'Cellpadding', # not retrievable
            'HasFontSize', # not retrievable
            'BorderColor', # not retrievable
            'BorderWidth', # not retrievable
            'Width', # not retrievable
            'IsLocked', # not retrievable
            'Cellspacing', # not retrievable
            'FontFamily'] # not retrievable
    }

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

                # get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                # collect actual values
                messages = synced_records.get(stream)
                actual_all_keys = [set(message['data'].keys()) for message in messages['messages']
                                       if message['action'] == 'upsert'][0]

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # remove some fields as data cannot be generated / retrieved
                fields = self.fields_to_remove.get(stream)
                for field in fields:
                    expected_all_keys.remove(field)

                self.assertSetEqual(expected_all_keys, actual_all_keys)
