from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetFieldSelection(ExactTargetBase):

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

    # fields not to select
    non_selected_fields = {
            "campaign": ["createdDate", "campaignCode", "description"],
            "content_area": ["Name"],
            "data_extension.test emails": ["name", "email"],
            "data_extension.This is a test": ["Birthday"],
            "data_extension.my_test": ["Address"],
            "data_extension.test 1": ["name"],
            "email": ["Name", "CharacterSet", "HasDynamicSubjectLine", "EmailType"],
            "event": ["EventType"],
            "folder": ["Name", "ContentType", "Description", "ObjectID"],
            "list": ["ListName", "Category", "Type"],
            "list_send": ["MissingAddresses", "ExistingUndeliverables", "HardBounces", "NumberDelivered"],
            "list_subscriber": ["Status", "ObjectID", "ListID"],
            "send": ["Status", "EmailName", "FromAddress", "IsMultipart"],
            "subscriber": ["Status", "EmailAddress", "SubscriberKey", "PartnerKey"]
        }

    def name(self):
        return "tap_tester_exacttarget_field_selection"

    def test_run(self):
        # run test with all fields of stream 'selected = False'
        self.run_test(only_automatic_fields=True)
        # run test with fields in 'non_selected_fields', 'selected = False'
        self.run_test(only_automatic_fields=False)

    def run_test(self, only_automatic_fields=False):
        expected_streams = self.streams_to_select()
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        expected_stream_fields = dict()

        found_catalogs = menagerie.get_catalogs(conn_id)
        for catalog in found_catalogs:
            stream_name = catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            if not stream_name in expected_streams:
                continue
            # select catalog fields
            self.select_found_catalogs(
                conn_id,
                [catalog],
                only_streams=[stream_name],
                deselect_all_fields=True if only_automatic_fields else False,
                non_selected_props=[] if only_automatic_fields else self.non_selected_fields[stream_name])
            # add expected fields for assertion
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            if only_automatic_fields:
                expected_stream_fields[stream_name] = self.expected_primary_keys()[stream_name] | self.expected_replication_keys()[stream_name]
            else:
                expected_stream_fields[stream_name] = set(fields_from_field_level_md) - set(self.non_selected_fields[stream_name])

        self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                expected_primary_keys = self.expected_primary_keys()[stream]

                # get expected keys
                expected_keys = expected_stream_fields[stream]

                # collect all actual values
                messages = synced_records.get(stream)

                # collect actual synced fields
                actual_keys = [set(message['data'].keys()) for message in messages['messages']
                                   if message['action'] == 'upsert'][0]

                fields = self.fields_to_remove.get(stream) or []
                expected_keys = expected_keys - set(fields)

                # verify expected and actual fields
                self.assertEqual(expected_keys, actual_keys,
                                 msg='Selected keys in catalog is not as expected')

                # Verify we did not duplicate any records across pages
                records_pks_set = {tuple([message.get('data').get(primary_key)
                                          for primary_key in expected_primary_keys])
                                   for message in messages.get('messages')}
                records_pks_list = [tuple([message.get('data').get(primary_key)
                                           for primary_key in expected_primary_keys])
                                    for message in messages.get('messages')]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
