from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner

class ExactTargetFieldSelection(ExactTargetBase):

    # fields not to select
    non_selected_fields = {
            "campaign": ["createdDate", "campaignCode", "description"],
            "content_area": ["Name"],
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
            stream_name = catalog['stream']
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
            if only_automatic_fields:
                expected_stream_fields[stream_name] = self.expected_primary_keys()[stream_name] | self.expected_replication_keys()[stream_name]
            else:
                expected_stream_fields[stream_name] = set(catalog.get('schema').get('properties').keys()) - set(self.non_selected_fields[stream_name])

        self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # get expected keys
                expected_keys = expected_stream_fields[stream]

                # collect all actual values
                messages = synced_records.get(stream)

                # collect actual synced fields
                actual_keys = [set(message['data'].keys()) for message in messages['messages']
                                   if message['action'] == 'upsert'][0]

                if stream == 'list':
                    expected_keys = expected_keys - {
                        'SendClassification',
                        'PartnerProperties'}
                elif stream == 'subscriber':
                    expected_keys = expected_keys - {
                        'CustomerKey',
                        'PartnerType',
                        'UnsubscribedDate',
                        'PrimarySMSAddress',
                        'PrimaryEmailAddress',
                        'PartnerProperties',
                        'SubscriberTypeDefinition',
                        'Addresses',
                        'ListIDs',
                        'Locale',
                        'PrimarySMSPublicationStatus',
                        'ModifiedDate'}
                elif stream == 'list_send':
                    expected_keys = expected_keys - {
                        'CreatedDate',
                        'CustomerKey',
                        'ID',
                        'PartnerProperties',
                        'ModifiedDate'}
                elif stream == 'folder':
                    expected_keys = expected_keys - {
                        'Type',
                        'PartnerProperties'}
                elif stream == 'email':
                    expected_keys = expected_keys - {
                        '__AdditionalEmailAttribute1',
                        '__AdditionalEmailAttribute3',
                        'SyncTextWithHTML',
                        'PartnerProperties',
                        '__AdditionalEmailAttribute5',
                        'ClonedFromID',
                        '__AdditionalEmailAttribute4',
                        '__AdditionalEmailAttribute2'}
                elif stream == 'content_area':
                    # most of them are included in the 'Content' data
                    expected_keys = expected_keys - {
                        'BackgroundColor',
                        'Cellpadding',
                        'HasFontSize',
                        'BorderColor',
                        'BorderWidth',
                        'Width',
                        'IsLocked',
                        'Cellspacing',
                        'FontFamily'}

                # verify expected and actual fields
                self.assertEqual(expected_keys, actual_keys,
                                 msg='Selected keys in catalog is not as expected')
