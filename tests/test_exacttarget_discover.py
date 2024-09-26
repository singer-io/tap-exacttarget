from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
import re

class ExactTargetDiscover(ExactTargetBase):

    def name(self):
        return "tap_tester_exacttarget_discover_v1"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props.pop('tenant_subdomain')
        return props

    def test_run(self):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.
            • Verify number of actual streams discovered match expected
            • Verify the stream names discovered were what we expect
                streams should only have lowercase alphas and underscores
            • verify there is only 1 top level breadcrumb
            • verify primary key(s)
            • verify replication key(s)
            • verify that primary keys and replication keys are given the inclusion of automatic.
            • verify that all other fields have inclusion of available in metadata.
        """
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        streams_to_test = self.streams_to_select(is_discovery=True)
        found_catalogs = menagerie.get_catalogs(conn_id)

        # verify the stream names discovered were what we expect
        # streams should only have lowercase alphas and underscores

        # skipped 'data_extension' streams, because they are the custom
        # tables we create in marketing cloud UI and, the stream name
        # will be the table name we set in the UI, as seen in our
        # instance the table name is 'This is a test'
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs if 'data_extension.' not in c['tap_stream_id']}
        self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertIsNotNone(catalog)

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]

                # add primary keys and replication keys in automatically replicated keys to check
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_primary_keys = set(
                    stream_properties[0].get(
                        "metadata", {"table-key-properties": []}).get("table-key-properties", [])
                )
                actual_replication_keys = set(
                    stream_properties[0].get(
                        "metadata", {"valid-replication-keys": []}).get("valid-replication-keys", [])
                )
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )

                ##########################################################################
                ### metadata assertions
                ##########################################################################

                # verify there is only 1 top level breadcrumb in metadata
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # verify replication key(s) match expectations
                self.assertSetEqual(expected_replication_keys, actual_replication_keys)

                # verify that primary keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields}),
                    msg="Not all non key properties are set to available in metadata")

class ExactTargetDiscover2(ExactTargetDiscover):
    def name(self):
        return "tap_tester_exacttarget_discover_v1_with_subdomain"

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['tenant_subdomain'] = os.getenv('TAP_EXACTTARGET_TENANT_SUBDOMAIN')
        return props

class ExactTargetDiscover3(ExactTargetDiscover):
    def name(self):
        return "tap_tester_exacttarget_discover_v2_with_subdomain"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_V2_CLIENT_SECRET')
        }

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props['client_id'] = os.getenv('TAP_EXACTTARGET_V2_CLIENT_ID')
        props['tenant_subdomain'] = os.getenv('TAP_EXACTTARGET_V2_TENANT_SUBDOMAIN')
        return props
