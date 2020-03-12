import datetime
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os
import unittest
import pdb
import json
import requests


class ExactTargetDiscover(unittest.TestCase):

    def name(self):
        return "tap_tester_exacttarget_discover_v1"

    def tap_name(self):
        return "tap-exacttarget"

    def setUp(self):
        required_env = {
            "TAP_EXACTTARGET_CLIENT_ID",
            "TAP_EXACTTARGET_CLIENT_SECRET",
            "TAP_EXACTTARGET_TENANT_SUBDOMAIN",
            "TAP_EXACTTARGET_V2_CLIENT_ID",
            "TAP_EXACTTARGET_V2_CLIENT_SECRET",
            "TAP_EXACTTARGET_V2_TENANT_SUBDOMAIN",
        }
        missing_envs = [v for v in required_env if not os.getenv(v)]
        if missing_envs:
            raise Exception("set " + ", ".join(missing_envs))

    def get_type(self):
        return "platform.exacttarget"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self):
        yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        return {
            'start_date': yesterday.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'client_id': os.getenv('TAP_EXACTTARGET_CLIENT_ID')
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        found_catalog = menagerie.get_catalog(conn_id)
        for catalog_entry in found_catalog['streams']:
            field_names_in_schema = set([ k for k in catalog_entry['schema']['properties'].keys()])
            field_names_in_breadcrumbs = set([x['breadcrumb'][1] for x in catalog_entry['metadata'] if len(x['breadcrumb']) == 2])
            self.assertEqual(field_names_in_schema, field_names_in_breadcrumbs)

            inclusions_set = set([(x['breadcrumb'][1], x['metadata']['inclusion'])
                                  for x in catalog_entry['metadata']
                                  if len(x['breadcrumb']) == 2])
            # Validate that all fields are in metadata
            self.assertEqual(len(inclusions_set), len(field_names_in_schema))
            self.assertEqual(set([i[0] for i in inclusions_set]), field_names_in_schema)
            # Validate that all metadata['inclusion'] are 'available'
            unique_inclusions = set([i[1] for i in inclusions_set])
            self.assertTrue(len(unique_inclusions) == 1 and 'available' in unique_inclusions)

class ExactTargetDiscover2(ExactTargetDiscover):
    def name(self):
        return "tap_tester_exacttarget_discover_v1_with_subdomain"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_CLIENT_SECRET')
        }

    def get_properties(self):
        yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        return {
            'start_date': yesterday.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'client_id': os.getenv('TAP_EXACTTARGET_CLIENT_ID'),
            'tenant_subdomain': os.getenv('TAP_EXACTTARGET_TENANT_SUBDOMAIN')
        }


class ExactTargetDiscover3(ExactTargetDiscover):
    def name(self):
        return "tap_tester_exacttarget_discover_v2_with_subdomain"

    def get_credentials(self):
        return {
            'client_secret': os.getenv('TAP_EXACTTARGET_V2_CLIENT_SECRET')
        }

    def get_properties(self):
        yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        return {
            'start_date': yesterday.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'client_id': os.getenv('TAP_EXACTTARGET_V2_CLIENT_ID'),
            'tenant_subdomain': os.getenv('TAP_EXACTTARGET_V2_TENANT_SUBDOMAIN')
        }
