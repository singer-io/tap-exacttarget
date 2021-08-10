from base import ExactTargetBase
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
import os

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
        conn_id = connections.ensure_connection(self)
        runner.run_check_mode(self, conn_id)

        found_catalog = menagerie.get_catalogs(conn_id)
        for catalog_entry in found_catalog:
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
