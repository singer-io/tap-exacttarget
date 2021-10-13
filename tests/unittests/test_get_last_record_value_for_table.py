import unittest
import tap_exacttarget.state as _state


class TestGetLastRecordValueForTable(unittest.TestCase):

    def test_stream_has_bookmark(self):
        state = {
            "bookmarks": {
                "content_area": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
                "sent": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "click": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "open": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "bounce": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "unsub": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "folder": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
                "list": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
                "list_subscriber": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
                "send": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" }
            }
        }
        table = 'folder'
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        start_value = _state.get_last_record_value_for_table(state, table, config)
        # verify that start value is 'bookmark_value - 1 day'
        # as 'get_last_record_value_for_table' subtracts 1 day from bookmark value
        self.assertEquals(start_value, "2021-07-31T00:00:00Z")

    def test_stream_has_no_bookmark(self):
        state = {
            "bookmarks": {
                "sent": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "open": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "bounce": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "unsub": { "field": "EventDate", "last_record": "2021-08-01T00:00:00Z" },
                "list": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
                "list_subscriber": { "field": "ModifiedDate", "last_record": "2021-08-01T00:00:00Z" },
            }
        }
        table = 'folder'
        config = {
            "client_id": "client_id_123",
            "client_secret": "client_secret_123",
            "tenant_subdomain": "tenant_subdomain_123",
            "start_date": "2021-01-01T00:00:00Z",
            "request_timeout": "900",
            "batch_size": 2500
        }

        start_value = _state.get_last_record_value_for_table(state, table, config)
        # verify that start value is 'start_date' as 'folder' does not have bookmark in the state
        self.assertEquals(start_value, config.get("start_date"))
