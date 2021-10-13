import unittest
import tap_exacttarget
from tap_exacttarget.state import incorporate


class TestState(unittest.TestCase):

    def test_incorporate(self):
        # verify that the state file is updated if there is no previous bookmark present
        self.assertEqual(
            incorporate({}, 'table', 'modifieddate', '2017-11-01'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2017-11-01T00:00:00Z',
                        'field': 'modifieddate'
                    }
                }
            })

        # verify that the bookmark value is updated as the previous
        # bookmark value is smaller than the current record's value
        self.assertEqual(
            incorporate({
                'bookmarks': {
                    'table': {
                        'last_record': '2017-01-01T00:00:00Z',
                        'field': 'modifieddate'
                    }
                }
            }, 'table', 'modifieddate', '2017-11-01'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2017-11-01T00:00:00Z',
                        'field': 'modifieddate'
                    }
                }
            })

        # verify that the bookmark value is not updated as the previous
        # bookmark value is greater than the current record's value
        self.assertEqual(
            incorporate({
                'bookmarks': {
                    'table': {
                        'last_record': '2017-11-01T00:00:00Z',
                        'field': 'modifieddate'
                    }
                }
            }, 'table', 'modifieddate', '2017-01-01'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2017-11-01T00:00:00Z',
                        'field': 'modifieddate'
                    }
                }
            })
