import unittest

from tap_exacttarget.state import incorporate


class TestState(unittest.TestCase):

    def test_incorporate(self):
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
