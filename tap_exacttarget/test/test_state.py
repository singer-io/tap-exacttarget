import unittest

from tap_exacttarget.state import incorporate


class TestState(unittest.TestCase):

    def test_incorporate(self):
        self.assertEqual(
            incorporate({}, 'table', 'modifieddate', '2017'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2017',
                        'field': 'modifieddate'
                    }
                }
            })

        self.assertEqual(
            incorporate({
                'bookmarks': {
                    'table': {
                        'last_record': '2017',
                        'field': 'modifieddate'
                    }
                }
            }, 'table', 'modifieddate', '2018'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2018',
                        'field': 'modifieddate'
                    }
                }
            })

        self.assertEqual(
            incorporate({
                'bookmarks': {
                    'table': {
                        'last_record': '2018',
                        'field': 'modifieddate'
                    }
                }
            }, 'table', 'modifieddate', '2017'),
            {
                'bookmarks': {
                    'table': {
                        'last_record': '2018',
                        'field': 'modifieddate'
                    }
                }
            })
