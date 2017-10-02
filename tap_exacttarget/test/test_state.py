import unittest

from tap_exacttarget.state import incorporate


class TestState(unittest.TestCase):

    def test__incorporate(self):
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

    def test__incorporate__new_greater_than_existing(self):
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

    def test__incorporate__existing_greater_than_new(self):
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
