import unittest

from tap_exacttarget.pagination import increment_date


class TestPagination(unittest.TestCase):

    def test_increment_date(self):
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z"),
            "2015-09-29T10:05:53Z")
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z", {'hours': 1}),
            "2015-09-28T11:05:53Z")
