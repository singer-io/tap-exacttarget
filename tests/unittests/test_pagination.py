import unittest
import tap_exacttarget
from tap_exacttarget.pagination import increment_date


class TestPagination(unittest.TestCase):

    def test_increment_date(self):
        # verify that if there is no 'unit' mentioned, then the date should be incremented by '1 day'
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z"),
            "2015-09-29T10:05:53Z")
        # verify that the 'increment_date' correctly increments the date by sepcified 'unit'(here: 1 hour)
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z", {'hours': 1}),
            "2015-09-28T11:05:53Z")
