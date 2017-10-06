import unittest

from tap_exacttarget.util import partition_all


class TestPartitionAll(unittest.TestCase):

    def test__partition_all(self):
        self.assertEqual(
            list(partition_all([1, 2, 3, 4, 5, 6, 7], 3)),
            [[1, 2, 3], [4, 5, 6], [7]])
