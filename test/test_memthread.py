from collections import namedtuple
import itertools
import unittest
from unittest import mock

from tempdir_test_case import TempdirTestCase


class TestMemoryMonitorThread(TempdirTestCase):

    # matching part of psutil schema used by MemoryMonitorThread._used_memory
    MockVMem = namedtuple('svmem', ['total', 'available'])
    MockSwapMem = namedtuple('sswap', ['used'])

    @mock.patch('psutil.virtual_memory', return_value=MockVMem(1000, 800))
    @mock.patch('psutil.swap_memory', return_value=MockSwapMem(40))
    def test(self, mock_vmem, mock_smem):
        # postpone import to redirect log file to temp dir
        from dpss.memthread import MemoryMonitorThread

        mmt = MemoryMonitorThread(0.1)
        memory = list(itertools.islice(mmt._log_memory(0), 10))
        self.assertEqual(memory, [240]*10)


if __name__ == '__main__':
    unittest.main()