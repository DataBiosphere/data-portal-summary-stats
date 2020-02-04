import io
import logging
import time
import unittest

import pandas as pd
import numpy as np

from dpss.memthread import MemoryMonitorThread
from tempdir_test_case import TempdirTestCase


@unittest.skip
class TestMemThread(TempdirTestCase):
    abs_mem_log = logging.getLogger(f'{__name__}.abs')
    rel_mem_log = logging.getLogger(f'{__name__}.rel')

    def setUp(self):
        super().setUp()
        self.abs_stream = io.StringIO()
        self.abs_mem_log.addHandler(logging.StreamHandler(self.abs_stream))
        self.rel_stream = io.StringIO()
        self.rel_mem_log.addHandler(logging.StreamHandler(self.rel_stream))

    def test_memthread(self):
        interval = 0.1
        abs_mmt = MemoryMonitorThread(interval, relative_to_start=False, target_log=self.abs_mem_log)
        rel_mmt = MemoryMonitorThread(interval, relative_to_start=True, target_log=self.rel_mem_log)
        start_memory = MemoryMonitorThread.used_memory()
        abs_mmt.start()
        rel_mmt.start()
        # noinspection PyUnusedLocal
        data = np.ones(10 ** 6)
        time.sleep(1)
        finish_memory = MemoryMonitorThread.used_memory()

        self.assertGreater(finish_memory, start_memory)

        print(self.abs_stream.read())
        print(self.rel_stream.read())

        abs_log_data = pd.read_csv(self.abs_stream, sep=r'\s+', header=None)
        rel_log_data = pd.read_csv(self.rel_stream, sep=r'\s+', header=None)

        print(abs_log_data)
        print(rel_log_data)

        return data
