import logging
import threading
import time

import psutil

from dpss.config import config
from dpss.utils import (
    setup_log,
    convert_size,
)

log = logging.getLogger(__name__)
setup_log(__name__,
          logging.DEBUG,
          logging.FileHandler(config.memory_log_file, mode='w'))


class MemoryMonitorThread(threading.Thread):

    def __init__(self, interval: float, relative_to_start: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.interval = interval
        self.relative = relative_to_start
        relativity_mesage = ' (relative to current usage)' if relative_to_start else ''
        log.info(f'Logging memory usage to {config.memory_log_file}'
                 f' every {self.interval} seconds{relativity_mesage}.')

    def run(self):
        origin = self.used_memory() if self.relative else 0
        while True:
            used_memory = self.used_memory() - origin
            log.debug(f'{used_memory}\t{convert_size(used_memory)}')
            time.sleep(self.interval)

    @classmethod
    def used_memory(cls):
        vmem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        vused = vmem.total - vmem.available
        return vused + swap.used
