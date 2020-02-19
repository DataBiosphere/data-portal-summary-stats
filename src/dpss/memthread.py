import logging
import threading
import time

import psutil

from dpss.config import config
from dpss.logging import (
    setup_log,
    MEMORY_USAGE,
)
from dpss.utils import (
    convert_size,
)

main_log = setup_log(__name__)
mem_log = setup_log('DPSS_MEMORY_LOG', logging.FileHandler(config.memory_log_file, mode='w'))


class MemoryMonitorThread(threading.Thread):

    def __init__(self, interval: float, relative_to_start: bool = True, target_log=mem_log, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.target_log = target_log
        self.interval = interval
        self.relative = relative_to_start
        relativity_message = ' (relative to current usage)' if relative_to_start else ''
        main_log.info(f'{self} logging memory usage to {config.memory_log_file}'
                      f' every {self.interval} seconds{relativity_message}.')

    def run(self):
        origin = self.used_memory() if self.relative else 0
        for memory_usage in self._log_memory(origin):
            self.target_log.log(MEMORY_USAGE, self._format_memory(memory_usage))

    def _log_memory(self, origin):
        while True:
            yield self.used_memory() - origin
            time.sleep(self.interval)

    @classmethod
    def _format_memory(cls, size_bytes):
        return f'{size_bytes} {convert_size(size_bytes)}'

    @classmethod
    def used_memory(cls):
        vmem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        vused = vmem.total - vmem.available
        return vused + swap.used
