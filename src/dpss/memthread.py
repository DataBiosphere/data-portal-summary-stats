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
setup_log(
    __name__,
    logging.DEBUG,
    logging.FileHandler(config.memory_log_file, mode='w')
)


class MemoryMonitorThread(threading.Thread):

    def __init__(self, interval: float, relative_to_start: bool = True, target_log=log, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.target_log = target_log
        self.interval = interval
        self.relative = relative_to_start
        relativity_message = ' (relative to current usage)' if relative_to_start else ''
        log.info(f'{self} logging memory usage to {config.memory_log_file}'
                 f' every {self.interval} seconds{relativity_message}.')

    def run(self):
        origin = self.used_memory() if self.relative else 0
        for memory_usage in self._log_memory(origin):
            self.target_log.debug(self._format_memory(memory_usage))

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
