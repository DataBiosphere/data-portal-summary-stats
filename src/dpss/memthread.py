import logging
import threading
import time

import psutil

from dpss.config import config
from dpss.utils import setup_log

log = logging.getLogger(__name__)
setup_log(__name__,
          logging.DEBUG,
          logging.FileHandler(config.memory_log_file, mode='w'))


class MemoryMonitorThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        kwargs = dict(kwargs)
        interval = kwargs.pop('interval')
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.interval = interval

    def run(self):
        while True:
            mem = psutil.virtual_memory()
            used = mem.total - mem.available
            log.debug(used)
            time.sleep(self.interval)
