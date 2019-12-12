import logging

from dpss import (
    run,
    matrix_provider,
)
from dpss.config import config
from dpss.memthread import MemoryMonitorThread
from dpss.utils import (
    TemporaryDirectoryChange,
    setup_log,
)

log = logging.getLogger(__name__)
setup_log(__name__,
          logging.INFO,
          logging.StreamHandler(),
          logging.FileHandler(config.main_log_file))

# These libraries make a lot of debug-level log messages which make the log file hard to read
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    log.info('Generating per-project summary statistics of matrix data.')
    log.info(f'{config.matrix_source.capitalize()} matrices will be obtained'
             f' from the {config.source_stage} deployment stage.')
    log.info(f'Results will be uploaded to the {config.target_stage} project assets folder.')

    memory_interval = 30
    memory_monitor = MemoryMonitorThread(interval=memory_interval)
    log.info(f'Logging memory usage to {config.memory_log_file}'
             f' every {memory_interval} seconds.')
    memory_monitor.start()

    provider = matrix_provider.get_provider()

    iter_matrices = iter(provider)
    finished = False
    while not finished:
        with TemporaryDirectoryChange():
            try:
                finished = run(iter_matrices)
            except Exception:
                log.error(f'Matrix failed; continuing', exc_info=True)


if __name__ == "__main__":
    main()
