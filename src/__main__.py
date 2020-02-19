import logging

from dpss import (
    run,
    matrix_provider,
    setup_log,
)
from dpss.config import config
from dpss.memthread import MemoryMonitorThread
from dpss.utils import (
    TemporaryDirectoryChange,
)

log = setup_log(__name__)

# These libraries make a lot of debug-level log messages which make the log file hard to read
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    log.info('Generating per-project summary statistics of matrix data.')
    log.info(f'{config.matrix_source.capitalize()} matrices will be obtained'
             f' from the {config.source_stage} deployment stage.')
    log.info(f'Results will be uploaded to the {config.target_stage} project assets folder.')

    memory_monitor = MemoryMonitorThread(interval=config.memory_interval)
    memory_monitor.start()

    provider_type = matrix_provider.get_provider_type()
    provider = provider_type()

    iter_matrices = iter(provider)
    finished = False
    while not finished:
        with TemporaryDirectoryChange():
            try:
                finished = run(iter_matrices)
            except Exception:
                log.error(f'Matrix failed; continuing', exc_info=True)
                continue
    log.info('Finished.')


if __name__ == "__main__":
    main()
