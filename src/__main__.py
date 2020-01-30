import logging

from dpss import (
    matrix_provider,
    run,
    TemporaryDirectoryChange,
)
from dpss.config import config
log = logging.getLogger(__name__)

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
   )
# These libraries make a lot of debug-level log messages which make the log file hard to read
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    log.info('Generating per-project summary statistics of matrix data.')
    log.info(f'{config.matrix_source.capitalize()} matrices will be obtained'
             f' from the {config.source_stage} deployment stage.')
    log.info(f'Results will be uploaded to the {config.target_stage} project assets folder.')

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
