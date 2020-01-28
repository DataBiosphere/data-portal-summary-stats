import logging

from dpss import matrix_provider
from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.utils import TemporaryDirectoryChange
from src import (
    prepare_matrices,
    generate_matrix_stats,
)

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
    while True:
        with TemporaryDirectoryChange() as tempdir:
            try:
                project_mtx_info = next(iter_matrices)
            except StopIteration:
                break
            except SkipMatrix as s:
                log.info(f'Skipping targeted matrix: {s.__cause__}')
                continue

            log.info(f'Writing to temporary directory {tempdir}')
            log.info(f'Processing matrix for project {project_mtx_info.project_uuid} ({project_mtx_info.source})')

            try:
                processed_mtx_infos = prepare_matrices(project_mtx_info)
                generate_matrix_stats(project_mtx_info, processed_mtx_infos)
            except Exception:
                log.error(f'Failed to process matrix.', exc_info=True)
                continue

    log.info('Finished.')


if __name__ == "__main__":
    main()
