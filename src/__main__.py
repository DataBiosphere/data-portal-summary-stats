#!/usr/env/python3

import logging
import sys
import time
import os

from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_provider import (
    FreshMatrixProvider,
    CannedMatrixProvider,
)
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import S3Service
from dpss.utils import TemporaryDirectoryChange

log = logging.getLogger(__name__)

# Set up logging
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
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

    s3 = S3Service()

    # Temporary work-around for matrices that can't be processed for various reasons.
    do_not_process = s3.get_blacklist() if config.use_blacklist else []

    if config.matrix_source == 'fresh':
        provider = FreshMatrixProvider(blacklist=do_not_process)
    elif config.matrix_source == 'canned':
        provider = CannedMatrixProvider(blacklist=do_not_process,
                                        s3_service=s3)
    else:
        log.error(f'Unrecognized matrix source: {config.matrix_source} (should be "canned" or "fresh)')
        sys.exit(1)

    iter_matrices = iter(provider)
    while True:
        with TemporaryDirectoryChange() as tempdir:
            try:
                mtx_info = next(iter_matrices)
            except SkipMatrix as s:
                log.info(f'Skipping matrix: {s.__cause__}')
                continue
            except StopIteration:
                break

            log.info(f'Writing to temporary directory {tempdir}')
            log.info(f'Processing matrix for project {mtx_info.project_uuid} ({mtx_info.source})')

            try:
                preparer = MatrixPreparer(mtx_info)
                preparer.unzip()
                preparer.preprocess()
                sep_mtx_infos = preparer.separate()
            except RuntimeError as e:
                log.error(f'Matrix preparation failed: {e}')
                continue

            for sep_mtx_info in sep_mtx_infos:
                log.info(f'Generating stats for {sep_mtx_info.extract_path}')
                try:
                    mss = MatrixSummaryStats(sep_mtx_info)
                    mss.create_images()
                    s3.upload_figures(mtx_info)
                except RuntimeError as e:
                    log.error(f'Matrix stats generation failed: {e}')
                    continue
                # This logic was in Krause's code, no idea why
                if mtx_info.source == 'fresh':
                    time.sleep(15)
    log.info('Finished.')


if __name__ == "__main__":
    main()
