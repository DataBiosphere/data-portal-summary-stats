#!/usr/env/python3

import logging
import sys
import time
import os

from dpss import matrix_provider
from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service
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

    try:
        provider = matrix_provider.get_provider()
    except RuntimeError:
        log.error(f'Unrecognized matrix source: {config.matrix_source}')
        sys.exit(1)

    iter_matrices = iter(provider)
    while True:
        with TemporaryDirectoryChange() as tempdir:
            try:
                download_mtx_info = next(iter_matrices)
            except StopIteration:
                break
            except SkipMatrix as s:
                log.info(f'Skipping targeted matrix: {s.__cause__}')
                continue

            log.info(f'Writing to temporary directory {tempdir}')
            log.info(f'Processing matrix for project {download_mtx_info.project_uuid} ({download_mtx_info.source})')

            try:
                extract_mtx_infos = MatrixPreparer(download_mtx_info).unzip()
                sep_mtx_infos = []
                for mi in extract_mtx_infos:
                    mp = MatrixPreparer(mi)
                    mp.preprocess()
                    sep_mtx_infos.extend(mp.separate())
            except Exception as e:
                log.error(f'Matrix preparation failed: {repr(e)}', exc_info=True)
                continue

            log.info(f'Generating stats for {download_mtx_info.extract_path}')

            try:
                mss = MatrixSummaryStats(sep_mtx_infos)
                mss.load_data()
                mss.create_images()

                # FIXME https://github.com/DailyDreaming/load-project/issues/21
                if not download_mtx_info.lib_con_approaches:
                    download_mtx_info.lib_con_approaches = frozenset(['SS2'])

                s3service.upload_figures(download_mtx_info)
            except Exception as e:
                log.error(f'Matrix stats generation failed: {repr(e)}', exc_info=True)
                continue

            # This logic was in Krause's code, no idea why
            if download_mtx_info.source == 'fresh':
                time.sleep(15)
    log.info('Finished.')


if __name__ == "__main__":
    main()
