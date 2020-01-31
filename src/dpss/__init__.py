from pathlib import Path
import logging
from typing import (
    Sequence,
    List,
)

from dpss.exceptions import SkipMatrix
from dpss.matrix_info import MatrixInfo
from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service
from dpss.utils import TemporaryDirectoryChange

log = logging.getLogger(__name__)


def prepare_matrices(project_mtx_info: MatrixInfo) -> List[MatrixInfo]:
    log.info(f'Transforming {project_mtx_info.project_uuid} files')

    extracted_mtx_infos = MatrixPreparer(project_mtx_info).unzip()
    sep_mtx_infos = []
    for mi in extracted_mtx_infos:
        mp = MatrixPreparer(mi)
        mp.preprocess()
        sep_mtx_infos.extend(mp.separate())
    return sep_mtx_infos


def generate_matrix_stats(project_mtx_info: MatrixInfo, processed_mtx_infos: Sequence[MatrixInfo]) -> None:
    log.info(f'Generating stats for {project_mtx_info.project_uuid} ({len(processed_mtx_infos)} matrices)')

    mss = MatrixSummaryStats(processed_mtx_infos)
    mss.load_data()
    mss.create_images()


def upload_figures(project_matrix_info: MatrixInfo):
    log.info(f'Uploading figures for {project_matrix_info.project_uuid}')
    for figure in MatrixSummaryStats.target_images():
        s3service.upload_figure(project_matrix_info, figure)


def run(iter_matrices):
    try:
        project_mtx_info = next(iter_matrices)
    except StopIteration:
        log.info('Finsished.')
        return True
    except SkipMatrix as s:
        log.info(f'Skipping targeted matrix: {s.__cause__}')
        return False

    log.info(f'Writing to temporary directory {str(Path.cwd())}')
    log.info(f'Processing matrix for project {project_mtx_info.project_uuid} ({project_mtx_info.source})')

    processed_mtx_infos = prepare_matrices(project_mtx_info)
    generate_matrix_stats(project_mtx_info, processed_mtx_infos)
    upload_figures(project_mtx_info)

    return False
