import logging
from typing import (
    Sequence,
    List,
)

from dpss.matrix_info import MatrixInfo
from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service

log = logging.getLogger(__name__)


def prepare_matrices(project_mtx_info: MatrixInfo) -> List[MatrixInfo]:
    log.info(f'Processing {project_mtx_info.project_uuid}')

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

    s3service.upload_figures(project_mtx_info)
