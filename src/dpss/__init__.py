from collections import defaultdict
from pathlib import Path
from typing import (
    Sequence,
    List,
    Dict,
)

from dpss.exceptions import SkipMatrix
from dpss.logging import setup_log
from dpss.matrix_info import MatrixInfo
from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service
from dpss.utils import (
    TemporaryDirectoryChange,
    common_attr,
)

log = setup_log(__name__)


def prepare_matrices(project_mtx_info: MatrixInfo) -> Dict[str, List[MatrixInfo]]:
    log.info(f'Transforming {project_mtx_info.project_uuid} files')

    sep_mtx_infos = defaultdict(list)

    extracted_mtx_infos = MatrixPreparer(project_mtx_info).unzip()
    for mi in extracted_mtx_infos:
        mp = MatrixPreparer(mi)
        mp.preprocess()
        for sep_mtx_info in mp.separate():
            sep_mtx_infos[sep_mtx_info.lib_con_approaches].append(sep_mtx_info)

    return sep_mtx_infos


def generate_matrix_stats(processed_mtx_infos: Sequence[MatrixInfo]) -> None:
    uuid = common_attr(processed_mtx_infos, 'project_uuid')
    log.info(f'Generating stats for {uuid} ({len(processed_mtx_infos)} matrices)')

    mss = MatrixSummaryStats(processed_mtx_infos)
    mss.load_data()
    mss.create_images()


def upload_figures(mtx_infos: Sequence[MatrixInfo]) -> None:
    uuid = common_attr(mtx_infos, 'project_uuid')
    log.info(f'Uploading figures for {uuid}')
    folder = common_attr(mtx_infos, 'figures_folder')
    for figure in MatrixSummaryStats.target_images():
        try:
            s3service.upload_figure(folder, figure)
        except FileNotFoundError:
            continue


def run(iter_matrices) -> bool:
    try:
        project_mtx_info = next(iter_matrices)
    except StopIteration:
        log.info('Finsished.')
        return True
    else:
        log.info(f'Writing to temporary directory {str(Path.cwd())}')
        log.info(f'Processing matrix for project {project_mtx_info.project_uuid} ({project_mtx_info.source})')

        processed_mtx_infos = prepare_matrices(project_mtx_info)
        for lca, mtxs in processed_mtx_infos.items():
            generate_matrix_stats(mtxs)
            upload_figures(mtxs)

    return False
