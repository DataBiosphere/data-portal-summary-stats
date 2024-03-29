import os
from pathlib import Path
from typing import (
    Set,
    Optional,
)
import datetime


class Config:
    _project_url = 'https://service.{}explore.data.humancellatlas.org/repository/projects/'
    _matrix_url = 'https://matrix.{}data.humancellatlas.org/v1/'
    _assets_bucket = '{}project-assets.data.humancellatlas.org'

    def __init__(self):
        self._init_time = datetime.datetime.now()

    @property
    def log_level(self) -> int:
        env = os.environ.get('DPSS_LOG_LEVEL', 'INFO')
        try:
            import logging as lib_logging
            level = getattr(lib_logging, env)
        except AttributeError:
            import dpss.logging as dpss_logging
            level = getattr(dpss_logging, env)
        assert isinstance(level, int)
        return level

    @property
    def source_stage(self) -> str:
        return os.environ['DPSS_MTX_SOURCE_STAGE']

    @property
    def target_stage(self) -> str:
        return os.environ['DPSS_MTX_TARGET_STAGE']

    @property
    def use_blacklist(self) -> bool:
        return os.environ.get('DPSS_BLACKLIST') == '1'

    @property
    def matrix_source(self) -> str:
        return os.environ['DPSS_MATRIX_SOURCE']

    @property
    def target_uuids(self) -> Optional[Set[str]]:
        var = os.environ.get('DPSS_TARGET_UUIDS', '')
        return set(var.split(',')) if var.strip() else None

    @property
    def ignore_mtime(self) -> bool:
        return os.environ.get('DPSS_FORCE') == '1'

    @classmethod
    def stage_str(cls, stage: str) -> str:
        return '' if stage == 'prod' else f'{stage}.'

    @property
    def azul_project_endpoint(self) -> str:
        return self._project_url.format(self.stage_str(self.source_stage))

    @property
    def hca_matrix_service_endpoint(self) -> str:
        return self._matrix_url.format(self.stage_str(self.source_stage))

    @property
    def s3_matrix_bucket_name(self) -> str:
        return self._assets_bucket.format(self.stage_str(self.source_stage))

    @property
    def s3_figure_bucket_name(self) -> str:
        return self._assets_bucket.format(self.stage_str(self.target_stage))

    @property
    def s3_canned_matrix_prefix(self) -> str:
        return 'project-assets/project-matrices/'

    @property
    def s3_figures_prefix(self) -> str:
        return 'project-assets/project-stats/'

    @property
    def local_projects_path(self) -> Path:
        return Path('projects')

    @property
    def memory_log_file(self) -> Path:
        return Path('dpss_memory_log.txt')

    @property
    def memory_interval(self) -> float:
        return 1.0

    @classmethod
    def time_fmt(cls, t: datetime.datetime) -> str:
        return t.strftime('%m-%d-%Y_%H:%M:%S;%f')


config = Config()
