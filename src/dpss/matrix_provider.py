from abc import (
    ABC,
    abstractmethod,
)
from datetime import datetime
import logging
import os
import shutil
import time
from typing import (
    List,
    Dict,
    Union,
    Optional,
    TypeVar,
)
import urllib.parse

import requests
import pandas as pd

from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.matrix_info import MatrixInfo
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service
from dpss.utils import (
    convert_size,
    file_id,
    remove_ext,
)
from more_itertools import (
    one,
)

log = logging.getLogger(__name__)


class MatrixProvider(ABC):

    def __init__(self):
        self.blacklist = frozenset(s3service.get_blacklist() if config.use_blacklist else [])

    @abstractmethod
    def get_entity_ids(self) -> List[str]:
        """
        Obtain identifiers for matrix sources.
        What the ids refer to can vary depending on matrix source (e.g. object
        keys for canned S3, project ids for HCA service).
        """
        raise NotImplementedError

    @abstractmethod
    def obtain_matrix(self, entity_id: str) -> MatrixInfo:
        """
        Acquire matrix file in cwd.
        :param entity_id: identifier of the matrix source.
        :return: info of the downloaded matrix.
        """
        raise NotImplementedError

    def filter_entity_id(self, entity_id: str) -> bool:
        if entity_id in self.blacklist:
            log.info(f'Matrix {entity_id} is blacklisted')
            return False
        else:
            return True

    def __iter__(self):
        """
        Download matrices and yield info objects.
        """
        entity_ids = self.get_entity_ids()
        log.info(f'Found {len(entity_ids)} target entities, {len(self.blacklist)} of which may be blacklisted.')
        for entity_id in entity_ids:
            if self.filter_entity_id(entity_id):
                yield self.obtain_matrix(entity_id)


class IdempotentMatrixProvider(MatrixProvider, ABC):

    def __init__(self):
        self.figure_mtimes, self.matrix_mtimes = (None, None) \
            if config.ignore_mtime else \
            (self.get_figure_modification_times(), self.get_matrix_modifications_times())
        super().__init__()

    @abstractmethod
    def get_matrix_modifications_times(self) -> Dict[str, datetime]:
        raise NotImplementedError

    def filter_entity_id(self, entity_id: str) -> bool:
        not_blacklisted = super().filter_entity_id(entity_id)
        if not_blacklisted is False or config.ignore_mtime:
            return not_blacklisted

        matrix_mtime = self.matrix_mtimes[entity_id]
        try:
            figure_mtime = self.figure_mtimes[entity_id]
        except KeyError:
            return True
        else:
            oudated = matrix_mtime > figure_mtime
            if not oudated:
                log.info(f'Matrix {entity_id} is up-to-date; '
                         f'matrix modified {matrix_mtime}, oldest figure uploaded {figure_mtime}')
            return oudated

    def get_figure_modification_times(self) -> Dict[str, datetime]:
        objects = s3service.list_bucket('figures', map_fields='LastModified')
        figures = {k: v for k, v in objects.items() if k.endswith('.' + MatrixSummaryStats.figure_format)}
        df = pd.DataFrame(figures.items(), columns=['Key', 'LastModified'])
        df['uuid'] = df['Key'].map(lambda k: k.split('/')[2])

        return {uuid: group['LastModified'].min() for uuid, group in df.groupby('uuid')}


class CannedMatrixProvider(IdempotentMatrixProvider):
    SOURCE_NAME = 'canned'

    mtx_ext = '.mtx.zip'

    def __init__(self):
        objects = s3service.list_bucket('matrices', map_fields='LastModified')
        matrix_keys, mtimes = zip(*[(k, v) for k, v in objects.items() if k.endswith(self.mtx_ext)])
        ordered_matrix_keys = s3service.sort_by_size('matrices', matrix_keys)
        self.matrix_mtimes = mtimes
        self.matrix_uuids = [file_id(key, self.mtx_ext) for key in ordered_matrix_keys]
        super().__init__()

    def get_entity_ids(self) -> List[str]:
        """List matrix objects in S3 bucket."""
        return self.matrix_uuids

    def obtain_matrix(self, matrix_id) -> MatrixInfo:
        """Download matrix from S3."""
        log.info(f'Downloading matrix {matrix_id} from S3.')
        filename = matrix_id + self.mtx_ext
        s3service.download('matrices', filename)
        assert filename in os.listdir('.')  # confirm successful download
        size = os.path.getsize(filename)  # in bytes
        log.info(f'Size of {filename}: {convert_size(size)}')
        return MatrixInfo(source=self.SOURCE_NAME,
                          project_uuid=matrix_id,
                          zip_path=filename,
                          extract_path=remove_ext(filename, '.zip'))

    def get_matrix_modifications_times(self) -> Dict[str, datetime]:
        return self.matrix_mtimes


class FreshMatrixProvider(MatrixProvider):
    SOURCE_NAME = 'fresh'

    project_id_field = 'project.provenance.document_id'
    min_gene_count_field = 'genes_detected'
    mtx_feature = 'gene'
    mtx_format = 'mtx'

    def __init__(self):
        super().__init__()
        self.projects = self._get_project_info_from_azul()

    @property
    def hca_matrix_service_project_list_url(self):
        return f'{config.hca_matrix_service_endpoint}filters/{self.project_id_field}'

    @property
    def hca_matrix_service_request_url(self):
        return f'{config.hca_matrix_service_endpoint}matrix/'

    def get_entity_ids(self):
        """
        Return list of matrix directory names (with prefix keys) from matrix service
        """
        response = requests.get(self.hca_matrix_service_project_list_url)
        self.check_response(response)
        return list(response.json()['cell_counts'].keys())

    def obtain_matrix(self, project_id: str) -> MatrixInfo:
        log.info(f'Requesting matrix from project {project_id} from HCA.')

        project_title = self.get_project_field(project_id, 'project_title')
        if project_title is None:
            log.info(f'No project title found for project ID {project_id} in Azul')
        else:
            log.info(f'Project title: {project_title}')

        lcas = self.get_project_field(project_id, 'project_lcas', [])
        try:
            lcas = frozenset(MatrixSummaryStats.translate_lca(lca) for lca in lcas)
        except ValueError:
            raise SkipMatrix

        status_response = self._request_matrix(project_id)
        assert status_response.status_code == 200
        s3_download_url = status_response.json()['matrix_url']
        log.info(f'Download URL for matrix is {s3_download_url}')

        matrix_response = requests.get(s3_download_url, stream=True)
        matrix_zipfile_name = os.path.basename(s3_download_url)

        with open(matrix_zipfile_name, 'wb') as matrix_zipfile:
            shutil.copyfileobj(matrix_response.raw, matrix_zipfile)

        return MatrixInfo(source=self.SOURCE_NAME,
                          project_uuid=project_id,
                          zip_path=matrix_zipfile_name,
                          extract_path=remove_ext(matrix_zipfile_name, '.zip'),
                          lib_con_approaches=lcas)

    def _request_matrix(self, project_id: str) -> requests.models.Response:

        payload = {
            'feature': self.mtx_feature,
            'format': self.mtx_format,
            'filter': {
                'op': 'and',
                'value': [
                    {
                        'op': '=',
                        'value': project_id,
                        'field': self.project_id_field
                    }
                ]
            }
        }

        log.info(f'Requesting expression matrix for project document ID {project_id}')
        log.info(f'Request payload and filter settings: {payload}')
        response = requests.post(self.hca_matrix_service_request_url, json=payload)
        request_id = response.json()["request_id"]
        self.check_response(response)
        minute_counter = 0
        while True:
            status_response = requests.get(self.hca_matrix_service_request_url + request_id)
            status = status_response.json()['status']
            if status == 'Complete':
                break
            elif status == 'In Progress':
                log.info(f'Matrix request status: {status}...')
                time.sleep(30)
                minute_counter += 0.5
            else:
                raise RuntimeError(f'Matrix service returned unexpected request status: {status}')
        log.info(f'Successfully requested matrix in {minute_counter} min.')

        return status_response

    def _get_project_info_from_azul(self) -> Dict[str, Dict[str, Union[str, List[str]]]]:
        """
        Get all projects from Azul by using the service APIs search_after query parameter.
        :return: dictionary of project id's to relevant project fields:
        title and library construction approach.
        """
        search_after = ''
        projects = {}
        while search_after is not None:
            response_json = requests.get(config.azul_project_endpoint + search_after).json()
            hits = response_json['hits']

            projects.update({
                hit['entryId']: {
                    'project_title': one(hit['projects'])['projectTitle'],
                    'project_lcas': one(hit['protocols'])['libraryConstructionApproach']
                }
                for hit in hits
            })

            pagination = response_json['pagination']
            search_after = self._format_search_after_params(
                pagination['search_after'],
                pagination['search_after_uid']
            )
        return projects

    @staticmethod
    def _format_search_after_params(project_title: Optional[str], document_id: Optional[str]) -> Optional[str]:
        """
        Return input string to be URL compliant, i.e., replacing special characters by their
        corresponding hexadecimal representation.
        """
        if document_id is None:
            return None
        else:
            project_title = urllib.parse.quote(project_title)
            document_id = urllib.parse.quote(document_id)
            return f'?search_after={project_title}&search_after_uid={document_id}'

    @staticmethod
    def check_response(response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            log.warning(f'{str(err)}')

    T = TypeVar('T')

    def get_project_field(self, project_id: str, field: str, default: T = None) -> Union[str, T]:
        try:
            project = self.projects[project_id]
        except KeyError:
            return default
        else:
            return project[field]


class LocalMatrixProvider(IdempotentMatrixProvider):
    SOURCE_NAME = 'local'

    def __init__(self):
        self.projects_dir = config.local_projects_path.resolve()
        try:
            from util import get_target_project_dirs
        except ImportError:
            log.info(f'Looking for local projects in {self.projects_dir}')
            project_dirs = [p for p in self.projects_dir.iterdir() if not p.is_symlink()]
        else:
            log.info(f'Using skunkworks accessions')
            project_dirs = get_target_project_dirs(root_dir=self.projects_dir, uuids=True)

        self.matrix_mtimes = {p.name: p.stat().st_mtime for p in project_dirs}
        self.matrix_uuids = list(self.matrix_mtimes.keys())

        super().__init__()

    def get_entity_ids(self) -> List[str]:
        return self.matrix_uuids

    def obtain_matrix(self, entity_id: str) -> MatrixInfo:
        return MatrixInfo(zip_path=self.projects_dir / entity_id / 'matrix.mtx.zip',
                          extract_path=entity_id,
                          project_uuid=entity_id,
                          source=self.SOURCE_NAME)

    def get_matrix_modifications_times(self) -> Dict[str, datetime]:
        return self.matrix_mtimes


def get_provider() -> MatrixProvider:
    provider_types = [
        g
        for g
        in globals().values()
        if isinstance(g, type) and issubclass(g, MatrixProvider) and hasattr(g, 'SOURCE_NAME')
    ]

    for provider_type in provider_types:
        if provider_type.SOURCE_NAME == config.matrix_source:
            break
    else:
        raise EnvironmentError('Provided matrix source does not match any implementation')

    return provider_type()
