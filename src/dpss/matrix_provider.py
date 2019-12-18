from abc import (
    ABC,
    abstractmethod,
)
import logging
import os
import shutil
import time
from typing import (
    List,
    Dict,
    Union,
    Any,
    Optional,
)
import urllib.parse

import requests

from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.matrix_info import MatrixInfo
from dpss.matrix_summary_stats import MatrixSummaryStats
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

    def __init__(self, **kwargs):
        """
        :param kwargs:
        Optional: blacklist: Iterable[str] - matrices to skip.
        """
        self.blacklist = frozenset(kwargs.get('blacklist', []))

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

    def __iter__(self):
        """
        Download matrices and yield info objects.
        """
        entity_ids = self.get_entity_ids()
        log.info(f'Found {len(entity_ids)} target entities, {len(self.blacklist)} of which may be blacklisted.')
        for entity_id in entity_ids:
            if entity_id in self.blacklist:
                log.info(f'Skipping blacklisted matrix {entity_id}')
            else:
                yield self.obtain_matrix(entity_id)


class CannedMatrixProvider(MatrixProvider):
    mtx_ext = '.mtx.zip'

    def __init__(self, **kwargs):
        """
        :param kwargs:
        Required: s3_service: S3Service - AWS S3 service
        Other params defined in superclass.
        """
        self.s3 = kwargs.pop('s3_service')
        super().__init__(**kwargs)

    def get_entity_ids(self) -> List[str]:
        """List matrix objects in S3 bucket."""
        keys = self.s3.list_bucket('matrices')
        return [file_id(key, self.mtx_ext)
                for key in keys
                if key.endswith(self.mtx_ext)]

    def obtain_matrix(self, matrix_id) -> MatrixInfo:
        """Download matrix from S3."""
        log.info(f'Downloading matrix {matrix_id} from S3.')
        filename = matrix_id + self.mtx_ext
        self.s3.download('matrices', filename)
        assert filename in os.listdir('.')  # confirm successful download
        size = os.path.getsize(filename)  # in bytes
        log.info(f'Size of {filename}: {convert_size(size)}')
        return MatrixInfo(source='canned',
                          project_uuid=matrix_id,
                          zip_path=filename,
                          extract_path=remove_ext(filename, '.zip'))


class FreshMatrixProvider(MatrixProvider):
    project_id_field = 'project.provenance.document_id'
    min_gene_count_field = 'genes_detected'
    mtx_feature = 'gene'
    mtx_format = 'mtx'

    def __init__(self, **kwargs):
        """
        :param kwargs:
        Other params defined in superclass.
        """
        super().__init__(**kwargs)

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

        status_response = self._request_matrix(project_id)
        assert status_response.status_code == 200
        s3_download_url = status_response.json()['matrix_url']
        log.info(f'Download URL for matrix is {s3_download_url}')

        matrix_response = requests.get(s3_download_url, stream=True)
        matrix_zipfile_name = os.path.basename(s3_download_url)

        with open(matrix_zipfile_name, 'wb') as matrix_zipfile:
            shutil.copyfileobj(matrix_response.raw, matrix_zipfile)

        lcas = self.get_project_field(project_id, 'project_lca', [])

        try:
            tr_lcas = frozenset(MatrixSummaryStats.translate_lca(lca) for lca in lcas)
        except ValueError:
            raise SkipMatrix

        return MatrixInfo(source='fresh',
                          project_uuid=project_id,
                          zip_path=matrix_zipfile_name,
                          extract_path=remove_ext(matrix_zipfile_name, '.zip'),
                          lib_con_approaches=tr_lcas)

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
                    },
                    {
                        'op': '>=',
                        'value': self.get_gene_threshold(project_id),
                        'field': self.min_gene_count_field
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
                    'project_lca': one(hit['protocols'])['libraryConstructionApproach']
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

    def get_project_field(self, project_id: str, field: str, default: Any = None) -> Any:
        try:
            project = self.projects[project_id]
        except KeyError:
            return default
        else:
            return project[field]

    def get_gene_threshold(self, project_id: str) -> int:
        lcas = self.get_project_field(project_id, 'project_lca')
        if lcas:
            try:
                return min(map(MatrixSummaryStats.get_min_gene_count, lcas))
            except ValueError:
                raise SkipMatrix
        else:
            return 0
