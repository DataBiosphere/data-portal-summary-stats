from abc import (
    ABC,
    abstractmethod,
)
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import (
    List,
    Dict,
    Union,
    Optional,
    TypeVar,
    Type,
)
import unicodedata
import urllib.parse

import dateutil.tz.tz as tz
import requests
import pandas as pd

from dpss.config import config
from dpss.exceptions import SkipMatrix
from dpss.logging import setup_log
from dpss.matrix_info import MatrixInfo
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import s3service
from dpss.utils import (
    convert_size,
    file_id,
    remove_ext,
    filter_exceptions,
    sort_optionals,
)
from more_itertools import (
    one,
)

log = setup_log(__name__)


class MatrixProvider(ABC):
    """
    Base class for matrix provision.
    """
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

    def get_matrix_size(self, entity_id: str) -> Optional[int]:
        """
        Size of matrix in bytes, it this can be efficiently determined from the
        id alone.
        """
        return None

    def filter_entity_id(self, entity_id: str) -> None:
        """
        This method is used to filter matrices that are unfit for processing
        *before* resources are invested into actually acquiring the matrix.
        """
        if entity_id in self.blacklist:
            raise SkipMatrix('blacklisted')

        if config.target_uuids is not None and entity_id not in config.target_uuids:
            raise SkipMatrix('not targeted')

    def __iter__(self):
        """
        Download matrices and yield info objects.
        When matrix sizes can be determined ahead of time, they are processed in decreasing order of size.
        """
        entity_ids = self.get_entity_ids()
        filtered_entity_ids, skips = filter_exceptions(self.filter_entity_id, entity_ids, exc_cls=SkipMatrix)

        log.info(f'Found {len(entity_ids)} entities, {len(filtered_entity_ids)} of which seem fit for processing')

        for (entity_id, skip) in skips:
            log.debug(f'Matrix {entity_id} skipped for reason: {skip}')

        sorted_entity_ids = sort_optionals(
            filtered_entity_ids,
            none_behavior='back',
            key=self.get_matrix_size
        )

        for entity_id in sorted_entity_ids:
            try:
                yield self.obtain_matrix(entity_id)
            except SkipMatrix as skip:
                log.info(f'Skipping acquired matrix {entity_id}; unexpected problem: {skip}')


class IdempotentMatrixProvider(MatrixProvider, ABC):
    """
    Provider that can detect when figures are outdated and skip processing
    otherwise.
    """
    def __init__(self):
        super().__init__()
        self.figure_mtimes = self.get_figure_modification_times()

    @abstractmethod
    def get_matrix_modification_time(self, entity_id: str) -> datetime:
        """
        Time the matrix was last modified.
        Datetime objects should be timezone-aware and set to the local timezone.
        """
        raise NotImplementedError

    def filter_entity_id(self, entity_id: str) -> None:
        super().filter_entity_id(entity_id)
        if config.ignore_mtime:
            return

        matrix_mtime = self.get_matrix_modification_time(entity_id)
        try:
            figure_mtime = self.figure_mtimes[entity_id]
        except KeyError:
            log.debug(f'Unable to determine figure modification time for {entity_id}')
        else:
            log.debug(f'Matrix {entity_id} modified {matrix_mtime}, oldest figure uploaded {figure_mtime}')
            outdated = matrix_mtime > figure_mtime
            if not outdated:
                raise SkipMatrix(f'up to date')

    def get_figure_modification_times(self) -> Dict[str, datetime]:
        objects = s3service.list_bucket('figures', map_fields='LastModified')
        figures = {
            k: v
            for k, v
            in objects.items()
            if any(
                k.endswith(f'{figure}.{MatrixSummaryStats.figure_format}')
                for figure
                in MatrixSummaryStats.target_images()
            )
        }
        df = pd.DataFrame(figures.items(), columns=['Key', 'LastModified'])
        df['uuid'] = df['Key'].map(lambda k: k.split('/')[2].split('.')[0])

        return {uuid: group['LastModified'].min() for uuid, group in df.groupby('uuid')}


class CannedMatrixProvider(IdempotentMatrixProvider):
    """
    Download matrices from S3 bucket.
    """
    SOURCE_NAME = 'canned'

    mtx_ext = '.mtx.zip'

    def __init__(self):
        super().__init__()
        objects = s3service.list_bucket('matrices', map_fields='LastModified')
        matrices = [k for k in objects.keys() if k.endswith(self.mtx_ext)]
        self.matrix_mtimes = {file_id(key, self.mtx_ext): objects[key] for key in matrices}

    def get_entity_ids(self) -> List[str]:
        """
        List matrix objects in S3 bucket.
        """
        return list(self.matrix_mtimes.keys())

    def obtain_matrix(self, matrix_id) -> MatrixInfo:
        """
        Download matrix from S3.
        """
        log.info(f'Downloading matrix {matrix_id} from S3.')
        filename = matrix_id + self.mtx_ext
        s3service.download('matrices', filename)
        assert filename in os.listdir('.')  # confirm successful download
        size = os.path.getsize(filename)  # in bytes
        log.info(f'Size of {filename}: {convert_size(size)}')
        return MatrixInfo(
            source=self.SOURCE_NAME,
            project_uuid=matrix_id,
            zip_path=filename,
            extract_path=Path(remove_ext(filename, '.zip'))
        )

    def get_matrix_size(self, entity_id: str) -> Optional[int]:
        """
        Read size from S3 HeadObject.
        """
        return s3service.get_object_size('matrices', entity_id + self.mtx_ext)

    def get_matrix_modification_time(self, matrix_id) -> datetime:
        """
        Report S3 ContentLength
        """
        return self.matrix_mtimes[matrix_id]


class FreshMatrixProvider(MatrixProvider):
    """
    Download matrices from HCA matrix service.
    """
    SOURCE_NAME = 'fresh'

    project_id_field = 'project.provenance.document_id'
    min_gene_count_field = 'genes_detected'
    mtx_feature = 'gene'
    mtx_format = 'mtx'

    def __init__(self):
        super().__init__()
        self.projects = self._get_project_info_from_azul()

    def get_entity_ids(self):
        """
        Return list of matrix directory names (with prefix keys) from matrix service
        """
        response = requests.get(self.hca_matrix_service_project_list_url)
        self.check_response(response)
        return list(response.json()['cell_counts'].keys())

    def obtain_matrix(self, project_id: str) -> MatrixInfo:
        log.info(f'Requesting matrix from project {project_id} from HCA.')

        lcas = frozenset(
            MatrixSummaryStats.translate_lca(lca)
            for lca
            in self.get_project_field(project_id, 'project_lcas', [])
        )

        status_response = self._request_matrix(project_id)
        assert status_response.status_code == 200
        s3_download_url = status_response.json()['matrix_url']
        log.debug(f'Download URL for matrix is {s3_download_url}')

        matrix_response = requests.get(s3_download_url, stream=True)
        matrix_zipfile_name = os.path.basename(s3_download_url)

        with open(matrix_zipfile_name, 'wb') as matrix_zipfile:
            shutil.copyfileobj(matrix_response.raw, matrix_zipfile)

        return MatrixInfo(
            source=self.SOURCE_NAME,
            project_uuid=project_id,
            zip_path=Path(matrix_zipfile_name),
            extract_path=Path(remove_ext(matrix_zipfile_name, '.zip')),
            lib_con_approaches=lcas
        )

    def filter_entity_id(self, project_id: str) -> None:
        try:
            list(map(MatrixSummaryStats.translate_lca, self.get_project_field(project_id, 'project_lcas', [])))
        except ValueError:
            raise SkipMatrix('bad azul lca')

    def _request_matrix(self, project_id: str) -> requests.models.Response:

        payload = {
            'feature': self.mtx_feature,
            'format': self.mtx_format,
            'filter': {
                'op': '=',
                'value': project_id,
                'field': self.project_id_field
            }
        }

        log.debug(f'Requesting expression matrix for project document ID {project_id}')
        log.debug(f'Request payload and filter settings: {payload}')
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
                log.debug(f'Matrix request status: {status}...')
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

    @property
    def hca_matrix_service_project_list_url(self):
        return f'{config.hca_matrix_service_endpoint}filters/{self.project_id_field}'

    @property
    def hca_matrix_service_request_url(self):
        return f'{config.hca_matrix_service_endpoint}matrix/'


class LocalMatrixProvider(IdempotentMatrixProvider):
    """
    Use matrices already present on local disk.
    """
    SOURCE_NAME = 'local'

    def __init__(self):
        super().__init__()
        self.root_dir = config.local_projects_path.resolve()
        self.project_dirs = [p for p in self.root_dir.iterdir() if not p.is_symlink()]
        self.matrix_mtimes = {}
        self.species = {}

    def get_entity_ids(self) -> List[str]:
        """
        Matrix project folder names.
        """
        return [p.name for p in self.project_dirs]

    def obtain_matrix(self, entity_id: str) -> MatrixInfo:
        """
        Just construct MatrixInfo for already present files.
        """
        try:
            species = self.species[entity_id]
        except KeyError:
            species = self.species[entity_id] = self._find_species(entity_id)
        asset_label = entity_id if species is None else f'{entity_id}.{species}'
        return MatrixInfo(
            zip_path=self._matrix_file(entity_id),
            extract_path=Path(entity_id),
            project_uuid=asset_label,
            source=self.SOURCE_NAME
        )

    def get_matrix_modification_time(self, entity_id) -> datetime:
        """
        stat local files.
        """
        return datetime.fromtimestamp(
            self._matrix_file(entity_id).stat().st_mtime,
            tz=tz.tzlocal()
        )

    def get_matrix_size(self, entity_id: str) -> Optional[int]:
        """
        stat local files.
        """
        return self._matrix_file(entity_id).stat().st_size

    def filter_entity_id(self, entity_id: str) -> None:
        mtx_path = self._matrix_file(entity_id)
        if not mtx_path.exists():
            raise SkipMatrix(f'{mtx_path} does not exist')

        # AFTER checking if matrix exists since superclass checks mtime here
        super().filter_entity_id(entity_id)

        try:
            self.species[entity_id] = self._find_species(entity_id)
        except Exception:
            raise SkipMatrix('species not found')

    def _find_species(self, entity_id):
        # copied from /home/ubuntu/load-project/upload_assets.py
        # raises a million and one exceptions
        donor_path = self._bundle_dir(entity_id) / 'donor_organism_0.json'
        with open(str(donor_path), 'r') as cs_json:
            cell_suspension_json = json.load(cs_json)
        species_name = cell_suspension_json['genus_species'][0]['text']
        species_name = unicodedata.normalize('NFKD', species_name)
        return re.sub(r'[^\w,.@%&-_()\\[\]/{}]', '_', species_name).strip().lower()

    def _bundle_dir(self, entity_id: str) -> Path:
        return self.root_dir / entity_id / 'bundle'

    def _matrix_file(self, entity_id: str) -> Path:
        return self._bundle_dir(entity_id) / 'matrix.mtx.zip'


def get_provider_type() -> Type[MatrixProvider]:
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
        raise RuntimeError(f'Provided matrix source {config.matrix_source} '
                           'does not match any implementation')

    return provider_type
