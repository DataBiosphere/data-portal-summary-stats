from abc import (
    abstractmethod,
    ABC,
)
from datetime import (
    datetime,
    timedelta,
)
import os
from pathlib import Path
import shutil
from typing import List
import unittest
from unittest import mock

from mock import PropertyMock
import responses

from dpss import MatrixInfo
from dpss.config import (
    Config,
    config,
)
from dpss.exceptions import SkipMatrix
from dpss.matrix_provider import (
    CannedMatrixProvider,
    FreshMatrixProvider,
    LocalMatrixProvider,
    MatrixProvider,
    IdempotentMatrixProvider,
    get_provider_type,
)
from dpss.s3_service import (
    S3Service,
    s3service,
)
from dpss.utils import (
    TemporaryDirectoryChange,
)
from test.s3_test_case import S3TestCase
from test.tempdir_test_case import TempdirTestCase


class TestMatrixProvider(ABC):

    @abstractmethod
    def test_get_entity_ids(self):
        raise NotImplementedError

    @abstractmethod
    def test_obtain_matrix(self):
        raise NotImplementedError

    @abstractmethod
    def test_filter_entity_id(self):
        raise NotImplementedError


class TestAbstractMatrixProvider(TestMatrixProvider, unittest.TestCase):
    """
    Test non-abstract methods in abstract matrix provider superclass
    """

    class MockAbstractMatrixProvider(MatrixProvider):
        """
        Implement abstract methods to allow instantiation for testing of
        non-abstract methods
        """

        def obtain_matrix(self, entity_id: str) -> MatrixInfo:
            pass

        def get_entity_ids(self) -> List[str]:
            pass

    def test_get_entity_ids(self):
        pass

    def test_obtain_matrix(self):
        pass

    def test_filter_entity_id(self):
        with self.subTest('blacklist'):
            with mock.patch.object(S3Service, 'get_blacklist', return_value=['bad']), \
                 mock.patch.object(Config, 'use_blacklist', new=PropertyMock(return_value=True)):
                provider = self.MockAbstractMatrixProvider()
                self.assertRaises(SkipMatrix, provider.filter_entity_id, 'bad')

        with self.subTest('target uuids'):
            with mock.patch.object(Config, 'target_uuids', new=PropertyMock(return_value=None)):
                provider = self.MockAbstractMatrixProvider()
                self.assertIsNone(provider.filter_entity_id('not bad'))

            with mock.patch.object(Config, 'target_uuids', new=PropertyMock(return_value=['someone else'])):
                provider = self.MockAbstractMatrixProvider()
                self.assertRaises(SkipMatrix, provider.filter_entity_id, 'not bad')


const_date = datetime.now()

matrix_mtimes = {
    '123': const_date + timedelta(days=1),  # outdated
    '456': const_date - timedelta(days=1),  # not outdated
    '789': const_date  # no figure exists
}


class TestIdempotence(S3TestCase, TestAbstractMatrixProvider):
    class TestAbstractIdempotentMatrixProvider(
        IdempotentMatrixProvider,
        TestAbstractMatrixProvider.MockAbstractMatrixProvider
    ):

        def get_matrix_modification_time(self, entity_id: str) -> datetime:
            return matrix_mtimes[entity_id]

    @mock.patch.object(S3Service, 'get_blacklist', return_value=[])
    @mock.patch.object(
        IdempotentMatrixProvider,
        'get_figure_modification_times',
        return_value={uuid: const_date for uuid in ['123', '456']}
    )
    @mock.patch.object(Config, 'target_uuids', new=PropertyMock(return_value=None))
    def test_filter_entity_id(self, mock_blacklist, mock_fig_mtimes):
        provider = self.TestAbstractIdempotentMatrixProvider()

        with self.subTest('ignore mtime'):
            with mock.patch.object(Config, 'ignore_mtime', new=PropertyMock(return_value=True)):
                for uuid in matrix_mtimes:
                    self.assertIsNone(provider.filter_entity_id(uuid))

        with self.subTest('use mtime'):
            with mock.patch.object(Config, 'ignore_mtime', new=PropertyMock(return_value=False)):
                self.assertIsNone(provider.filter_entity_id('123'))
                self.assertIsNone(provider.filter_entity_id('789'))
                self.assertRaises(SkipMatrix, provider.filter_entity_id, '456')


class TestFresh(TempdirTestCase, TestMatrixProvider):
    matrix_id_good = 'iiiiiiiiiiiiiiiiii'
    matrix_id_bad = 'i93e85494839389'

    def _azul_mock(self):
        responses.add(
            responses.GET,
            config.azul_project_endpoint,
            status=200,
            json={
                "hits": [
                    {
                        "entryId": self.matrix_id_good,
                        "projects": [{
                            "projectTitle": "foo",
                        }],
                        "protocols": [{
                            "libraryConstructionApproach": [
                                "Smart-seq2"
                            ]
                        }]
                    },
                    {
                        "entryId": self.matrix_id_bad,
                        "projects": [{
                            "projectTitle": "bar",
                        }],
                        "protocols": [{
                            "libraryConstructionApproach": [
                                "not a good boy"
                            ]
                        }]
                    }
                ],
                "pagination": {
                    "search_after": None,
                    "search_after_uid": None
                }
            }
        )

    @responses.activate
    def test_get_entity_ids(self):
        self._azul_mock()

        provider = FreshMatrixProvider()

        cell_counts = {
            '08e7b6ba-5825-47e9-be2d-7978533c5f8c': 2,
            '425efc0a-d3fe-4fab-9f74-3bd829ebdf01': 1,
            '84e01f35-4f77-4e07-ac7b-058f545d782a': 1
        }

        responses.add(
            responses.GET,
            provider.hca_matrix_service_project_list_url,
            json={
                'cell_counts': cell_counts,
                'field_description': 'Unique identifier for overall project.',
                'field_name': FreshMatrixProvider.project_id_field,
                'field_type': 'categorical'
            }
        )

        observed_list = provider.get_entity_ids()
        self.assertEqual(set(observed_list), set(cell_counts.keys()))

    @responses.activate
    def test_obtain_matrix(self):
        self._azul_mock()

        provider = FreshMatrixProvider()

        request_id = '3c44455c-d751-4fc9-a119-3a55c05f8990'

        matrix_url = 'https://s3.amazonaws.com/fake-matrix-service-results/0/424242/13-13.mtx.zip'

        # First response: GET (to satisfy the assertion):
        responses.add(
            responses.GET,
            provider.hca_matrix_service_project_list_url,
            json={
                'cell_counts': {
                    self.matrix_id_good: 2,
                },
            }
        )

        # Second response: POST:
        responses.add(
            responses.POST,
            provider.hca_matrix_service_request_url,
            json={
                'message': 'Job started.',
                'non_human_request_ids': {},
                'request_id': request_id,
                'status': 'In Progress'
            },
            status=202
        )

        # Third response: GET returns in progress:
        responses.add(
            responses.GET,
            provider.hca_matrix_service_request_url + request_id,
            json={'status': 'In Progress'}
        )

        # Fourth response: GET returns "complete":
        responses.add(
            responses.GET,
            provider.hca_matrix_service_request_url + request_id,
            json={
                'eta': '',
                'matrix_url': matrix_url,
                'message': f'Request {request_id} has successfully ...',
                'request_id': request_id,
                'status': 'Complete'
            },
            status=200
        )

        # Fifth response: GET in get_expression_matrix_from_service
        responses.add(responses.GET, matrix_url, status=200, stream=True)

        mtx_info = provider.obtain_matrix(self.matrix_id_good)
        self.assertEqual(str(mtx_info.zip_path), '13-13.mtx.zip')
        self.assertEqual(mtx_info.source, 'fresh')

    @responses.activate
    def test_filter_entity_id(self):
        self._azul_mock()
        provider = FreshMatrixProvider()
        self.assertIsNone(provider.filter_entity_id(self.matrix_id_good))
        self.assertRaises(SkipMatrix, provider.filter_entity_id, self.matrix_id_bad)


class TestCanned(TempdirTestCase, S3TestCase, TestMatrixProvider):

    def test_filter_entity_id(self):
        pass

    uuids = {'123', '456', '789', 'bad'}

    def setUp(self) -> None:
        S3TestCase.setUp(self)
        TempdirTestCase.setUp(self)
        for uuid in self.uuids:
            s3service.client.put_object(
                Bucket=config.s3_matrix_bucket_name,
                Key=f'{config.s3_canned_matrix_prefix}{uuid}.mtx.zip'
            )

    def tearDown(self):
        TempdirTestCase.tearDown(self)
        S3TestCase.tearDown(self)

    def test_get_entity_ids(self):
        provider = CannedMatrixProvider()
        self.assertEqual(set(provider.get_entity_ids()), self.uuids)

    def test_obtain_matrix(self):
        uuid = '123'
        key = uuid + '.mtx.zip'
        provider = CannedMatrixProvider()
        with TemporaryDirectoryChange():
            mtx_info = provider.obtain_matrix(uuid)
            self.assertEqual(os.listdir('.'), [key])
            self.assertEqual(mtx_info.zip_path, key)
            self.assertEqual(mtx_info.source, 'canned')


local_test_dir = Path('/tmp/local_matrix_test')


@mock.patch.object(Config, 'local_projects_path', new=mock.PropertyMock(return_value=local_test_dir))
class TestLocal(S3TestCase, TestMatrixProvider):
    fake_projects = {'123', '456', '789'}

    def setUp(self):
        super().setUp()
        local_test_dir.mkdir()
        for project in self.fake_projects:
            bundle_dir = local_test_dir / project / 'bundle'
            bundle_dir.mkdir(parents=True)
            with open(str(bundle_dir / 'donor_organism_0.json'), 'w') as f:
                f.write('{"genus_species":[{"text":"Homo_sapiens"}]}')
            (bundle_dir / 'matrix.mtx.zip').touch()
            (local_test_dir / f'GSE_{project}').symlink_to(project)

    def tearDown(self):
        shutil.rmtree(local_test_dir)
        super().tearDown()

    def test_get_entity_ids(self):
        provider = LocalMatrixProvider()
        self.assertEqual(set(provider.get_entity_ids()), self.fake_projects)

    def test_obtain_matrix(self):
        provider = LocalMatrixProvider()
        for project_id in provider.get_entity_ids():
            mtx_info = provider.obtain_matrix(project_id)
            self.assertEqual(mtx_info.source, 'local')
            self.assertTrue(mtx_info.extract_path.name in self.fake_projects)
            self.assertTrue(mtx_info.zip_path.is_file())
            self.assertTrue(mtx_info.project_uuid.endswith('.homo_sapiens'))

    @mock.patch.object(S3Service, 'get_blacklist', return_value=[])
    @mock.patch.object(Config, 'target_uuids', new=PropertyMock(return_value=None))
    def test_filter_entity_id(self, mock_blacklist):
        projects = iter(self.fake_projects)
        provider = LocalMatrixProvider()

        with self.subTest('normal'):
            project = next(projects)
            self.assertIsNone(provider.filter_entity_id(project))

        with self.subTest('no species'):
            project = next(projects)
            (local_test_dir / project / 'bundle' / 'donor_organism_0.json').unlink()
            self.assertRaises(SkipMatrix, provider.filter_entity_id, project)

        with self.subTest('no matrix file'):
            project = next(projects)
            (local_test_dir / project / 'bundle' / 'matrix.mtx.zip').unlink()
            self.assertRaises(SkipMatrix, provider.filter_entity_id, project)


class TestProvision(S3TestCase):

    def test_get_provider(self):
        types = {
            'canned': CannedMatrixProvider,
            'fresh': FreshMatrixProvider,
            'local': LocalMatrixProvider,
        }

        for source, type_ in types.items():
            with mock.patch.object(Config, 'matrix_source', new=PropertyMock(return_value=source)):
                self.assertIs(get_provider_type(), type_)

        with mock.patch.object(Config, 'matrix_source', new=PropertyMock(return_value='invalid')):
            self.assertRaises(RuntimeError, get_provider_type)


if __name__ == '__main__':
    unittest.main()
