from abc import (
    abstractmethod,
    ABC,
)
import os
import unittest
from unittest import mock

import responses

from dpss.config import (
    Config,
    config,
)
from dpss.exceptions import SkipMatrix
from dpss.matrix_provider import (
    CannedMatrixProvider,
    FreshMatrixProvider,
)
from dpss.s3_service import (
    S3Service,
    s3service,
)
from dpss.utils import TemporaryDirectoryChange
from test.s3_test_case import S3TestCase
from test.tempdir_test_case import TempdirTestCase


class TestMatrixProvider(ABC):

    @abstractmethod
    def test_get_entity_ids(self):
        raise NotImplementedError

    @abstractmethod
    def test_obtain_matrix(self):
        raise NotImplementedError


@mock.patch.object(Config, 'source_stage', new=mock.PropertyMock(return_value='dev'))
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

        self.assertRaises(SkipMatrix, provider.obtain_matrix, self.matrix_id_bad)


class TestCanned(TempdirTestCase, S3TestCase, TestMatrixProvider):
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

    @mock.patch.object(S3Service, 'get_blacklist')
    def test_get_entity_ids(self, mock_method):
        mock_method.return_value = ['bad']
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


if __name__ == '__main__':
    unittest.main()
