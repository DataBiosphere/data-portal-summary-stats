import os
from pathlib import Path
import unittest

from dpss.config import config
from dpss.matrix_info import MatrixInfo
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.s3_service import (
    s3service,
)
from dpss.utils import TemporaryDirectoryChange
from test.s3_test_case import S3TestCase


class TestS3Service(S3TestCase):

    def test_list_bucket(self):
        target_bucket = 'matrices'
        keys = s3service.list_bucket(target_bucket)
        self.assertEqual(keys, [])

        target_key = config.s3_canned_matrix_prefix + 'foo'
        s3service.client.put_object(
            Bucket=config.s3_matrix_bucket_name,
            Key=target_key,
            Body=b'Hi'
        )
        s3service.client.put_object(
            Bucket=config.s3_matrix_bucket_name,
            Key='non-target',
            Body=b'Bye'
        )
        keys = s3service.list_bucket(target_bucket)
        self.assertEqual(keys, [target_key])

    def sort_by_size(self):
        names = [config.s3_figures_prefix + name for name in ['foo', 'bar', 'baz', 'quux', 'fitz', 'spatz']]
        for i, name in enumerate(names):
            s3service.client.put_object(
                Bucket=config.s3_figure_bucket_name,
                Key=name,
                Body=b'_' * i
            )
        ordered = s3service.sort_by_size('figures', names[::-1])
        self.assertEqual(ordered, names)

    def test_download(self):
        key = 'hi'
        s3service.client.put_object(
            Bucket=config.s3_matrix_bucket_name,
            Key=config.s3_canned_matrix_prefix + key,
            Body=b'Hi'
        )
        with TemporaryDirectoryChange():
            s3service.download('matrices', key)
            self.assertEqual(os.listdir('.'), [key])

    def test_get_blacklist(self):
        s3service.client.put_object(
            Bucket=config.s3_matrix_bucket_name,
            Key=f'blacklist',
            Body=b'123\n456\n789\n'
        )

        blacklist = s3service.get_blacklist()
        self.assertEqual(blacklist, ['123', '456', '789'])

    def test_upload_figures(self):
        figures_files = MatrixSummaryStats.target_images().keys()
        figures_dir = 'figures'
        project_uuid = '123'
        lca = 'SS2'
        with TemporaryDirectoryChange():
            os.mkdir(figures_dir)
            parent = Path(figures_dir)
            for file in figures_files:
                (parent / file).touch()
                s3service.upload_figure(
                    MatrixInfo(
                        source='nonexistent',
                        zip_path=None,
                        extract_path=Path('does/not/exist'),
                        project_uuid=project_uuid,
                        lib_con_approaches=frozenset([lca])
                    ),
                    file
                )
            found_objects = set(s3service.list_bucket('figures'))
            expected_objects = {
                f'{config.s3_figures_prefix}{project_uuid}/{lca}/{file}'
                for file
                in figures_files
            }
            self.assertEqual(found_objects, expected_objects)


if __name__ == '__main__':
    unittest.main()
