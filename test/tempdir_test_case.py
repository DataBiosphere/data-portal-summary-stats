from pathlib import Path
import shutil
import unittest

from dpss.matrix_info import MatrixInfo
from dpss.utils import (
    TemporaryDirectoryChange,
    file_id,
    remove_ext,
)


class TempdirTestCase(unittest.TestCase):

    def setUp(self):
        self.tempdir_manager = TemporaryDirectoryChange()
        self.tempdir_manager.__enter__()

    def tearDown(self):
        self.tempdir_manager.__exit__()


class MockMatrixTestCase(TempdirTestCase):
    mock_matrix = 'e7d811e2-832a-4452-85a5-989e2f8267bf.mtx.zip'

    def setUp(self):
        super().setUp()
        src = Path(__file__).resolve().parent / 'resources' / self.mock_matrix
        dst = f'./{self.mock_matrix}'
        shutil.copyfile(src, dst)

        self.info = MatrixInfo(
            source='mock',
            zip_path=Path(self.mock_matrix),
            extract_path=Path(remove_ext(self.mock_matrix, '.zip')),
            project_uuid=file_id(self.mock_matrix, '.mtx.zip'))
