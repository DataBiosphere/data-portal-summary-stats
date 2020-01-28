from collections import defaultdict
from pathlib import Path
import unittest
from unittest import mock

from more_itertools import first

from dpss.matrix_preparer import MatrixPreparer
from dpss.matrix_summary_stats import MatrixSummaryStats
from test.tempdir_test_case import MockMatrixTestCase


class TestMatrixSummaryStats(MockMatrixTestCase):

    def setUp(self) -> None:
        super().setUp()
        preparer = MatrixPreparer(self.info)
        preparer.unzip()
        preparer.preprocess()
        new_info = first(preparer.separate())
        self.mss = MatrixSummaryStats([new_info])
        self.mss.load_data()

    def test_lca_translation(self):
        pass_cases = [
            ('10X v2 sequencing', '10X'),
            ('10x v3 5\' whatever', '10X'),
            ('Smart-seq2', 'SS2'),
        ]

        for lca, tr_lca in pass_cases:
            self.assertEqual(MatrixSummaryStats.translate_lca(lca), tr_lca)

        fail_cases = [
           'beans',
           '20x',
           'smartseq2',
           'Smart-seq'
        ]

        for lca in fail_cases:
            self.assertRaises(ValueError, MatrixSummaryStats.translate_lca, lca)

    # Prevent zero division error with small matrix in ScanPy methods
    @mock.patch.object(MatrixSummaryStats, 'MIN_GENE_COUNTS', defaultdict(lambda: 0))
    def test_create_images(self):
        self.mss.create_images()
        fig_path = Path('figures')
        for figure in MatrixSummaryStats.target_images():
            self.assertTrue((fig_path / f'{figure}.{MatrixSummaryStats.figure_format}').is_file())


if __name__ == "__main__":
    unittest.main()
