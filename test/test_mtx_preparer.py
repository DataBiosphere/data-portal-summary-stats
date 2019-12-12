import logging

import unittest

from more_itertools import one
import numpy as np
import scanpy as sc

from dpss.matrix_preparer import (
    MatrixPreparer,
    Mtx,
    Tsv,
)
from dpss.utils import traverse_dirs
from test.tempdir_test_case import (
    MockMatrixTestCase,
)


def test_scanpy_read(path):
    sc.read_10x_mtx(path, var_names='gene_symbols')


class TestMatrixPreparer(MockMatrixTestCase):

    def setUp(self):
        super().setUp()
        self.preparer = MatrixPreparer(self.info)

    def test_unzip(self):
        self.assertFalse(self.info.extract_path.exists())
        self.preparer.unzip()
        self.assertTrue(self.preparer.info.extract_path.exists())
        self.assertTrue(all(map(
            lambda d: not any(f.name.endswith('.gz') for f in d.iterdir()),
            traverse_dirs(self.preparer.info.extract_path)
        )))

    def test_preprocess(self):

        self.preparer.unzip()
        self.preparer.preprocess()
        test_scanpy_read(self.info.extract_path)

    def test_prune(self):

        matrix_path = self.info.extract_path / 'matrix.mtx'

        target_frac = 0.25

        self.preparer.unzip()

        old_mat = Mtx(str(matrix_path))
        self.preparer.prune(target_frac)
        new_mat = Mtx(str(matrix_path))

        # confirm new_mat is subset of old_mat
        # https://stackoverflow.com/a/49531052/1530508
        self.assertEqual(len(new_mat.data.merge(old_mat.data)), len(new_mat))

        # confirm that len(new_mat) is as close as possible to target_frac * len(old_mat)
        deltas = np.abs((len(new_mat) + np.array([-1, 0, 1])) / len(old_mat) - target_frac)
        self.assertEqual(np.argmin(deltas), 1)

        self.preparer.preprocess()
        test_scanpy_read(self.info.extract_path)

    def test_separate_homogeneous(self):
        self.preparer.unzip()
        self.preparer.preprocess()
        sep_infos = self.preparer.separate()
        self.assertEqual(len(sep_infos), 1)
        self.assertEqual(sep_infos[0], self.info)
        self.assertEqual(self.info.lib_con_approaches, frozenset({'SS2'}))

        test_scanpy_read(self.info.extract_path)

    def test_separate_heterogeneous(self):

        other_approach = '10X v2 whatever'
        expected_lcas = {frozenset({'SS2'}), frozenset({'10X'})}

        self.preparer.unzip()
        self.preparer.preprocess()

        # introduce heterogeneity
        barcodes_path = f'{self.preparer.info.extract_path}/barcodes.tsv'
        barcodes = Tsv(barcodes_path, False)
        barcodes.data.iloc[1:20, 1] = other_approach
        barcodes.write()

        sep_infos = self.preparer.separate()

        self.assertEqual({i.lib_con_approaches for i in sep_infos}, expected_lcas)

        for sep_info in sep_infos:
            self.assertTrue(sep_info.extract_path.is_dir())
            self.assertEqual(sep_info.extract_path, self.info.extract_path / one(sep_info.lib_con_approaches))
            for file in ['matrix.mtx', 'genes.tsv', 'barcodes.tsv']:
                path = sep_info.extract_path / file
                self.assertTrue(path.exists())
                self.assertTrue(path.is_symlink() ^ file.endswith('.mtx'))
            test_scanpy_read(sep_info.extract_path)


if __name__ == '__main__':
    unittest.main()
