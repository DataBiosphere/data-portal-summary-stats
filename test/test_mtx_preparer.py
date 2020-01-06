import logging

import os
import unittest

from more_itertools import one
import scanpy as sc

from dpss.matrix_preparer import MatrixPreparer
from test.tempdir_test_case import (
    MockMatrixTestCase,
)

log = logging.getLogger(__name__)


def test_scanpy_read(path):
    sc.read_10x_mtx(path, var_names='gene_symbols')


class TestMatrixPreparer(MockMatrixTestCase):

    def setUp(self):
        super().setUp()
        self.preparer = MatrixPreparer(self.info)

    def test_unzip(self):
        self.preparer.unzip()

        self.assertFalse(os.path.exists(self.info.zip_path))
        self.assertEqual(set(os.listdir(self.info.extract_path)), set(MatrixPreparer.hca_filenames.values()))

    def test_preprocess(self):

        self.preparer.unzip()
        self.preparer.preprocess()
        self.assertEqual(set(os.listdir(self.info.extract_path)), set(MatrixPreparer.scanpy_filenames.values()))

        for filekey in ['barcodes', 'genes']:
            filename = os.path.join(self.info.extract_path, MatrixPreparer.scanpy_filenames[filekey])
            df = self.preparer._read_tsv(filename, False)
            #  Unfortunately Scanpy requires us to remove the headers so we lose
            #  a lot of info that would be useful in verification
            self.assertEqual(len(df.columns), len(MatrixPreparer.scanpy_tsv_columns[filekey]))

        test_scanpy_read(self.info.extract_path)

    def test_prune(self):

        matrix_path = os.path.join(self.info.extract_path, MatrixPreparer.hca_filenames['matrix'])

        target_frac = 0.25

        self.preparer.unzip()

        _, old_mat = self.preparer._read_matrixmarket(matrix_path)
        self.preparer.prune(target_frac)
        _, new_mat = self.preparer._read_matrixmarket(matrix_path)

        # confirm new_mat is subset of old_mat
        # https://stackoverflow.com/a/49531052/1530508
        # -1 for non-matching file size pseudo-headers
        self.assertEqual(len(new_mat.merge(old_mat)), len(new_mat)-1)

        old_entry_count = old_mat.index.size - 1
        new_entry_count = new_mat.index.size - 1
        observed_frac = new_entry_count / old_entry_count
        frac_bounds = ((new_entry_count - 1) / old_entry_count, (new_entry_count + 1) / old_entry_count)

        delta = lambda frac: abs(target_frac - frac)
        for bound in frac_bounds:
            self.assertLess(delta(observed_frac), delta(bound))

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
        barcodes = self.preparer._read_tsv(barcodes_path, False)
        barcodes.iloc[1:20, 1] = other_approach
        self.preparer._write_tsv(barcodes_path, barcodes)

        sep_infos = self.preparer.separate()

        observed_lcas = {i.lib_con_approaches for i in sep_infos}

        self.assertEqual(observed_lcas, expected_lcas)

        for sep_info in sep_infos:
            self.assertTrue(os.path.isdir(sep_info.extract_path))
            self.assertEqual(sep_info.extract_path, os.path.join(self.info.extract_path, one(sep_info.lib_con_approaches)))
            for filekey, filename in MatrixPreparer.scanpy_filenames.items():
                filename = os.path.join(sep_info.extract_path, filename)
                if filekey == 'matrix':
                    self.assertTrue(os.path.isfile(filename))
                    self.assertFalse(os.path.islink(filename))
                else:
                    self.assertTrue(os.path.islink(filename))
            test_scanpy_read(sep_info.extract_path)


if __name__ == '__main__':
    unittest.main()
