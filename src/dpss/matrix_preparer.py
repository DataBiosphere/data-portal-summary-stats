import gzip
import logging
import os
from pathlib import Path

from more_itertools import one
import numpy as np
import pandas as pd
import shutil
from typing import (
    List,
    Callable,
    Union,
    Optional,
    Sequence,
)
from zipfile import ZipFile

from dpss.matrix_info import MatrixInfo
from dpss.matrix_summary_stats import MatrixSummaryStats
from dpss.utils import (
    DirectoryChange,
    gunzip,
    traverse_dirs,
    setup_log,
)

log = logging.getLogger(__name__)
setup_log(__name__, logging.INFO, logging.StreamHandler())


class Mtx:

    def __init__(self, path: str, **pd_kwargs):
        self.path = path
        with open(self.path) as f:
            self.header = f.readline()
            assert self.header.startswith('%')
        # files sizes become the column names
        self.data = pd.read_csv(path, comment='%', sep=' ', float_precision='round_trip', **pd_kwargs)
        self.sizes = [int(float(c)) for c in self.data.columns]
        self.data.columns = ['gene_idx', 'barcode_idx', 'count']

    def __len__(self):
        return self.sizes[2]

    def process(self):
        if np.issubdtype(self.data.dtypes[2], np.integer):
            return False
        else:
            # Technically, this should always already be int data, some in the past
            # this code has had to be adapted for processing other expression metrics,
            # and other times integral data just has needless 0's after a needless
            # decimal point.
            self.data['count'] = np.rint(self.data['count']).astype(np.int32)
            return True

    def filter_entries(self, which_rows: Union[Sequence[int], Callable[[pd.Series], bool]]) -> None:
        """
        :param which_rows: predicate function or Series of integer indices.
        """
        if callable(which_rows):
            keep_labels = self.data.apply(which_rows, axis=1)
            self.data = self.data.drop(labels=np.flatnonzero(~keep_labels))
        else:
            self.data = self.data.iloc[which_rows, :]

        self.data.reset_index(inplace=True)
        self.sizes[2] = self.data.shape[0]

    def write(self, path: Optional[str] = None, **pd_kwargs) -> None:
        path = self.path if path is None else path
        # ScanPy needs MatrixMarket header even though pandas can't read it
        with open(path, 'w') as f:
            f.write(self.header)
            f.write(' '.join(map(str, self.sizes)) + '\n')
        self.data.to_csv(path, index=False, header=False, sep=' ', mode='a', **pd_kwargs)


class Tsv:

    def __init__(self, path: str, header: bool, **pd_kwargs):
        self.path = path
        if not header:
            pd_kwargs['header'] = None
        self.data = pd.read_csv(self.path, index_col=None, sep='\t', **pd_kwargs)
        self.header = None

    def detect_header(self, expected_size) -> bool:
        size = self.data.shape[0]
        if size == expected_size:
            return False
        elif size == expected_size + 1:
            self.header = self.data.iloc[0, :]
            self.data = self.data.iloc[1:]
            return True
        else:
            raise RuntimeError(f'Could not reconcile tsv file with {size} entries with expected size {expected_size}')

    def write(self, path: Optional[str] = None, **pd_kwargs) -> None:
        path = self.path if path is None else path
        self.data.to_csv(path if path is None else path, index=False, header=False, sep='\t', **pd_kwargs)


class GenesTsv(Tsv):

    def process(self) -> bool:
        if self.data.shape[1] == 1:
            # scanpy needs both ids and symbols so if we lack one column we just
            # duplicate the existing one
            log.debug('Duplicating gene column')
            self.data = self.data.iloc[:, [0, 0]]
            return True
        else:
            return False


class BarcodesTsv(Tsv):
    lca_column = 'library_preparation_protocol.library_construction_method.ontology_label'

    def process(self) -> bool:
        # Keep LCA if we can identify it, otherwise drop all other columns
        # to save memory when AnnData is loaded
        if self.data.shape[1] == 1:
            return False
        else:
            if self.header is None:
                log.debug('No barcodes header, ignoring LCA')
                self.data = self.data.iloc[:, [0]]
            else:
                try:
                    lca_index = list(self.header).index(self.lca_column)
                except ValueError:
                    log.debug('Could not find LCA in barcodes header, ignoring')
                    self.data = self.data.iloc[:, [0]]
                else:
                    log.debug('Found LCA in barcodes header')
                    self.data = self.data.iloc[:, [0, lca_index]]
        return True


class MatrixPreparer:

    def __init__(self, mtx_info: MatrixInfo):
        self.info = mtx_info

    def unzip(self, remove_archive: bool = False) -> List[MatrixInfo]:
        """
        Extract files from top-level zip archive, uncompress .gz files, and
        optionally remove archive.
        """
        with ZipFile(self.info.zip_path) as zipfile:
            zipfile.extractall(self.info.extract_path)

        if remove_archive:
            os.remove(str(self.info.zip_path))

        def find_matrix(path: Path) -> Optional[Path]:
            anchor_file = path / 'matrix.mtx.gz'
            genes_file = path / 'genes.tsv.gz'
            cells_file = path / 'cells.tsv.gz'
            barcodes_file = path / 'barcodes.tsv.gz'
            try:
                gunzip(anchor_file)
            except FileNotFoundError:
                return
            else:
                gunzip(genes_file)
                if cells_file.exists():
                    os.rename(str(cells_file), str(barcodes_file))
                gunzip(barcodes_file)
                return path

        return [
            MatrixInfo(
                zip_path=None,
                extract_path=path,
                source=self.info.source,
                project_uuid=self.info.project_uuid,
                lib_con_approaches=self.info.lib_con_approaches
            )
            for path
            in filter(None, map(find_matrix, traverse_dirs(self.info.extract_path)))
        ]

    def preprocess(self):
        """
        Transform tsv/mtx files for ScanPy compatibility.
        """
        with DirectoryChange(self.info.extract_path):

            mtx = Mtx('matrix.mtx')
            sizes = mtx.sizes
            if mtx.process():
                mtx.write()

            del mtx

            genes = GenesTsv('genes.tsv', False)
            if genes.detect_header(sizes[0]) | genes.process():
                genes.write()

            del genes

            barcodes = BarcodesTsv('barcodes.tsv', False)
            if barcodes.detect_header(sizes[1]) | barcodes.process():
                barcodes.write()

            del barcodes

    def prune(self, keep_frac: float) -> None:
        """
        Shrink matrix by removing entries at random to reach a desired fraction
        of the original size.
        :param keep_frac: the fraction of entries to keep, e.g. 0.05 to remove
        95% of the entries.
        :return: nothing, operations occurs on disk.
        """
        if not (0 < keep_frac <= 1):
            raise ValueError(f'Invalid prune fraction: {keep_frac}')
        mtx_path = self.info.extract_path / 'matrix.mtx'
        mtx = Mtx(str(mtx_path))
        mtx.filter_entries(
            np.random.choice(
                np.arange(len(mtx)),
                round(keep_frac * len(mtx)),
                replace=False
            )
        )
        mtx.write()

    def separate(self) -> List[MatrixInfo]:
        """
        Split matrix into independent entities based on library construction approach.

        If the LCA is homogeneous, no change is made to the directory structure.
        Otherwise, a new directory is created within the extraction directory
        for every observed LCA and populated with the subset of matrix.mtx
        corresponding to that approach.
        Links are created for the row and column tsv files, which remain in the
        top-level extraction dir.
        :return: series of MatrixInfo objects describing the results of the
        separation.
        """
        with DirectoryChange(self.info.extract_path):
            barcodes = Tsv('barcodes.tsv', False)
            try:
                lib_con_data = barcodes.data.pop(barcodes.data.columns[1])
            except (IndexError, KeyError):
                log.info('No LCA data for matrix; skipping separation')
                return [self.info]
            # remove LCA column to save memory during stats generation
            barcodes.write()

            found_lcas = frozenset(lib_con_data.map(MatrixSummaryStats.translate_lca))
            assert len(found_lcas) > 0

            if not self.info.lib_con_approaches:
                log.debug('Filling empty LCA from file')
            elif found_lcas == self.info.lib_con_approaches:
                log.debug('All expected LCAs accounted for')
            elif found_lcas < self.info.lib_con_approaches:
                log.warning('Not all expected LCAS were found')
            else:
                raise RuntimeError(f'Unexpected LCA(s) found: {found_lcas} (expected {self.info.lib_con_approaches})')

            self.info.lib_con_approaches = found_lcas

            if len(self.info.lib_con_approaches) == 1:
                log.debug(f'Homogeneous LCA: {one(self.info.lib_con_approaches)}')
                return [self.info]
            else:
                for lca in self.info.lib_con_approaches:
                    log.debug(f'Consolidating {lca} cells')
                    lca_dir = Path(lca)
                    lca_dir.mkdir()

                    def matrix_filter(entry):
                        barcode_lineno = entry[1]
                        entry_lca = lib_con_data.iloc[barcode_lineno - 1]
                        return entry_lca == lca

                    # TODO only read this once and copy data
                    mtx = Mtx('matrix.mtx')
                    mtx.filter_entries(matrix_filter)
                    mtx.write(lca_dir / 'matrix.mtx')
                    for filename in ['genes.tsv', 'barcodes.tsv']:
                        (lca_dir / filename).symlink_to(f'../{filename}')

                return [
                    MatrixInfo(
                        source=self.info.source,
                        project_uuid=self.info.project_uuid,
                        zip_path=None,
                        extract_path=self.info.extract_path / lca,
                        lib_con_approaches=frozenset({lca})
                    )
                    for lca
                    in self.info.lib_con_approaches
                ]

    def rezip(self, zip_path: str = None, remove_dir: bool = False) -> None:
        """
        Compress unprocessed files to zip archive.
        This is the inverse operation of `unzip`.
        :param remove_dir: whether the extraction directory should be removed
        after zipping.
        :param zip_path: path of resulting zip archive. Updates matrix info if
        provided.
        """
        if zip_path is not None:
            self.info.zip_path = zip_path

        with ZipFile(self.info.zip_path, 'w') as zipfile:
            with DirectoryChange(self.info.extract_path):
                for filename in ['matrix.mtx', 'genes.tsv', 'barcodes.tsv']:
                    gzfilename = f'{filename}.gz'
                    with open(filename, 'rb') as infile:
                        with gzip.open(gzfilename, 'wb') as gzfile:
                            shutil.copyfileobj(infile, gzfile)
                            os.remove(filename)
                    zipfile.write(gzfilename)

        if remove_dir:
            shutil.rmtree(self.info.extract_path)
