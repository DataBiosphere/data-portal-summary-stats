import os
from typing import Iterable

from more_itertools import one
import pandas as pd
import scanpy as sc
import numpy as np
import logging
import matplotlib
import warnings

from dpss.matrix_info import MatrixInfo
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)

# See https://stackoverflow.com/questions/27147300/
# matplotlib-tcl-asyncdelete-async-handler-deleted-by-the-wrong-thread
# for why the following line is required.
matplotlib.use('Agg')


class MatrixSummaryStats:
    figure_format = 'png'

    # What to do for this parameter?
    min_cell_count = 10

    MIN_GENE_COUNTS = {
        'SS2': 1200,
        '10X': 1200,
    }

    @classmethod
    def translate_lca(cls, lca: str) -> str:
        # note: project f8aa201c-4ff1-45a4-890e-840d63459ca2 declares the LCA
        # 'Smart-seq' but there don't seem to be any cells with that LCA after filtering
        if lca == 'Smart-seq2':
            return 'SS2'
        # both upper and lower case present in Azul
        elif lca.upper().startswith('10X'):
            return '10X'
        raise ValueError(f'Could not parse matrix LCA: {lca}')

    def __init__(self, mtx_infos: Iterable[MatrixInfo]):
        self.infos = list(mtx_infos)
        self.adatas = None

    def load_data(self):
        self.adatas = []

        for info in self.infos:

            adata = sc.read_10x_mtx(
                info.extract_path,
                var_names='gene_ids',
                cache=False
            )
            adata.var_names = adata.var_names.map(str)
            adata.var_names_make_unique()

            # Not actually doing any filtering at the moment,
            # but need these function calls to fill in vars
            sc.pp.filter_cells(adata, min_genes=0)
            sc.pp.filter_genes(adata, min_cells=0)

            # Add the total counts per cell as observations-annotation to adata.
            adata.obs['n_counts'] = adata.X.sum(axis=1).A1

            mito_genes = adata.var_names.str.startswith('MT-')
            found_mito_genes = any(mito_genes)
            adata.uns['found_mito_genes'] = found_mito_genes
            if found_mito_genes:
                # For each cell compute fraction of counts of mitochondrian genes vs. all genes. The `.A1`
                # method flattens the matrix (i.e., converts it into an 1-by-n vector). This is necessary
                # as X is sparse (to transform to a dense array after summing).
                adata.obs['percent_mito_genes'] = np.sum(adata[:, mito_genes].X, axis=1).A1 / np.sum(adata.X, axis=1).A1

            self.adatas.append(adata)

    def _create_image(self, name, callback, *args, **kwargs):
        fig, axs = plt.subplots(nrows=len(self.adatas), ncols=1, squeeze=False)
        for ax, adata in zip(axs.flat, self.adatas):
            callback(adata, ax=ax, *args, **kwargs, save=False, show=False)
        plt.tight_layout()
        os.makedirs('figures', exist_ok=True)
        plt.savefig(f'figures/{name}.{self.figure_format}')
        plt.close(fig=fig)

    def create_images(self) -> None:
        log.info(f'Figures saved in {self.figure_format} format.')

        # 1. Figure: highest-expressing genes.
        self._create_image('highest_expr_genes',
                           sc.pl.highest_expr_genes,
                           n_top=20)

        # 2. Figure: Violin plots of cells, all genes, and percent of mitochondrial genes
        keys = ['n_counts', 'n_genes']
        show_mito_genes = any(adata.uns['found_mito_genes'] for adata in self.adatas)
        if show_mito_genes:
            keys.append('percent_mito_genes')

        self._create_image('violin',
                           sc.pl.violin,
                           keys,
                           stripplot=False,
                           multi_panel=True)

        # 3. Figure: Number of genes over number of counts.
        self._create_image('scatter_genes_vs_counts',
                           sc.pl.scatter,
                           x='n_counts',
                           y='n_genes')

        if show_mito_genes:
            # 4. Figure: Percent mitochondrial genes over number of counts.
            self._create_image('scatter_percentMitoGenes_vs_count',
                               sc.pl.scatter,
                               x='n_counts',
                               y='percent_mito_genes')

