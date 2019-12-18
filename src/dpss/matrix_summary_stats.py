from typing import (
    Optional,
    Iterable,
)

from more_itertools import one
import pandas as pd
import scanpy as sc
import numpy as np
import logging
import matplotlib
import warnings

from dpss.matrix_info import MatrixInfo

log = logging.getLogger(__name__)

# See https://stackoverflow.com/questions/27147300/
# matplotlib-tcl-asyncdelete-async-handler-deleted-by-the-wrong-thread
# for why the following line is required.
matplotlib.use('Agg')


class MatrixSummaryStats:
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

    def __init__(self, mtx_info: MatrixInfo):
        self.info = mtx_info
        self.lca = one(self.info.lib_con_approaches)

    def create_images(self) -> None:
        figure_format = '.png'
        log.info(f'Figures saved in {figure_format} format.')
        log.info(f'Path to matrix files is {self.info.extract_path}')

        # SHOULDN'T '/figures/' be in a path somewhere? Or does scanpy automatically create
        # and populate that directory?

        # 1. Figure: highest-expressing genes.
        adata = sc.read_10x_mtx(self.info.extract_path, var_names='gene_symbols', cache=True)
        adata.var_names_make_unique()
        sc.pl.highest_expr_genes(adata, n_top=20, save=figure_format, show=False)  # write to disk

        # 2. Figure: Violin plots of cells, all genes, and percent of mitochondrial genes

        # Shouldn't this be AFTER QC plots? Whole point is show the quality of
        # the data. At he very least this needs to be documented where the images
        # are displayed, probably in the figures themselves.
        # TODO add these filter thresholds as annotations to the plots?
        sc.pp.filter_cells(adata, min_genes=self.MIN_GENE_COUNTS[self.lca])
        sc.pp.filter_genes(adata, min_cells=self.min_cell_count)

        mito_genes = adata.var_names.str.startswith('MT-')
        # For each cell compute fraction of counts of mitochondrian genes vs. all genes. The `.A1`
        # method flattens the matrix (i.e., converts it into an 1-by-n vector). This is necessary
        # as X is sparse (to transform to a dense array after summing).
        adata.obs['percent_mito_genes'] = np.sum(adata[:, mito_genes].X, axis=1).A1 / np.sum(adata.X, axis=1).A1
        # Add the total counts per cell as observations-annotation to adata.
        adata.obs['n_counts'] = adata.X.sum(axis=1).A1
        sc.pl.violin(adata, ['n_counts', 'n_genes', 'percent_mito_genes'],
                     jitter=0.4, multi_panel=True, save=figure_format, show=False)

        # 3. Figure: Number of genes over number of counts.
        sc.pl.scatter(adata, x='n_counts', y='n_genes',
                      save=f'_genes_vs_counts{figure_format}', show=False)

        # 4. Figure: Percent mitochondrial genes over number of counts.
        sc.pl.scatter(adata, x='n_counts', y='percent_mito_genes',
                      save=f'_percentMitoGenes_vs_count{figure_format}', show=False)

        # 5. Figure: visualize highly-variable genes:
        sc.pp.normalize_per_cell(adata, counts_per_cell_after=1e3)
        sc.pp.log1p(adata)  # logarithmize
        adata.raw = adata  # save raw data
        sc.pp.highly_variable_genes(adata, min_mean=0.05, max_mean=30, min_disp=1.9)
        sc.pl.highly_variable_genes(adata, save=figure_format, show=False)  # write to disk

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            # see https://www.gitmemory.com/issue/lmcinnes/umap/252/505984440

            # 6. Figure: Principal components, PC2 against PC1
            sc.tl.pca(adata, svd_solver='arpack')
            sc.pl.pca(adata, color='CST3', show=False, save=figure_format)

            # 7. Figure: tSNE, Umap 2 against Umap1, of Louvain and CST3.
            sc.pp.neighbors(adata, n_neighbors=10, n_pcs=40)
            sc.tl.umap(adata)
            sc.tl.louvain(adata)
            sc.pl.umap(adata, color=['louvain', 'CST3'], show=False, save=figure_format)

            # For Jing, Barcelona conference:
            results_file = f'./{self.info.project_uuid}_clusters.txt'
            df = pd.DataFrame(adata.obs['louvain'])
            df.columns = ['louvain cluster']
            df.to_csv(path_or_buf=results_file, sep='\t', index_label='cell')

            # 8. Figure: Ranks genes
            # Options for "method" in the following line are:
            # {'logreg', 't-test', 'wilcoxon', 't-test_overestim_var'}
            sc.tl.rank_genes_groups(adata, 'louvain', method='t-test')
            sc.pl.rank_genes_groups(adata, n_genes=10, sharey=False, show=False, save=figure_format)

            # For Jing, Barcelona conference:
            results_file = f'./{self.info.project_uuid}_marker_genes.txt'
            df = pd.DataFrame(adata.uns['rank_genes_groups']['names'])
            df.to_csv(path_or_buf=results_file, sep='\t')
