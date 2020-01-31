from collections import OrderedDict
import os
from typing import (
    Iterable,
    Dict,
    Callable,
)

from more_itertools import one
import scanpy as sc
import numpy as np
import logging
import matplotlib

from dpss.matrix_info import MatrixInfo
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)

# See https://stackoverflow.com/questions/27147300/
# matplotlib-tcl-asyncdelete-async-handler-deleted-by-the-wrong-thread
# for why the following line is required.
matplotlib.use('Agg')


class MatrixSummaryStats:
    """
    Analysis and plotting.
    """
    figure_format = 'png'
    figure_width = 6
    figure_dpi = 100

    default_example_gene = 'CST3'

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
        assert len(self.infos) > 0
        self.show_mito_genes = None
        self.example_gene = None
        self.adatas = None

    def load_data(self):
        self.adatas = []
        self.show_mito_genes = True

        for info in self.infos:

            adata = sc.read_10x_mtx(
                info.extract_path,
                var_names='gene_symbols',
                cache=False
            )

            # some files use numbers as ids which causes following steps to fail
            adata.var_names = adata.var_names.map(str)
            adata.obs_names = adata.obs_names.map(str)
            adata.var_names_make_unique()
            adata.obs_names_make_unique()

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
            self.show_mito_genes &= found_mito_genes

        common_gene = self.default_example_gene
        genes = iter(self.adatas[0].var_names)
        try:
            while not all(common_gene in adata.var_names for adata in self.adatas):
                common_gene = next(genes)
        except StopIteration:
            pass
        else:
            self.example_gene = common_gene

    @classmethod
    def target_images(cls) -> Dict[str, Callable]:
        # Processing order is important because some steps add annotations used
        # by later steps
        return OrderedDict([
            ('highest_expr_genes', cls.highest_expr_genes),
            ('violin', cls.violin),
            ('scatter_genes_vs_counts', cls.scatter_genes_counts),
            ('scatter_percentMitoGenes_vs_count', cls.scatter_mito_counts),
            ('highly_variable_genes', cls.highly_variable_genes),
            ('pca', cls.pca),
            ('umap', cls.umap),
            ('rank_gene_groups', cls.rank_gene_groups)
        ])

    def highest_expr_genes(self):
        # 1. Figure: highest-expressing genes.
        fig, axes = self._create_figure()
        self._plot(
            sc.pl.highest_expr_genes,
            fig, axes,
            n_top=20
        )
        for ax in axes.flat:
            ax.set_ylabel('')
        self._save_image('highest_expr_genes')

    def violin(self):
        # 2. Figure: Violin plots of cells, all genes, and percent of mitochondrial genes
        keys = ['n_counts', 'n_genes']

        if self.show_mito_genes:
            keys.append('percent_mito_genes')

        fig, axes = self._create_figure(len(keys))
        # can't use multi_panel because the FacetGrid will resize ignore the existing figure size
        for column, key in zip(axes.T, keys):
            self._plot(
                sc.pl.violin,
                fig, column,
                key,
                stripplot=False,
            )
            for ax in axes.flat:
                ax.set_ylabel('')
        self._save_image('violin')

    def scatter_genes_counts(self):
        # 3. Figure: Number of genes over number of counts.
        self._simple_plot(
            'scatter_genes_vs_counts',
            sc.pl.scatter,
            x='n_counts',
            y='n_genes'
        )

    def scatter_mito_counts(self):
        # 4. Figure: Percent mitochondrial genes over number of counts.
        if self.show_mito_genes:
            self._simple_plot(
                'scatter_percentMitoGenes_vs_count',
                sc.pl.scatter,
                x='n_counts',
                y='percent_mito_genes'
            )

    def highly_variable_genes(self):
        # 5. Figure: visualize highly-variable genes
        try:
            adata = one(self.adatas)
        except ValueError:
            log.error('Cannot generate highly_variable_genes for multiple matrices')
        else:
            sc.pp.normalize_per_cell(adata, counts_per_cell_after=1e3)
            sc.pp.log1p(adata)
            adata.raw = adata
            sc.pp.highly_variable_genes(adata, min_mean=0.05, max_mean=30, min_disp=1.9)
            sc.pl.highly_variable_genes(adata, show=False, save=False)
            self._set_fig_width(plt.gcf(), self.figure_width * 2)
            self._save_image('highly_variable_genes')

    def pca(self):
        # 6. Figure: Principal components, PC2 against PC1
        if self.example_gene is None:
            log.error('No common gene to plot pca')
        elif any(adata.shape[0] < 51 for adata in self.adatas):
            log.error('matrix is too small for pca')
        else:
            for adata in self.adatas:
                sc.tl.pca(adata, svd_solver='arpack')
            self._simple_plot('pca', sc.pl.pca, color=self.example_gene)

    def umap(self):
        # 7. Figure: tSNE, Umap 2 against Umap1, of Louvain and CST3.
        if any(adata.shape[0] < 40 for adata in self.adatas):
            log.error('matrix is too small for umap')
            return

        for adata in self.adatas:
            sc.pp.neighbors(adata, n_neighbors=10, n_pcs=40)
            sc.tl.umap(adata)
            sc.tl.louvain(adata)

        keys = ['louvain']
        if self.example_gene is not None:
            keys.append(self.example_gene)

        fig, axes = self._create_figure(len(keys))
        for column, key in zip(axes.T, keys):
            self._plot(sc.pl.umap, fig, column, color=key)
        self._set_fig_width(fig, self.figure_width * len(keys))
        self._save_image('umap')

    def rank_gene_groups(self):
        # 8. Figure: Ranks genes
        # Options for "method" in the following line are:
        # {'logreg', 't-test', 'wilcoxon', 't-test_overestim_var'}
        for adata in self.adatas:
            sc.tl.rank_genes_groups(adata, 'louvain', method='t-test')
        self._simple_plot('rank_gene_groups', sc.pl.rank_genes_groups, n_genes=10, sharey=False)

    def create_images(self) -> None:
        log.info(f'Figures saved in {self.figure_format} format.')

        for figure, method in self.target_images().items():
            try:
                method(self)
            except Exception:
                log.error(f'Failed to plot {figure}; continuing', exc_info=True)

    def _create_figure(self, ncols=1):
        return plt.subplots(
            nrows=len(self.adatas),
            ncols=ncols,
            squeeze=False
        )

    def _plot(self, callback, figure, axes, *sc_args, **sc_kwargs):
        for ax, adata in zip(axes.flat, self.adatas):
            callback(adata, ax=ax, *sc_args, **sc_kwargs, save=False, show=False)
        self._set_fig_width(figure, self.figure_width)

    def _save_image(self, name):
        if name not in self.target_images():
            log.warning(f'Generating non-target image {name}.{self.figure_format}')
        plt.tight_layout()
        os.makedirs('figures', exist_ok=True)
        plt.savefig(
            f'figures/{name}.{self.figure_format}',
            dpi=self.figure_dpi
        )
        plt.close('all')

    def _simple_plot(self, name, callback, *sc_args, **sc_kwargs):
        pl = self._create_figure()
        self._plot(callback, *pl, *sc_args, **sc_kwargs)
        self._save_image(name)

    @classmethod
    def _set_fig_width(cls, fig, width):
        fig.set_size_inches(width, fig.get_size_inches()[1])

