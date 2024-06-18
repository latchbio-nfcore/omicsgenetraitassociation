
from dataclasses import dataclass
import typing
import typing_extensions

from flytekit.core.annotation import FlyteAnnotation

from latch.types.metadata import NextflowParameter
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir

# Import these into your `__init__.py` file:
#
# from .parameters import generated_parameters

generated_parameters = {
    'gene_col_name': NextflowParameter(
        type=typing.Optional[str],
        default='markname',
        section_title='MEA options',
        description='Column name for gene ID/names',
    ),
    'pval_col_name': NextflowParameter(
        type=typing.Optional[str],
        default='meta_p',
        section_title=None,
        description='Column name for p-values',
    ),
    'numtests': NextflowParameter(
        type=typing.Optional[int],
        default=17551,
        section_title=None,
        description='Number of tests for multiple testing',
    ),
    'alpha': NextflowParameter(
        type=typing.Optional[float],
        default=0.05,
        section_title=None,
        description='P-value threshold alpha value',
    ),
    'mmap_header': NextflowParameter(
        type=typing.Optional[int],
        default=1,
        section_title='MMAP options',
        description='Whether MMAP output has a header or not',
    ),
    'mmap_pval_col': NextflowParameter(
        type=typing.Optional[str],
        default='p_vals',
        section_title=None,
        description='Column name or number for p-values',
    ),
    'mmap_beta_col': NextflowParameter(
        type=typing.Optional[str],
        default='betas_genes',
        section_title=None,
        description='Column name or number for Beta values',
    ),
    'mmap_se_genes': NextflowParameter(
        type=typing.Optional[str],
        default='se_genes',
        section_title=None,
        description='Column name or number for SE values',
    ),
    'pascal_header': NextflowParameter(
        type=typing.Optional[int],
        default=0,
        section_title='PASCAL options',
        description='Whether PASCAL output has a header or not',
    ),
    'pascal_pval_col': NextflowParameter(
        type=typing.Optional[int],
        default=1,
        section_title=None,
        description='Column name or number for p-values',
    ),
    'input': NextflowParameter(
        type=str,
        default=None,
        section_title='Input/output options',
        description='path to samplesheet',
    ),
    'outdir': NextflowParameter(
        type=typing.Optional[typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})]],
        default=None,
        section_title=None,
        description='output directory',
    ),
}

