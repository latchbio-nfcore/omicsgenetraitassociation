from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: str, outdir: typing.Optional[typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})]], gene_col_name: typing.Optional[str], pval_col_name: typing.Optional[str], numtests: typing.Optional[int], alpha: typing.Optional[float], mmap_header: typing.Optional[int], mmap_pval_col: typing.Optional[str], mmap_beta_col: typing.Optional[str], mmap_se_genes: typing.Optional[str], pascal_header: typing.Optional[int], pascal_pval_col: typing.Optional[int]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('gene_col_name', gene_col_name),
                *get_flag('pval_col_name', pval_col_name),
                *get_flag('numtests', numtests),
                *get_flag('alpha', alpha),
                *get_flag('mmap_header', mmap_header),
                *get_flag('mmap_pval_col', mmap_pval_col),
                *get_flag('mmap_beta_col', mmap_beta_col),
                *get_flag('mmap_se_genes', mmap_se_genes),
                *get_flag('pascal_header', pascal_header),
                *get_flag('pascal_pval_col', pascal_pval_col),
                *get_flag('input', input),
                *get_flag('outdir', outdir)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_omicsgenetraitassociation", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_omicsgenetraitassociation(input: str, outdir: typing.Optional[typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})]], gene_col_name: typing.Optional[str] = 'markname', pval_col_name: typing.Optional[str] = 'meta_p', numtests: typing.Optional[int] = 17551, alpha: typing.Optional[float] = 0.05, mmap_header: typing.Optional[int] = 1, mmap_pval_col: typing.Optional[str] = 'p_vals', mmap_beta_col: typing.Optional[str] = 'betas_genes', mmap_se_genes: typing.Optional[str] = 'se_genes', pascal_header: typing.Optional[int] = 0, pascal_pval_col: typing.Optional[int] = 1) -> None:
    """
    nf-core/omicsgenetraitassociation

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, gene_col_name=gene_col_name, pval_col_name=pval_col_name, numtests=numtests, alpha=alpha, mmap_header=mmap_header, mmap_pval_col=mmap_pval_col, mmap_beta_col=mmap_beta_col, mmap_se_genes=mmap_se_genes, pascal_header=pascal_header, pascal_pval_col=pascal_pval_col, input=input, outdir=outdir)

