from pathlib import Path
from typing import Iterable, Optional, Sequence

from ldb.add import add
from ldb.config import get_ldb_dir
from ldb.core import init_quickstart
from ldb.data_formats import Format
from ldb.dataset import OpDef
from ldb.exceptions import LDBException
from ldb.instantiate import InstantiateResult, instantiate_collection
from ldb.path import WorkspacePath
from ldb.stage import stage_new
from ldb.utils import DATASET_PREFIX, ROOT, temp_dataset_name


def get(
    workspace_path: Path,
    dest: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    fmt: str = Format.BARE,
    force: bool = False,
    apply: Sequence[str] = (),
    ldb_dir: Optional[Path] = None,
) -> InstantiateResult:
    if not paths:
        if not query_args:
            raise LDBException(
                "Must provide either a query or at least one path",
            )
        paths = [f"{DATASET_PREFIX}{ROOT}"]

    if ldb_dir is None:
        ldb_dir = get_ldb_dir()
    if not ldb_dir.is_dir():
        ldb_dir = init_quickstart()

    if not (workspace_path / WorkspacePath.DATASET).is_file():
        stage_new(
            workspace_path,
            temp_dataset_name(),
        )
    collection = add(
        workspace_path,
        paths,
        query_args,
        ldb_dir=ldb_dir,
    )
    print("Instantiating data...\n")
    result = instantiate_collection(
        ldb_dir,
        workspace_path,
        dict(collection),
        dest,
        fmt,
        force,
        apply=apply,
        clean=False,
    )
    print(
        "Copied data to workspace.\n"
        f"  Data objects: {result.num_data_objects:9d}\n"
        f"  Annotations:  {result.num_annotations:9d}",
    )
    return result
