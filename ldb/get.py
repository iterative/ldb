import os
from pathlib import Path
from typing import Iterable, Optional, Sequence

from ldb.add import add, paths_to_transforms
from ldb.config import get_ldb_dir
from ldb.core import init_quickstart
from ldb.data_formats import Format
from ldb.dataset import OpDef
from ldb.exceptions import LDBException, WorkspaceError
from ldb.instantiate import InstantiateResult, instantiate_collection
from ldb.path import WorkspacePath
from ldb.stage import stage_new
from ldb.utils import DATASET_PREFIX, ROOT, temp_dataset_name
from ldb.workspace import iter_workspace_dir


def get(
    workspace_path: Path,
    dest: Path,
    paths: Sequence[str],
    query_args: Iterable[OpDef],
    fmt: str = Format.BARE,
    apply: Sequence[str] = (),
    make_parent_dirs: bool = False,
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
        if workspace_path.exists() and any(iter_workspace_dir(workspace_path)):
            raise WorkspaceError(
                "Target must be empty, missing, or a valid workspace. "
                f"Directory is not empty: {os.fspath(workspace_path)!r}",
            )
        stage_new(
            workspace_path,
            temp_dataset_name(),
            make_parent_dirs=make_parent_dirs,
        )
    collection = add(
        workspace_path,
        paths,
        query_args,
        ldb_dir=ldb_dir,
    )
    transform_infos = paths_to_transforms(ldb_dir, paths)
    print("Instantiating data...\n")
    result = instantiate_collection(
        ldb_dir,
        dict(collection),
        dest,
        transform_infos=transform_infos,
        fmt=fmt,
        force=False,
        apply=apply,
        clean=False,
    )
    print(
        "Copied data to workspace.\n"
        f"  Data objects: {result.num_data_objects:9d}\n"
        f"  Annotations:  {result.num_annotations:9d}",
    )
    return result
