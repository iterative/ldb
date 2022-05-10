from pathlib import Path
from typing import Iterable, Optional, Sequence

from ldb.add import add
from ldb.core import get_ldb_instance
from ldb.data_formats import Format
from ldb.dataset import OpDef
from ldb.instantiate import InstantiateResult, instantiate_collection
from ldb.path import WorkspacePath
from ldb.stage import stage


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
    if ldb_dir is None:
        ldb_dir = get_ldb_instance()
    if not (workspace_path / WorkspacePath.DATASET).is_file():
        stage(
            "ds:a",
            workspace_path,
            force=force,
            ldb_dir=ldb_dir,
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
