import os
from pathlib import Path
from typing import List

from ldb.exceptions import LDBException
from ldb.path import InstanceDir, WorkspacePath
from ldb.utils import get_hash_path, load_data_file


def add(ldb_dir: Path, workspace_path: Path, data_object_hashes: List[str]):
    workspace_path = Path(os.path.normpath(workspace_path))
    workspace_dataset_path = workspace_path / WorkspacePath.DATASET
    try:
        workspace_ds = load_data_file(workspace_dataset_path)
    except FileNotFoundError as exc:
        raise LDBException(
            "No workspace dataset staged at "
            f"{repr(os.fspath(workspace_path))}",
        ) from exc
    ds_name = workspace_ds["dataset_name"]
    collection_dir_path = workspace_path / WorkspacePath.COLLECTION
    collection_dir_path.mkdir(exist_ok=True)

    to_write = []
    for hash_str in data_object_hashes:
        data_object_dir = get_hash_path(
            ldb_dir / InstanceDir.DATA_OBJECT_INFO,
            hash_str,
        )
        if not data_object_dir.is_dir():
            raise LDBException(f"No data object found: {hash_str}")
        try:
            with (data_object_dir / "current").open() as file:
                annotation_hash = file.read()
        except FileNotFoundError:
            annotation_hash = ""
        to_write.append(
            (get_hash_path(collection_dir_path, hash_str), annotation_hash),
        )

    for collection_member_path, annotation_hash in to_write:
        collection_member_path.parent.mkdir(exist_ok=True)
        with collection_member_path.open("w") as file:
            file.write(annotation_hash)
    ds_ident = "ds:" + ds_name
    print(f"Added {len(data_object_hashes)} data objects to {repr(ds_ident)}")
